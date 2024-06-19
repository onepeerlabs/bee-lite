package beelite

import (
	"archive/tar"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/ethersphere/bee/v2/pkg/file/loadsave"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/manifest"
	storage "github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

var (
	errEmptyDir           = errors.New("no files in root directory")
	errInvalidContentType = errors.New("invalid content-type")
)

const (
	multiPartFormData   = "multipart/form-data"
	contentTypeTar      = "application/x-tar"
	ContentTypeHeader   = "Content-Type"
	ContentLengthHeader = "Content-Length"
)

func (bl *Beelite) AddDirBzz(
	parentContext context.Context,
	batchHex,
	filename,
	contentType,
	indexFilename,
	errorFilename string,
	encrypt bool,
	rLevel redundancy.Level,
	reader io.Reader,
) (reference swarm.Address, err error) {
	reference = swarm.ZeroAddress
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		err = fmt.Errorf("content type parse failed: %w", err)
		return
	}

	var dReader dirReader
	switch mediaType {
	case contentTypeTar:
		dReader = &tarReader{r: tar.NewReader(reader), logger: bl.logger}
	case multiPartFormData:
		dReader = &multipartReader{r: multipart.NewReader(reader, params["boundary"])}
	default:
		err = errInvalidContentType
		bl.logger.Error(err, "invalid content-type for directory upload")
		return
	}

	if batchHex == "" {
		err = fmt.Errorf("batch is not set")
		return
	}
	batchID, err := hex.DecodeString(batchHex)
	if err != nil {
		err = fmt.Errorf("invalid postage batch")
		return
	}

	var (
		tag      uint64
		deferred = false
		pin      = false
	)

	if deferred || pin {
		tag, err = bl.getOrCreateSessionID(uint64(0))
		if err != nil {
			bl.logger.Error(err, "get or create tag failed")
			return
		}
	}
	putter, err := bl.newStamperPutter(parentContext, putterOptions{
		BatchID:  batchID,
		TagID:    tag,
		Pin:      pin,
		Deferred: deferred,
	})
	if err != nil {
		err = fmt.Errorf("get putter failed: %w", err)
		return
	}

	reference, err = bl.storeDir(
		parentContext,
		encrypt,
		dReader,
		putter,
		bl.storer.ChunkStore(),
		indexFilename,
		errorFilename,
		rLevel,
	)
	if err != nil {
		err = fmt.Errorf("store dir failed 1: %w", err)
		return
	}

	err = putter.Done(reference)
	if err != nil {
		bl.logger.Error(err, "store dir failed")
		err = errors.Join(fmt.Errorf("store dir failed 2: %w", err), putter.Cleanup())
		return
	}

	return
}

// storeDir stores all files recursively contained in the directory given as a tar/multipart
// it returns the hash for the uploaded manifest corresponding to the uploaded dir
func (bl *Beelite) storeDir(
	ctx context.Context,
	encrypt bool,
	reader dirReader,
	putter storage.Putter,
	getter storage.Getter,
	indexFilename,
	errorFilename string,
	rLevel redundancy.Level,
) (swarm.Address, error) {
	p := requestPipelineFn(putter, encrypt, rLevel)
	factory := requestPipelineFactory(ctx, putter, encrypt, rLevel)
	ls := loadsave.New(getter, putter, factory)

	dirManifest, err := manifest.NewDefaultManifest(ls, encrypt)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	if indexFilename != "" && strings.ContainsRune(indexFilename, '/') {
		return swarm.ZeroAddress, errors.New("index document suffix must not include slash character")
	}

	filesAdded := 0

	// iterate through the files in the supplied tar
	for {
		fileInfo, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return swarm.ZeroAddress, fmt.Errorf("read dir stream: %w", err)
		}

		fileReference, err := p(ctx, fileInfo.Reader)
		if err != nil {
			return swarm.ZeroAddress, fmt.Errorf("store dir file: %w", err)
		}
		bl.logger.Debug("bzz upload dir: file dir uploaded", "file_path", fileInfo.Path, "address", fileReference)

		fileMtdt := map[string]string{
			manifest.EntryMetadataContentTypeKey: fileInfo.ContentType,
			manifest.EntryMetadataFilenameKey:    fileInfo.Name,
		}
		// add file entry to dir manifest
		err = dirManifest.Add(ctx, fileInfo.Path, manifest.NewEntry(fileReference, fileMtdt))
		if err != nil {
			return swarm.ZeroAddress, fmt.Errorf("add to manifest: %w", err)
		}

		filesAdded++
	}

	// check if files were uploaded through the manifest
	if filesAdded == 0 {
		return swarm.ZeroAddress, errEmptyDir
	}

	// store website information
	if indexFilename != "" || errorFilename != "" {
		metadata := map[string]string{}
		if indexFilename != "" {
			metadata[manifest.WebsiteIndexDocumentSuffixKey] = indexFilename
		}
		if errorFilename != "" {
			metadata[manifest.WebsiteErrorDocumentPathKey] = errorFilename
		}
		rootManifestEntry := manifest.NewEntry(swarm.ZeroAddress, metadata)
		err = dirManifest.Add(ctx, manifest.RootPath, rootManifestEntry)
		if err != nil {
			return swarm.ZeroAddress, fmt.Errorf("add to manifest: %w", err)
		}
	}

	// save manifest
	manifestReference, err := dirManifest.Store(ctx)
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("store manifest: %w", err)
	}
	bl.logger.Debug("bzz upload dir: uploaded dir finished", "address", manifestReference)

	return manifestReference, nil
}

type FileInfo struct {
	Path        string
	Name        string
	ContentType string
	Size        int64
	Reader      io.Reader
}

type dirReader interface {
	Next() (*FileInfo, error)
}

type tarReader struct {
	r      *tar.Reader
	logger log.Logger
}

func (t *tarReader) Next() (*FileInfo, error) {
	for {
		fileHeader, err := t.r.Next()
		if err != nil {
			return nil, err
		}

		fileName := fileHeader.FileInfo().Name()
		contentType := mime.TypeByExtension(filepath.Ext(fileHeader.Name))
		fileSize := fileHeader.FileInfo().Size()
		filePath := filepath.Clean(fileHeader.Name)

		if filePath == "." {
			t.logger.Warning("skipping file upload empty path")
			continue
		}
		if runtime.GOOS == "windows" {
			// always use Unix path separator
			filePath = filepath.ToSlash(filePath)
		}
		// only store regular files
		if !fileHeader.FileInfo().Mode().IsRegular() {
			t.logger.Warning("bzz upload dir: skipping file upload as it is not a regular file", "file_path", filePath)
			continue
		}

		return &FileInfo{
			Path:        filePath,
			Name:        fileName,
			ContentType: contentType,
			Size:        fileSize,
			Reader:      t.r,
		}, nil
	}
}

// multipart reader returns files added as a multipart form. We will ensure all the
// part headers are passed correctly
type multipartReader struct {
	r *multipart.Reader
}

func (m *multipartReader) Next() (*FileInfo, error) {
	part, err := m.r.NextPart()
	if err != nil {
		return nil, err
	}

	filePath := part.FileName()
	if filePath == "" {
		filePath = part.FormName()
	}
	if filePath == "" {
		return nil, errors.New("filepath missing")
	}

	fileName := filepath.Base(filePath)

	contentType := part.Header.Get(ContentTypeHeader)
	if contentType == "" {
		return nil, errors.New("content-type missing")
	}

	contentLength := part.Header.Get(ContentLengthHeader)
	if contentLength == "" {
		return nil, errors.New("content-length missing")
	}
	fileSize, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		return nil, errors.New("invalid file size")
	}

	return &FileInfo{
		Path:        filePath,
		Name:        fileName,
		ContentType: contentType,
		Size:        fileSize,
		Reader:      part,
	}, nil
}
