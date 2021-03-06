package bee

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/pkg/feeds"
	"github.com/ethersphere/bee/pkg/file/loadsave"
	"github.com/ethersphere/bee/pkg/file/pipeline"
	"github.com/ethersphere/bee/pkg/manifest"
	"github.com/ethersphere/bee/pkg/soc"
	"io"
	"path"
	"path/filepath"
	"time"

	"github.com/ethersphere/bee/pkg/file/joiner"
	"github.com/ethersphere/bee/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/sctx"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

var errInvalidFeedUpdate = errors.New("invalid feed update")

func (b *Bee) AddFileBzz(parentContext context.Context, batchHex, filename, contentType string, reader io.Reader) (reference swarm.Address, err error) {
	if batchHex == "" {
		err = fmt.Errorf("batch is not set")
		return
	}
	batch, err := hex.DecodeString(batchHex)
	if err != nil {
		err = fmt.Errorf("invalid postage batch")
		return
	}
	i, err := b.post.GetStampIssuer(batch)
	if err != nil {
		err = fmt.Errorf("stamp issuer: %w", err)
		return
	}
	tag, err := b.tagService.Create(0)
	if err != nil {
		err = fmt.Errorf("tagService create: %w", err)
		return
	}
	stamper := postage.NewStamper(i, b.signer)
	putter := &stamperPutter{Storer: b.ns, stamper: stamper}
	ctx := sctx.SetTag(parentContext, tag)
	pipe := builder.NewPipelineBuilder(ctx, putter, storage.ModePutUpload, false)
	reference, err = builder.FeedPipeline(ctx, pipe, reader)
	if err != nil {
		err = fmt.Errorf("upload failed 0: %w", err)
		return
	}
	if filename == "" {
		filename = reference.String()
	}
	pipelineFactory := func() pipeline.Interface {
		return builder.NewPipelineBuilder(ctx, putter, storage.ModePutUpload, false)
	}
	l := loadsave.New(putter, pipelineFactory)
	m, err := manifest.NewDefaultManifest(l, false)
	if err != nil {
		err = fmt.Errorf("upload failed 1: %w", err)
		return
	}
	rootMetadata := map[string]string{
		manifest.WebsiteIndexDocumentSuffixKey: filename,
	}
	err = m.Add(ctx, manifest.RootPath, manifest.NewEntry(swarm.ZeroAddress, rootMetadata))
	if err != nil {
		err = fmt.Errorf("upload failed 2: %w", err)
		return
	}
	fileMtdt := map[string]string{
		manifest.EntryMetadataContentTypeKey: contentType,
		manifest.EntryMetadataFilenameKey:    filename,
	}

	err = m.Add(ctx, filename, manifest.NewEntry(reference, fileMtdt))
	if err != nil {
		err = fmt.Errorf("upload failed 3: %w", err)
		return
	}
	b.logger.Debugf("bzz upload file: filename: %s hash: %s metadata: %v",
		filename, reference.String(), fileMtdt)
	storeSizeFn := []manifest.StoreSizeFunc{}
	manifestReference, err := m.Store(ctx, storeSizeFn...)
	if err != nil {
		err = fmt.Errorf("upload failed 4: %w", err)
		return
	}
	_, err = tag.DoneSplit(manifestReference)
	if err != nil {
		err = fmt.Errorf("upload failed 5: %w", err)
		return
	}
	reference = manifestReference
	return
}

func (b *Bee) GetBzz(parentContext context.Context, address swarm.Address) (io.Reader, string, error) {
	ls := loadsave.NewReadonly(b.ns)
	feedDereferenced := false

	ctx := parentContext

FETCH:
	// read manifest entry
	m, err := manifest.NewDefaultManifestReference(
		address,
		ls,
	)
	if err != nil {
		return nil, "", err
	}

	// there's a possible ambiguity here, right now the data which was
	// read can be an entry.Entry or a mantaray feed manifest. Try to
	// unmarshal as mantaray first and possibly resolve the feed, otherwise
	// go on normally.
	if !feedDereferenced {
		if l, err := b.manifestFeed(ctx, m); err == nil {
			//we have a feed manifest here
			ch, cur, _, err := l.At(ctx, time.Now().Unix(), 0)
			if err != nil {
				return nil, "", err
			}
			if ch == nil {
				return nil, "", err
			}
			ref, _, err := parseFeedUpdate(ch)
			if err != nil {
				return nil, "", err
			}
			address = ref
			feedDereferenced = true
			curBytes, err := cur.MarshalBinary()
			if err != nil {
				return nil, "", err
			}
			_ = curBytes
			goto FETCH
		}
	}
	if indexDocumentSuffixKey, ok := manifestMetadataLoad(ctx, m, manifest.RootPath, manifest.WebsiteIndexDocumentSuffixKey); ok {
		pathWithIndex := path.Join("", indexDocumentSuffixKey)
		indexDocumentManifestEntry, err := m.Lookup(ctx, pathWithIndex)
		if err == nil {
			// index document exists
			b.logger.Debugf("bzz download: serving path: %s", pathWithIndex)
			mtdt := indexDocumentManifestEntry.Metadata()
			fname, ok := mtdt[manifest.EntryMetadataFilenameKey]
			if ok {
				fname = filepath.Base(fname) // only keep the file name
			}
			reader, _, err := joiner.New(parentContext, b.ns, indexDocumentManifestEntry.Reference())
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return nil, "", fmt.Errorf("api download: not found : %s", err.Error())
				}
				return nil, "", fmt.Errorf("unexpected error: %s: %v", indexDocumentManifestEntry.Reference(), err)
			}
			return reader, fname, nil
		}
	}

	return nil, "", fmt.Errorf("failed to get bzz reference")
}

func (b *Bee) manifestFeed(
	ctx context.Context,
	m manifest.Interface,
) (feeds.Lookup, error) {
	e, err := m.Lookup(ctx, "/")
	if err != nil {
		return nil, fmt.Errorf("node lookup: %w", err)
	}
	var (
		owner, topic []byte
		t            = new(feeds.Type)
	)
	meta := e.Metadata()
	if e := meta[feedMetadataEntryOwner]; e != "" {
		owner, err = hex.DecodeString(e)
		if err != nil {
			return nil, err
		}
	}
	if e := meta[feedMetadataEntryTopic]; e != "" {
		topic, err = hex.DecodeString(e)
		if err != nil {
			return nil, err
		}
	}
	if e := meta[feedMetadataEntryType]; e != "" {
		err := t.FromString(e)
		if err != nil {
			return nil, err
		}
	}
	if len(owner) == 0 || len(topic) == 0 {
		return nil, fmt.Errorf("node lookup: %s", "feed metadata absent")
	}
	f := feeds.New(topic, common.BytesToAddress(owner))
	return b.feedFactory.NewLookup(*t, f)
}

func parseFeedUpdate(ch swarm.Chunk) (swarm.Address, int64, error) {
	s, err := soc.FromChunk(ch)
	if err != nil {
		return swarm.ZeroAddress, 0, fmt.Errorf("soc unmarshal: %w", err)
	}

	update := s.WrappedChunk().Data()
	// split the timestamp and reference
	// possible values right now:
	// unencrypted ref: span+timestamp+ref => 8+8+32=48
	// encrypted ref: span+timestamp+ref+decryptKey => 8+8+64=80
	if len(update) != 48 && len(update) != 80 {
		return swarm.ZeroAddress, 0, errInvalidFeedUpdate
	}
	ts := binary.BigEndian.Uint64(update[8:16])
	ref := swarm.NewAddress(update[16:])
	return ref, int64(ts), nil
}

// manifestMetadataLoad returns the value for a key stored in the metadata of
// manifest path, or empty string if no value is present.
// The ok result indicates whether value was found in the metadata.
func manifestMetadataLoad(
	ctx context.Context,
	manifest manifest.Interface,
	path, metadataKey string,
) (string, bool) {
	me, err := manifest.Lookup(ctx, path)
	if err != nil {
		return "", false
	}

	manifestRootMetadata := me.Metadata()
	if val, ok := manifestRootMetadata[metadataKey]; ok {
		return val, ok
	}

	return "", false
}
