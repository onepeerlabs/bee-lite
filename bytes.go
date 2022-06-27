package bee

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ethersphere/bee/pkg/file/joiner"
	"github.com/ethersphere/bee/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/sctx"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"io"
)

func (b *Bee) AddBytes(parentContext context.Context, batchHex string, reader io.Reader) (reference swarm.Address, err error) {
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
		err = fmt.Errorf("upload failed: %w", err)
		return
	}
	return
}

func (b *Bee) GetBytes(parentContext context.Context, reference swarm.Address) (io.Reader, error) {
	reader, _, err := joiner.New(parentContext, b.ns, reference)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("api download: not found : %s", err.Error())
		}
		return nil, fmt.Errorf("unexpected error: %s: %v", reference, err)
	}
	return reader, nil
}
