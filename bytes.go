package beelite

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/ethersphere/bee/pkg/file/joiner"
	"github.com/ethersphere/bee/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

func (bl *Beelite) AddBytes(parentContext context.Context, batchHex string, reader io.Reader) (reference swarm.Address, err error) {
	if batchHex == "" {
		err = fmt.Errorf("batch is not set")
		return
	}
	batch, err := hex.DecodeString(batchHex)
	if err != nil {
		err = fmt.Errorf("invalid postage batch")
		return
	}

	var (
		tag      uint64
		deferred = false
		pin = false
	)

	if deferred || pin {
		tag, err = bl.getOrCreateSessionID(uint64(0))
		if err != nil {
			bl.Logger.Error(err, "get or create tag failed")
			return
		}
	}
	putter, err := bl.newStamperPutter(parentContext, putterOptions{
		BatchID:  batch,
		TagID:    tag,
		Pin:      pin,
		Deferred: deferred,
	})
	if err != nil {
		bl.Logger.Error(err, "get putter failed")
		return
	}

	encrypt := false
	// TODO: v2.0.0-rc1 rLevel := redundancyLevelFromInt(r)
	// var rLevel redundancy.Level
	pipe := builder.NewPipelineBuilder(parentContext, putter, encrypt)
	reference, err = builder.FeedPipeline(parentContext, pipe, reader)
	if err != nil {
		err = fmt.Errorf("upload failed: %w", err)
		return
	}
	return
}

func (bl *Beelite) GetBytes(parentContext context.Context, reference swarm.Address) (io.Reader, error) {
	reader, _, err := joiner.New(parentContext, bl.Storer.Download(true), reference)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("api download: not found : %v", err.Error())
		}
		return nil, fmt.Errorf("unexpected error: %v: %v", reference, err)
	}
	return reader, nil
}
