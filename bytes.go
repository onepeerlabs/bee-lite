package beelite

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/ethersphere/bee/pkg/file/joiner"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

func (bl *Beelite) AddBytes(parentContext context.Context, batchHex string, encrypt bool, reader io.Reader) (reference swarm.Address, err error) {
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
		BatchID:  batch,
		TagID:    tag,
		Pin:      pin,
		Deferred: deferred,
	})
	if err != nil {
		bl.logger.Error(err, "get putter failed")
		return
	}

	// TODO: v2.0.0-rc1 rLevel := redundancyLevelFromInt(r)
	// var rLevel redundancy.Level
	p := requestPipelineFn(putter, encrypt)
	reference, err = p(parentContext, reader)
	if err != nil {
		err = fmt.Errorf("(split write all) upload failed 1: %w", err)
		return
	}

	err = putter.Done(reference)
	if err != nil {
		err = fmt.Errorf("(done split) upload failed 2: %w", err)
		return
	}
	return
}

func (bl *Beelite) GetBytes(parentContext context.Context, reference swarm.Address) (io.Reader, error) {
	// TODO: add cache option
	cache := true
	reader, _, err := joiner.New(parentContext, bl.storer.Download(cache), reference)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("api download: not found : %w", err)
		}
		return nil, fmt.Errorf("unexpected error: %v: %v", reference, err)
	}
	return reader, nil
}
