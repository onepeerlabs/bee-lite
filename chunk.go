package beelite

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

func (bl *Beelite) GetChunk(parentContext context.Context, reference swarm.Address) (swarm.Chunk, error) {
	chunk, err := bl.Storer.Download(true).Get(parentContext, reference)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			msg := fmt.Sprintf("chunk: chunk not found. addr %s", reference)
			bl.Logger.Debug(msg)
			return nil, fmt.Errorf(msg)

		}
		return nil, fmt.Errorf("chunk: chunk read error: %v ,addr %s", err, reference)
	}
	return chunk, nil
}

func (bl *Beelite) AddChunk(parentContext context.Context, batchHex string, chunk swarm.Chunk) (swarm.Address, error) {
	batch, err := hex.DecodeString(batchHex)
	if err != nil {
		err = fmt.Errorf("invalid postage batch")
		return swarm.ZeroAddress, err
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
			return swarm.ZeroAddress, err
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
		return swarm.ZeroAddress, err
	}

	err = putter.Put(parentContext, chunk)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	return chunk.Address(), nil
}
