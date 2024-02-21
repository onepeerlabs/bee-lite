package beelite

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/ethersphere/bee/pkg/cac"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

func (bl *Beelite) GetChunk(parentContext context.Context, reference swarm.Address) (swarm.Chunk, error) {
	// TODO: add cache option
	cache := true
	chunk, err := bl.storer.Download(cache).Get(parentContext, reference)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			msg := fmt.Sprintf("chunk: chunk not found. addr %s", reference)
			bl.logger.Debug(msg)
			return nil, fmt.Errorf(msg)

		}
		return nil, fmt.Errorf("chunk: chunk read error: %v ,addr %s", err, reference)
	}
	return chunk, nil
}

func (bl *Beelite) AddChunk(parentContext context.Context, batchHex string, reader io.Reader, swarmTag uint64) (swarm.Address, error) {
	batch, err := hex.DecodeString(batchHex)
	if err != nil {
		err = fmt.Errorf("invalid postage batch")
		return swarm.ZeroAddress, err
	}

	var (
		tag uint64
	)

	if swarmTag > 0 {
		tag, err = bl.getOrCreateSessionID(swarmTag)
		if err != nil {
			bl.logger.Error(err, "get or create tag failed")
			return swarm.ZeroAddress, err
		}
	}
	deferred := tag != 0
	putter, err := bl.newStamperPutter(parentContext, putterOptions{
		BatchID:  batch,
		TagID:    tag,
		Deferred: deferred,
	})
	if err != nil {
		bl.logger.Error(err, "get putter failed")
		return swarm.ZeroAddress, err
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		bl.logger.Error(err, "chunk upload: read chunk data failed")
		return swarm.ZeroAddress, err

	}

	if len(data) < swarm.SpanSize {
		err = errors.New("insufficient data length")
		bl.logger.Error(err, "chunk upload: insufficient data length")
		return swarm.ZeroAddress, err
	}

	chunk, err := cac.NewWithDataSpan(data)
	if err != nil {
		bl.logger.Error(err, "chunk upload: create chunk error")
		return swarm.ZeroAddress, err
	}

	err = putter.Put(parentContext, chunk)
	if err != nil {
		bl.logger.Error(err, "chunk upload: write chunk failed", "chunk_address", chunk.Address())
		return swarm.ZeroAddress, err
	}

	err = putter.Done(chunk.Address())
	if err != nil {
		bl.logger.Error(err, "done split failed")
		return swarm.ZeroAddress, err
	}
	return chunk.Address(), nil
}
