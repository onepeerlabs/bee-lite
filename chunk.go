package beelite

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

func (bl *Beelite) GetChunk(parentContext context.Context, reference swarm.Address) (swarm.Chunk, error) {
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

func (bl *Beelite) AddChunk(parentContext context.Context, batchHex string, reader io.Reader, swarmTag uint64) (reference swarm.Address, err error) {
	reference = swarm.ZeroAddress
	batch, err := hex.DecodeString(batchHex)
	if err != nil {
		err = fmt.Errorf("invalid postage batch")
		return
	}

	var (
		tag uint64
	)

	if swarmTag > 0 {
		tag, err = bl.getOrCreateSessionID(swarmTag)
		if err != nil {
			bl.logger.Error(err, "get or create tag failed")
			return
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
		return
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		bl.logger.Error(err, "chunk upload: read chunk data failed")
		return

	}

	if len(data) < swarm.SpanSize {
		err = errors.New("insufficient data length")
		bl.logger.Error(err, "chunk upload: insufficient data length")
		return
	}

	chunk, err := cac.NewWithDataSpan(data)
	if err != nil {
		bl.logger.Error(err, "chunk upload: create chunk error")
		return
	}

	err = putter.Put(parentContext, chunk)
	if err != nil {
		bl.logger.Error(err, "chunk upload: write chunk failed", "chunk_address", chunk.Address())
		return
	}

	err = putter.Done(chunk.Address())
	if err != nil {
		bl.logger.Error(err, "done split failed")
		err = errors.Join(fmt.Errorf("done split failed: %w", err), putter.Cleanup())
		return
	}

	reference = chunk.Address()
	return
}
