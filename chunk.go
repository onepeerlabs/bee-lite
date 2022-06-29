package bee

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

func (b *Bee) GetChunk(parentContext context.Context, reference swarm.Address) (swarm.Chunk, error) {
	chunk, err := b.ns.Get(parentContext, storage.ModeGetRequest, reference)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			b.logger.Tracef("chunk: chunk not found. addr %s", reference)
			return nil, fmt.Errorf("chunk: chunk not found. addr %s", reference)

		}
		return nil, fmt.Errorf("chunk: chunk read error: %v ,addr %s", err, reference)
	}
	return chunk, nil
}

func (b *Bee) AddChunk(parentContext context.Context, batchHex string, chunk swarm.Chunk) (swarm.Address, error) {
	batch, err := hex.DecodeString(batchHex)
	if err != nil {
		err = fmt.Errorf("invalid postage batch")
		return swarm.ZeroAddress, err
	}
	i, err := b.post.GetStampIssuer(batch)
	if err != nil {
		err = fmt.Errorf("stamp issuer: %w", err)
		return swarm.ZeroAddress, err
	}
	stamper := postage.NewStamper(i, b.signer)
	putter := &stamperPutter{Storer: b.ns, stamper: stamper}
	_, err = putter.Put(parentContext, storage.ModePutUpload, chunk)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	return chunk.Address(), nil
}
