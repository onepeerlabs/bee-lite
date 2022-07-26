package bee

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

const (
	TopicLength = 32
)

type Topic [TopicLength]byte

func (b *Bee) AddSOC(ctx context.Context, batchHex string, ch swarm.Chunk) (reference swarm.Address, err error) {
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
		b.logger.Debugf("soc upload: postage batch issuer: %v", err)
		return
	}
	stamper := postage.NewStamper(i, b.signer)
	stamp, err := stamper.Stamp(ch.Address())
	if err != nil {
		b.logger.Debugf("soc upload: stamp: %v", err)
		return
	}
	ch = ch.WithStamp(stamp)
	_, err = b.ns.Put(ctx, storage.ModePutUpload, ch)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	reference = ch.Address()
	return
}
