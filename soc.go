package bee

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/ethersphere/bee/pkg/cac"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/soc"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

const (
	TopicLength    = 32
	MaxChunkLength = 4096
)

type Topic [TopicLength]byte

func (b *Bee) AddSOC(ctx context.Context, batchHex string, id, data []byte) (reference swarm.Address, err error) {

	if len(id) != TopicLength {
		err = fmt.Errorf("invalid topic length")
		return
	}
	if len(data) > MaxChunkLength {
		err = fmt.Errorf("payload size is too large. maximum payload size is %d bytes", MaxChunkLength)
		return
	}

	ch, err := cac.New(data)
	if err != nil {
		return
	}
	s := soc.New(id, ch)
	sch, err := s.Sign(b.signer)
	if err != nil {
		return
	}

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
	stamp, err := stamper.Stamp(sch.Address())
	if err != nil {
		b.logger.Debugf("soc upload: stamp: %v", err)
		return
	}
	sch = sch.WithStamp(stamp)
	_, err = b.ns.Put(ctx, storage.ModePutUpload, sch)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	reference = sch.Address()
	return
}
