package bee

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/ethersphere/bee/pkg/swarm"
)

const (
	TopicLength = 32
)

type Topic [TopicLength]byte

func (bl *Beelite) AddSOC(ctx context.Context, batchHex string, ch swarm.Chunk) (reference swarm.Address, err error) {
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
			bl.logger.Error(err, "get or create tag failed")
			return
		}
	}
	putter, err := bl.newStamperPutter(ctx, putterOptions{
		BatchID:  batch,
		TagID:    tag,
		Pin:      pin,
		Deferred: deferred,
	})
	if err != nil {
		bl.logger.Error(err, "get putter failed")
		return
	}
	stamper, _, err := bl.getStamper(batch)
	if err != nil {
		bl.logger.Error(err, "get stamper failed")
		return
	}

	stamp, err := stamper.Stamp(ch.Address())
	if err != nil {
		bl.logger.Error(err, "soc upload: stamping failed")
		return
	}
	ch = ch.WithStamp(stamp)
	err = putter.Put(ctx, ch)
	if err != nil {
		bl.logger.Error(err, "soc upload: put failed")
		return
	}

	reference = ch.Address()
	return
}
