package beelite

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/ethersphere/bee/pkg/cac"
	"github.com/ethersphere/bee/pkg/soc"
	"github.com/ethersphere/bee/pkg/swarm"
)

const (
	TopicLength = 32
)

type Topic [TopicLength]byte

func (bl *Beelite) AddSOC(ctx context.Context,
	batchHex string,
	reader io.Reader,
	id []byte,
	owner []byte,
	sig []byte,
) (reference swarm.Address, err error) {
	reference = swarm.ZeroAddress
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
		tag uint64
		pin = false
	)

	// TODO: add deferred and pinning options
	// if pinning header is set we do a deferred upload, else we do a direct upload
	if pin {
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
		Deferred: pin,
	})
	if err != nil {
		bl.logger.Error(err, "get putter failed")
		return
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		bl.logger.Error(err, "soc upload: read chunk data failed")
		return

	}

	if len(data) < swarm.SpanSize {
		err = errors.New("chunk data too short")
		bl.logger.Error(err, "soc upload: chunk data too short")
		return
	}

	if len(data) > swarm.ChunkSize+swarm.SpanSize {
		err = errors.New("chunk data exceeds required length")
		bl.logger.Error(err, "required_length", swarm.ChunkSize+swarm.SpanSize)
		return
	}

	chunk, err := cac.NewWithDataSpan(data)
	if err != nil {
		bl.logger.Error(err, "soc upload: create content addressed chunk failed")
		return
	}

	ss, err := soc.NewSigned(id, chunk, owner, sig)
	if err != nil {
		bl.logger.Error(err, "create soc failed", "id", id, "owner", owner, "error", err)
		return
	}

	sch, err := ss.Chunk()
	if err != nil {
		bl.logger.Error(err, "read chunk data failed", "error")
		return
	}

	if !soc.Valid(sch) {
		bl.logger.Error(nil, "invalid chunk", "error")
		return swarm.ZeroAddress, nil
	}

	err = putter.Put(ctx, sch)
	if err != nil {
		bl.logger.Error(err, "soc upload: write chunk failed", "chunk_address", chunk.Address())
		return
	}

	err = putter.Done(sch.Address())
	if err != nil {
		bl.logger.Error(err, "done split failed")
		err = errors.Join(fmt.Errorf("done split failed: %w", err), putter.Cleanup())
		return
	}

	reference = sch.Address()
	return
}
