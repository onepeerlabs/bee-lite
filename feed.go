package beelite

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/ethersphere/bee/v2/pkg/feeds"
	"github.com/ethersphere/bee/v2/pkg/file/loadsave"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/manifest"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

type pipelineFunc func(context.Context, io.Reader) (swarm.Address, error)

func requestPipelineFn(s storage.Putter, encrypt bool, rLevel redundancy.Level) pipelineFunc {
	return func(ctx context.Context, r io.Reader) (swarm.Address, error) {
		pipe := builder.NewPipelineBuilder(ctx, s, encrypt, rLevel)
		return builder.FeedPipeline(ctx, pipe, r)
	}
}

func requestPipelineFactory(ctx context.Context, s storage.Putter, encrypt bool, rLevel redundancy.Level) func() pipeline.Interface {
	return func() pipeline.Interface {
		return builder.NewPipelineBuilder(ctx, s, encrypt, rLevel)
	}
}

func (bl *Beelite) AddFeed(ctx context.Context, batchHex, owner, topic string, encrypt bool, rLevel redundancy.Level) (reference swarm.Address, err error) {
	reference = swarm.ZeroAddress
	ownerB, err := hex.DecodeString(owner)
	if err != nil {
		bl.logger.Debug("feed put: decode owner: %v", err)
		return
	}
	topicB, err := hex.DecodeString(topic)
	if err != nil {
		bl.logger.Debug("feed put: decode topic: %v", err)
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

	factory := requestPipelineFactory(ctx, putter, encrypt, rLevel)
	l := loadsave.New(bl.storer.ChunkStore(), bl.storer.Cache(), factory)
	feedManifest, err := manifest.NewDefaultManifest(l, encrypt)
	if err != nil {
		bl.logger.Debug("feed put: create manifest failed: %v", err)
		return
	}

	meta := map[string]string{
		feedMetadataEntryOwner: hex.EncodeToString(ownerB),
		feedMetadataEntryTopic: hex.EncodeToString(topicB),
		feedMetadataEntryType:  feeds.Sequence.String(), // only sequence allowed for now
	}

	emptyAddr := make([]byte, 32)
	// a feed manifest stores the metadata at the root "/" path
	err = feedManifest.Add(ctx, "/", manifest.NewEntry(swarm.NewAddress(emptyAddr), meta))
	if err != nil {
		bl.logger.Debug("feed post: add manifest entry failed: %v", err)
		return
	}
	reference, err = feedManifest.Store(ctx)
	if err != nil {
		bl.logger.Debug("feed post: store manifest failed: %v", err)
		return
	}

	err = putter.Done(reference)
	if err != nil {
		bl.logger.Error(err, "done split failed")
		err = errors.Join(fmt.Errorf("done split failed: %w", err), putter.Cleanup())
		return
	}

	return
}
