package beelite

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/ethersphere/bee/pkg/feeds"
	"github.com/ethersphere/bee/pkg/file/loadsave"
	"github.com/ethersphere/bee/pkg/file/pipeline"
	"github.com/ethersphere/bee/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/pkg/manifest"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

type pipelineFunc func(context.Context, io.Reader) (swarm.Address, error)

func requestPipelineFn(s storage.Putter, encrypt bool /*, rLevel redundancy.Level*/) pipelineFunc {
	return func(ctx context.Context, r io.Reader) (swarm.Address, error) {
		pipe := builder.NewPipelineBuilder(ctx, s, encrypt /*, rLevel*/)
		return builder.FeedPipeline(ctx, pipe, r)
	}
}

func requestPipelineFactory(ctx context.Context, s storage.Putter, encrypt bool /*, rLevel redundancy.Level*/) func() pipeline.Interface {
	return func() pipeline.Interface {
		return builder.NewPipelineBuilder(ctx, s, encrypt /*, rLevel*/)
	}
}

func (bl *Beelite) AddFeed(ctx context.Context, batchHex, owner, topic string, encrypt bool) (reference swarm.Address, err error) {
	ownerB, err := hex.DecodeString(owner)
	if err != nil {
		bl.logger.Debug("feed put: decode owner: %v", err)
		return swarm.ZeroAddress, err
	}
	topicB, err := hex.DecodeString(topic)
	if err != nil {
		bl.logger.Debug("feed put: decode topic: %v", err)
		return swarm.ZeroAddress, err
	}
	if batchHex == "" {
		err = fmt.Errorf("batch is not set")
		return swarm.ZeroAddress, err
	}
	batch, err := hex.DecodeString(batchHex)
	if err != nil {
		err = fmt.Errorf("invalid postage batch")
		return swarm.ZeroAddress, err
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
			return swarm.ZeroAddress, err
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
		return swarm.ZeroAddress, err
	}

	factory := requestPipelineFactory(ctx, putter, encrypt /*, 0*/)
	l := loadsave.New(bl.storer.ChunkStore() /*, bl.storer.Cache()*/, factory)
	feedManifest, err := manifest.NewDefaultManifest(l, encrypt)
	if err != nil {
		bl.logger.Debug("feed put: create manifest failed: %v", err)
		return swarm.ZeroAddress, err
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
		return swarm.ZeroAddress, err
	}
	reference, err = feedManifest.Store(ctx)
	if err != nil {
		bl.logger.Debug("feed post: store manifest failed: %v", err)
		return swarm.ZeroAddress, err
	}

	err = putter.Done(reference)
	if err != nil {
		bl.logger.Error(err, "done split failed")
		return swarm.ZeroAddress, err
	}

	return
}
