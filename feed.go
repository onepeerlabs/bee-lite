package beelite

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/ethersphere/bee/pkg/feeds"
	"github.com/ethersphere/bee/pkg/file/loadsave"
	"github.com/ethersphere/bee/pkg/file/pipeline"
	"github.com/ethersphere/bee/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/pkg/manifest"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

func requestPipelineFactory(ctx context.Context, s storage.Putter, encrypt bool/*, rLevel redundancy.Level*/) func() pipeline.Interface {
	return func() pipeline.Interface {
		return builder.NewPipelineBuilder(ctx, s, encrypt/*, rLevel*/)
	}
}

func (bl *Beelite) AddFeed(ctx context.Context, batchHex, owner, topic string) (reference swarm.Address, err error) {
	ownerB, err := hex.DecodeString(owner)
	if err != nil {
		bl.Logger.Debug("feed put: decode owner: %v", err)
		return
	}
	topicB, err := hex.DecodeString(topic)
	if err != nil {
		bl.Logger.Debug("feed put: decode topic: %v", err)
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
		pin = false
	)

	if deferred || pin {
		tag, err = bl.getOrCreateSessionID(uint64(0))
		if err != nil {
			bl.Logger.Error(err, "get or create tag failed")
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
		bl.Logger.Error(err, "get putter failed")
		return
	}

	l := loadsave.New(bl.Storer.ChunkStore()/*, bl.Storer.Cache()*/, requestPipelineFactory(ctx, putter, false/*, 0*/))
	feedManifest, err := manifest.NewDefaultManifest(l, false)
	if err != nil {
		bl.Logger.Debug("feed put: new manifest: %v", err)
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
		bl.Logger.Debug("feed post: add manifest entry: %v", err)
		return
	}
	reference, err = feedManifest.Store(ctx)
	if err != nil {
		bl.Logger.Debug("feed post: store manifest: %v", err)
		return
	}
	return
}
