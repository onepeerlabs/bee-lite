package bee

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/ethersphere/bee/pkg/feeds"
	"github.com/ethersphere/bee/pkg/file/loadsave"
	"github.com/ethersphere/bee/pkg/file/pipeline"
	"github.com/ethersphere/bee/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/pkg/manifest"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

func (b *Bee) AddFeed(ctx context.Context, batchHex, owner, topic string) (reference swarm.Address, err error) {
	ownerB, err := hex.DecodeString(owner)
	if err != nil {
		b.logger.Debugf("feed put: decode owner: %v", err)
		return
	}
	topicB, err := hex.DecodeString(topic)
	if err != nil {
		b.logger.Debugf("feed put: decode topic: %v", err)
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
		err = fmt.Errorf("stamp issuer: %w", err)
		return
	}
	stamper := postage.NewStamper(i, b.signer)
	putter := &stamperPutter{Storer: b.ns, stamper: stamper}

	pipeFn := func() pipeline.Interface {
		return builder.NewPipelineBuilder(ctx, putter, storage.ModePutUpload, false)
	}
	l := loadsave.New(putter, pipeFn)
	feedManifest, err := manifest.NewDefaultManifest(l, false)
	if err != nil {
		b.logger.Debugf("feed put: new manifest: %v", err)
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
		b.logger.Debugf("feed post: add manifest entry: %v", err)
		return
	}
	reference, err = feedManifest.Store(ctx)
	if err != nil {
		b.logger.Debugf("feed post: store manifest: %v", err)
		return
	}
	return
}
