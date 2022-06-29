package bee

import (
	"github.com/ethersphere/bee/pkg/postage"
	"math/big"
)

func (b *Bee) GetAllBatches() []*postage.StampIssuer {
	return b.post.StampIssuers()
}

func (b *Bee) GetUsableBatches() []*postage.StampIssuer {
	panic("TODO implement method to send all the useble batches")
}

func (b *Bee) BuyStamp(amount *big.Int, depth uint64, label string, immutable bool) ([]byte, error) {
	return b.postageContract.CreateBatch(b.ctx, amount, uint8(depth), immutable, label)
}
