package beelite

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/pkg/postage"
)

func (bl *Beelite) GetAllBatches() []*postage.StampIssuer {
	return bl.Post.StampIssuers()
}

func (bl *Beelite) GetUsableBatches() []*postage.StampIssuer {
	panic("TODO implement method to send all the useble batches")
}

func (bl *Beelite) BuyStamp(amount *big.Int, depth uint64, label string, immutable bool) (common.Hash, []byte, error) {
	return bl.PostageContract.CreateBatch(bl.Ctx, amount, uint8(depth), immutable, label)
}
