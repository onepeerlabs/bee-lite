package beelite

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/pkg/postage"
)

func (bl *Beelite) GetAllBatches() []*postage.StampIssuer {
	return bl.post.StampIssuers()
}

func (bl *Beelite) GetUsableBatches() []*postage.StampIssuer {
	usableIssuers := []*postage.StampIssuer{}
	stampIssuers := bl.post.StampIssuers()
	for _, v := range stampIssuers {
		exists, err := bl.batchStore.Exists(v.ID())
		if err != nil {
			bl.logger.Debug("batch exists: %w", err)
			continue
		}

		issuer, _, err := bl.post.GetStampIssuer(v.ID())
		if err != nil {
			bl.logger.Debug("stamp issuer: %w", err)
			continue
		}

		if usable := exists && bl.post.IssuerUsable(issuer); usable {
			usableIssuers = append(usableIssuers, v)
		}
	}
	return usableIssuers
}

func (bl *Beelite) BuyStamp(amount *big.Int, depth uint64, label string, immutable bool) (common.Hash, []byte, error) {
	return bl.postageContract.CreateBatch(bl.ctx, amount, uint8(depth), immutable, label)
}
