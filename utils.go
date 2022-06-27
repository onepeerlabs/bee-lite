package bee

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/pkg/accounting"
	"github.com/ethersphere/bee/pkg/addressbook"
	"github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/feeds"
	"github.com/ethersphere/bee/pkg/feeds/factory"
	"github.com/ethersphere/bee/pkg/file/joiner"
	"github.com/ethersphere/bee/pkg/file/loadsave"
	"github.com/ethersphere/bee/pkg/hive"
	"github.com/ethersphere/bee/pkg/manifest"
	"github.com/ethersphere/bee/pkg/netstore"
	"github.com/ethersphere/bee/pkg/p2p"
	"github.com/ethersphere/bee/pkg/p2p/libp2p"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/postage/postagecontract"
	"github.com/ethersphere/bee/pkg/pricer"
	"github.com/ethersphere/bee/pkg/pricing"
	"github.com/ethersphere/bee/pkg/retrieval"
	"github.com/ethersphere/bee/pkg/settlement/pseudosettle"
	"github.com/ethersphere/bee/pkg/settlement/swap/chequebook"
	"github.com/ethersphere/bee/pkg/shed"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/storage/inmemstore"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/topology"
	"github.com/ethersphere/bee/pkg/topology/kademlia"
	"github.com/ethersphere/bee/pkg/topology/lightnode"
	"github.com/ethersphere/bee/pkg/transaction"
	ma "github.com/multiformats/go-multiaddr"
)

type Event int

const (
	ContextCreated Event = iota
	StateStore
	BatchStoreCheck
	AddressBook
	InitChain
	SyncChain
	SwapEnable
	Identity
	LightNodes
	SenderMatcher
	Bootstrap
	PaymentThresholdCalculation
	BatchState
	BeeLibp2p
	BatchStore
	LocalStore
	PostageService
	EventListener
	BatchService
	PostageContractService
	NATManager
	Hive
	MetricsDB
	KAD
	BatchServiceStart
	Accounting
	Pseudosettle
	InitSwap
	MultipleServices
	LiteNodeProtocols
	MultiResolver
	Ready

	maxDelay                      = 1 * time.Minute
	refreshRate                   = int64(4500000)
	lightRefreshRate              = int64(450000)
	basePrice                     = 10000
	postageSyncingStallingTimeout = 10 * time.Minute
	postageSyncingBackoffTimeout  = 5 * time.Second
	minPaymentThreshold           = 2 * refreshRate
	maxPaymentThreshold           = 24 * refreshRate
	mainnetNetworkID              = uint64(1)

	feedMetadataEntryOwner = "swarm-feed-owner"
	feedMetadataEntryTopic = "swarm-feed-topic"
	feedMetadataEntryType  = "swarm-feed-type"
)

func batchStoreExists(s storage.StateStorer) (bool, error) {

	hasOne := false
	err := s.Iterate("batchstore_", func(key, value []byte) (stop bool, err error) {
		hasOne = true
		return true, err
	})

	return hasOne, err
}

func isChainEnabled(o *Options) bool {
	chainDisabled := !o.ChainEnable

	if chainDisabled { // ultra light mode is LightNode mode with chain disabled
		o.Logger.Info("starting with a disabled chain backend")
		return false
	}

	o.Logger.Info("starting with an enabled chain backend")
	return true // all other modes operate require chain enabled
}

func initChequeStoreCashout(
	stateStore storage.StateStorer,
	swapBackend transaction.Backend,
	chequebookFactory chequebook.Factory,
	chainID int64,
	overlayEthAddress common.Address,
	transactionService transaction.Service,
) (chequebook.ChequeStore, chequebook.CashoutService) {
	chequeStore := chequebook.NewChequeStore(
		stateStore,
		chequebookFactory,
		chainID,
		overlayEthAddress,
		transactionService,
		chequebook.RecoverCheque,
	)

	cashout := chequebook.NewCashoutService(
		stateStore,
		swapBackend,
		transactionService,
		chequeStore,
	)

	return chequeStore, cashout
}

// bootstrap
var snapshotFeed = swarm.MustParseHexAddress("b181b084df07a550c9fc0007110bff67000fa92a090af6c5212fe8e19f888a28")

func bootstrapNode(
	addr string,
	swarmAddress swarm.Address,
	txHash []byte,
	chainID int64,
	overlayEthAddress common.Address,
	addressbook addressbook.Interface,
	bootnodes []ma.Multiaddr,
	lightNodes *lightnode.Container,
	senderMatcher p2p.SenderMatcher,
	chequebookService chequebook.Service,
	chequeStore chequebook.ChequeStore,
	cashoutService chequebook.CashoutService,
	transactionService transaction.Service,
	stateStore storage.StateStorer,
	signer crypto.Signer,
	networkID uint64,
	libp2pPrivateKey *ecdsa.PrivateKey,
	o *Options,
) (snapshot *postage.ChainSnapshot, retErr error) {

	p2pCtx, p2pCancel := context.WithCancel(context.Background())
	defer p2pCancel()

	p2ps, err := libp2p.New(p2pCtx, signer, networkID, swarmAddress, addr, addressbook, stateStore, lightNodes, senderMatcher, o.Logger, nil, libp2p.Options{
		PrivateKey:     libp2pPrivateKey,
		NATAddr:        o.NATAddr,
		WelcomeMessage: o.WelcomeMessage,
		FullNode:       false,
		Transaction:    txHash,
	})
	if err != nil {
		return nil, fmt.Errorf("p2p service: %w", err)
	}

	hive, err := hive.New(p2ps, addressbook, networkID, false, false, o.Logger)
	if err != nil {
		return nil, fmt.Errorf("hive: %w", err)
	}

	if err = p2ps.AddProtocol(hive.Protocol()); err != nil {
		return nil, fmt.Errorf("hive service: %w", err)
	}

	metricsDB, err := shed.NewDBWrap(stateStore.DB())
	if err != nil {
		return nil, fmt.Errorf("unable to create metrics storage for kademlia: %w", err)
	}

	kad, err := kademlia.New(swarmAddress, addressbook, hive, p2ps, &noopPinger{}, metricsDB, o.Logger,
		kademlia.Options{Bootnodes: bootnodes, BootnodeMode: false})
	if err != nil {
		return nil, fmt.Errorf("unable to create kademlia: %w", err)
	}

	hive.SetAddPeersHandler(kad.AddPeers)
	p2ps.SetPickyNotifier(kad)

	paymentThreshold, _ := new(big.Int).SetString(o.PaymentThreshold, 10)

	pricer := pricer.NewFixedPricer(swarmAddress, basePrice)

	pricing := pricing.New(p2ps, o.Logger, paymentThreshold, big.NewInt(minPaymentThreshold))
	if err = p2ps.AddProtocol(pricing.Protocol()); err != nil {
		return nil, fmt.Errorf("pricing service: %w", err)
	}

	acc, err := accounting.NewAccounting(
		paymentThreshold,
		o.PaymentTolerance,
		o.PaymentEarly,
		o.Logger,
		stateStore,
		pricing,
		big.NewInt(refreshRate),
		p2ps,
	)
	if err != nil {
		return nil, fmt.Errorf("accounting: %w", err)
	}

	// bootstraper mode uses the light node refresh rate
	enforcedRefreshRate := big.NewInt(lightRefreshRate)

	pseudosettleService := pseudosettle.New(p2ps, o.Logger, stateStore, acc, enforcedRefreshRate, enforcedRefreshRate, p2ps)
	if err = p2ps.AddProtocol(pseudosettleService.Protocol()); err != nil {
		return nil, fmt.Errorf("pseudosettle service: %w", err)
	}

	acc.SetRefreshFunc(pseudosettleService.Pay)

	pricing.SetPaymentThresholdObserver(acc)

	noopValidStamp := func(chunk swarm.Chunk, _ []byte) (swarm.Chunk, error) {
		return chunk, nil
	}

	storer := inmemstore.New()

	retrieve := retrieval.New(swarmAddress, storer, p2ps, kad, o.Logger, acc, pricer, nil, o.RetrievalCaching, noopValidStamp)
	if err = p2ps.AddProtocol(retrieve.Protocol()); err != nil {
		return nil, fmt.Errorf("retrieval service: %w", err)
	}

	ns := netstore.New(storer, noopValidStamp, retrieve, o.Logger)

	if err := kad.Start(p2pCtx); err != nil {
		return nil, err
	}

	if err := p2ps.Ready(); err != nil {
		return nil, err
	}

	if !waitPeers(kad) {
		return nil, errors.New("timed out waiting for kademlia peers")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	o.Logger.Info("bootstrap: trying to fetch stamps snapshot")

	snapshotReference, err := getLatestSnapshot(ctx, ns, snapshotFeed)
	if err != nil {
		return nil, err
	}

	reader, l, err := joiner.New(ctx, ns, snapshotReference)
	if err != nil {
		return nil, err
	}

	eventsJSON, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	if len(eventsJSON) != int(l) {
		return nil, err
	}

	events := postage.ChainSnapshot{}
	err = json.Unmarshal(eventsJSON, &events)
	if err != nil {
		return nil, err
	}

	return &events, nil
}

// wait till some peers are connected. returns true if all is ok
func waitPeers(kad *kademlia.Kad) bool {
	for i := 0; i < 30; i++ {
		items := 0
		_ = kad.EachPeer(func(_ swarm.Address, _ uint8) (bool, bool, error) {
			items++
			return false, false, nil
		}, topology.Filter{})
		if items >= 5 {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

type noopPinger struct{}

func (p *noopPinger) Ping(context.Context, swarm.Address, ...string) (time.Duration, error) {
	return time.Duration(1), nil
}

func getLatestSnapshot(
	ctx context.Context,
	st storage.Storer,
	address swarm.Address,
) (swarm.Address, error) {
	ls := loadsave.NewReadonly(st)
	feedFactory := factory.New(st)

	m, err := manifest.NewDefaultManifestReference(
		address,
		ls,
	)
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("not a manifest: %w", err)
	}

	e, err := m.Lookup(ctx, "/")
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("node lookup: %w", err)
	}

	var (
		owner, topic []byte
		t            = new(feeds.Type)
	)
	meta := e.Metadata()
	if e := meta["swarm-feed-owner"]; e != "" {
		owner, err = hex.DecodeString(e)
		if err != nil {
			return swarm.ZeroAddress, err
		}
	}
	if e := meta["swarm-feed-topic"]; e != "" {
		topic, err = hex.DecodeString(e)
		if err != nil {
			return swarm.ZeroAddress, err
		}
	}
	if e := meta["swarm-feed-type"]; e != "" {
		err := t.FromString(e)
		if err != nil {
			return swarm.ZeroAddress, err
		}
	}
	if len(owner) == 0 || len(topic) == 0 {
		return swarm.ZeroAddress, fmt.Errorf("node lookup: %s", "feed metadata absent")
	}
	f := feeds.New(topic, common.BytesToAddress(owner))

	l, err := feedFactory.NewLookup(*t, f)
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("feed lookup failed: %w", err)
	}

	u, _, _, err := l.At(ctx, time.Now().Unix(), 0)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	_, ref, err := feeds.FromChunk(u)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	return swarm.NewAddress(ref), nil
}

// noOpChequebookService is a noOp implementation for chequebook.Service interface.
type noOpChequebookService struct{}

func (m *noOpChequebookService) Deposit(context.Context, *big.Int) (hash common.Hash, err error) {
	return hash, postagecontract.ErrChainDisabled
}
func (m *noOpChequebookService) Withdraw(context.Context, *big.Int) (hash common.Hash, err error) {
	return hash, postagecontract.ErrChainDisabled
}
func (m *noOpChequebookService) WaitForDeposit(context.Context, common.Hash) error {
	return postagecontract.ErrChainDisabled
}
func (m *noOpChequebookService) Balance(context.Context) (*big.Int, error) {
	return nil, postagecontract.ErrChainDisabled
}
func (m *noOpChequebookService) AvailableBalance(context.Context) (*big.Int, error) {
	return nil, postagecontract.ErrChainDisabled
}
func (m *noOpChequebookService) Address() common.Address {
	return common.Address{}
}
func (m *noOpChequebookService) Issue(context.Context, common.Address, *big.Int, chequebook.SendChequeFunc) (*big.Int, error) {
	return nil, postagecontract.ErrChainDisabled
}
func (m *noOpChequebookService) LastCheque(common.Address) (*chequebook.SignedCheque, error) {
	return nil, postagecontract.ErrChainDisabled
}
func (m *noOpChequebookService) LastCheques() (map[common.Address]*chequebook.SignedCheque, error) {
	return nil, postagecontract.ErrChainDisabled
}

type stamperPutter struct {
	storage.Storer
	stamper postage.Stamper
}

func (p *stamperPutter) Put(ctx context.Context, mode storage.ModePut, chs ...swarm.Chunk) (exists []bool, err error) {
	_, file, no, ok := runtime.Caller(1)
	if ok {
		fmt.Printf("stamperPutter called from %s#%d\n", file, no)
	}
	var (
		ctp []swarm.Chunk
		idx []int
	)
	exists = make([]bool, len(chs))

	for i, c := range chs {
		has, err := p.Storer.Has(ctx, c.Address())
		if err != nil {
			return nil, err
		}
		if has || containsChunk(c.Address(), chs[:i]...) {
			exists[i] = true
			continue
		}
		stamp, err := p.stamper.Stamp(c.Address())
		if err != nil {
			return nil, err
		}
		chs[i] = c.WithStamp(stamp)
		ctp = append(ctp, chs[i])
		idx = append(idx, i)
	}
	exists2, err := p.Storer.Put(ctx, mode, ctp...)
	if err != nil {
		return nil, err
	}
	for i, v := range idx {
		exists[v] = exists2[i]
	}
	return exists, nil
}

// containsChunk returns true if the chunk with a specific address
// is present in the provided chunk slice.
func containsChunk(addr swarm.Address, chs ...swarm.Chunk) bool {
	for _, c := range chs {
		if addr.Equal(c.Address()) {
			return true
		}
	}
	return false
}
