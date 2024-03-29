package bee

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/pkg/accounting"
	"github.com/ethersphere/bee/pkg/addressbook"
	"github.com/ethersphere/bee/pkg/chainsync"
	"github.com/ethersphere/bee/pkg/chainsyncer"
	"github.com/ethersphere/bee/pkg/config"
	"github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/feeds"
	"github.com/ethersphere/bee/pkg/feeds/factory"
	"github.com/ethersphere/bee/pkg/hive"
	filekeystore "github.com/ethersphere/bee/pkg/keystore/file"
	"github.com/ethersphere/bee/pkg/localstore"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/netstore"
	"github.com/ethersphere/bee/pkg/node"
	"github.com/ethersphere/bee/pkg/p2p"
	"github.com/ethersphere/bee/pkg/p2p/libp2p"
	"github.com/ethersphere/bee/pkg/pingpong"
	"github.com/ethersphere/bee/pkg/pinning"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/postage/batchservice"
	"github.com/ethersphere/bee/pkg/postage/batchstore"
	"github.com/ethersphere/bee/pkg/postage/listener"
	"github.com/ethersphere/bee/pkg/postage/postagecontract"
	"github.com/ethersphere/bee/pkg/pricer"
	"github.com/ethersphere/bee/pkg/pricing"
	"github.com/ethersphere/bee/pkg/pss"
	"github.com/ethersphere/bee/pkg/puller"
	"github.com/ethersphere/bee/pkg/pullsync"
	"github.com/ethersphere/bee/pkg/pullsync/pullstorage"
	"github.com/ethersphere/bee/pkg/pusher"
	"github.com/ethersphere/bee/pkg/pushsync"
	"github.com/ethersphere/bee/pkg/resolver/multiresolver"
	"github.com/ethersphere/bee/pkg/retrieval"
	"github.com/ethersphere/bee/pkg/settlement/pseudosettle"
	"github.com/ethersphere/bee/pkg/settlement/swap/chequebook"
	"github.com/ethersphere/bee/pkg/settlement/swap/erc20"
	"github.com/ethersphere/bee/pkg/shed"
	"github.com/ethersphere/bee/pkg/steward"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/tags"
	"github.com/ethersphere/bee/pkg/topology"
	"github.com/ethersphere/bee/pkg/topology/kademlia"
	"github.com/ethersphere/bee/pkg/topology/lightnode"
	"github.com/ethersphere/bee/pkg/transaction"
	"github.com/ethersphere/bee/pkg/traversal"
	ma "github.com/multiformats/go-multiaddr"
	"golang.org/x/crypto/sha3"
)

type Options struct {
	FullNodeMode               bool
	Keystore                   string
	DataDir                    string
	Addr                       string
	NATAddr                    string
	WelcomeMessage             string
	Bootnodes                  []string
	Logger                     logging.Logger
	SwapEndpoint               string
	SwapFactoryAddress         string
	SwapLegacyFactoryAddresses []string
	SwapInitialDeposit         string
	SwapEnable                 bool
	WarmupTime                 time.Duration
	ChainID                    int64
	ChequebookEnable           bool
	ChainEnable                bool
	DeployGasPrice             string
	BlockTime                  uint64
	Transaction                string
	BlockHash                  string
	PostageContractAddress     string
	PriceOracleAddress         string
	PaymentThreshold           string
	PaymentTolerance           int64
	PaymentEarly               int64
	UsePostageSnapshot         bool
	Mainnet                    bool
	NetworkID                  uint64
	Resync                     bool
	CacheCapacity              uint64
	DBOpenFilesLimit           uint64
	DBWriteBufferSize          uint64
	DBDisableSeeksCompaction   bool
	DBBlockCacheCapacity       uint64
	RetrievalCaching           bool
}

// Bee client.
type Bee struct {
	ctx               context.Context
	cancel            context.CancelFunc
	post              postage.Service
	logger            logging.Logger
	tagService        *tags.Tags
	signer            crypto.Signer
	ns                storage.Storer
	overlayEthAddress common.Address
	topologyDriver    topology.Driver
	chequebook        chequebook.Service
	postageContract   postagecontract.Interface
	steward           steward.Interface
	feedFactory       feeds.Factory

	p2pHalter       p2p.Halter
	topologyHalter  topology.Halter
	ethClientCloser func()
	closers         []io.Closer
	syncingStopped  chan struct{}
}

func Start(o *Options, password string) (*Bee, error) {
	var (
		errMain error
		b       *Bee
	)
	defer func() {
		if errMain != nil && b != nil {
			err := b.Shutdown()
			if err != nil {
				o.Logger.Error("shutdown failed")
			}
		}
	}()
	if o.Keystore == "" {
		o.Keystore = o.DataDir
	}
	logger := o.Logger
	keystore := filekeystore.New(filepath.Join(o.Keystore, "keys"))
	swarmPrivateKey, _, err := keystore.Key("swarm", password)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("swarm key: %w", err))
		return nil, fmt.Errorf("swarm key: %w", err)
	}
	signer := crypto.NewDefaultSigner(swarmPrivateKey)
	libp2pPrivateKey, _, err := keystore.Key("libp2p", password)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("libp2p key: %w", err))
		return nil, fmt.Errorf("libp2p key: %w", err)
	}
	p2pCtx, p2pCancel := context.WithCancel(context.Background())

	b = &Bee{
		logger:         o.Logger,
		ctx:            p2pCtx,
		cancel:         p2pCancel,
		signer:         signer,
		syncingStopped: make(chan struct{}),
	}

	stateStore, err := node.InitStateStore(logger, o.DataDir)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("init statestore: %w", err))
		return nil, fmt.Errorf("init statestore: %w", err)
	}
	b.closers = append(b.closers, stateStore)

	batchStoreExists, err := batchStoreExists(stateStore)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("batchStoreExists: %w", err))
		return nil, fmt.Errorf("batchStoreExists: %w", err)
	}

	addressbook := addressbook.New(stateStore)
	var (
		chainBackend       transaction.Backend
		overlayEthAddress  common.Address
		chainID            int64
		transactionService transaction.Service
		transactionMonitor transaction.Monitor
		chequebookFactory  chequebook.Factory
		chequebookService  chequebook.Service = new(noOpChequebookService)
		chequeStore        chequebook.ChequeStore
		cashoutService     chequebook.CashoutService
		pollingInterval    = time.Duration(o.BlockTime) * time.Second
		erc20Service       erc20.Service
	)

	chainEnabled := isChainEnabled(o)
	chainBackend, overlayEthAddress, chainID, transactionMonitor, transactionService, err = node.InitChain(
		p2pCtx,
		logger,
		stateStore,
		o.SwapEndpoint,
		o.ChainID,
		signer,
		pollingInterval,
		chainEnabled)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("init chain: %w", err))
		return nil, fmt.Errorf("init chain: %w", err)
	}
	b.overlayEthAddress = overlayEthAddress
	b.ethClientCloser = chainBackend.Close

	logger.Info("overlay address : ", overlayEthAddress)
	if o.ChainID != -1 && o.ChainID != chainID {
		logger.Error(fmt.Errorf("connected to wrong ethereum network; network chainID %d; configured chainID %d", chainID, o.ChainID))
		return nil, fmt.Errorf("connected to wrong ethereum network; network chainID %d; configured chainID %d", chainID, o.ChainID)
	}
	b.closers = append(b.closers, transactionService)
	b.closers = append(b.closers, transactionMonitor)

	isSynced, _, err := transaction.IsSynced(p2pCtx, chainBackend, maxDelay)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("is synced: %w", err))
		return nil, fmt.Errorf("is synced: %w", err)
	}
	if !isSynced {
		logger.Infof("waiting to sync with the Ethereum backend")

		err := transaction.WaitSynced(p2pCtx, logger, chainBackend, maxDelay)
		if err != nil {
			errMain = err
			log.Fatal(fmt.Errorf("waiting backend sync: %w", err))
		}
	}

	if o.SwapEnable {
		chequebookFactory, err = node.InitChequebookFactory(
			logger,
			chainBackend,
			chainID,
			transactionService,
			o.SwapFactoryAddress,
			o.SwapLegacyFactoryAddresses,
		)
		if err != nil {
			errMain = err
			logger.Error(fmt.Errorf("init chequebook factory: %w", err))
			return nil, fmt.Errorf("init chequebook factory: %w", err)
		}

		if err = chequebookFactory.VerifyBytecode(p2pCtx); err != nil {
			logger.Error(fmt.Errorf("factory fail: %w", err))
			return nil, fmt.Errorf("factory fail: %w", err)
		}

		erc20Address, err := chequebookFactory.ERC20Address(p2pCtx)
		if err != nil {
			errMain = err
			logger.Error(fmt.Errorf("factory fail: %w", err))
			return nil, fmt.Errorf("factory fail: %w", err)
		}
		erc20Service = erc20.New(transactionService, erc20Address)
		if o.ChequebookEnable && chainEnabled {
			var txHash common.Hash
			err = stateStore.Get(chequebook.ChequebookDeploymentKey, &txHash)
			if err != nil && err != storage.ErrNotFound {
				logger.Error(fmt.Errorf("chequebook deployment key fail: %w", err))
				return nil, fmt.Errorf("chequebook deployment key: %w", err)
			}
			if err == storage.ErrNotFound {
				deposit, ok := new(big.Int).SetString(o.SwapInitialDeposit, 10)
				if !ok {
					logger.Error(fmt.Errorf("initial swap deposit \"%s\" cannot be parsed", o.SwapInitialDeposit))
					return nil, fmt.Errorf("initial swap deposit \"%s\" cannot be parsed", o.SwapInitialDeposit)
				}
				err = checkBalance(p2pCtx, logger, deposit, chainBackend, overlayEthAddress, erc20Service)
				if err != nil {
					errMain = err
					logger.Error(fmt.Errorf("check balance: %w", err))
					return nil, fmt.Errorf("check balance: %w", err)
				}
			}
			chequebookService, err = node.InitChequebookService(
				p2pCtx,
				logger,
				stateStore,
				signer,
				chainID,
				chainBackend,
				overlayEthAddress,
				transactionService,
				chequebookFactory,
				o.SwapInitialDeposit,
				o.DeployGasPrice,
				erc20Service,
			)
			if err != nil {
				errMain = err
				logger.Error(fmt.Errorf("init chequebook service failed: %w", err))
				return nil, fmt.Errorf("init chequebook service failed: %w", err)
			}
			b.chequebook = chequebookService
		}

		chequeStore, cashoutService = initChequeStoreCashout(
			stateStore,
			chainBackend,
			chequebookFactory,
			chainID,
			overlayEthAddress,
			transactionService,
		)
	}

	pubKey, _ := signer.PublicKey()
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("publickey: %w", err))
		return nil, fmt.Errorf("publickey: %w", err)
	}

	var (
		blockHash []byte
		txHash    []byte
	)

	txHash, err = node.GetTxHash(stateStore, logger, o.Transaction)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("invalid transaction hash: %w", err))
		return nil, fmt.Errorf("invalid transaction hash: %w", err)
	}

	blockHash, err = node.GetTxNextBlock(p2pCtx, logger, chainBackend, transactionMonitor, pollingInterval, txHash, o.BlockHash)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("invalid block hash: %w", err))
		return nil, fmt.Errorf("invalid block hash: %w", err)
	}

	swarmAddress, err := crypto.NewOverlayAddress(*pubKey, o.NetworkID, blockHash)
	if err != nil {
		return nil, fmt.Errorf("compute overlay address: %w", err)
	}
	logger.Infof("using overlay address %s", swarmAddress)

	err = node.CheckOverlayWithStore(swarmAddress, stateStore)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("check overlay: %w", err))
		return nil, fmt.Errorf("check overlay: %w", err)
	}

	lightNodes := lightnode.NewContainer(swarmAddress)

	senderMatcher := transaction.NewMatcher(chainBackend, types.NewLondonSigner(big.NewInt(chainID)), stateStore, chainEnabled)
	_, err = senderMatcher.Matches(p2pCtx, txHash, o.NetworkID, swarmAddress, true)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("identity transaction verification failed: %w", err))
		return nil, fmt.Errorf("identity transaction verification failed: %w", err)
	}

	var bootnodes []ma.Multiaddr

	for _, a := range o.Bootnodes {
		addr, err := ma.NewMultiaddr(a)
		if err != nil {
			errMain = err
			logger.Debugf("multiaddress fail %s: %v", a, err)
			logger.Warningf("invalid bootnode address %s", a)
			continue
		}

		bootnodes = append(bootnodes, addr)
	}

	paymentThreshold, ok := new(big.Int).SetString(o.PaymentThreshold, 10)
	if !ok {
		logger.Error(fmt.Errorf("invalid payment threshold: %s", paymentThreshold))
		return nil, fmt.Errorf("invalid payment threshold: %s", paymentThreshold)
	}

	if paymentThreshold.Cmp(big.NewInt(minPaymentThreshold)) < 0 {
		logger.Error(fmt.Errorf("payment threshold below minimum generally accepted value, need at least %d", minPaymentThreshold))
		return nil, fmt.Errorf("payment threshold below minimum generally accepted value, need at least %d", minPaymentThreshold)
	}

	if paymentThreshold.Cmp(big.NewInt(maxPaymentThreshold)) > 0 {
		logger.Error(fmt.Errorf("payment threshold above maximum generally accepted value, needs to be reduced to at most %d", maxPaymentThreshold))
		return nil, fmt.Errorf("payment threshold above maximum generally accepted value, needs to be reduced to at most %d", maxPaymentThreshold)
	}

	if o.PaymentTolerance < 0 {
		logger.Error(fmt.Errorf("invalid payment tolerance: %d", o.PaymentTolerance))
		return nil, fmt.Errorf("invalid payment tolerance: %d", o.PaymentTolerance)
	}

	if o.PaymentEarly > 100 || o.PaymentEarly < 0 {
		logger.Error(fmt.Errorf("invalid payment early: %d", o.PaymentEarly))
		return nil, fmt.Errorf("invalid payment early: %d", o.PaymentEarly)
	}

	var initBatchState *postage.ChainSnapshot
	// Bootstrap node with postage snapshot only if it is running on mainnet, is a fresh
	// install or explicitly asked by user to resync
	if o.NetworkID == mainnetNetworkID && o.UsePostageSnapshot && (!batchStoreExists || o.Resync) {
		start := time.Now()
		logger.Info("cold postage start detected. fetching postage stamp snapshot from swarm")
		initBatchState, err = bootstrapNode(
			o.Addr,
			swarmAddress,
			txHash,
			chainID,
			overlayEthAddress,
			addressbook,
			bootnodes,
			lightNodes,
			senderMatcher,
			chequebookService,
			chequeStore,
			cashoutService,
			transactionService,
			stateStore,
			signer,
			o.NetworkID,
			libp2pPrivateKey,
			o,
		)
		logger.Infof("bootstrapper took %s", time.Since(start))
		if err != nil {
			errMain = err
			logger.Error(fmt.Errorf("bootstrapper failed to fetch batch state: %v", err))
			return nil, fmt.Errorf("bootstrapper failed to fetch batch state: %v", err)
		}
	}

	p2ps, err := libp2p.New(p2pCtx, signer, o.NetworkID, swarmAddress, o.Addr, addressbook, stateStore, lightNodes, senderMatcher, logger, nil, libp2p.Options{
		PrivateKey:      libp2pPrivateKey,
		NATAddr:         o.NATAddr,
		WelcomeMessage:  o.WelcomeMessage,
		Transaction:     txHash,
		ValidateOverlay: chainEnabled,
		FullNode:        o.FullNodeMode,
	})

	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("p2p service: %w", err))
		return nil, fmt.Errorf("p2p service: %w", err)
	}
	b.closers = append(b.closers, p2ps)
	b.p2pHalter = p2ps

	var unreserveFn func([]byte, uint8) (uint64, error)
	var evictFn = func(b []byte) error {
		_, err := unreserveFn(b, swarm.MaxPO+1)
		return err
	}

	batchStore, err := batchstore.New(stateStore, evictFn, logger)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("batchstore: %s", err))
		return nil, fmt.Errorf("batchstore: %s", err)
	}
	var path string

	if o.DataDir != "" {
		logger.Infof("using datadir in: '%s'", o.DataDir)
		path = filepath.Join(o.DataDir, "localstore")
	}
	lo := &localstore.Options{
		Capacity:               o.CacheCapacity,
		ReserveCapacity:        uint64(batchstore.Capacity),
		UnreserveFunc:          batchStore.Unreserve,
		OpenFilesLimit:         o.DBOpenFilesLimit,
		BlockCacheCapacity:     o.DBBlockCacheCapacity,
		WriteBufferSize:        o.DBWriteBufferSize,
		DisableSeeksCompaction: o.DBDisableSeeksCompaction,
	}

	storer, err := localstore.New(path, swarmAddress.Bytes(), stateStore, lo, logger)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("localstore: %w", err))
		return nil, fmt.Errorf("localstore: %w", err)
	}
	b.closers = append(b.closers, storer)
	unreserveFn = storer.UnreserveBatch

	validStamp := postage.ValidStamp(batchStore)
	b.post, err = postage.NewService(stateStore, batchStore, chainID)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("postage service load: %w", err))
		return nil, fmt.Errorf("postage service load: %w", err)
	}
	b.closers = append(b.closers, b.post)

	var (
		batchSvc      postage.EventUpdater
		eventListener postage.Listener
	)

	var postageSyncStart uint64 = 0

	chainCfg, found := config.GetChainConfig(chainID)
	postageContractAddress, startBlock := chainCfg.PostageStamp, chainCfg.StartBlock
	if o.PostageContractAddress != "" {
		if !common.IsHexAddress(o.PostageContractAddress) {
			logger.Error(fmt.Errorf("malformed postage stamp address"))
			return nil, fmt.Errorf("malformed postage stamp address")
		}
		postageContractAddress = common.HexToAddress(o.PostageContractAddress)
	} else if !found {
		logger.Error(fmt.Errorf("no known postage stamp addresses for this network"))
		return nil, fmt.Errorf("no known postage stamp addresses for this network")
	}
	if found {
		postageSyncStart = startBlock
	}

	eventListener = listener.New(b.syncingStopped, logger, chainBackend, postageContractAddress, o.BlockTime, postageSyncingStallingTimeout, postageSyncingBackoffTimeout)
	b.closers = append(b.closers, eventListener)

	batchSvc, err = batchservice.New(stateStore, batchStore, logger, eventListener, overlayEthAddress.Bytes(), b.post, sha3.New256, o.Resync)
	if err != nil {
		errMain = err
		logger.Error(err)
		return nil, err
	}

	erc20Address, err := postagecontract.LookupERC20Address(p2pCtx, transactionService, postageContractAddress, chainEnabled)
	if err != nil {
		errMain = err
		logger.Error(err)
		return nil, err
	}

	postageContractService := postagecontract.New(
		overlayEthAddress,
		postageContractAddress,
		erc20Address,
		transactionService,
		b.post,
		batchStore,
		chainEnabled,
	)
	b.postageContract = postageContractService

	if natManager := p2ps.NATManager(); natManager != nil {
		// wait for nat manager to init
		logger.Debug("initializing NAT manager")
		select {
		case <-natManager.Ready():
			// this is magic sleep to give NAT time to sync the mappings
			// this is a hack, kind of alchemy and should be improved
			time.Sleep(3 * time.Second)
			logger.Debug("NAT manager initialized")
		case <-time.After(10 * time.Second):
			logger.Warning("NAT manager init timeout")
		}
	}

	pingPong := pingpong.New(p2ps, logger, nil)

	if err = p2ps.AddProtocol(pingPong.Protocol()); err != nil {
		logger.Error(fmt.Errorf("pingpong service: %w", err))
		return nil, fmt.Errorf("pingpong service: %w", err)
	}

	hive, err := hive.New(p2ps, addressbook, o.NetworkID, false, false, logger)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("hive: %w", err))
		return nil, fmt.Errorf("hive: %w", err)
	}

	if err = p2ps.AddProtocol(hive.Protocol()); err != nil {
		logger.Error(fmt.Errorf("hive service: %w", err))
		return nil, fmt.Errorf("hive service: %w", err)
	}
	b.closers = append(b.closers, hive)

	metricsDB, err := shed.NewDBWrap(stateStore.DB())
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("unable to create metrics storage for kademlia: %w", err))
		return nil, fmt.Errorf("unable to create metrics storage for kademlia: %w", err)
	}

	kad, err := kademlia.New(swarmAddress, addressbook, hive, p2ps, pingPong, metricsDB, logger,
		kademlia.Options{Bootnodes: bootnodes, BootnodeMode: false, IgnoreRadius: !o.ChainEnable})
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("unable to create kademlia: %w", err))
		return nil, fmt.Errorf("unable to create kademlia: %w", err)
	}
	b.topologyHalter = kad
	b.topologyDriver = kad
	b.closers = append(b.closers, kad)

	hive.SetAddPeersHandler(kad.AddPeers)
	p2ps.SetPickyNotifier(kad)
	batchStore.SetRadiusSetter(kad)

	interruptChannel := make(chan os.Signal, 1)
	signal.Notify(interruptChannel, syscall.SIGINT, syscall.SIGTERM)
	if batchSvc != nil && chainEnabled {
		syncedChan, err := batchSvc.Start(postageSyncStart, initBatchState)
		if err != nil {
			errMain = err
			logger.Error(fmt.Errorf("unable to start batch service: %w", err))
			return nil, fmt.Errorf("unable to start batch service: %w", err)
		}
		// wait for the postage contract listener to sync
		logger.Info("waiting to sync postage contract data, this may take a while... more info available in Debug loglevel")

		// arguably this is not a very nice solution since we dont support
		// interrupts at this stage of the application lifecycle. some changes
		// would be needed on the cmd level to support context cancellation at
		// this stage
		select {
		case <-syncedChan:
		case <-interruptChannel:
			logger.Error(fmt.Errorf("done"))
			return nil, fmt.Errorf("done")
		}
	}

	pricer := pricer.NewFixedPricer(swarmAddress, basePrice)

	pricing := pricing.New(p2ps, logger, paymentThreshold, big.NewInt(minPaymentThreshold))

	if err = p2ps.AddProtocol(pricing.Protocol()); err != nil {
		logger.Error(fmt.Errorf("pricing service: %w", err))
		return nil, fmt.Errorf("pricing service: %w", err)
	}

	addrs, err := p2ps.Addresses()
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("get server addresses: %w", err))
		return nil, fmt.Errorf("get server addresses: %w", err)
	}

	for _, addr := range addrs {
		logger.Debugf("p2p address: %s", addr)
	}

	acc, err := accounting.NewAccounting(
		paymentThreshold,
		o.PaymentTolerance,
		o.PaymentEarly,
		logger,
		stateStore,
		pricing,
		big.NewInt(refreshRate),
		p2ps,
	)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("accounting: %w", err))
		return nil, fmt.Errorf("accounting: %w", err)
	}
	b.closers = append(b.closers, acc)

	var enforcedRefreshRate *big.Int
	if o.FullNodeMode {
		enforcedRefreshRate = big.NewInt(refreshRate)
	} else {
		enforcedRefreshRate = big.NewInt(lightRefreshRate)
	}
	pseudosettleService := pseudosettle.New(p2ps, logger, stateStore, acc, enforcedRefreshRate, big.NewInt(lightRefreshRate), p2ps)
	if err = p2ps.AddProtocol(pseudosettleService.Protocol()); err != nil {
		logger.Error(fmt.Errorf("pseudosettle service: %w", err))
		return nil, fmt.Errorf("pseudosettle service: %w", err)
	}
	acc.SetRefreshFunc(pseudosettleService.Pay)

	if o.SwapEnable && chainEnabled {
		swapService, priceOracle, err := node.InitSwap(
			p2ps,
			logger,
			stateStore,
			o.NetworkID,
			overlayEthAddress,
			chequebookService,
			chequeStore,
			cashoutService,
			acc,
			o.PriceOracleAddress,
			chainID,
			transactionService,
		)
		if err != nil {
			errMain = err
			logger.Error(err)
			return nil, err
		}
		b.closers = append(b.closers, priceOracle)

		if o.ChequebookEnable {
			acc.SetPayFunc(swapService.Pay)
		}
	}
	pricing.SetPaymentThresholdObserver(acc)

	retrieve := retrieval.New(swarmAddress, storer, p2ps, kad, logger, acc, pricer, nil, o.RetrievalCaching, validStamp)
	b.tagService = tags.NewTags(stateStore, logger)
	b.closers = append(b.closers, b.tagService)

	pssPrivateKey, _, err := keystore.Key("pss", password)
	if err != nil {
		errMain = err
		logger.Error(fmt.Errorf("pss key: %w", err))
		return nil, fmt.Errorf("pss key: %w", err)
	}
	pssService := pss.New(pssPrivateKey, logger)
	b.closers = append(b.closers, pssService)

	b.ns = netstore.New(storer, validStamp, retrieve, logger)
	b.closers = append(b.closers, b.ns)

	traversalService := traversal.New(b.ns)

	pinningService := pinning.NewService(storer, stateStore, traversalService)
	_ = pinningService
	pushSyncProtocol := pushsync.New(swarmAddress, blockHash, p2ps, storer, kad, b.tagService, o.FullNodeMode, pssService.TryUnwrap, validStamp, logger, acc, pricer, signer, nil, o.WarmupTime)

	// set the pushSyncer in the PSS
	pssService.SetPushSyncer(pushSyncProtocol)

	pusherService := pusher.New(o.NetworkID, storer, kad, pushSyncProtocol, validStamp, b.tagService, logger, nil, o.WarmupTime)
	b.closers = append(b.closers, pusherService)

	pullStorage := pullstorage.New(storer)

	pullSyncProtocol := pullsync.New(p2ps, pullStorage, pssService.TryUnwrap, validStamp, logger)
	b.closers = append(b.closers, pullSyncProtocol)
	var pullerService *puller.Puller
	if o.FullNodeMode {
		pullerService = puller.New(stateStore, kad, pullSyncProtocol, logger, puller.Options{}, o.WarmupTime)
		b.closers = append(b.closers, pullerService)
	}

	retrieveProtocolSpec := retrieve.Protocol()
	pushSyncProtocolSpec := pushSyncProtocol.Protocol()
	pullSyncProtocolSpec := pullSyncProtocol.Protocol()

	if o.FullNodeMode {
		logger.Info("starting in full mode")
	} else {
		logger.Info("starting in light mode")
		p2p.WithBlocklistStreams(p2p.DefaultBlocklistTime, retrieveProtocolSpec)
		p2p.WithBlocklistStreams(p2p.DefaultBlocklistTime, pushSyncProtocolSpec)
		p2p.WithBlocklistStreams(p2p.DefaultBlocklistTime, pullSyncProtocolSpec)
	}

	if err = p2ps.AddProtocol(retrieveProtocolSpec); err != nil {
		logger.Error(fmt.Errorf("retrieval service: %w", err))
		return nil, fmt.Errorf("retrieval service: %w", err)
	}
	if err = p2ps.AddProtocol(pushSyncProtocolSpec); err != nil {
		logger.Error(fmt.Errorf("pushsync service: %w", err))
		return nil, fmt.Errorf("pushsync service: %w", err)
	}
	if err = p2ps.AddProtocol(pullSyncProtocolSpec); err != nil {
		logger.Error(fmt.Errorf("pullsync protocol: %w", err))
		return nil, fmt.Errorf("pullsync protocol: %w", err)
	}
	multiResolver := multiresolver.NewMultiResolver(
		multiresolver.WithLogger(o.Logger),
	)
	b.closers = append(b.closers, multiResolver)
	var chainSyncer *chainsyncer.ChainSyncer

	if o.FullNodeMode {
		cs, err := chainsync.New(p2ps, chainBackend)
		if err != nil {
			errMain = err
			logger.Error(fmt.Errorf("new chainsync: %w", err))
			return nil, fmt.Errorf("new chainsync: %w", err)
		}
		if err = p2ps.AddProtocol(cs.Protocol()); err != nil {
			logger.Error(fmt.Errorf("chainsync protocol: %w", err))
			return nil, fmt.Errorf("chainsync protocol: %w", err)
		}
		chainSyncer, err = chainsyncer.New(chainBackend, cs, kad, p2ps, logger, nil)
		if err != nil {
			errMain = err
			logger.Error(fmt.Errorf("new chainsyncer: %w", err))
			return nil, fmt.Errorf(": %w", err)
		}

		b.closers = append(b.closers, chainSyncer)
	}

	b.feedFactory = factory.New(b.ns)
	b.steward = steward.New(storer, traversalService, retrieve, pushSyncProtocol)

	if err := kad.Start(p2pCtx); err != nil {
		logger.Error(err)
		return nil, err
	}

	if err := p2ps.Ready(); err != nil {
		logger.Error(err)
		return nil, err
	}
	return b, nil
}

func (b *Bee) Shutdown() error {
	// halt kademlia while shutting down other
	// components.
	if b.topologyHalter != nil {
		b.topologyHalter.Halt()
	}

	// halt p2p layer from accepting new connections
	// while shutting down other components
	if b.p2pHalter != nil {
		b.p2pHalter.Halt()
	}

	for _, v := range b.closers {
		v.Close()
	}
	if c := b.ethClientCloser; c != nil {
		c()
	}
	b.cancel()
	return nil
}

func (b *Bee) Addr() common.Address {
	return b.overlayEthAddress
}

func (b *Bee) ChequebookAddr() common.Address {
	if b.chequebook != nil {
		return b.chequebook.Address()
	}
	return common.HexToAddress(swarm.ZeroAddress.String())
}

func (b *Bee) ChequebookBalance() (*big.Int, error) {
	if b.chequebook != nil {
		return b.chequebook.Balance(b.ctx)
	}
	return nil, fmt.Errorf("chequebook not initialised")
}

func (b *Bee) ChequebookWithdraw(amount *big.Int) (common.Hash, error) {
	if b.chequebook != nil {
		return b.chequebook.Withdraw(b.ctx, amount)
	}
	return common.HexToHash(""), fmt.Errorf("chequebook not initialised")
}

func (b *Bee) Signer() crypto.Signer {
	return b.signer
}

func (b *Bee) Topology() *topology.KadParams {
	return b.topologyDriver.Snapshot()
}

func (b *Bee) SyncingStopped() chan struct{} {
	return b.syncingStopped
}
