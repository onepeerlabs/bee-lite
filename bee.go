package bee

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ethersphere/bee/pkg/cac"
	"github.com/ethersphere/bee/pkg/feeds"
	"github.com/ethersphere/bee/pkg/file/joiner"
	"github.com/ethersphere/bee/pkg/file/loadsave"
	"github.com/ethersphere/bee/pkg/file/pipeline"
	"github.com/ethersphere/bee/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/pkg/manifest"
	"github.com/ethersphere/bee/pkg/resolver/multiresolver"
	"github.com/ethersphere/bee/pkg/sctx"
	"github.com/ethersphere/bee/pkg/soc"
	"github.com/ethersphere/bee/pkg/topology"
	"io"
	"log"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/pkg/accounting"
	"github.com/ethersphere/bee/pkg/addressbook"
	"github.com/ethersphere/bee/pkg/config"
	"github.com/ethersphere/bee/pkg/crypto"
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
	"github.com/ethersphere/bee/pkg/pullsync"
	"github.com/ethersphere/bee/pkg/pullsync/pullstorage"
	"github.com/ethersphere/bee/pkg/pusher"
	"github.com/ethersphere/bee/pkg/pushsync"
	"github.com/ethersphere/bee/pkg/retrieval"
	"github.com/ethersphere/bee/pkg/settlement/pseudosettle"
	"github.com/ethersphere/bee/pkg/settlement/swap/chequebook"
	"github.com/ethersphere/bee/pkg/settlement/swap/erc20"
	"github.com/ethersphere/bee/pkg/shed"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/tags"
	"github.com/ethersphere/bee/pkg/topology/kademlia"
	"github.com/ethersphere/bee/pkg/topology/lightnode"
	"github.com/ethersphere/bee/pkg/transaction"
	"github.com/ethersphere/bee/pkg/traversal"
	ma "github.com/multiformats/go-multiaddr"
	"golang.org/x/crypto/sha3"
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

type Options struct {
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

// TODO - Add option to start full node
// TODO - satisfy blockstore Client (https://github.com/fairDataSociety/fairOS-dfs/blob/development/pkg/blockstore/client.go)
// Bee client.
type Bee struct {
	cancel     context.CancelFunc
	post       postage.Service
	batch      string
	logger     logging.Logger
	tagService *tags.Tags
	signer     crypto.Signer
	ns         storage.Storer

	p2pHalter       p2p.Halter
	topologyHalter  topology.Halter
	ethClientCloser func()
	closers         []io.Closer
}

func Start(o *Options, password string) (chan Event, chan *Bee, chan error) {
	ch := make(chan Event)
	errCh := make(chan error)
	beeCh := make(chan *Bee)
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		defer func() {
			wg.Done()

			close(ch)
			close(errCh)
			close(beeCh)
		}()

		logger := o.Logger
		keystore := filekeystore.New(filepath.Join(o.DataDir, "keys"))
		swarmPrivateKey, _, err := keystore.Key("swarm", password)
		if err != nil {
			logger.Error(fmt.Errorf("swarm key: %w", err))
			errCh <- fmt.Errorf("swarm key: %w", err)
			return
		}
		signer := crypto.NewDefaultSigner(swarmPrivateKey)
		libp2pPrivateKey, _, err := keystore.Key("libp2p", password)
		if err != nil {
			logger.Error(fmt.Errorf("libp2p key: %w", err))
			errCh <- fmt.Errorf("libp2p key: %w", err)
			return
		}

		p2pCtx, p2pCancel := context.WithCancel(context.Background())
		defer func() {
			// if there's been an error on this function
			// we'd like to cancel the p2p context so that
			// incoming connections will not be possible
			if err != nil {
				p2pCancel()
			}
		}()
		ch <- ContextCreated

		b := &Bee{
			logger: o.Logger,
			cancel: p2pCancel,
			signer: signer,
		}

		stateStore, err := node.InitStateStore(logger, o.DataDir)
		if err != nil {
			logger.Error(fmt.Errorf("init statestore: %w", err))
			errCh <- fmt.Errorf("init statestore: %w", err)
			return
		}
		b.closers = append(b.closers, stateStore)
		ch <- StateStore

		batchStoreExists, err := batchStoreExists(stateStore)
		if err != nil {
			logger.Error(fmt.Errorf("batchStoreExists: %w", err))
			errCh <- fmt.Errorf("batchStoreExists: %w", err)
			return
		}
		ch <- BatchStoreCheck

		addressbook := addressbook.New(stateStore)
		ch <- AddressBook

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
		pollingInterval = time.Duration(o.BlockTime) * time.Second
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
			logger.Error(fmt.Errorf("init chain: %w", err))
			errCh <- fmt.Errorf("init chain: %w", err)
			return
		}
		b.ethClientCloser = chainBackend.Close

		logger.Info("overlay address : ", overlayEthAddress)
		if o.ChainID != -1 && o.ChainID != chainID {
			logger.Error(fmt.Errorf("connected to wrong ethereum network; network chainID %d; configured chainID %d", chainID, o.ChainID))
			errCh <- fmt.Errorf("connected to wrong ethereum network; network chainID %d; configured chainID %d", chainID, o.ChainID)
			return
		}
		b.closers = append(b.closers, transactionService)
		b.closers = append(b.closers, transactionMonitor)
		ch <- InitChain

		isSynced, _, err := transaction.IsSynced(p2pCtx, chainBackend, maxDelay)
		if err != nil {
			logger.Error(fmt.Errorf("is synced: %w", err))
			errCh <- fmt.Errorf("is synced: %w", err)
			return
		}
		if !isSynced {
			logger.Infof("waiting to sync with the Ethereum backend")

			err := transaction.WaitSynced(p2pCtx, logger, chainBackend, maxDelay)
			if err != nil {
				log.Fatal(fmt.Errorf("waiting backend sync: %w", err))
			}
		}
		ch <- SyncChain

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
				logger.Error(fmt.Errorf("init chequebook factory: %w", err))
				errCh <- fmt.Errorf("init chequebook factory: %w", err)
				return
			}

			if err = chequebookFactory.VerifyBytecode(p2pCtx); err != nil {
				logger.Error(fmt.Errorf("factory fail: %w", err))
				errCh <- fmt.Errorf("factory fail: %w", err)
				return
			}

			erc20Address, err := chequebookFactory.ERC20Address(p2pCtx)
			if err != nil {
				logger.Error(fmt.Errorf("factory fail: %w", err))
				errCh <- fmt.Errorf("factory fail: %w", err)
				return
			}

			erc20Service = erc20.New(transactionService, erc20Address)

			if o.ChequebookEnable && chainEnabled {
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
					logger.Error(fmt.Errorf("init chequebook service failed: %w", err))
					errCh <- fmt.Errorf("init chequebook service failed: %w", err)
					return
				}
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
		ch <- SwapEnable

		pubKey, _ := signer.PublicKey()
		if err != nil {
			logger.Error(fmt.Errorf("publickey: %w", err))
			errCh <- fmt.Errorf("publickey: %w", err)
			return
		}

		var (
			blockHash []byte
			txHash    []byte
		)

		txHash, err = node.GetTxHash(stateStore, logger, o.Transaction)
		if err != nil {
			logger.Error(fmt.Errorf("invalid transaction hash: %w", err))
			errCh <- fmt.Errorf("invalid transaction hash: %w", err)
			return
		}

		blockHash, err = node.GetTxNextBlock(p2pCtx, logger, chainBackend, transactionMonitor, pollingInterval, txHash, o.BlockHash)
		if err != nil {
			logger.Error(fmt.Errorf("invalid block hash: %w", err))
			errCh <- fmt.Errorf("invalid block hash: %w", err)
			return
		}

		swarmAddress, err := crypto.NewOverlayAddress(*pubKey, o.NetworkID, blockHash)

		err = node.CheckOverlayWithStore(swarmAddress, stateStore)
		if err != nil {
			logger.Error(fmt.Errorf("check overlay: %w", err))
			errCh <- fmt.Errorf("check overlay: %w", err)
			return
		}
		logger.Infof("using overlay address %s", swarmAddress)
		ch <- Identity

		lightNodes := lightnode.NewContainer(swarmAddress)
		ch <- LightNodes

		senderMatcher := transaction.NewMatcher(chainBackend, types.NewLondonSigner(big.NewInt(chainID)), stateStore, chainEnabled)
		_, err = senderMatcher.Matches(p2pCtx, txHash, o.NetworkID, swarmAddress, true)
		if err != nil {
			logger.Error(fmt.Errorf("identity transaction verification failed: %w", err))
			errCh <- fmt.Errorf("identity transaction verification failed: %w", err)
			return
		}

		var bootnodes []ma.Multiaddr
		ch <- SenderMatcher

		for _, a := range o.Bootnodes {
			addr, err := ma.NewMultiaddr(a)
			if err != nil {
				logger.Debugf("multiaddress fail %s: %v", a, err)
				logger.Warningf("invalid bootnode address %s", a)
				continue
			}

			bootnodes = append(bootnodes, addr)
		}
		ch <- Bootstrap

		paymentThreshold, ok := new(big.Int).SetString(o.PaymentThreshold, 10)
		if !ok {
			logger.Error(fmt.Errorf("invalid payment threshold: %s", paymentThreshold))
			errCh <- fmt.Errorf("invalid payment threshold: %s", paymentThreshold)
			return
		}

		if paymentThreshold.Cmp(big.NewInt(minPaymentThreshold)) < 0 {
			logger.Error(fmt.Errorf("payment threshold below minimum generally accepted value, need at least %d", minPaymentThreshold))
			errCh <- fmt.Errorf("payment threshold below minimum generally accepted value, need at least %d", minPaymentThreshold)
			return
		}

		if paymentThreshold.Cmp(big.NewInt(maxPaymentThreshold)) > 0 {
			logger.Error(fmt.Errorf("payment threshold above maximum generally accepted value, needs to be reduced to at most %d", maxPaymentThreshold))
			errCh <- fmt.Errorf("payment threshold above maximum generally accepted value, needs to be reduced to at most %d", maxPaymentThreshold)
			return
		}

		if o.PaymentTolerance < 0 {
			logger.Error(fmt.Errorf("invalid payment tolerance: %d", o.PaymentTolerance))
			errCh <- fmt.Errorf("invalid payment tolerance: %d", o.PaymentTolerance)
			return
		}

		if o.PaymentEarly > 100 || o.PaymentEarly < 0 {
			logger.Error(fmt.Errorf("invalid payment early: %d", o.PaymentEarly))
			errCh <- fmt.Errorf("invalid payment early: %d", o.PaymentEarly)
			return
		}
		ch <- PaymentThresholdCalculation

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
				logger.Error(fmt.Errorf("bootstrapper failed to fetch batch state: %v", err))
				errCh <- fmt.Errorf("bootstrapper failed to fetch batch state: %v", err)
				return
			}
		}
		ch <- BatchState

		p2ps, err := libp2p.New(p2pCtx, signer, o.NetworkID, swarmAddress, o.Addr, addressbook, stateStore, lightNodes, senderMatcher, logger, nil, libp2p.Options{
			PrivateKey:      libp2pPrivateKey,
			NATAddr:         o.NATAddr,
			WelcomeMessage:  o.WelcomeMessage,
			Transaction:     txHash,
			ValidateOverlay: chainEnabled,
		})

		if err != nil {
			logger.Error(fmt.Errorf("p2p service: %w", err))
			errCh <- fmt.Errorf("p2p service: %w", err)
			return
		}
		b.closers = append(b.closers, p2ps)
		b.p2pHalter = p2ps
		ch <- BeeLibp2p

		var unreserveFn func([]byte, uint8) (uint64, error)
		var evictFn = func(b []byte) error {
			_, err := unreserveFn(b, swarm.MaxPO+1)
			return err
		}

		batchStore, err := batchstore.New(stateStore, evictFn, logger)
		if err != nil {
			logger.Error(fmt.Errorf("batchstore: %s", err))
			errCh <- fmt.Errorf("batchstore: %s", err)
			return
		}
		ch <- BatchStore

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
			logger.Error(fmt.Errorf("localstore: %w", err))
			errCh <- fmt.Errorf("localstore: %w", err)
			return
		}
		b.closers = append(b.closers, storer)
		unreserveFn = storer.UnreserveBatch
		ch <- LocalStore

		validStamp := postage.ValidStamp(batchStore)
		b.post, err = postage.NewService(stateStore, batchStore, chainID)
		if err != nil {
			logger.Error(fmt.Errorf("postage service load: %w", err))
			errCh <- fmt.Errorf("postage service load: %w", err)
			return
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
				errCh <- fmt.Errorf("malformed postage stamp address")
				return
			}
			postageContractAddress = common.HexToAddress(o.PostageContractAddress)
		} else if !found {
			logger.Error(fmt.Errorf("no known postage stamp addresses for this network"))
			errCh <- fmt.Errorf("no known postage stamp addresses for this network")
			return
		}
		if found {
			postageSyncStart = startBlock
		}

		ch <- PostageService

		eventListener = listener.New(logger, chainBackend, postageContractAddress, o.BlockTime, nil, postageSyncingStallingTimeout, postageSyncingBackoffTimeout)
		b.closers = append(b.closers, eventListener)
		ch <- EventListener

		batchSvc, err = batchservice.New(stateStore, batchStore, logger, eventListener, overlayEthAddress.Bytes(), b.post, sha3.New256, o.Resync)
		if err != nil {
			logger.Error(err)
			errCh <- err
			return
		}
		ch <- BatchService

		erc20Address, err := postagecontract.LookupERC20Address(p2pCtx, transactionService, postageContractAddress, chainEnabled)
		if err != nil {
			logger.Error(err)
			errCh <- err
			return
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
		_ = postageContractService
		ch <- PostageContractService

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
		ch <- NATManager

		pingPong := pingpong.New(p2ps, logger, nil)

		if err = p2ps.AddProtocol(pingPong.Protocol()); err != nil {
			logger.Error(fmt.Errorf("pingpong service: %w", err))
			errCh <- fmt.Errorf("pingpong service: %w", err)
			return
		}

		hive, err := hive.New(p2ps, addressbook, o.NetworkID, false, false, logger)
		if err != nil {
			logger.Error(fmt.Errorf("hive: %w", err))
			errCh <- fmt.Errorf("hive: %w", err)
			return
		}

		if err = p2ps.AddProtocol(hive.Protocol()); err != nil {
			logger.Error(fmt.Errorf("hive service: %w", err))
			errCh <- fmt.Errorf("hive service: %w", err)
			return
		}
		b.closers = append(b.closers, hive)
		ch <- Hive

		metricsDB, err := shed.NewDBWrap(stateStore.DB())
		if err != nil {
			logger.Error(fmt.Errorf("unable to create metrics storage for kademlia: %w", err))
			errCh <- fmt.Errorf("unable to create metrics storage for kademlia: %w", err)
			return
		}
		ch <- MetricsDB

		kad, err := kademlia.New(swarmAddress, addressbook, hive, p2ps, pingPong, metricsDB, logger,
			kademlia.Options{Bootnodes: bootnodes, BootnodeMode: false, IgnoreRadius: !o.ChainEnable})
		if err != nil {
			logger.Error(fmt.Errorf("unable to create kademlia: %w", err))
			errCh <- fmt.Errorf("unable to create kademlia: %w", err)
			return
		}
		b.topologyHalter = kad
		b.closers = append(b.closers, kad)

		hive.SetAddPeersHandler(kad.AddPeers)
		p2ps.SetPickyNotifier(kad)
		batchStore.SetRadiusSetter(kad)
		ch <- KAD

		interruptChannel := make(chan os.Signal, 1)
		signal.Notify(interruptChannel, syscall.SIGINT, syscall.SIGTERM)
		if batchSvc != nil && chainEnabled {
			syncedChan, err := batchSvc.Start(postageSyncStart, initBatchState)
			if err != nil {
				logger.Error(fmt.Errorf("unable to start batch service: %w", err))
				errCh <- fmt.Errorf("unable to start batch service: %w", err)
				return
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
				errCh <- fmt.Errorf("done")
				return
			}
		}
		ch <- BatchServiceStart

		pricer := pricer.NewFixedPricer(swarmAddress, basePrice)

		pricing := pricing.New(p2ps, logger, paymentThreshold, big.NewInt(minPaymentThreshold))

		if err = p2ps.AddProtocol(pricing.Protocol()); err != nil {
			logger.Error(fmt.Errorf("pricing service: %w", err))
			errCh <- fmt.Errorf("pricing service: %w", err)
			return
		}

		addrs, err := p2ps.Addresses()
		if err != nil {
			logger.Error(fmt.Errorf("get server addresses: %w", err))
			errCh <- fmt.Errorf("get server addresses: %w", err)
			return
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
			logger.Error(fmt.Errorf("accounting: %w", err))
			errCh <- fmt.Errorf("accounting: %w", err)
			return
		}
		b.closers = append(b.closers, acc)
		ch <- Accounting

		enforcedRefreshRate := big.NewInt(lightRefreshRate)
		pseudosettleService := pseudosettle.New(p2ps, logger, stateStore, acc, enforcedRefreshRate, big.NewInt(lightRefreshRate), p2ps)
		if err = p2ps.AddProtocol(pseudosettleService.Protocol()); err != nil {
			logger.Error(fmt.Errorf("pseudosettle service: %w", err))
			errCh <- fmt.Errorf("pseudosettle service: %w", err)
			return
		}
		acc.SetRefreshFunc(pseudosettleService.Pay)
		ch <- Pseudosettle

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
				logger.Error(err)
				errCh <- err
				return
			}
			b.closers = append(b.closers, priceOracle)

			if o.ChequebookEnable {
				acc.SetPayFunc(swapService.Pay)
			}
		}
		ch <- InitSwap

		pricing.SetPaymentThresholdObserver(acc)

		retrieve := retrieval.New(swarmAddress, storer, p2ps, kad, logger, acc, pricer, nil, o.RetrievalCaching, validStamp)
		b.tagService = tags.NewTags(stateStore, logger)
		b.closers = append(b.closers, b.tagService)

		pssPrivateKey, _, err := keystore.Key("pss", password)
		if err != nil {
			logger.Error(fmt.Errorf("pss key: %w", err))
			errCh <- fmt.Errorf("pss key: %w", err)
			return
		}
		pssService := pss.New(pssPrivateKey, logger)
		b.closers = append(b.closers, pssService)

		b.ns = netstore.New(storer, validStamp, retrieve, logger)
		b.closers = append(b.closers, b.ns)

		traversalService := traversal.New(b.ns)

		pinningService := pinning.NewService(storer, stateStore, traversalService)
		_ = pinningService
		pushSyncProtocol := pushsync.New(swarmAddress, blockHash, p2ps, storer, kad, b.tagService, false, pssService.TryUnwrap, validStamp, logger, acc, pricer, signer, nil, o.WarmupTime)

		// set the pushSyncer in the PSS
		pssService.SetPushSyncer(pushSyncProtocol)

		pusherService := pusher.New(o.NetworkID, storer, kad, pushSyncProtocol, validStamp, b.tagService, logger, nil, o.WarmupTime)
		b.closers = append(b.closers, pusherService)

		pullStorage := pullstorage.New(storer)

		pullSyncProtocol := pullsync.New(p2ps, pullStorage, pssService.TryUnwrap, validStamp, logger)
		b.closers = append(b.closers, pullSyncProtocol)

		ch <- MultipleServices

		retrieveProtocolSpec := retrieve.Protocol()
		pushSyncProtocolSpec := pushSyncProtocol.Protocol()
		pullSyncProtocolSpec := pullSyncProtocol.Protocol()

		logger.Info("starting in light mode")
		p2p.WithBlocklistStreams(p2p.DefaultBlocklistTime, retrieveProtocolSpec)
		p2p.WithBlocklistStreams(p2p.DefaultBlocklistTime, pushSyncProtocolSpec)
		p2p.WithBlocklistStreams(p2p.DefaultBlocklistTime, pullSyncProtocolSpec)

		if err = p2ps.AddProtocol(retrieveProtocolSpec); err != nil {
			logger.Error(fmt.Errorf("retrieval service: %w", err))
			errCh <- fmt.Errorf("retrieval service: %w", err)
			return
		}
		if err = p2ps.AddProtocol(pushSyncProtocolSpec); err != nil {
			logger.Error(fmt.Errorf("pushsync service: %w", err))
			errCh <- fmt.Errorf("pushsync service: %w", err)
			return
		}
		if err = p2ps.AddProtocol(pullSyncProtocolSpec); err != nil {
			logger.Error(fmt.Errorf("pullsync protocol: %w", err))
			errCh <- fmt.Errorf("pullsync protocol: %w", err)
			return
		}
		multiResolver := multiresolver.NewMultiResolver(
			multiresolver.WithLogger(o.Logger),
		)
		b.closers = append(b.closers, multiResolver)
		ch <- LiteNodeProtocols

		if err := kad.Start(p2pCtx); err != nil {
			logger.Error(err)
			errCh <- err
			return
		}

		if err := p2ps.Ready(); err != nil {
			logger.Error(err)
			errCh <- err
			return
		}
		ch <- Ready
		beeCh <- b
	}()
	wg.Wait()
	return ch, beeCh, errCh
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

func (b *Bee) UseBatch(batch string) {
	b.batch = batch
}

func (b *Bee) GetCurrentBatch() string {
	return b.batch
}

func (b *Bee) GetBatches() []*postage.StampIssuer {
	return b.post.StampIssuers()
}

func (b *Bee) AddBytes(parentContext context.Context, reader io.Reader) (reference swarm.Address, err error) {
	if b.batch == "" {
		err = fmt.Errorf("batch is not set")
		return
	}
	batch, err := hex.DecodeString(b.batch)
	if err != nil {
		err = fmt.Errorf("invalid postage batch")
		return
	}
	i, err := b.post.GetStampIssuer(batch)
	if err != nil {
		err = fmt.Errorf("stamp issuer: %w", err)
		return
	}
	tag, err := b.tagService.Create(0)
	stamper := postage.NewStamper(i, b.signer)
	putter := &stamperPutter{Storer: b.ns, stamper: stamper}
	ctx := sctx.SetTag(parentContext, tag)
	pipe := builder.NewPipelineBuilder(ctx, putter, storage.ModePutUpload, false)
	reference, err = builder.FeedPipeline(ctx, pipe, reader)
	if err != nil {
		err = fmt.Errorf("upload failed: %w", err)
		return
	}
	return
}

func (b *Bee) GetBytes(reference swarm.Address) (io.Reader, error) {
	reader, _, err := joiner.New(context.Background(), b.ns, reference)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("api download: not found : %s", err.Error())
		}
		return nil, fmt.Errorf("unexpected error: %s: %v", reference, err)
	}
	return reader, nil
}

func (b *Bee) AddSOC(ctx context.Context, owner, id, sigStr string, reader io.Reader) (reference swarm.Address, err error) {
	ownerB, err := hex.DecodeString(owner)
	if err != nil {
		b.logger.Debugf("soc upload: bad owner: %v", err)
		return
	}
	idB, err := hex.DecodeString(id)
	if err != nil {
		b.logger.Debugf("soc upload: bad id: %v", err)
		return
	}

	if sigStr == "" {
		b.logger.Debugf("soc upload: empty signature")
		return
	}

	sigB, err := hex.DecodeString(sigStr)
	if err != nil {
		b.logger.Debugf("soc upload: bad signature: %v", err)
		return
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		b.logger.Debugf("soc upload: read chunk data error: %v", err)
		return
	}

	if len(data) < swarm.SpanSize {
		b.logger.Debugf("soc upload: chunk data too short")
		return
	}

	if len(data) > swarm.ChunkSize+swarm.SpanSize {
		b.logger.Debugf("soc upload: chunk data exceeds %d bytes", swarm.ChunkSize+swarm.SpanSize)
		return
	}

	ch, err := cac.NewWithDataSpan(data)
	if err != nil {
		b.logger.Debugf("soc upload: create content addressed chunk: %v", err)
		return
	}

	ss, err := soc.NewSigned(idB, ch, ownerB, sigB)
	if err != nil {
		b.logger.Debugf("soc upload: address soc error: %v", err)
		return
	}

	sch, err := ss.Chunk()
	if err != nil {
		b.logger.Debugf("soc upload: read chunk data error: %v", err)
		return
	}

	if !soc.Valid(sch) {
		b.logger.Debugf("soc upload: invalid chunk: %v", err)
		return
	}

	has, err := b.ns.Has(ctx, sch.Address())
	if err != nil {
		b.logger.Debugf("soc upload: store has: %v", err)
		return
	}
	if has {
		b.logger.Error("soc upload: chunk already exists")
		return
	}
	batch, err := hex.DecodeString(b.batch)
	if err != nil {
		err = fmt.Errorf("invalid postage batch")
		return
	}
	i, err := b.post.GetStampIssuer(batch)
	if err != nil {
		b.logger.Debugf("soc upload: postage batch issuer: %v", err)
		return
	}

	stamper := postage.NewStamper(i, b.signer)
	stamp, err := stamper.Stamp(sch.Address())
	if err != nil {
		b.logger.Debugf("soc upload: stamp: %v", err)
		return
	}
	sch = sch.WithStamp(stamp)
	_, err = b.ns.Put(ctx, storage.ModePutUpload, sch)
	if err != nil {
		b.logger.Debugf("soc upload: chunk write error: %v", err)
		return
	}
	reference = sch.Address()
	return
}

func (b *Bee) AddFeed(ctx context.Context, owner, topic string) (reference swarm.Address, err error) {
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
	if b.batch == "" {
		err = fmt.Errorf("batch is not set")
		return
	}
	batch, err := hex.DecodeString(b.batch)
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
