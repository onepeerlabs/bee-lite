package beelite

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	chaincfg "github.com/ethersphere/bee/pkg/config"
	"github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/keystore"
	filekeystore "github.com/ethersphere/bee/pkg/keystore/file"
	beelog "github.com/ethersphere/bee/pkg/log"
	"github.com/ethersphere/bee/pkg/resolver/multiresolver"
	"github.com/ethersphere/bee/pkg/swarm"
)

type LiteOptions struct {
	FullNodeMode             bool
	BootnodeMode             bool
	Bootnodes                []string
	StaticNodes              []string
	DataDir                  string
	WelcomeMessage           string
	BlockchainRpcEndpoint    string
	SwapInitialDeposit       string
	PaymentThreshold         string
	SwapEnable               bool
	ChequebookEnable         bool
	UsePostageSnapshot       bool
	DebugAPIEnable           bool
	Mainnet                  bool
	NetworkID                uint64
	NATAddr                  string
	CacheCapacity            uint64
	DBOpenFilesLimit         uint64
	DBWriteBufferSize        uint64
	DBBlockCacheCapacity     uint64
	DBDisableSeeksCompaction bool
	RetrievalCaching         bool
}

type buildBeeliteNodeResp struct {
	beelite *Beelite
	err     error
}

type signerConfig struct {
	signer           crypto.Signer
	publicKey        *ecdsa.PublicKey
	libp2pPrivateKey *ecdsa.PrivateKey
	pssPrivateKey    *ecdsa.PrivateKey
}

type networkConfig struct {
	bootNodes []string
	blockTime time.Duration
	chainID   int64
}

func configureSigner(lo *LiteOptions, password string, beelogger beelog.Logger) (config *signerConfig, err error) {
	var keystore keystore.Service
	const libp2pPKFilename = "libp2p_v2"
	keystore = filekeystore.New(filepath.Join(lo.DataDir, "keys"))

	var signer crypto.Signer
	var publicKey *ecdsa.PublicKey
	if p := password; p != "" {
		password = p
	}

	swarmPrivateKey, _, err := keystore.Key("swarm", password, crypto.EDGSecp256_K1)
	if err != nil {
		return nil, fmt.Errorf("swarm key: %w", err)
	}
	signer = crypto.NewDefaultSigner(swarmPrivateKey)
	publicKey = &swarmPrivateKey.PublicKey

	beelogger.Info("swarm public key", "public_key", hex.EncodeToString(crypto.EncodeSecp256k1PublicKey(publicKey)))

	libp2pPrivateKey, created, err := keystore.Key(libp2pPKFilename, password, crypto.EDGSecp256_R1)
	if err != nil {
		return nil, fmt.Errorf("libp2p v2 key: %w", err)
	}
	if created {
		beelogger.Debug("new libp2p v2 key created")
	} else {
		beelogger.Debug("using existing libp2p key")
	}

	pssPrivateKey, created, err := keystore.Key("pss", password, crypto.EDGSecp256_K1)
	if err != nil {
		return nil, fmt.Errorf("pss key: %w", err)
	}
	if created {
		beelogger.Debug("new pss key created")
	} else {
		beelogger.Debug("using existing pss key")
	}

	beelogger.Info("pss public key", "public_key", hex.EncodeToString(crypto.EncodeSecp256k1PublicKey(&pssPrivateKey.PublicKey)))

	overlayEthAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, err
	}
	beelogger.Info("using ethereum address", "address", overlayEthAddress)

	return &signerConfig{
		signer:           signer,
		publicKey:        publicKey,
		libp2pPrivateKey: libp2pPrivateKey,
		pssPrivateKey:    pssPrivateKey,
	}, nil
}

func buildBeeNodeAsync(ctx context.Context, lo *LiteOptions, password string, beelogger beelog.Logger) <-chan buildBeeliteNodeResp {
	respC := make(chan buildBeeliteNodeResp, 1)

	go func() {
		beelite, err := buildBeeNode(ctx, lo, password, beelogger)
		respC <- buildBeeliteNodeResp{beelite, err}
	}()

	return respC
}

func buildBeeNode(ctx context.Context, lo *LiteOptions, password string, beelogger beelog.Logger) (*Beelite, error) {
	var err error

	debugAPIAddr := ":1635"
	if !lo.DebugAPIEnable {
		debugAPIAddr = ""
	}

	signerCfg, err := configureSigner(lo, password, beelogger)
	if err != nil {
		return nil, err
	}

	if lo.DataDir == "" {
		return nil, errors.New("keystore/data dir empty")
	}

	bootnodeMode := lo.BootnodeMode
	fullNodeMode := lo.FullNodeMode
	if bootnodeMode && !fullNodeMode {
		return nil, errors.New("boot node must be started as a full node")
	}

	mainnet := lo.Mainnet
	networkID := lo.NetworkID
	if mainnet && networkID != chaincfg.Mainnet.NetworkID {
		return nil, errors.New("provided network ID does not match mainnet")
	}

	bootnodes := lo.Bootnodes
	networkCfg := getConfigByNetworkID(networkID)
	if lo.Bootnodes != nil {
		networkCfg.bootNodes = bootnodes
	}

	staticNodesOpt := []string{}
	if nil != lo.StaticNodes {
		staticNodesOpt = lo.StaticNodes
	}
	staticNodes := make([]swarm.Address, 0, len(staticNodesOpt))
	for _, p := range staticNodesOpt {
		addr, err := swarm.ParseHexAddress(p)
		if err != nil {
			return nil, fmt.Errorf("invalid swarm address %q configured for static node", p)
		}

		staticNodes = append(staticNodes, addr)
	}
	if len(staticNodes) > 0 && !bootnodeMode {
		return nil, errors.New("static nodes can only be configured on bootnodes")
	}

	beelite, err := NewBee(ctx, ":1634", signerCfg.publicKey, signerCfg.signer, networkID, beelogger, signerCfg.libp2pPrivateKey, signerCfg.pssPrivateKey, &Options{
		DataDir:                       lo.DataDir,
		CacheCapacity:                 lo.CacheCapacity,
		DBOpenFilesLimit:              lo.DBOpenFilesLimit,
		DBBlockCacheCapacity:          lo.DBBlockCacheCapacity,
		DBWriteBufferSize:             lo.DBWriteBufferSize,
		DBDisableSeeksCompaction:      lo.DBDisableSeeksCompaction,
		APIAddr:                       ":1633",
		DebugAPIAddr:                  debugAPIAddr,
		Addr:                          ":1634",
		NATAddr:                       lo.NATAddr,
		EnableWS:                      false,
		WelcomeMessage:                lo.WelcomeMessage,
		Bootnodes:                     networkCfg.bootNodes,
		CORSAllowedOrigins:            []string{"*"},
		TracingEnabled:                false,
		TracingEndpoint:               ":6831",
		TracingServiceName:            LoggerName,
		Logger:                        beelogger,
		PaymentThreshold:              lo.PaymentThreshold,
		PaymentTolerance:              25,
		PaymentEarly:                  50,
		ResolverConnectionCfgs:        []multiresolver.ConnectionConfig{},
		BootnodeMode:                  bootnodeMode,
		BlockchainRpcEndpoint:         lo.BlockchainRpcEndpoint,
		SwapFactoryAddress:            "",
		SwapLegacyFactoryAddresses:    []string{},
		SwapInitialDeposit:            lo.SwapInitialDeposit,
		SwapEnable:                    lo.SwapEnable,
		ChequebookEnable:              lo.ChequebookEnable,
		FullNodeMode:                  fullNodeMode,
		PostageContractAddress:        "",
		PostageContractStartBlock:     0,
		PriceOracleAddress:            "",
		RedistributionContractAddress: "",
		StakingContractAddress:        "",
		BlockTime:                     networkCfg.blockTime,
		DeployGasPrice:                "",
		WarmupTime:                    0,
		ChainID:                       networkCfg.chainID,
		RetrievalCaching:              lo.RetrievalCaching,
		Resync:                        false,
		BlockProfile:                  false,
		MutexProfile:                  false,
		StaticNodes:                   staticNodes,
		AllowPrivateCIDRs:             false,
		Restricted:                    false,
		TokenEncryptionKey:            "",
		AdminPasswordHash:             "",
		UsePostageSnapshot:            lo.UsePostageSnapshot,
		EnableStorageIncentives:       true,
		StatestoreCacheCapacity:       1000000,
		TargetNeighborhood:            "",
	})

	return beelite, err
}

func Start(lo *LiteOptions, password string, verbosity string) (bl *Beelite, errMain error) {
	beelogger, err := newLogger(LoggerName, verbosity)
	if err != nil {
		errMain = fmt.Errorf("logger creation error: %w", err)
		return nil, errMain
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	sysInterruptChannel := make(chan os.Signal, 1)
	signal.Notify(sysInterruptChannel, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case <-sysInterruptChannel:
			beelogger.Info("received interrupt signal")
			ctxCancel()
		case <-ctx.Done():
		}
	}()

	defer func() {
		if errMain != nil && bl != nil && bl.Bee != nil {
			beelogger.Info("shutting down...")
			err := bl.Bee.Shutdown()
			if err != nil {
				beelogger.Error(err, "shutdown failed")
			}
		}
	}()

	respC := buildBeeNodeAsync(ctx, lo, password, beelogger)
	// Wait for bee node to fully build and initialize
	select {
	case resp := <-respC:
		if resp.err != nil {
			errMain = resp.err
			beelogger.Error(resp.err, "failed to build bee node")
			return nil, errMain
		}

		bl = resp.beelite
		bl.Bee = resp.beelite.Bee
		beelogger.Info("bee node built")
	case <-ctx.Done():
		beelogger.Info("ctx done")
		return nil, ctx.Err()
	}

	beelogger.Info("bee start finished")
	return bl, errMain
}
