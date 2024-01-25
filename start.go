package beelite

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	chaincfg "github.com/ethersphere/bee/pkg/config"
	"github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/keystore"
	filekeystore "github.com/ethersphere/bee/pkg/keystore/file"
	beelog "github.com/ethersphere/bee/pkg/log"
	"github.com/ethersphere/bee/pkg/resolver/multiresolver"
	"github.com/ethersphere/bee/pkg/swarm"
)

type MobileOptions struct {
	FullNodeMode             bool
	BootnodeMode             bool
	Bootnodes                []string
	StaticNodes              []string
	DataDir                  string
	WelcomeMessage           string
	SwapEndpoint             string
	BlockchainRpcEndpoint    string
	SwapInitialDeposit       string
	SwapEnable               bool
	ChequebookEnable         bool
	DebugAPIEnable           bool
	Mainnet                  bool
	NetworkID                uint64
	NATAddr                  string
}

type buildBeeliteNodeResp struct {
	beelite *Beelite
	err error
}

type signerConfig struct {
	signer              crypto.Signer
	publicKey           *ecdsa.PublicKey
	libp2pPrivateKey    *ecdsa.PrivateKey
	pssPrivateKey       *ecdsa.PrivateKey
}

type networkConfig struct {
	bootNodes []string
	blockTime time.Duration
	chainID   int64
}


func configureSigner(mo *MobileOptions, password string, beelogger beelog.Logger) (config *signerConfig, err error) {
	var keystore keystore.Service
	const libp2pPKFilename = "libp2p_v2"
	keystore = filekeystore.New(filepath.Join(mo.DataDir, "keys"))

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

	// postinst and post scripts inside packaging/{deb,rpm} depend and parse on this log output
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

func buildBeeNodeAsync(ctx context.Context, mo *MobileOptions, password string, beelogger beelog.Logger) <-chan buildBeeliteNodeResp {
	respC := make(chan buildBeeliteNodeResp, 1)

	go func() {
		beelite, err := buildBeeNode(ctx, mo, password, beelogger)
		respC <- buildBeeliteNodeResp{beelite, err}
	}()

	return respC
}

func buildBeeNode(ctx context.Context, mo *MobileOptions, password string, beelogger beelog.Logger) (*Beelite, error) {
	var err error

	debugAPIAddr := ":1635"
	if !mo.DebugAPIEnable {
		debugAPIAddr = ""
	}

	signerCfg, err := configureSigner(mo, password, beelogger)
	if err != nil {
		return nil, err
	}

	if mo.DataDir == "" {
		return nil, errors.New("keystore/data dir empty")
	}

	bootnodeMode := mo.BootnodeMode
	fullNodeMode := mo.FullNodeMode

	if bootnodeMode && !fullNodeMode {
		return nil, errors.New("boot node must be started as a full node")
	}

	mainnet := mo.Mainnet
	networkID := mo.NetworkID
	if mainnet && networkID != chaincfg.Mainnet.NetworkID {
		return nil, errors.New("provided network ID does not match mainnet")
	}

	bootnodes := mo.Bootnodes
	networkCfg := getConfigByNetworkID(networkID)

	if mo.Bootnodes != nil {
		networkCfg.bootNodes = bootnodes
	}

	staticNodesOpt := []string{}
	if nil != mo.StaticNodes {
		staticNodesOpt = mo.StaticNodes
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

	swapEndpoint := mo.SwapEndpoint
	blockchainRpcEndpoint := mo.BlockchainRpcEndpoint
	if swapEndpoint != "" {
		blockchainRpcEndpoint = swapEndpoint
	}

	beelite, err := NewBee(ctx, ":1634", signerCfg.publicKey, signerCfg.signer, networkID, beelogger, signerCfg.libp2pPrivateKey, signerCfg.pssPrivateKey, &Options{
		DataDir:                       mo.DataDir,
		CacheCapacity:                 defaultBlockCacheCapacity,
		DBOpenFilesLimit:              50,
		DBBlockCacheCapacity:          defaultBlockCacheCapacity,
		DBWriteBufferSize:             defaultWriteBufferSize,
		DBDisableSeeksCompaction:      false,
		APIAddr:                       ":1633",
		DebugAPIAddr:                  debugAPIAddr,
		Addr:                          ":1634",
		NATAddr:                       mo.NATAddr,
		EnableWS:                      false,
		WelcomeMessage:                mo.WelcomeMessage,
		Bootnodes:                     networkCfg.bootNodes,
		CORSAllowedOrigins:            []string{"*"},
		TracingEnabled:                false,
		TracingEndpoint:               ":6831",
		TracingServiceName:            "beelite",
		Logger:                        beelogger,
		PaymentThreshold:              "100000000",
		PaymentTolerance:              25,
		PaymentEarly:                  50,
		ResolverConnectionCfgs:        []multiresolver.ConnectionConfig{},
		BootnodeMode:                  bootnodeMode,
		BlockchainRpcEndpoint:         blockchainRpcEndpoint,
		SwapFactoryAddress:            "",
		SwapLegacyFactoryAddresses:    []string{},
		SwapInitialDeposit:            mo.SwapInitialDeposit,
		SwapEnable:                    mo.SwapEnable,
		ChequebookEnable:              mo.ChequebookEnable,
		FullNodeMode:                  fullNodeMode,
		PostageContractAddress:        "",
		PostageContractStartBlock:     0,
		PriceOracleAddress:            "",
		RedistributionContractAddress: "",
		StakingContractAddress:        "",
		BlockTime:                     networkCfg.blockTime,
		DeployGasPrice:                "",
		WarmupTime:                    time.Minute*5,
		ChainID:                       networkCfg.chainID,
		RetrievalCaching:              true,
		Resync:                        false,
		BlockProfile:                  false,
		MutexProfile:                  false,
		StaticNodes:                   staticNodes,
		AllowPrivateCIDRs:             false,
		Restricted:                    false,
		TokenEncryptionKey:            "",
		AdminPasswordHash:             "",
		UsePostageSnapshot:            false,
		EnableStorageIncentives:       true,
		StatestoreCacheCapacity:       1000000,
		TargetNeighborhood:            "",
	})

	return beelite, err
}

func Start(mo *MobileOptions, password string, verbosity string) (bl *Beelite, errMain error) {
	beelogger, err := newLogger(LoggerName, verbosity)
	if err != nil {
		errMain = fmt.Errorf("logger creation error: %w", err)
		return nil, errMain
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer func() {
		if errMain != nil && bl != nil && bl.bee != nil {
			beelogger.Info("shutting down...")
			ctxCancel()
			err := bl.bee.Shutdown()
			if err != nil {
				beelogger.Error(err, "shutdown failed")
			}
		}
	}()

	respC := buildBeeNodeAsync(ctx, mo, password, beelogger)

	select {
	case resp := <-respC:
		if resp.err != nil {
			errMain = resp.err
			beelogger.Error(resp.err, "failed to build bee node")
			return nil, errMain
		}

		bl = resp.beelite
		bl.bee = resp.beelite.bee
		beelogger.Info("bee node built")
	case <-ctx.Done():
		beelogger.Info("ctx done")
	}

	beelogger.Info("bee start finished")
	return bl, errMain
}
