# bee-lite
[![Go Reference](https://pkg.go.dev/badge/github.com/onepeerlabs/bee-lite.svg)](https://pkg.go.dev/github.com/onepeerlabs/bee-lite)

bee-lite is an embeddable, lightweight bee node for applications to use swarm directly

## How to run
```
o := &bee.Options{
    FullNodeMode:             true,
    Keystore:                 keystore,
    DataDir:                  dataDir,
    Addr:                     ":1836",
    WelcomeMessage:           "welcome from bee-lite",
    Bootnodes:                []string{"/dnsaddr/testnet.ethswarm.org"},
    Logger:                   logging.New(os.Stdout, logrus.ErrorLevel),
    SwapEndpoint:             <SWAP_ENDPOINT>,
    SwapInitialDeposit:       "10000000000000000",
    SwapEnable:               true,
    WarmupTime:               0,
    ChainID:                  100,
    ChequebookEnable:         true,
    ChainEnable:              true,
    BlockTime:                uint64(5),
    PaymentThreshold:         "100000000",
    UsePostageSnapshot:       false,
    Mainnet:                  true,
    NetworkID:                1,
    DBOpenFilesLimit:         200,
    DBWriteBufferSize:        32 * 1024 * 1024,
    DBDisableSeeksCompaction: false,
    DBBlockCacheCapacity:     32 * 1024 * 1024,
    RetrievalCaching:         true,
}
b, err := bee.Start(o, password)
if err != nil {
    return err
}
```