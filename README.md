# bee-lite

[![Go Reference](https://pkg.go.dev/badge/github.com/onepeerlabs/bee-lite.svg)](https://pkg.go.dev/github.com/onepeerlabs/bee-lite)

bee-lite is an embeddable, lightweight bee node for applications to use swarm directly

## How to run

```
lo := &beelite.LiteOptions {
    FullNodeMode:             false,
    BootnodeMode:             false,
    Bootnodes:                []string{"/dnsaddr/mainnet.ethswarm.org"},
    StaticNodes:              []string{<STATIC_NODE_ADDRESSES>},
    DataDir:                  dataDir,
    WelcomeMessage:           "Welcome from bee-lite by Solar Punk",
    BlockchainRpcEndpoint:    <RPC_ENDPOINT>,
    SwapInitialDeposit:       "10000000000000000",
    PaymentThreshold:         "100000000",
    SwapEnable:               true,
    ChequebookEnable:         true,
    DebugAPIEnable:           false,
    UsePostageSnapshot:       false,
    Mainnet:                  true,
    NetworkID:                1,
    NATAddr:                  "<NAT_ADDRESS>:<PORT>",
    CacheCapacity:            32 * 1024 * 1024,
    DBOpenFilesLimit:         50,
    DBWriteBufferSize:        32 * 1024 * 1024,
    DBBlockCacheCapacity:     32 * 1024 * 1024,
    DBDisableSeeksCompaction: false,
    RetrievalCaching:         true,
}

const loglevel = "4"
bl, err := beelite.Start(lo, password, loglevel)
if err != nil {
    return err
}
```
