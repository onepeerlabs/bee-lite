# bee-lite
[![Go Reference](https://pkg.go.dev/badge/github.com/onepeerlabs/bee-lite.svg)](https://pkg.go.dev/github.com/onepeerlabs/bee-lite)

bee-lite is an embeddable, lightweight bee node for application to use swarm directly


### What are events in bee-lite?
While starting the bee node we perform multiple tasks. On each completed task we emit an event so
that we can keep track of how much progress we have made during startup.

### Events
We have the following events
```
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
    PseudoSettle
    InitSwap
    MultipleServices
    LiteNodeProtocols
    Ready
)
```

## How to run
```
o := &bee.Options{
    FullNodeMode:             true,
    Keystore:                 keystore,
    DataDir:                  dataDir,
    Addr:                     ":1836",
    WelcomeMessage:           "welcome from bee-lite",
    Bootnodes:                []string{"/dnsaddr/mainnet.ethswarm.org"},
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

// start returns three channels
// first one is for events. Events channel will emit "Ready" once the node has started.
// After we get the Ready event, we can read the bee instance itself from the second channel.
// Third channel is for errors. During the startup if we encounter any error that will be send
// via this third channel.
ch, beeCh, errCh := bee.Start(o, password)
ready:
    for {
        select {
        case evt := <-ch:
            if evt == bee.Ready {
                fmt.Println("node ready")
                i.b = <-beeCh
                break ready
            }
        case err := <-errCh:
            return error
        }
    }
```