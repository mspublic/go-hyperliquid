# go-hyperliquid

[![Go Reference](https://pkg.go.dev/badge/github.com/sonirico/go-hyperliquid.svg)](https://pkg.go.dev/github.com/sonirico/go-hyperliquid)
[![Go Report Card](https://goreportcard.com/badge/github.com/sonirico/go-hyperliquid)](https://goreportcard.com/report/github.com/sonirico/go-hyperliquid)
[![CI](https://github.com/sonirico/go-hyperliquid/actions/workflows/ci.yml/badge.svg)](https://github.com/sonirico/go-hyperliquid/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/sonirico/go-hyperliquid/badge.svg?branch=main)](https://coveralls.io/github/sonirico/go-hyperliquid?branch=main)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/github/go-mod/go-version/sonirico/go-hyperliquid)](https://go.dev/)

Unofficial Go client for the Hyperliquid exchange API. This implementation follows the same philosophy and patterns as the official Python SDK.

## Installation

```bash
go get github.com/sonirico/go-hyperliquid
```

## Features

This Go SDK provides **full feature parity** with the official Python SDK, including:

### Trading Features

- **Order Management**: Limit orders, market orders, trigger orders, order modifications
- **Position Management**: Leverage updates, isolated margin, position closing
- **Bulk Operations**: Bulk orders, bulk cancellations, bulk modifications
- **Advanced Trading**: Market open/close with slippage protection, scheduled cancellations
- **Builder Support**: Order routing through builders with fee structures

### Account Management

- **Referral System**: Set referral codes, track referral state
- **Sub-Accounts**: Create and manage sub-accounts, transfer funds
- **Multi-Signature**: Convert to multi-sig, execute multi-sig actions
- **Vault Operations**: Vault deposits, withdrawals, and transfers

### Asset Management

- **USD Transfers**: Cross-chain USD transfers, spot transfers
- **Class Transfers**: USD class transfers (perp ↔ spot), perp dex transfers
- **Bridge Operations**: Withdraw from bridge with fee management
- **Token Delegation**: Stake tokens with validators
- **Spot Trading**: Full spot market support

### Advanced Features

- **Agent Approval**: Approve trading agents with permissions
- **Builder Fee Management**: Approve and manage builder fees
- **Big Blocks**: Enable/disable big block usage

### Deployment Features (Advanced)

- **Spot Deployment**: Token registration, genesis, freeze privileges
- **Perp Deployment**: Asset registration, oracle management
- **Hyperliquidity**: Register hyperliquidity assets

### Consensus Layer (Validators)

- **Validator Operations**: Register, unregister, profile management
- **Signer Operations**: Jail/unjail self, inner actions
- **Consensus Actions**: Full consensus layer interaction

### WebSocket Features

- **Market Data**: Real-time L2 book, trades, candles, mid prices
- **User Events**: Order updates, fills, funding, ledger updates
- **Advanced Streams**: BBO, active asset context, web data v2

## Usage

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ethereum/go-ethereum/crypto"
    hyperliquid "github.com/sonirico/go-hyperliquid"
)

func main() {
    // Initialize client
    client := hyperliquid.NewClient(hyperliquid.MainnetAPIURL)

    // For trading, create an Exchange with your private key
    privateKey, _ := crypto.HexToECDSA("your-private-key")
    exchange := hyperliquid.NewExchange(
        privateKey,
        hyperliquid.MainnetAPIURL,
        nil,    // Meta will be fetched automatically
        "vault-address",
        "account-address",
        nil,    // SpotMeta will be fetched automatically
    )

    // Place a limit order
    order := hyperliquid.OrderRequest{
        Coin:    "BTC",
        IsBuy:   true,
        Size:    0.1,
        LimitPx: 40000.0,
        OrderType: hyperliquid.OrderType{
            Limit: &hyperliquid.LimitOrderType{
                Tif: "Gtc",
            },
        },
    }

    resp, err := exchange.Order(order, nil)
    if err != nil {
        log.Fatal(err)
    }

    // Subscribe to WebSocket updates
    ws := hyperliquid.NewWebsocketClient(hyperliquid.MainnetAPIURL)
    if err := ws.Connect(context.Background()); err != nil {
        log.Fatal(err)
    }
    defer ws.Close()

    // Subscribe to BTC trades
    _, err = ws.Subscribe(hyperliquid.Subscription{
        Type: "trades",
        Coin: "BTC",
    }, func(msg hyperliquid.wsMessage) {
        fmt.Printf("Trade: %+v\n", msg)
    })
}
```

## Performance Configuration

The library includes advanced performance optimizations that can be configured for your specific use case:

### WebSocket Performance Tuning

```go
// High-frequency trading configuration
ws := hyperliquid.NewWebsocketClient(hyperliquid.MainnetAPIURL,
    hyperliquid.WsOptBatchSize(20),                    // Larger batches for throughput
    hyperliquid.WsOptBatchTimeout(2*time.Millisecond), // Lower latency
    hyperliquid.WsOptBufferSize(500),                  // Handle burst traffic
    hyperliquid.WsOptAsyncCallbacks(true),             // Non-blocking callbacks
    hyperliquid.WsOptMaxReconnectAttempts(5),          // Circuit breaker
)

// Low-latency configuration
ws := hyperliquid.NewWebsocketClient(hyperliquid.MainnetAPIURL,
    hyperliquid.WsOptBatchSize(1),                     // Immediate processing
    hyperliquid.WsOptBatchTimeout(1*time.Millisecond), // Minimal delay
    hyperliquid.WsOptAsyncCallbacks(false),            // Synchronous for predictability
)
```

### Configuration Limits

The library enforces reasonable limits to prevent resource exhaustion:

- **Batch Size**: 1 to `hyperliquid.MaxBatchSize` (1000)
- **Batch Timeout**: 1ns to `hyperliquid.MaxBatchTimeout` (1 second)
- **Buffer Size**: 1 to `hyperliquid.MaxBufferSize` (10000)
- **Reconnect Attempts**: 1 to `hyperliquid.MaxReconnectAttempts` (100)

### Health Monitoring

Monitor connection health with built-in methods:

```go
ws := hyperliquid.NewWebsocketClient(hyperliquid.MainnetAPIURL)

// Check connection health
if !ws.IsHealthy() {
    if err := ws.GetLastError(); err != nil {
        log.Printf("Connection error: %v", err)
    }
    log.Printf("Reconnect attempts: %d", ws.GetReconnectAttempts())
}
```

## Documentation

For detailed API documentation, please refer to:

- [Official Hyperliquid API docs](https://hyperliquid.xyz/docs)
- [Go package documentation](https://pkg.go.dev/github.com/sonirico/go-hyperliquid)

### Examples

Check the `examples/` directory for more usage examples:

- WebSocket subscriptions
- Order management
- Position handling
- Market data retrieval

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Quick Start for Contributors

```bash
# Clone the repository
git clone https://github.com/sonirico/go-hyperliquid.git
cd go-hyperliquid

# Install dependencies and tools
make deps install-tools

# Run all checks
make ci-full

# Run tests (excluding examples)
make ci-test
```

## Roadmap

### ✅ Completed Features

- [x] Complete WebSocket API implementation
- [x] REST API client
- [x] All trading operations (orders, positions, leverage)
- [x] Market data (L2 book, trades, candles, all mids)
- [x] User account management
- [x] Referral system implementation
- [x] Sub-account management
- [x] Vault operations
- [x] USD and spot transfers
- [x] Bridge operations
- [x] Agent approval system
- [x] Builder fee management
- [x] Multi-signature support
- [x] Token delegation/staking
- [x] Spot deployment features
- [x] Perp deployment features
- [x] Consensus layer (validator operations)
- [x] Full feature parity with Python SDK

### 🚀 Recent Performance Enhancements

- [x] **Comprehensive Performance Optimizations** - 5-10x faster JSON operations
- [x] **Advanced Memory Management** - Object pooling, reduced allocations
- [x] **WebSocket Optimizations** - Message batching, async callbacks, circuit breaker
- [x] **Concurrency Improvements** - Lock-free data structures, atomic operations
- [x] **Error Handling Enhancements** - Pre-defined errors, 163x faster error creation
- [x] **Enhanced documentation with more examples**
- [x] **Comprehensive benchmarks and performance tests**
- [x] **Monitoring and observability features**
- [x] **Order management**
- [x] **User account operations**

### 🎯 Future Enhancements

- [ ] Advanced order types
- [ ] Historical data API  
- [ ] Rate limiting improvements
- [ ] HTTP connection pooling

## License

MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
