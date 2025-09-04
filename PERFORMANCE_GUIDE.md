# Performance Guide

This guide provides detailed information about the performance optimizations in go-hyperliquid and how to configure them for your specific use case.

## Overview

The go-hyperliquid library has been extensively optimized for high-performance trading applications with:

- **5-10x faster JSON operations** through EasyJSON integration
- **2-3x faster WebSocket processing** with message batching and pooling
- **65-100% reduction in memory allocations** across critical paths
- **Lock-free data structures** for better concurrency
- **Pre-defined errors** for 163x faster error handling
- **HTTP connection pooling** with high-performance defaults for trading applications

## WebSocket Performance Configuration

### Basic Configuration

```go
import (
    "time"
    hyperliquid "github.com/sonirico/go-hyperliquid"
)

// Default configuration (good for most use cases)
ws := hyperliquid.NewWebsocketClient(hyperliquid.MainnetAPIURL)

// High-performance configuration
ws := hyperliquid.NewWebsocketClient(hyperliquid.MainnetAPIURL,
    hyperliquid.WsOptBatchSize(20),                    // Process 20 messages per batch
    hyperliquid.WsOptBatchTimeout(2*time.Millisecond), // Max 2ms batching delay
    hyperliquid.WsOptBufferSize(500),                  // Buffer 500 messages
    hyperliquid.WsOptAsyncCallbacks(true),             // Non-blocking callbacks
)
```

### Configuration Options

| Option | Purpose | Default | Range | Best For |
|--------|---------|---------|-------|----------|
| `WsOptBatchSize` | Messages per batch | 10 | 1-1000 | Higher values for throughput |
| `WsOptBatchTimeout` | Max batch delay | 5ms | 1ns-1s | Lower values for latency |
| `WsOptBufferSize` | Message buffer size | 100 | 1-10000 | Higher for burst traffic |
| `WsOptAsyncCallbacks` | Async dispatch | false | true/false | true for heavy processing |
| `WsOptMaxReconnectAttempts` | Circuit breaker | 10 | 1-100 | Lower for fail-fast behavior |

### Use Case Configurations

#### High-Frequency Trading
```go
ws := hyperliquid.NewWebsocketClient(url,
    hyperliquid.WsOptBatchSize(50),                    // Large batches
    hyperliquid.WsOptBatchTimeout(1*time.Millisecond), // Minimal delay
    hyperliquid.WsOptBufferSize(1000),                 // Large buffer
    hyperliquid.WsOptAsyncCallbacks(true),             // Non-blocking
)
```

#### Low-Latency Trading
```go
ws := hyperliquid.NewWebsocketClient(url,
    hyperliquid.WsOptBatchSize(1),                     // No batching
    hyperliquid.WsOptBatchTimeout(1*time.Millisecond), // Immediate
    hyperliquid.WsOptAsyncCallbacks(false),            // Synchronous
)
```

#### Market Data Analysis
```go
ws := hyperliquid.NewWebsocketClient(url,
    hyperliquid.WsOptBatchSize(100),                   // Large batches
    hyperliquid.WsOptBatchTimeout(10*time.Millisecond), // Longer delay OK
    hyperliquid.WsOptBufferSize(2000),                 // Large buffer
    hyperliquid.WsOptAsyncCallbacks(true),             // Process in background
)
```

## HTTP Client Performance Configuration

The library uses optimized HTTP connection pooling by default for maximum performance:

### Default Configuration

```go
// Default optimized HTTP client (automatic)
client := hyperliquid.NewClient(hyperliquid.MainnetAPIURL)
```

**Default Settings:**
- **500 idle connections** across all hosts
- **100 connections per host** 
- **50 idle connections per host**
- **90s idle connection timeout**
- **30s request timeout**
- **10s dial timeout**

### Custom HTTP Configuration

```go
import (
    "net/http"
    "time"
    hyperliquid "github.com/sonirico/go-hyperliquid"
)

// Ultra high-throughput configuration
client := hyperliquid.NewClient(hyperliquid.MainnetAPIURL,
    hyperliquid.ClientOptMaxIdleConns(1000),             // Maximum idle connections
    hyperliquid.ClientOptMaxConnsPerHost(200),           // Very high per-host limit
    hyperliquid.ClientOptMaxIdleConnsPerHost(100),       // Maximum idle per host
    hyperliquid.ClientOptIdleConnTimeout(120*time.Second), // Longer keep-alive
    hyperliquid.ClientOptRequestTimeout(15*time.Second),   // Faster timeout
    hyperliquid.ClientOptDialTimeout(5*time.Second),       // Quick connection
)

// Low-latency configuration
client := hyperliquid.NewClient(hyperliquid.MainnetAPIURL,
    hyperliquid.ClientOptMaxConnsPerHost(10),            // Fewer connections
    hyperliquid.ClientOptRequestTimeout(5*time.Second),   // Very fast timeout
    hyperliquid.ClientOptDialTimeout(2*time.Second),      // Quick dial
)

// Custom HTTP client for advanced use cases
customClient := &http.Client{
    Transport: &http.Transport{
        MaxIdleConns: 500,
        // ... other custom settings
    },
}
client := hyperliquid.NewClient(hyperliquid.MainnetAPIURL,
    hyperliquid.ClientOptHTTPClient(customClient),
)
```

### HTTP Configuration Options

| Option | Purpose | Default | Recommended Range |
|--------|---------|---------|-------------------|
| `ClientOptMaxIdleConns` | Total idle connections | 500 | 200-1000 |
| `ClientOptMaxConnsPerHost` | Max connections per host | 100 | 50-200 |
| `ClientOptMaxIdleConnsPerHost` | Idle connections per host | 50 | 20-100 |
| `ClientOptIdleConnTimeout` | Keep-alive duration | 90s | 30-300s |
| `ClientOptRequestTimeout` | Total request timeout | 30s | 5-60s |
| `ClientOptDialTimeout` | Connection dial timeout | 10s | 2-30s |

## Health Monitoring

### Connection Health Checks

```go
ws := hyperliquid.NewWebsocketClient(hyperliquid.MainnetAPIURL)

// Regular health monitoring
ticker := time.NewTicker(30 * time.Second)
go func() {
    for range ticker.C {
        if !ws.IsHealthy() {
            log.Printf("WebSocket unhealthy - attempts: %d, error: %v", 
                ws.GetReconnectAttempts(), ws.GetLastError())
        }
    }
}()
```

### Error Recovery

```go
// Monitor for connection issues
go func() {
    for {
        if attempts := ws.GetReconnectAttempts(); attempts > 5 {
            log.Printf("High reconnection attempts: %d", attempts)
            // Consider implementing custom recovery logic
        }
        time.Sleep(10 * time.Second)
    }
}()
```

## Performance Benchmarks

Run benchmarks to measure performance on your system:

```bash
# Run all performance benchmarks
go test -bench=. -benchmem -run=^$ github.com/sonirico/go-hyperliquid

# Run specific benchmark categories
go test -bench=BenchmarkWebSocket -benchmem -run=^$
go test -bench=BenchmarkJSON -benchmem -run=^$
go test -bench=BenchmarkConcurrency -benchmem -run=^$
go test -bench=BenchmarkErrorHandling -benchmem -run=^$
```

## Memory Optimization

### Automatic Optimizations

The library automatically optimizes memory usage through:

- **Object pooling** for WebSocket messages and HTTP buffers
- **Slice reuse** for subscriber operations
- **String builder pooling** for error messages
- **Pre-allocated buffers** based on Content-Length headers

### Memory Monitoring

```go
import (
    "runtime"
    "time"
)

// Monitor memory usage
go func() {
    var m runtime.MemStats
    for {
        runtime.ReadMemStats(&m)
        log.Printf("Alloc: %d KB, Sys: %d KB, NumGC: %d", 
            m.Alloc/1024, m.Sys/1024, m.NumGC)
        time.Sleep(30 * time.Second)
    }
}()
```

## JSON Performance

### Automatic EasyJSON Usage

The library automatically uses high-performance EasyJSON for:

- All WebSocket message types (Trade, L2Book, Candle, etc.)
- Trading operation types (OrderStatus, Position, UserState)
- Exchange and info API responses
- Subscription parameters

### Performance Comparison

| Operation | Standard JSON | EasyJSON | Improvement |
|-----------|---------------|----------|-------------|
| Trade Marshal | 903.5 ns/op | 153.7 ns/op | 5.9x faster |
| Trade Unmarshal | 1402 ns/op | 430.6 ns/op | 3.3x faster |
| OrderStatus Marshal | 514.0 ns/op | 82.7 ns/op | 6.2x faster |
| Position Marshal | 1398 ns/op | 347.7 ns/op | 4.0x faster |
| UserState Marshal | 3064 ns/op | 575.8 ns/op | 5.3x faster |

## Comprehensive Benchmark Results

### JSON Operations Performance

| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| **Trade Marshal** | 903.5 ns/op, 384 B/op | 153.7 ns/op, 128 B/op | 5.9x faster, 3x less memory |
| **Trade Unmarshal** | 1402 ns/op, 400 B/op | 430.6 ns/op, 96 B/op | 3.3x faster, 4.2x less memory |
| **OrderStatus Marshal** | 514.0 ns/op, 232 B/op | 82.7 ns/op, 128 B/op | 6.2x faster, 1.8x less memory |
| **Position Marshal** | 1398 ns/op, 1137 B/op | 347.7 ns/op, 784 B/op | 4.0x faster, 1.4x less memory |
| **UserState Marshal** | 3064 ns/op, 1931 B/op | 575.8 ns/op, 1176 B/op | 5.3x faster, 1.6x less memory |

### WebSocket Operations Performance

| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| **Message Processing** | 1737 ns/op, 384 B/op | 734.2 ns/op, 136 B/op | 2.4x faster, 2.8x less memory |
| **API Response Parsing** | 3310 ns/op, 3400 B/op | 303.8 ns/op, 0 B/op | 10.9x faster, ∞ less memory |

### Concurrency Operations Performance

| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| **Subscriber Access** | 580.6 ns/op, 0 B/op | 502.1 ns/op, 0 B/op | 13.5% faster |
| **Counter Operations** | 7.978 ns/op, 0 B/op | 4.218 ns/op, 0 B/op | 89% faster |
| **Concurrent Ops** | 7362 ns/op, 1523 B/op | 6288 ns/op, 1523 B/op | 17% faster |

## Error Handling Best Practices

### Efficient Error Handling

```go
// The library uses pre-defined errors for common cases
if err := ws.Connect(ctx); err != nil {
    // These comparisons are very fast due to pre-defined errors
    if errors.Is(err, hyperliquid.ErrConnectionClosed) {
        // Handle connection closed
    } else if errors.Is(err, hyperliquid.ErrCallbackNil) {
        // Handle nil callback
    }
}

// For custom error wrapping, use the optimized wrapper
err := hyperliquid.WrapError(baseErr, "failed to process order")
```

### Error Performance

| Error Type | Standard | Optimized | Improvement |
|-----------|----------|-----------|-------------|
| Common Errors | 48.79 ns/op | 0.298 ns/op | 163x faster |
| Error Wrapping | 86.16 ns/op | 64.85 ns/op | 25% faster |

## Concurrency Optimization

### Lock-Free Operations

The library uses lock-free data structures where possible:

- **sync.Map** for WebSocket subscribers (13.5% faster access)
- **atomic.Int64** for counters (89% faster operations)
- **Minimized critical sections** for better concurrency

### Concurrent Usage

```go
// The library is designed for high concurrency
var wg sync.WaitGroup

// Multiple concurrent subscriptions
for _, coin := range []string{"BTC", "ETH", "SOL"} {
    wg.Add(1)
    go func(coin string) {
        defer wg.Done()
        ws.Trades(hyperliquid.TradesSubscriptionParams{
            Coin: coin,
        }, func(trades []hyperliquid.Trade, err error) {
            // Process trades concurrently
        })
    }(coin)
}

wg.Wait()
```

## Troubleshooting Performance Issues

### Common Performance Issues

1. **Slow Callbacks**: Use `WsOptAsyncCallbacks(true)` for heavy processing
2. **Memory Growth**: Monitor with health checks and adjust buffer sizes
3. **Connection Instability**: Tune `WsOptMaxReconnectAttempts` and monitor health
4. **High Latency**: Reduce batch sizes and timeouts for real-time needs

### Performance Monitoring

```go
// Monitor key performance metrics
go func() {
    ticker := time.NewTicker(60 * time.Second)
    for range ticker.C {
        log.Printf("WebSocket Health: %v, Reconnects: %d", 
            ws.IsHealthy(), ws.GetReconnectAttempts())
    }
}()
```

## Production Recommendations

### High-Frequency Trading
- **WebSocket**: Use `WsOptAsyncCallbacks(true)` to prevent blocking
- **WebSocket**: Set `WsOptBatchSize(20-50)` for optimal throughput
- **HTTP**: Use `ClientOptMaxConnsPerHost(100-200)` for high API usage
- **HTTP**: Set `ClientOptRequestTimeout(5-15s)` for fast responses
- Monitor connection health continuously
- Implement custom retry logic if needed

### Market Data Processing
- **WebSocket**: Use larger batch sizes for better throughput
- **WebSocket**: Enable async callbacks for data analysis
- **HTTP**: Use default connection pooling settings (already optimized)
- **HTTP**: Consider `ClientOptIdleConnTimeout(120s)` for longer sessions
- Implement proper error handling for data gaps

### Resource-Constrained Environments
- **WebSocket**: Use smaller buffer sizes to limit memory usage
- **WebSocket**: Disable batching for immediate processing
- **WebSocket**: Set lower reconnection attempt limits
- **HTTP**: Reduce `ClientOptMaxIdleConns(100-200)` to save memory
- **HTTP**: Lower `ClientOptMaxConnsPerHost(20-50)` for conservative usage

### Latency-Critical Applications
- **WebSocket**: Use `WsOptBatchSize(1)` for immediate processing
- **WebSocket**: Set `WsOptBatchTimeout(1ms)` for minimal delay
- **HTTP**: Use `ClientOptDialTimeout(2-5s)` for quick connections
- **HTTP**: Set `ClientOptRequestTimeout(5-10s)` for fast timeouts

The library's performance optimizations are designed to work automatically while providing fine-grained control when needed.
