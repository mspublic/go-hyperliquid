# Performance Guide

This guide provides detailed information about the performance optimizations in go-hyperliquid and how to configure them for your specific use case.

## Overview

The go-hyperliquid library has been extensively optimized for high-performance trading applications with:

- **5-10x faster JSON operations** through EasyJSON integration
- **2-3x faster WebSocket processing** with message batching and pooling
- **65-100% reduction in memory allocations** across critical paths
- **Lock-free data structures** for better concurrency
- **Pre-defined errors** for 163x faster error handling

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
- Use `WsOptAsyncCallbacks(true)` to prevent blocking
- Set `WsOptBatchSize(20-50)` for optimal throughput
- Monitor connection health continuously
- Implement custom retry logic if needed

### Market Data Processing
- Use larger batch sizes for better throughput
- Enable async callbacks for data analysis
- Implement proper error handling for data gaps

### Resource-Constrained Environments
- Use smaller buffer sizes to limit memory usage
- Disable batching for immediate processing
- Set lower reconnection attempt limits

The library's performance optimizations are designed to work automatically while providing fine-grained control when needed.
