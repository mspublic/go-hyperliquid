package hyperliquid

import (
	"os"
	"time"

	"github.com/sonirico/vago/lol"
)

const (
	// Configuration limits for WebSocket options
	MaxBatchSize         = 1000        // Maximum messages per batch
	MaxBatchTimeout      = time.Second // Maximum batch timeout
	MaxBufferSize        = 10000       // Maximum message buffer size
	MaxReconnectAttempts = 100         // Maximum reconnection attempts
)

type Opt[T any] func(*T)

func (o Opt[T]) Apply(opt *T) {
	o(opt)
}

type (
	ClientOpt   = Opt[Client]
	ExchangeOpt = Opt[Exchange]
	InfoOpt     = Opt[Info]
	WsOpt       = Opt[WebsocketClient]
)

func WsOptDebugMode() WsOpt {
	return func(w *WebsocketClient) {
		w.debug = true
		w.logger = lol.NewZerolog(
			lol.WithLevel(lol.LevelTrace),
			lol.WithWriter(os.Stderr),
			lol.WithEnv(lol.EnvDev),
		)
	}
}

func InfoOptDebugMode() InfoOpt {
	return func(i *Info) {
		i.debug = true
	}
}

func ExchangeOptDebugMode() ExchangeOpt {
	return func(e *Exchange) {
		e.debug = true
	}
}

func ClientOptDebugMode() ClientOpt {
	return func(c *Client) {
		c.debug = true
		c.logger = lol.NewZerolog(
			lol.WithLevel(lol.LevelTrace),
			lol.WithWriter(os.Stderr),
			lol.WithEnv(lol.EnvDev),
		)
	}
}

// WsOptBatchSize configures the WebSocket message batching size for improved throughput.
// Messages are batched together and processed in groups to reduce overhead.
// Default: 10 messages per batch.
// Recommended: 5-20 for most use cases, higher for extreme high-frequency scenarios.
// Valid range: 1 to MaxBatchSize (1000).
func WsOptBatchSize(size int) WsOpt {
	return func(w *WebsocketClient) {
		if size > 0 && size <= MaxBatchSize {
			w.batchSize = size
		}
	}
}

// WsOptBatchTimeout configures the maximum time to wait before processing a partial batch.
// This ensures messages are processed promptly even when batch size isn't reached.
// Default: 5 milliseconds.
// Recommended: 1-10ms for trading applications.
// Valid range: 1ns to MaxBatchTimeout (1 second).
func WsOptBatchTimeout(timeout time.Duration) WsOpt {
	return func(w *WebsocketClient) {
		if timeout > 0 && timeout <= MaxBatchTimeout {
			w.batchTimeout = timeout
		}
	}
}

// WsOptBufferSize configures the WebSocket message buffer size for handling burst traffic.
// This buffer prevents message loss during temporary processing delays.
// Default: 100 messages.
// Recommended: 50-500 depending on expected message volume.
// Valid range: 1 to MaxBufferSize (10000).
func WsOptBufferSize(size int) WsOpt {
	return func(w *WebsocketClient) {
		if size > 0 && size <= MaxBufferSize {
			w.messageBuffer = make(chan wsMessage, size)
		}
	}
}

// WsOptAsyncCallbacks enables asynchronous callback dispatch for improved performance.
// When enabled, callbacks are executed in separate goroutines to prevent blocking
// the message processing pipeline. Useful for heavy callback processing.
// Default: false (synchronous for deterministic behavior).
// Note: This applies to new subscribers created after this option is set.
func WsOptAsyncCallbacks(enabled bool) WsOpt {
	return func(w *WebsocketClient) {
		w.asyncCallbacks = enabled
	}
}

// WsOptMaxReconnectAttempts configures the circuit breaker for reconnection attempts.
// After reaching this limit, the client stops trying to reconnect automatically.
// This prevents infinite reconnection loops and resource exhaustion.
// Default: 10 attempts.
// Recommended: 5-20 depending on network reliability requirements.
// Valid range: 1 to MaxReconnectAttempts (100).
func WsOptMaxReconnectAttempts(maxAttempts int64) WsOpt {
	return func(w *WebsocketClient) {
		if maxAttempts > 0 && maxAttempts <= MaxReconnectAttempts {
			w.maxReconnectAttempts = maxAttempts
		}
	}
}
