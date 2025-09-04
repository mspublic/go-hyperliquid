package hyperliquid

import (
	"net"
	"net/http"
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

// ClientOptHTTPClient allows setting a custom HTTP client for the API client.
// This gives full control over HTTP transport settings including connection pooling,
// timeouts, proxy settings, and TLS configuration.
// By default, the client uses an optimized HTTP client with connection pooling.
func ClientOptHTTPClient(httpClient *http.Client) ClientOpt {
	return func(c *Client) {
		if httpClient != nil {
			c.httpClient = httpClient
		}
	}
}

// ClientOptMaxIdleConns configures the maximum number of idle connections
// across all hosts. Higher values allow better connection reuse but consume more memory.
// Default: 100 connections.
// Recommended: 50-200 depending on expected concurrent usage.
func ClientOptMaxIdleConns(maxIdleConns int) ClientOpt {
	return func(c *Client) {
		if transport, ok := c.httpClient.Transport.(*http.Transport); ok && maxIdleConns > 0 {
			transport.MaxIdleConns = maxIdleConns
		}
	}
}

// ClientOptMaxConnsPerHost configures the maximum number of connections per host.
// This limits concurrent connections to prevent overwhelming the target server.
// Default: 20 connections per host.
// Recommended: 10-50 depending on server capacity and usage patterns.
func ClientOptMaxConnsPerHost(maxConnsPerHost int) ClientOpt {
	return func(c *Client) {
		if transport, ok := c.httpClient.Transport.(*http.Transport); ok && maxConnsPerHost > 0 {
			transport.MaxConnsPerHost = maxConnsPerHost
		}
	}
}

// ClientOptMaxIdleConnsPerHost configures the maximum number of idle connections per host.
// This balances connection reuse with memory usage for each target host.
// Default: 10 idle connections per host.
// Recommended: 5-20 depending on request frequency to each host.
func ClientOptMaxIdleConnsPerHost(maxIdleConnsPerHost int) ClientOpt {
	return func(c *Client) {
		if transport, ok := c.httpClient.Transport.(*http.Transport); ok &&
			maxIdleConnsPerHost > 0 {
			transport.MaxIdleConnsPerHost = maxIdleConnsPerHost
		}
	}
}

// ClientOptIdleConnTimeout configures how long idle connections are kept alive.
// Longer timeouts improve connection reuse but consume server resources.
// Default: 90 seconds.
// Recommended: 30-300 seconds depending on request patterns and server policies.
func ClientOptIdleConnTimeout(timeout time.Duration) ClientOpt {
	return func(c *Client) {
		if transport, ok := c.httpClient.Transport.(*http.Transport); ok && timeout > 0 {
			transport.IdleConnTimeout = timeout
		}
	}
}

// ClientOptRequestTimeout configures the overall timeout for HTTP requests.
// This includes connection establishment, request sending, and response reading.
// Default: 30 seconds.
// Recommended: 10-60 seconds depending on expected response times and network conditions.
func ClientOptRequestTimeout(timeout time.Duration) ClientOpt {
	return func(c *Client) {
		if timeout > 0 {
			c.httpClient.Timeout = timeout
		}
	}
}

// ClientOptDialTimeout configures the timeout for establishing new connections.
// This affects how long to wait when connecting to the server.
// Default: 10 seconds.
// Recommended: 5-30 seconds depending on network conditions and requirements.
func ClientOptDialTimeout(timeout time.Duration) ClientOpt {
	return func(c *Client) {
		if transport, ok := c.httpClient.Transport.(*http.Transport); ok && timeout > 0 {
			dialer := &net.Dialer{
				Timeout: timeout,
			}
			transport.DialContext = dialer.DialContext
		}
	}
}
