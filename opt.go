package hyperliquid

import (
	"os"
	"time"

	"github.com/sonirico/vago/lol"
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

// WsOptBatchSize configures WebSocket message batching size
func WsOptBatchSize(size int) WsOpt {
	return func(w *WebsocketClient) {
		if size > 0 {
			w.batchSize = size
		}
	}
}

// WsOptBatchTimeout configures WebSocket message batching timeout
func WsOptBatchTimeout(timeout time.Duration) WsOpt {
	return func(w *WebsocketClient) {
		if timeout > 0 {
			w.batchTimeout = timeout
		}
	}
}

// WsOptBufferSize configures WebSocket message buffer size
func WsOptBufferSize(size int) WsOpt {
	return func(w *WebsocketClient) {
		if size > 0 {
			w.messageBuffer = make(chan wsMessage, size)
		}
	}
}

// WsOptAsyncCallbacks enables asynchronous callback dispatch for better performance
func WsOptAsyncCallbacks(enabled bool) WsOpt {
	return func(w *WebsocketClient) {
		// Note: This will be applied to new subscribers created after this option
		// Existing subscribers retain their current dispatch mode
		w.asyncCallbacks = enabled
	}
}
