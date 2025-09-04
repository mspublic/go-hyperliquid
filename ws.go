package hyperliquid

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sonirico/vago/lol"
)

const (
	// pingInterval is the interval for sending ping messages to keep WebSocket alive
	pingInterval = 50 * time.Second
)

// wsMessagePool reduces allocations for WebSocket message processing
var wsMessagePool = sync.Pool{
	New: func() any {
		return &wsMessage{}
	},
}

// subscriberSlicePool reduces allocations for subscriber slice operations
var subscriberSlicePool = sync.Pool{
	New: func() any {
		slice := make([]*uniqSubscriber, 0, 16) // Pre-allocate for 16 subscribers
		return &slice
	},
}

type Subscription struct {
	ID      string
	Payload any
	Close   func()
}

type WebsocketClient struct {
	url                   string
	conn                  *websocket.Conn
	mu                    sync.RWMutex // Only for connection state
	writeMu               sync.Mutex
	subscribers           sync.Map // Lock-free subscriber map
	msgDispatcherRegistry map[string]msgDispatcher
	nextSubID             atomic.Int64
	done                  chan struct{}
	closeOnce             sync.Once
	reconnectWait         time.Duration
	debug                 bool
	logger                lol.Logger

	// Message batching for high-frequency scenarios
	batchSize      int
	batchTimeout   time.Duration
	messageBuffer  chan wsMessage
	asyncCallbacks bool // Enable async callback dispatch

	// Error handling and circuit breaker
	reconnectAttempts    atomic.Int64
	maxReconnectAttempts int64
	lastError            atomic.Value // stores error
}

func NewWebsocketClient(baseURL string, opts ...WsOpt) *WebsocketClient {
	if baseURL == "" {
		baseURL = MainnetAPIURL
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		log.Fatalf("invalid URL: %v", err)
	}
	parsedURL.Scheme = "wss"
	parsedURL.Path = "/ws"
	wsURL := parsedURL.String()

	cli := &WebsocketClient{
		url:                  wsURL,
		done:                 make(chan struct{}),
		reconnectWait:        time.Second,
		batchSize:            10,                        // Process up to 10 messages in batch
		batchTimeout:         5 * time.Millisecond,      // Max 5ms batching delay
		messageBuffer:        make(chan wsMessage, 100), // Buffer up to 100 messages
		maxReconnectAttempts: 10,                        // Max reconnection attempts
		msgDispatcherRegistry: map[string]msgDispatcher{
			ChannelPong:         NewPongDispatcher(),
			ChannelTrades:       NewMsgDispatcher[Trades](ChannelTrades),
			ChannelL2Book:       NewMsgDispatcher[L2Book](ChannelL2Book),
			ChannelCandle:       NewMsgDispatcher[Candles](ChannelCandle),
			ChannelAllMids:      NewMsgDispatcher[AllMids](ChannelAllMids),
			ChannelNotification: NewMsgDispatcher[Notification](ChannelNotification),
			ChannelOrderUpdates: NewMsgDispatcher[WsOrders](ChannelOrderUpdates),
			ChannelWebData2:     NewMsgDispatcher[WebData2](ChannelWebData2),
			ChannelBbo:          NewMsgDispatcher[Bbo](ChannelBbo),
			ChannelSubResponse:  NewNoopDispatcher(),
		},
	}

	for _, opt := range opts {
		opt.Apply(cli)
	}

	return cli
}

func (w *WebsocketClient) Connect(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		return nil
	}

	dialer := websocket.Dialer{
		HandshakeTimeout:  45 * time.Second,
		ReadBufferSize:    4096,  // 4KB read buffer
		WriteBufferSize:   1024,  // 1KB write buffer
		EnableCompression: false, // Disable compression for trading (latency over bandwidth)
	}

	//nolint:bodyclose // WebSocket connections don't have response bodies to close
	conn, _, err := dialer.DialContext(ctx, w.url, nil)
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}

	w.conn = conn

	go w.readPump(ctx)
	go w.pingPump(ctx)
	go w.batchProcessor(ctx) // Start batch processor

	return w.resubscribeAll()
}

type Handler[T subscriptable] func(wsMessage) (T, error)

func (w *WebsocketClient) subscribe(
	payload subscriptable,
	callback func(any),
) (*Subscription, error) {
	if callback == nil {
		return nil, ErrCallbackNil
	}

	pkey := payload.Key()
	subscriberVal, exists := w.subscribers.Load(pkey)
	var subscriber *uniqSubscriber
	if !exists {
		subscriber = newUniqSubscriber(
			pkey,
			payload,
			// on subscribe
			func(p subscriptable) {
				if err := w.sendSubscribe(p); err != nil {
					if w.logger != nil {
						w.logger.Errorf("failed to subscribe: %v", err)
					}
				}
			},
			// on unsubscribe
			func(p subscriptable) {
				w.subscribers.Delete(pkey)
				if err := w.sendUnsubscribe(p); err != nil {
					if w.logger != nil {
						w.logger.Errorf("failed to unsubscribe: %v", err)
					}
				}
			},
			w.asyncCallbacks, // Pass async dispatch setting
		)

		// Try to store, but use existing if another goroutine created it first
		if actualVal, loaded := w.subscribers.LoadOrStore(pkey, subscriber); loaded {
			subscriber = actualVal.(*uniqSubscriber)
		}
	} else {
		subscriber = subscriberVal.(*uniqSubscriber)
	}

	nextID := w.nextSubID.Add(1)
	subID := key(pkey, strconv.Itoa(int(nextID)))
	subscriber.subscribe(subID, callback)

	return &Subscription{
		ID: subID,
		Close: func() {
			subscriber.unsubscribe(subID)
		},
	}, nil
}

func (w *WebsocketClient) Close() error {
	var err error
	w.closeOnce.Do(func() {
		err = w.close()
	})
	return err
}

func (w *WebsocketClient) close() error {
	close(w.done)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		err := w.conn.Close()

		// Clear all subscribers using sync.Map
		w.subscribers.Range(func(key, value any) bool {
			subscriber := value.(*uniqSubscriber)
			subscriber.clear()
			return true
		})

		return err
	}

	return nil
}

// Private methods

func (w *WebsocketClient) readPump(ctx context.Context) {
	defer func() {
		w.mu.Lock()
		if w.conn != nil {
			_ = w.conn.Close() // Ignore close error in defer
			w.conn = nil
		}
		w.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		default:
			_, msg, err := w.conn.ReadMessage()
			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					if w.logger != nil {
						w.logger.Errorf("websocket read error: %v", err)
					}
				}
				return
			}

			if w.debug && w.logger != nil {
				w.logger.Debugf("[<] %s", string(msg))
			}

			wsMsg := wsMessagePool.Get().(*wsMessage)
			wsMsg.Channel = "" // Reset fields
			wsMsg.Data = nil

			if err := wsMsg.UnmarshalJSON(msg); err != nil {
				if w.logger != nil {
					w.logger.Errorf("websocket message parse error: %v", err)
				}
				wsMessagePool.Put(wsMsg) // Return to pool on error
				continue
			}

			// Send to batch processor for efficient handling
			select {
			case w.messageBuffer <- *wsMsg:
				// Message sent to batch processor
			default:
				// Buffer full, process immediately to avoid blocking
				if err := w.dispatch(*wsMsg); err != nil {
					if w.logger != nil {
						w.logger.Errorf("failed to dispatch websocket message: %v", err)
					}
				}
			}

			wsMessagePool.Put(wsMsg) // Return to pool after processing
		}
	}
}

func (w *WebsocketClient) pingPump(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.sendPing(); err != nil {
				if w.logger != nil {
					w.logger.Errorf("ping error: %v", err)
				}
				w.reconnect(ctx)
				return
			}
		}
	}
}

func (w *WebsocketClient) dispatch(msg wsMessage) error {
	dispatcher, ok := w.msgDispatcherRegistry[msg.Channel]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNoDispatcher, msg.Channel)
	}

	// Use pool for subscriber slice to reduce allocations
	subscribersPtr := subscriberSlicePool.Get().(*[]*uniqSubscriber)
	subscribers := (*subscribersPtr)[:0] // Reset length but keep capacity

	// Lock-free iteration over subscribers using sync.Map
	w.subscribers.Range(func(key, value any) bool {
		subscribers = append(subscribers, value.(*uniqSubscriber))
		return true // Continue iteration
	})

	err := dispatcher.Dispatch(subscribers, msg)

	// Reset and return slice to pool
	*subscribersPtr = subscribers[:0]
	subscriberSlicePool.Put(subscribersPtr)

	return err
}

// batchProcessor handles message batching for improved throughput
func (w *WebsocketClient) batchProcessor(ctx context.Context) {
	batch := make([]wsMessage, 0, w.batchSize)
	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case msg := <-w.messageBuffer:
			batch = append(batch, msg)

			// Process batch when full or on timeout
			if len(batch) >= w.batchSize {
				w.processBatch(batch)
				batch = batch[:0] // Reset batch
				ticker.Reset(w.batchTimeout)
			}

		case <-ticker.C:
			// Process partial batch on timeout
			if len(batch) > 0 {
				w.processBatch(batch)
				batch = batch[:0] // Reset batch
			}
		}
	}
}

// processBatch efficiently processes a batch of messages
func (w *WebsocketClient) processBatch(batch []wsMessage) {
	for _, msg := range batch {
		if err := w.dispatch(msg); err != nil {
			if w.logger != nil {
				w.logger.Errorf("failed to dispatch batched message: %v", err)
			}
		}
	}
}

func (w *WebsocketClient) reconnect(ctx context.Context) {
	for {
		select {
		case <-w.done:
			return
		case <-ctx.Done():
			return
		default:
			// Circuit breaker: stop reconnecting after max attempts
			attempts := w.reconnectAttempts.Load()
			if attempts >= w.maxReconnectAttempts {
				if w.logger != nil {
					w.logger.Errorf("max reconnection attempts reached (%d), giving up", w.maxReconnectAttempts)
				}
				return
			}

			w.reconnectAttempts.Add(1)

			if err := w.Connect(ctx); err == nil {
				// Reset attempts on successful connection
				w.reconnectAttempts.Store(0)
				w.lastError.Store(nil)
				return
			} else {
				// Store last error
				w.lastError.Store(err)
			}

			time.Sleep(w.reconnectWait)
			w.reconnectWait *= 2 // Exponential backoff
			if w.reconnectWait > time.Minute {
				w.reconnectWait = time.Minute
			}
		}
	}
}

func (w *WebsocketClient) resubscribeAll() error {
	var firstError error
	w.subscribers.Range(func(key, value any) bool {
		subscriber := value.(*uniqSubscriber)
		if err := w.sendSubscribe(subscriber.subscriptionPayload); err != nil {
			if firstError == nil {
				firstError = fmt.Errorf("resubscribe: %w", err)
			}
			return false // Stop iteration on first error
		}
		return true // Continue iteration
	})
	return firstError
}

func (w *WebsocketClient) sendSubscribe(payload subscriptable) error {
	return w.writeJSON(wsCommand{
		Method:       "subscribe",
		Subscription: payload,
	})
}

func (w *WebsocketClient) sendUnsubscribe(payload subscriptable) error {
	return w.writeJSON(wsCommand{
		Method:       "unsubscribe",
		Subscription: payload,
	})
}

func (w *WebsocketClient) sendPing() error {
	return w.writeJSON(wsCommand{Method: "ping"})
}

func (w *WebsocketClient) writeJSON(v any) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if w.conn == nil {
		return ErrConnectionClosed
	}

	if w.debug && w.logger != nil {
		bts, _ := json.Marshal(v)
		w.logger.Debugf("[>] %s", string(bts))
	}

	return w.conn.WriteJSON(v)
}

// GetLastError returns the last connection error (for monitoring)
func (w *WebsocketClient) GetLastError() error {
	if err := w.lastError.Load(); err != nil {
		return err.(error)
	}
	return nil
}

// GetReconnectAttempts returns the current number of reconnection attempts
func (w *WebsocketClient) GetReconnectAttempts() int64 {
	return w.reconnectAttempts.Load()
}

// IsHealthy returns true if the WebSocket connection is healthy
func (w *WebsocketClient) IsHealthy() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.conn != nil && w.GetLastError() == nil
}
