package hyperliquid

import (
	"sync"
	"sync/atomic"
)

type callback func(any)

// uniqSubscriber is a subscriber that ensures only one active websocket subscription per unique key, keeping 1:N observers
// in sync with the latest data.
type uniqSubscriber struct {
	mu                  sync.RWMutex
	id                  string       // trades:<coin>, ...
	count               atomic.Int64 // Use atomic for lock-free counter
	subscribers         map[string]callback
	subscriberFunc      func(subscriptable)
	unsubscriberFunc    func(subscriptable)
	subscriptionPayload subscriptable
	asyncDispatch       bool // Enable async callback dispatch
}

func newUniqSubscriber(
	id string,
	payload subscriptable,
	subscriberFunc, unsubscriberFunc func(subscriptable),
	asyncDispatch bool,
) *uniqSubscriber {
	return &uniqSubscriber{
		id:                  id,
		subscriptionPayload: payload,
		subscribers:         make(map[string]callback),
		subscriberFunc:      subscriberFunc,
		unsubscriberFunc:    unsubscriberFunc,
		asyncDispatch:       asyncDispatch,
	}
}

func (u *uniqSubscriber) subscribe(id string, cb callback) {
	u.mu.Lock()
	if _, exists := u.subscribers[id]; exists {
		u.mu.Unlock()
		return
	}
	u.subscribers[id] = cb
	c := u.count.Add(1) // Atomic increment
	u.mu.Unlock()

	if c == 1 {
		u.subscriberFunc(u.subscriptionPayload)
	}
}

func (u *uniqSubscriber) unsubscribe(id string) {
	u.mu.Lock()
	if _, exists := u.subscribers[id]; !exists {
		u.mu.Unlock()
		return
	}
	delete(u.subscribers, id)
	c := u.count.Add(-1) // Atomic decrement
	u.mu.Unlock()

	if c == 0 {
		u.unsubscriberFunc(u.subscriptionPayload)
	}
}

func (u *uniqSubscriber) dispatch(data any) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if u.asyncDispatch {
		// Dispatch to callbacks asynchronously to avoid blocking
		for _, cb := range u.subscribers {
			go func(callback callback, msg any) {
				defer func() {
					if r := recover(); r != nil {
						// Log panic in callback but don't crash the dispatcher
						// Note: In production, you might want to use a proper logger here
					}
				}()
				callback(msg)
			}(cb, data)
		}
	} else {
		// Synchronous dispatch for tests and deterministic behavior
		for _, cb := range u.subscribers {
			cb(data)
		}
	}
}

func (u *uniqSubscriber) clear() {
	u.mu.Lock()
	defer u.mu.Unlock()

	for id := range u.subscribers {
		delete(u.subscribers, id)
	}
	u.count.Store(0) // Atomic reset
	u.unsubscriberFunc(u.subscriptionPayload)
}
