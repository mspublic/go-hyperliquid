package hyperliquid

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fastjson"
)

// BenchmarkWebSocketMessageProcessing benchmarks the WebSocket message processing
// with and without object pooling
func BenchmarkWebSocketMessageProcessing(b *testing.B) {
	sampleMessage := []byte(
		`{"channel":"trades","data":{"coin":"BTC","side":"B","px":"50000.0","sz":"0.1","time":1640995200000,"hash":"abc123","tid":12345,"users":["user1","user2"]}}`,
	)

	b.Run("WithoutPool", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var msg wsMessage
			_ = json.Unmarshal(sampleMessage, &msg)
		}
	})

	b.Run("WithPool", func(b *testing.B) {
		pool := sync.Pool{
			New: func() any {
				return &wsMessage{}
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			msg := pool.Get().(*wsMessage)
			msg.Channel = ""
			msg.Data = nil
			_ = msg.UnmarshalJSON(sampleMessage)
			pool.Put(msg)
		}
	})
}

// BenchmarkEasyJSONvsStandardJSON compares easyjson vs standard json performance
func BenchmarkEasyJSONvsStandardJSON(b *testing.B) {
	trade := Trade{
		Coin:  "BTC",
		Side:  "B",
		Px:    "50000.0",
		Sz:    "0.1",
		Time:  1640995200000,
		Hash:  "abc123",
		Tid:   12345,
		Users: []string{"user1", "user2"},
	}

	b.Run("StandardJSON_Marshal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = json.Marshal(trade)
		}
	})

	b.Run("EasyJSON_Marshal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = trade.MarshalJSON()
		}
	})

	tradeJSON, _ := json.Marshal(trade)

	b.Run("StandardJSON_Unmarshal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var t Trade
			_ = json.Unmarshal(tradeJSON, &t)
		}
	})

	b.Run("EasyJSON_Unmarshal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var t Trade
			_ = t.UnmarshalJSON(tradeJSON)
		}
	})
}

// BenchmarkHTTPResponseReading benchmarks HTTP response body reading
// with and without pre-allocation
func BenchmarkHTTPResponseReading(b *testing.B) {
	sampleResponse := make([]byte, 1024) // Simulate 1KB response
	for i := range sampleResponse {
		sampleResponse[i] = byte('a' + (i % 26))
	}

	b.Run("WithoutPreallocation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			body := make([]byte, 0)
			body = append(body, sampleResponse...)
			_ = body
		}
	})

	b.Run("WithPreallocation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			body := make([]byte, 0, len(sampleResponse))
			body = append(body, sampleResponse...)
			_ = body
		}
	})
}

// BenchmarkAPIResponseParsing benchmarks API response parsing with fastjson
func BenchmarkAPIResponseParsing(b *testing.B) {
	responseJSON := `{"status":"ok","response":{"type":"clearinghouse","data":{"assetPositions":[{"position":{"coin":"BTC","entryPx":"50000.0","leverage":{"type":"cross","value":10},"liquidationPx":"45000.0","marginUsed":"5000.0","positionValue":"50000.0","returnOnEquity":"0.05","szi":"1.0","unrealizedPnl":"2500.0"}}]}}}`
	responseBytes := []byte(responseJSON)

	b.Run("StandardJSON", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var response map[string]any
			_ = json.Unmarshal(responseBytes, &response)
		}
	})

	b.Run("FastJSON", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			parser := parserPool.Get().(*fastjson.Parser)
			_, _ = parser.ParseBytes(responseBytes)
			parserPool.Put(parser)
		}
	})
}

// BenchmarkMemoryAllocations measures memory allocations in critical paths
func BenchmarkMemoryAllocations(b *testing.B) {
	b.Run("StringConcatenation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := "prefix" + "_" + "suffix" + "_" + "end"
			_ = result
		}
	})

	b.Run("StringBuilderConcatenation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var builder strings.Builder
			builder.WriteString("prefix")
			builder.WriteString("_")
			builder.WriteString("suffix")
			builder.WriteString("_")
			builder.WriteString("end")
			_ = builder.String()
		}
	})
}

// BenchmarkCriticalStructsSerialization benchmarks the newly optimized structs
func BenchmarkCriticalStructsSerialization(b *testing.B) {
	// Test OrderStatus - critical for trading
	orderStatus := OrderStatus{
		Resting: &OrderStatusResting{
			Oid:      12345678901,
			ClientID: stringPtr("test-client-id"),
			Status:   "open",
		},
	}

	b.Run("OrderStatus_EasyJSON_Marshal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = orderStatus.MarshalJSON()
		}
	})

	b.Run("OrderStatus_StandardJSON_Marshal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = json.Marshal(orderStatus)
		}
	})

	// Test Position - critical for account management
	position := Position{
		Coin:           "BTC",
		EntryPx:        stringPtr("50000.0"),
		Leverage:       Leverage{Type: "cross", Value: 10},
		LiquidationPx:  stringPtr("45000.0"),
		MarginUsed:     "5000.0",
		PositionValue:  "50000.0",
		ReturnOnEquity: "0.05",
		Szi:            "1.0",
		UnrealizedPnl:  "2500.0",
	}

	b.Run("Position_EasyJSON_Marshal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = position.MarshalJSON()
		}
	})

	b.Run("Position_StandardJSON_Marshal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = json.Marshal(position)
		}
	})

	// Test UserState - critical for account data
	userState := UserState{
		AssetPositions: []AssetPosition{
			{Position: position, Type: "oneWay"},
		},
		CrossMarginSummary: MarginSummary{
			AccountValue:    "100000.0",
			TotalMarginUsed: "5000.0",
			TotalNtlPos:     "50000.0",
			TotalRawUsd:     "100000.0",
		},
		MarginSummary: MarginSummary{
			AccountValue:    "100000.0",
			TotalMarginUsed: "5000.0",
			TotalNtlPos:     "50000.0",
			TotalRawUsd:     "100000.0",
		},
		Withdrawable: "95000.0",
	}

	b.Run("UserState_EasyJSON_Marshal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = userState.MarshalJSON()
		}
	})

	b.Run("UserState_StandardJSON_Marshal", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = json.Marshal(userState)
		}
	})
}

// BenchmarkHTTPResponseReadingOptimized benchmarks the improved HTTP response reading
func BenchmarkHTTPResponseReadingOptimized(b *testing.B) {
	// Simulate different response sizes
	sizes := []int{1024, 4096, 16384} // 1KB, 4KB, 16KB

	for _, size := range sizes {
		responseData := make([]byte, size)
		for i := range responseData {
			responseData[i] = byte('a' + (i % 26))
		}

		b.Run(fmt.Sprintf("Size_%dB_ReadAll", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := bytes.NewReader(responseData)
				_, _ = io.ReadAll(reader)
			}
		})

		b.Run(fmt.Sprintf("Size_%dB_ReadFull", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := bytes.NewReader(responseData)
				buffer := make([]byte, size)
				_, _ = io.ReadFull(reader, buffer)
			}
		})
	}
}

// BenchmarkOrderWirePoolUsage benchmarks the order wire pool optimization
func BenchmarkOrderWirePoolUsage(b *testing.B) {
	orders := make([]CreateOrderRequest, 10)
	for i := range orders {
		orders[i] = CreateOrderRequest{
			Coin:      "BTC",
			IsBuy:     true,
			Price:     50000.0,
			Size:      0.1,
			OrderType: OrderType{Limit: &LimitOrderType{Tif: "Gtc"}},
		}
	}

	b.Run("WithoutPool", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Direct allocation (what we had before)
			orderRequests := make([]OrderWire, len(orders))
			for j := range orders {
				orderRequests[j] = OrderWire{} // Simulate usage
			}
			_ = orderRequests
		}
	})

	b.Run("WithPool", func(b *testing.B) {
		pool := sync.Pool{
			New: func() any {
				slice := make([]OrderWire, 0, 8)
				return &slice
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Pool-based allocation (current implementation)
			orderRequestsPtr := pool.Get().(*[]OrderWire)
			if cap(*orderRequestsPtr) < len(orders) {
				*orderRequestsPtr = make([]OrderWire, 0, len(orders))
			}
			*orderRequestsPtr = (*orderRequestsPtr)[:len(orders)]
			orderRequests := *orderRequestsPtr

			for j := range orders {
				orderRequests[j] = OrderWire{} // Simulate usage
			}

			*orderRequestsPtr = (*orderRequestsPtr)[:0]
			pool.Put(orderRequestsPtr)
		}
	})
}

// BenchmarkErrorHandlingOptimizations benchmarks error handling improvements
func BenchmarkErrorHandlingOptimizations(b *testing.B) {
	// Test pre-defined errors vs fmt.Errorf
	b.Run("ErrorCreation_FmtErrorf", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := fmt.Errorf("callback cannot be nil")
			_ = err
		}
	})

	b.Run("ErrorCreation_Predefined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := ErrCallbackNil
			_ = err
		}
	})

	// Test error wrapping with context
	baseErr := errors.New("base error")

	b.Run("ErrorWrapping_FmtErrorf", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := fmt.Errorf("failed to process: %w", baseErr)
			_ = err
		}
	})

	b.Run("ErrorWrapping_WithPool", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := WrapError(baseErr, "failed to process")
			_ = err
		}
	})

	// Test error with dynamic content
	b.Run("DynamicError_FmtErrorf", func(b *testing.B) {
		channel := "trades"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := fmt.Errorf("no dispatcher for channel: %s", channel)
			_ = err
		}
	})

	b.Run("DynamicError_Optimized", func(b *testing.B) {
		channel := "trades"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := fmt.Errorf("%w: %s", ErrNoDispatcher, channel)
			_ = err
		}
	})
}

// BenchmarkConcurrencyOptimizations benchmarks the concurrency improvements
func BenchmarkConcurrencyOptimizations(b *testing.B) {
	// Test sync.Map vs regular map with RWMutex for subscriber access
	b.Run("SubscriberAccess_RegularMap", func(b *testing.B) {
		var mu sync.RWMutex
		subscribers := make(map[string]*uniqSubscriber)
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("sub%d", i)
			subscribers[key] = &uniqSubscriber{}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mu.RLock()
			var count int
			for _, sub := range subscribers {
				if sub != nil {
					count++
				}
			}
			mu.RUnlock()
			_ = count
		}
	})

	b.Run("SubscriberAccess_SyncMap", func(b *testing.B) {
		var subscribers sync.Map
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("sub%d", i)
			subscribers.Store(key, &uniqSubscriber{})
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var count int
			subscribers.Range(func(key, value any) bool {
				if value != nil {
					count++
				}
				return true
			})
			_ = count
		}
	})

	// Test atomic vs mutex for counters
	b.Run("Counter_WithMutex", func(b *testing.B) {
		var mu sync.Mutex
		var counter int64

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mu.Lock()
			counter++
			mu.Unlock()
		}
	})

	b.Run("Counter_WithAtomic", func(b *testing.B) {
		var counter atomic.Int64

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			counter.Add(1)
		}
	})

	// Test concurrent subscriber operations
	b.Run("ConcurrentSubscriberOps_WithLocks", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var mu sync.RWMutex
			subscribers := make(map[string]callback)
			var count int64

			var wg sync.WaitGroup
			// Simulate concurrent access
			for j := 0; j < 10; j++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					mu.Lock()
					subscribers[fmt.Sprintf("sub%d", id)] = func(any) {}
					count++
					mu.Unlock()
				}(j)
			}
			wg.Wait()
		}
	})

	b.Run("ConcurrentSubscriberOps_WithAtomic", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var mu sync.RWMutex // Still need mutex for map, but counter is atomic
			subscribers := make(map[string]callback)
			var count atomic.Int64

			var wg sync.WaitGroup
			// Simulate concurrent access
			for j := 0; j < 10; j++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					mu.Lock()
					subscribers[fmt.Sprintf("sub%d", id)] = func(any) {}
					mu.Unlock()
					count.Add(1) // Atomic increment
				}(j)
			}
			wg.Wait()
		}
	})
}

// BenchmarkWebSocketOptimizations benchmarks the WebSocket improvements
func BenchmarkWebSocketOptimizations(b *testing.B) {
	// Test message batching vs individual processing
	messages := make([]wsMessage, 100)
	for i := range messages {
		messages[i] = wsMessage{
			Channel: "trades",
			Data: []byte(
				fmt.Sprintf(
					`{"coin":"BTC","side":"B","px":"50000.%d","sz":"0.1","time":%d}`,
					i,
					time.Now().UnixNano(),
				),
			),
		}
	}

	b.Run("IndividualProcessing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, msg := range messages {
				// Simulate individual message processing
				_ = msg.Channel
				_ = msg.Data
			}
		}
	})

	b.Run("BatchProcessing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Simulate batch processing
			for j := 0; j < len(messages); j += 10 {
				end := j + 10
				if end > len(messages) {
					end = len(messages)
				}
				batch := messages[j:end]
				for _, msg := range batch {
					_ = msg.Channel
					_ = msg.Data
				}
			}
		}
	})

	// Test async vs sync callback dispatch
	b.Run("SyncCallbackDispatch", func(b *testing.B) {
		callbacks := make([]func(any), 5)
		for i := range callbacks {
			callbacks[i] = func(data any) {
				// Simulate callback processing
				time.Sleep(100 * time.Nanosecond)
			}
		}

		data := "test_data"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, cb := range callbacks {
				cb(data)
			}
		}
	})

	b.Run("AsyncCallbackDispatch", func(b *testing.B) {
		callbacks := make([]func(any), 5)
		for i := range callbacks {
			callbacks[i] = func(data any) {
				// Simulate callback processing
				time.Sleep(100 * time.Nanosecond)
			}
		}

		data := "test_data"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			for _, cb := range callbacks {
				wg.Add(1)
				go func(callback func(any), msg any) {
					defer wg.Done()
					callback(msg)
				}(cb, data)
			}
			wg.Wait()
		}
	})
}

// BenchmarkMemoryPoolingOptimizations benchmarks the memory pooling improvements
func BenchmarkMemoryPoolingOptimizations(b *testing.B) {
	// Test WebSocket subscriber slice optimization
	b.Run("SubscriberSlice_WithoutPool", func(b *testing.B) {
		mockSubscribers := make(map[string]*uniqSubscriber)
		for i := 0; i < 10; i++ {
			mockSubscribers[fmt.Sprintf("sub%d", i)] = &uniqSubscriber{}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var subscribers []*uniqSubscriber
			for _, subscriber := range mockSubscribers {
				subscribers = append(subscribers, subscriber)
			}
			_ = subscribers
		}
	})

	b.Run("SubscriberSlice_WithPool", func(b *testing.B) {
		mockSubscribers := make(map[string]*uniqSubscriber)
		for i := 0; i < 10; i++ {
			mockSubscribers[fmt.Sprintf("sub%d", i)] = &uniqSubscriber{}
		}

		pool := sync.Pool{
			New: func() any {
				slice := make([]*uniqSubscriber, 0, 16)
				return &slice
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			subscribersPtr := pool.Get().(*[]*uniqSubscriber)
			subscribers := (*subscribersPtr)[:0]
			for _, subscriber := range mockSubscribers {
				subscribers = append(subscribers, subscriber)
			}
			*subscribersPtr = (*subscribersPtr)[:0] // Reset slice
			//nolint:staticcheck // SA6002: Pool.Put is more readable this way
			pool.Put(subscribersPtr)
		}
	})

	// Test string building optimization
	b.Run("StringBuilding_Concatenation", func(b *testing.B) {
		base := "transfer_amount_123.45"
		vault := "vault_address_abc123"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := base + " subaccount:" + vault
			_ = result
		}
	})

	b.Run("StringBuilding_WithPool", func(b *testing.B) {
		base := "transfer_amount_123.45"
		vault := "vault_address_abc123"

		pool := sync.Pool{
			New: func() any {
				return &strings.Builder{}
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			builder := pool.Get().(*strings.Builder)
			builder.Reset()
			builder.WriteString(base)
			builder.WriteString(" subaccount:")
			builder.WriteString(vault)
			result := builder.String()
			pool.Put(builder)
			_ = result
		}
	})
}
