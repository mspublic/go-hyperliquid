package hyperliquid

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/valyala/fastjson"
)

// BenchmarkWebSocketMessageProcessing benchmarks the WebSocket message processing
// with and without object pooling
func BenchmarkWebSocketMessageProcessing(b *testing.B) {
	sampleMessage := []byte(`{"channel":"trades","data":{"coin":"BTC","side":"B","px":"50000.0","sz":"0.1","time":1640995200000,"hash":"abc123","tid":12345,"users":["user1","user2"]}}`)

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
