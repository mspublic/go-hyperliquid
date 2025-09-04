package hyperliquid

import (
	"testing"
	"time"
)

// TestConfigurationValidation tests that configuration options properly validate input
func TestConfigurationValidation(t *testing.T) {
	t.Run("WsOptBatchSize validation", func(t *testing.T) {
		ws := NewWebsocketClient(TestnetAPIURL)
		originalBatchSize := ws.batchSize

		// Test valid value
		WsOptBatchSize(50).Apply(ws)
		if ws.batchSize != 50 {
			t.Errorf("Expected batch size 50, got %d", ws.batchSize)
		}

		// Test invalid value (too large)
		WsOptBatchSize(MaxBatchSize + 1).Apply(ws)
		if ws.batchSize != 50 { // Should remain unchanged
			t.Errorf("Batch size should not change for invalid input, got %d", ws.batchSize)
		}

		// Test invalid value (negative)
		WsOptBatchSize(-1).Apply(ws)
		if ws.batchSize != 50 { // Should remain unchanged
			t.Errorf("Batch size should not change for negative input, got %d", ws.batchSize)
		}

		// Reset for other tests
		ws.batchSize = originalBatchSize
	})

	t.Run("WsOptBatchTimeout validation", func(t *testing.T) {
		ws := NewWebsocketClient(TestnetAPIURL)
		originalTimeout := ws.batchTimeout

		// Test valid value
		WsOptBatchTimeout(10 * time.Millisecond).Apply(ws)
		if ws.batchTimeout != 10*time.Millisecond {
			t.Errorf("Expected timeout 10ms, got %v", ws.batchTimeout)
		}

		// Test invalid value (too large)
		WsOptBatchTimeout(MaxBatchTimeout + time.Second).Apply(ws)
		if ws.batchTimeout != 10*time.Millisecond { // Should remain unchanged
			t.Errorf("Timeout should not change for invalid input, got %v", ws.batchTimeout)
		}

		// Reset for other tests
		ws.batchTimeout = originalTimeout
	})

	t.Run("WsOptMaxReconnectAttempts validation", func(t *testing.T) {
		ws := NewWebsocketClient(TestnetAPIURL)
		originalAttempts := ws.maxReconnectAttempts

		// Test valid value
		WsOptMaxReconnectAttempts(20).Apply(ws)
		if ws.maxReconnectAttempts != 20 {
			t.Errorf("Expected max attempts 20, got %d", ws.maxReconnectAttempts)
		}

		// Test invalid value (too large)
		WsOptMaxReconnectAttempts(MaxReconnectAttempts + 1).Apply(ws)
		if ws.maxReconnectAttempts != 20 { // Should remain unchanged
			t.Errorf("Max attempts should not change for invalid input, got %d", ws.maxReconnectAttempts)
		}

		// Reset for other tests
		ws.maxReconnectAttempts = originalAttempts
	})
}
