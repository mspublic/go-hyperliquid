// Package hyperliquid provides a Go client library for the Hyperliquid exchange API.
// It includes support for both REST API and WebSocket connections, allowing users to
// access market data, manage orders, and handle user account operations.
package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/sonirico/vago/lol"
)

const (
	MainnetAPIURL = "https://api.hyperliquid.xyz"
	TestnetAPIURL = "https://api.hyperliquid-testnet.xyz"
	LocalAPIURL   = "http://localhost:3001"

	// httpErrorStatusCode is the minimum status code considered an error
	httpErrorStatusCode = 400

	// HTTP client configuration constants
	defaultBufferCapacity          = 4096        // 4KB for request buffers
	defaultResponseCapacity        = 8192        // 8KB for response buffers
	maxResponseSizeForOptimization = 1024 * 1024 // 1MB max for pre-allocation
)

// bufferPool reduces allocations for HTTP request/response buffers
var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, defaultBufferCapacity))
	},
}

// responseBufferPool for different response sizes
var responseBufferPool = sync.Pool{
	New: func() any {
		slice := make([]byte, 0, defaultResponseCapacity)
		return &slice
	},
}

type Client struct {
	logger     lol.Logger
	debug      bool
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new HTTP client for the Hyperliquid API.
// The client is configured with sensible defaults and can be customized using ClientOpt functions.
//
// Parameters:
//   - baseURL: The base URL for the API (use MainnetAPIURL, TestnetAPIURL, or custom URL)
//   - opts: Optional configuration functions to customize the client behavior
//
// Returns a configured Client ready for API operations.
func NewClient(baseURL string, opts ...ClientOpt) *Client {
	if baseURL == "" {
		baseURL = MainnetAPIURL
	}

	cli := &Client{
		baseURL:    baseURL,
		httpClient: new(http.Client),
	}

	for _, opt := range opts {
		opt.Apply(cli)
	}

	return cli
}

func (c *Client) post(path string, payload any) ([]byte, error) {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Use buffer pool for request body
	buffer := bufferPool.Get().(*bytes.Buffer)
	buffer.Reset()
	defer bufferPool.Put(buffer)
	buffer.Write(jsonData)

	url := c.baseURL + path
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		url,
		buffer,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	if c.debug {
		c.logger.WithFields(lol.Fields{
			"method": "POST",
			"url":    url,
			"body":   string(jsonData),
		}).Debug("HTTP request")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	var body []byte
	if resp.Body != nil {
		// Use optimized reading based on Content-Length
		if resp.ContentLength > 0 && resp.ContentLength < maxResponseSizeForOptimization {
			// Pre-allocate exact size and read directly
			body = make([]byte, resp.ContentLength)
			_, err = io.ReadFull(resp.Body, body)
			if err != nil {
				return nil, fmt.Errorf("failed to read response body: %w", err)
			}
		} else {
			// Use pooled buffer for unknown sizes
			bufferPtr := responseBufferPool.Get().(*[]byte)
			buffer := (*bufferPtr)[:0] // Reset length
			//nolint:staticcheck // SA6002: Pool.Put is more readable this way
			defer responseBufferPool.Put(bufferPtr)

			// Read in chunks to reuse buffer
			chunk := make([]byte, 4096)
			for {
				n, err := resp.Body.Read(chunk)
				if n > 0 {
					buffer = append(buffer, chunk[:n]...)
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, fmt.Errorf("failed to read response body: %w", err)
				}
			}

			// Copy to final result
			body = make([]byte, len(buffer))
			copy(body, buffer)
		}
	}

	if c.debug {
		c.logger.WithFields(lol.Fields{
			"status": resp.Status,
			"body":   string(body),
		}).Debug("HTTP response")
	}

	if resp.StatusCode >= httpErrorStatusCode {
		if !json.Valid(body) {
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
		}
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err != nil {
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
		}
		return nil, apiErr
	}

	return body, nil
}
