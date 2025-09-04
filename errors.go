package hyperliquid

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

//go:generate easyjson -all

// Pre-defined errors to reduce allocations
var (
	ErrCallbackNil         = errors.New("callback cannot be nil")
	ErrConnectionClosed    = errors.New("connection closed")
	ErrInvalidMessageType  = errors.New("invalid message type")
	ErrNoDispatcher        = errors.New("no dispatcher for channel")
	ErrMissingResponseData = errors.New("missing response.data field in successful response")
	ErrInvalidArrayLength  = errors.New("expected array of length 2")
	ErrPositionNotFound    = errors.New("position not found")
	ErrNoOrderStatus       = errors.New("no status for order")
	ErrFloatRounding       = errors.New("float_to_wire causes rounding")
)

// errorBuilderPool reduces allocations for error message construction
var errorBuilderPool = sync.Pool{
	New: func() any {
		return &strings.Builder{}
	},
}

// WrapError efficiently wraps an error with context using pooled string builder
func WrapError(base error, context string) error {
	if base == nil {
		return nil
	}

	builder := errorBuilderPool.Get().(*strings.Builder)
	builder.Reset()
	defer errorBuilderPool.Put(builder)

	builder.WriteString(context)
	builder.WriteString(": ")
	builder.WriteString(base.Error())

	return errors.New(builder.String())
}

type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"msg"`
	Data    any    `json:"data,omitempty"`
}

func (e APIError) Error() string {
	return fmt.Sprintf("API error %d: %s", e.Code, e.Message)
}

type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error on field %s: %s", e.Field, e.Message)
}
