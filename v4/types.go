package socketio

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

type SocketID string
type PrivateSessionID string
type Room string
type DisconnectReason string

type Handshake struct {
	Headers http.Header
	Time    time.Time
	Address string
	Secure  bool
	URL     *url.URL
	Auth    map[string]any
}

type ConnectError struct {
	Message string
	Data    any
	Err     error
}

func (e *ConnectError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Message
	}
	if e.Message == "" {
		return e.Err.Error()
	}
	return e.Message + ": " + e.Err.Error()
}

func (e *ConnectError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type Listener func(context.Context, ...any) error

type Subscription interface {
	Close() error
}

type RawEmitter interface {
	Emit(string, ...any) error
}

type RawRegistrar interface {
	On(string, Listener) Subscription
	Once(string, Listener) Subscription
}

type ValueDecoder interface {
	DecodeValue(any, any) error
}

type Ack func([]any, error)

type Middleware func(context.Context, *Socket) error

type NamespaceMatcher func(context.Context, string, map[string]any) (bool, error)
