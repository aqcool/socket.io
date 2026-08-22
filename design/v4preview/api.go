package v4preview

import (
	"context"
	"errors"
	"log/slog"
	"net"
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
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func (e *ConnectError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

var (
	ErrClosed       = errors.New("socketio: closed")
	ErrNotConnected = errors.New("socketio: not connected")
	ErrQueueFull    = errors.New("socketio: event queue full")
	ErrUnsupported  = errors.New("socketio: unsupported")
	ErrInvalidEvent = errors.New("socketio: invalid event")
)

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
	DecodeValue(src any, dst any) error
}

type Ack func([]any, error)

type Middleware func(context.Context, Socket) error

type OverflowPolicy uint8

const (
	OverflowDisconnect OverflowPolicy = iota
	OverflowDropNewest
	OverflowReject
)

type QueueOptions struct {
	MaxPending int
	Overflow   OverflowPolicy
}

type RecoveryOptions struct {
	MaxDisconnectionDuration time.Duration
	SkipMiddleware           bool
	CleanupInterval          time.Duration
}

type Config struct {
	Path                        string
	ServeClient                 bool
	ConnectTimeout              time.Duration
	CleanupEmptyChildNamespaces bool
	Queue                       QueueOptions
	Recovery                    *RecoveryOptions
	Logger                      *slog.Logger
	Adapter                     AdapterFactory
	PacketCodec                 PacketCodec
	ValueCodec                  ValueCodec
}

type Option func(*Config) error

type Server interface {
	RawEmitter
	RawRegistrar

	Handler() http.Handler
	Serve(net.Listener) error
	ListenAndServe(string) error
	Shutdown(context.Context) error
	Close() error
	Done() <-chan struct{}

	Use(...Middleware)
	OnConnection(func(Socket)) Subscription

	Of(string) Namespace
	Namespace(string) (Namespace, bool)
	Namespaces() []Namespace
	OfMatch(NamespaceMatcher) ParentNamespace

	To(...Room) BroadcastOperator
	In(...Room) BroadcastOperator
	Except(...Room) BroadcastOperator
}

type NamespaceMatcher func(context.Context, string, map[string]any) (bool, error)

type Namespace interface {
	RawEmitter
	RawRegistrar

	Name() string
	Use(...Middleware)
	OnConnection(func(Socket)) Subscription

	To(...Room) BroadcastOperator
	In(...Room) BroadcastOperator
	Except(...Room) BroadcastOperator

	FetchSockets(context.Context) ([]RemoteSocket, error)
	CountSockets(context.Context) (uint64, error)
	ListRooms(context.Context) (map[Room]uint64, error)
	SocketsJoin(context.Context, ...Room) error
	SocketsLeave(context.Context, ...Room) error
	DisconnectSockets(context.Context, bool) error

	ServerSideEmit(context.Context, string, ...any) error
	ServerSideEmitAck(context.Context, string, ...any) ([]any, error)
}

type ParentNamespace interface {
	Namespace
	Children() []Namespace
}

type Socket interface {
	RawEmitter
	RawRegistrar
	ValueDecoder

	ID() SocketID
	Context() context.Context
	Handshake() Handshake
	Recovered() bool
	Connected() bool
	Rooms() []Room

	Join(context.Context, ...Room) error
	Leave(context.Context, ...Room) error
	Disconnect(bool) error

	Set(string, any)
	Get(string) (any, bool)
	Delete(string)

	To(...Room) BroadcastOperator
	In(...Room) BroadcastOperator
	Except(...Room) BroadcastOperator
	Broadcast() BroadcastOperator
	Local() BroadcastOperator
	Volatile() SocketOperator
	Compress(bool) SocketOperator
	Timeout(time.Duration) SocketOperator
}

type SocketOperator interface {
	RawEmitter
	Volatile() SocketOperator
	Compress(bool) SocketOperator
	Timeout(time.Duration) SocketOperator
}

type BroadcastOperator interface {
	RawEmitter
	ValueDecoder

	To(...Room) BroadcastOperator
	In(...Room) BroadcastOperator
	Except(...Room) BroadcastOperator
	Local() BroadcastOperator
	Volatile() BroadcastOperator
	Compress(bool) BroadcastOperator
	Timeout(time.Duration) BroadcastOperator

	FetchSockets(context.Context) ([]RemoteSocket, error)
	CountSockets(context.Context) (uint64, error)
	ListRooms(context.Context) (map[Room]uint64, error)
	SocketsJoin(context.Context, ...Room) error
	SocketsLeave(context.Context, ...Room) error
	DisconnectSockets(context.Context, bool) error
}

type RemoteSocket interface {
	ID() SocketID
	Rooms() []Room
	Data() any
	Join(context.Context, ...Room) error
	Leave(context.Context, ...Room) error
	Disconnect(context.Context, bool) error
	Emit(context.Context, string, ...any) error
}

type Packet struct {
	Type      uint8
	Namespace string
	ID        *uint64
	Data      any
}

type PacketEncoder interface {
	Encode(Packet) ([][]byte, error)
}

type PacketDecoder interface {
	Add([]byte) error
	Close() error
}

type PacketCodec interface {
	NewEncoder() PacketEncoder
	NewDecoder() PacketDecoder
}

type ValueCodec interface {
	Decode(src any, dst any) error
}
