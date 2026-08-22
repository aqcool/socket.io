# Socket.IO Go v4 API Design

Status: **proposed API freeze candidate**  
Reference behavior: Socket.IO Node.js 4.8.x  
Target module path: `github.com/aqcool/socket.io/v4`

This document defines the public API and architectural boundaries for v4. It is intentionally stricter than v3: protocol behavior remains compatible with upstream Socket.IO, while Go-facing APIs adopt Go lifecycle, error, context and concurrency semantics.

## 1. Goals

v4 MUST:

1. Preserve Socket.IO wire compatibility and the upstream object model: Server -> Namespace -> Socket -> Adapter -> Engine.IO.
2. Be safe when public APIs are called from multiple goroutines unless a method explicitly documents otherwise.
3. Use `context.Context` for cancellation/deadlines at I/O and request-operation boundaries.
4. Return errors from operations that can fail; never use a background goroutine panic as the primary error path.
5. Make modifier chains immutable (`To`, `Except`, `Timeout`, `Volatile`, `Compress`, `Local`).
6. Make typed events a first-class root-package feature, not an `any`-based runtime wrapper.
7. Give every long-lived resource an explicit lifecycle (`Close`, `Shutdown`, context cancellation).
8. Keep dynamic events available for full Socket.IO compatibility.
9. Keep distributed adapters isolated from the core dependency graph.
10. Make v3 -> v4 migration mechanical and documentable.

v4 MUST NOT:

- change the Socket.IO or Engine.IO wire protocol;
- rewrite proven transport-upgrade logic merely for style;
- require schema/code generation for ordinary use;
- hide errors to mimic Node.js callbacks;
- promise JavaScript function identity semantics for Go functions.

## 2. Public package layout

Recommended release layout:

```text
module github.com/aqcool/socket.io/v4

socketio (root package)
  Server
  Namespace
  Socket
  BroadcastOperator
  Event[TReq, TAck]
  typed helpers
  middleware
  lifecycle/context

engineio/
  Server
  Socket
  Transport
  Options

engineio/parser/
  packet and payload codec

parser/
  Socket.IO packet codec

adapter/
  Adapter interface
  Memory adapter
  cluster contracts

contract/
  schema description
  Go/TypeScript generator-facing model

internal/
  implementation-only helpers
```

Provider-specific integrations stay as independent modules so users do not inherit their dependencies:

```text
github.com/aqcool/socket.io/adapters/redis/v4
github.com/aqcool/socket.io/adapters/valkey/v4
github.com/aqcool/socket.io/adapters/postgres/v4
github.com/aqcool/socket.io/adapters/mongo/v4
github.com/aqcool/socket.io/adapters/nats/v4
github.com/aqcool/socket.io/adapters/kafka/v4
github.com/aqcool/socket.io/adapters/amqp/v4
```

`typed` SHOULD NOT remain a separate runtime module in v4. Typed events belong to the root `socketio` API. A generator may live in `cmd/socketio-gen` or consume `contract/`.

## 3. Core scalar types

```go
type SocketID string
type PrivateSessionID string
type Room string

type DisconnectReason string
```

Keep `Room` as `string` for upstream compatibility.

`Handshake` is immutable after connection:

```go
type Handshake struct {
    Headers http.Header
    Time    time.Time
    Address string
    Secure  bool
    URL     *url.URL
    Auth    map[string]any
}
```

Differences from v3:

- prefer `time.Time` over duplicated string + Unix millisecond fields;
- prefer `http.Header` and `url.URL` over JavaScript-shaped compatibility maps;
- retain `Auth map[string]any` because authentication payload is protocol-dynamic.

## 4. Server construction and options

Primary constructor:

```go
func New(opts ...Option) (*Server, error)
```

Options use functional options for validation and dependency injection:

```go
type Option func(*Config) error

func WithPath(path string) Option
func WithClientServing(enabled bool) Option
func WithConnectTimeout(d time.Duration) Option
func WithAdapter(factory AdapterFactory) Option
func WithParser(codec PacketCodec) Option
func WithConnectionStateRecovery(opts RecoveryOptions) Option
func WithCleanupEmptyChildNamespaces(enabled bool) Option
func WithQueue(opts QueueOptions) Option
func WithLogger(logger *slog.Logger) Option
```

`Config` may be exported as a read-only snapshot for inspection, but mutating setters such as `SetPath`, `SetParser`, `SetAdapter`, `GetRawXxx`, `Assign` SHOULD NOT be part of the normal v4 public API.

Example:

```go
io, err := socketio.New(
    socketio.WithPath("/socket.io"),
    socketio.WithConnectTimeout(45*time.Second),
    socketio.WithQueue(socketio.QueueOptions{
        MaxPending: 4096,
        Overflow:   socketio.OverflowDisconnect,
    }),
)
if err != nil {
    return err
}
```

## 5. Server lifecycle

The primary lifecycle is Go-native:

```go
func (s *Server) Handler() http.Handler
func (s *Server) Serve(l net.Listener) error
func (s *Server) ListenAndServe(addr string) error
func (s *Server) Shutdown(ctx context.Context) error
func (s *Server) Close() error
func (s *Server) Done() <-chan struct{}
```

Rules:

- `Serve` blocks like `http.Server.Serve`.
- bind/listen errors are returned synchronously.
- normal shutdown is normalized consistently.
- `Shutdown(ctx)` stops accepting new connections, drains/terminates active Socket.IO resources and closes adapters.
- all Socket lifetime contexts are cancelled before `Shutdown` returns.
- compatibility convenience APIs may exist, but MUST be implemented on top of this lifecycle.

HTTP integration:

```go
mux := http.NewServeMux()
mux.Handle("/socket.io/", io.Handler())

srv := &http.Server{Addr: ":3000", Handler: mux}
```

The library MUST work as an ordinary `http.Handler`; it must not require ownership of the user's HTTP server.

## 6. Context model

Each connected Socket owns a lifetime context:

```go
func (s *Socket) Context() context.Context
```

The context is cancelled when:

- the Socket disconnects;
- the owning namespace is forcibly closed;
- server shutdown begins;
- the underlying Engine.IO connection is terminated.

Middleware and typed event handlers derive from this context. Per-operation APIs may additionally accept a caller context for shorter deadlines.

Do not store `context.Context` inside global config. Socket context is lifecycle state, not configuration.

## 7. Connection middleware

Replace Node callback-style middleware with ordinary Go error returns:

```go
type Middleware func(context.Context, *Socket) error

func (s *Server) Use(mw ...Middleware)
func (n *Namespace) Use(mw ...Middleware)
```

Example:

```go
io.Use(func(ctx context.Context, socket *socketio.Socket) error {
    user, err := auth.Authenticate(ctx, socket.Handshake().Auth)
    if err != nil {
        return err
    }
    socket.Set("user", user)
    return nil
})
```

Middleware execution is sequential and receives a snapshot of the chain. A middleware that blocks can be cancelled by the Socket lifetime context.

Connection errors sent to clients use a typed error wrapper:

```go
type ConnectError struct {
    Message string
    Data    any
    Err     error
}
```

## 8. Connection handlers

Reserved connection events get explicit APIs:

```go
func (s *Server) OnConnection(fn func(*Socket)) Subscription
func (n *Namespace) OnConnection(fn func(*Socket)) Subscription
```

This is preferred over registering `"connection"` through the dynamic event emitter.

Dynamic compatibility remains available where required:

```go
func (s *Server) On(event string, fn Listener) Subscription
```

## 9. Listener identity and Subscription

Go functions do not have JavaScript function-object identity. v4 MUST NOT identify a listener solely by `reflect.ValueOf(fn).Pointer()`.

Registration returns an exact token:

```go
type Subscription interface {
    Close() error
}

func (s *Socket) On(event string, fn Listener) Subscription
func (s *Socket) Once(event string, fn Listener) Subscription
```

`Close` is idempotent.

Bulk removal is allowed:

```go
func (s *Socket) RemoveAllListeners(event string)
```

Function-based `Off(event, fn)` MAY exist only as best-effort compatibility and SHOULD be deprecated in favor of `Subscription`.

## 10. Raw event API

Dynamic events remain necessary for full Socket.IO compatibility:

```go
type Listener func(context.Context, ...any) error

type RawEmitter interface {
    Emit(event string, args ...any) error
}

type RawRegistrar interface {
    On(event string, listener Listener) Subscription
    Once(event string, listener Listener) Subscription
}
```

For `Socket`, `Namespace`, `Server`, and `BroadcastOperator`, `Emit` SHOULD use a consistent `error` return rather than different fluent return types.

Convenience aliases like `Send`/`Write` can remain, but they SHOULD return `error`.

## 11. Typed events

Typed events are root-package values:

```go
type Event[Request, Response any] struct {
    name string
}

func NewEvent[Request, Response any](name string) Event[Request, Response]
func (e Event[Request, Response]) Name() string
```

No-ACK events use `struct{}` or a dedicated alias:

```go
type NoAck = struct{}
```

Example contract:

```go
type SendMessageRequest struct {
    Room    string `json:"room"`
    Message string `json:"message"`
}

type SendMessageResponse struct {
    ID string `json:"id"`
}

var SendMessage = socketio.NewEvent[
    SendMessageRequest,
    SendMessageResponse,
]("message:send")
```

Registration:

```go
sub := socketio.Handle(
    socket,
    SendMessage,
    func(ctx context.Context, req SendMessageRequest) (SendMessageResponse, error) {
        return service.Send(ctx, req)
    },
)
defer sub.Close()
```

No-ACK handler:

```go
sub := socketio.On(
    socket,
    Typing,
    func(ctx context.Context, req TypingRequest) error {
        return service.Typing(ctx, req)
    },
)
```

Typed ACK emission:

```go
resp, err := socketio.EmitAck(ctx, socket, SendMessage, req)
```

Typed fire-and-forget emission:

```go
err := socketio.Emit(socket, Typing, req)
```

### Typed implementation rule

The public typed API MUST depend on narrow contracts, not `any` + runtime method-shape switching:

```go
type RawEmitter interface {
    Emit(string, ...any) error
}

type RawRegistrar interface {
    On(string, Listener) Subscription
}
```

The default decoder should fast-path direct type assertions. Conversion fallback is delegated to a configurable codec; typed helpers MUST NOT hard-code a JSON marshal/unmarshal round-trip as the only conversion strategy.

## 12. Codec contracts

Socket.IO packet coding and application-value coding are separate concerns.

Protocol packet codec:

```go
type PacketCodec interface {
    NewEncoder() PacketEncoder
    NewDecoder() PacketDecoder
}
```

Typed application values:

```go
type ValueCodec interface {
    Decode(src any, dst any) error
}
```

Default behavior:

1. direct Go type assertion;
2. protocol-native representation conversion;
3. configured `ValueCodec` fallback.

JSON can be the default fallback, but it is not embedded into the generic API contract.

## 13. Socket API

Proposed core surface:

```go
type Socket struct { /* unexported state */ }

func (s *Socket) ID() SocketID
func (s *Socket) Context() context.Context
func (s *Socket) Handshake() Handshake
func (s *Socket) Recovered() bool
func (s *Socket) Connected() bool
func (s *Socket) Rooms() []Room

func (s *Socket) Join(ctx context.Context, rooms ...Room) error
func (s *Socket) Leave(ctx context.Context, rooms ...Room) error
func (s *Socket) Disconnect(closeTransport bool) error

func (s *Socket) Emit(event string, args ...any) error
func (s *Socket) On(event string, fn Listener) Subscription
func (s *Socket) Once(event string, fn Listener) Subscription

func (s *Socket) To(rooms ...Room) *BroadcastOperator
func (s *Socket) Except(rooms ...Room) *BroadcastOperator
func (s *Socket) Broadcast() *BroadcastOperator
func (s *Socket) Local() *BroadcastOperator
func (s *Socket) Volatile() *SocketOperator
func (s *Socket) Compress(enabled bool) *SocketOperator
func (s *Socket) Timeout(d time.Duration) *SocketOperator
```

Socket application data SHOULD use a concurrency-safe typed key/value bag or explicit user-owned state. Avoid `atomic.Pointer[any]` as the only public model.

Minimal generic-free API:

```go
func (s *Socket) Set(key string, value any)
func (s *Socket) Get(key string) (any, bool)
func (s *Socket) Delete(key string)
```

Typed applications should keep domain state outside the transport object when practical.

## 14. Immutable SocketOperator

`Volatile`, `Compress`, and `Timeout` MUST NOT mutate shared transient flags on `Socket`.

```go
type SocketOperator struct {
    socket *Socket
    flags  BroadcastFlags
}

func (o *SocketOperator) Volatile() *SocketOperator
func (o *SocketOperator) Compress(bool) *SocketOperator
func (o *SocketOperator) Timeout(time.Duration) *SocketOperator
func (o *SocketOperator) Emit(string, ...any) error
```

Every modifier returns a new immutable snapshot or a logically immutable copy.

This makes the following safe:

```go
go socket.Volatile().Emit("telemetry", a)
go socket.Timeout(time.Second).Emit("command", b)
```

## 15. BroadcastOperator

Broadcast operators are immutable value/snapshot builders:

```go
type BroadcastOperator struct { /* immutable snapshot */ }

func (o *BroadcastOperator) To(...Room) *BroadcastOperator
func (o *BroadcastOperator) In(...Room) *BroadcastOperator
func (o *BroadcastOperator) Except(...Room) *BroadcastOperator
func (o *BroadcastOperator) Local() *BroadcastOperator
func (o *BroadcastOperator) Volatile() *BroadcastOperator
func (o *BroadcastOperator) Compress(bool) *BroadcastOperator
func (o *BroadcastOperator) Timeout(time.Duration) *BroadcastOperator

func (o *BroadcastOperator) Emit(string, ...any) error
func (o *BroadcastOperator) FetchSockets(context.Context) ([]RemoteSocket, error)
func (o *BroadcastOperator) CountSockets(context.Context) (uint64, error)
func (o *BroadcastOperator) ListRooms(context.Context) (map[Room]uint64, error)
func (o *BroadcastOperator) SocketsJoin(context.Context, ...Room) error
func (o *BroadcastOperator) SocketsLeave(context.Context, ...Room) error
func (o *BroadcastOperator) DisconnectSockets(context.Context, bool) error
```

No callback-returning functions such as `func(func([]RemoteSocket, error))` in the v4 primary API.

## 16. Namespace API

`Namespace` should become a concrete type unless third-party implementations are a real supported extension point. Prefer small interfaces at consumer boundaries rather than one huge public interface.

```go
type Namespace struct { /* unexported */ }

func (n *Namespace) Name() string
func (n *Namespace) Use(...Middleware)
func (n *Namespace) OnConnection(func(*Socket)) Subscription
func (n *Namespace) Emit(string, ...any) error
func (n *Namespace) To(...Room) *BroadcastOperator
func (n *Namespace) In(...Room) *BroadcastOperator
func (n *Namespace) Except(...Room) *BroadcastOperator
func (n *Namespace) FetchSockets(context.Context) ([]RemoteSocket, error)
func (n *Namespace) CountSockets(context.Context) (uint64, error)
func (n *Namespace) ListRooms(context.Context) (map[Room]uint64, error)
```

Internal prototype/constructor machinery (`Prototype`, `Proto`, `Construct`, `InitAdapter`) is not public API in v4.

Namespace creation on `Server.Of()` MUST be atomic. Concurrent calls for the same name return the same Namespace instance.

Pre-connect sockets are explicitly tracked until middleware completes or the transport closes.

## 17. Server namespace API

```go
func (s *Server) Of(name string) *Namespace
func (s *Server) Namespace(name string) (*Namespace, bool)
func (s *Server) Namespaces() []*Namespace
```

Dynamic namespace support:

```go
type NamespaceMatcher func(context.Context, string, map[string]any) (bool, error)

func (s *Server) OfMatch(matcher NamespaceMatcher) *ParentNamespace
```

Do not expose callback-shaped `ParentNspNameMatchFn` as the primary Go API.

## 18. Adapter v4 contract

The Adapter API is a deliberate v4 breaking change.

```go
type Adapter interface {
    Init(context.Context) error
    Close() error

    Capabilities() AdapterCapabilities
    ServerCount(context.Context) (int64, error)

    AddAll(context.Context, SocketID, ...Room) error
    Delete(context.Context, SocketID, Room) error
    DeleteAll(context.Context, SocketID) error
    SocketRooms(context.Context, SocketID) ([]Room, error)

    Broadcast(context.Context, Packet, BroadcastOptions) error
    BroadcastWithAck(
        context.Context,
        Packet,
        BroadcastOptions,
        func(clientCount uint64),
        func(args []any, err error),
    ) error

    FetchSockets(context.Context, BroadcastOptions) ([]SocketDetails, error)
    CountSockets(context.Context, BroadcastOptions) (uint64, error)
    ListRooms(context.Context, BroadcastOptions) (map[Room]uint64, error)

    AddSockets(context.Context, BroadcastOptions, ...Room) error
    DeleteSockets(context.Context, BroadcastOptions, ...Room) error
    DisconnectSockets(context.Context, BroadcastOptions, bool) error

    ServerSideEmit(context.Context, []any) error

    PersistSession(context.Context, Session) error
    RestoreSession(context.Context, PrivateSessionID, string) (*RecoveredSession, error)
}
```

`AdapterFactory`:

```go
type AdapterFactory interface {
    New(*Namespace) (Adapter, error)
}
```

No `Prototype`, `Proto`, `Construct`, JS-style getters or public internal maps are required.

Optional features remain explicit through `AdapterCapabilities`. Unsupported operations return a documented sentinel such as `ErrUnsupported`.

## 19. Queue and backpressure

Per-Socket ordered dispatch remains useful to preserve event ordering, but the queue MUST be bounded by policy.

```go
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
```

Requirements:

- no unbounded memory growth under a slow event handler;
- queue shutdown is explicit and tied to Socket lifetime;
- pre-connect sockets do not leave permanent queue goroutines;
- queue metrics expose pending depth and overflow count;
- a zero/negative `MaxPending` must have a documented meaning; recommended default is bounded, not unlimited.

## 20. ACK model

Dynamic compatibility ACK remains:

```go
type Ack func([]any, error)
```

Go-native typed ACK uses return values:

```go
socketio.Handle(socket, Event, func(ctx context.Context, req Req) (Resp, error) {
    ...
})

resp, err := socketio.EmitAck(ctx, socket, Event, req)
```

Timeout is provided by caller context or immutable operator timeout. The implementation must ensure an ACK callback is completed at most once and cleaned up on disconnect.

## 21. Server-side events

```go
func (n *Namespace) ServerSideEmit(ctx context.Context, event string, args ...any) error
func (n *Namespace) ServerSideEmitAck(ctx context.Context, event string, args ...any) ([]any, error)
```

Typed helpers should support the same `Event[Req, Resp]` contract.

## 22. Error model

Export sentinel errors where callers need branching:

```go
var (
    ErrClosed       = errors.New("socketio: closed")
    ErrNotConnected = errors.New("socketio: not connected")
    ErrQueueFull    = errors.New("socketio: event queue full")
    ErrUnsupported  = errors.New("socketio: unsupported")
    ErrInvalidEvent = errors.New("socketio: invalid event")
)
```

Use wrapping (`fmt.Errorf("...: %w", err)`) so `errors.Is`/`errors.As` work.

Do not use panic for normal network/configuration failures.

## 23. Concurrency contract

The following are safe for concurrent use:

- `Server.Of`, `Namespace`, `Emit`, `Shutdown`;
- Socket `Emit`, room operations and modifier creation;
- listener registration/removal;
- BroadcastOperator methods;
- adapter public methods unless a provider explicitly documents a stronger restriction.

Guarantees:

- same-namespace creation is single-instance;
- transient emission flags never live in shared mutable Socket state;
- listener tokens identify registrations, not code addresses;
- disconnect/close events fire once;
- Socket context cancellation happens once;
- ACK completion happens at most once.

## 24. Compatibility layer

v4 should preserve protocol compatibility, not every v3 Go surface artifact.

A small `compat` package MAY be provided for migration helpers, but the root package should not keep `Prototype/Proto/Construct`, `GetRawXxx`, callback-returning fetch APIs, or mutable transient Socket flags merely because v3 exposed them.

Node-compatible naming (`On`, `Emit`, `Of`, `To`, `Except`, `Volatile`) remains where it maps naturally to Go.

## 25. Node.js 4.8.x behavior mapping

Keep behavior:

- Server / Namespace / Socket object hierarchy;
- namespace middleware order;
- room membership semantics;
- reserved event restrictions;
- ACK and timeout semantics;
- connection-state recovery wire behavior;
- parser and binary packet behavior;
- polling/WebSocket/WebTransport protocol behavior;
- adapter broadcast semantics;
- dynamic namespaces.

Do not copy implementation assumptions:

- single-threaded mutation of Socket flags;
- JavaScript function-object identity;
- Promise/callback APIs where Go can return `(T, error)`;
- `process.nextTick` as a lifecycle primitive;
- public prototype emulation;
- implicit background server-start errors.

## 26. Release/module policy

Core protocol modules may version together at v4 for the first v4 release. Provider adapters should be independently releasable afterward.

A module should exist only if at least one is true:

1. users import it independently;
2. it isolates a meaningful third-party dependency graph;
3. it follows an externally versioned protocol/component;
4. independent release cadence has real value.

Internal helper directories should not become modules solely to mirror npm workspaces.

Recommended core set:

```text
github.com/aqcool/socket.io/v4
github.com/aqcool/socket.io/engine.io/v4        (if separate import remains valuable)
github.com/aqcool/socket.io/parser/v4           (only if independently imported/versioned)
```

Final module boundaries should be verified with actual downstream imports before v4 freeze.

## 27. Observability

Use interfaces/hooks that do not force OpenTelemetry/Prometheus into the core dependency graph.

Socket lifetime context is the trace propagation anchor. Adapter and typed operations accept context so tracing can propagate naturally.

Provider-specific metrics remain in optional modules/packages.

## 28. Security and resource limits

v4 defaults must be production-safe:

- bounded event execution queue;
- maximum HTTP payload inherited from Engine.IO config;
- configurable handshake/middleware timeout;
- explicit origin/CORS policy;
- rate/guard hooks must not require replacing protocol internals;
- server shutdown must close timers, adapter workers and queue goroutines.

## 29. Recommended end-user API

```go
package main

import (
    "context"
    "log"
    "time"

    socketio "github.com/aqcool/socket.io/v4"
)

type SendRequest struct {
    Room string `json:"room"`
    Text string `json:"text"`
}

type SendResponse struct {
    ID string `json:"id"`
}

var Send = socketio.NewEvent[SendRequest, SendResponse]("message:send")

func main() {
    io, err := socketio.New(
        socketio.WithConnectTimeout(45*time.Second),
        socketio.WithQueue(socketio.QueueOptions{
            MaxPending: 4096,
            Overflow:   socketio.OverflowDisconnect,
        }),
    )
    if err != nil {
        log.Fatal(err)
    }

    io.Use(func(ctx context.Context, s *socketio.Socket) error {
        return nil
    })

    io.OnConnection(func(s *socketio.Socket) {
        socketio.Handle(s, Send, func(ctx context.Context, req SendRequest) (SendResponse, error) {
            return SendResponse{ID: "example"}, nil
        })
    })

    if err := io.ListenAndServe(":3000"); err != nil {
        log.Fatal(err)
    }
}
```

## 30. Implementation sequence

Implement v4 in this order:

1. Freeze narrow interfaces (`RawEmitter`, `RawRegistrar`, `Subscription`, lifecycle).
2. Introduce Socket lifetime context and exact listener tokens.
3. Replace mutable Socket flags with immutable `SocketOperator`.
4. Make Namespace creation and pre-connect tracking lifecycle-safe.
5. Bound the per-Socket ordered queue.
6. Replace callback-returning Namespace/Broadcast operations with `(T, error)` + context.
7. Introduce Adapter v4 interface and port memory adapter.
8. Port Redis/Valkey first, then other providers.
9. Move typed helpers into root package and add codec abstraction.
10. Run upstream Node interoperability matrix, `go test -race`, fuzz targets and soak tests.
11. Publish migration guide and release candidate.

## 31. API freeze checklist

Before v4.0.0-rc.1:

- [ ] no public `Prototype/Proto/Construct` in core API;
- [ ] no primary API returns callback-producing functions for async results;
- [ ] all network starts return bind/runtime errors through Go error paths;
- [ ] all Socket transient modifiers are immutable;
- [ ] Socket has a lifetime context;
- [ ] exact Subscription tokens are the canonical listener removal API;
- [ ] typed API has no `Emitter = any` / `Registrar = any` method-shape switch;
- [ ] Adapter uses context/error semantics;
- [ ] pre-connect sockets are tracked and always cleaned;
- [ ] queue has a bounded production default;
- [ ] official Node compatibility tests remain green;
- [ ] full repository passes `go test -race` for supported modules;
- [ ] v3 -> v4 migration guide covers every removed/renamed public API.
