# v3 -> v4 Migration Guide (Draft)

This guide maps the planned v4 API to the current v3 surface. The wire protocol remains Socket.IO-compatible; most migration work is Go API cleanup rather than protocol migration.

## 1. Server construction

### v3

```go
opts := socket.DefaultServerOptions()
opts.SetServeClient(false)
opts.SetConnectTimeout(45 * time.Second)
io := socket.NewServer(nil, opts)
```

### v4

```go
io, err := socketio.New(
    socketio.WithClientServing(false),
    socketio.WithConnectTimeout(45*time.Second),
)
```

Changes:

- constructor returns `(*Server, error)`;
- setter-heavy option interfaces are replaced by validated functional options;
- `GetRawXxx`, `Assign`, `SetXxx` are removed from normal public use.

## 2. Starting the server

### v3 compatibility style

```go
io.Listen(":3000", nil)
```

### v4

```go
if err := io.ListenAndServe(":3000"); err != nil {
    return err
}
```

or integrate with an existing HTTP server:

```go
mux.Handle("/socket.io/", io.Handler())
```

Bind errors are returned instead of surfacing as background goroutine panics.

## 3. Shutdown

### v3

```go
io.Close(func(err error) {
    // callback
})
```

### v4

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := io.Shutdown(ctx); err != nil {
    return err
}
```

## 4. Connection middleware

### v3

```go
io.Use(func(s *socket.Socket, next func(*socket.ExtendedError)) {
    if !authorized(s) {
        next(socket.NewExtendedError("unauthorized", nil))
        return
    }
    next(nil)
})
```

### v4

```go
io.Use(func(ctx context.Context, s *socketio.Socket) error {
    if !authorized(s) {
        return &socketio.ConnectError{Message: "unauthorized"}
    }
    return nil
})
```

No callback is required; cancellation propagates through `ctx`.

## 5. Connection events

### v3

```go
io.On("connection", func(args ...any) {
    s := args[0].(*socket.Socket)
})
```

### v4

```go
sub := io.OnConnection(func(s *socketio.Socket) {
    // ...
})
defer sub.Close()
```

Dynamic event registration remains available for compatibility, but reserved lifecycle events get explicit APIs.

## 6. Listener removal

### v3

```go
handler := func(args ...any) {}
io.On("event", handler)
io.RemoveListener("event", handler)
```

v3 function-based removal relies on function code-address matching and cannot perfectly model distinct Go closure identities.

### v4

```go
sub := socket.On("event", handler)
defer sub.Close()
```

`Subscription.Close()` is exact and idempotent.

## 7. Listener signature

### v3

```go
socket.On("event", func(args ...any) {
    // ...
})
```

### v4 raw API

```go
socket.On("event", func(ctx context.Context, args ...any) error {
    // ...
    return nil
})
```

Use typed helpers for application code whenever the event schema is known.

## 8. Typed events

### v3

```go
var Event = typed.Event[Req, Resp]{Name: "event"}

typed.Handle(socket, Event, func(ctx context.Context, req Req) (Resp, error) {
    // v3 runtime currently supplies context.Background() in the original helper
})
```

### v4

```go
var Event = socketio.NewEvent[Req, Resp]("event")

sub := socketio.Handle(socket, Event, func(ctx context.Context, req Req) (Resp, error) {
    // ctx is derived from socket lifetime
    return Resp{}, nil
})
defer sub.Close()
```

The `typed` runtime module is removed; typed events are root-package features.

## 9. Typed emit + ACK

### v3

```go
resp, err := typed.EmitAck(ctx, socket, Event, req)
```

### v4

```go
resp, err := socketio.EmitAck(ctx, socket, Event, req)
```

The call shape is intentionally similar, but v4 depends on narrow emitter interfaces instead of runtime `any` method-shape switching.

## 10. Socket context

### v3

No canonical socket lifetime context.

### v4

```go
ctx := socket.Context()
```

`ctx.Done()` closes on disconnect/server shutdown and is the base context for middleware and typed handlers.

## 11. Socket transient modifiers

### v3

```go
socket.Volatile().Emit("a")
socket.Timeout(time.Second).Emit("b")
```

The syntax survives, but v3 internally mutates transient Socket flags.

### v4

```go
socket.Volatile().Emit("a")
socket.Timeout(time.Second).Emit("b")
```

The syntax is intentionally preserved; implementation changes to immutable `SocketOperator` snapshots, making concurrent calls safe.

## 12. Room operations

### v3

```go
socket.Join("room")
socket.Leave("room")
```

### v4

```go
if err := socket.Join(ctx, "room"); err != nil {
    return err
}
if err := socket.Leave(ctx, "room"); err != nil {
    return err
}
```

Adapter failures are no longer silently hidden.

## 13. Namespace

### v3

```go
nsp := io.Of("/chat")
```

### v4

```go
nsp := io.Of("/chat")
```

Call shape remains the same, but v4 guarantees atomic same-name creation under concurrent goroutines.

Removed from public v4 Namespace API:

- `Prototype`
- `Proto`
- `Construct`
- `InitAdapter`
- direct internal `Fns` and mutable socket-map accessors

## 14. Fetch sockets

### v3

```go
nsp.FetchSockets()(func(sockets []*socket.RemoteSocket, err error) {
    // ...
})
```

### v4

```go
sockets, err := nsp.FetchSockets(ctx)
```

The same applies to `CountSockets`, `ListRooms`, socket management and server-side ACK operations.

## 15. Broadcast operations

### v3

```go
nsp.To("room").Except("room2").Timeout(time.Second).Emit("event", data)
```

### v4

```go
err := nsp.
    To("room").
    Except("room2").
    Timeout(time.Second).
    Emit("event", data)
```

The chain remains familiar, but each operator is immutable and `Emit` consistently returns `error`.

## 16. Adapter factory

### v3

```go
type AdapterConstructor interface {
    New(Namespace) Adapter
    SupportsConnectionStateRecovery() bool
}
```

### v4

```go
type AdapterFactory interface {
    New(*Namespace) (Adapter, error)
}
```

Capabilities move to the created adapter or a capability-aware factory implementation.

## 17. Adapter lifecycle

### v3

```go
Init()
Close()
ServerCount() int64
```

### v4

```go
Init(context.Context) error
Close() error
ServerCount(context.Context) (int64, error)
```

Provider failures are explicit.

## 18. Adapter room membership

### v3

```go
AddAll(SocketId, *types.Set[Room])
Del(SocketId, Room)
DelAll(SocketId)
```

### v4

```go
AddAll(context.Context, SocketID, ...Room) error
Delete(context.Context, SocketID, Room) error
DeleteAll(context.Context, SocketID) error
```

## 19. Adapter query operations

### v3

```go
FetchSockets(opts) func(func([]SocketDetails, error))
CountSockets(opts) func(func(uint64, error))
ListRooms(opts) func(func(map[Room]uint64, error))
```

### v4

```go
FetchSockets(ctx, opts) ([]SocketDetails, error)
CountSockets(ctx, opts) (uint64, error)
ListRooms(ctx, opts) (map[Room]uint64, error)
```

## 20. Connection state recovery

### v3

```go
recovery := socket.DefaultConnectionStateRecovery()
recovery.SetMaxDisconnectionDuration(...)
recovery.SetSkipMiddlewares(true)
```

### v4

```go
socketio.WithConnectionStateRecovery(socketio.RecoveryOptions{
    MaxDisconnectionDuration: 2 * time.Minute,
    SkipMiddleware:           true,
    CleanupInterval:          time.Minute,
})
```

## 21. Queue/backpressure

v3 uses an unbounded per-Socket sequential task queue.

v4 requires an explicit bounded policy:

```go
socketio.WithQueue(socketio.QueueOptions{
    MaxPending: 4096,
    Overflow:   socketio.OverflowDisconnect,
})
```

Applications with slow handlers should tune this deliberately.

## 22. Handshake

### v3

`Handshake` mirrors JavaScript-oriented structures and includes duplicated textual/numeric timestamps.

### v4

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

Migration is mostly field-type adaptation.

## 23. Error handling

v4 introduces sentinel/wrapped errors suitable for:

```go
if errors.Is(err, socketio.ErrQueueFull) {
    // ...
}
```

Normal network/configuration failures are not signaled with panic.

## 24. Removed/deprecated v3 concepts

The following should not be used in new v4 code:

| v3 API/concept | v4 replacement |
| --- | --- |
| `Prototype/Proto` | ordinary Go composition/internal implementation |
| `Construct` | constructors |
| `GetRawXxx` | validated config/options |
| `SetXxx` option mutation | functional options |
| callback-returning async methods | `(T, error)` + context |
| listener code-address identity | `Subscription` |
| mutable Socket flags | immutable `SocketOperator` |
| `typed.Emitter = any` | `RawEmitter` narrow interface |
| `typed.Registrar = any` | `RawRegistrar` narrow interface |
| hardcoded `context.Background()` handlers | Socket lifetime context |
| unbounded event task queue | bounded queue policy |

## 25. Recommended migration order for applications

1. Replace server startup/close with `ListenAndServe`/`Shutdown`.
2. Replace connection middleware callbacks with error-returning middleware.
3. Move listener cleanup to `Subscription`.
4. Move application events to root-package typed `Event` helpers.
5. Add context to room/query/adapter operations.
6. Adapt Handshake field types.
7. If using a custom Adapter, port it to the v4 Adapter interface last.

## 26. Recommended migration order for repository modules

1. root Socket.IO server + in-memory adapter;
2. parser and Engine.IO contracts;
3. Socket.IO client;
4. Redis/Valkey adapters;
5. Postgres/Mongo;
6. NATS/Kafka/AMQP/Unix;
7. observability/instrumentation;
8. remove v3 compatibility shims before v4.0.0 final.
