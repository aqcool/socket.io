# Socket.IO Go v4 (alpha)

This directory contains the first implementation of the Go-native v4 API defined in `docs/V4_API_DESIGN.md`.

## Status

**Alpha. Do not treat this as the final v4 release yet.**

The public API is implemented as a v4 module while the wire/protocol execution currently bridges to the proven v3 Socket.IO/Engine.IO core. This lets v4 application code start using Go-native lifecycle, context, subscription and typed-event semantics without rewriting protocol code first.

Implemented in this alpha:

- `github.com/aqcool/socket.io/v4` module
- validated functional options
- `Server` lifecycle: `Handler`, `Serve`, `ListenAndServe`, `Shutdown`, `Close`, `Done`
- atomic namespace lookup/creation at the v4 boundary
- Go error-return middleware
- exact `Subscription` listener identity
- Socket lifetime `context.Context`
- concurrency-safe Socket application data bag
- immutable `SocketOperator` for `Volatile`, `Compress`, `Timeout`
- immutable broadcast operators
- context-based socket queries and remote operations
- root-package typed `Event[Request, Response]`, `On`, `Handle`, `Emit`, `EmitAck`, `EmitAcks`

Not implemented natively yet:

- v4 Adapter providers (Redis/Valkey/Postgres/Mongo/NATS/Kafka/AMQP)
- custom v4 PacketCodec integration
- bounded per-Socket dispatch queue enforcement (the bridge still uses the v3 queue)
- v3 protocol-core dependency removed

`WithAdapter` and `WithParser` therefore return `ErrUnsupported` in the current bridge implementation rather than silently accepting contracts that are not actually wired.

## Basic server

```go
package main

import (
    "context"
    "log"
    "time"

    socketio "github.com/aqcool/socket.io/v4"
)

func main() {
    io, err := socketio.New(
        socketio.WithPath("/socket.io"),
        socketio.WithConnectTimeout(45*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }

    io.Use(func(ctx context.Context, socket *socketio.Socket) error {
        return nil
    })

    io.OnConnection(func(socket *socketio.Socket) {
        socket.On("ping", func(ctx context.Context, args ...any) error {
            return socket.Emit("pong", args...)
        })
    })

    if err := io.ListenAndServe(":3000"); err != nil {
        log.Fatal(err)
    }
}
```

## Typed event with ACK

```go
type SendRequest struct {
    Text string `json:"text"`
}

type SendResponse struct {
    ID string `json:"id"`
}

var Send = socketio.NewEvent[SendRequest, SendResponse]("message:send")

io.OnConnection(func(socket *socketio.Socket) {
    subscription := socketio.Handle(
        socket,
        Send,
        func(ctx context.Context, request SendRequest) (SendResponse, error) {
            return SendResponse{ID: "message-1"}, nil
        },
    )

    // Keep the subscription for as long as the handler should stay registered.
    _ = subscription
})
```

Client-facing ACK emission from Go:

```go
ctx, cancel := context.WithTimeout(socket.Context(), time.Second)
defer cancel()

response, err := socketio.EmitAck(ctx, socket, Send, SendRequest{Text: "hello"})
```

## Lifecycle

The v4 server behaves like a normal Go HTTP component:

```go
io, _ := socketio.New()

mux := http.NewServeMux()
mux.Handle("/socket.io/", io.Handler())
```

or let Socket.IO own a listener:

```go
if err := io.ListenAndServe(":3000"); err != nil {
    // bind and serve errors are returned to the caller
}
```

Graceful shutdown:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := io.Shutdown(ctx); err != nil {
    log.Println(err)
}
```

All Socket lifetime contexts are cancelled when server shutdown begins.

## Concurrency

v4 application-facing APIs are designed for Go concurrency. In particular:

```go
go socket.Volatile().Emit("telemetry", telemetry)
go socket.Timeout(time.Second).Emit("command", command)
```

The modifier state belongs to independent immutable v4 operators. During the alpha bridge, emission is serialized when applying those options to the v3 backend so transient flags cannot leak between concurrent calls.

See `../docs/V4_API_DESIGN.md`, `../docs/V4_MIGRATION.md`, and `../docs/V4_IMPLEMENTATION_PLAN.md` for the target release contract and migration sequence.
