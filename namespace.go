package socketio

import (
	"context"
	"errors"
	"time"

	core "github.com/aqcool/socket.io/servers/socket/v4"
)

type Namespace struct {
	server *Server
	raw    core.Namespace
	hub    *eventHub
}

func newNamespace(server *Server, raw core.Namespace) *Namespace {
	namespace := &Namespace{server: server, raw: raw}
	if server == nil || raw == nil {
		return namespace
	}
	namespace.hub = newEventHub(
		func(event string, listener func(...any)) error {
			return raw.On(event, listener)
		},
		server.Context,
		server.transformArgs,
		server.cfg.Logger,
	)
	return namespace
}

func (n *Namespace) Name() string {
	if n == nil || n.raw == nil {
		return ""
	}
	return n.raw.Name()
}

func (n *Namespace) On(event string, listener Listener) Subscription {
	if n == nil || n.hub == nil {
		return closedSubscription{}
	}
	return n.hub.On(event, listener)
}

func (n *Namespace) Once(event string, listener Listener) Subscription {
	if n == nil || n.hub == nil {
		return closedSubscription{}
	}
	return n.hub.Once(event, listener)
}

func (n *Namespace) RemoveAllListeners(event string) {
	if n != nil && n.hub != nil {
		n.hub.RemoveAll(event)
	}
}

func (n *Namespace) OnConnection(fn func(*Socket)) Subscription {
	if fn == nil {
		return closedSubscription{}
	}
	return n.On("connection", func(_ context.Context, args ...any) error {
		if len(args) > 0 {
			if socket, ok := args[0].(*Socket); ok {
				fn(socket)
			}
		}
		return nil
	})
}

func (n *Namespace) Use(middleware ...Middleware) {
	if n == nil || n.raw == nil {
		return
	}
	for _, current := range middleware {
		if current == nil {
			continue
		}
		mw := current
		n.raw.Use(func(rawSocket *core.Socket, next func(*core.ExtendedError)) {
			socket := n.server.wrapSocket(rawSocket)
			ctx := socket.Context()
			if err := ctx.Err(); err != nil {
				next(toLegacyConnectError(err))
				return
			}
			if err := mw(ctx, socket); err != nil {
				next(toLegacyConnectError(err))
				return
			}
			next(nil)
		})
	}
}

func toLegacyConnectError(err error) *core.ExtendedError {
	if err == nil {
		return nil
	}
	var connectErr *ConnectError
	if errors.As(err, &connectErr) {
		message := connectErr.Message
		if message == "" {
			message = connectErr.Error()
		}
		return core.NewExtendedError(message, connectErr.Data)
	}
	return core.NewExtendedError(err.Error(), nil)
}

func (n *Namespace) Emit(event string, args ...any) error {
	if n == nil || n.raw == nil {
		return ErrClosed
	}
	return n.raw.Emit(event, args...)
}

func (n *Namespace) To(rooms ...Room) *BroadcastOperator {
	if n == nil || n.raw == nil {
		return nil
	}
	return newBroadcastOperator(n.server, n.raw.To(toLegacyRooms(rooms)...), nil)
}

func (n *Namespace) In(rooms ...Room) *BroadcastOperator {
	return n.To(rooms...)
}

func (n *Namespace) Except(rooms ...Room) *BroadcastOperator {
	if n == nil || n.raw == nil {
		return nil
	}
	return newBroadcastOperator(n.server, n.raw.Except(toLegacyRooms(rooms)...), nil)
}

func (n *Namespace) Local() *BroadcastOperator {
	if n == nil || n.raw == nil {
		return nil
	}
	return newBroadcastOperator(n.server, n.raw.Local(), nil)
}

func (n *Namespace) Volatile() *BroadcastOperator {
	if n == nil || n.raw == nil {
		return nil
	}
	return newBroadcastOperator(n.server, n.raw.Volatile(), nil)
}

func (n *Namespace) Compress(enabled bool) *BroadcastOperator {
	if n == nil || n.raw == nil {
		return nil
	}
	return newBroadcastOperator(n.server, n.raw.Compress(enabled), nil)
}

func (n *Namespace) Timeout(timeout time.Duration) *BroadcastOperator {
	if n == nil || n.raw == nil {
		return nil
	}
	return newBroadcastOperator(n.server, n.raw.Timeout(timeout), &timeout)
}

func (n *Namespace) FetchSockets(ctx context.Context) ([]*RemoteSocket, error) {
	if n == nil || n.raw == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	type result struct {
		sockets []*core.RemoteSocket
		err     error
	}
	ch := make(chan result, 1)
	n.raw.FetchSockets()(func(sockets []*core.RemoteSocket, err error) {
		ch <- result{sockets: sockets, err: err}
	})
	select {
	case value := <-ch:
		if value.err != nil {
			return nil, value.err
		}
		wrapped := make([]*RemoteSocket, 0, len(value.sockets))
		for _, socket := range value.sockets {
			wrapped = append(wrapped, newRemoteSocket(n.server, socket))
		}
		return wrapped, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Namespace) CountSockets(ctx context.Context) (uint64, error) {
	if n == nil || n.raw == nil {
		return 0, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	type result struct {
		count uint64
		err   error
	}
	ch := make(chan result, 1)
	n.raw.CountSockets()(func(count uint64, err error) {
		ch <- result{count: count, err: err}
	})
	select {
	case value := <-ch:
		return value.count, value.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (n *Namespace) ListRooms(ctx context.Context) (map[Room]uint64, error) {
	if n == nil || n.raw == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	type result struct {
		rooms map[core.Room]uint64
		err   error
	}
	ch := make(chan result, 1)
	n.raw.ListRooms()(func(rooms map[core.Room]uint64, err error) {
		ch <- result{rooms: rooms, err: err}
	})
	select {
	case value := <-ch:
		if value.err != nil {
			return nil, value.err
		}
		rooms := make(map[Room]uint64, len(value.rooms))
		for room, count := range value.rooms {
			rooms[Room(room)] = count
		}
		return rooms, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Namespace) SocketsJoin(ctx context.Context, rooms ...Room) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if n == nil || n.raw == nil {
		return ErrClosed
	}
	n.raw.SocketsJoin(toLegacyRooms(rooms)...)
	return nil
}

func (n *Namespace) SocketsLeave(ctx context.Context, rooms ...Room) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if n == nil || n.raw == nil {
		return ErrClosed
	}
	n.raw.SocketsLeave(toLegacyRooms(rooms)...)
	return nil
}

func (n *Namespace) DisconnectSockets(ctx context.Context, closeTransport bool) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if n == nil || n.raw == nil {
		return ErrClosed
	}
	n.raw.DisconnectSockets(closeTransport)
	return nil
}

func (n *Namespace) ServerSideEmit(ctx context.Context, event string, args ...any) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if n == nil || n.raw == nil {
		return ErrClosed
	}
	return n.raw.ServerSideEmit(event, args...)
}

func (n *Namespace) ServerSideEmitAck(ctx context.Context, event string, args ...any) ([]any, error) {
	if n == nil || n.raw == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	type result struct {
		values []any
		err    error
	}
	ch := make(chan result, 1)
	ack := func(values []any, err error) {
		ch <- result{values: values, err: err}
	}
	if err := n.raw.ServerSideEmitWithAck(event, args...)(ack); err != nil {
		return nil, err
	}
	select {
	case value := <-ch:
		return value.values, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Namespace) EmitAcks(ctx context.Context, event string, args ...any) ([][]any, error) {
	operator := n.To()
	if operator == nil {
		return nil, ErrClosed
	}
	return operator.EmitAcks(ctx, event, args...)
}

func (n *Namespace) DecodeValue(src any, dst any) error {
	if n == nil || n.server == nil {
		return ErrUnsupported
	}
	return n.server.DecodeValue(src, dst)
}

type ParentNamespace struct {
	*Namespace
	raw core.ParentNamespace
}

func (p *ParentNamespace) Children() []*Namespace {
	if p == nil || p.raw == nil || p.server == nil {
		return nil
	}
	children := p.raw.Children().Keys()
	result := make([]*Namespace, 0, len(children))
	for _, child := range children {
		result = append(result, p.server.wrapNamespace(child))
	}
	return result
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
