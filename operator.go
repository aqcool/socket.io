package socketio

import (
	"context"
	"time"

	core "github.com/aqcool/socket.io/servers/socket/v4"
)

type BroadcastOperator struct {
	server  *Server
	raw     *core.BroadcastOperator
	timeout *time.Duration
}

func newBroadcastOperator(server *Server, raw *core.BroadcastOperator, timeout *time.Duration) *BroadcastOperator {
	operator := &BroadcastOperator{server: server, raw: raw}
	if timeout != nil {
		value := *timeout
		operator.timeout = &value
	}
	return operator
}

func (o *BroadcastOperator) To(rooms ...Room) *BroadcastOperator {
	if o == nil || o.raw == nil {
		return nil
	}
	return newBroadcastOperator(o.server, o.raw.To(toLegacyRooms(rooms)...), o.timeout)
}

func (o *BroadcastOperator) In(rooms ...Room) *BroadcastOperator {
	return o.To(rooms...)
}

func (o *BroadcastOperator) Except(rooms ...Room) *BroadcastOperator {
	if o == nil || o.raw == nil {
		return nil
	}
	return newBroadcastOperator(o.server, o.raw.Except(toLegacyRooms(rooms)...), o.timeout)
}

func (o *BroadcastOperator) Local() *BroadcastOperator {
	if o == nil || o.raw == nil {
		return nil
	}
	return newBroadcastOperator(o.server, o.raw.Local(), o.timeout)
}

func (o *BroadcastOperator) Volatile() *BroadcastOperator {
	if o == nil || o.raw == nil {
		return nil
	}
	return newBroadcastOperator(o.server, o.raw.Volatile(), o.timeout)
}

func (o *BroadcastOperator) Compress(enabled bool) *BroadcastOperator {
	if o == nil || o.raw == nil {
		return nil
	}
	return newBroadcastOperator(o.server, o.raw.Compress(enabled), o.timeout)
}

func (o *BroadcastOperator) Timeout(timeout time.Duration) *BroadcastOperator {
	if o == nil || o.raw == nil {
		return nil
	}
	return newBroadcastOperator(o.server, o.raw.Timeout(timeout), &timeout)
}

func (o *BroadcastOperator) Emit(event string, args ...any) error {
	if o == nil || o.raw == nil {
		return ErrClosed
	}
	return o.raw.Emit(event, args...)
}

func (o *BroadcastOperator) EmitAcks(ctx context.Context, event string, args ...any) ([][]any, error) {
	if o == nil || o.raw == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	raw := o.raw
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return nil, context.DeadlineExceeded
		}
		if o.timeout == nil || timeout < *o.timeout {
			raw = raw.Timeout(timeout)
		}
	}

	type result struct {
		values [][]any
		err    error
	}
	ch := make(chan result, 1)
	ack := func(values []any, err error) {
		responses := make([][]any, 0, len(values))
		for _, value := range values {
			if tuple, ok := value.([]any); ok {
				responses = append(responses, tuple)
			} else {
				responses = append(responses, []any{value})
			}
		}
		select {
		case ch <- result{values: responses, err: err}:
		default:
		}
	}
	if err := raw.Emit(event, append(args, ack)...); err != nil {
		return nil, err
	}
	select {
	case value := <-ch:
		return value.values, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (o *BroadcastOperator) FetchSockets(ctx context.Context) ([]*RemoteSocket, error) {
	if o == nil || o.raw == nil {
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
	o.raw.FetchSockets()(func(sockets []*core.RemoteSocket, err error) {
		ch <- result{sockets: sockets, err: err}
	})
	select {
	case value := <-ch:
		if value.err != nil {
			return nil, value.err
		}
		wrapped := make([]*RemoteSocket, 0, len(value.sockets))
		for _, socket := range value.sockets {
			wrapped = append(wrapped, newRemoteSocket(o.server, socket))
		}
		return wrapped, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (o *BroadcastOperator) CountSockets(ctx context.Context) (uint64, error) {
	if o == nil || o.raw == nil {
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
	o.raw.CountSockets()(func(count uint64, err error) {
		ch <- result{count: count, err: err}
	})
	select {
	case value := <-ch:
		return value.count, value.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (o *BroadcastOperator) ListRooms(ctx context.Context) (map[Room]uint64, error) {
	if o == nil || o.raw == nil {
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
	o.raw.ListRooms()(func(rooms map[core.Room]uint64, err error) {
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

func (o *BroadcastOperator) SocketsJoin(ctx context.Context, rooms ...Room) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if o == nil || o.raw == nil {
		return ErrClosed
	}
	o.raw.SocketsJoin(toLegacyRooms(rooms)...)
	return nil
}

func (o *BroadcastOperator) SocketsLeave(ctx context.Context, rooms ...Room) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if o == nil || o.raw == nil {
		return ErrClosed
	}
	o.raw.SocketsLeave(toLegacyRooms(rooms)...)
	return nil
}

func (o *BroadcastOperator) DisconnectSockets(ctx context.Context, closeTransport bool) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if o == nil || o.raw == nil {
		return ErrClosed
	}
	o.raw.DisconnectSockets(closeTransport)
	return nil
}

func (o *BroadcastOperator) DecodeValue(src any, dst any) error {
	if o == nil || o.server == nil {
		return ErrUnsupported
	}
	return o.server.DecodeValue(src, dst)
}

type RemoteSocket struct {
	server *Server
	raw    *core.RemoteSocket
}

func newRemoteSocket(server *Server, raw *core.RemoteSocket) *RemoteSocket {
	return &RemoteSocket{server: server, raw: raw}
}

func (s *RemoteSocket) ID() SocketID {
	if s == nil || s.raw == nil {
		return ""
	}
	return SocketID(s.raw.Id())
}

func (s *RemoteSocket) Handshake() Handshake {
	if s == nil || s.raw == nil {
		return Handshake{}
	}
	return convertHandshake(s.raw.Handshake())
}

func (s *RemoteSocket) Rooms() []Room {
	if s == nil || s.raw == nil {
		return nil
	}
	rooms := s.raw.Rooms().Keys()
	result := make([]Room, len(rooms))
	for i, room := range rooms {
		result[i] = Room(room)
	}
	return result
}

func (s *RemoteSocket) Data() any {
	if s == nil || s.raw == nil {
		return nil
	}
	return s.raw.Data()
}

func (s *RemoteSocket) Join(ctx context.Context, rooms ...Room) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if s == nil || s.raw == nil {
		return ErrClosed
	}
	s.raw.Join(toLegacyRooms(rooms)...)
	return nil
}

func (s *RemoteSocket) Leave(ctx context.Context, rooms ...Room) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if s == nil || s.raw == nil {
		return ErrClosed
	}
	s.raw.Leave(toLegacyRooms(rooms)...)
	return nil
}

func (s *RemoteSocket) Disconnect(ctx context.Context, closeTransport bool) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if s == nil || s.raw == nil {
		return ErrClosed
	}
	s.raw.Disconnect(closeTransport)
	return nil
}

func (s *RemoteSocket) Emit(ctx context.Context, event string, args ...any) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if s == nil || s.raw == nil {
		return ErrClosed
	}
	return s.raw.Emit(event, args...)
}

func (s *RemoteSocket) EmitAck(ctx context.Context, event string, args ...any) ([]any, error) {
	if s == nil || s.raw == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw := s.raw
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return nil, context.DeadlineExceeded
		}
		rawOperator := raw.Timeout(timeout)
		return emitLegacySingleAck(ctx, rawOperator, event, args...)
	}
	type result struct {
		values []any
		err    error
	}
	ch := make(chan result, 1)
	ack := func(values []any, err error) {
		select {
		case ch <- result{values: values, err: err}:
		default:
		}
	}
	if err := raw.Emit(event, append(args, ack)...); err != nil {
		return nil, err
	}
	select {
	case value := <-ch:
		return value.values, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func emitLegacySingleAck(ctx context.Context, raw *core.BroadcastOperator, event string, args ...any) ([]any, error) {
	type result struct {
		values []any
		err    error
	}
	ch := make(chan result, 1)
	ack := func(values []any, err error) {
		select {
		case ch <- result{values: values, err: err}:
		default:
		}
	}
	if err := raw.Emit(event, append(args, ack)...); err != nil {
		return nil, err
	}
	select {
	case value := <-ch:
		return value.values, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *RemoteSocket) DecodeValue(src any, dst any) error {
	if s == nil || s.server == nil {
		return ErrUnsupported
	}
	return s.server.DecodeValue(src, dst)
}
