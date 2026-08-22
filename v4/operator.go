package socketio

import (
	"context"
	"errors"
	"sync"
	"time"
)

type BroadcastOperator struct {
	nsp  *Namespace
	opts *BroadcastOptions
}

func newBroadcastOperator(nsp *Namespace, opts *BroadcastOptions) *BroadcastOperator {
	return &BroadcastOperator{nsp: nsp, opts: cloneBroadcastOptions(opts)}
}

func cloneBroadcastOptions(opts *BroadcastOptions) *BroadcastOptions {
	if opts == nil {
		return &BroadcastOptions{}
	}
	out := *opts
	out.Rooms = append([]Room(nil), opts.Rooms...)
	out.Except = append([]Room(nil), opts.Except...)
	if opts.Flags.Compress != nil {
		value := *opts.Flags.Compress
		out.Flags.Compress = &value
	}
	if opts.Flags.Timeout != nil {
		value := *opts.Flags.Timeout
		out.Flags.Timeout = &value
	}
	return &out
}

func (o *BroadcastOperator) clone() *BroadcastOperator {
	if o == nil {
		return nil
	}
	return newBroadcastOperator(o.nsp, o.opts)
}

func appendUniqueRooms(base []Room, rooms ...Room) []Room {
	seen := make(map[Room]struct{}, len(base)+len(rooms))
	result := make([]Room, 0, len(base)+len(rooms))
	for _, room := range append(append([]Room(nil), base...), rooms...) {
		if room == "" {
			continue
		}
		if _, exists := seen[room]; exists {
			continue
		}
		seen[room] = struct{}{}
		result = append(result, room)
	}
	return result
}

func (o *BroadcastOperator) To(rooms ...Room) *BroadcastOperator {
	copyOperator := o.clone()
	if copyOperator != nil {
		copyOperator.opts.Rooms = appendUniqueRooms(copyOperator.opts.Rooms, rooms...)
	}
	return copyOperator
}

func (o *BroadcastOperator) In(rooms ...Room) *BroadcastOperator { return o.To(rooms...) }

func (o *BroadcastOperator) Except(rooms ...Room) *BroadcastOperator {
	copyOperator := o.clone()
	if copyOperator != nil {
		copyOperator.opts.Except = appendUniqueRooms(copyOperator.opts.Except, rooms...)
	}
	return copyOperator
}

func (o *BroadcastOperator) Local() *BroadcastOperator {
	copyOperator := o.clone()
	if copyOperator != nil {
		copyOperator.opts.Flags.Local = true
	}
	return copyOperator
}

func (o *BroadcastOperator) Volatile() *BroadcastOperator {
	copyOperator := o.clone()
	if copyOperator != nil {
		copyOperator.opts.Flags.Volatile = true
	}
	return copyOperator
}

func (o *BroadcastOperator) Compress(enabled bool) *BroadcastOperator {
	copyOperator := o.clone()
	if copyOperator != nil {
		copyOperator.opts.Flags.Compress = &enabled
	}
	return copyOperator
}

func (o *BroadcastOperator) Timeout(timeout time.Duration) *BroadcastOperator {
	copyOperator := o.clone()
	if copyOperator != nil {
		copyOperator.opts.Flags.Timeout = &timeout
	}
	return copyOperator
}

func (o *BroadcastOperator) Emit(event string, args ...any) error {
	if o == nil || o.nsp == nil {
		return ErrClosed
	}
	if reservedEvent(event) {
		return errors.New("socket.io: reserved event name: " + event)
	}
	if len(args) > 0 {
		if ack, ok := args[len(args)-1].(Ack); ok {
			responses, err := o.EmitAcks(context.Background(), event, args[:len(args)-1]...)
			if err != nil {
				ack(nil, err)
				return err
			}
			flattened := make([]any, 0, len(responses))
			for _, values := range responses {
				if len(values) == 1 {
					flattened = append(flattened, values[0])
				} else {
					flattened = append(flattened, values)
				}
			}
			ack(flattened, nil)
			return nil
		}
	}
	return o.nsp.adapter.Broadcast(
		context.Background(),
		Packet{Type: PacketEvent, Namespace: o.nsp.Name(), Data: append([]any{event}, args...)},
		o.opts,
	)
}

func (o *BroadcastOperator) EmitAcks(ctx context.Context, event string, args ...any) ([][]any, error) {
	if o == nil || o.nsp == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	operator := o
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return nil, context.DeadlineExceeded
		}
		if o.opts.Flags.Timeout == nil || timeout < *o.opts.Flags.Timeout {
			operator = o.Timeout(timeout)
		}
	}

	packet := Packet{
		Type:      PacketEvent,
		Namespace: operator.nsp.Name(),
		Data:      append([]any{event}, args...),
	}
	var mu sync.Mutex
	responses := make([][]any, 0)
	var firstErr error
	if err := operator.nsp.adapter.BroadcastWithAck(
		ctx,
		packet,
		operator.opts,
		nil,
		func(values []any, err error) {
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			if err == nil {
				responses = append(responses, append([]any(nil), values...))
			}
		},
	); err != nil {
		return nil, err
	}
	mu.Lock()
	defer mu.Unlock()
	return append([][]any(nil), responses...), firstErr
}

func (o *BroadcastOperator) FetchSockets(ctx context.Context) ([]*RemoteSocket, error) {
	if o == nil || o.nsp == nil {
		return nil, ErrClosed
	}
	details, err := o.nsp.adapter.FetchSockets(ctx, o.opts)
	if err != nil {
		return nil, err
	}
	result := make([]*RemoteSocket, 0, len(details))
	for index := range details {
		result = append(result, newRemoteSocket(o.nsp, &details[index]))
	}
	return result, nil
}

func (o *BroadcastOperator) CountSockets(ctx context.Context) (uint64, error) {
	if o == nil || o.nsp == nil {
		return 0, ErrClosed
	}
	return o.nsp.adapter.CountSockets(ctx, o.opts)
}

func (o *BroadcastOperator) ListRooms(ctx context.Context) (map[Room]uint64, error) {
	if o == nil || o.nsp == nil {
		return nil, ErrClosed
	}
	return o.nsp.adapter.ListRooms(ctx, o.opts)
}

func (o *BroadcastOperator) SocketsJoin(ctx context.Context, rooms ...Room) error {
	if o == nil || o.nsp == nil {
		return ErrClosed
	}
	return o.nsp.adapter.AddSockets(ctx, o.opts, rooms...)
}

func (o *BroadcastOperator) SocketsLeave(ctx context.Context, rooms ...Room) error {
	if o == nil || o.nsp == nil {
		return ErrClosed
	}
	return o.nsp.adapter.DeleteSockets(ctx, o.opts, rooms...)
}

func (o *BroadcastOperator) DisconnectSockets(ctx context.Context, closeTransport bool) error {
	if o == nil || o.nsp == nil {
		return ErrClosed
	}
	return o.nsp.adapter.DisconnectSockets(ctx, o.opts, closeTransport)
}

func (o *BroadcastOperator) DecodeValue(src any, dst any) error {
	if o == nil || o.nsp == nil {
		return ErrUnsupported
	}
	return o.nsp.DecodeValue(src, dst)
}

type RemoteSocket struct {
	nsp      *Namespace
	details  SocketDetails
	operator *BroadcastOperator
}

func newRemoteSocket(nsp *Namespace, details *SocketDetails) *RemoteSocket {
	if details == nil {
		return nil
	}
	copyDetails := *details
	copyDetails.Rooms = append([]Room(nil), details.Rooms...)
	return &RemoteSocket{
		nsp:     nsp,
		details: copyDetails,
		operator: newBroadcastOperator(nsp, &BroadcastOptions{
			Rooms: []Room{Room(details.ID)},
			Flags: BroadcastFlags{ExpectSingleResponse: true},
		}),
	}
}

func (s *RemoteSocket) ID() SocketID {
	if s == nil {
		return ""
	}
	return s.details.ID
}
func (s *RemoteSocket) Handshake() Handshake {
	if s == nil {
		return Handshake{}
	}
	return s.details.Handshake
}
func (s *RemoteSocket) Rooms() []Room {
	if s == nil {
		return nil
	}
	return append([]Room(nil), s.details.Rooms...)
}
func (s *RemoteSocket) Data() any {
	if s == nil {
		return nil
	}
	return s.details.Data
}
func (s *RemoteSocket) Join(ctx context.Context, rooms ...Room) error {
	if s == nil {
		return ErrClosed
	}
	return s.operator.SocketsJoin(ctx, rooms...)
}
func (s *RemoteSocket) Leave(ctx context.Context, rooms ...Room) error {
	if s == nil {
		return ErrClosed
	}
	return s.operator.SocketsLeave(ctx, rooms...)
}
func (s *RemoteSocket) Disconnect(ctx context.Context, closeTransport bool) error {
	if s == nil {
		return ErrClosed
	}
	return s.operator.DisconnectSockets(ctx, closeTransport)
}
func (s *RemoteSocket) Emit(ctx context.Context, event string, args ...any) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if s == nil {
		return ErrClosed
	}
	return s.operator.Emit(event, args...)
}
func (s *RemoteSocket) EmitAck(ctx context.Context, event string, args ...any) ([]any, error) {
	if s == nil {
		return nil, ErrClosed
	}
	responses, err := s.operator.EmitAcks(ctx, event, args...)
	if err != nil {
		return nil, err
	}
	if len(responses) == 0 {
		return nil, nil
	}
	return responses[0], nil
}
func (s *RemoteSocket) DecodeValue(src any, dst any) error {
	if s == nil || s.nsp == nil {
		return ErrUnsupported
	}
	return s.nsp.DecodeValue(src, dst)
}
