package socketio

import (
	"context"
	"maps"
	"net/http"
	"net/url"
	"sync"
	"time"

	core "github.com/aqcool/socket.io/servers/socket/v4"
)

type Socket struct {
	server *Server
	raw    *core.Socket

	ctx    context.Context
	cancel context.CancelFunc
	hub    *eventHub

	emitMu sync.Mutex
	dataMu sync.RWMutex
	data   map[string]any
}

func newSocket(server *Server, raw *core.Socket) *Socket {
	ctx, cancel := context.WithCancel(server.Context())
	socket := &Socket{
		server: server,
		raw:    raw,
		ctx:    ctx,
		cancel: cancel,
		data:   make(map[string]any),
	}
	if existing, ok := raw.Data().(map[string]any); ok {
		maps.Copy(socket.data, existing)
	}
	socket.hub = newEventHub(
		func(event string, listener func(...any)) error {
			return raw.On(event, listener)
		},
		socket.Context,
		socket.transformArgs,
		server.cfg.Logger,
	)

	_ = raw.Once("disconnect", func(...any) {
		cancel()
		server.dropSocket(raw)
	})
	if !raw.Connected() {
		cancel()
	}
	return socket
}

func (s *Server) wrapSocket(raw *core.Socket) *Socket {
	if s == nil || raw == nil {
		return nil
	}
	s.socketMu.Lock()
	defer s.socketMu.Unlock()
	if socket := s.sockets[raw]; socket != nil {
		return socket
	}
	socket := newSocket(s, raw)
	s.sockets[raw] = socket
	return socket
}

func (s *Server) dropSocket(raw *core.Socket) {
	if s == nil || raw == nil {
		return
	}
	s.socketMu.Lock()
	delete(s.sockets, raw)
	s.socketMu.Unlock()
}

func (s *Socket) ID() SocketID {
	if s == nil || s.raw == nil {
		return ""
	}
	return SocketID(s.raw.Id())
}

func (s *Socket) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *Socket) Handshake() Handshake {
	if s == nil || s.raw == nil || s.raw.Handshake() == nil {
		return Handshake{}
	}
	return convertHandshake(s.raw.Handshake())
}

func convertHandshake(raw *core.Handshake) Handshake {
	if raw == nil {
		return Handshake{}
	}
	headers := make(http.Header, len(raw.Headers))
	for key, value := range raw.Headers {
		switch typed := value.(type) {
		case string:
			headers.Set(key, typed)
		case []string:
			headers[key] = append([]string(nil), typed...)
		case []any:
			for _, item := range typed {
				if text, ok := item.(string); ok {
					headers.Add(key, text)
				}
			}
		}
	}

	issued := time.UnixMilli(raw.Issued)
	if raw.Issued == 0 && raw.Time != "" {
		if parsed, err := time.Parse(time.RFC3339, raw.Time); err == nil {
			issued = parsed
		}
	}
	var parsedURL *url.URL
	if raw.Url != "" {
		if value, err := url.Parse(raw.Url); err == nil {
			parsedURL = value
		}
	}
	auth := make(map[string]any, len(raw.Auth))
	maps.Copy(auth, raw.Auth)
	return Handshake{
		Headers: headers,
		Time:    issued,
		Address: raw.Address,
		Secure:  raw.Secure,
		URL:     parsedURL,
		Auth:    auth,
	}
}

func (s *Socket) Recovered() bool {
	return s != nil && s.raw != nil && s.raw.Recovered()
}

func (s *Socket) Connected() bool {
	return s != nil && s.raw != nil && s.raw.Connected()
}

func (s *Socket) Rooms() []Room {
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

func (s *Socket) Join(ctx context.Context, rooms ...Room) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if s == nil || s.raw == nil {
		return ErrClosed
	}
	if !s.Connected() {
		return ErrNotConnected
	}
	return s.raw.TryJoin(toLegacyRooms(rooms)...)
}

func (s *Socket) Leave(ctx context.Context, rooms ...Room) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if s == nil || s.raw == nil {
		return ErrClosed
	}
	if !s.Connected() {
		return ErrNotConnected
	}
	for _, room := range rooms {
		s.raw.Leave(core.Room(room))
	}
	return nil
}

func (s *Socket) Disconnect(closeTransport bool) error {
	if s == nil || s.raw == nil {
		return ErrClosed
	}
	if !s.Connected() {
		return ErrNotConnected
	}
	s.raw.Disconnect(closeTransport)
	if !s.raw.Connected() {
		s.cancel()
	}
	return nil
}

func (s *Socket) Set(key string, value any) {
	if s == nil || key == "" {
		return
	}
	s.dataMu.Lock()
	s.data[key] = value
	s.syncLegacyDataLocked()
	s.dataMu.Unlock()
}

func (s *Socket) Get(key string) (any, bool) {
	if s == nil || key == "" {
		return nil, false
	}
	s.dataMu.RLock()
	value, ok := s.data[key]
	s.dataMu.RUnlock()
	return value, ok
}

func (s *Socket) Delete(key string) {
	if s == nil || key == "" {
		return
	}
	s.dataMu.Lock()
	delete(s.data, key)
	s.syncLegacyDataLocked()
	s.dataMu.Unlock()
}

func (s *Socket) Data() map[string]any {
	if s == nil {
		return nil
	}
	s.dataMu.RLock()
	defer s.dataMu.RUnlock()
	result := make(map[string]any, len(s.data))
	maps.Copy(result, s.data)
	return result
}

func (s *Socket) syncLegacyDataLocked() {
	if s.raw == nil {
		return
	}
	copy := make(map[string]any, len(s.data))
	maps.Copy(copy, s.data)
	s.raw.SetData(copy)
}

func (s *Socket) On(event string, listener Listener) Subscription {
	if s == nil || s.hub == nil {
		return closedSubscription{}
	}
	return s.hub.On(event, listener)
}

func (s *Socket) Once(event string, listener Listener) Subscription {
	if s == nil || s.hub == nil {
		return closedSubscription{}
	}
	return s.hub.Once(event, listener)
}

func (s *Socket) RemoveAllListeners(event string) {
	if s != nil && s.hub != nil {
		s.hub.RemoveAll(event)
	}
}

func (s *Socket) transformArgs(_ string, args []any) []any {
	result := append([]any(nil), args...)
	for i, value := range result {
		if rawAck, ok := value.(core.Ack); ok {
			result[i] = Ack(func(values []any, err error) {
				rawAck(values, err)
			})
		}
	}
	return result
}

type socketEmitFlags struct {
	volatile bool
	compress *bool
	timeout  *time.Duration
}

func (s *Socket) Emit(event string, args ...any) error {
	return s.emitWithFlags(event, args, socketEmitFlags{})
}

func (s *Socket) emitWithFlags(event string, args []any, flags socketEmitFlags) error {
	if s == nil || s.raw == nil {
		return ErrClosed
	}
	if !s.Connected() {
		return ErrNotConnected
	}
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	s.applyFlags(flags)
	return s.raw.Emit(event, args...)
}

func (s *Socket) applyFlags(flags socketEmitFlags) {
	if flags.compress != nil {
		s.raw.Compress(*flags.compress)
	}
	if flags.volatile {
		s.raw.Volatile()
	}
	if flags.timeout != nil {
		s.raw.Timeout(*flags.timeout)
	}
}

func (s *Socket) EmitAck(ctx context.Context, event string, args ...any) ([]any, error) {
	return s.emitAckWithFlags(ctx, event, args, socketEmitFlags{})
}

func (s *Socket) emitAckWithFlags(ctx context.Context, event string, args []any, flags socketEmitFlags) ([]any, error) {
	if s == nil || s.raw == nil {
		return nil, ErrClosed
	}
	if !s.Connected() {
		return nil, ErrNotConnected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if flags.timeout == nil {
		if deadline, ok := ctx.Deadline(); ok {
			timeout := time.Until(deadline)
			if timeout <= 0 {
				return nil, context.DeadlineExceeded
			}
			flags.timeout = &timeout
		}
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

	s.emitMu.Lock()
	s.applyFlags(flags)
	err := s.raw.Emit(event, append(args, ack)...)
	s.emitMu.Unlock()
	if err != nil {
		return nil, err
	}

	select {
	case value := <-ch:
		return value.values, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.Context().Done():
		return nil, ErrNotConnected
	}
}

func (s *Socket) Send(args ...any) error {
	return s.Emit("message", args...)
}

func (s *Socket) Write(args ...any) error {
	return s.Send(args...)
}

func (s *Socket) To(rooms ...Room) *BroadcastOperator {
	if s == nil || s.raw == nil {
		return nil
	}
	s.emitMu.Lock()
	raw := s.raw.To(toLegacyRooms(rooms)...)
	s.emitMu.Unlock()
	return newBroadcastOperator(s.server, raw, nil)
}

func (s *Socket) In(rooms ...Room) *BroadcastOperator {
	return s.To(rooms...)
}

func (s *Socket) Except(rooms ...Room) *BroadcastOperator {
	if s == nil || s.raw == nil {
		return nil
	}
	s.emitMu.Lock()
	raw := s.raw.Except(toLegacyRooms(rooms)...)
	s.emitMu.Unlock()
	return newBroadcastOperator(s.server, raw, nil)
}

func (s *Socket) Broadcast() *BroadcastOperator {
	if s == nil || s.raw == nil {
		return nil
	}
	s.emitMu.Lock()
	raw := s.raw.Broadcast()
	s.emitMu.Unlock()
	return newBroadcastOperator(s.server, raw, nil)
}

func (s *Socket) Local() *BroadcastOperator {
	if s == nil || s.raw == nil {
		return nil
	}
	s.emitMu.Lock()
	raw := s.raw.Local()
	s.emitMu.Unlock()
	return newBroadcastOperator(s.server, raw, nil)
}

func (s *Socket) Volatile() *SocketOperator {
	return &SocketOperator{socket: s, flags: socketEmitFlags{volatile: true}}
}

func (s *Socket) Compress(enabled bool) *SocketOperator {
	return &SocketOperator{socket: s, flags: socketEmitFlags{compress: &enabled}}
}

func (s *Socket) Timeout(timeout time.Duration) *SocketOperator {
	return &SocketOperator{socket: s, flags: socketEmitFlags{timeout: &timeout}}
}

func (s *Socket) DecodeValue(src any, dst any) error {
	if s == nil || s.server == nil {
		return ErrUnsupported
	}
	return s.server.DecodeValue(src, dst)
}

type SocketOperator struct {
	socket *Socket
	flags  socketEmitFlags
}

func (o *SocketOperator) Volatile() *SocketOperator {
	if o == nil {
		return nil
	}
	copy := *o
	copy.flags.volatile = true
	return &copy
}

func (o *SocketOperator) Compress(enabled bool) *SocketOperator {
	if o == nil {
		return nil
	}
	copy := *o
	copy.flags.compress = &enabled
	return &copy
}

func (o *SocketOperator) Timeout(timeout time.Duration) *SocketOperator {
	if o == nil {
		return nil
	}
	copy := *o
	copy.flags.timeout = &timeout
	return &copy
}

func (o *SocketOperator) Emit(event string, args ...any) error {
	if o == nil || o.socket == nil {
		return ErrClosed
	}
	return o.socket.emitWithFlags(event, args, o.flags)
}

func (o *SocketOperator) EmitAck(ctx context.Context, event string, args ...any) ([]any, error) {
	if o == nil || o.socket == nil {
		return nil, ErrClosed
	}
	return o.socket.emitAckWithFlags(ctx, event, args, o.flags)
}

func (o *SocketOperator) DecodeValue(src any, dst any) error {
	if o == nil || o.socket == nil {
		return ErrUnsupported
	}
	return o.socket.DecodeValue(src, dst)
}
