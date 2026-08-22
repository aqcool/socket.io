package socketio

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

type ackResult struct {
	values []any
	err    error
}

type Socket struct {
	server    *Server
	nsp       *Namespace
	client    *client
	id        SocketID
	pid       PrivateSessionID
	handshake Handshake
	recovered bool

	recoveredRooms []Room
	missedPackets  []any

	ctx       context.Context
	cancel    context.CancelFunc
	connected atomic.Bool
	closeOnce sync.Once
	hub       *eventHub
	queue     *dispatchQueue

	dataMu sync.RWMutex
	data   map[string]any
	ackMu  sync.Mutex
	acks   map[uint64]Ack
}

func newSocket(nsp *Namespace, current *client, auth map[string]any) (*Socket, error) {
	ctx, cancel := context.WithCancel(nsp.server.Context())
	socket := &Socket{
		server: nsp.server,
		nsp:    nsp,
		client: current,
		ctx:    ctx,
		cancel: cancel,
		data:   make(map[string]any),
		acks:   make(map[uint64]Ack),
	}
	socket.hub = newEventHub(nil, socket.Context, nil, nsp.server.cfg.Logger)
	socket.queue = newDispatchQueue(nsp.server.cfg.Queue, func(policy OverflowPolicy) {
		if policy == OverflowDisconnect {
			go func() { _ = socket.Disconnect(true) }()
		}
	})

	if nsp.server.cfg.Recovery != nil && auth != nil {
		if rawPID, ok := auth["pid"].(string); ok && rawPID != "" {
			recovered, err := nsp.adapter.RestoreSession(
				ctx,
				PrivateSessionID(rawPID),
				stringValue(auth["offset"]),
			)
			if err != nil {
				cancel()
				return nil, err
			}
			if recovered != nil {
				socket.id = recovered.SID
				socket.pid = recovered.PID
				socket.recovered = true
				socket.recoveredRooms = append([]Room(nil), recovered.Rooms...)
				socket.missedPackets = append([]any(nil), recovered.MissedPackets...)
				if values, ok := recovered.Data.(map[string]any); ok {
					maps.Copy(socket.data, values)
				}
			}
		}
	}
	if socket.id == "" {
		socket.id = SocketID(randomID())
	}
	if nsp.server.cfg.Recovery != nil && socket.pid == "" {
		socket.pid = PrivateSessionID(randomID())
	}
	socket.handshake = buildNativeHandshake(current, auth)
	return socket, nil
}

func randomID() string {
	var raw [15]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return base64.RawURLEncoding.EncodeToString([]byte(time.Now().String()))
	}
	return base64.RawURLEncoding.EncodeToString(raw[:])
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func buildNativeHandshake(current *client, auth map[string]any) Handshake {
	handshake := Handshake{
		Time:    time.Now(),
		Address: current.conn.RemoteAddress(),
		Auth:    make(map[string]any),
	}
	maps.Copy(handshake.Auth, auth)
	requestContext := current.conn.Request()
	if requestContext == nil {
		return handshake
	}
	request := requestContext.Request()
	if request == nil {
		return handshake
	}
	handshake.Headers = request.Header.Clone()
	handshake.Secure = request.TLS != nil
	if request.URL != nil {
		copyURL := *request.URL
		handshake.URL = &copyURL
	}
	return handshake
}

func (s *Socket) markConnected() {
	s.connected.Store(true)
	s.queue.Start()
}

func (s *Socket) ID() SocketID {
	if s == nil {
		return ""
	}
	return s.id
}

func (s *Socket) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *Socket) Handshake() Handshake {
	if s == nil {
		return Handshake{}
	}
	handshake := s.handshake
	handshake.Headers = handshake.Headers.Clone()
	if handshake.URL != nil {
		copyURL := *handshake.URL
		handshake.URL = &copyURL
	}
	handshake.Auth = maps.Clone(handshake.Auth)
	return handshake
}

func (s *Socket) Recovered() bool { return s != nil && s.recovered }
func (s *Socket) Connected() bool { return s != nil && s.connected.Load() }

func (s *Socket) Rooms() []Room {
	if s == nil {
		return nil
	}
	rooms, err := s.nsp.adapter.SocketRooms(context.Background(), s.id)
	if err != nil {
		return nil
	}
	return rooms
}

func (s *Socket) Join(ctx context.Context, rooms ...Room) error {
	if !s.Connected() {
		return ErrNotConnected
	}
	return s.nsp.adapter.AddAll(ctx, s.id, rooms...)
}

func (s *Socket) Leave(ctx context.Context, rooms ...Room) error {
	if !s.Connected() {
		return ErrNotConnected
	}
	for _, room := range rooms {
		if err := s.nsp.adapter.Delete(ctx, s.id, room); err != nil {
			return err
		}
	}
	return nil
}

func (s *Socket) Disconnect(closeTransport bool) error {
	if s == nil {
		return ErrClosed
	}
	if !s.Connected() {
		return ErrNotConnected
	}
	if closeTransport {
		s.client.conn.Close(true)
		return nil
	}
	if err := s.sendPacket(Packet{
		Type:      PacketDisconnect,
		Namespace: s.nsp.Name(),
	}, BroadcastFlags{}); err != nil {
		return err
	}
	s.close("server namespace disconnect")
	return nil
}

func (s *Socket) Set(key string, value any) {
	if s == nil || key == "" {
		return
	}
	s.dataMu.Lock()
	s.data[key] = value
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
	s.dataMu.Unlock()
}

func (s *Socket) Data() map[string]any {
	if s == nil {
		return nil
	}
	s.dataMu.RLock()
	result := maps.Clone(s.data)
	s.dataMu.RUnlock()
	return result
}

func (s *Socket) On(event string, listener Listener) Subscription {
	if s == nil {
		return closedSubscription{}
	}
	return s.hub.On(event, listener)
}

func (s *Socket) Once(event string, listener Listener) Subscription {
	if s == nil {
		return closedSubscription{}
	}
	return s.hub.Once(event, listener)
}

func (s *Socket) RemoveAllListeners(event string) {
	if s != nil {
		s.hub.RemoveAll(event)
	}
}

func reservedEvent(event string) bool {
	switch event {
	case "connect", "connect_error", "disconnect", "disconnecting", "newListener", "removeListener":
		return true
	default:
		return false
	}
}

func (s *Socket) Emit(event string, args ...any) error {
	return s.emitWithFlags(event, args, BroadcastFlags{})
}

func (s *Socket) emitWithFlags(event string, args []any, flags BroadcastFlags) error {
	if !s.Connected() {
		return ErrNotConnected
	}
	if reservedEvent(event) {
		return errors.New("socket.io: reserved event name: " + event)
	}
	data := append([]any{event}, args...)
	packet := Packet{Type: PacketEvent, Namespace: s.nsp.Name(), Data: data}
	if len(args) > 0 {
		if ack, ok := args[len(args)-1].(Ack); ok {
			packet.Data = append([]any{event}, args[:len(args)-1]...)
			packet.ID = s.registerAck(ack, flags.Timeout)
		}
	}
	return s.sendPacket(packet, flags)
}

func (s *Socket) registerAck(ack Ack, timeout *time.Duration) *uint64 {
	id := s.nsp.nextID()
	s.ackMu.Lock()
	s.acks[id] = ack
	s.ackMu.Unlock()
	if timeout != nil {
		time.AfterFunc(*timeout, func() {
			s.ackMu.Lock()
			fn, ok := s.acks[id]
			if ok {
				delete(s.acks, id)
			}
			s.ackMu.Unlock()
			if ok {
				fn(nil, context.DeadlineExceeded)
			}
		})
	}
	return &id
}

func (s *Socket) EmitAck(ctx context.Context, event string, args ...any) ([]any, error) {
	return s.emitAckWithFlags(ctx, event, args, BroadcastFlags{})
}

func (s *Socket) emitAckWithFlags(
	ctx context.Context,
	event string,
	args []any,
	flags BroadcastFlags,
) ([]any, error) {
	if !s.Connected() {
		return nil, ErrNotConnected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return nil, context.DeadlineExceeded
		}
		if flags.Timeout == nil || timeout < *flags.Timeout {
			flags.Timeout = &timeout
		}
	}

	id := s.nsp.nextID()
	packet := Packet{
		Type:      PacketEvent,
		Namespace: s.nsp.Name(),
		ID:        &id,
		Data:      append([]any{event}, args...),
	}
	channel := make(chan ackResult, 1)
	s.ackMu.Lock()
	s.acks[id] = func(values []any, err error) {
		select {
		case channel <- ackResult{values: values, err: err}:
		default:
		}
	}
	s.ackMu.Unlock()
	if flags.Timeout != nil {
		time.AfterFunc(*flags.Timeout, func() {
			s.ackMu.Lock()
			_, ok := s.acks[id]
			if ok {
				delete(s.acks, id)
			}
			s.ackMu.Unlock()
			if ok {
				select {
				case channel <- ackResult{err: context.DeadlineExceeded}:
				default:
				}
			}
		})
	}
	if err := s.sendPacket(packet, flags); err != nil {
		s.deleteAck(id)
		return nil, err
	}

	select {
	case result := <-channel:
		return result.values, result.err
	case <-ctx.Done():
		s.deleteAck(id)
		return nil, ctx.Err()
	case <-s.Context().Done():
		s.deleteAck(id)
		return nil, ErrNotConnected
	}
}

func (s *Socket) emitPacketAck(
	ctx context.Context,
	packet Packet,
	flags BroadcastFlags,
) ([]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	id := s.nsp.nextID()
	packet.ID = &id
	channel := make(chan ackResult, 1)
	s.ackMu.Lock()
	s.acks[id] = func(values []any, err error) {
		select {
		case channel <- ackResult{values: values, err: err}:
		default:
		}
	}
	s.ackMu.Unlock()
	if err := s.sendPacket(packet, flags); err != nil {
		s.deleteAck(id)
		return nil, err
	}
	select {
	case result := <-channel:
		return result.values, result.err
	case <-ctx.Done():
		s.deleteAck(id)
		return nil, ctx.Err()
	case <-s.Context().Done():
		s.deleteAck(id)
		return nil, ErrNotConnected
	}
}

func (s *Socket) deleteAck(id uint64) {
	s.ackMu.Lock()
	delete(s.acks, id)
	s.ackMu.Unlock()
}

func (s *Socket) Send(args ...any) error  { return s.Emit("message", args...) }
func (s *Socket) Write(args ...any) error { return s.Send(args...) }

func (s *Socket) onPacket(packet Packet) {
	if !s.Connected() {
		return
	}
	switch packet.Type {
	case PacketEvent, PacketBinaryEvent:
		s.onEvent(packet)
	case PacketAck, PacketBinaryAck:
		s.onAck(packet)
	case PacketDisconnect:
		s.close("client namespace disconnect")
	}
}

func (s *Socket) onEvent(packet Packet) {
	values, ok := packet.Data.([]any)
	if !ok || len(values) == 0 {
		return
	}
	event, ok := values[0].(string)
	if !ok || event == "" {
		return
	}
	args := append([]any(nil), values[1:]...)
	if packet.ID != nil {
		args = append(args, s.incomingAck(*packet.ID))
	}
	if err := s.queue.Enqueue(func() {
		s.hub.dispatch(event, args)
	}); err != nil && s.server.cfg.Queue.Overflow == OverflowReject {
		s.hub.dispatch("error", []any{err})
	}
}

func (s *Socket) incomingAck(id uint64) Ack {
	var once sync.Once
	return func(values []any, _ error) {
		once.Do(func() {
			if s.Connected() {
				_ = s.sendPacket(Packet{
					Type:      PacketAck,
					Namespace: s.nsp.Name(),
					ID:        &id,
					Data:      values,
				}, BroadcastFlags{})
			}
		})
	}
}

func (s *Socket) onAck(packet Packet) {
	if packet.ID == nil {
		return
	}
	s.ackMu.Lock()
	ack, ok := s.acks[*packet.ID]
	if ok {
		delete(s.acks, *packet.ID)
	}
	s.ackMu.Unlock()
	if !ok {
		return
	}
	values, _ := packet.Data.([]any)
	ack(values, nil)
}

func (s *Socket) sendPacket(packet Packet, flags BroadcastFlags) error {
	if s == nil || s.client == nil {
		return ErrClosed
	}
	return s.client.writePacket(packet, flags)
}

func (s *Socket) close(reason string) {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		wasConnected := s.connected.Swap(false)
		if wasConnected {
			s.hub.dispatch("disconnecting", []any{reason})
			if s.server.cfg.Recovery != nil && isRecoverableDisconnect(reason) {
				_ = s.nsp.adapter.PersistSession(context.Background(), Session{
					SID:   s.id,
					PID:   s.pid,
					Rooms: s.Rooms(),
					Data:  s.Data(),
				})
			}
		}
		s.cancel()
		s.nsp.remove(s)
		s.client.remove(s.nsp.Name(), s)

		s.ackMu.Lock()
		pending := s.acks
		s.acks = make(map[uint64]Ack)
		s.ackMu.Unlock()
		for _, ack := range pending {
			ack(nil, ErrNotConnected)
		}
		s.queue.Close(true)
		if wasConnected {
			s.hub.dispatch("disconnect", []any{reason})
		}
	})
}

func isRecoverableDisconnect(reason string) bool {
	switch reason {
	case "client namespace disconnect", "server namespace disconnect", "server shutting down":
		return false
	default:
		return true
	}
}

func (s *Socket) QueueDepth() int {
	if s == nil {
		return 0
	}
	return s.queue.Pending()
}
func (s *Socket) QueueOverflows() uint64 {
	if s == nil {
		return 0
	}
	return s.queue.Overflows()
}

func (s *Socket) To(rooms ...Room) *BroadcastOperator {
	return newBroadcastOperator(s.nsp, &BroadcastOptions{
		Rooms:  append([]Room(nil), rooms...),
		Except: []Room{Room(s.id)},
	})
}
func (s *Socket) In(rooms ...Room) *BroadcastOperator { return s.To(rooms...) }
func (s *Socket) Except(rooms ...Room) *BroadcastOperator {
	return s.To().Except(rooms...)
}
func (s *Socket) Broadcast() *BroadcastOperator { return s.To() }
func (s *Socket) Local() *BroadcastOperator     { return s.Broadcast().Local() }
func (s *Socket) Volatile() *SocketOperator {
	return &SocketOperator{socket: s, flags: BroadcastFlags{Volatile: true}}
}
func (s *Socket) Compress(enabled bool) *SocketOperator {
	return &SocketOperator{socket: s, flags: BroadcastFlags{Compress: &enabled}}
}
func (s *Socket) Timeout(timeout time.Duration) *SocketOperator {
	return &SocketOperator{socket: s, flags: BroadcastFlags{Timeout: &timeout}}
}
func (s *Socket) DecodeValue(src any, dst any) error {
	if s == nil {
		return ErrUnsupported
	}
	return s.server.DecodeValue(src, dst)
}

type SocketOperator struct {
	socket *Socket
	flags  BroadcastFlags
}

func (o *SocketOperator) Volatile() *SocketOperator {
	if o == nil {
		return nil
	}
	copyOperator := *o
	copyOperator.flags.Volatile = true
	return &copyOperator
}
func (o *SocketOperator) Compress(enabled bool) *SocketOperator {
	if o == nil {
		return nil
	}
	copyOperator := *o
	copyOperator.flags.Compress = &enabled
	return &copyOperator
}
func (o *SocketOperator) Timeout(timeout time.Duration) *SocketOperator {
	if o == nil {
		return nil
	}
	copyOperator := *o
	copyOperator.flags.Timeout = &timeout
	return &copyOperator
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
