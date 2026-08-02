package socket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
	"github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/slices"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/utils"
)

var ErrBufferOverflow = errors.New("socket.io-client: buffer overflow")

// Socket represents a Socket.IO connection to a specific namespace.
// It implements an event-driven interface for real-time bidirectional communication.
//
// Socket belongs to a specific namespace (default '/') and uses an underlying Manager
// for network communication. It supports event emission, acknowledgments, and automatic
// reconnection.
//
// Example usage:
//
//	socket := io.Connect("http://localhost:8000", nil)
//
//	socket.On("connect", func() {
//		fmt.Println("Connected!")
//		// Send an event to the server
//		socket.Emit("message", "Hello server!")
//	})
//
//	// Listen for server events
//	socket.On("reply", func(msg string) {
//		fmt.Printf("Received: %s\n", msg)
//	})
//
//	// Handle disconnection
//	socket.On("disconnect", func(reason string) {
//		fmt.Printf("Disconnected: %s\n", reason)
//	})
type Socket struct {
	types.EventEmitter

	// io represents the Manager instance that created this Socket.
	io *Manager

	// id is the session identifier, only available when connected.
	id types.Atomic[string]

	// _pid is the private session ID used for connection state recovery.
	_pid types.Atomic[string]

	// _lastOffset stores the offset of the last received packet for connection recovery.
	_lastOffset types.Atomic[string]

	// connected indicates whether the socket is currently connected to the server.
	connected atomic.Bool

	// recovered indicates if the connection state was recovered after reconnection.
	recovered atomic.Bool

	// auth stores the authentication credentials for namespace access.
	auth         map[string]any
	authProvider AuthProvider
	authMu       sync.RWMutex
	authCancel   context.CancelFunc
	authAttempt  atomic.Uint64

	// receiveBuffer stores packets received before the CONNECT packet.
	receiveBuffer *types.Slice[[]any]

	// sendBuffer stores packets that will be sent after connection is established.
	sendBuffer *types.Slice[*Packet]

	// _queue manages packets requiring guaranteed delivery with retry support.
	_queue *types.Slice[*QueuedPacket]

	// _queueSeq generates unique IDs for queued packets.
	_queueSeq atomic.Uint64

	bufferMu           sync.Mutex
	sendBufferBytes    atomic.Int64
	receiveBufferBytes atomic.Int64
	retryQueueBytes    atomic.Int64

	// nsp is the namespace this socket belongs to.
	nsp string

	// _opts stores the socket configuration options.
	_opts SocketOptionsInterface

	// ids generates unique IDs for emitted events requiring acknowledgment.
	ids atomic.Uint64

	// acks stores callbacks for event acknowledgments.
	acks *types.Map[uint64, socket.Ack]

	// flags stores temporary modifiers for the next emission.
	flags atomic.Pointer[Flags]

	// subs stores active event subscriptions.
	subs atomic.Pointer[types.Slice[types.Callable]]

	// _anyListeners stores listeners that catch all incoming events.
	_anyListeners *types.Slice[types.EventListener]

	// _anyOutgoingListeners stores listeners that catch all outgoing events.
	_anyOutgoingListeners *types.Slice[types.EventListener]
}

// MakeSocket creates a new Socket instance with default buffers and event listeners.
func MakeSocket() *Socket {
	r := &Socket{
		EventEmitter: types.NewEventEmitter(),

		receiveBuffer:         types.NewSlice[[]any](),
		sendBuffer:            types.NewSlice[*Packet](),
		_queue:                types.NewSlice[*QueuedPacket](),
		acks:                  &types.Map[uint64, socket.Ack]{},
		_anyListeners:         types.NewSlice[types.EventListener](),
		_anyOutgoingListeners: types.NewSlice[types.EventListener](),
	}

	r.flags.Store(&Flags{})
	return r
}

// NewSocket creates a new Socket instance for the given manager, namespace, and options.
func NewSocket(io *Manager, nsp string, opts SocketOptionsInterface) *Socket {
	r := MakeSocket()

	r.Construct(io, nsp, opts)

	return r
}

// Io returns the Manager instance that created this Socket.
func (s *Socket) Io() *Manager {
	return s.io
}

// Nsp returns the namespace of this Socket.
func (s *Socket) Nsp() string {
	return s.nsp
}

// Id returns the session identifier for this socket, only available when connected.
func (s *Socket) Id() string {
	return s.id.Load()
}

// Connected reports whether the socket is currently connected to the server.
func (s *Socket) Connected() bool {
	return s.connected.Load()
}

// Recovered reports if the connection state was recovered after reconnection.
func (s *Socket) Recovered() bool {
	return s.recovered.Load()
}

// Auth returns the authentication credentials for namespace access.
func (s *Socket) Auth() map[string]any {
	s.authMu.RLock()
	defer s.authMu.RUnlock()
	return cloneAuth(s.auth)
}

// ReceiveBuffer returns the buffer of packets received before the CONNECT packet.
func (s *Socket) ReceiveBuffer() *types.Slice[[]any] {
	return s.receiveBuffer
}

// SendBuffer returns the buffer of packets to be sent after connection is established.
func (s *Socket) SendBuffer() *types.Slice[*Packet] {
	return s.sendBuffer
}

// Construct initializes the Socket instance.
//
// io: The Manager instance.
// nsp: The namespace.
// opts: The socket options.
func (s *Socket) Construct(io *Manager, nsp string, opts SocketOptionsInterface) {
	s.io = io
	s.nsp = nsp
	s._opts = DefaultSocketOptions().Assign(opts)
	if auth := s._opts.Auth(); auth != nil {
		s.auth = cloneAuth(auth)
	}
	s.authProvider = s._opts.AuthProvider()

	if s.io._autoConnect {
		s.Open()
	}
}

// Disconnected checks whether the socket is currently disconnected.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.On("connect", func(...any) {
//	  fmt.Println(socket.Disconnected()) // false
//	})
//
//	socket.On("disconnect", func(...any) {
//	  fmt.Println(socket.Disconnected()) // true
//	})
func (s *Socket) Disconnected() bool {
	return !s.connected.Load()
}

// subEvents subscribes to open, close, and packet events.
func (s *Socket) subEvents() {
	if s.Active() {
		return
	}

	s.subs.Store(types.NewSlice(
		on(s.io, "open", s.onopen),
		on(s.io, "packet", func(args ...any) {
			s.onpacket(slices.TryGetAny[*parser.Packet](args, 0))
		}),
		on(s.io, "error", s.onerror),
		on(s.io, "close", func(args ...any) {
			s.onclose(slices.TryGetAny[string](args, 0), slices.TryGetAny[error](args, 1))
		}),
	))
}

// Active checks whether the Socket will try to reconnect when its Manager connects or reconnects.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	fmt.Println(socket.Active()) // true
//
//	socket.On("disconnect", func(reason ...any) {
//	  if slices.TryGetAny[string](reason, 0) == "io server disconnect" {
//	    // the disconnection was initiated by the server, you need to manually reconnect
//	    fmt.Println(socket.Active()) // false
//	  }
//	  // else the socket will automatically try to reconnect
//	  fmt.Println(socket.Active()) // true
//	})
func (s *Socket) Active() bool {
	return s.subs.Load() != nil
}

// Connect "opens" the socket.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.SetAutoConnect(false)
//	socket.Connect()
func (s *Socket) Connect() *Socket {
	if s.connected.Load() {
		return s
	}

	s.subEvents()
	if !s.io._reconnecting.Load() {
		s.io.Open(nil) // ensure open
	}
	if ReadyStateOpen == s.io._readyState.Load() {
		s.onopen()
	}
	return s
}

// SetAuth updates the authentication data for this namespace only.
func (s *Socket) SetAuth(auth map[string]any) *Socket {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	s.auth = cloneAuth(auth)
	s._opts.SetAuth(cloneAuth(auth))
	return s
}

// SetAuthProvider updates the dynamic authentication provider for this
// namespace. The provider is called before every connection and reconnection.
func (s *Socket) SetAuthProvider(provider AuthProvider) *Socket {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	s.authProvider = provider
	s._opts.SetAuthProvider(provider)
	return s
}

// Open is an alias for Connect.
func (s *Socket) Open() *Socket {
	return s.Connect()
}

// Send sends a `message` event.
//
// This method mimics the WebSocket.send() method.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.Send("hello")
//
//	// this is equivalent to
//	socket.Emit("message", "hello")
func (s *Socket) Send(args ...any) *Socket {
	_ = s.Emit("message", args...)
	return s
}

// Emit overrides the default emit behavior.
// If the event is in `events`, it's emitted normally.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.Emit("hello", "world")
//
//	// all serializable datastructures are supported (no need to call JSON.stringify)
//	socket.Emit("hello", 1, "2", map[int][]string{3: {"4"}, 5: {"6"}})
//
//	// with an acknowledgement from the server
//
//	socket.Emit("hello", "world", func(val []any, err error) {
//	  // ...
//	})
func (s *Socket) Emit(ev string, args ...any) error {
	if RESERVED_EVENTS.Has(ev) {
		return fmt.Errorf(`"%s" is a reserved event name`, ev)
	}

	data := append([]any{ev}, args...)
	data_len := len(data)

	flags := s.flags.Swap(&Flags{})

	if s._opts.Retries() > 0 && !flags.FromQueue && !flags.Volatile {
		return s._addToQueue(data, flags)
	}

	compress := true
	if flags.Compress != nil {
		compress = *flags.Compress
	}

	packet := &Packet{
		Packet: &parser.Packet{
			Type: parser.EVENT,
			Data: data,
		},
		Options: &packet.Options{
			Compress: utils.Ptr(compress),
		},
	}

	// event ack callback
	if ack, withAck := data[data_len-1].(socket.Ack); withAck {
		id := s.ids.Add(1) - 1
		socketLog.Debug("emitting packet with ack id %d", id)

		packet.Data = data[:data_len-1]
		s._registerAckCallback(id, ack, flags.Timeout)
		packet.Id = &id
	}

	isTransportWritable := false
	if engine := s.io.Engine(); engine != nil {
		if transport := engine.Transport(); transport != nil {
			isTransportWritable = transport.Writable()
		}
	}

	isConnected := false
	if s.connected.Load() {
		if engine := s.io.Engine(); engine != nil && !engine.HasPingExpired() {
			isConnected = true
		}
	}

	if flags.Volatile && !isTransportWritable {
		socketLog.Debug("discard packet as the transport is not currently writable")
	} else if isConnected {
		s.notifyOutgoingListeners(packet)
		s.packet(packet)
	} else {
		_, err := s.enqueueSendPacket(packet)
		return err
	}

	return nil
}

func (s *Socket) _registerAckCallback(id uint64, ack socket.Ack, timeout *time.Duration) {
	if timeout == nil {
		if s._opts.GetRawAckTimeout() != nil {
			timeout = utils.Ptr(s._opts.AckTimeout())
		}
	}

	if timeout == nil {
		s.acks.Store(id, ack)
		return
	}

	timer := utils.SetTimeout(func() {
		s.acks.Delete(id)
		s.removeBufferedAck(id)
		socketLog.Debug("event with ack id %d has timed out after %d ms", id, *timeout)
		ack(nil, errors.New("operation has timed out"))
	}, *timeout)

	s.acks.Store(id, func(data []any, err error) {
		utils.ClearTimeout(timer)
		ack(data, err)
	})
}

// EmitWithAck emits an event and waits for an acknowledgement.
//
// Example:
//
//	// without timeout
//	socket.EmitWithAck("hello", "world")(func([]any, error){
//
//	})
//
//	// with a specific timeout
//	socket.Timeout(1000 * time.Millisecond).EmitWithAck("hello", "world")(func([]any, error){
//
//	})
func (s *Socket) EmitWithAck(ev string, args ...any) func(socket.Ack) {
	return func(ack socket.Ack) {
		_ = s.Emit(ev, append(args, ack)...)
	}
}

// _addToQueue adds the packet to the queue.
//
// args: The packet arguments.
func (s *Socket) _addToQueue(args []any, flags *Flags) error {
	args_len := len(args)
	ack, withAck := args[args_len-1].(socket.Ack)
	if withAck {
		args = args[:args_len-1]
	}

	packet := &QueuedPacket{
		Id:    s._queueSeq.Add(1) - 1,
		Flags: cloneQueueFlags(flags),
		Size:  estimateBufferBytes(args),
		Ack:   ack,
	}

	args = append(args, func(responseArgs []any, err error) {
		if q, queueErr := s._queue.Get(0); queueErr != nil || packet != q {
			// the packet has already been acknowledged
			socketLog.Debug("packet [%d] already acknowledged", packet.Id)
			return
		}
		if err != nil {
			if tryCount := packet.TryCount.Load(); float64(tryCount) > s._opts.Retries() {
				socketLog.Debug("packet [%d] is discarded after %d tries", packet.Id, tryCount)
				if !s.shiftRetryQueue(packet) {
					return
				}
				if ack != nil {
					ack(nil, err)
				}
			}
		} else {
			socketLog.Debug("packet [%d] was successfully sent", packet.Id)
			if !s.shiftRetryQueue(packet) {
				return
			}
			if ack != nil {
				ack(responseArgs, nil)
			}
		}
		packet.Pending.Store(false)
		s._drainQueue(false)
	})

	packet.Args = args

	accepted, overflowErr := s.enqueueRetryPacket(packet)
	if !accepted {
		if ack != nil {
			ack(nil, ErrBufferOverflow)
		}
		return overflowErr
	}
	s._drainQueue(false)
	return nil
}

func cloneQueueFlags(flags *Flags) *Flags {
	cloned := &Flags{FromQueue: true}
	if flags == nil {
		return cloned
	}
	cloned.Options = flags.Options
	if flags.Compress != nil {
		compress := *flags.Compress
		cloned.Compress = &compress
	}
	cloned.Volatile = flags.Volatile
	if flags.Timeout != nil {
		timeout := *flags.Timeout
		cloned.Timeout = &timeout
	}
	return cloned
}

// _drainQueue sends the first packet of the queue and waits for an acknowledgement from the server.
//
// force: Whether to resend a packet that has not been acknowledged yet.
func (s *Socket) _drainQueue(force bool) {
	socketLog.Debug("draining queue")
	if !s.connected.Load() || s._queue.Len() == 0 {
		return
	}
	packet, err := s._queue.Get(0)
	if err != nil {
		return
	}

	if !force && packet.Pending.Load() {
		socketLog.Debug("packet [%d] has already been sent and is waiting for an ack", packet.Id)
		return
	}
	packet.Pending.Store(true)
	packet.TryCount.Add(1)
	socketLog.Debug("sending packet [%d] (try n°%d)", packet.Id, packet.TryCount.Load())
	s.flags.Store(packet.Flags)
	_ = s.Emit(slices.TryGetAny[string](packet.Args, 0), slices.Slice(packet.Args, 1)...)
}

// packet sends a packet.
//
// packet: The packet to send.
func (s *Socket) packet(packet *Packet) {
	packet.Nsp = s.nsp
	s.io._packet(packet)
}

// onopen is called upon engine `open`.
func (s *Socket) onopen(...any) {
	socketLog.Debug("transport is open - connecting")
	s.authMu.RLock()
	provider := s.authProvider
	staticAuth := cloneAuth(s.auth)
	s.authMu.RUnlock()
	if provider == nil {
		s._sendConnectPacket(staticAuth)
		return
	}

	s.cancelAuthAttempt()
	attempt := s.authAttempt.Add(1)
	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout := s.io.Timeout(); timeout != nil && *timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, *timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	s.authMu.Lock()
	s.authCancel = cancel
	s.authMu.Unlock()

	go func() {
		auth, err := provider(ctx)
		cancel()
		if attempt != s.authAttempt.Load() {
			return
		}
		if err != nil {
			s.EventEmitter.Emit("connect_error", err)
			return
		}
		s.authMu.Lock()
		defer s.authMu.Unlock()
		if attempt != s.authAttempt.Load() {
			return
		}
		s.auth = cloneAuth(auth)
		s._opts.SetAuth(cloneAuth(auth))
		s._sendConnectPacket(auth)
	}()
}

// _sendConnectPacket sends a CONNECT packet to initiate the Socket.IO session.
//
// data: The data to send.
func (s *Socket) _sendConnectPacket(data map[string]any) {
	data = cloneAuth(data)
	if _pid := s._pid.Load(); _pid != "" {
		if data == nil {
			data = map[string]any{}
		}
		data["pid"] = _pid
		data["offset"] = s._lastOffset.Load()
	}
	s.packet(&Packet{
		Packet: &parser.Packet{
			Type: parser.CONNECT,
			Data: data,
		},
	})
}

// onerror is called upon engine or manager `error`.
//
// errs: The errors.
func (s *Socket) onerror(errs ...any) {
	if !s.connected.Load() {
		s.EventEmitter.Emit("connect_error", errs...)
	}
}

// onclose is called upon engine `close`.
//
// reason: The reason for the close.
// description: The error description.
func (s *Socket) onclose(reason string, description error) {
	socketLog.Debug("close (%s)", reason)
	s.cancelAuthAttempt()
	s.connected.Store(false)
	s.id.Store("")
	s.EventEmitter.Emit("disconnect", reason, description)
	s._clearAcks()
}

func (s *Socket) cancelAuthAttempt() {
	s.authAttempt.Add(1)
	s.authMu.Lock()
	cancel := s.authCancel
	s.authCancel = nil
	s.authMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Socket) removeBufferedAck(id uint64) {
	s.bufferMu.Lock()
	removed, _ := s.sendBuffer.RangeAndSplice(func(p *Packet, index int) (bool, int, int, []*Packet) {
		return p.Id != nil && *p.Id == id, index, 1, nil
	})
	if len(removed) > 0 {
		s.sendBufferBytes.Add(-estimateBufferBytes(removed[0].Packet))
		socketLog.Debug("removing packet with ack id %d from the buffer", id)
	}
	s.bufferMu.Unlock()
}

func cloneAuth(auth map[string]any) map[string]any {
	if auth == nil {
		return nil
	}
	cloned := make(map[string]any, len(auth))
	maps.Copy(cloned, auth)
	return cloned
}

// BufferStats returns the current packet and byte backlog for this namespace.
func (s *Socket) BufferStats() BufferStats {
	return BufferStats{
		SendPackets:    s.sendBuffer.Len(),
		SendBytes:      s.sendBufferBytes.Load(),
		ReceivePackets: s.receiveBuffer.Len(),
		ReceiveBytes:   s.receiveBufferBytes.Load(),
		RetryPackets:   s._queue.Len(),
		RetryBytes:     s.retryQueueBytes.Load(),
	}
}

func estimateBufferBytes(value any) int64 {
	encoded, err := json.Marshal(value)
	if err == nil {
		return int64(len(encoded))
	}
	return int64(len(fmt.Sprintf("%v", value)))
}

func bufferLimitExceeded(packets int, bytes, incomingBytes int64, maxPackets int, maxBytes int64) bool {
	return (maxPackets > 0 && packets+1 > maxPackets) ||
		(maxBytes > 0 && bytes+incomingBytes > maxBytes)
}

func (s *Socket) enqueueSendPacket(packet *Packet) (bool, error) {
	size := estimateBufferBytes(packet.Packet)
	var droppedAcks []socket.Ack

	s.bufferMu.Lock()
	overflowed := false
	for bufferLimitExceeded(s.sendBuffer.Len(), s.sendBufferBytes.Load(), size, s._opts.MaxSendBufferPackets(), s._opts.MaxSendBufferBytes()) {
		overflowed = true
		if s._opts.OverflowStrategy() != OverflowDropOldest || s.sendBuffer.Len() == 0 {
			s.bufferMu.Unlock()
			accepted, overflowErr := s.handleOverflow("send", size)
			if packet.Id != nil {
				if ack, ok := s.acks.LoadAndDelete(*packet.Id); ok {
					ack(nil, ErrBufferOverflow)
				}
			}
			return accepted, overflowErr
		}
		dropped, err := s.sendBuffer.Shift()
		if err != nil {
			break
		}
		s.sendBufferBytes.Add(-estimateBufferBytes(dropped.Packet))
		if dropped.Id != nil {
			if ack, ok := s.acks.LoadAndDelete(*dropped.Id); ok {
				droppedAcks = append(droppedAcks, ack)
			}
		}
	}
	s.sendBuffer.Push(packet)
	s.sendBufferBytes.Add(size)
	s.bufferMu.Unlock()

	for _, ack := range droppedAcks {
		ack(nil, ErrBufferOverflow)
	}
	if overflowed {
		s.emitOverflow("send", size)
	}
	return true, nil
}

func (s *Socket) enqueueReceiveEvent(args []any, size int64) {
	s.bufferMu.Lock()
	overflowed := false
	for bufferLimitExceeded(s.receiveBuffer.Len(), s.receiveBufferBytes.Load(), size, s._opts.MaxReceiveBufferPackets(), s._opts.MaxReceiveBufferBytes()) {
		overflowed = true
		if s._opts.OverflowStrategy() != OverflowDropOldest || s.receiveBuffer.Len() == 0 {
			s.bufferMu.Unlock()
			_, _ = s.handleOverflow("receive", size)
			return
		}
		dropped, err := s.receiveBuffer.Shift()
		if err != nil {
			break
		}
		s.receiveBufferBytes.Add(-estimateBufferBytes(dropped))
	}
	s.receiveBuffer.Push(args)
	s.receiveBufferBytes.Add(size)
	s.bufferMu.Unlock()
	if overflowed {
		s.emitOverflow("receive", size)
	}
}

func (s *Socket) enqueueRetryPacket(packet *QueuedPacket) (bool, error) {
	var dropped []*QueuedPacket
	s.bufferMu.Lock()
	overflowed := false
	for bufferLimitExceeded(s._queue.Len(), s.retryQueueBytes.Load(), packet.Size, s._opts.MaxRetryQueuePackets(), s._opts.MaxRetryQueueBytes()) {
		overflowed = true
		if s._opts.OverflowStrategy() != OverflowDropOldest || s._queue.Len() == 0 {
			s.bufferMu.Unlock()
			return s.handleOverflow("retry", packet.Size)
		}
		oldest, err := s._queue.Shift()
		if err != nil {
			break
		}
		s.retryQueueBytes.Add(-oldest.Size)
		dropped = append(dropped, oldest)
	}
	s._queue.Push(packet)
	s.retryQueueBytes.Add(packet.Size)
	s.bufferMu.Unlock()

	for _, queued := range dropped {
		if queued.Ack != nil {
			queued.Ack(nil, ErrBufferOverflow)
		}
	}
	if overflowed {
		s.emitOverflow("retry", packet.Size)
	}
	return true, nil
}

func (s *Socket) shiftRetryQueue(expected *QueuedPacket) bool {
	s.bufferMu.Lock()
	current, err := s._queue.Get(0)
	if err != nil || current != expected {
		s.bufferMu.Unlock()
		return false
	}
	packet, err := s._queue.Shift()
	if err != nil {
		s.bufferMu.Unlock()
		return false
	}
	s.retryQueueBytes.Add(-packet.Size)
	empty := s._queue.Len() == 0
	s.bufferMu.Unlock()
	if empty {
		s.EventEmitter.Emit("drain", "retry", s.BufferStats())
	}
	return true
}

func (s *Socket) handleOverflow(buffer string, incomingBytes int64) (bool, error) {
	s.emitOverflow(buffer, incomingBytes)
	switch s._opts.OverflowStrategy() {
	case OverflowDropNewest:
		return false, nil
	case OverflowDisconnect:
		s.EventEmitter.Emit("slow_consumer", buffer, s.BufferStats())
		s.Disconnect()
		return false, ErrBufferOverflow
	default:
		return false, ErrBufferOverflow
	}
}

func (s *Socket) emitOverflow(buffer string, incomingBytes int64) {
	s.EventEmitter.Emit("overflow", &OverflowDetails{
		Buffer:        buffer,
		Strategy:      s._opts.OverflowStrategy(),
		IncomingBytes: incomingBytes,
		Stats:         s.BufferStats(),
	})
}

// _clearAcks clears the acknowledgement handlers upon disconnection, since the client will never receive an acknowledgement from the server.
func (s *Socket) _clearAcks() {
	s.acks.Range(func(id uint64, ack socket.Ack) bool {
		isBuffered := false
		s.sendBuffer.FindIndex(func(packet *Packet) bool {
			if packet.Id != nil && *packet.Id == id {
				isBuffered = true
			}
			return isBuffered
		})
		if !isBuffered {
			s.acks.Delete(id)
			ack(nil, errors.New("socket has been disconnected"))
		}
		return true
	})
}

// onpacket is called with socket packet.
//
// packet: The packet.
func (s *Socket) onpacket(packet *parser.Packet) {
	if packet.Nsp != s.nsp {
		return
	}

	switch packet.Type {
	case parser.CONNECT:
		data, _ := packet.Data.(map[string]any)
		handshake, err := processHandshake(data)
		if err != nil || handshake.Sid == "" {
			s.EventEmitter.Emit(
				"connect_error",
				errors.New("it seems you are trying to reach a Socket.IO server in v2.x with a v3.x client, but they are not compatible (more information here: https://socket.io/docs/v3/migrating-from-2-x-to-3-0/)"),
			)
			return
		}
		s.onconnect(handshake.Sid, handshake.Pid)

	case parser.EVENT, parser.BINARY_EVENT:
		s.onevent(packet)

	case parser.ACK, parser.BINARY_ACK:
		s.onack(packet)

	case parser.DISCONNECT:
		s.ondisconnect()

	case parser.CONNECT_ERROR:
		s.destroy()
		data, _ := packet.Data.(map[string]any)
		extendedError, err := processExtendedError(data)
		if err != nil {
			s.EventEmitter.Emit("connect_error", err)
			return
		}
		s.EventEmitter.Emit("connect_error", extendedError.Err())
	}
}

// onevent is called upon a server event.
//
// packet: The packet.
func (s *Socket) onevent(packet *parser.Packet) {
	args, _ := packet.Data.([]any)
	socketLog.Debug("emitting event %v", args)
	bufferSize := estimateBufferBytes(args)

	if nil != packet.Id {
		socketLog.Debug("attaching ack callback to event")
		args = append(args, s.ack(*packet.Id))
	}

	if s.connected.Load() {
		s.emitEvent(args)
	} else {
		s.enqueueReceiveEvent(args, bufferSize)
	}
}

func (s *Socket) emitEvent(args []any) {
	for _, listener := range s._anyListeners.All() {
		listener(args...)
	}
	s.EventEmitter.Emit(types.EventName(slices.TryGetAny[string](args, 0)), slices.Slice(args, 1)...)
	if _pid := s._pid.Load(); _pid != "" {
		if args_len := len(args); args_len > 0 {
			if lastOffset, ok := args[args_len-1].(string); ok {
				s._lastOffset.Store(lastOffset)
			}
		}
	}
}

// ack produces an ack callback to emit with an event.
//
// id: The ack ID.
func (s *Socket) ack(id uint64) socket.Ack {
	sent := &sync.Once{}
	return func(args []any, _ error) {
		// prevent double callbacks
		sent.Do(func() {
			socketLog.Debug("sending ack %v", args)
			s.packet(&Packet{
				Packet: &parser.Packet{
					Type: parser.ACK,
					Id:   &id,
					Data: args,
				},
			})
		})
	}
}

// onack is called upon a server acknowledgement.
//
// packet: The packet.
func (s *Socket) onack(packet *parser.Packet) {
	if packet.Id == nil {
		socketLog.Debug("bad ack nil")
		return
	}
	ack, ok := s.acks.Load(*packet.Id)
	if !ok {
		socketLog.Debug("bad ack %d", *packet.Id)
		return
	}
	s.acks.Delete(*packet.Id)
	socketLog.Debug("calling ack %d with %v", *packet.Id, packet.Data)
	ack(utils.TryCast[[]any](packet.Data), nil)
}

// onconnect is called upon server connect.
//
// id: The connection ID.
// pid: The private session ID.
func (s *Socket) onconnect(id string, pid string) {
	socketLog.Debug("socket connected with id %s", id)
	s.id.Store(id)
	s.recovered.Store(pid != "" && s._pid.Load() == pid)
	s._pid.Store(pid) // defined only if connection state recovery is enabled
	s.connected.Store(true)
	s.emitBuffered()
	s._drainQueue(true)
	s.EventEmitter.Emit("connect")
}

// emitBuffered emits buffered events (received and emitted).
func (s *Socket) emitBuffered() {
	s.bufferMu.Lock()
	received := s.receiveBuffer.All()
	sent := s.sendBuffer.All()
	s.receiveBuffer.Clear()
	s.sendBuffer.Clear()
	s.receiveBufferBytes.Store(0)
	s.sendBufferBytes.Store(0)
	s.bufferMu.Unlock()

	for _, args := range received {
		s.emitEvent(args)
	}
	for _, packet := range sent {
		s.notifyOutgoingListeners(packet)
		s.packet(packet)
	}
	if len(received) > 0 {
		s.EventEmitter.Emit("drain", "receive", s.BufferStats())
	}
	if len(sent) > 0 {
		s.EventEmitter.Emit("drain", "send", s.BufferStats())
	}
}

// ondisconnect is called upon server disconnect.
func (s *Socket) ondisconnect() {
	socketLog.Debug("server disconnect (%s)", s.nsp)
	s.destroy()
	s.onclose("io server disconnect", nil)
}

// destroy is called upon forced client/server side disconnections.
// This method ensures the manager stops tracking us and
// that reconnections don't get triggered for s.
func (s *Socket) destroy() {
	if subs := s.subs.Load(); subs != nil {
		// clean subscriptions to avoid reconnections
		subs.DoWrite(func(subDestroys []func()) []func() {
			for _, subDestroy := range subDestroys {
				subDestroy()
			}
			return subDestroys[:0]
		})
		s.subs.Store(nil)
	}
	s.io._destroy(s)
}

// Disconnect disconnects the socket manually. In that case, the socket will not try to reconnect.
//
// If this is the last active Socket instance of the [Manager], the low-level connection will be closed.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.On("disconnect", func(reason ...any) {
//	  fmt.Println(reason[0]) // prints "io client disconnect"
//	})
//
//	socket.Disconnect()
func (s *Socket) Disconnect() *Socket {
	if s.connected.Load() {
		socketLog.Debug("performing disconnect (%s)", s.nsp)
		s.packet(&Packet{Packet: &parser.Packet{Type: parser.DISCONNECT}})

		// fire events
		defer s.onclose("io client disconnect", nil)
	}

	// remove socket from pool
	s.destroy()

	return s
}

// Alias for [Socket.Disconnect].
func (s *Socket) Close() *Socket {
	return s.Disconnect()
}

// Compress sets the compress flag.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.Compress(false).Emit("hello")
//
// compress: If `true`, compresses the sending data.
func (s *Socket) Compress(compress bool) *Socket {
	s.flags.Load().Compress = &compress
	return s
}

// Volatile sets a modifier for a subsequent event emission that the event message will be dropped when this socket is not
// ready to send messages.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.Volatile().Emit("hello") // the server may or may not receive it
func (s *Socket) Volatile() *Socket {
	s.flags.Load().Volatile = true
	return s
}

// Timeout sets a modifier for a subsequent event emission that the callback will be called with an error when the
// given number of milliseconds have elapsed without an acknowledgement from the server:
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.Timeout(5000 * time.Millisecond).Emit("my-event", func([]any, err) {
//	  if err != nil {
//	    // the server did not acknowledge the event in the given delay
//	  }
//	})
func (s *Socket) Timeout(timeout time.Duration) *Socket {
	s.flags.Load().Timeout = &timeout
	return s
}

// OnAny adds a listener that will be fired when any event is emitted. The event name is passed as the first argument to the
// callback.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.OnAny(func(...any) {
//	  fmt.Println("got event")
//	})
func (s *Socket) OnAny(listener types.EventListener) *Socket {
	s._anyListeners.Push(listener)
	return s
}

// PrependAny adds a listener that will be fired when any event is emitted. The event name is passed as the first argument to the
// callback. The listener is added to the beginning of the listeners array.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.PrependAny(func(...any) {
//	  fmt.Println("got event")
//	})
func (s *Socket) PrependAny(listener types.EventListener) *Socket {
	s._anyListeners.Unshift(listener)
	return s
}

// OffAny removes the listener that will be fired when any event is emitted.
//
// Example:
//
//	catchAllListener := func(...any) {
//	  fmt.Println("got event")
//	}
//
//	socket := io.NewClient("", nil)
//	socket.OnAny(catchAllListener)
//
//	// remove a specific listener
//	socket.OffAny(catchAllListener)
//
//	// or remove all listeners
//	socket.OffAny()
func (s *Socket) OffAny(listener types.EventListener) *Socket {
	if listener != nil {
		listenerPointer := reflect.ValueOf(listener).Pointer()
		_, _ = s._anyListeners.RangeAndSplice(func(_listener types.EventListener, i int) (bool, int, int, []types.EventListener) {
			return reflect.ValueOf(_listener).Pointer() == listenerPointer, i, 1, nil
		})
	} else {
		s._anyListeners.Clear()
	}
	return s
}

// ListenersAny returns an array of listeners that are listening for any event that is specified. This array can be manipulated,
// e.g. to remove listeners.
func (s *Socket) ListenersAny() []types.EventListener {
	return s._anyListeners.All()
}

// OnAnyOutgoing adds a listener that will be fired when any event is emitted. The event name is passed as the first argument to the
// callback.
//
// Note: acknowledgements sent to the server are not included.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.OnAnyOutgoing(func(...any) {
//	  fmt.Println("sent event")
//	})
func (s *Socket) OnAnyOutgoing(listener types.EventListener) *Socket {
	s._anyOutgoingListeners.Push(listener)
	return s
}

// PrependAnyOutgoing adds a listener that will be fired when any event is emitted. The event name is passed as the first argument to the
// callback. The listener is added to the beginning of the listeners array.
//
// Note: acknowledgements sent to the server are not included.
//
// Example:
//
//	socket := io.NewClient("", nil)
//	socket.PrependAnyOutgoing(func(...any) {
//	  fmt.Println("sent event")
//	})
func (s *Socket) PrependAnyOutgoing(listener types.EventListener) *Socket {
	s._anyOutgoingListeners.Unshift(listener)
	return s
}

// OffAnyOutgoing removes the listener that will be fired when any event is emitted.
//
// Example:
//
//	catchAllListener := func(...any) {
//	  fmt.Println("sent event")
//	}
//
//	socket := io.NewClient("", nil)
//	socket.OnAnyOutgoing(catchAllListener)
//
//	// remove a specific listener
//	socket.OffAnyOutgoing(catchAllListener)
//
//	// or remove all listeners
//	socket.OffAnyOutgoing()
func (s *Socket) OffAnyOutgoing(listener types.EventListener) *Socket {
	if listener != nil {
		listenerPointer := reflect.ValueOf(listener).Pointer()
		_, _ = s._anyOutgoingListeners.RangeAndSplice(func(_listener types.EventListener, i int) (bool, int, int, []types.EventListener) {
			return reflect.ValueOf(_listener).Pointer() == listenerPointer, i, 1, nil
		})
	} else {
		s._anyOutgoingListeners.Clear()
	}
	return s
}

// ListenersAnyOutgoing returns an array of listeners that are listening for any event that is specified. This array can be manipulated,
// e.g. to remove listeners.
func (s *Socket) ListenersAnyOutgoing() []types.EventListener {
	return s._anyOutgoingListeners.All()
}

// notifyOutgoingListeners notifies the listeners for each packet sent.
//
// packet: The packet.
func (s *Socket) notifyOutgoingListeners(packet *Packet) {
	if s._anyOutgoingListeners.Len() > 0 {
		for _, listener := range s._anyOutgoingListeners.All() {
			if args, ok := packet.Data.([]any); ok {
				listener(args...)
			} else {
				listener(packet.Data)
			}
		}
	}
}
