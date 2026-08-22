package socket

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/parsers/socket/v4/parser"
	"github.com/aqcool/socket.io/servers/engine/v4"
	"github.com/aqcool/socket.io/v4/pkg/log"
	"github.com/aqcool/socket.io/v4/pkg/queue"
	"github.com/aqcool/socket.io/v4/pkg/slices"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/aqcool/socket.io/v4/pkg/utils"
)

var (
	socketLog                      = log.NewLog("socket.io:socket")
	SOCKET_RESERVED_EVENTS         = types.NewSet("connect", "connect_error", "disconnect", "disconnecting", "newListener", "removeListener")
	RECOVERABLE_DISCONNECT_REASONS = types.NewSet("transport error", "transport close", "forced close", "ping timeout", "server shutting down", "forced server close")
)

type (
	Ack = func([]any, error)

	SocketMiddleware = func([]any, func(error))
	JoinMiddleware   = func(*Socket, []Room) error

	// Handshake represents the initial connection information exchanged
	// between the client and server during the handshake process.
	Handshake struct {
		// HTTP headers sent by the client
		Headers types.IncomingHttpHeaders `json:"headers" msgpack:"headers"`

		// Creation time as a human-readable string
		Time string `json:"time" msgpack:"time"`

		// Client's IP address
		Address string `json:"address" msgpack:"address"`

		// Indicates if the connection is cross-domain
		Xdomain bool `json:"xdomain" msgpack:"xdomain"`

		// Indicates if the connection is established over TLS/HTTPS
		Secure bool `json:"secure" msgpack:"secure"`

		// Creation time as a Unix timestamp (seconds since epoch)
		Issued int64 `json:"issued" msgpack:"issued"`

		// Full request URL
		Url string `json:"url" msgpack:"url"`

		// Query parameters
		Query types.ParsedUrlQuery `json:"query" msgpack:"query"`

		// Authentication data map[string]any or nil
		Auth map[string]any `json:"auth" msgpack:"auth"`
	}

	// This is the main object for interacting with a client.
	//
	// A Socket belongs to a given [Namespace] and uses an underlying [Client] to communicate.
	//
	// Within each [Namespace], you can also define arbitrary channels (called "rooms") that the [Socket] can
	// join and leave. That provides a convenient way to broadcast to a group of socket instances.
	//
	//	io.On("connection", func(args ...any) {
	//		socket := args[0].(*socket.Socket)
	//
	//		utils.Log().Info(`socket %s connected`, socket.Id())
	//
	//		// send an event to the client
	//		socket.Emit("foo", "bar")
	//
	//		socket.On("foobar", func(...any) {
	//			// an event was received from the client
	//		})
	//
	//		// join the room named "room1"
	//		socket.Join("room1")
	//
	//		// broadcast to everyone in the room named "room1"
	//		io.to("room1").Emit("hello")
	//
	//		// upon disconnection
	//		socket.On("disconnect", func(reason ...any) {
	//			utils.Log().Info(`socket %s disconnected due to %s`, socket.Id(), reason[0])
	//		})
	//	})
	Socket struct {
		*StrictEventEmitter

		nsp    Namespace
		client *Client

		// An unique identifier for the session.
		id SocketId
		// Whether the connection state was recovered after a temporary disconnection. In that case, any missed packets will
		// be transmitted to the client, the data attribute and the rooms will be restored.
		recovered bool
		// The handshake details.
		handshake *Handshake

		// Additional information that can be attached to the Socket instance and which will be used in the
		// [Server.fetchSockets()] method.
		data atomic.Pointer[any]

		// Whether the socket is currently connected or not.
		//
		//	io.Use(func(socket *socket.Socket, next func(*ExtendedError)) {
		//		fmt.Println(socket.Connected()) // false
		//		next(nil)
		//	})
		//
		//	io.On("connection", func(args ...any) {
		//		socket := args[0].(*socket.Socket)
		//		fmt.Println(socket.Connected()) // true
		//	})
		connected atomic.Bool
		// closeMu serializes competing transport, namespace and server shutdown
		// paths so the disconnect lifecycle is emitted exactly once.
		closeMu sync.Mutex
		// connectReady prevents a fast client from dispatching its first event while
		// Namespace connection handlers are still being installed. Node.js gets
		// this ordering from its single-threaded event loop; Go needs an explicit
		// barrier between the CONNECT packet and concurrent transport readers.
		connectReady     chan struct{}
		connectReadyOnce sync.Once

		// The session ID, which must not be shared (unlike [id]).
		pid PrivateSessionId

		// TODO: remove this unused reference
		server                *Server
		adapter               Adapter
		acks                  *types.Map[uint64, Ack]
		fns                   *types.Slice[SocketMiddleware]
		joinFns               *types.Slice[JoinMiddleware]
		flags                 atomic.Pointer[BroadcastFlags]
		_anyListeners         *types.Slice[types.EventListener]
		_anyOutgoingListeners *types.Slice[types.EventListener]

		canJoin atomic.Bool
		// joinMu serializes Join (AddAll) against _cleanup (DelAll).
		// Join holds a read lock for the duration of adapter.AddAll so that
		// _cleanup's write lock waits for any in-flight AddAll to finish before
		// leaveAll/DelAll runs — preventing orphaned rooms[room] entries.
		joinMu sync.RWMutex

		taskQueue *queue.Queue
	}
)

func MakeSocket() *Socket {
	s := makeSocket()
	s.markConnectReady()
	return s
}

func makeSocket() *Socket {
	s := &Socket{
		StrictEventEmitter: NewStrictEventEmitter(),

		// Initialize default value
		acks:                  &types.Map[uint64, Ack]{},
		fns:                   types.NewSlice[SocketMiddleware](),
		joinFns:               types.NewSlice[JoinMiddleware](),
		_anyListeners:         types.NewSlice[types.EventListener](),
		_anyOutgoingListeners: types.NewSlice[types.EventListener](),
		taskQueue:             queue.New(),
		connectReady:          make(chan struct{}),
	}
	s.flags.Store(&BroadcastFlags{})
	s.canJoin.Store(true)

	return s
}

func NewSocket(nsp Namespace, client *Client, auth map[string]any, previousSession *Session) *Socket {
	s := makeSocket()

	s.Construct(nsp, client, auth, previousSession)

	return s
}

func (s *Socket) markConnectReady() {
	s.connectReadyOnce.Do(func() {
		close(s.connectReady)
	})
}

// An unique identifier for the session.
func (s *Socket) Id() SocketId {
	return s.id
}

// Whether the connection state was recovered after a temporary disconnection. In that case, any missed packets will
// be transmitted to the client, the data attribute and the rooms will be restored.
func (s *Socket) Recovered() bool {
	return s.recovered
}

// The handshake details.
func (s *Socket) Handshake() *Handshake {
	return s.handshake
}

// Additional information that can be attached to the Socket instance and which will be used in the
// [Server.fetchSockets()] method.
func (s *Socket) SetData(data any) {
	s.data.Store(&data)
}
func (s *Socket) Data() any {
	if data := s.data.Load(); data != nil {
		return *data
	}
	return nil
}

// Whether the socket is currently connected or not.
//
//	io.Use(func(socket *socket.Socket, next func(*ExtendedError)) {
//		fmt.Println(socket.Connected()) // false
//		next(nil)
//	})
//
//	io.On("connection", func(args ...any) {
//		socket := args[0].(*socket.Socket)
//		fmt.Println(socket.Connected()) // true
//	})
func (s *Socket) Connected() bool {
	return s.connected.Load()
}

func (s *Socket) Acks() *types.Map[uint64, Ack] {
	return s.acks
}

func (s *Socket) Nsp() Namespace {
	return s.nsp
}

func (s *Socket) Client() *Client {
	return s.client
}

func (s *Socket) Construct(nsp Namespace, client *Client, auth map[string]any, previousSession *Session) {
	s.nsp = nsp
	s.client = client

	s.server = nsp.Server()
	s.adapter = s.nsp.Adapter()
	if previousSession != nil {
		s.id = previousSession.Sid
		s.pid = previousSession.Pid
		for _, room := range previousSession.Rooms.Keys() {
			s.Join(room)
		}
		s.SetData(previousSession.Data)
		for _, packet := range previousSession.MissedPackets {
			s.packet(&parser.Packet{
				Type: parser.EVENT,
				Data: packet,
			}, nil)
		}
		s.recovered = true
	} else {
		if client.conn.Protocol() == 3 {
			if name := nsp.Name(); name != "/" {
				s.id = SocketId(name + "#" + client.id)
			} else {
				s.id = SocketId(client.id)
			}
		} else {
			s.id = SocketId(utils.Base64Id().GenerateId()) // don't reuse the Engine.IO id because it's sensitive information
		}
		if s.server.Opts().ConnectionStateRecovery() != nil {
			s.pid = PrivateSessionId(utils.Base64Id().GenerateId())
		}
	}
	s.handshake = s.buildHandshake(auth)

	// prevents crash when the socket receives an "error" event without listener
	//
	// Golang defines the error by itself. It seems that this logic is not needed?
	_ = s.On("error", func(...any) {})
}

// Builds the `handshake` BC object
func (s *Socket) buildHandshake(auth map[string]any) *Handshake {
	return buildHandshake(s.Request(), s.Conn().RemoteAddress(), auth)
}

func buildHandshake(request *types.HttpContext, address string, auth map[string]any) *Handshake {
	issued := time.Now()
	handshake := &Handshake{
		Headers: types.IncomingHttpHeaders{},
		Time:    issued.Format(time.RFC3339),
		Address: address,
		Issued:  issued.UnixMilli(),
		Query:   types.ParsedUrlQuery{},
		Auth:    auth,
	}
	if request == nil {
		// A WebTransport-only connection does not have an Engine.IO HTTP
		// request by the time the Socket.IO namespace is created. WebTransport
		// always runs over secure HTTP/3, matching the official fallback.
		handshake.Secure = true
		return handshake
	}

	handshake.Headers = compactHandshakeValues(request.Headers().All(), true)
	handshake.Xdomain = request.Headers().Peek("Origin") != ""
	handshake.Secure = request.Secure()
	handshake.Url = request.Request().RequestURI
	handshake.Query = compactHandshakeValues(request.Query().All(), false)
	return handshake
}

func compactHandshakeValues(values map[string][]string, lowerKeys bool) map[string]any {
	result := make(map[string]any, len(values))
	for key, items := range values {
		if lowerKeys {
			key = strings.ToLower(key)
		}
		switch len(items) {
		case 0:
			result[key] = ""
		case 1:
			result[key] = items[0]
		default:
			result[key] = append([]string(nil), items...)
		}
	}
	return result
}

// Emits to this client.
//
//	io.On("connection", func(args ...any) {
//		socket := args[0].(*socket.Socket)
//		socket.Emit("hello", "world")
//
//		// all serializable datastructures are supported (no need to call json.Marshal, But the map can only be of `map[string]any` type, currently does not support other types of maps)
//		socket.Emit("hello", 1, "2", map[string]any{"3": []string{"4"}, "5": types.NewBytesBuffer([]byte{6})})
//
//		// with an acknowledgement from the client
//		socket.Emit("hello", "world", func(args []any, err error) {
//			// ...
//		})
//	})
func (s *Socket) Emit(ev string, args ...any) error {
	if SOCKET_RESERVED_EVENTS.Has(ev) {
		return fmt.Errorf(`"%s" is a reserved event name`, ev)
	}
	data := append([]any{ev}, args...)
	data_len := len(data)
	packet := &parser.Packet{
		Type: parser.EVENT,
		Data: data,
	}
	flags := *s.flags.Swap(&BroadcastFlags{})

	// access last argument to see if it's an ACK callback
	if fn, ok := data[data_len-1].(Ack); ok {
		id := s.nsp.Ids()
		socketLog.Debug("emitting packet with ack id %d", id)
		packet.Data = data[:data_len-1]
		s.registerAckCallback(id, ev, telemetryTraceMetadata(data), fn, flags.Timeout)
		packet.Id = &id
	}

	if s.nsp.Server().Opts().ConnectionStateRecovery() != nil {
		// this ensures the packet is stored and can be transmitted upon reconnection
		s.adapter.Broadcast(packet, &BroadcastOptions{
			Rooms:  types.NewSet(Room(s.id)),
			Except: types.NewSet[Room](),
			Flags:  &flags,
		})
	} else {
		s.notifyOutgoingListeners(packet)
		s.packet(packet, &flags)
	}

	return nil
}

// Emits an event and waits for an acknowledgement
//
//	io.On("connection", func(args ...any) => {
//		client := args[0].(*socket.Socket)
//		// without timeout
//		client.EmitWithAck("hello", "world")(func(args []any, err error) {
//			if err == nil {
//				fmt.Println(args) // one response per client
//			} else {
//				// some clients did not acknowledge the event in the given delay
//			}
//		})
//
//		// with a specific timeout
//		client.Timeout(1000 * time.Millisecond).EmitWithAck("hello", "world")(func(args []any, err error) {
//			if err == nil {
//				fmt.Println(args) // one response per client
//			} else {
//				// some clients did not acknowledge the event in the given delay
//			}
//		})
//	})
//
// Return:  a `func(socket.Ack)` that will be fulfilled when all clients have acknowledged the event
func (s *Socket) EmitWithAck(ev string, args ...any) func(Ack) {
	return func(ack Ack) {
		_ = s.Emit(ev, append(args, ack)...)
	}
}

func (s *Socket) registerAckCallback(
	id uint64,
	eventName string,
	traceMetadata map[string]string,
	ack Ack,
	timeout *time.Duration,
) {
	startedAt := time.Now()
	if timeout == nil {
		s.acks.Store(id, func(args []any, err error) {
			s.emitTelemetry(&TelemetryEvent{
				Kind:          TelemetryAckCompleted,
				Event:         eventName,
				Duration:      time.Since(startedAt),
				Success:       err == nil,
				TraceMetadata: traceMetadata,
			})
			ack(args, err)
		})
		return
	}
	// Store the callback before starting the timer. With a zero timeout, the
	// previous order allowed the timer to run first, observe no callback and
	// leave a subsequently stored ACK pending forever. The one-slot channel
	// also lets an extremely fast ACK wait until its timer is available without
	// racing on the timer pointer.
	timerReady := make(chan *utils.Timer, 1)
	s.acks.Store(id, func(args []any, _ error) {
		timer := <-timerReady
		utils.ClearTimeout(timer)
		s.emitTelemetry(&TelemetryEvent{
			Kind:          TelemetryAckCompleted,
			Event:         eventName,
			Duration:      time.Since(startedAt),
			Success:       true,
			TraceMetadata: traceMetadata,
		})
		ack(args, nil)
	})
	timer := utils.SetTimeout(func() {
		if _, loaded := s.acks.LoadAndDelete(id); !loaded {
			return
		}
		socketLog.Debug("event with ack id %d has timed out after %d ms", id, *timeout/time.Millisecond)
		s.emitTelemetry(&TelemetryEvent{
			Kind:          TelemetryAckTimeout,
			Event:         eventName,
			Duration:      time.Since(startedAt),
			Success:       false,
			TraceMetadata: traceMetadata,
		})
		ack(nil, errors.New("operation has timed out"))
	}, *timeout)
	timerReady <- timer
}

// To targets a room when broadcasting. Returns a new BroadcastOperator for chaining.
func (s *Socket) To(room ...Room) *BroadcastOperator {
	return s.newBroadcastOperator().To(room...)
}

// In targets a room when broadcasting. Returns a new BroadcastOperator for chaining.
func (s *Socket) In(room ...Room) *BroadcastOperator {
	return s.newBroadcastOperator().In(room...)
}

// Except excludes a room when broadcasting. Returns a new BroadcastOperator for chaining.
func (s *Socket) Except(room ...Room) *BroadcastOperator {
	return s.newBroadcastOperator().Except(room...)
}

// Sends a `message` event.
//
// This method mimics the WebSocket.send() method.
//
// See: https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/send
//
//	io.On("connection", func(clients ...any) {
//		socket := clients[0].(*socket.Socket)
//		socket.Send("hello");
//
//		// this is equivalent to
//		socket.Emit("message", "hello");
//	});
func (s *Socket) Send(args ...any) *Socket {
	_ = s.Emit("message", args...)
	return s
}

// Sends a `message` event. Alias of Send.
func (s *Socket) Write(args ...any) *Socket {
	_ = s.Emit("message", args...)
	return s
}

// Writes a packet.
//
// Param:  packet - packet struct
//
// Param:  opts - options
func (s *Socket) packet(packet *parser.Packet, opts *BroadcastFlags) {
	packet.Nsp = s.nsp.Name()
	if opts == nil {
		opts = &BroadcastFlags{}
	}
	s.client._packet(packet, &opts.WriteOptions)
}

// Join adds the socket to one or more rooms.
func (s *Socket) Join(rooms ...Room) {
	_ = s.TryJoin(rooms...)
}

// TryJoin adds rooms and returns a validation error from join middleware.
func (s *Socket) TryJoin(rooms ...Room) error {
	s.joinMu.RLock()
	defer s.joinMu.RUnlock()
	if !s.canJoin.Load() {
		return errors.New("socket.io: socket is closing")
	}
	for _, middleware := range s.joinFns.All() {
		if err := middleware(s, rooms); err != nil {
			return err
		}
	}
	socketLog.Debug("join room %s", rooms)
	s.adapter.AddAll(s.id, types.NewSet(rooms...))
	return nil
}

// UseJoinMiddleware validates future room joins before they reach the Adapter.
func (s *Socket) UseJoinMiddleware(middleware JoinMiddleware) *Socket {
	if middleware != nil {
		s.joinFns.Push(middleware)
	}
	return s
}

// Leave removes the socket from a room.
func (s *Socket) Leave(room Room) {
	socketLog.Debug("leave room %s", room)
	s.adapter.Del(s.id, room)
}

// Leave all rooms.
func (s *Socket) leaveAll() {
	s.adapter.DelAll(s.id)
}

// Called by `Namespace` upon successful
// middleware execution (ie authorization).
// Socket is added to namespace array before
// call to join, so adapters can access it.
func (s *Socket) _onconnect() {
	socketLog.Debug("socket connected - writing packet")

	s.connected.Store(true)

	s.Join(Room(s.id))
	if s.Conn().Protocol() == 3 {
		s.packet(&parser.Packet{
			Type: parser.CONNECT,
		}, nil)
	} else {
		s.packet(&parser.Packet{
			Type: parser.CONNECT,
			Data: &struct {
				Sid SocketId         `json:"sid" msgpack:"sid"`
				Pid PrivateSessionId `json:"pid,omitempty" msgpack:"pid,omitempty"`
			}{
				Sid: s.id,
				Pid: s.pid,
			},
		}, nil)
	}
}

// Called with each packet. Called by `Client`.
func (s *Socket) _onpacket(packet *parser.Packet) {
	// A client can answer the namespace CONNECT packet on another goroutine
	// before Namespace._doConnect() has finished firing the user connection
	// callbacks. Wait for that setup to complete so the first event is never
	// observed without its handlers.
	<-s.connectReady

	socketLog.Debug("got packet %v", packet)
	switch packet.Type {
	case parser.EVENT:
		s.onevent(packet)
	case parser.BINARY_EVENT:
		s.onevent(packet)
	case parser.ACK:
		s.onack(packet)
	case parser.BINARY_ACK:
		s.onack(packet)
	case parser.DISCONNECT:
		s.ondisconnect()
	}
}

// Called upon event packet.
//
// Param:  packet - packet struct
func (s *Socket) onevent(packet *parser.Packet) {
	startedAt := time.Now()
	args, ok := packet.Data.([]any)
	if !ok {
		socketLog.Debug("invalid event packet data format")
		return
	}
	socketLog.Debug("emitting event %v", args)
	eventName := slices.TryGetAny[string](args, 0)
	if nil != packet.Id {
		socketLog.Debug("attaching ack callback to event")
		args = append(args, s.ack(*packet.Id, eventName, startedAt))
	}
	for _, listener := range s._anyListeners.All() {
		listener(args...)
	}
	s.dispatch(args, startedAt)
}

// Produces an ack callback to emit with an event.
//
// Param: id - packet id
func (s *Socket) ack(id uint64, eventName string, startedAt time.Time) Ack {
	sent := &sync.Once{}
	return func(args []any, _ error) {
		// prevent double callbacks
		sent.Do(func() {
			if !s.Connected() {
				socketLog.Debug("socket disconnected, skipping ack %d", id)
				return
			}
			socketLog.Debug("sending ack %v", args)
			s.packet(&parser.Packet{
				Id:   &id,
				Type: parser.ACK,
				Data: args,
			}, nil)
			s.emitTelemetry(&TelemetryEvent{
				Kind:     TelemetryAckCompleted,
				Event:    eventName,
				Duration: time.Since(startedAt),
				Success:  true,
			})
		})
	}
}

// Called upon ack packet.
func (s *Socket) onack(packet *parser.Packet) {
	if packet.Id != nil {
		if ack, ok := s.acks.LoadAndDelete(*packet.Id); ok {
			socketLog.Debug("calling ack %d with %v", *packet.Id, packet.Data)
			ack(utils.TryCast[[]any](packet.Data), nil)
		} else {
			socketLog.Debug("bad ack %d", *packet.Id)
		}
	} else {
		socketLog.Debug("bad ack nil")
	}
}

// Called upon client disconnect packet.
func (s *Socket) ondisconnect() {
	socketLog.Debug("got disconnect packet")
	s._onclose("client namespace disconnect")
}

// Handles a client error.
func (s *Socket) _onerror(err any) {
	// FIXME the meaning of the "error" event is overloaded:
	//  - it can be sent by the client (`socket.emit("error")`)
	//  - it can be emitted when the connection encounters an error (an invalid packet for example)
	//  - it can be emitted when a packet is rejected in a middleware (`socket.use()`)
	s.EmitReserved("error", err)
}

// Called upon closing. Called by `Client`.
//
// Param: reason
// Param: description
func (s *Socket) _onclose(args ...any) {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.oncloseLocked(args...)
}

// oncloseLocked completes the disconnect lifecycle while closeMu is held.
// Keeping packet emission and state transition in one critical section is
// required for server namespace disconnects: the peer may close the transport
// immediately after receiving DISCONNECT, and that transport callback must not
// persist the session under a different, recoverable reason.
func (s *Socket) oncloseLocked(args ...any) {
	if !s.Connected() {
		return
	}
	socketLog.Debug("closing socket - reason %v", slices.TryGet(args, 0))
	s.EmitReserved("disconnecting", args...)

	if s.server.Opts().ConnectionStateRecovery() != nil && RECOVERABLE_DISCONNECT_REASONS.Has(slices.TryGetAny[string](args, 0)) {
		socketLog.Debug("connection state recovery is enabled for sid %s", s.id)
		s.adapter.PersistSession(&SessionToPersist{
			Sid:   s.id,
			Pid:   s.pid,
			Rooms: types.NewSet(s.Rooms().Keys()...),
			Data:  s.Data(),
		})
	}
	s._cleanup()
	s.client._remove(s)
	s.connected.Store(false)
	s.emitTelemetry(&TelemetryEvent{
		Kind:    TelemetryDisconnection,
		Reason:  slices.TryGetAny[string](args, 0),
		Success: true,
	})
	s.EmitReserved("disconnect", args...)
}

// Makes the socket leave all the rooms it was part of and prevents it from joining any other room
func (s *Socket) _cleanup() {
	// Acquire the write lock to wait for any in-flight Join/AddAll to complete,
	// then set canJoin=false so no new Joins can start after we release.
	// leaveAll/DelAll then runs without risk of a concurrent AddAll adding rooms
	// back into adapter.rooms after they've been removed — which would orphan them.
	s.joinMu.Lock()
	s.canJoin.Store(false)
	s.joinMu.Unlock()

	s.leaveAll()
	s.nsp.Remove(s)
	// Clear pending ack callbacks to prevent memory leaks
	s.acks.Clear()
	s.taskQueue.TryClose()
}

// Enqueue adds a task to the socket's sequential task queue for ordered execution.
// This ensures that events and packet processing for this socket are serialized,
// preventing race conditions from Go's preemptive scheduling.
func (s *Socket) Enqueue(task func()) {
	s.taskQueue.Enqueue(task)
}

// Produces an `error` packet.
func (s *Socket) _error(err any) {
	s.packet(&parser.Packet{
		Type: parser.CONNECT_ERROR,
		Data: err,
	}, nil)
}

// Disconnects this client.
//
//	io.On("connection", func(clients ...any) {
//		socket := clients[0].(*socket.Socket)
//		// disconnect this socket (the connection might be kept alive for other namespaces)
//		socket.Disconnect(false)
//
//		// disconnect this socket and close the underlying connection
//		socket.Disconnect(true)
//	})
//
// Param: status - if `true`, closes the underlying connection
func (s *Socket) Disconnect(status bool) *Socket {
	if !s.Connected() {
		return s
	}
	if status {
		s.client._disconnect()
	} else {
		s.closeMu.Lock()
		defer s.closeMu.Unlock()
		if !s.Connected() {
			return s
		}
		s.packet(&parser.Packet{
			Type: parser.DISCONNECT,
		}, nil)
		s.oncloseLocked("server namespace disconnect")
	}
	return s
}

// Sets the compress flag.
//
//	io.On("connection", func(clients ...any) {
//		socket := clients[0].(*socket.Socket)
//		socket.Compress(false).Emit("hello")
//	})
//
// Param: compress - if `true`, compresses the sending data
func (s *Socket) Compress(compress bool) *Socket {
	s.flags.Load().Compress = &compress
	return s
}

// Sets a modifier for a subsequent event emission that the event data may be lost if the client is not ready to
// receive messages (because of network slowness or other issues, or because they’re connected through long polling
// and is in the middle of a request-response cycle).
//
//	io.On("connection", func(clients ...any) {
//		socket := clients[0].(*socket.Socket)
//		socket.Volatile().Emit("hello") // the client may or may not receive it
//	})
func (s *Socket) Volatile() *Socket {
	s.flags.Load().Volatile = true
	return s
}

// Sets a modifier for a subsequent event emission that the event data will only be broadcast to every sockets but the
// sender.
//
//	io.On("connection", func(clients ...any) {
//		socket := clients[0].(*socket.Socket)
//		// the “foo” event will be broadcast to all connected clients, except this socket
//		socket.Broadcast().Emit("foo", "bar")
//	})
//
// Return: a new [BroadcastOperator] instance for chaining
func (s *Socket) Broadcast() *BroadcastOperator {
	return s.newBroadcastOperator()
}

// Sets a modifier for a subsequent event emission that the event data will only be broadcast to the current node.
//
//	io.On("connection", func(clients ...any) {
//		socket := clients[0].(*socket.Socket)
//		// the “foo” event will be broadcast to all connected clients on this node, except this socket
//		socket.Local().Emit("foo", "bar")
//	})
//
// Return: a new [BroadcastOperator] instance for chaining
func (s *Socket) Local() *BroadcastOperator {
	return s.newBroadcastOperator().Local()
}

// Sets a modifier for a subsequent event emission that the callback will be called with an error when the
// given number of milliseconds have elapsed without an acknowledgement from the client:
//
//	io.On("connection", func(clients ...any) {
//		socket := clients[0].(*socket.Socket)
//		socket.Timeout(1000 * time.Millisecond).Emit("my-event", func(args []any, err error) {
//			if err != nil {
//				// the client did not acknowledge the event in the given delay
//			}
//		})
//	})
func (s *Socket) Timeout(timeout time.Duration) *Socket {
	s.flags.Load().Timeout = &timeout
	return s
}

// Dispatch incoming event to socket listeners.
func (s *Socket) dispatch(event []any, startedAt time.Time) {
	socketLog.Debug("dispatching an event %v", event)
	s.run(event, func(err error) {
		s.Enqueue(func() {
			if err != nil {
				s._onerror(err)
				return
			}
			if s.Connected() {
				s.EmitUntyped(slices.TryGetAny[string](event, 0), slices.Slice(event, 1)...)
			} else {
				socketLog.Debug("ignore packet received after disconnection")
			}
			s.emitTelemetry(&TelemetryEvent{
				Kind:          TelemetryEventReceived,
				Event:         slices.TryGetAny[string](event, 0),
				Bytes:         telemetryBytes(event),
				Duration:      time.Since(startedAt),
				Success:       err == nil,
				TraceMetadata: telemetryTraceMetadata(event),
			})
		})
	})
}

// Use registers a middleware function for this socket.
func (s *Socket) Use(fn SocketMiddleware) *Socket {
	s.fns.Push(fn)
	return s
}

// Executes the middleware for an incoming event.
//
// Pparam: event - event that will get emitted
//
// Pparam: fn - last fn call in the middleware
func (s *Socket) run(event []any, fn func(error)) {
	fns := s.fns.All()
	if length := len(fns); length > 0 {
		var run func(i int)
		run = func(i int) {
			fns[i](event, func(err error) {
				// upon error, short-circuit
				if err != nil {
					fn(err)
					return
				}
				// if no middleware left, summon callback
				if i >= length-1 {
					fn(nil)
					return
				}
				// go on to next
				run(i + 1)
			})
		}
		run(0)
	} else {
		fn(nil)
	}
}

// Whether the socket is currently disconnected
func (s *Socket) Disconnected() bool {
	return !s.Connected()
}

// A reference to the request that originated the underlying Engine.IO Socket.
func (s *Socket) Request() *types.HttpContext {
	return s.client.Request()
}

// A reference to the underlying Client transport connection (Engine.IO Socket interface).
//
//	io.On("connection", func(clients ...any) {
//		socket := clients[0].(*socket.Socket)
//		fmt.Println(socket.Conn().Transport().Name()) // prints "polling" or "websocket" or "webtransport"
//
//		socket.Conn().Once("upgrade", func(...any) {
//			fmt.Println(socket.Conn().Transport().Name()) // prints "websocket"
//		})
//	})
func (s *Socket) Conn() engine.Socket {
	return s.client.conn
}

// Returns the rooms the socket is currently in.
//
//	io.On("connection", func(clients ...any) {
//		socket := clients[0].(*socket.Socket)
//		fmt.Println(socket.Rooms()) // *types.Set { <socket.id> }
//
//		socket.Join("room1")
//
//		fmt.Println(socket.Rooms()) // *types.Set { <socket.id>, "room1" }
//	})
func (s *Socket) Rooms() *types.Set[Room] {
	if rooms := s.adapter.SocketRooms(s.id); rooms != nil {
		return rooms
	}
	return types.NewSet[Room]()
}

// OnAny adds a listener that will be fired when any event is received.
func (s *Socket) OnAny(listener types.EventListener) *Socket {
	s._anyListeners.Push(listener)
	return s
}

// PrependAny adds a listener to the beginning of the listeners array for any event received.
func (s *Socket) PrependAny(listener types.EventListener) *Socket {
	s._anyListeners.Unshift(listener)
	return s
}

// OffAny removes a specific or all listeners for any event received.
func (s *Socket) OffAny(listener types.EventListener) *Socket {
	if listener != nil {
		anyListeners := reflect.ValueOf(listener).Pointer()
		_, _ = s._anyListeners.RangeAndSplice(func(listener types.EventListener, i int) (bool, int, int, []types.EventListener) {
			return reflect.ValueOf(listener).Pointer() == anyListeners, i, 1, nil
		})
	} else {
		s._anyListeners.Clear()
	}
	return s
}

// Returns an array of listeners that are listening for any event that is specified. This array can be manipulated,
// e.g. to remove listeners.
func (s *Socket) ListenersAny() []types.EventListener {
	return s._anyListeners.All()
}

// OnAnyOutgoing adds a listener that will be fired when any event is sent.
func (s *Socket) OnAnyOutgoing(listener types.EventListener) *Socket {
	s._anyOutgoingListeners.Push(listener)
	return s
}

// PrependAnyOutgoing adds a listener to the beginning of the listeners array for any event sent.
func (s *Socket) PrependAnyOutgoing(listener types.EventListener) *Socket {
	s._anyOutgoingListeners.Unshift(listener)
	return s
}

// OffAnyOutgoing removes a specific or all listeners for any event sent.
func (s *Socket) OffAnyOutgoing(listener types.EventListener) *Socket {
	if listener != nil {
		listenerPointer := reflect.ValueOf(listener).Pointer()
		_, _ = s._anyOutgoingListeners.RangeAndSplice(func(listener types.EventListener, i int) (bool, int, int, []types.EventListener) {
			return reflect.ValueOf(listener).Pointer() == listenerPointer, i, 1, nil
		})
	} else {
		s._anyOutgoingListeners.Clear()
	}
	return s
}

// Returns an array of listeners that are listening for any event that is specified. This array can be manipulated,
// e.g. to remove listeners.
func (s *Socket) ListenersAnyOutgoing() []types.EventListener {
	return s._anyOutgoingListeners.All()
}

// Notify the listeners for each packet sent (emit or broadcast)
func (s *Socket) notifyOutgoingListeners(packet *parser.Packet) {
	if args, ok := packet.Data.([]any); ok {
		s.emitTelemetry(&TelemetryEvent{
			Kind:          TelemetryEventSent,
			Event:         slices.TryGetAny[string](args, 0),
			Bytes:         telemetryBytes(args),
			Success:       true,
			TraceMetadata: telemetryTraceMetadata(args),
		})
	}
	for _, listener := range s._anyOutgoingListeners.All() {
		if args, ok := packet.Data.([]any); ok {
			listener(args...)
		} else {
			listener(packet.Data)
		}
	}
}

func telemetryBytes(value any) int {
	encoded, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return len(encoded)
}

func telemetryTraceMetadata(args []any) map[string]string {
	for _, arg := range args {
		values, ok := arg.(map[string]any)
		if !ok {
			continue
		}
		metadata := make(map[string]string, 2)
		for _, key := range []string{"traceparent", "tracestate"} {
			if value, ok := values[key].(string); ok && value != "" {
				metadata[key] = value
			}
		}
		if len(metadata) > 0 {
			return metadata
		}
	}
	return nil
}

func telemetryHandshakeTrace(handshake *Handshake) map[string]string {
	if handshake == nil {
		return nil
	}
	metadata := make(map[string]string, 2)
	for headerName, headerValue := range handshake.Headers {
		for _, key := range []string{"traceparent", "tracestate"} {
			if !strings.EqualFold(headerName, key) {
				continue
			}
			switch value := headerValue.(type) {
			case string:
				metadata[key] = value
			case []string:
				if len(value) > 0 {
					metadata[key] = value[0]
				}
			case []any:
				if len(value) > 0 {
					metadata[key], _ = value[0].(string)
				}
			}
		}
	}
	return metadata
}
func (s *Socket) NotifyOutgoingListeners() func(*parser.Packet) {
	return s.notifyOutgoingListeners
}

func (s *Socket) newBroadcastOperator() *BroadcastOperator {
	flags := *s.flags.Swap(&BroadcastFlags{})
	return NewBroadcastOperator(s.adapter, types.NewSet[Room](), types.NewSet(Room(s.id)), &flags)
}
