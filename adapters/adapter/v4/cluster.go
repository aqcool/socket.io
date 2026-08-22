package adapter

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
	socketio "github.com/aqcool/socket.io/v4"
)

// Transport is the provider-facing boundary of the native cluster adapter.
// Redis, MongoDB, PostgreSQL and broker-backed implementations only need to
// provide message transport and node counting; all Socket.IO behavior lives in
// ClusterAdapter.
type Transport interface {
	Start(context.Context, func(*ClusterMessage, Offset)) error
	Publish(context.Context, *ClusterMessage) (Offset, error)
	PublishResponse(context.Context, ServerID, *ClusterMessage) error
	ServerCount(context.Context) (int64, error)
	Close() error
}

type TransportBuilder interface {
	NewTransport(*socketio.Namespace, ServerID) (Transport, error)
}

type TransportBuilderFunc func(*socketio.Namespace, ServerID) (Transport, error)

func (f TransportBuilderFunc) NewTransport(namespace *socketio.Namespace, id ServerID) (Transport, error) {
	return f(namespace, id)
}

type ClusterOptions struct {
	RequestTimeout time.Duration

	OrderedDelivery         bool
	DuplicateSuppression    bool
	ConnectionStateRecovery bool
	ExternalEmitter         bool
}

func (o ClusterOptions) requestTimeout() time.Duration {
	if o.RequestTimeout <= 0 {
		return DefaultTimeout
	}
	return o.RequestTimeout
}

type Factory struct {
	Transport TransportBuilder
	Options   ClusterOptions
}

func (f Factory) New(namespace *socketio.Namespace) (socketio.Adapter, error) {
	if namespace == nil || f.Transport == nil {
		return nil, socketio.ErrInvalidArgument
	}
	local, err := socketio.NewLocalAdapter(namespace)
	if err != nil {
		return nil, err
	}
	uid := ServerID(randomID())
	transport, err := f.Transport.NewTransport(namespace, uid)
	if err != nil {
		return nil, err
	}
	if transport == nil {
		return nil, fmt.Errorf("adapter/v4: transport builder returned nil")
	}
	return &ClusterAdapter{
		LocalAdapter: local,
		nsp:          namespace,
		transport:    transport,
		uid:          uid,
		opts:         f.Options,
		pending:      make(map[string]*pendingRequest),
		ackRequests:  make(map[string]*ackRequest),
	}, nil
}

type pendingRequest struct {
	mu       sync.Mutex
	expected int64
	received int64
	values   []any
	err      error
	done     chan struct{}
	once     sync.Once
}

func newPendingRequest(expected int64) *pendingRequest {
	request := &pendingRequest{expected: expected, done: make(chan struct{})}
	if expected <= 0 {
		close(request.done)
	}
	return request
}

func (r *pendingRequest) add(value any) {
	r.mu.Lock()
	if r.received < r.expected {
		r.values = append(r.values, value)
		r.received++
	}
	complete := r.received >= r.expected
	r.mu.Unlock()
	if complete {
		r.once.Do(func() { close(r.done) })
	}
}

func (r *pendingRequest) fail(err error) {
	r.mu.Lock()
	if r.err == nil {
		r.err = err
	}
	r.mu.Unlock()
	r.once.Do(func() { close(r.done) })
}

func (r *pendingRequest) result() ([]any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]any(nil), r.values...), r.err
}

type ackRequest struct {
	clientCount func(uint64)
	ack         socketio.Ack
	timer       *time.Timer
}

type ClusterAdapter struct {
	*socketio.LocalAdapter

	nsp       *socketio.Namespace
	transport Transport
	uid       ServerID
	opts      ClusterOptions

	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
	mu        sync.Mutex
	pending   map[string]*pendingRequest
	ackRequests map[string]*ackRequest
}

func (c *ClusterAdapter) UID() ServerID {
	if c == nil {
		return ""
	}
	return c.uid
}

func (c *ClusterAdapter) Init(ctx context.Context) error {
	if c == nil || c.LocalAdapter == nil || c.transport == nil {
		return socketio.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	if err := c.LocalAdapter.Init(c.ctx); err != nil {
		c.cancel()
		return err
	}
	if err := c.transport.Start(c.ctx, c.HandleMessage); err != nil {
		c.cancel()
		_ = c.LocalAdapter.Close()
		return err
	}
	return nil
}

func (c *ClusterAdapter) Close() error {
	if c == nil {
		return nil
	}
	var result error
	c.closeOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		c.mu.Lock()
		for id, request := range c.pending {
			request.fail(socketio.ErrClosed)
			delete(c.pending, id)
		}
		for id, request := range c.ackRequests {
			if request.timer != nil {
				request.timer.Stop()
			}
			delete(c.ackRequests, id)
		}
		c.mu.Unlock()
		result = errors.Join(c.transport.Close(), c.LocalAdapter.Close())
	})
	return result
}

func (c *ClusterAdapter) Capabilities() socketio.AdapterCapabilities {
	capabilities := c.LocalAdapter.Capabilities()
	capabilities.ServerSideEmit = true
	capabilities.NodeDiscovery = true
	capabilities.OrderedDelivery = c.opts.OrderedDelivery
	capabilities.DuplicateSuppression = c.opts.DuplicateSuppression
	capabilities.ConnectionStateRecovery = c.opts.ConnectionStateRecovery
	capabilities.ExternalEmitter = c.opts.ExternalEmitter
	return capabilities
}

func (c *ClusterAdapter) ServerCount(ctx context.Context) (int64, error) {
	if c == nil || c.transport == nil {
		return 0, socketio.ErrClosed
	}
	count, err := c.transport.ServerCount(ctx)
	if err != nil {
		return 0, err
	}
	if count < 1 {
		count = 1
	}
	return count, nil
}

func (c *ClusterAdapter) Broadcast(ctx context.Context, packet socketio.Packet, opts *socketio.BroadcastOptions) error {
	if c == nil {
		return socketio.ErrClosed
	}
	if opts == nil {
		opts = &socketio.BroadcastOptions{}
	}
	if opts.Flags.Local {
		return c.LocalAdapter.Broadcast(ctx, packet, opts)
	}

	wirePacket, err := toWirePacket(packet)
	if err != nil {
		return err
	}
	offset, publishErr := c.transport.Publish(ctx, &ClusterMessage{
		UID:  c.uid,
		NSP:  c.nsp.Name(),
		Type: Broadcast,
		Data: &BroadcastMessage{Packet: wirePacket, Opts: EncodeOptions(opts)},
	})
	localErr := c.LocalAdapter.BroadcastWithOffset(ctx, packet, opts, string(offset))
	return errors.Join(publishErr, localErr)
}

func (c *ClusterAdapter) BroadcastWithAck(
	ctx context.Context,
	packet socketio.Packet,
	opts *socketio.BroadcastOptions,
	clientCount func(uint64),
	ack socketio.Ack,
) error {
	if c == nil {
		return socketio.ErrClosed
	}
	if opts == nil {
		opts = &socketio.BroadcastOptions{}
	}
	if opts.Flags.Local {
		return c.LocalAdapter.BroadcastWithAck(ctx, packet, opts, clientCount, ack)
	}

	wirePacket, err := toWirePacket(packet)
	if err != nil {
		return err
	}
	requestID := randomID()
	timeout := c.opts.requestTimeout()
	if opts.Flags.Timeout != nil && *opts.Flags.Timeout > 0 {
		timeout = *opts.Flags.Timeout
	}
	request := &ackRequest{clientCount: clientCount, ack: ack}
	request.timer = time.AfterFunc(timeout, func() {
		c.mu.Lock()
		delete(c.ackRequests, requestID)
		c.mu.Unlock()
	})
	c.mu.Lock()
	c.ackRequests[requestID] = request
	c.mu.Unlock()

	_, publishErr := c.transport.Publish(ctx, &ClusterMessage{
		UID:  c.uid,
		NSP:  c.nsp.Name(),
		Type: Broadcast,
		Data: &BroadcastMessage{
			Packet:    wirePacket,
			Opts:      EncodeOptions(opts),
			RequestID: &requestID,
		},
	})
	localErr := c.LocalAdapter.BroadcastWithAck(ctx, packet, opts, clientCount, ack)
	if publishErr != nil {
		c.removeAckRequest(requestID)
	}
	return errors.Join(publishErr, localErr)
}

func (c *ClusterAdapter) FetchSockets(ctx context.Context, opts *socketio.BroadcastOptions) ([]socketio.SocketDetails, error) {
	local, err := c.LocalAdapter.FetchSockets(ctx, opts)
	if err != nil {
		return nil, err
	}
	if opts != nil && opts.Flags.Local {
		return local, nil
	}
	expected, err := c.remoteCount(ctx)
	if err != nil || expected == 0 {
		return local, err
	}
	requestID := randomID()
	request := c.putPending(requestID, expected)
	if _, err := c.transport.Publish(ctx, &ClusterMessage{
		UID:  c.uid,
		NSP:  c.nsp.Name(),
		Type: FetchSockets,
		Data: &FetchSocketsMessage{Opts: EncodeOptions(opts), RequestID: requestID},
	}); err != nil {
		c.removePending(requestID)
		return nil, err
	}
	values, err := c.waitPending(ctx, requestID, request)
	if err != nil {
		return nil, err
	}
	result := append([]socketio.SocketDetails(nil), local...)
	for _, value := range values {
		if sockets, ok := value.([]socketio.SocketDetails); ok {
			result = append(result, sockets...)
		}
	}
	return result, nil
}

func (c *ClusterAdapter) CountSockets(ctx context.Context, opts *socketio.BroadcastOptions) (uint64, error) {
	local, err := c.LocalAdapter.CountSockets(ctx, opts)
	if err != nil {
		return 0, err
	}
	if opts != nil && opts.Flags.Local {
		return local, nil
	}
	expected, err := c.remoteCount(ctx)
	if err != nil || expected == 0 {
		return local, err
	}
	requestID := randomID()
	request := c.putPending(requestID, expected)
	if _, err := c.transport.Publish(ctx, &ClusterMessage{
		UID:  c.uid,
		NSP:  c.nsp.Name(),
		Type: CountSockets,
		Data: &CountSocketsMessage{Opts: EncodeOptions(opts), RequestID: requestID},
	}); err != nil {
		c.removePending(requestID)
		return 0, err
	}
	values, err := c.waitPending(ctx, requestID, request)
	if err != nil {
		return 0, err
	}
	result := local
	for _, value := range values {
		if count, ok := value.(uint64); ok {
			result += count
		}
	}
	return result, nil
}

func (c *ClusterAdapter) ListRooms(ctx context.Context, opts *socketio.BroadcastOptions) (map[socketio.Room]uint64, error) {
	local, err := c.LocalAdapter.ListRooms(ctx, opts)
	if err != nil {
		return nil, err
	}
	if opts != nil && opts.Flags.Local {
		return local, nil
	}
	expected, err := c.remoteCount(ctx)
	if err != nil || expected == 0 {
		return local, err
	}
	requestID := randomID()
	request := c.putPending(requestID, expected)
	if _, err := c.transport.Publish(ctx, &ClusterMessage{
		UID:  c.uid,
		NSP:  c.nsp.Name(),
		Type: ListRooms,
		Data: &ListRoomsMessage{Opts: EncodeOptions(opts), RequestID: requestID},
	}); err != nil {
		c.removePending(requestID)
		return nil, err
	}
	values, err := c.waitPending(ctx, requestID, request)
	if err != nil {
		return nil, err
	}
	result := make(map[socketio.Room]uint64, len(local))
	for room, count := range local {
		result[room] = count
	}
	for _, value := range values {
		rooms, ok := value.(map[socketio.Room]uint64)
		if !ok {
			continue
		}
		for room, count := range rooms {
			result[room] += count
		}
	}
	return result, nil
}

func (c *ClusterAdapter) AddSockets(ctx context.Context, opts *socketio.BroadcastOptions, rooms ...socketio.Room) error {
	if opts == nil {
		opts = &socketio.BroadcastOptions{}
	}
	var publishErr error
	if !opts.Flags.Local {
		_, publishErr = c.transport.Publish(ctx, &ClusterMessage{
			UID:  c.uid,
			NSP:  c.nsp.Name(),
			Type: SocketsJoin,
			Data: &SocketsJoinLeaveMessage{Opts: EncodeOptions(opts), Rooms: append([]socketio.Room(nil), rooms...)},
		})
	}
	return errors.Join(publishErr, c.LocalAdapter.AddSockets(ctx, opts, rooms...))
}

func (c *ClusterAdapter) DeleteSockets(ctx context.Context, opts *socketio.BroadcastOptions, rooms ...socketio.Room) error {
	if opts == nil {
		opts = &socketio.BroadcastOptions{}
	}
	var publishErr error
	if !opts.Flags.Local {
		_, publishErr = c.transport.Publish(ctx, &ClusterMessage{
			UID:  c.uid,
			NSP:  c.nsp.Name(),
			Type: SocketsLeave,
			Data: &SocketsJoinLeaveMessage{Opts: EncodeOptions(opts), Rooms: append([]socketio.Room(nil), rooms...)},
		})
	}
	return errors.Join(publishErr, c.LocalAdapter.DeleteSockets(ctx, opts, rooms...))
}

func (c *ClusterAdapter) DisconnectSockets(ctx context.Context, opts *socketio.BroadcastOptions, closeTransport bool) error {
	if opts == nil {
		opts = &socketio.BroadcastOptions{}
	}
	var publishErr error
	if !opts.Flags.Local {
		_, publishErr = c.transport.Publish(ctx, &ClusterMessage{
			UID:  c.uid,
			NSP:  c.nsp.Name(),
			Type: DisconnectSockets,
			Data: &DisconnectSocketsMessage{Opts: EncodeOptions(opts), Close: closeTransport},
		})
	}
	return errors.Join(publishErr, c.LocalAdapter.DisconnectSockets(ctx, opts, closeTransport))
}

func (c *ClusterAdapter) ServerSideEmit(ctx context.Context, packet []any) error {
	if len(packet) == 0 {
		return socketio.ErrInvalidEvent
	}
	_, err := c.transport.Publish(ctx, &ClusterMessage{
		UID:  c.uid,
		NSP:  c.nsp.Name(),
		Type: ServerSideEmit,
		Data: &ServerSideEmitMessage{Packet: append([]any(nil), packet...)},
	})
	return err
}

func (c *ClusterAdapter) ServerSideEmitAck(ctx context.Context, packet []any) ([]any, error) {
	if len(packet) == 0 {
		return nil, socketio.ErrInvalidEvent
	}
	expected, err := c.remoteCount(ctx)
	if err != nil || expected == 0 {
		return nil, err
	}
	requestID := randomID()
	request := c.putPending(requestID, expected)
	if _, err := c.transport.Publish(ctx, &ClusterMessage{
		UID:  c.uid,
		NSP:  c.nsp.Name(),
		Type: ServerSideEmit,
		Data: &ServerSideEmitMessage{RequestID: &requestID, Packet: append([]any(nil), packet...)},
	}); err != nil {
		c.removePending(requestID)
		return nil, err
	}
	return c.waitPending(ctx, requestID, request)
}

// HandleMessage is called by the provider transport for every cluster frame.
func (c *ClusterAdapter) HandleMessage(message *ClusterMessage, offset Offset) {
	if c == nil || message == nil || message.UID == c.uid || message.NSP != c.nsp.Name() {
		return
	}
	if isResponseType(message.Type) {
		c.handleResponse(message)
		return
	}

	switch message.Type {
	case Broadcast:
		c.handleBroadcast(message, offset)
	case SocketsJoin:
		if data, ok := message.Data.(*SocketsJoinLeaveMessage); ok {
			_ = c.LocalAdapter.AddSockets(c.context(), DecodeOptions(data.Opts), data.Rooms...)
		}
	case SocketsLeave:
		if data, ok := message.Data.(*SocketsJoinLeaveMessage); ok {
			_ = c.LocalAdapter.DeleteSockets(c.context(), DecodeOptions(data.Opts), data.Rooms...)
		}
	case DisconnectSockets:
		if data, ok := message.Data.(*DisconnectSocketsMessage); ok {
			_ = c.LocalAdapter.DisconnectSockets(c.context(), DecodeOptions(data.Opts), data.Close)
		}
	case FetchSockets:
		c.handleFetchSockets(message)
	case CountSockets:
		c.handleCountSockets(message)
	case ListRooms:
		c.handleListRooms(message)
	case ServerSideEmit:
		c.handleServerSideEmit(message)
	}
}

func (c *ClusterAdapter) handleBroadcast(message *ClusterMessage, offset Offset) {
	data, ok := message.Data.(*BroadcastMessage)
	if !ok || data.Packet == nil {
		return
	}
	packet, err := fromWirePacket(data.Packet)
	if err != nil {
		return
	}
	opts := DecodeOptions(data.Opts)
	if data.RequestID == nil {
		recoveryOffset := message.Offset
		if recoveryOffset == "" {
			recoveryOffset = offset
		}
		_ = c.LocalAdapter.BroadcastWithOffset(c.context(), packet, opts, string(recoveryOffset))
		return
	}

	requestID := *data.RequestID
	requestCtx, cancel := c.operationContext(c.context(), opts)
	defer cancel()
	_ = c.LocalAdapter.BroadcastWithAck(
		requestCtx,
		packet,
		opts,
		func(count uint64) {
			_ = c.transport.PublishResponse(c.context(), message.UID, &ClusterMessage{
				UID:  c.uid,
				NSP:  c.nsp.Name(),
				Type: BroadcastClientCount,
				Data: &BroadcastClientCount{RequestID: requestID, ClientCount: count},
			})
		},
		func(values []any, _ error) {
			_ = c.transport.PublishResponse(c.context(), message.UID, &ClusterMessage{
				UID:  c.uid,
				NSP:  c.nsp.Name(),
				Type: BroadcastAck,
				Data: &BroadcastAckResponse{RequestID: requestID, Packet: firstValue(values)},
			})
		},
	)
}

func (c *ClusterAdapter) handleFetchSockets(message *ClusterMessage) {
	data, ok := message.Data.(*FetchSocketsMessage)
	if !ok {
		return
	}
	sockets, err := c.LocalAdapter.FetchSockets(c.context(), DecodeOptions(data.Opts))
	if err != nil {
		return
	}
	wireSockets := make([]*SocketResponse, 0, len(sockets))
	for index := range sockets {
		details := &sockets[index]
		wireSockets = append(wireSockets, &SocketResponse{
			ID:        details.ID,
			Handshake: EncodeHandshake(details.Handshake),
			Rooms:     append([]socketio.Room(nil), details.Rooms...),
			Data:      details.Data,
		})
	}
	_ = c.transport.PublishResponse(c.context(), message.UID, &ClusterMessage{
		UID:  c.uid,
		NSP:  c.nsp.Name(),
		Type: FetchSocketsResponse,
		Data: &FetchSocketsResponseData{RequestID: data.RequestID, Sockets: wireSockets},
	})
}

func (c *ClusterAdapter) handleCountSockets(message *ClusterMessage) {
	data, ok := message.Data.(*CountSocketsMessage)
	if !ok {
		return
	}
	count, err := c.LocalAdapter.CountSockets(c.context(), DecodeOptions(data.Opts))
	if err != nil {
		return
	}
	_ = c.transport.PublishResponse(c.context(), message.UID, &ClusterMessage{
		UID:  c.uid,
		NSP:  c.nsp.Name(),
		Type: CountSocketsResponse,
		Data: &CountSocketsResponse{RequestID: data.RequestID, Count: count},
	})
}

func (c *ClusterAdapter) handleListRooms(message *ClusterMessage) {
	data, ok := message.Data.(*ListRoomsMessage)
	if !ok {
		return
	}
	rooms, err := c.LocalAdapter.ListRooms(c.context(), DecodeOptions(data.Opts))
	if err != nil {
		return
	}
	_ = c.transport.PublishResponse(c.context(), message.UID, &ClusterMessage{
		UID:  c.uid,
		NSP:  c.nsp.Name(),
		Type: ListRoomsResponse,
		Data: &ListRoomsResponse{RequestID: data.RequestID, Rooms: rooms},
	})
}

func (c *ClusterAdapter) handleServerSideEmit(message *ClusterMessage) {
	data, ok := message.Data.(*ServerSideEmitMessage)
	if !ok || len(data.Packet) == 0 {
		return
	}
	event, ok := data.Packet[0].(string)
	if !ok || event == "" {
		return
	}
	args := append([]any(nil), data.Packet[1:]...)
	if data.RequestID != nil {
		requestID := *data.RequestID
		var once sync.Once
		ack := socketio.Ack(func(values []any, _ error) {
			once.Do(func() {
				_ = c.transport.PublishResponse(c.context(), message.UID, &ClusterMessage{
					UID:  c.uid,
					NSP:  c.nsp.Name(),
					Type: ServerSideEmitResponse,
					Data: &ServerSideEmitResponse{RequestID: requestID, Packet: firstValue(values)},
				})
			})
		})
		args = append(args, ack)
	}
	_ = c.nsp.ReceiveServerSideEvent(event, args...)
}

func (c *ClusterAdapter) handleResponse(message *ClusterMessage) {
	switch message.Type {
	case BroadcastClientCount:
		if data, ok := message.Data.(*BroadcastClientCount); ok {
			c.mu.Lock()
			request := c.ackRequests[data.RequestID]
			c.mu.Unlock()
			if request != nil && request.clientCount != nil {
				request.clientCount(data.ClientCount)
			}
		}
	case BroadcastAck:
		if data, ok := message.Data.(*BroadcastAckResponse); ok {
			c.mu.Lock()
			request := c.ackRequests[data.RequestID]
			c.mu.Unlock()
			if request != nil && request.ack != nil {
				request.ack([]any{data.Packet}, nil)
			}
		}
	case FetchSocketsResponse:
		if data, ok := message.Data.(*FetchSocketsResponseData); ok {
			result := make([]socketio.SocketDetails, 0, len(data.Sockets))
			for _, socket := range data.Sockets {
				if socket == nil {
					continue
				}
				result = append(result, socketio.SocketDetails{
					ID:        socket.ID,
					Handshake: DecodeHandshake(socket.Handshake),
					Rooms:     append([]socketio.Room(nil), socket.Rooms...),
					Data:      socket.Data,
				})
			}
			c.addPending(data.RequestID, result)
		}
	case CountSocketsResponse:
		if data, ok := message.Data.(*CountSocketsResponse); ok {
			c.addPending(data.RequestID, data.Count)
		}
	case ListRoomsResponse:
		if data, ok := message.Data.(*ListRoomsResponse); ok {
			c.addPending(data.RequestID, data.Rooms)
		}
	case ServerSideEmitResponse:
		if data, ok := message.Data.(*ServerSideEmitResponse); ok {
			c.addPending(data.RequestID, data.Packet)
		}
	}
}

func (c *ClusterAdapter) remoteCount(ctx context.Context) (int64, error) {
	count, err := c.ServerCount(ctx)
	if err != nil {
		return 0, err
	}
	if count <= 1 {
		return 0, nil
	}
	return count - 1, nil
}

func (c *ClusterAdapter) putPending(id string, expected int64) *pendingRequest {
	request := newPendingRequest(expected)
	c.mu.Lock()
	c.pending[id] = request
	c.mu.Unlock()
	return request
}

func (c *ClusterAdapter) addPending(id string, value any) {
	c.mu.Lock()
	request := c.pending[id]
	c.mu.Unlock()
	if request != nil {
		request.add(value)
	}
}

func (c *ClusterAdapter) removePending(id string) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *ClusterAdapter) removeAckRequest(id string) {
	c.mu.Lock()
	request := c.ackRequests[id]
	delete(c.ackRequests, id)
	c.mu.Unlock()
	if request != nil && request.timer != nil {
		request.timer.Stop()
	}
}

func (c *ClusterAdapter) waitPending(ctx context.Context, id string, request *pendingRequest) ([]any, error) {
	waitCtx, cancel := context.WithTimeout(ctx, c.opts.requestTimeout())
	defer cancel()
	defer c.removePending(id)

	select {
	case <-request.done:
		return request.result()
	case <-waitCtx.Done():
		return nil, waitCtx.Err()
	}
}

func (c *ClusterAdapter) operationContext(parent context.Context, opts *socketio.BroadcastOptions) (context.Context, context.CancelFunc) {
	if opts != nil && opts.Flags.Timeout != nil && *opts.Flags.Timeout > 0 {
		return context.WithTimeout(parent, *opts.Flags.Timeout)
	}
	return context.WithCancel(parent)
}

func (c *ClusterAdapter) context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}

func isResponseType(messageType MessageType) bool {
	switch messageType {
	case FetchSocketsResponse, ServerSideEmitResponse, BroadcastClientCount, BroadcastAck, CountSocketsResponse, ListRoomsResponse:
		return true
	default:
		return false
	}
}

func firstValue(values []any) any {
	if len(values) == 0 {
		return nil
	}
	return values[0]
}

func randomID() string {
	var data [15]byte
	if _, err := rand.Read(data[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(data[:])
}

func toWirePacket(packet socketio.Packet) (*parser.Packet, error) {
	result := &parser.Packet{Nsp: packet.Namespace, Id: packet.ID, Data: packet.Data}
	switch packet.Type {
	case socketio.PacketConnect:
		result.Type = parser.CONNECT
	case socketio.PacketDisconnect:
		result.Type = parser.DISCONNECT
	case socketio.PacketEvent:
		result.Type = parser.EVENT
	case socketio.PacketAck:
		result.Type = parser.ACK
	case socketio.PacketConnectError:
		result.Type = parser.CONNECT_ERROR
	case socketio.PacketBinaryEvent:
		result.Type = parser.BINARY_EVENT
	case socketio.PacketBinaryAck:
		result.Type = parser.BINARY_ACK
	default:
		return nil, fmt.Errorf("adapter/v4: unsupported packet type %d", packet.Type)
	}
	return result, nil
}

func fromWirePacket(packet *parser.Packet) (socketio.Packet, error) {
	if packet == nil {
		return socketio.Packet{}, socketio.ErrInvalidArgument
	}
	result := socketio.Packet{Namespace: packet.Nsp, ID: packet.Id, Data: packet.Data}
	switch packet.Type {
	case parser.CONNECT:
		result.Type = socketio.PacketConnect
	case parser.DISCONNECT:
		result.Type = socketio.PacketDisconnect
	case parser.EVENT:
		result.Type = socketio.PacketEvent
	case parser.ACK:
		result.Type = socketio.PacketAck
	case parser.CONNECT_ERROR:
		result.Type = socketio.PacketConnectError
	case parser.BINARY_EVENT:
		result.Type = socketio.PacketBinaryEvent
	case parser.BINARY_ACK:
		result.Type = socketio.PacketBinaryAck
	default:
		return socketio.Packet{}, fmt.Errorf("adapter/v4: unsupported wire packet type %d", packet.Type)
	}
	return result, nil
}
