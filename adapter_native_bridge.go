package socketio

import (
	"context"
	"fmt"
	"sync"
	"time"

	coreparser "github.com/aqcool/socket.io/parsers/socket/v4/parser"
	core "github.com/aqcool/socket.io/servers/socket/v4"
	coretypes "github.com/aqcool/socket.io/v4/pkg/types"
)

// AdapterFactoryCapabilities may be implemented by an AdapterFactory when its
// capabilities are known before a namespace-specific Adapter is created.
type AdapterFactoryCapabilities interface {
	Capabilities() AdapterCapabilities
}

type nativeAdapterConstructor struct {
	owner   *Server
	factory AdapterFactory

	mu       sync.Mutex
	firstErr error
}

func newNativeAdapterConstructor(owner *Server, factory AdapterFactory) *nativeAdapterConstructor {
	return &nativeAdapterConstructor{owner: owner, factory: factory}
}

func (c *nativeAdapterConstructor) record(err error) {
	if c == nil || err == nil {
		return
	}
	c.mu.Lock()
	if c.firstErr == nil {
		c.firstErr = err
	}
	c.mu.Unlock()
}

func (c *nativeAdapterConstructor) Err() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.firstErr
}

func (c *nativeAdapterConstructor) Capabilities() core.AdapterCapabilities {
	if c == nil || c.factory == nil {
		return core.AdapterCapabilities{}
	}
	provider, ok := c.factory.(AdapterFactoryCapabilities)
	if !ok {
		return core.AdapterCapabilities{}
	}
	return toCoreAdapterCapabilities(provider.Capabilities())
}

func (c *nativeAdapterConstructor) SupportsConnectionStateRecovery() bool {
	return c.Capabilities().ConnectionStateRecovery
}

func (c *nativeAdapterConstructor) New(namespace core.Namespace) core.Adapter {
	bridge := &nativeAdapterBridge{
		StrictEventEmitter: core.NewStrictEventEmitter(),
		constructor:        c,
		namespace:          namespace,
		rooms:              &coretypes.Map[core.Room, *coretypes.Set[core.SocketId]]{},
		sids:               &coretypes.Map[core.SocketId, *coretypes.Set[core.Room]]{},
	}
	if c == nil || c.factory == nil || c.owner == nil {
		return bridge
	}
	native, err := c.factory.New(c.owner.wrapNamespace(namespace))
	if err != nil {
		c.record(fmt.Errorf("socketio: create adapter: %w", err))
		return bridge
	}
	bridge.native = native
	return bridge
}

type nativeAdapterBridge struct {
	*core.StrictEventEmitter

	constructor *nativeAdapterConstructor
	namespace   core.Namespace
	native      Adapter
	rooms       *coretypes.Map[core.Room, *coretypes.Set[core.SocketId]]
	sids        *coretypes.Map[core.SocketId, *coretypes.Set[core.Room]]
}

func (b *nativeAdapterBridge) context() context.Context {
	if b != nil && b.constructor != nil && b.constructor.owner != nil {
		return b.constructor.owner.Context()
	}
	return context.Background()
}

func (b *nativeAdapterBridge) report(err error) {
	if err == nil || b == nil {
		return
	}
	if b.constructor != nil {
		b.constructor.record(err)
		if b.constructor.owner != nil && b.constructor.owner.cfg.Logger != nil {
			b.constructor.owner.cfg.Logger.Error("socket.io adapter error", "error", err)
		}
	}
}

func (b *nativeAdapterBridge) SupportsConnectionStateRecovery() bool {
	return b != nil && b.native != nil && b.native.Capabilities().ConnectionStateRecovery
}

func (b *nativeAdapterBridge) Capabilities() core.AdapterCapabilities {
	if b == nil || b.native == nil {
		return core.AdapterCapabilities{}
	}
	return toCoreAdapterCapabilities(b.native.Capabilities())
}

func (b *nativeAdapterBridge) Prototype(core.Adapter) {}
func (b *nativeAdapterBridge) Proto() core.Adapter      { return b }
func (b *nativeAdapterBridge) Rooms() *coretypes.Map[core.Room, *coretypes.Set[core.SocketId]] {
	return b.rooms
}
func (b *nativeAdapterBridge) Sids() *coretypes.Map[core.SocketId, *coretypes.Set[core.Room]] {
	return b.sids
}
func (b *nativeAdapterBridge) Nsp() core.Namespace { return b.namespace }
func (b *nativeAdapterBridge) Construct(namespace core.Namespace) {
	b.namespace = namespace
}

func (b *nativeAdapterBridge) Init() {
	if b == nil || b.native == nil {
		return
	}
	b.report(b.native.Init(b.context()))
}

func (b *nativeAdapterBridge) Close() {
	if b != nil && b.native != nil {
		b.report(b.native.Close())
	}
}

func (b *nativeAdapterBridge) ServerCount() int64 {
	if b == nil || b.native == nil {
		return 0
	}
	count, err := b.native.ServerCount(b.context())
	b.report(err)
	return count
}

func (b *nativeAdapterBridge) CountSockets(opts *core.BroadcastOptions) func(func(uint64, error)) {
	return func(callback func(uint64, error)) {
		if b == nil || b.native == nil {
			callback(0, ErrClosed)
			return
		}
		count, err := b.native.CountSockets(b.context(), fromCoreBroadcastOptions(opts))
		callback(count, err)
	}
}

func (b *nativeAdapterBridge) ListRooms(opts *core.BroadcastOptions) func(func(map[core.Room]uint64, error)) {
	return func(callback func(map[core.Room]uint64, error)) {
		if b == nil || b.native == nil {
			callback(nil, ErrClosed)
			return
		}
		rooms, err := b.native.ListRooms(b.context(), fromCoreBroadcastOptions(opts))
		if err != nil {
			callback(nil, err)
			return
		}
		result := make(map[core.Room]uint64, len(rooms))
		for room, count := range rooms {
			result[core.Room(room)] = count
		}
		callback(result, nil)
	}
}

func (b *nativeAdapterBridge) AddAll(id core.SocketId, rooms *coretypes.Set[core.Room]) {
	if b == nil || b.native == nil {
		return
	}
	values := fromCoreRooms(rooms)
	if err := b.native.AddAll(b.context(), SocketID(id), values...); err != nil {
		b.report(err)
		return
	}
	b.shadowAdd(id, rooms)
}

func (b *nativeAdapterBridge) Del(id core.SocketId, room core.Room) {
	if b == nil || b.native == nil {
		return
	}
	if err := b.native.Delete(b.context(), SocketID(id), Room(room)); err != nil {
		b.report(err)
		return
	}
	b.shadowDelete(id, room)
}

func (b *nativeAdapterBridge) DelAll(id core.SocketId) {
	if b == nil || b.native == nil {
		return
	}
	if err := b.native.DeleteAll(b.context(), SocketID(id)); err != nil {
		b.report(err)
		return
	}
	b.shadowDeleteAll(id)
}

func (b *nativeAdapterBridge) Broadcast(packet *coreparser.Packet, opts *core.BroadcastOptions) {
	if b == nil || b.native == nil {
		return
	}
	b.report(b.native.Broadcast(b.context(), fromCorePacket(packet), fromCoreBroadcastOptions(opts)))
}

func (b *nativeAdapterBridge) BroadcastWithAck(packet *coreparser.Packet, opts *core.BroadcastOptions, clientCount func(uint64), ack core.Ack) {
	if b == nil || b.native == nil {
		return
	}
	err := b.native.BroadcastWithAck(
		b.context(),
		fromCorePacket(packet),
		fromCoreBroadcastOptions(opts),
		clientCount,
		func(values []any, err error) { ack(values, err) },
	)
	b.report(err)
}

func (b *nativeAdapterBridge) Sockets(rooms *coretypes.Set[core.Room]) *coretypes.Set[core.SocketId] {
	result := coretypes.NewSet[core.SocketId]()
	if b == nil || b.native == nil {
		return result
	}
	details, err := b.native.FetchSockets(b.context(), BroadcastOptions{Rooms: fromCoreRooms(rooms)})
	if err != nil {
		b.report(err)
		return result
	}
	for _, detail := range details {
		result.Add(core.SocketId(detail.ID))
	}
	return result
}

func (b *nativeAdapterBridge) SocketRooms(id core.SocketId) *coretypes.Set[core.Room] {
	result := coretypes.NewSet[core.Room]()
	if b == nil || b.native == nil {
		return result
	}
	rooms, err := b.native.SocketRooms(b.context(), SocketID(id))
	if err != nil {
		b.report(err)
		return result
	}
	for _, room := range rooms {
		result.Add(core.Room(room))
	}
	return result
}

func (b *nativeAdapterBridge) FetchSockets(opts *core.BroadcastOptions) func(func([]core.SocketDetails, error)) {
	return func(callback func([]core.SocketDetails, error)) {
		if b == nil || b.native == nil {
			callback(nil, ErrClosed)
			return
		}
		details, err := b.native.FetchSockets(b.context(), fromCoreBroadcastOptions(opts))
		if err != nil {
			callback(nil, err)
			return
		}
		result := make([]core.SocketDetails, 0, len(details))
		for _, detail := range details {
			result = append(result, nativeCoreSocketDetails{detail: detail})
		}
		callback(result, nil)
	}
}

func (b *nativeAdapterBridge) AddSockets(opts *core.BroadcastOptions, rooms []core.Room) {
	if b != nil && b.native != nil {
		b.report(b.native.AddSockets(b.context(), fromCoreBroadcastOptions(opts), fromCoreRoomSlice(rooms)...))
	}
}

func (b *nativeAdapterBridge) DelSockets(opts *core.BroadcastOptions, rooms []core.Room) {
	if b != nil && b.native != nil {
		b.report(b.native.DeleteSockets(b.context(), fromCoreBroadcastOptions(opts), fromCoreRoomSlice(rooms)...))
	}
}

func (b *nativeAdapterBridge) DisconnectSockets(opts *core.BroadcastOptions, closeTransport bool) {
	if b != nil && b.native != nil {
		b.report(b.native.DisconnectSockets(b.context(), fromCoreBroadcastOptions(opts), closeTransport))
	}
}

func (b *nativeAdapterBridge) ServerSideEmit(args []any) error {
	if b == nil || b.native == nil {
		return ErrClosed
	}
	return b.native.ServerSideEmit(b.context(), args)
}

func (b *nativeAdapterBridge) PersistSession(session *core.SessionToPersist) {
	if b == nil || b.native == nil || session == nil {
		return
	}
	b.report(b.native.PersistSession(b.context(), Session{
		SID:   SocketID(session.Sid),
		PID:   PrivateSessionID(session.Pid),
		Rooms: fromCoreRooms(session.Rooms),
		Data:  session.Data,
	}))
}

func (b *nativeAdapterBridge) RestoreSession(pid core.PrivateSessionId, offset string) (*core.Session, error) {
	if b == nil || b.native == nil {
		return nil, ErrClosed
	}
	session, err := b.native.RestoreSession(b.context(), PrivateSessionID(pid), offset)
	if err != nil || session == nil {
		return nil, err
	}
	return &core.Session{
		SessionToPersist: &core.SessionToPersist{
			Sid:   core.SocketId(session.SID),
			Pid:   core.PrivateSessionId(session.PID),
			Rooms: toCoreRooms(session.Rooms),
			Data:  session.Data,
		},
		MissedPackets: append([]any(nil), session.MissedPackets...),
	}, nil
}

func (b *nativeAdapterBridge) shadowAdd(id core.SocketId, rooms *coretypes.Set[core.Room]) {
	if rooms == nil {
		return
	}
	sidRooms, ok := b.sids.Load(id)
	if !ok || sidRooms == nil {
		sidRooms = coretypes.NewSet[core.Room]()
		b.sids.Store(id, sidRooms)
	}
	for _, room := range rooms.Keys() {
		sidRooms.Add(room)
		roomSids, ok := b.rooms.Load(room)
		if !ok || roomSids == nil {
			roomSids = coretypes.NewSet[core.SocketId]()
			b.rooms.Store(room, roomSids)
		}
		roomSids.Add(id)
	}
}

func (b *nativeAdapterBridge) shadowDelete(id core.SocketId, room core.Room) {
	if sidRooms, ok := b.sids.Load(id); ok && sidRooms != nil {
		sidRooms.Delete(room)
		if sidRooms.Len() == 0 {
			b.sids.Delete(id)
		}
	}
	if roomSids, ok := b.rooms.Load(room); ok && roomSids != nil {
		roomSids.Delete(id)
		if roomSids.Len() == 0 {
			b.rooms.Delete(room)
		}
	}
}

func (b *nativeAdapterBridge) shadowDeleteAll(id core.SocketId) {
	rooms, ok := b.sids.LoadAndDelete(id)
	if !ok || rooms == nil {
		return
	}
	for _, room := range rooms.Keys() {
		if roomSids, exists := b.rooms.Load(room); exists && roomSids != nil {
			roomSids.Delete(id)
			if roomSids.Len() == 0 {
				b.rooms.Delete(room)
			}
		}
	}
}

type nativeCoreSocketDetails struct {
	detail SocketDetails
}

func (d nativeCoreSocketDetails) Id() core.SocketId { return core.SocketId(d.detail.ID) }
func (d nativeCoreSocketDetails) Handshake() *core.Handshake {
	return toCoreHandshake(d.detail.Handshake)
}
func (d nativeCoreSocketDetails) Rooms() *coretypes.Set[core.Room] {
	return toCoreRooms(d.detail.Rooms)
}
func (d nativeCoreSocketDetails) Data() any { return d.detail.Data }

func fromCoreBroadcastOptions(opts *core.BroadcastOptions) BroadcastOptions {
	if opts == nil {
		return BroadcastOptions{}
	}
	result := BroadcastOptions{
		Rooms:  fromCoreRooms(opts.Rooms),
		Except: fromCoreRooms(opts.Except),
	}
	if opts.Flags != nil {
		result.Flags = BroadcastFlags{
			Volatile:             opts.Flags.Volatile,
			Compress:             opts.Flags.Compress,
			Local:                opts.Flags.Local,
			Broadcast:            opts.Flags.Broadcast,
			Binary:               opts.Flags.Binary,
			Timeout:              opts.Flags.Timeout,
			ExpectSingleResponse: opts.Flags.ExpectSingleResponse,
		}
	}
	return result
}

func fromCoreRooms(rooms *coretypes.Set[core.Room]) []Room {
	if rooms == nil {
		return nil
	}
	return fromCoreRoomSlice(rooms.Keys())
}

func fromCoreRoomSlice(rooms []core.Room) []Room {
	result := make([]Room, len(rooms))
	for i, room := range rooms {
		result[i] = Room(room)
	}
	return result
}

func toCoreRooms(rooms []Room) *coretypes.Set[core.Room] {
	result := coretypes.NewSet[core.Room]()
	for _, room := range rooms {
		result.Add(core.Room(room))
	}
	return result
}

func toCoreHandshake(handshake Handshake) *core.Handshake {
	result := &core.Handshake{
		Headers: coretypes.IncomingHttpHeaders{},
		Time:    handshake.Time.Format(time.RFC3339),
		Address: handshake.Address,
		Secure:  handshake.Secure,
		Issued:  handshake.Time.UnixMilli(),
		Query:   coretypes.ParsedUrlQuery{},
		Auth:    make(map[string]any, len(handshake.Auth)),
	}
	for key, values := range handshake.Headers {
		if len(values) == 1 {
			result.Headers[key] = values[0]
		} else {
			result.Headers[key] = append([]string(nil), values...)
		}
	}
	for key, value := range handshake.Auth {
		result.Auth[key] = value
	}
	if handshake.URL != nil {
		result.Url = handshake.URL.RequestURI()
		for key, values := range handshake.URL.Query() {
			if len(values) == 1 {
				result.Query[key] = values[0]
			} else {
				result.Query[key] = append([]string(nil), values...)
			}
		}
	}
	return result
}

func toCoreAdapterCapabilities(capabilities AdapterCapabilities) core.AdapterCapabilities {
	return core.AdapterCapabilities{
		Broadcast:               capabilities.Broadcast,
		RoomBroadcast:           capabilities.RoomBroadcast,
		BroadcastAck:            capabilities.BroadcastAck,
		FetchSockets:            capabilities.FetchSockets,
		SocketManagement:        capabilities.SocketManagement,
		ServerSideEmit:          capabilities.ServerSideEmit,
		NodeDiscovery:           capabilities.NodeDiscovery,
		OrderedDelivery:         capabilities.OrderedDelivery,
		DuplicateSuppression:    capabilities.DuplicateSuppression,
		ConnectionStateRecovery: capabilities.ConnectionStateRecovery,
		ExternalEmitter:         capabilities.ExternalEmitter,
	}
}
