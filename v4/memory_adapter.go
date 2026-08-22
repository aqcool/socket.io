package socketio

import (
	"context"
	"errors"
	"sync"
	"time"
)

type MemoryAdapterFactory struct{}

func (MemoryAdapterFactory) New(namespace *Namespace) (Adapter, error) {
	if namespace == nil {
		return nil, ErrInvalidArgument
	}
	return newMemoryAdapter(namespace), nil
}

type sessionRecord struct {
	session        Session
	disconnectedAt time.Time
}

type persistedPacket struct {
	id        string
	emittedAt time.Time
	data      []any
	opts      *BroadcastOptions
}

type memoryAdapter struct {
	nsp *Namespace
	mu  sync.RWMutex

	rooms map[Room]map[SocketID]struct{}
	sids  map[SocketID]map[Room]struct{}

	sessions map[PrivateSessionID]sessionRecord
	packets  []persistedPacket
}

func newMemoryAdapter(nsp *Namespace) *memoryAdapter {
	return &memoryAdapter{
		nsp:      nsp,
		rooms:    make(map[Room]map[SocketID]struct{}),
		sids:     make(map[SocketID]map[Room]struct{}),
		sessions: make(map[PrivateSessionID]sessionRecord),
	}
}

func (a *memoryAdapter) Init(context.Context) error { return nil }

func (a *memoryAdapter) Close() error {
	a.mu.Lock()
	clear(a.rooms)
	clear(a.sids)
	clear(a.sessions)
	a.packets = nil
	a.mu.Unlock()
	return nil
}

func (a *memoryAdapter) Capabilities() AdapterCapabilities {
	return AdapterCapabilities{
		Broadcast:               true,
		RoomBroadcast:           true,
		BroadcastAck:            true,
		FetchSockets:            true,
		SocketManagement:        true,
		NodeDiscovery:           true,
		OrderedDelivery:         true,
		ConnectionStateRecovery: true,
	}
}

func (*memoryAdapter) ServerCount(context.Context) (int64, error) { return 1, nil }

func (a *memoryAdapter) AddAll(ctx context.Context, id SocketID, rooms ...Room) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	owned := a.sids[id]
	if owned == nil {
		owned = make(map[Room]struct{})
		a.sids[id] = owned
	}
	for _, room := range rooms {
		if room == "" {
			continue
		}
		owned[room] = struct{}{}
		members := a.rooms[room]
		if members == nil {
			members = make(map[SocketID]struct{})
			a.rooms[room] = members
		}
		members[id] = struct{}{}
	}
	return nil
}

func (a *memoryAdapter) Delete(ctx context.Context, id SocketID, room Room) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if owned := a.sids[id]; owned != nil {
		delete(owned, room)
		if len(owned) == 0 {
			delete(a.sids, id)
		}
	}
	if members := a.rooms[room]; members != nil {
		delete(members, id)
		if len(members) == 0 {
			delete(a.rooms, room)
		}
	}
	return nil
}

func (a *memoryAdapter) DeleteAll(ctx context.Context, id SocketID) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	for room := range a.sids[id] {
		if members := a.rooms[room]; members != nil {
			delete(members, id)
			if len(members) == 0 {
				delete(a.rooms, room)
			}
		}
	}
	delete(a.sids, id)
	return nil
}

func (a *memoryAdapter) SocketRooms(ctx context.Context, id SocketID) ([]Room, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make([]Room, 0, len(a.sids[id]))
	for room := range a.sids[id] {
		result = append(result, room)
	}
	return result, nil
}

func (a *memoryAdapter) matchingIDs(opts *BroadcastOptions) []SocketID {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.matchingIDsLocked(opts)
}

func (a *memoryAdapter) matchingIDsLocked(opts *BroadcastOptions) []SocketID {
	if opts == nil {
		opts = &BroadcastOptions{}
	}
	excluded := make(map[SocketID]struct{})
	for _, room := range opts.Except {
		for id := range a.rooms[room] {
			excluded[id] = struct{}{}
		}
	}

	ids := make(map[SocketID]struct{})
	if len(opts.Rooms) == 0 {
		for id := range a.sids {
			ids[id] = struct{}{}
		}
	} else {
		for _, room := range opts.Rooms {
			for id := range a.rooms[room] {
				ids[id] = struct{}{}
			}
		}
	}

	result := make([]SocketID, 0, len(ids))
	for id := range ids {
		if _, skip := excluded[id]; !skip {
			result = append(result, id)
		}
	}
	return result
}

func (a *memoryAdapter) Broadcast(ctx context.Context, packet Packet, opts *BroadcastOptions) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if opts == nil {
		opts = &BroadcastOptions{}
	}

	packet = a.persistRecoverablePacket(packet, opts)
	var firstErr error
	for _, id := range a.matchingIDs(opts) {
		if socket, ok := a.nsp.Socket(id); ok {
			if err := socket.sendPacket(packet, opts.Flags); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (a *memoryAdapter) persistRecoverablePacket(packet Packet, opts *BroadcastOptions) Packet {
	if a.nsp.server.cfg.Recovery == nil || packet.Type != PacketEvent || packet.ID != nil || opts.Flags.Volatile {
		return packet
	}
	data, ok := packet.Data.([]any)
	if !ok {
		return packet
	}
	offset := randomID()
	data = append(append([]any(nil), data...), offset)
	packet.Data = data

	a.mu.Lock()
	a.cleanupExpiredLocked(time.Now())
	a.packets = append(a.packets, persistedPacket{
		id:        offset,
		emittedAt: time.Now(),
		data:      append([]any(nil), data...),
		opts:      cloneBroadcastOptions(opts),
	})
	a.mu.Unlock()
	return packet
}

func (a *memoryAdapter) BroadcastWithAck(
	ctx context.Context,
	packet Packet,
	opts *BroadcastOptions,
	clientCount func(uint64),
	ack Ack,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if opts == nil {
		opts = &BroadcastOptions{}
	}
	ids := a.matchingIDs(opts)
	if clientCount != nil {
		clientCount(uint64(len(ids)))
	}
	for _, id := range ids {
		socket, ok := a.nsp.Socket(id)
		if !ok {
			continue
		}
		values, err := socket.emitPacketAck(ctx, packet, opts.Flags)
		if ack != nil {
			ack(values, err)
		}
	}
	return nil
}

func (a *memoryAdapter) FetchSockets(ctx context.Context, opts *BroadcastOptions) ([]SocketDetails, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	ids := a.matchingIDs(opts)
	result := make([]SocketDetails, 0, len(ids))
	for _, id := range ids {
		if socket, ok := a.nsp.Socket(id); ok {
			result = append(result, SocketDetails{
				ID:        id,
				Handshake: socket.Handshake(),
				Rooms:     socket.Rooms(),
				Data:      socket.Data(),
			})
		}
	}
	return result, nil
}

func (a *memoryAdapter) CountSockets(ctx context.Context, opts *BroadcastOptions) (uint64, error) {
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	return uint64(len(a.matchingIDs(opts))), nil
}

func (a *memoryAdapter) ListRooms(ctx context.Context, _ *BroadcastOptions) (map[Room]uint64, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make(map[Room]uint64, len(a.rooms))
	for room, members := range a.rooms {
		result[room] = uint64(len(members))
	}
	return result, nil
}

func (a *memoryAdapter) AddSockets(ctx context.Context, opts *BroadcastOptions, rooms ...Room) error {
	for _, id := range a.matchingIDs(opts) {
		if err := a.AddAll(ctx, id, rooms...); err != nil {
			return err
		}
	}
	return nil
}

func (a *memoryAdapter) DeleteSockets(ctx context.Context, opts *BroadcastOptions, rooms ...Room) error {
	for _, id := range a.matchingIDs(opts) {
		for _, room := range rooms {
			if err := a.Delete(ctx, id, room); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *memoryAdapter) DisconnectSockets(ctx context.Context, opts *BroadcastOptions, closeTransport bool) error {
	for _, id := range a.matchingIDs(opts) {
		if socket, ok := a.nsp.Socket(id); ok {
			if err := socket.Disconnect(closeTransport); err != nil && !errors.Is(err, ErrNotConnected) {
				return err
			}
		}
	}
	return nil
}

func (*memoryAdapter) ServerSideEmit(context.Context, []any) error { return nil }

func (a *memoryAdapter) PersistSession(ctx context.Context, session Session) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if session.PID == "" {
		return ErrInvalidArgument
	}
	a.mu.Lock()
	a.cleanupExpiredLocked(time.Now())
	a.sessions[session.PID] = sessionRecord{session: session, disconnectedAt: time.Now()}
	a.mu.Unlock()
	return nil
}

func (a *memoryAdapter) RestoreSession(
	ctx context.Context,
	pid PrivateSessionID,
	offset string,
) (*RecoveredSession, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cleanupExpiredLocked(time.Now())

	record, ok := a.sessions[pid]
	if !ok {
		return nil, nil
	}
	index := -1
	for i := range a.packets {
		if a.packets[i].id == offset {
			index = i
			break
		}
	}
	if index == -1 {
		return nil, nil
	}

	missed := make([]any, 0, len(a.packets)-index-1)
	for i := index + 1; i < len(a.packets); i++ {
		packet := &a.packets[i]
		if shouldIncludeRecoveredPacket(record.session.Rooms, packet.opts) {
			missed = append(missed, append([]any(nil), packet.data...))
		}
	}
	return &RecoveredSession{Session: record.session, MissedPackets: missed}, nil
}

func (a *memoryAdapter) cleanupExpiredLocked(now time.Time) {
	recovery := a.nsp.server.cfg.Recovery
	if recovery == nil || recovery.MaxDisconnectionDuration <= 0 {
		return
	}
	threshold := now.Add(-recovery.MaxDisconnectionDuration)
	for pid, record := range a.sessions {
		if record.disconnectedAt.Before(threshold) {
			delete(a.sessions, pid)
		}
	}
	firstValid := 0
	for firstValid < len(a.packets) && a.packets[firstValid].emittedAt.Before(threshold) {
		firstValid++
	}
	if firstValid > 0 {
		copy(a.packets, a.packets[firstValid:])
		clear(a.packets[len(a.packets)-firstValid:])
		a.packets = a.packets[:len(a.packets)-firstValid]
	}
}

func shouldIncludeRecoveredPacket(sessionRooms []Room, opts *BroadcastOptions) bool {
	if opts == nil {
		return true
	}
	included := len(opts.Rooms) == 0
	excluded := false
	for _, sessionRoom := range sessionRooms {
		for _, room := range opts.Rooms {
			if sessionRoom == room {
				included = true
				break
			}
		}
		for _, room := range opts.Except {
			if sessionRoom == room {
				excluded = true
				break
			}
		}
		if included && excluded {
			break
		}
	}
	return included && !excluded
}
