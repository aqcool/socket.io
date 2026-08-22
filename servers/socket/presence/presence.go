// Package presence provides cluster-aware user presence and room statistics.
package presence

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	socket "github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

const (
	DefaultUserRoomPrefix = "\x00presence:user:"
	metadataEvent         = "\x00presence:metadata"
)

var ErrInvalidOptions = errors.New("socket.io presence: invalid options")

type UserIDResolver func(*socket.Socket) (string, bool)
type RoomFilter func(string, socket.Room) bool

type Options struct {
	UserID          UserIDResolver
	UserRoomPrefix  string
	EmptyRoomTTL    time.Duration
	CleanupInterval time.Duration
	RoomFilter      RoomFilter
}

type RoomStats struct {
	Namespace  string      `json:"namespace"`
	Room       socket.Room `json:"room"`
	Count      uint64      `json:"count"`
	Metadata   any         `json:"metadata,omitempty"`
	EmptySince *time.Time  `json:"emptySince,omitempty"`
	ExpiresAt  *time.Time  `json:"expiresAt,omitempty"`
}

type RoomCountChange struct {
	Namespace string      `json:"namespace"`
	Room      socket.Room `json:"room"`
	Count     uint64      `json:"count"`
	Delta     int64       `json:"delta"`
	At        time.Time   `json:"at"`
}

type roomKey struct {
	namespace string
	room      socket.Room
}

type roomRecord struct {
	metadata   any
	emptySince *time.Time
	expiresAt  *time.Time
}

type namespaceListeners struct {
	namespace  socket.Namespace
	connection types.EventListener
	join       types.EventListener
	leave      types.EventListener
	metadata   types.EventListener
}

// Tracker manages presence across every namespace of a Socket.IO server.
type Tracker struct {
	types.EventEmitter
	server               *socket.Server
	options              Options
	mu                   sync.RWMutex
	rooms                map[roomKey]*roomRecord
	listeners            map[string]namespaceListeners
	newNamespaceListener types.EventListener
	stop                 chan struct{}
	done                 chan struct{}
	closeOnce            sync.Once
}

func New(server *socket.Server, options *Options) (*Tracker, error) {
	if server == nil {
		return nil, ErrInvalidOptions
	}
	opts := normalizeOptions(options)
	tracker := &Tracker{
		EventEmitter: types.NewEventEmitter(),
		server:       server,
		options:      opts,
		rooms:        make(map[roomKey]*roomRecord),
		listeners:    make(map[string]namespaceListeners),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	tracker.newNamespaceListener = tracker.onNewNamespace
	for _, namespace := range server.Namespaces() {
		tracker.attach(namespace)
	}
	_ = server.Sockets().On("new_namespace", tracker.newNamespaceListener)
	go tracker.cleanupLoop()
	return tracker, nil
}

func normalizeOptions(options *Options) Options {
	opts := Options{
		UserID:          StandardUserID,
		UserRoomPrefix:  DefaultUserRoomPrefix,
		EmptyRoomTTL:    5 * time.Minute,
		CleanupInterval: time.Minute,
	}
	if options == nil {
		return opts
	}
	opts = *options
	if opts.UserID == nil {
		opts.UserID = StandardUserID
	}
	if opts.UserRoomPrefix == "" {
		opts.UserRoomPrefix = DefaultUserRoomPrefix
	}
	if opts.EmptyRoomTTL < 0 {
		opts.EmptyRoomTTL = 0
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = time.Minute
		if opts.EmptyRoomTTL > 0 && opts.EmptyRoomTTL < opts.CleanupInterval {
			opts.CleanupInterval = opts.EmptyRoomTTL
		}
	}
	return opts
}

// StandardUserID resolves data.userId, then handshake auth.userId.
func StandardUserID(s *socket.Socket) (string, bool) {
	if s == nil {
		return "", false
	}
	if values, ok := s.Data().(map[string]any); ok {
		if userID, ok := values["userId"].(string); ok && userID != "" {
			return userID, true
		}
	}
	if userID, ok := s.Handshake().Auth["userId"].(string); ok && userID != "" {
		return userID, true
	}
	return "", false
}

// DataUserID creates a resolver for a custom field in Socket data.
func DataUserID(field string) UserIDResolver {
	return func(s *socket.Socket) (string, bool) {
		if s == nil {
			return "", false
		}
		values, ok := s.Data().(map[string]any)
		if !ok {
			return "", false
		}
		userID, ok := values[field].(string)
		return userID, ok && userID != ""
	}
}

func (t *Tracker) userRoom(userID string) socket.Room {
	return socket.Room(t.options.UserRoomPrefix + userID)
}

func (t *Tracker) onNewNamespace(args ...any) {
	if len(args) == 0 {
		return
	}
	if namespace, ok := args[0].(socket.Namespace); ok {
		t.attach(namespace)
	}
}

func (t *Tracker) attach(namespace socket.Namespace) {
	t.mu.Lock()
	if _, exists := t.listeners[namespace.Name()]; exists {
		t.mu.Unlock()
		return
	}
	listeners := namespaceListeners{namespace: namespace}
	listeners.connection = func(args ...any) {
		if len(args) == 0 || t.options.UserID == nil {
			return
		}
		if s, ok := args[0].(*socket.Socket); ok {
			if userID, resolved := t.options.UserID(s); resolved && userID != "" {
				s.Join(t.userRoom(userID))
			}
		}
	}
	listeners.join = func(args ...any) {
		t.onRoomEvent(namespace, 1, args...)
	}
	listeners.leave = func(args ...any) {
		t.onRoomEvent(namespace, -1, args...)
	}
	listeners.metadata = func(args ...any) {
		t.applyMetadataEvent(namespace.Name(), args...)
	}
	t.listeners[namespace.Name()] = listeners
	t.mu.Unlock()

	_ = namespace.On("connection", listeners.connection)
	_ = namespace.On(metadataEvent, listeners.metadata)
	_ = namespace.Adapter().On("join-room", listeners.join)
	_ = namespace.Adapter().On("leave-room", listeners.leave)
}

func (t *Tracker) onRoomEvent(namespace socket.Namespace, delta int64, args ...any) {
	if len(args) < 2 {
		return
	}
	room, roomOK := args[0].(socket.Room)
	id, idOK := args[1].(socket.SocketId)
	if !roomOK || !idOK || room == socket.Room(id) || strings.HasPrefix(string(room), t.options.UserRoomPrefix) {
		return
	}
	if t.options.RoomFilter != nil && !t.options.RoomFilter(namespace.Name(), room) {
		return
	}
	go namespace.In(room).CountSockets()(func(count uint64, err error) {
		if err != nil {
			t.Emit("error", err)
			return
		}
		t.updateEmptyState(namespace.Name(), room, count)
		t.Emit("room_count_changed", RoomCountChange{
			Namespace: namespace.Name(),
			Room:      room,
			Count:     count,
			Delta:     delta,
			At:        time.Now(),
		})
	})
}

func (t *Tracker) updateEmptyState(namespace string, room socket.Room, count uint64) {
	key := roomKey{namespace: namespace, room: room}
	t.mu.Lock()
	defer t.mu.Unlock()
	record, exists := t.rooms[key]
	if count > 0 {
		if exists {
			record.emptySince = nil
			record.expiresAt = nil
		}
		return
	}
	if !exists {
		record = &roomRecord{}
		t.rooms[key] = record
	}
	now := time.Now()
	record.emptySince = &now
	if t.options.EmptyRoomTTL > 0 {
		expiresAt := now.Add(t.options.EmptyRoomTTL)
		record.expiresAt = &expiresAt
	} else if record.metadata == nil {
		delete(t.rooms, key)
	}
}

// BindUser adds a Socket to the standard cluster-visible user room.
func (t *Tracker) BindUser(s *socket.Socket, userID string) error {
	if s == nil || userID == "" {
		return ErrInvalidOptions
	}
	s.Join(t.userRoom(userID))
	return nil
}

func (t *Tracker) CountSockets(namespace string) func(func(uint64, error)) {
	return t.server.Of(namespace, nil).CountSockets()
}

func (t *Tracker) CountRoom(namespace string, room socket.Room) func(func(uint64, error)) {
	return t.server.Of(namespace, nil).In(room).CountSockets()
}

func (t *Tracker) IsUserOnline(namespace, userID string) func(func(bool, error)) {
	return func(callback func(bool, error)) {
		t.CountRoom(namespace, t.userRoom(userID))(func(count uint64, err error) {
			callback(count > 0, err)
		})
	}
}

func (t *Tracker) UserSockets(namespace, userID string) func(func([]*socket.RemoteSocket, error)) {
	return t.server.Of(namespace, nil).In(t.userRoom(userID)).FetchSockets()
}

func (t *Tracker) ListRooms(namespace string) func(func([]RoomStats, error)) {
	return func(callback func([]RoomStats, error)) {
		t.server.Of(namespace, nil).ListRooms()(func(counts map[socket.Room]uint64, err error) {
			if err != nil {
				callback(nil, err)
				return
			}
			stats := t.mergeRoomStats(namespace, counts)
			callback(stats, nil)
		})
	}
}

func (t *Tracker) RoomStats(namespace string, room socket.Room) func(func(RoomStats, error)) {
	return func(callback func(RoomStats, error)) {
		t.CountRoom(namespace, room)(func(count uint64, err error) {
			if err != nil {
				callback(RoomStats{}, err)
				return
			}
			stats := t.roomStats(namespace, room, count)
			callback(stats, nil)
		})
	}
}

func (t *Tracker) mergeRoomStats(namespace string, counts map[socket.Room]uint64) []RoomStats {
	t.mu.RLock()
	for key := range t.rooms {
		if key.namespace == namespace {
			if _, exists := counts[key.room]; !exists {
				counts[key.room] = 0
			}
		}
	}
	t.mu.RUnlock()
	stats := make([]RoomStats, 0, len(counts))
	for room, count := range counts {
		if strings.HasPrefix(string(room), t.options.UserRoomPrefix) {
			continue
		}
		if t.options.RoomFilter != nil && !t.options.RoomFilter(namespace, room) {
			continue
		}
		stats = append(stats, t.roomStats(namespace, room, count))
	}
	sort.Slice(stats, func(a, b int) bool {
		return stats[a].Room < stats[b].Room
	})
	return stats
}

func (t *Tracker) roomStats(namespace string, room socket.Room, count uint64) RoomStats {
	stats := RoomStats{Namespace: namespace, Room: room, Count: count}
	t.mu.RLock()
	if record := t.rooms[roomKey{namespace: namespace, room: room}]; record != nil {
		stats.Metadata = record.metadata
		stats.EmptySince = cloneTime(record.emptySince)
		stats.ExpiresAt = cloneTime(record.expiresAt)
	}
	t.mu.RUnlock()
	return stats
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func (t *Tracker) SetRoomMetadata(namespace string, room socket.Room, metadata any) error {
	if namespace == "" || room == "" {
		return ErrInvalidOptions
	}
	t.setMetadata(namespace, room, metadata)
	nsp := t.server.Of(namespace, nil)
	nsp.In(room).CountSockets()(func(count uint64, countErr error) {
		if countErr == nil {
			t.updateEmptyState(namespace, room, count)
		}
	})
	err := nsp.ServerSideEmit(metadataEvent, "set", string(room), metadata)
	if errors.Is(err, socket.ErrServerSideEmitNotSupported) {
		return nil
	}
	return err
}

func (t *Tracker) DeleteRoomMetadata(namespace string, room socket.Room) error {
	t.setMetadata(namespace, room, nil)
	nsp := t.server.Of(namespace, nil)
	err := nsp.ServerSideEmit(metadataEvent, "delete", string(room))
	if errors.Is(err, socket.ErrServerSideEmitNotSupported) {
		return nil
	}
	return err
}

func (t *Tracker) applyMetadataEvent(namespace string, args ...any) {
	if len(args) < 2 {
		return
	}
	action, actionOK := args[0].(string)
	roomName, roomOK := args[1].(string)
	if !actionOK || !roomOK {
		return
	}
	switch action {
	case "set":
		if len(args) > 2 {
			t.setMetadata(namespace, socket.Room(roomName), args[2])
		}
	case "delete":
		t.setMetadata(namespace, socket.Room(roomName), nil)
	}
}

func (t *Tracker) setMetadata(namespace string, room socket.Room, metadata any) {
	key := roomKey{namespace: namespace, room: room}
	t.mu.Lock()
	record := t.rooms[key]
	if record == nil {
		record = &roomRecord{}
		t.rooms[key] = record
	}
	record.metadata = metadata
	if metadata == nil && record.emptySince == nil {
		delete(t.rooms, key)
	}
	t.mu.Unlock()
}

func (t *Tracker) cleanupLoop() {
	defer close(t.done)
	ticker := time.NewTicker(t.options.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			t.cleanup(now)
		case <-t.stop:
			return
		}
	}
}

func (t *Tracker) cleanup(now time.Time) {
	expired := make([]RoomStats, 0)
	t.mu.Lock()
	for key, record := range t.rooms {
		if record.expiresAt != nil && !now.Before(*record.expiresAt) {
			delete(t.rooms, key)
			expired = append(expired, RoomStats{
				Namespace:  key.namespace,
				Room:       key.room,
				Count:      0,
				Metadata:   record.metadata,
				EmptySince: cloneTime(record.emptySince),
				ExpiresAt:  cloneTime(record.expiresAt),
			})
		}
	}
	t.mu.Unlock()
	for _, stats := range expired {
		t.Emit("room_expired", stats)
	}
}

func (t *Tracker) Close() {
	t.closeOnce.Do(func() {
		close(t.stop)
		<-t.done
		t.server.Sockets().EventEmitter().RemoveListener("new_namespace", t.newNamespaceListener)
		t.mu.Lock()
		listeners := make([]namespaceListeners, 0, len(t.listeners))
		for _, listener := range t.listeners {
			listeners = append(listeners, listener)
		}
		t.listeners = make(map[string]namespaceListeners)
		t.mu.Unlock()
		for _, listener := range listeners {
			listener.namespace.EventEmitter().RemoveListener("connection", listener.connection)
			listener.namespace.EventEmitter().RemoveListener(metadataEvent, listener.metadata)
			listener.namespace.Adapter().RemoveListener("join-room", listener.join)
			listener.namespace.Adapter().RemoveListener("leave-room", listener.leave)
		}
	})
}
