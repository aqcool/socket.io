// Package guard provides Socket.IO handshake, event, connection and room limits.
package guard

import (
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	socket "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/servers/socket/v3/presence"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

type Action string

const (
	Reject     Action = "reject"
	Throttle   Action = "throttle"
	Disconnect Action = "disconnect"
)

type RateLimit struct {
	Limit  int
	Window time.Duration
}

type Options struct {
	HandshakeRate         RateLimit
	MaxConnectionsPerUser int
	SocketEventRate       RateLimit
	NamespaceEventRate    RateLimit
	EventRates            map[string]RateLimit
	MaxRoomsPerSocket     int
	MaxRoomMembers        int
	Action                Action
	MaxThrottleDelay      time.Duration
	ClusterQueryTimeout   time.Duration
	UserID                presence.UserIDResolver
}

type AuditEvent struct {
	Rule       string
	Action     Action
	Namespace  string
	SocketID   socket.SocketId
	UserID     string
	IP         string
	Event      string
	Room       socket.Room
	Limit      int
	RetryAfter time.Duration
	At         time.Time
}

var ErrLimitExceeded = errors.New("socket.io guard: limit exceeded")

type window struct {
	start time.Time
	count int
}

type reservation struct {
	userID string
	timer  *time.Timer
}

type Guard struct {
	types.EventEmitter
	server       *socket.Server
	options      Options
	mu           sync.Mutex
	windows      map[string]window
	userCounts   map[string]int
	reservations map[socket.SocketId]reservation
}

func New(server *socket.Server, options *Options) (*Guard, error) {
	if server == nil {
		return nil, errors.New("socket.io guard: server is required")
	}
	opts := normalize(options)
	guard := &Guard{
		EventEmitter: types.NewEventEmitter(),
		server:       server,
		options:      opts,
		windows:      make(map[string]window),
		userCounts:   make(map[string]int),
		reservations: make(map[socket.SocketId]reservation),
	}
	for _, namespace := range server.Namespaces() {
		guard.attach(namespace)
	}
	_ = server.On("new_namespace", func(args ...any) {
		if len(args) > 0 {
			if namespace, ok := args[0].(socket.Namespace); ok {
				guard.attach(namespace)
			}
		}
	})
	return guard, nil
}

func normalize(options *Options) Options {
	opts := Options{
		Action:              Reject,
		MaxThrottleDelay:    5 * time.Second,
		ClusterQueryTimeout: time.Second,
		UserID:              presence.StandardUserID,
	}
	if options != nil {
		opts = *options
	}
	if opts.Action == "" {
		opts.Action = Reject
	}
	if opts.MaxThrottleDelay <= 0 {
		opts.MaxThrottleDelay = 5 * time.Second
	}
	if opts.ClusterQueryTimeout <= 0 {
		opts.ClusterQueryTimeout = time.Second
	}
	if opts.UserID == nil {
		opts.UserID = presence.StandardUserID
	}
	return opts
}

func (g *Guard) attach(namespace socket.Namespace) {
	namespace.Use(func(client *socket.Socket, next func(*socket.ExtendedError)) {
		ip := clientIP(client.Handshake().Address)
		if allowed, retry := g.allow("handshake:"+ip, g.options.HandshakeRate); !allowed {
			g.audit(client, &AuditEvent{Rule: "handshake_rate", IP: ip, Limit: g.options.HandshakeRate.Limit, RetryAfter: retry})
			next(socket.NewExtendedError(ErrLimitExceeded.Error(), map[string]any{"rule": "handshake_rate", "retryAfterMs": retry.Milliseconds()}))
			return
		}
		userID, _ := g.options.UserID(client)
		if g.options.MaxConnectionsPerUser > 0 && userID != "" && !g.reserve(client, userID) {
			g.audit(client, &AuditEvent{Rule: "user_connections", UserID: userID, Limit: g.options.MaxConnectionsPerUser})
			next(socket.NewExtendedError(ErrLimitExceeded.Error(), map[string]any{"rule": "user_connections"}))
			return
		}
		next(nil)
	})
	_ = namespace.On("connection", func(args ...any) {
		if len(args) == 0 {
			return
		}
		client, ok := args[0].(*socket.Socket)
		if !ok {
			return
		}
		g.activate(client)
		client.Use(g.eventMiddleware(client))
		client.UseJoinMiddleware(g.joinMiddleware)
	})
}

func (g *Guard) eventMiddleware(client *socket.Socket) socket.SocketMiddleware {
	return func(args []any, next func(error)) {
		event := ""
		if len(args) > 0 {
			event, _ = args[0].(string)
		}
		rules := []struct {
			name  string
			key   string
			limit RateLimit
		}{
			{"socket_event_rate", "socket:" + string(client.Id()), g.options.SocketEventRate},
			{"namespace_event_rate", "namespace:" + client.Nsp().Name(), g.options.NamespaceEventRate},
			{"event_rate", "event:" + client.Nsp().Name() + ":" + event, g.options.EventRates[event]},
		}
		for _, rule := range rules {
			if allowed, retry := g.allow(rule.key, rule.limit); !allowed {
				g.audit(client, &AuditEvent{Rule: rule.name, Event: event, Limit: rule.limit.Limit, RetryAfter: retry})
				g.apply(client, retry, next)
				return
			}
		}
		next(nil)
	}
}

func (g *Guard) joinMiddleware(client *socket.Socket, rooms []socket.Room) error {
	current := client.Rooms()
	additional := 0
	for _, room := range rooms {
		if room == socket.Room(client.Id()) || strings.HasPrefix(string(room), "\x00") {
			continue
		}
		if !current.Has(room) {
			additional++
		}
	}
	if g.options.MaxRoomsPerSocket > 0 && publicRoomCount(client, current)+additional > g.options.MaxRoomsPerSocket {
		g.audit(client, &AuditEvent{Rule: "socket_rooms", Limit: g.options.MaxRoomsPerSocket})
		if g.options.Action == Disconnect {
			client.Disconnect(true)
		}
		return ErrLimitExceeded
	}
	if g.options.MaxRoomMembers <= 0 {
		return nil
	}
	for _, room := range rooms {
		if room == socket.Room(client.Id()) || strings.HasPrefix(string(room), "\x00") || current.Has(room) {
			continue
		}
		count, err := g.roomCount(client.Nsp(), room)
		if err != nil {
			return err
		}
		if count >= uint64(g.options.MaxRoomMembers) {
			g.audit(client, &AuditEvent{Rule: "room_members", Room: room, Limit: g.options.MaxRoomMembers})
			if g.options.Action == Disconnect {
				client.Disconnect(true)
			}
			return ErrLimitExceeded
		}
	}
	return nil
}

func (g *Guard) roomCount(namespace socket.Namespace, room socket.Room) (uint64, error) {
	result := make(chan struct {
		count uint64
		err   error
	}, 1)
	namespace.In(room).CountSockets()(func(count uint64, err error) {
		result <- struct {
			count uint64
			err   error
		}{count: count, err: err}
	})
	select {
	case value := <-result:
		return value.count, value.err
	case <-time.After(g.options.ClusterQueryTimeout):
		return 0, errors.New("socket.io guard: room count timed out")
	}
}

func (g *Guard) allow(key string, limit RateLimit) (bool, time.Duration) {
	if limit.Limit <= 0 || limit.Window <= 0 {
		return true, 0
	}
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	value := g.windows[key]
	if value.start.IsZero() || now.Sub(value.start) >= limit.Window {
		g.windows[key] = window{start: now, count: 1}
		return true, 0
	}
	if value.count >= limit.Limit {
		return false, limit.Window - now.Sub(value.start)
	}
	value.count++
	g.windows[key] = value
	return true, 0
}

func (g *Guard) reserve(client *socket.Socket, userID string) bool {
	count, err := g.roomCount(client.Nsp(), socket.Room(presence.DefaultUserRoomPrefix+userID))
	if err != nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if int(count)+g.userCounts[userID] >= g.options.MaxConnectionsPerUser {
		return false
	}
	// userCounts tracks only handshakes which passed the distributed count but
	// have not reached the connection event yet.
	g.userCounts[userID]++
	timer := time.AfterFunc(30*time.Second, func() { g.releaseReservation(client.Id()) })
	g.reservations[client.Id()] = reservation{userID: userID, timer: timer}
	return true
}

func (g *Guard) activate(client *socket.Socket) {
	g.mu.Lock()
	reserved, ok := g.reservations[client.Id()]
	if ok {
		reserved.timer.Stop()
		delete(g.reservations, client.Id())
		if g.userCounts[reserved.userID] > 1 {
			g.userCounts[reserved.userID]--
		} else {
			delete(g.userCounts, reserved.userID)
		}
	}
	g.mu.Unlock()
	if !ok {
		return
	}
	client.Join(socket.Room(presence.DefaultUserRoomPrefix + reserved.userID))
}

func (g *Guard) releaseReservation(id socket.SocketId) {
	g.mu.Lock()
	reserved, ok := g.reservations[id]
	if ok {
		delete(g.reservations, id)
		if g.userCounts[reserved.userID] > 1 {
			g.userCounts[reserved.userID]--
		} else {
			delete(g.userCounts, reserved.userID)
		}
	}
	g.mu.Unlock()
}

func (g *Guard) apply(client *socket.Socket, retry time.Duration, next func(error)) {
	switch g.options.Action {
	case Throttle:
		if retry > g.options.MaxThrottleDelay {
			retry = g.options.MaxThrottleDelay
		}
		time.AfterFunc(retry, func() { next(nil) })
	case Disconnect:
		client.Disconnect(true)
		next(ErrLimitExceeded)
	default:
		next(ErrLimitExceeded)
	}
}

func (g *Guard) audit(client *socket.Socket, event *AuditEvent) {
	event.Action = g.options.Action
	event.At = time.Now()
	if client != nil {
		event.Namespace = client.Nsp().Name()
		event.SocketID = client.Id()
		if event.IP == "" {
			event.IP = clientIP(client.Handshake().Address)
		}
	}
	g.Emit("audit", *event)
	g.server.EmitReserved("audit", *event)
}

func publicRoomCount(client *socket.Socket, rooms *types.Set[socket.Room]) int {
	count := 0
	for _, room := range rooms.Keys() {
		if room != socket.Room(client.Id()) && !strings.HasPrefix(string(room), "\x00") {
			count++
		}
	}
	return count
}

func clientIP(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err == nil {
		return host
	}
	return address
}
