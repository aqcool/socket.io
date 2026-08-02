// Package instrumentation exposes a Socket.IO server through the protocol used
// by the official Socket.IO Admin UI.
package instrumentation

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"os"
	"sort"
	"sync"
	"time"

	enginepacket "github.com/aqcool/socket.io/parsers/engine/v3/packet"
	engine "github.com/aqcool/socket.io/servers/engine/v3"
	socket "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"golang.org/x/crypto/bcrypt"
)

const (
	DefaultNamespace     = "/admin"
	DefaultStatsInterval = 2 * time.Second
)

// Mode controls the amount of runtime detail sent to the Admin UI.
type Mode string

const (
	DevelopmentMode Mode = "development"
	ProductionMode  Mode = "production"
)

// Feature names are part of the official @socket.io/admin-ui protocol.
type Feature string

const (
	FeatureEmit             Feature = "EMIT"
	FeatureJoin             Feature = "JOIN"
	FeatureLeave            Feature = "LEAVE"
	FeatureDisconnect       Feature = "DISCONNECT"
	FeatureMultiJoin        Feature = "MJOIN"
	FeatureMultiLeave       Feature = "MLEAVE"
	FeatureMultiDisconnect  Feature = "MDISCONNECT"
	FeatureAggregatedEvents Feature = "AGGREGATED_EVENTS"
	FeatureAllEvents        Feature = "ALL_EVENTS"
)

var ErrInvalidOptions = errors.New("instrumentation: invalid options")

// BasicAuth contains credentials required by the Admin UI namespace.
type BasicAuth struct {
	Username     string
	Password     string
	PasswordHash string
}

// SessionStore keeps Admin UI authentication sessions so reconnecting clients
// can send sessionId instead of credentials, as defined by @socket.io/admin-ui.
type SessionStore interface {
	DoesSessionExist(string) (bool, error)
	SaveSession(string) error
}

// InMemorySessionStore is the default process-local Admin UI session store.
type InMemorySessionStore struct {
	mu       sync.RWMutex
	sessions map[string]struct{}
}

func NewInMemorySessionStore() *InMemorySessionStore {
	return &InMemorySessionStore{sessions: make(map[string]struct{})}
}

func (s *InMemorySessionStore) DoesSessionExist(id string) (bool, error) {
	s.mu.RLock()
	_, ok := s.sessions[id]
	s.mu.RUnlock()
	return ok, nil
}

func (s *InMemorySessionStore) SaveSession(id string) error {
	s.mu.Lock()
	s.sessions[id] = struct{}{}
	s.mu.Unlock()
	return nil
}

// Options configures Instrument.
type Options struct {
	NamespaceName string
	ServerID      string
	ReadOnly      bool
	Mode          Mode
	StatsInterval time.Duration
	BasicAuth     *BasicAuth
	SessionStore  SessionStore
	Auth          socket.NamespaceMiddleware
}

// Config is emitted to every Admin UI client on connection.
type Config struct {
	SupportedFeatures []Feature `json:"supportedFeatures"`
}

type NamespaceDetails struct {
	Name         string `json:"name"`
	SocketsCount int    `json:"socketsCount"`
}

type NamespaceEvent struct {
	Timestamp int64  `json:"timestamp"`
	Type      string `json:"type"`
	SubType   string `json:"subType,omitempty"`
	Count     int64  `json:"count"`
}

type ServerStats struct {
	ServerID            string             `json:"serverId"`
	Hostname            string             `json:"hostname"`
	PID                 int                `json:"pid"`
	Uptime              float64            `json:"uptime"`
	ClientsCount        uint64             `json:"clientsCount"`
	PollingClientsCount int                `json:"pollingClientsCount"`
	AggregatedEvents    []NamespaceEvent   `json:"aggregatedEvents"`
	Namespaces          []NamespaceDetails `json:"namespaces"`
}

type SerializedSocket struct {
	ID        string         `json:"id"`
	ClientID  string         `json:"clientId,omitempty"`
	Transport string         `json:"transport,omitempty"`
	Namespace string         `json:"nsp"`
	Data      any            `json:"data"`
	Handshake map[string]any `json:"handshake"`
	Rooms     []string       `json:"rooms"`
}

// Instrumentation is a running Admin UI protocol bridge.
type Instrumentation struct {
	server                    *socket.Server
	admin                     socket.Namespace
	options                   Options
	startedAt                 time.Time
	stop                      chan struct{}
	done                      chan struct{}
	closeOnce                 sync.Once
	mu                        sync.Mutex
	events                    map[string]*NamespaceEvent
	instrumented              map[string]struct{}
	engineAttached            bool
	engine                    engine.BaseServer
	newNamespaceListener      types.EventListener
	engineConnectionListener  types.EventListener
	engineInitializedListener types.EventListener
}

// Instrument enables official Socket.IO Admin UI support on server.
func Instrument(server *socket.Server, options *Options) (*Instrumentation, error) {
	if server == nil {
		return nil, ErrInvalidOptions
	}

	opts := normalizeOptions(options)
	if opts.Mode != DevelopmentMode && opts.Mode != ProductionMode {
		return nil, ErrInvalidOptions
	}
	if opts.StatsInterval <= 0 {
		return nil, ErrInvalidOptions
	}
	if opts.BasicAuth != nil && opts.BasicAuth.PasswordHash != "" {
		if _, err := bcrypt.Cost([]byte(opts.BasicAuth.PasswordHash)); err != nil {
			return nil, ErrInvalidOptions
		}
	}

	i := &Instrumentation{
		server:       server,
		options:      opts,
		startedAt:    time.Now(),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
		events:       make(map[string]*NamespaceEvent),
		instrumented: make(map[string]struct{}),
	}
	i.admin = server.Of(opts.NamespaceName, nil)
	i.installAuthentication()
	i.newNamespaceListener = i.onNewNamespace
	i.engineConnectionListener = i.onEngineConnection
	i.engineInitializedListener = i.onEngineInitialized

	for _, nsp := range server.Namespaces() {
		i.instrumentNamespace(nsp)
	}
	_ = server.Sockets().On("new_namespace", i.newNamespaceListener)
	_ = server.On("engine_initialized", i.engineInitializedListener)
	if engineServer := server.Engine(); engineServer != nil {
		i.attachEngineListener(engineServer)
	}
	go i.statsLoop()
	return i, nil
}

func (i *Instrumentation) onEngineInitialized(args ...any) {
	if len(args) == 0 {
		return
	}
	if engineServer, ok := args[0].(engine.BaseServer); ok {
		i.attachEngineListener(engineServer)
	}
}

func (i *Instrumentation) attachEngineListener(engineServer engine.BaseServer) {
	i.mu.Lock()
	if i.engineAttached {
		i.mu.Unlock()
		return
	}
	i.engine = engineServer
	i.engineAttached = true
	i.mu.Unlock()
	_ = engineServer.On("connection", i.engineConnectionListener)
}

func normalizeOptions(options *Options) Options {
	opts := Options{
		NamespaceName: DefaultNamespace,
		Mode:          DevelopmentMode,
		StatsInterval: DefaultStatsInterval,
		SessionStore:  NewInMemorySessionStore(),
	}
	if options == nil {
		return opts
	}
	opts = *options
	if opts.NamespaceName == "" {
		opts.NamespaceName = DefaultNamespace
	}
	if opts.NamespaceName[0] != '/' {
		opts.NamespaceName = "/" + opts.NamespaceName
	}
	if opts.Mode == "" {
		opts.Mode = DevelopmentMode
	}
	if opts.StatsInterval == 0 {
		opts.StatsInterval = DefaultStatsInterval
	}
	if opts.SessionStore == nil {
		opts.SessionStore = NewInMemorySessionStore()
	}
	return opts
}

func (i *Instrumentation) installAuthentication() {
	if i.options.BasicAuth != nil || i.options.Auth != nil {
		i.admin.Use(func(s *socket.Socket, next func(*socket.ExtendedError)) {
			var newSessionID string
			if credentials := i.options.BasicAuth; credentials != nil {
				auth := s.Handshake().Auth
				authenticated := false
				if sessionID, ok := auth["sessionId"].(string); ok && sessionID != "" {
					exists, err := i.options.SessionStore.DoesSessionExist(sessionID)
					if err != nil {
						next(socket.NewExtendedError("session store error", nil))
						return
					}
					authenticated = exists
				}
				if !authenticated {
					username, usernameOK := auth["username"].(string)
					password, passwordOK := auth["password"].(string)
					if !usernameOK || !passwordOK ||
						!secureEqual(username, credentials.Username) ||
						!matchesPassword(password, credentials) {
						next(socket.NewExtendedError("invalid credentials", nil))
						return
					}
					generated := make([]byte, 8)
					if _, err := rand.Read(generated); err != nil {
						next(socket.NewExtendedError("session generation error", nil))
						return
					}
					newSessionID = hex.EncodeToString(generated)
				}
			}
			finish := func(err *socket.ExtendedError) {
				if err != nil {
					next(err)
					return
				}
				if newSessionID != "" {
					if err := i.options.SessionStore.SaveSession(newSessionID); err != nil {
						next(socket.NewExtendedError("session store error", nil))
						return
					}
					_ = s.Emit("session", newSessionID)
				}
				next(nil)
			}
			if i.options.Auth != nil {
				i.options.Auth(s, finish)
				return
			}
			finish(nil)
		})
	}
}

func matchesPassword(actual string, credentials *BasicAuth) bool {
	if credentials.PasswordHash != "" {
		return bcrypt.CompareHashAndPassword([]byte(credentials.PasswordHash), []byte(actual)) == nil
	}
	return secureEqual(actual, credentials.Password)
}

func secureEqual(actual, expected string) bool {
	if len(actual) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) == 1
}

func (i *Instrumentation) onNewNamespace(args ...any) {
	if len(args) == 0 {
		return
	}
	if nsp, ok := args[0].(socket.Namespace); ok {
		i.instrumentNamespace(nsp)
	}
}

func (i *Instrumentation) instrumentNamespace(nsp socket.Namespace) {
	i.mu.Lock()
	if _, ok := i.instrumented[nsp.Name()]; ok {
		i.mu.Unlock()
		return
	}
	i.instrumented[nsp.Name()] = struct{}{}
	i.mu.Unlock()

	_ = nsp.On("connection", func(args ...any) {
		if len(args) == 0 {
			return
		}
		s, ok := args[0].(*socket.Socket)
		if !ok {
			return
		}
		if nsp == i.admin {
			i.configureAdminSocket(s)
		}
		if i.options.Mode == DevelopmentMode {
			i.observeSocket(nsp, s)
		}
	})

	adapter := nsp.Adapter()
	_ = adapter.On("join-room", func(args ...any) {
		if len(args) < 2 {
			return
		}
		room, roomOK := args[0].(socket.Room)
		id, idOK := args[1].(socket.SocketId)
		if roomOK && idOK {
			_ = i.admin.Emit("room_joined", nsp.Name(), string(room), string(id), adminTimestamp())
		}
	})
	_ = adapter.On("leave-room", func(args ...any) {
		if len(args) < 2 {
			return
		}
		room, roomOK := args[0].(socket.Room)
		id, idOK := args[1].(socket.SocketId)
		if roomOK && idOK {
			_ = i.admin.Emit("room_left", nsp.Name(), string(room), string(id), adminTimestamp())
		}
	})
}

func (i *Instrumentation) configureAdminSocket(s *socket.Socket) {
	features := i.supportedFeatures()
	_ = s.Emit("config", Config{SupportedFeatures: features})
	if i.options.Mode == DevelopmentMode {
		i.emitAllSockets(s)
	}
	if i.options.ReadOnly {
		return
	}
	_ = s.On("emit", i.handleEmit)
	_ = s.On("join", i.handleJoin)
	_ = s.On("leave", i.handleLeave)
	_ = s.On("_disconnect", i.handleDisconnect)
}

func (i *Instrumentation) supportedFeatures() []Feature {
	features := make([]Feature, 0, 9)
	if !i.options.ReadOnly {
		features = append(features,
			FeatureEmit, FeatureJoin, FeatureLeave, FeatureDisconnect,
			FeatureMultiJoin, FeatureMultiLeave, FeatureMultiDisconnect,
		)
	}
	features = append(features, FeatureAggregatedEvents)
	if i.options.Mode == DevelopmentMode {
		features = append(features, FeatureAllEvents)
	}
	return features
}

func (i *Instrumentation) observeSocket(nsp socket.Namespace, s *socket.Socket) {
	_ = i.admin.Emit("socket_connected", serializeLocalSocket(nsp.Name(), s), adminTimestamp())

	if nsp != i.admin {
		s.OnAny(func(args ...any) {
			eventArgs := append([]any(nil), stripAck(args)...)
			go func() {
				_ = i.admin.Emit("event_received", nsp.Name(), string(s.Id()), eventArgs, adminTimestamp())
			}()
		})
		s.OnAnyOutgoing(func(args ...any) {
			eventArgs := append([]any(nil), args...)
			go func() {
				_ = i.admin.Emit("event_sent", nsp.Name(), string(s.Id()), eventArgs, adminTimestamp())
			}()
		})
	}
	_ = s.On("disconnect", func(args ...any) {
		reason := ""
		if len(args) > 0 {
			reason, _ = args[0].(string)
		}
		_ = i.admin.Emit("socket_disconnected", nsp.Name(), string(s.Id()), reason, adminTimestamp())
	})
	_ = s.Conn().On("upgrade", func(...any) {
		_ = i.admin.Emit("socket_updated", map[string]any{
			"id":        string(s.Id()),
			"nsp":       nsp.Name(),
			"transport": s.Conn().Transport().Name(),
		})
	})
}

// adminTimestamp matches the JSON representation of the JavaScript Date values
// emitted by @socket.io/admin-ui: UTC with exactly millisecond precision.
func adminTimestamp() string {
	return time.Now().UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z")
}

func stripAck(args []any) []any {
	if len(args) == 0 {
		return args
	}
	if _, ok := args[len(args)-1].(socket.Ack); ok {
		return args[:len(args)-1]
	}
	return args
}

func (i *Instrumentation) handleEmit(args ...any) {
	if len(args) < 3 {
		return
	}
	nspName, ok1 := args[0].(string)
	event, ok2 := args[2].(string)
	if !ok1 || !ok2 {
		return
	}
	nsp := i.server.Of(nspName, nil)
	payload := args[3:]
	if filter, ok := optionalString(args[1]); ok {
		_ = nsp.In(socket.Room(filter)).Emit(event, payload...)
		return
	}
	_ = nsp.Emit(event, payload...)
}

func (i *Instrumentation) handleJoin(args ...any) {
	if len(args) < 2 {
		return
	}
	nspName, ok1 := args[0].(string)
	room, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return
	}
	nsp := i.server.Of(nspName, nil)
	if len(args) > 2 {
		if filter, ok := optionalString(args[2]); ok {
			nsp.In(socket.Room(filter)).SocketsJoin(socket.Room(room))
			return
		}
	}
	nsp.SocketsJoin(socket.Room(room))
}

func (i *Instrumentation) handleLeave(args ...any) {
	if len(args) < 2 {
		return
	}
	nspName, ok1 := args[0].(string)
	room, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return
	}
	nsp := i.server.Of(nspName, nil)
	if len(args) > 2 {
		if filter, ok := optionalString(args[2]); ok {
			nsp.In(socket.Room(filter)).SocketsLeave(socket.Room(room))
			return
		}
	}
	nsp.SocketsLeave(socket.Room(room))
}

func (i *Instrumentation) handleDisconnect(args ...any) {
	if len(args) < 2 {
		return
	}
	nspName, ok1 := args[0].(string)
	closeConnection, ok2 := args[1].(bool)
	if !ok1 || !ok2 {
		return
	}
	nsp := i.server.Of(nspName, nil)
	if len(args) > 2 {
		if filter, ok := optionalString(args[2]); ok {
			nsp.In(socket.Room(filter)).DisconnectSockets(closeConnection)
			return
		}
	}
	nsp.DisconnectSockets(closeConnection)
}

func optionalString(value any) (string, bool) {
	if value == nil {
		return "", false
	}
	valueString, ok := value.(string)
	return valueString, ok && valueString != ""
}

func (i *Instrumentation) emitAllSockets(adminSocket *socket.Socket) {
	namespaces := i.server.Namespaces()
	if len(namespaces) == 0 {
		_ = adminSocket.Emit("all_sockets", []SerializedSocket{})
		return
	}

	var mu sync.Mutex
	var wait sync.WaitGroup
	all := make([]SerializedSocket, 0)
	for _, nsp := range namespaces {
		wait.Add(1)
		nsp.FetchSockets()(func(sockets []*socket.RemoteSocket, _ error) {
			defer wait.Done()
			items := make([]SerializedSocket, 0, len(sockets))
			for _, remote := range sockets {
				items = append(items, serializeRemoteSocket(nsp.Name(), remote))
			}
			mu.Lock()
			all = append(all, items...)
			mu.Unlock()
		})
	}
	go func() {
		wait.Wait()
		sort.Slice(all, func(a, b int) bool {
			if all[a].Namespace == all[b].Namespace {
				return all[a].ID < all[b].ID
			}
			return all[a].Namespace < all[b].Namespace
		})
		_ = adminSocket.Emit("all_sockets", all)
	}()
}

func serializeLocalSocket(namespace string, s *socket.Socket) SerializedSocket {
	serialized := serializeSocket(namespace, string(s.Id()), s.Handshake(), s.Rooms(), s.Data())
	clientID := s.Client().Conn().Id()
	if len(clientID) > 12 {
		clientID = clientID[:12]
	}
	serialized.ClientID = clientID
	serialized.Transport = s.Conn().Transport().Name()
	return serialized
}

func serializeRemoteSocket(namespace string, s *socket.RemoteSocket) SerializedSocket {
	return serializeSocket(namespace, string(s.Id()), s.Handshake(), s.Rooms(), s.Data())
}

func serializeSocket(namespace, id string, handshake *socket.Handshake, rooms *types.Set[socket.Room], data any) SerializedSocket {
	roomNames := make([]string, 0, rooms.Len())
	for _, room := range rooms.Keys() {
		roomNames = append(roomNames, string(room))
	}
	sort.Strings(roomNames)
	return SerializedSocket{
		ID:        id,
		Namespace: namespace,
		Data:      data,
		Handshake: map[string]any{
			"address": handshake.Address,
			"headers": handshake.Headers,
			"query":   handshake.Query,
			"issued":  handshake.Issued,
			"secure":  handshake.Secure,
			"time":    handshake.Time,
			"url":     handshake.Url,
			"xdomain": handshake.Xdomain,
		},
		Rooms: roomNames,
	}
}

func (i *Instrumentation) onEngineConnection(args ...any) {
	i.pushEvent("rawConnection", "", 1)
	if len(args) == 0 {
		return
	}
	rawSocket, ok := args[0].(engine.Socket)
	if !ok {
		return
	}
	packetIn := func(packetArgs ...any) {
		i.pushPacketMetrics("packetsIn", "bytesIn", packetArgs...)
	}
	packetOut := func(packetArgs ...any) {
		i.pushPacketMetrics("packetsOut", "bytesOut", packetArgs...)
	}
	_ = rawSocket.On("packet", packetIn)
	_ = rawSocket.On("packetCreate", packetOut)
	_ = rawSocket.Once("close", func(closeArgs ...any) {
		rawSocket.RemoveListener("packet", packetIn)
		rawSocket.RemoveListener("packetCreate", packetOut)
		reason := ""
		if len(closeArgs) > 0 {
			reason, _ = closeArgs[0].(string)
		}
		i.pushEvent("rawDisconnection", reason, 1)
	})
}

func (i *Instrumentation) pushPacketMetrics(packetType, byteType string, args ...any) {
	if len(args) == 0 {
		return
	}
	value, ok := args[0].(*enginepacket.Packet)
	if !ok || value == nil || value.Data == nil {
		return
	}
	reader, ok := value.Data.(interface{ Len() int })
	if !ok || reader.Len() <= 0 {
		return
	}
	i.pushEvent(packetType, "", 1)
	i.pushEvent(byteType, "", int64(reader.Len()))
}

func (i *Instrumentation) pushEvent(eventType, subType string, count int64) {
	timestamp := time.Now().Truncate(time.Second).UnixMilli()
	key := eventType + "\x00" + subType
	i.mu.Lock()
	event, ok := i.events[key]
	if !ok || event.Timestamp != timestamp {
		i.events[key] = &NamespaceEvent{
			Timestamp: timestamp,
			Type:      eventType,
			SubType:   subType,
			Count:     count,
		}
	} else {
		event.Count += count
	}
	i.mu.Unlock()
}

func (i *Instrumentation) takeEvents() []NamespaceEvent {
	i.mu.Lock()
	events := make([]NamespaceEvent, 0, len(i.events))
	for _, event := range i.events {
		events = append(events, *event)
	}
	clear(i.events)
	i.mu.Unlock()
	sort.Slice(events, func(a, b int) bool {
		if events[a].Timestamp == events[b].Timestamp {
			return events[a].Type < events[b].Type
		}
		return events[a].Timestamp < events[b].Timestamp
	})
	return events
}

func (i *Instrumentation) statsLoop() {
	defer close(i.done)
	ticker := time.NewTicker(i.options.StatsInterval)
	defer ticker.Stop()
	i.emitStats()
	for {
		select {
		case <-ticker.C:
			i.emitStats()
		case <-i.stop:
			return
		}
	}
}

func (i *Instrumentation) emitStats() {
	namespaces := i.server.Namespaces()
	namespaceStats := make([]NamespaceDetails, 0, len(namespaces))
	pollingClients := 0
	for _, nsp := range namespaces {
		namespaceStats = append(namespaceStats, NamespaceDetails{
			Name:         nsp.Name(),
			SocketsCount: nsp.Sockets().Len(),
		})
		nsp.Sockets().Range(func(_ socket.SocketId, s *socket.Socket) bool {
			if s.Conn().Transport().Name() == "polling" {
				pollingClients++
			}
			if i.options.Mode == DevelopmentMode {
				_ = i.admin.Emit("socket_updated", map[string]any{
					"id":   string(s.Id()),
					"nsp":  nsp.Name(),
					"data": s.Data(),
				})
			}
			return true
		})
	}
	sort.Slice(namespaceStats, func(a, b int) bool {
		return namespaceStats[a].Name < namespaceStats[b].Name
	})
	hostname, _ := os.Hostname()
	serverID := i.options.ServerID
	if serverID == "" {
		serverID = hostname
	}
	i.mu.Lock()
	engineServer := i.engine
	i.mu.Unlock()
	var clientsCount uint64
	if engineServer != nil {
		clientsCount = engineServer.ClientsCount()
	}
	_ = i.admin.Emit("server_stats", ServerStats{
		ServerID:            serverID,
		Hostname:            hostname,
		PID:                 os.Getpid(),
		Uptime:              time.Since(i.startedAt).Seconds(),
		ClientsCount:        clientsCount,
		PollingClientsCount: pollingClients,
		AggregatedEvents:    i.takeEvents(),
		Namespaces:          namespaceStats,
	})
}

// Close stops periodic collection and detaches server-level listeners.
func (i *Instrumentation) Close() {
	i.closeOnce.Do(func() {
		close(i.stop)
		<-i.done
		i.server.Sockets().EventEmitter().RemoveListener("new_namespace", i.newNamespaceListener)
		i.server.RemoveListener("engine_initialized", i.engineInitializedListener)
		i.mu.Lock()
		engineServer := i.engine
		i.mu.Unlock()
		if engineServer != nil {
			engineServer.RemoveListener("connection", i.engineConnectionListener)
		}
	})
}
