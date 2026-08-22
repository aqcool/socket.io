package instrumentation

import (
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	client "github.com/aqcool/socket.io/clients/socket/v4"
	server "github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

func TestNormalizeOptionsAndFeatures(t *testing.T) {
	t.Parallel()

	opts := normalizeOptions(&Options{NamespaceName: "ops", ReadOnly: true})
	if opts.NamespaceName != "/ops" {
		t.Fatalf("namespace = %q, want /ops", opts.NamespaceName)
	}
	io := server.NewServer(nil, nil)
	instrumentation, err := Instrument(io, &opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instrumentation.Close)

	features := instrumentation.supportedFeatures()
	if len(features) != 2 ||
		features[0] != FeatureAggregatedEvents ||
		features[1] != FeatureAllEvents {
		t.Fatalf("unexpected readonly features: %v", features)
	}
}

func TestSecureEqual(t *testing.T) {
	t.Parallel()

	if !secureEqual("admin", "admin") {
		t.Fatal("equal credentials were rejected")
	}
	if secureEqual("admin", "wrong") || secureEqual("short", "longer") {
		t.Fatal("invalid credentials were accepted")
	}
}

func TestAdminUIProtocolAndManagementCommands(t *testing.T) {
	io := server.NewServer(nil, nil)
	var customAuthCalls atomic.Int64
	instrumentation, err := Instrument(io, &Options{
		ServerID:      "test-node",
		StatsInterval: 200 * time.Millisecond,
		BasicAuth:     &BasicAuth{Username: "admin", Password: "secret"},
		Auth: func(_ *server.Socket, next func(*server.ExtendedError)) {
			customAuthCalls.Add(1)
			next(nil)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instrumentation.Close)

	var applicationSocket atomic.Pointer[server.Socket]
	applicationConnected := make(chan struct{}, 1)
	applicationEventReceived := make(chan struct{}, 1)
	applicationNamespace := io.Of("/app", nil)
	_ = applicationNamespace.On("connection", func(args ...any) {
		s := args[0].(*server.Socket)
		applicationSocket.Store(s)
		_ = s.On("from-client", func(eventArgs ...any) {
			signal(applicationEventReceived)
			if len(eventArgs) > 0 {
				if ack, ok := eventArgs[len(eventArgs)-1].(server.Ack); ok {
					ack([]any{"ok"}, nil)
				}
			}
		})
		applicationConnected <- struct{}{}
	})

	httpServer := httptest.NewServer(io.ServeHandler(nil))
	t.Cleanup(func() {
		io.Close(nil)
		httpServer.CloseClientConnections()
		httpServer.Close()
	})

	appClient := connectClient(t, httpServer.URL+"/app", nil)
	t.Cleanup(func() { appClient.Close() })
	waitSignal(t, applicationConnected, "application connection")
	time.Sleep(50 * time.Millisecond)
	appClient.EmitWithAck("from-client", "before-admin")(func([]any, error) {})
	waitSignal(t, applicationEventReceived, "application event before admin connection")

	configReceived := make(chan struct{}, 1)
	allSocketsReceived := make(chan struct{}, 1)
	statsReceived := make(chan struct{}, 1)
	eventReceived := make(chan struct{}, 1)
	eventSent := make(chan struct{}, 1)
	disconnected := make(chan struct{}, 1)
	adminClient := connectClientWithSetup(t, httpServer.URL+"/admin", map[string]any{
		"username": "admin",
		"password": "secret",
	}, func(s *client.Socket) {
		_ = s.On("config", func(...any) { signal(configReceived) })
		_ = s.On("all_sockets", func(...any) { signal(allSocketsReceived) })
		_ = s.On("server_stats", func(...any) { signal(statsReceived) })
		_ = s.On("event_received", func(...any) { signal(eventReceived) })
		_ = s.On("event_sent", func(...any) { signal(eventSent) })
		_ = s.On("socket_disconnected", func(...any) { signal(disconnected) })
	})
	t.Cleanup(func() { adminClient.Close() })

	waitSignal(t, configReceived, "config event")
	waitSignal(t, allSocketsReceived, "all_sockets event")
	waitSignal(t, statsReceived, "server_stats event")
	if customAuthCalls.Load() != 1 {
		t.Fatalf("custom authentication calls = %d, want 1", customAuthCalls.Load())
	}

	appSocket := applicationSocket.Load()
	if appSocket == nil {
		t.Fatal("application socket was not captured")
	}
	if !appClient.Connected() {
		t.Fatal("application client disconnected before event test")
	}
	if !appSocket.Connected() {
		t.Fatal("application server socket disconnected before event test")
	}
	appClient.EmitWithAck("from-client", "payload")(func([]any, error) {})
	waitSignal(t, applicationEventReceived, "application event")
	waitSignal(t, eventReceived, "event_received event")
	_ = appSocket.Emit("to-client", "payload")
	waitSignal(t, eventSent, "event_sent event")

	_ = adminClient.Emit("join", "/app", "managed-room", string(appSocket.Id()))
	waitFor(t, "remote join", func() bool {
		return appSocket.Rooms().Has(server.Room("managed-room"))
	})
	_ = adminClient.Emit("leave", "/app", "managed-room", string(appSocket.Id()))
	waitFor(t, "remote leave", func() bool {
		return !appSocket.Rooms().Has(server.Room("managed-room"))
	})

	_ = adminClient.Emit("_disconnect", "/app", false, string(appSocket.Id()))
	waitSignal(t, disconnected, "socket_disconnected event")
}

func TestReadOnlyModeDoesNotAdvertiseManagement(t *testing.T) {
	io := server.NewServer(nil, nil)
	instrumentation, err := Instrument(io, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instrumentation.Close)

	for _, feature := range instrumentation.supportedFeatures() {
		switch feature {
		case FeatureEmit, FeatureJoin, FeatureLeave, FeatureDisconnect,
			FeatureMultiJoin, FeatureMultiLeave, FeatureMultiDisconnect:
			t.Fatalf("readonly mode advertised management feature %q", feature)
		}
	}
}

func TestBasicAuthRejectsInvalidCredentials(t *testing.T) {
	io := server.NewServer(nil, nil)
	instrumentation, err := Instrument(io, &Options{
		BasicAuth: &BasicAuth{Username: "admin", Password: "secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instrumentation.Close)

	httpServer := httptest.NewServer(io.ServeHandler(nil))
	t.Cleanup(func() {
		io.Close(nil)
		httpServer.CloseClientConnections()
		httpServer.Close()
	})

	options := client.DefaultOptions()
	options.SetTransports(types.NewSet(client.WebSocket))
	options.SetAuth(map[string]any{"username": "admin", "password": "wrong"})
	options.SetAutoConnect(false)
	connectError := make(chan struct{}, 1)
	connected := make(chan struct{}, 1)
	adminClient, err := client.Connect(httpServer.URL+"/admin", options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { adminClient.Close() })
	_ = adminClient.On("connect_error", func(...any) { signal(connectError) })
	_ = adminClient.On("connect", func(...any) { signal(connected) })
	adminClient.Connect()

	waitSignal(t, connectError, "connect_error")
	select {
	case <-connected:
		t.Fatal("invalid Basic Auth credentials connected")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestBasicAuthSessionCanReconnectWithoutCredentials(t *testing.T) {
	io := server.NewServer(nil, nil)
	instrumentation, err := Instrument(io, &Options{
		BasicAuth: &BasicAuth{Username: "admin", Password: "secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instrumentation.Close)

	httpServer := httptest.NewServer(io.ServeHandler(nil))
	t.Cleanup(func() {
		io.Close(nil)
		httpServer.CloseClientConnections()
		httpServer.Close()
	})

	sessions := make(chan string, 1)
	first := connectClientWithSetup(t, httpServer.URL+"/admin", map[string]any{
		"username": "admin",
		"password": "secret",
	}, func(s *client.Socket) {
		_ = s.On("session", func(args ...any) {
			if len(args) > 0 {
				if id, ok := args[0].(string); ok {
					sessions <- id
				}
			}
		})
	})

	var sessionID string
	select {
	case sessionID = <-sessions:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Admin UI session")
	}
	if len(sessionID) != 16 {
		t.Fatalf("session ID = %q, want 16 hexadecimal characters", sessionID)
	}
	first.Close()

	second := connectClient(t, httpServer.URL+"/admin", map[string]any{"sessionId": sessionID})
	second.Close()
}

func TestAggregatedEnginePacketMetrics(t *testing.T) {
	io := server.NewServer(nil, nil)
	instrumentation, err := Instrument(io, &Options{StatsInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instrumentation.Close)

	connected := make(chan *server.Socket, 1)
	received := make(chan struct{}, 1)
	io.Of("/metrics", func(args ...any) {
		s := args[0].(*server.Socket)
		_ = s.On("client-event", func(...any) { signal(received) })
		connected <- s
	})
	httpServer := httptest.NewServer(io.ServeHandler(nil))
	t.Cleanup(func() {
		io.Close(nil)
		httpServer.CloseClientConnections()
		httpServer.Close()
	})

	application := connectClient(t, httpServer.URL+"/metrics", nil)
	t.Cleanup(func() { application.Close() })
	var applicationSocket *server.Socket
	select {
	case applicationSocket = <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for metrics socket")
	}
	_ = application.Emit("client-event", "incoming-payload")
	waitSignal(t, received, "metrics client event")
	_ = applicationSocket.Emit("server-event", "outgoing-payload")

	time.Sleep(100 * time.Millisecond)
	counts := make(map[string]int64)
	for _, event := range instrumentation.takeEvents() {
		counts[event.Type] += event.Count
	}
	for _, name := range []string{"rawConnection", "packetsIn", "bytesIn", "packetsOut", "bytesOut"} {
		if counts[name] <= 0 {
			t.Fatalf("aggregated metric %q missing: %v", name, counts)
		}
	}
}

func connectClient(t *testing.T, uri string, auth map[string]any) *client.Socket {
	t.Helper()
	return connectClientWithSetup(t, uri, auth, nil)
}

func connectClientWithSetup(
	t *testing.T,
	uri string,
	auth map[string]any,
	setup func(*client.Socket),
) *client.Socket {
	t.Helper()
	options := client.DefaultOptions()
	options.SetTransports(types.NewSet(client.WebSocket))
	if auth != nil {
		options.SetAuth(auth)
	}
	connected := make(chan struct{}, 1)
	s, err := client.Connect(uri, options)
	if err != nil {
		t.Fatal(err)
	}
	if setup != nil {
		setup(s)
	}
	_ = s.On("connect", func(...any) { signal(connected) })
	waitSignal(t, connected, "client connection")
	return s
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func waitSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitFor(t *testing.T, name string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", name)
}
