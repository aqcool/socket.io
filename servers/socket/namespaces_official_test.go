package socket

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

func TestOfficialNamespaceAliasesAndNormalization(t *testing.T) {
	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })

	if server.Sockets() == nil || server.Sockets().Name() != "/" {
		t.Fatalf("default namespace = %#v, want /", server.Sockets())
	}
	if server.Of("", nil) != server.Of("/", nil) {
		t.Fatal("empty and slash namespace names did not resolve to the same namespace")
	}
	if server.Of("abc", nil) != server.Of("/abc", nil) {
		t.Fatal("names with and without a leading slash did not resolve to the same namespace")
	}
}

func TestOfficialServerNamespaceAliases(t *testing.T) {
	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })

	middleware := func(_ *Socket, next func(*ExtendedError)) { next(nil) }
	if got := server.Use(middleware); got != server {
		t.Fatal("Use did not return the Server for chaining")
	}
	for name, operator := range map[string]*BroadcastOperator{
		"to":       server.To("room"),
		"in":       server.In("room"),
		"except":   server.Except("room"),
		"compress": server.Compress(false),
		"volatile": server.Volatile(),
		"local":    server.Local(),
	} {
		if operator == nil {
			t.Fatalf("%s alias returned nil", name)
		}
	}
	if server.Emit("event") != server || server.Send("message") != server || server.Write("message") != server {
		t.Fatal("Emit, Send, or Write did not return the Server for chaining")
	}

	called := false
	server.AllSockets()(func(ids *types.Set[SocketId], err error) {
		called = true
		if err != nil || ids == nil || ids.Len() != 0 {
			t.Fatalf("AllSockets on an empty Server = %v, %v; want empty set", ids, err)
		}
	})
	if !called {
		t.Fatal("AllSockets alias did not invoke its callback")
	}
}

func TestOfficialNamespaceConnectionsAndListenerRegistration(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	var rootConnection atomic.Int32
	var rootConnect atomic.Int32
	var chatConnection atomic.Int32
	var newsListeners atomic.Int32
	var newsSecondParams atomic.Int32

	_ = server.On("connection", func(args ...any) {
		if _, ok := args[0].(*Socket); !ok {
			t.Errorf("root connection argument = %T, want *Socket", args[0])
		}
		rootConnection.Add(1)
	})
	_ = server.On("connect", func(args ...any) {
		if _, ok := args[0].(*Socket); !ok {
			t.Errorf("root connect argument = %T, want *Socket", args[0])
		}
		rootConnect.Add(1)
	})
	_ = server.Of("abc", nil).On("connection", func(args ...any) {
		if args[0].(*Socket).Nsp().Name() != "/abc" {
			t.Errorf("chat Socket namespace = %q, want /abc", args[0].(*Socket).Nsp().Name())
		}
		chatConnection.Add(1)
	})
	news := server.Of("/news", nil)
	for range 2 {
		_ = news.On("connection", func(args ...any) {
			if _, ok := args[0].(*Socket); !ok {
				t.Errorf("news connection argument = %T, want *Socket", args[0])
			}
			newsListeners.Add(1)
		})
		server.Of("/news", func(args ...any) {
			if _, ok := args[0].(*Socket); !ok {
				t.Errorf("Of second argument = %T, want *Socket", args[0])
			}
			newsSecondParams.Add(1)
		})
	}

	connect := func(namespace string) {
		t.Helper()
		sid := socketIOPollingHandshake(t, httpServer.URL)
		packet := "40"
		if namespace != "" && namespace != "/" {
			packet += namespace + ","
		}
		socketIOPollingPush(t, httpServer.URL, sid, packet)
		if payload := socketIOPollingPoll(t, httpServer.URL, sid); !strings.HasPrefix(payload, "40") {
			t.Fatalf("%s CONNECT reply = %q", namespace, payload)
		}
	}
	connect("")
	connect("/abc")
	connect("/news")

	if rootConnection.Load() != 1 || rootConnect.Load() != 1 {
		t.Fatalf("root connection/connect counts = %d/%d, want 1/1", rootConnection.Load(), rootConnect.Load())
	}
	if chatConnection.Load() != 1 {
		t.Fatalf("normalized /abc connection count = %d, want 1", chatConnection.Load())
	}
	if newsListeners.Load() != 2 || newsSecondParams.Load() != 2 {
		t.Fatalf("news listener/second-param counts = %d/%d, want 2/2", newsListeners.Load(), newsSecondParams.Load())
	}
}

func TestOfficialNewNamespaceEvent(t *testing.T) {
	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })

	created := make(chan Namespace, 2)
	_ = server.On("new_namespace", func(args ...any) {
		created <- args[0].(Namespace)
	})

	static := server.Of("/nsp", nil)
	select {
	case namespace := <-created:
		if namespace != static || namespace.Name() != "/nsp" {
			t.Fatalf("new_namespace value = %q/%p, want /nsp/%p", namespace.Name(), namespace, static)
		}
	case <-time.After(time.Second):
		t.Fatal("static new_namespace event was not emitted")
	}

	server.Of("/nsp", nil)
	select {
	case namespace := <-created:
		t.Fatalf("existing namespace emitted new_namespace again: %s", namespace.Name())
	case <-time.After(25 * time.Millisecond):
	}
}

func TestOfficialDynamicNamespaceConcurrentCreationEmitsOnce(t *testing.T) {
	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })
	parent := server.Of(regexp.MustCompile(`^/dynamic-\d+$`), nil).(ParentNamespace)

	var created atomic.Int32
	_ = server.On("new_namespace", func(args ...any) {
		if namespace := args[0].(Namespace); namespace.Name() == "/dynamic-101" {
			created.Add(1)
		}
	})

	const callers = 32
	results := make(chan Namespace, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			server._checkNamespace("/dynamic-101", nil, func(namespace Namespace) {
				results <- namespace
			})
		}()
	}
	wait.Wait()

	var expected Namespace
	for range callers {
		namespace := <-results
		if namespace == nil {
			t.Fatal("dynamic namespace was unexpectedly rejected")
		}
		if expected == nil {
			expected = namespace
		} else if namespace != expected {
			t.Fatal("concurrent match created more than one child namespace")
		}
	}
	if got := created.Load(); got != 1 {
		t.Fatalf("new_namespace event count = %d, want 1", got)
	}
	if got := parent.Children().Len(); got != 1 {
		t.Fatalf("parent child count = %d, want 1", got)
	}
}

func TestOfficialRegexDynamicNamespaceConnectionAndRoomBroadcast(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	parent := server.Of(regexp.MustCompile(`^/dynamic-\d+$`), nil).(ParentNamespace)
	connected := make(chan *Socket, 1)
	var middlewareCalls atomic.Int32
	parent.Use(func(_ *Socket, next func(*ExtendedError)) {
		middlewareCalls.Add(1)
		next(nil)
	})
	_ = parent.On("connection", func(args ...any) {
		connected <- args[0].(*Socket)
	})

	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40/dynamic-101,")
	connectPayload := socketIOPollingPoll(t, httpServer.URL, sid)
	if !strings.HasPrefix(connectPayload, `40/dynamic-101,{"sid":"`) {
		t.Fatalf("dynamic namespace CONNECT reply = %q", connectPayload)
	}

	var socket *Socket
	select {
	case socket = <-connected:
	case <-time.After(time.Second):
		t.Fatal("dynamic namespace connection event was not emitted")
	}
	if socket.Nsp().Name() != "/dynamic-101" {
		t.Fatalf("dynamic Socket namespace = %q, want /dynamic-101", socket.Nsp().Name())
	}
	if middlewareCalls.Load() != 1 {
		t.Fatalf("parent middleware calls = %d, want 1", middlewareCalls.Load())
	}
	if !parent.Children().Has(socket.Nsp()) {
		t.Fatal("connected dynamic namespace was not attached to its regex parent")
	}

	socket.Join("some-room")
	if err := parent.To("some-room").Emit("hello", 4, "3", map[string]string{"2": "1"}); err != nil {
		t.Fatalf("parent room broadcast: %v", err)
	}
	if payload := socketIOPollingPoll(t, httpServer.URL, sid); payload != `42/dynamic-101,["hello",4,"3",{"2":"1"}]` {
		t.Fatalf("dynamic parent room payload = %q", payload)
	}
}

func TestOfficialDisconnectingEventRoomOrder(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	connected := make(chan *Socket, 1)
	_ = server.On("connection", func(args ...any) {
		connected <- args[0].(*Socket)
	})

	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40")
	_ = socketIOPollingPoll(t, httpServer.URL, sid)

	var socket *Socket
	select {
	case socket = <-connected:
	case <-time.After(time.Second):
		t.Fatal("Socket.IO connection event was not emitted")
	}
	socket.Join("a")

	order := make(chan string, 2)
	_ = socket.On("disconnecting", func(...any) {
		if !socket.Rooms().Has(Room(socket.Id())) || !socket.Rooms().Has("a") {
			t.Errorf("disconnecting rooms = %v, want socket ID and a", socket.Rooms().Keys())
		}
		order <- "disconnecting"
	})
	_ = socket.On("disconnect", func(...any) {
		if socket.Rooms().Len() != 0 {
			t.Errorf("disconnect rooms = %v, want empty", socket.Rooms().Keys())
		}
		order <- "disconnect"
	})

	socket.Disconnect(false)
	if first, second := <-order, <-order; first != "disconnecting" || second != "disconnect" {
		t.Fatalf("disconnect event order = %q, %q", first, second)
	}
}

func TestOfficialTransportDisconnectClosesAllNamespaces(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	rootConnected := make(chan *Socket, 1)
	chatConnected := make(chan *Socket, 1)
	rootDisconnected := make(chan string, 1)
	chatDisconnected := make(chan string, 1)

	_ = server.On("connection", func(args ...any) {
		socket := args[0].(*Socket)
		_ = socket.On("disconnect", func(reasons ...any) {
			rootDisconnected <- reasons[0].(string)
		})
		rootConnected <- socket
	})
	server.Of("/chat", func(args ...any) {
		socket := args[0].(*Socket)
		_ = socket.On("disconnect", func(reasons ...any) {
			chatDisconnected <- reasons[0].(string)
		})
		chatConnected <- socket
	})

	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40")
	_ = socketIOPollingPoll(t, httpServer.URL, sid)
	socketIOPollingPush(t, httpServer.URL, sid, "40/chat,")
	_ = socketIOPollingPoll(t, httpServer.URL, sid)

	root := <-rootConnected
	<-chatConnected
	root.Disconnect(true)

	for name, disconnected := range map[string]<-chan string{
		"root": rootDisconnected,
		"chat": chatDisconnected,
	} {
		select {
		case reason := <-disconnected:
			if reason != "server namespace disconnect" {
				t.Fatalf("%s disconnect reason = %q", name, reason)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s namespace was not disconnected", name)
		}
	}

	disconnectPayload := socketIOPollingPoll(t, httpServer.URL, sid)
	packets := strings.Split(disconnectPayload, "\x1e")
	seen := map[string]bool{}
	for _, payload := range packets {
		seen[payload] = true
	}
	if !seen["41"] || !seen["41/chat,"] || len(packets) != 2 {
		t.Fatalf("namespace DISCONNECT payload = %q, want root and /chat packets", disconnectPayload)
	}
	if closePayload := socketIOPollingPoll(t, httpServer.URL, sid); closePayload != "6\x1e1" {
		t.Fatalf("Engine.IO close payload = %q, want %q", closePayload, "6\x1e1")
	}
	deadline := time.Now().Add(time.Second)
	for server.Engine().ClientsCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if clients := server.Engine().ClientsCount(); clients != 0 {
		t.Fatalf("Engine.IO client count after orderly close = %d, want 0", clients)
	}
}

func TestOfficialInvalidNamespaceConnectError(t *testing.T) {
	_, httpServer := newOfficialCloseTestServer(t)
	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40/doesnotexist,")

	want := `44/doesnotexist,{"message":"Invalid namespace"}`
	if payload := socketIOPollingPoll(t, httpServer.URL, sid); payload != want {
		t.Fatalf("CONNECT_ERROR payload = %q, want %q", payload, want)
	}
}

func TestOfficialNamespaceBroadcastOperatorIsImmutable(t *testing.T) {
	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })

	operator := server.Local().To("room1", "room2").Except("room3")
	compressed := operator.Compress(true)
	volatile := operator.Volatile()
	withRoom := operator.To("room4")
	withException := operator.Except("room5")

	if !operator.flags.Local || operator.flags.Volatile || operator.flags.Compress != nil {
		t.Fatalf("original flags mutated: %#v", operator.flags)
	}
	if !operator.rooms.Has("room1") || !operator.rooms.Has("room2") || operator.rooms.Has("room4") {
		t.Fatalf("original rooms mutated: %v", operator.rooms.Keys())
	}
	if !operator.exceptRooms.Has("room3") || operator.exceptRooms.Has("room5") {
		t.Fatalf("original except rooms mutated: %v", operator.exceptRooms.Keys())
	}
	if compressed.flags.Compress == nil || !*compressed.flags.Compress || operator.flags.Compress != nil {
		t.Fatal("Compress did not return an immutable derived operator")
	}
	if !volatile.flags.Volatile || operator.flags.Volatile {
		t.Fatal("Volatile did not return an immutable derived operator")
	}
	if !withRoom.rooms.Has("room4") || operator.rooms.Has("room4") {
		t.Fatal("To did not return an immutable derived operator")
	}
	if !withException.exceptRooms.Has("room5") || operator.exceptRooms.Has("room5") {
		t.Fatal("Except did not return an immutable derived operator")
	}
}

func TestOfficialServerRejectsReservedEvent(t *testing.T) {
	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })

	defer func() {
		recovered := recover()
		if recovered == nil || !strings.Contains(recovered.(error).Error(), `"connect" is a reserved event name`) {
			t.Fatalf("reserved event panic = %v", recovered)
		}
	}()
	server.Emit("connect")
}

func TestOfficialNamespaceCompressionFlags(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	connected := make(chan *Socket, 1)
	_ = server.On("connection", func(args ...any) { connected <- args[0].(*Socket) })

	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40")
	_ = socketIOPollingPoll(t, httpServer.URL, sid)
	socket := <-connected

	for _, test := range []struct {
		name string
		emit func()
		want bool
	}{
		{name: "enabled by default", emit: func() { server.Emit("default-compression") }, want: true},
		{name: "explicitly disabled", emit: func() { _ = server.Compress(false).Emit("disabled-compression") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := make(chan bool, 1)
			_ = socket.Conn().Once("packetCreate", func(args ...any) {
				packet := args[0].(*packet.Packet)
				observed <- packet.Options != nil && packet.Options.Compress != nil && *packet.Options.Compress
			})
			test.emit()
			select {
			case got := <-observed:
				if got != test.want {
					t.Fatalf("compress flag = %t, want %t", got, test.want)
				}
			case <-time.After(time.Second):
				t.Fatal("packetCreate event was not emitted")
			}
		})
	}
}

func TestOfficialNamespaceVolatilePollingBehavior(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	connected := make(chan *Socket, 1)
	_ = server.On("connection", func(args ...any) { connected <- args[0].(*Socket) })

	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40")
	_ = socketIOPollingPoll(t, httpServer.URL, sid)
	socket := <-connected

	server.Emit("regular", "one")
	_ = server.Volatile().Emit("volatile", "dropped")
	if payload := socketIOPollingPoll(t, httpServer.URL, sid); payload != `42["regular","one"]` {
		t.Fatalf("payload with blocked transport = %q", payload)
	}

	type pollResult struct {
		payload string
		err     error
		status  int
	}
	result := make(chan pollResult, 1)
	go func() {
		client := &http.Client{Timeout: 2 * time.Second}
		response, err := client.Get(httpServer.URL + "/socket.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(sid))
		if err != nil {
			result <- pollResult{err: err}
			return
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		result <- pollResult{payload: string(body), err: readErr, status: response.StatusCode}
	}()
	deadline := time.Now().Add(time.Second)
	for !socket.Conn().Transport().Writable() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !socket.Conn().Transport().Writable() {
		t.Fatal("pending Polling request did not become writable")
	}
	_ = server.Volatile().Emit("volatile", "delivered")

	select {
	case response := <-result:
		if response.err != nil || response.status != http.StatusOK || response.payload != `42["volatile","delivered"]` {
			t.Fatalf("writable volatile response = status %d, payload %q, err %v", response.status, response.payload, response.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("volatile Polling response timed out")
	}
}

func TestOfficialDynamicNamespaceCleanupAfterMiddlewareRejection(t *testing.T) {
	for _, cleanupEnabled := range []bool{false, true} {
		name := "cleanup disabled"
		if cleanupEnabled {
			name = "cleanup enabled"
		}
		t.Run(name, func(t *testing.T) {
			options := DefaultServerOptions()
			options.SetCleanupEmptyChildNamespaces(cleanupEnabled)
			server := NewServer(nil, options)
			parent := server.Of(regexp.MustCompile(`^/dynamic-\d+$`), nil).(ParentNamespace)
			parent.Use(func(_ *Socket, next func(*ExtendedError)) {
				next(NewExtendedError("denied", nil))
			})
			httpServer := httptest.NewServer(server.ServeHandler(nil))
			t.Cleanup(func() {
				server.Close(nil)
				httpServer.Close()
			})

			sid := socketIOPollingHandshake(t, httpServer.URL)
			socketIOPollingPush(t, httpServer.URL, sid, "40/dynamic-101,")
			payload := socketIOPollingPoll(t, httpServer.URL, sid)
			if !strings.HasPrefix(payload, "44/dynamic-101,") || !strings.Contains(payload, `"message":"denied"`) {
				t.Fatalf("middleware CONNECT_ERROR payload = %q", payload)
			}

			_, exists := server._nsps.Load("/dynamic-101")
			if exists != !cleanupEnabled {
				t.Fatalf("dynamic namespace exists = %t with cleanup enabled = %t", exists, cleanupEnabled)
			}
			wantChildren := 1
			if cleanupEnabled {
				wantChildren = 0
			}
			if got := parent.Children().Len(); got != wantChildren {
				t.Fatalf("parent child count = %d, want %d", got, wantChildren)
			}
		})
	}
}

func TestOfficialDynamicNamespaceIsKeptWhenAnotherSocketRemains(t *testing.T) {
	options := DefaultServerOptions()
	options.SetCleanupEmptyChildNamespaces(true)
	server := NewServer(nil, options)
	parent := server.Of(regexp.MustCompile(`^/dynamic-\d+$`), nil).(ParentNamespace)
	var middlewareCalls atomic.Int32
	parent.Use(func(_ *Socket, next func(*ExtendedError)) {
		if middlewareCalls.Add(1) == 1 {
			next(nil)
			return
		}
		next(NewExtendedError("denied", nil))
	})
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})

	firstSID := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, firstSID, "40/dynamic-101,")
	_ = socketIOPollingPoll(t, httpServer.URL, firstSID)
	secondSID := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, secondSID, "40/dynamic-101,")
	if payload := socketIOPollingPoll(t, httpServer.URL, secondSID); !strings.HasPrefix(payload, "44/dynamic-101,") || !strings.Contains(payload, `"message":"denied"`) {
		t.Fatalf("second CONNECT_ERROR payload = %q", payload)
	}

	namespace, exists := server._nsps.Load("/dynamic-101")
	if !exists {
		t.Fatal("dynamic namespace was removed while another socket remained")
	}
	if namespace.Sockets().Len() != 1 || parent.Children().Len() != 1 {
		t.Fatalf("namespace after rejection = sockets %d, children %d", namespace.Sockets().Len(), parent.Children().Len())
	}
}

func TestOfficialStaticNamespaceIsNotCleanedUp(t *testing.T) {
	options := DefaultServerOptions()
	options.SetCleanupEmptyChildNamespaces(true)
	server := NewServer(nil, options)
	namespace := server.Of("/chat", nil)
	connected := make(chan *Socket, 1)
	_ = namespace.On("connection", func(args ...any) { connected <- args[0].(*Socket) })
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})

	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40/chat,")
	_ = socketIOPollingPoll(t, httpServer.URL, sid)
	(<-connected).Disconnect(false)

	stored, exists := server._nsps.Load("/chat")
	if !exists {
		t.Fatal("static namespace was removed")
	}
	if stored != namespace || stored.Sockets().Len() != 0 {
		t.Fatalf("static namespace after disconnect = same %t, sockets %d", stored == namespace, stored.Sockets().Len())
	}
}

func TestOfficialManualDynamicChildAttachesToRegexParent(t *testing.T) {
	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })
	parent := server.Of(regexp.MustCompile(`^/dynamic-\d+$`), nil).(ParentNamespace)
	child := server.Of("/dynamic-101", nil)

	if !parent.Children().Has(child) {
		t.Fatal("manually created matching namespace was not attached to its regex parent")
	}
}

func TestOfficialConnectTimeoutClosesClientWithoutNamespace(t *testing.T) {
	options := DefaultServerOptions()
	options.SetConnectTimeout(10 * time.Millisecond)
	server := NewServer(nil, options)
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})

	sid := socketIOPollingHandshake(t, httpServer.URL)
	// Deliberately do not send a Socket.IO CONNECT packet.
	time.Sleep(25 * time.Millisecond)
	if payload := socketIOPollingPoll(t, httpServer.URL, sid); payload != "6\x1e1" {
		t.Fatalf("connect-timeout close payload = %q, want %q", payload, "6\x1e1")
	}
	deadline := time.Now().Add(time.Second)
	for server.Engine().ClientsCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if clients := server.Engine().ClientsCount(); clients != 0 {
		t.Fatalf("Engine.IO clients after connect timeout = %d, want 0", clients)
	}
}

func TestOfficialNamespaceAllSocketsFilters(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	chat := server.Of("/chat", nil)
	other := server.Of("/other", nil)
	chatConnected := make(chan *Socket, 2)
	otherConnected := make(chan *Socket, 1)
	_ = chat.On("connection", func(args ...any) { chatConnected <- args[0].(*Socket) })
	_ = other.On("connection", func(args ...any) { otherConnected <- args[0].(*Socket) })

	connect := func(namespace string) {
		sid := socketIOPollingHandshake(t, httpServer.URL)
		socketIOPollingPush(t, httpServer.URL, sid, "40"+namespace+",")
		_ = socketIOPollingPoll(t, httpServer.URL, sid)
	}
	connect("/chat")
	chatOne := <-chatConnected
	connect("/chat")
	chatTwo := <-chatConnected
	connect("/other")
	otherSocket := <-otherConnected

	chatOne.Join("foo")
	chatTwo.Join("bar")
	otherSocket.Join("foo")

	assertIDs := func(name string, operation func(func(*types.Set[SocketId], error)), want ...SocketId) {
		t.Helper()
		called := false
		operation(func(ids *types.Set[SocketId], err error) {
			called = true
			if err != nil {
				t.Fatalf("%s AllSockets error: %v", name, err)
			}
			if ids.Len() != len(want) {
				t.Fatalf("%s IDs = %v, want %v", name, ids.Keys(), want)
			}
			for _, id := range want {
				if !ids.Has(id) {
					t.Fatalf("%s IDs = %v, missing %s", name, ids.Keys(), id)
				}
			}
		})
		if !called {
			t.Fatalf("%s AllSockets callback was not called", name)
		}
	}

	assertIDs("chat namespace", chat.AllSockets(), chatOne.Id(), chatTwo.Id())
	assertIDs("chat foo room", chat.In("foo").AllSockets(), chatOne.Id())
	assertIDs("chat bar room", chat.In("bar").AllSockets(), chatTwo.Id())
	if otherSocket.Id() == chatOne.Id() || otherSocket.Id() == chatTwo.Id() {
		t.Fatal("separate Engine.IO clients unexpectedly reused Socket IDs")
	}
}

type namespacePendingPollResult struct {
	payload string
	status  int
	err     error
}

func startNamespacePendingPoll(t *testing.T, baseURL, sid string, socket *Socket) (context.CancelFunc, <-chan namespacePendingPollResult) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		baseURL+"/socket.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid),
		nil,
	)
	if err != nil {
		cancel()
		t.Fatalf("creating pending Polling request: %v", err)
	}
	result := make(chan namespacePendingPollResult, 1)
	go func() {
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr != nil {
			result <- namespacePendingPollResult{err: requestErr}
			return
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		result <- namespacePendingPollResult{payload: string(body), status: response.StatusCode, err: readErr}
	}()

	deadline := time.Now().Add(time.Second)
	for !socket.Conn().Transport().Writable() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !socket.Conn().Transport().Writable() {
		cancel()
		t.Fatal("pending Polling request did not become writable")
	}
	return cancel, result
}

func assertExcludedPollRemainsPending(t *testing.T, socket *Socket, cancel context.CancelFunc, result <-chan namespacePendingPollResult) {
	t.Helper()
	select {
	case response := <-result:
		t.Fatalf("excluded Polling request unexpectedly completed: status %d, payload %q, err %v", response.status, response.payload, response.err)
	case <-time.After(25 * time.Millisecond):
	}
	if !socket.Conn().Transport().Writable() {
		t.Fatal("excluded Socket transport became non-writable")
	}
	cancel()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("canceled excluded Polling request did not return")
	}
}

func TestOfficialNamespaceExclusionBroadcasts(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		exclude   func(*Server, Namespace, *Socket) *BroadcastOperator
	}{
		{
			name:      "socket ID on default namespace",
			namespace: "/",
			exclude: func(server *Server, _ Namespace, socket *Socket) *BroadcastOperator {
				return server.Except(Room(socket.Id()))
			},
		},
		{
			name:      "socket ID on custom namespace",
			namespace: "/nsp",
			exclude: func(_ *Server, namespace Namespace, socket *Socket) *BroadcastOperator {
				return namespace.Except(Room(socket.Id()))
			},
		},
		{
			name:      "room on custom namespace",
			namespace: "/nsp",
			exclude: func(_ *Server, namespace Namespace, socket *Socket) *BroadcastOperator {
				socket.Join("room1")
				return namespace.Except("room1")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, httpServer := newOfficialCloseTestServer(t)
			namespace := server.Of(test.namespace, nil)
			connected := make(chan *Socket, 2)
			_ = namespace.On("connection", func(args ...any) { connected <- args[0].(*Socket) })

			connect := func() string {
				sid := socketIOPollingHandshake(t, httpServer.URL)
				packet := "40"
				if test.namespace != "/" {
					packet += test.namespace + ","
				}
				socketIOPollingPush(t, httpServer.URL, sid, packet)
				_ = socketIOPollingPoll(t, httpServer.URL, sid)
				return sid
			}
			firstSID := connect()
			first := <-connected
			secondSID := connect()
			second := <-connected

			cancel, excludedResult := startNamespacePendingPoll(t, httpServer.URL, secondSID, second)
			if err := test.exclude(server, namespace, second).Emit("a"); err != nil {
				cancel()
				t.Fatalf("broadcasting with exclusion: %v", err)
			}
			wantPayload := `42["a"]`
			if test.namespace != "/" {
				wantPayload = "42" + test.namespace + `,["a"]`
			}
			if payload := socketIOPollingPoll(t, httpServer.URL, firstSID); payload != wantPayload {
				cancel()
				t.Fatalf("included Socket payload = %q", payload)
			}
			assertExcludedPollRemainsPending(t, second, cancel, excludedResult)
			if first.Id() == second.Id() {
				t.Fatal("distinct clients reused a Socket ID")
			}
		})
	}
}
