package adapter

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	enginepacket "github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/parsers/socket/v4/parser"
	"github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/gorilla/websocket"
)

type official258ServerFixture struct {
	server      *socket.Server
	httpServer  *httptest.Server
	sockets     []*socket.Socket
	connections []*websocket.Conn
}

func newOfficial258ServerFixture(t *testing.T, clientCount int, builder socket.AdapterConstructor) *official258ServerFixture {
	t.Helper()
	options := socket.DefaultServerOptions()
	if builder != nil {
		options.SetAdapter(builder)
	}
	server := socket.NewServer(nil, options)
	connected := make(chan *socket.Socket, clientCount)
	_ = server.On("connection", func(args ...any) {
		connected <- args[0].(*socket.Socket)
	})
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	fixture := &official258ServerFixture{server: server, httpServer: httpServer}
	t.Cleanup(func() {
		for _, connection := range fixture.connections {
			_ = connection.Close()
		}
		server.Close(nil)
		httpServer.Close()
	})

	for range clientCount {
		connection := dialOfficial258Namespace(t, httpServer.URL, "/")
		fixture.connections = append(fixture.connections, connection)
		select {
		case connectedSocket := <-connected:
			fixture.sockets = append(fixture.sockets, connectedSocket)
		case <-time.After(2 * time.Second):
			t.Fatal("Socket.IO connection event was not emitted")
		}
	}
	return fixture
}

func dialOfficial258Namespace(t *testing.T, baseURL, namespace string) *websocket.Conn {
	t.Helper()
	websocketURL := "ws" + strings.TrimPrefix(baseURL, "http") +
		"/socket.io/?EIO=4&transport=websocket"
	connection, _, err := websocket.DefaultDialer.Dial(websocketURL, nil)
	if err != nil {
		t.Fatalf("WebSocket handshake: %v", err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("setting WebSocket read deadline: %v", err)
	}
	if _, payload, readErr := connection.ReadMessage(); readErr != nil || !strings.HasPrefix(string(payload), "0{") {
		_ = connection.Close()
		t.Fatalf("Engine.IO OPEN packet = %q/%v", payload, readErr)
	}
	connectPacket := "40"
	wantPrefix := `40{"sid":"`
	if namespace != "/" {
		connectPacket = "40" + namespace + ","
		wantPrefix = "40" + namespace + `,{"sid":"`
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte(connectPacket)); err != nil {
		_ = connection.Close()
		t.Fatalf("writing Socket.IO CONNECT packet: %v", err)
	}
	if _, payload, readErr := connection.ReadMessage(); readErr != nil || !strings.HasPrefix(string(payload), wantPrefix) {
		_ = connection.Close()
		t.Fatalf("Socket.IO CONNECT packet = %q/%v, want prefix %q", payload, readErr, wantPrefix)
	}
	return connection
}

func official258Outgoing(socketInstance *socket.Socket) <-chan []any {
	events := make(chan []any, 8)
	socketInstance.OnAnyOutgoing(func(args ...any) {
		events <- append([]any(nil), args...)
	})
	return events
}

func assertOfficial258TestEvent(t *testing.T, events <-chan []any) []any {
	t.Helper()
	select {
	case event := <-events:
		if len(event) == 0 || event[0] != "test" {
			t.Fatalf("outgoing event = %#v, want %q", event, "test")
		}
		return event
	case <-time.After(time.Second):
		t.Fatal("outgoing event \"test\" was not emitted")
		return nil
	}
}

func assertOfficial258NoEvent(t *testing.T, events <-chan []any) {
	t.Helper()
	select {
	case event := <-events:
		t.Fatalf("unexpected outgoing event: %#v", event)
	default:
	}
}

func newOfficial258SessionAdapter(t *testing.T) SessionAwareAdapter {
	t.Helper()
	options := socket.DefaultServerOptions()
	recovery := socket.DefaultConnectionStateRecovery()
	recovery.SetMaxDisconnectionDuration(5_000)
	options.SetConnectionStateRecovery(recovery)
	options.SetAdapter(&SessionAwareAdapterBuilder{})
	server := socket.NewServer(nil, options)
	t.Cleanup(func() { server.Close(nil) })
	return server.Sockets().Adapter().(SessionAwareAdapter)
}

// TestOfficial258InMemoryAdapter maps every runtime test in the official
// packages/socket.io-adapter/test/index.ts file at socket.io-adapter@2.5.8.
func TestOfficial258InMemoryAdapter(t *testing.T) {
	t.Run("should add/remove sockets", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1", "r2"))
		adapter.AddAll("s2", types.NewSet[socket.Room]("r2", "r3"))
		hasRoom := func(room socket.Room) bool {
			_, ok := adapter.Rooms().Load(room)
			return ok
		}
		hasSID := func(id socket.SocketId) bool {
			_, ok := adapter.Sids().Load(id)
			return ok
		}
		if !hasRoom("r1") || !hasRoom("r2") || !hasRoom("r3") || hasRoom("r4") {
			t.Fatalf("rooms after AddAll = %v", adapter.Rooms().Keys())
		}
		if !hasSID("s1") || !hasSID("s2") || hasSID("s3") {
			t.Fatalf("sids after AddAll = %v", adapter.Sids().Keys())
		}
		adapter.Del("s1", "r1")
		adapter.DelAll("s2")
		if hasRoom("r1") || !hasRoom("r2") || hasRoom("r3") || hasSID("s2") {
			t.Fatalf("rooms/sids after removal = %v/%v", adapter.Rooms().Keys(), adapter.Sids().Keys())
		}
	})

	t.Run("should return a list of sockets", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 3, &AdapterBuilder{})
		fixture.sockets[0].Join("r1", "r2")
		fixture.sockets[1].Join("r2", "r3")
		fixture.sockets[2].Join("r3")
		adapter := fixture.server.Sockets().Adapter()
		if got := adapter.Sockets(types.NewSet[socket.Room]()); got.Len() != 3 {
			t.Fatalf("all sockets = %v, want 3", got.Keys())
		}
		if got := adapter.Sockets(types.NewSet[socket.Room]("r2")); got.Len() != 2 {
			t.Fatalf("r2 sockets = %v, want 2", got.Keys())
		}
		if got := adapter.Sockets(types.NewSet[socket.Room]("r4")); got.Len() != 0 {
			t.Fatalf("r4 sockets = %v, want none", got.Keys())
		}
	})

	t.Run("should return a list of rooms", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1", "r2"))
		adapter.AddAll("s2", types.NewSet[socket.Room]("r2", "r3"))
		adapter.AddAll("s3", types.NewSet[socket.Room]("r3"))
		rooms := adapter.SocketRooms("s2")
		if rooms == nil || rooms.Len() != 2 || !rooms.Has("r2") || !rooms.Has("r3") {
			t.Fatalf("s2 rooms = %v, want r2/r3", rooms)
		}
		if adapter.SocketRooms("s4") != nil {
			t.Fatal("unknown socket returned a room set")
		}
	})

	t.Run("should exclude sockets in specific rooms when broadcasting", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 3, &AdapterBuilder{})
		fixture.sockets[0].Join("r1")
		fixture.sockets[2].Join("r1")
		events := []<-chan []any{
			official258Outgoing(fixture.sockets[0]),
			official258Outgoing(fixture.sockets[1]),
			official258Outgoing(fixture.sockets[2]),
		}
		if err := fixture.server.Except("r1").Emit("test"); err != nil {
			t.Fatal(err)
		}
		assertOfficial258NoEvent(t, events[0])
		assertOfficial258TestEvent(t, events[1])
		assertOfficial258NoEvent(t, events[2])
	})

	t.Run("should exclude sockets in specific rooms when broadcasting to rooms", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 3, &AdapterBuilder{})
		fixture.sockets[0].Join("r1", "r2")
		fixture.sockets[1].Join("r2")
		fixture.sockets[2].Join("r1")
		events := []<-chan []any{
			official258Outgoing(fixture.sockets[0]),
			official258Outgoing(fixture.sockets[1]),
			official258Outgoing(fixture.sockets[2]),
		}
		if err := fixture.server.To("r1").Except("r2").Emit("test", types.NewBytesBuffer([]byte{1, 2, 3})); err != nil {
			t.Fatal(err)
		}
		assertOfficial258NoEvent(t, events[0])
		assertOfficial258NoEvent(t, events[1])
		assertOfficial258TestEvent(t, events[2])
	})

	t.Run("should precompute the WebSocket frames when broadcasting", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 1, &AdapterBuilder{})
		created := make(chan *enginepacket.Packet, 1)
		_ = fixture.sockets[0].Conn().Once("packetCreate", func(args ...any) {
			created <- args[0].(*enginepacket.Packet)
		})
		fixture.server.Emit("test", 1)
		select {
		case packet := <-created:
			if packet.Options == nil || packet.Options.WsPreEncodedFrame == nil {
				t.Fatalf("packet options = %#v, want precomputed frame", packet.Options)
			}
			if got := string(packet.Options.WsPreEncodedFrame.Bytes()); got != `42["test",1]` {
				t.Fatalf("precomputed frame = %q", got)
			}
		case <-time.After(time.Second):
			t.Fatal("packetCreate event was not emitted")
		}
	})

	t.Run("fetchSockets returns the matching socket instances", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 3, &AdapterBuilder{})
		fixture.server.Sockets().Adapter().FetchSockets(nil)(func(sockets []socket.SocketDetails, err error) {
			if err != nil || len(sockets) != 3 {
				t.Fatalf("FetchSockets = %d/%v, want 3/nil", len(sockets), err)
			}
		})
	})

	t.Run("fetchSockets returns matching socket instances within room", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 3, &AdapterBuilder{})
		fixture.sockets[0].Join("r1", "r2")
		fixture.sockets[1].Join("r1")
		fixture.sockets[2].Join("r2")
		fixture.server.Sockets().Adapter().FetchSockets(&socket.BroadcastOptions{
			Rooms:  types.NewSet[socket.Room]("r1"),
			Except: types.NewSet[socket.Room]("r2"),
		})(func(sockets []socket.SocketDetails, err error) {
			if err != nil || len(sockets) != 1 || sockets[0].Id() != fixture.sockets[1].Id() {
				t.Fatalf("filtered FetchSockets = %#v/%v", sockets, err)
			}
		})
	})

	t.Run("should emit a create-room event", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		var rooms []socket.Room
		_ = adapter.On("create-room", func(args ...any) {
			rooms = append(rooms, args[0].(socket.Room))
		})
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1"))
		if len(rooms) != 1 || rooms[0] != "r1" {
			t.Fatalf("create-room events = %v", rooms)
		}
	})

	t.Run("should not emit a create-room event if the room already exists", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1"))
		calls := 0
		_ = adapter.On("create-room", func(...any) { calls++ })
		adapter.AddAll("s2", types.NewSet[socket.Room]("r1"))
		if calls != 0 {
			t.Fatalf("create-room calls for existing room = %d", calls)
		}
	})

	t.Run("should emit a join-room event", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		var joined [][2]string
		_ = adapter.On("join-room", func(args ...any) {
			joined = append(joined, [2]string{string(args[0].(socket.Room)), string(args[1].(socket.SocketId))})
		})
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1"))
		if len(joined) != 1 || joined[0] != [2]string{"r1", "s1"} {
			t.Fatalf("join-room events = %v", joined)
		}
	})

	t.Run("should not emit a join-room event if the sid is already in the room", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1", "r2"))
		calls := 0
		_ = adapter.On("join-room", func(...any) { calls++ })
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1"))
		if calls != 0 {
			t.Fatalf("join-room calls for existing membership = %d", calls)
		}
	})

	t.Run("should emit a leave-room event with del method", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		var left [][2]string
		_ = adapter.On("leave-room", func(args ...any) {
			left = append(left, [2]string{string(args[0].(socket.Room)), string(args[1].(socket.SocketId))})
		})
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1"))
		adapter.Del("s1", "r1")
		if len(left) != 1 || left[0] != [2]string{"r1", "s1"} {
			t.Fatalf("leave-room events from Del = %v", left)
		}
	})

	t.Run("should not throw when calling del twice", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		calls := 0
		_ = adapter.On("leave-room", func(...any) {
			calls++
			adapter.Del("s1", "r1")
		})
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1"))
		adapter.Del("s1", "r1")
		if calls != 1 {
			t.Fatalf("leave-room calls = %d, want 1", calls)
		}
	})

	t.Run("should emit a leave-room event with delAll method", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		var left [][2]string
		_ = adapter.On("leave-room", func(args ...any) {
			left = append(left, [2]string{string(args[0].(socket.Room)), string(args[1].(socket.SocketId))})
		})
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1"))
		adapter.DelAll("s1")
		if len(left) != 1 || left[0] != [2]string{"r1", "s1"} {
			t.Fatalf("leave-room events from DelAll = %v", left)
		}
	})

	t.Run("should emit a delete-room event", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		var deleted []socket.Room
		_ = adapter.On("delete-room", func(args ...any) {
			deleted = append(deleted, args[0].(socket.Room))
		})
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1"))
		adapter.DelAll("s1")
		if len(deleted) != 1 || deleted[0] != "r1" {
			t.Fatalf("delete-room events = %v", deleted)
		}
	})

	t.Run("should not emit a delete-room event if there is another sid in the room", func(t *testing.T) {
		fixture := newOfficial258ServerFixture(t, 0, &AdapterBuilder{})
		adapter := fixture.server.Sockets().Adapter()
		calls := 0
		_ = adapter.On("delete-room", func(...any) { calls++ })
		adapter.AddAll("s1", types.NewSet[socket.Room]("r1"))
		adapter.AddAll("s2", types.NewSet[socket.Room]("r1"))
		adapter.DelAll("s1")
		if calls != 0 {
			t.Fatalf("delete-room calls while another sid remains = %d", calls)
		}
	})

	t.Run("should persist and restore session", func(t *testing.T) {
		adapter := newOfficial258SessionAdapter(t)
		adapter.PersistSession(&socket.SessionToPersist{
			Sid: "abc", Pid: "def", Data: "ghi", Rooms: types.NewSet[socket.Room]("r1", "r2"),
		})
		packet := &parser.Packet{Nsp: "/", Type: parser.EVENT, Data: []any{"hello"}}
		adapter.Broadcast(packet, &socket.BroadcastOptions{})
		data := packet.Data.([]any)
		offset, ok := data[1].(string)
		if !ok || offset == "" {
			t.Fatalf("packet offset = %#v", data)
		}
		session, err := adapter.RestoreSession("def", offset)
		if err != nil || session == nil || session.Sid != "abc" || session.Pid != "def" || len(session.MissedPackets) != 0 {
			t.Fatalf("restored session = %#v/%v", session, err)
		}
	})

	t.Run("should restore missed packets", func(t *testing.T) {
		adapter := newOfficial258SessionAdapter(t)
		adapter.PersistSession(&socket.SessionToPersist{
			Sid: "abc", Pid: "def", Data: "ghi", Rooms: types.NewSet[socket.Room]("r1", "r2"),
		})
		baseline := &parser.Packet{Nsp: "/", Type: parser.EVENT, Data: []any{"hello"}}
		adapter.Broadcast(baseline, &socket.BroadcastOptions{})
		offset := baseline.Data.([]any)[1].(string)
		broadcast := func(packet *parser.Packet, opts *socket.BroadcastOptions) {
			t.Helper()
			adapter.Broadcast(packet, opts)
		}
		broadcast(&parser.Packet{Nsp: "/", Type: parser.EVENT, Data: []any{"all"}}, &socket.BroadcastOptions{})
		broadcast(&parser.Packet{Nsp: "/", Type: parser.EVENT, Data: []any{"room"}}, &socket.BroadcastOptions{Rooms: types.NewSet[socket.Room]("r1")})
		broadcast(&parser.Packet{Nsp: "/", Type: parser.EVENT, Data: []any{"except"}}, &socket.BroadcastOptions{Except: types.NewSet[socket.Room]("r2")})
		broadcast(&parser.Packet{Nsp: "/", Type: parser.EVENT, Data: []any{"no except"}}, &socket.BroadcastOptions{Except: types.NewSet[socket.Room]("r3")})
		ackID := uint64(0)
		broadcast(&parser.Packet{Nsp: "/", Type: parser.EVENT, Id: &ackID, Data: []any{"with ack"}}, &socket.BroadcastOptions{})
		broadcast(&parser.Packet{Nsp: "/", Type: parser.ACK, Data: []any{"ack type"}}, &socket.BroadcastOptions{})
		broadcast(&parser.Packet{Nsp: "/", Type: parser.EVENT, Data: []any{"volatile"}}, &socket.BroadcastOptions{Flags: &socket.BroadcastFlags{WriteOptions: socket.WriteOptions{Volatile: true}}})

		session, err := adapter.RestoreSession("def", offset)
		if err != nil || session == nil {
			t.Fatalf("RestoreSession = %#v/%v", session, err)
		}
		want := []string{"all", "room", "no except"}
		if len(session.MissedPackets) != len(want) {
			t.Fatalf("missed packets = %#v, want %v", session.MissedPackets, want)
		}
		for index, packetData := range session.MissedPackets {
			values, ok := packetData.([]any)
			if !ok || len(values) != 2 || values[0] != want[index] {
				t.Fatalf("missed packet %d = %#v, want event %q plus offset", index, packetData, want[index])
			}
		}
	})

	t.Run("should fail to restore an unknown session", func(t *testing.T) {
		adapter := newOfficial258SessionAdapter(t)
		session, err := adapter.RestoreSession("abc", "def")
		if err != nil || session != nil {
			t.Fatalf("unknown RestoreSession = %#v/%v", session, err)
		}
	})

	t.Run("should fail to restore a known session with an unknown offset", func(t *testing.T) {
		adapter := newOfficial258SessionAdapter(t)
		adapter.PersistSession(&socket.SessionToPersist{
			Sid: "abc", Pid: "def", Data: "ghi", Rooms: types.NewSet[socket.Room]("r1", "r2"),
		})
		session, err := adapter.RestoreSession("def", "unknown-offset")
		if err != nil || session != nil {
			t.Fatalf("unknown-offset RestoreSession = %#v/%v", session, err)
		}
	})
}
