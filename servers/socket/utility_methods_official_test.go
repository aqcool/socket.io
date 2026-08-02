package socket

import (
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aqcool/socket.io/v3/pkg/types"
)

func newOfficialUtilitySockets(t *testing.T) (*Server, []*Socket) {
	t.Helper()
	server := NewServer(nil, nil)
	connected := make(chan *Socket, 3)
	var mu sync.Mutex
	sockets := make([]*Socket, 0, 3)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(*Socket)
		mu.Lock()
		sockets = append(sockets, socket)
		mu.Unlock()
		connected <- socket
	})
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})
	for range 3 {
		_ = connectOfficialWebSocketAt(t, httpServer.URL, connected)
	}
	mu.Lock()
	result := append([]*Socket(nil), sockets...)
	mu.Unlock()
	return server, result
}

func fetchOfficialUtilitySockets(t *testing.T, operation func(func([]*RemoteSocket, error))) []*RemoteSocket {
	t.Helper()
	var result []*RemoteSocket
	called := false
	operation(func(sockets []*RemoteSocket, err error) {
		called = true
		if err != nil {
			t.Fatalf("FetchSockets: %v", err)
		}
		result = sockets
	})
	if !called {
		t.Fatal("FetchSockets callback was not called")
	}
	return result
}

func TestOfficialUtilityFetchJoinAndLeaveSockets(t *testing.T) {
	server, sockets := newOfficialUtilitySockets(t)
	sockets[0].Join("room1", "room2")
	sockets[1].Join("room1")
	sockets[2].Join("room2")

	if got := fetchOfficialUtilitySockets(t, server.FetchSockets()); len(got) != 3 {
		t.Fatalf("all fetched sockets = %d, want 3", len(got))
	}
	roomOne := fetchOfficialUtilitySockets(t, server.In("room1").FetchSockets())
	if len(roomOne) != 2 {
		t.Fatalf("room1 fetched sockets = %d, want 2", len(roomOne))
	}

	server.SocketsJoin("all")
	for index, socket := range sockets {
		if !socket.Rooms().Has("all") {
			t.Fatalf("socket %d did not join all", index)
		}
	}
	server.In("room1").SocketsJoin("room3")
	if !sockets[0].Rooms().Has("room3") || !sockets[1].Rooms().Has("room3") || sockets[2].Rooms().Has("room3") {
		t.Fatalf("filtered SocketsJoin room3 = %v/%v/%v", sockets[0].Rooms().Keys(), sockets[1].Rooms().Keys(), sockets[2].Rooms().Keys())
	}

	server.SocketsLeave("all")
	for index, socket := range sockets {
		if socket.Rooms().Has("all") {
			t.Fatalf("socket %d did not leave all", index)
		}
	}
	server.SocketsLeave("room1")
	if sockets[0].Rooms().Has("room1") || sockets[1].Rooms().Has("room1") {
		t.Fatal("unfiltered SocketsLeave did not remove room1")
	}
	sockets[0].Join("room1")
	sockets[1].Join("room1")
	server.In("room2").SocketsLeave("room1")
	if sockets[0].Rooms().Has("room1") || !sockets[1].Rooms().Has("room1") {
		t.Fatalf("filtered SocketsLeave room1 = %v/%v", sockets[0].Rooms().Keys(), sockets[1].Rooms().Keys())
	}
}

type officialUtilitySocketDetails struct {
	id        SocketId
	handshake *Handshake
	rooms     *types.Set[Room]
	data      any
}

func (d *officialUtilitySocketDetails) Id() SocketId            { return d.id }
func (d *officialUtilitySocketDetails) Handshake() *Handshake   { return d.handshake }
func (d *officialUtilitySocketDetails) Rooms() *types.Set[Room] { return d.rooms }
func (d *officialUtilitySocketDetails) Data() any               { return d.data }

type officialUtilityAdapterBuilder struct{}

func (*officialUtilityAdapterBuilder) New(namespace Namespace) Adapter {
	return &allSocketsAdapter{
		Adapter: NewAdapter(namespace),
		details: []SocketDetails{&officialUtilitySocketDetails{
			id: "42",
			handshake: &Handshake{
				Headers: types.IncomingHttpHeaders{"accept": "*/*"},
				Query:   types.ParsedUrlQuery{"transport": "polling", "EIO": "4"},
			},
			rooms: types.NewSet[Room]("42", "room1"),
			data:  map[string]any{"username": "john"},
		}},
	}
}

func (*officialUtilityAdapterBuilder) SupportsConnectionStateRecovery() bool { return false }

func TestOfficialUtilityFetchSocketsWithCustomAdapter(t *testing.T) {
	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })
	server.SetAdapter(&officialUtilityAdapterBuilder{})
	sockets := fetchOfficialUtilitySockets(t, server.FetchSockets())
	if len(sockets) != 1 {
		t.Fatalf("custom Adapter fetched sockets = %d, want 1", len(sockets))
	}
	remote := sockets[0]
	data, ok := remote.Data().(map[string]any)
	if remote.Id() != "42" || !remote.Rooms().Has("42") || !remote.Rooms().Has("room1") || !ok || data["username"] != "john" {
		t.Fatalf("custom Adapter RemoteSocket = id %q, rooms %v, data %#v", remote.Id(), remote.Rooms().Keys(), remote.Data())
	}
	if remote.Handshake().Headers["accept"] != "*/*" || remote.Handshake().Query["EIO"] != "4" {
		t.Fatalf("custom Adapter handshake = %#v", remote.Handshake())
	}
}

func TestOfficialUtilityDisconnectSockets(t *testing.T) {
	t.Run("all sockets", func(t *testing.T) {
		server, sockets := newOfficialUtilitySockets(t)
		server.DisconnectSockets(true)
		for index, socket := range sockets {
			if socket.Connected() {
				t.Fatalf("socket %d remained connected", index)
			}
		}
		if server.Sockets().Sockets().Len() != 0 {
			t.Fatalf("namespace sockets after disconnect all = %d", server.Sockets().Sockets().Len())
		}
	})

	t.Run("sockets in a room", func(t *testing.T) {
		server, sockets := newOfficialUtilitySockets(t)
		sockets[0].Join("room1", "room2")
		sockets[1].Join("room1")
		sockets[2].Join("room2")
		server.In("room2").DisconnectSockets(true)
		if sockets[0].Connected() || !sockets[1].Connected() || sockets[2].Connected() {
			t.Fatalf("filtered disconnect states = %t/%t/%t", sockets[0].Connected(), sockets[1].Connected(), sockets[2].Connected())
		}
		if server.Sockets().Sockets().Len() != 1 {
			t.Fatalf("namespace sockets after filtered disconnect = %d, want 1", server.Sockets().Sockets().Len())
		}
	})
}
