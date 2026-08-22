package adapter

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/socket/v4"
)

type official258ClusterBus struct {
	mu       sync.RWMutex
	adapters map[ServerId]*official258ClusterAdapter
	offset   atomic.Uint64
}

func newOfficial258ClusterBus() *official258ClusterBus {
	return &official258ClusterBus{adapters: make(map[ServerId]*official258ClusterAdapter)}
}

func (b *official258ClusterBus) add(adapter *official258ClusterAdapter) {
	b.mu.Lock()
	b.adapters[adapter.Uid()] = adapter
	b.mu.Unlock()
}

func (b *official258ClusterBus) remove(uid ServerId) {
	b.mu.Lock()
	delete(b.adapters, uid)
	b.mu.Unlock()
}

func (b *official258ClusterBus) publish(message *ClusterMessage) (Offset, error) {
	payload, err := EncodeClusterMessage(message)
	if err != nil {
		return "", err
	}
	b.mu.RLock()
	peers := make([]*official258ClusterAdapter, 0, len(b.adapters))
	for _, adapter := range b.adapters {
		peers = append(peers, adapter)
	}
	b.mu.RUnlock()
	for _, peer := range peers {
		copyOfMessage, decodeErr := DecodeClusterMessage(payload)
		if decodeErr != nil {
			return "", decodeErr
		}
		peer.OnMessage(copyOfMessage, "")
	}
	return Offset(strconv.FormatUint(b.offset.Add(1), 10)), nil
}

func (b *official258ClusterBus) respond(requesterUID ServerId, response *ClusterResponse) error {
	payload, err := EncodeClusterMessage(response)
	if err != nil {
		return err
	}
	b.mu.RLock()
	peer := b.adapters[requesterUID]
	b.mu.RUnlock()
	if peer == nil {
		return fmt.Errorf("unknown requester %q", requesterUID)
	}
	copyOfResponse, err := DecodeClusterMessage(payload)
	if err != nil {
		return err
	}
	peer.OnMessage(copyOfResponse, "")
	return nil
}

type official258ClusterAdapter struct {
	ClusterAdapterWithHeartbeat
	bus         *official258ClusterBus
	reject      atomic.Bool
	closeOnce   sync.Once
	publishHook func(*ClusterMessage)
}

func (a *official258ClusterAdapter) DoPublish(message *ClusterMessage) (Offset, error) {
	if a.reject.Load() {
		return "", errors.New("publish failed")
	}
	if a.publishHook != nil {
		a.publishHook(message)
	}
	return a.bus.publish(message)
}

func (a *official258ClusterAdapter) DoPublishResponse(requesterUID ServerId, response *ClusterResponse) error {
	return a.bus.respond(requesterUID, response)
}

func (a *official258ClusterAdapter) Close() {
	a.closeOnce.Do(func() {
		a.ClusterAdapterWithHeartbeat.Close()
		a.bus.remove(a.Uid())
	})
}

type official258ClusterBuilder struct {
	bus      *official258ClusterBus
	mu       sync.RWMutex
	adapters map[string]*official258ClusterAdapter
}

func newOfficial258ClusterBuilder(bus *official258ClusterBus) *official258ClusterBuilder {
	return &official258ClusterBuilder{bus: bus, adapters: make(map[string]*official258ClusterAdapter)}
}

func (b *official258ClusterBuilder) New(namespace socket.Namespace) Adapter {
	options := DefaultClusterAdapterOptions()
	options.SetHeartbeatInterval(time.Hour)
	options.SetHeartbeatTimeout(int64((2 * time.Hour) / time.Millisecond))
	result := &official258ClusterAdapter{
		ClusterAdapterWithHeartbeat: NewClusterAdapterWithHeartbeat(namespace, options),
		bus:                         b.bus,
	}
	result.Prototype(result)
	b.mu.Lock()
	b.adapters[namespace.Name()] = result
	b.mu.Unlock()
	b.bus.add(result)
	return result
}

func (*official258ClusterBuilder) SupportsConnectionStateRecovery() bool { return false }

func (b *official258ClusterBuilder) adapter(namespace string) *official258ClusterAdapter {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.adapters[namespace]
}

type official258ClusterFixture struct {
	bus         *official258ClusterBus
	servers     []*socket.Server
	httpServers []*official258ServerFixture
	builders    []*official258ClusterBuilder
	adapters    []*official258ClusterAdapter
	sockets     []*socket.Socket
	connections int
}

func newOfficial258ClusterFixture(t *testing.T) *official258ClusterFixture {
	t.Helper()
	fixture := &official258ClusterFixture{bus: newOfficial258ClusterBus()}
	for range 3 {
		builder := newOfficial258ClusterBuilder(fixture.bus)
		serverFixture := newOfficial258ServerFixture(t, 1, builder)
		adapter := builder.adapter("/")
		if adapter == nil {
			t.Fatal("root ClusterAdapter was not constructed")
		}
		fixture.builders = append(fixture.builders, builder)
		fixture.httpServers = append(fixture.httpServers, serverFixture)
		fixture.servers = append(fixture.servers, serverFixture.server)
		fixture.adapters = append(fixture.adapters, adapter)
		fixture.sockets = append(fixture.sockets, serverFixture.sockets[0])
		fixture.connections++
	}
	for index, adapter := range fixture.adapters {
		if count := adapter.ServerCount(); count != 3 {
			t.Fatalf("adapter %d ServerCount = %d, want 3", index, count)
		}
	}
	return fixture
}

func (f *official258ClusterFixture) connectNamespace(t *testing.T, namespace string) ([]socket.Namespace, []*socket.Socket) {
	t.Helper()
	namespaces := make([]socket.Namespace, 0, len(f.servers))
	sockets := make([]*socket.Socket, 0, len(f.servers))
	for index, server := range f.servers {
		nsp := server.Of(namespace, nil)
		connected := make(chan *socket.Socket, 1)
		_ = nsp.On("connection", func(args ...any) {
			connected <- args[0].(*socket.Socket)
		})
		connection := dialOfficial258Namespace(t, f.httpServers[index].httpServer.URL, namespace)
		f.httpServers[index].connections = append(f.httpServers[index].connections, connection)
		select {
		case connectedSocket := <-connected:
			namespaces = append(namespaces, nsp)
			sockets = append(sockets, connectedSocket)
		case <-time.After(2 * time.Second):
			t.Fatalf("connection event for namespace %q was not emitted", namespace)
		}
	}
	return namespaces, sockets
}

func invokeOfficial258Ack(t *testing.T, socketInstance *socket.Socket, response ...any) {
	t.Helper()
	keys := socketInstance.Acks().Keys()
	if len(keys) != 1 {
		t.Fatalf("pending acknowledgements for %s = %v, want exactly one", socketInstance.Id(), keys)
	}
	ack, ok := socketInstance.Acks().LoadAndDelete(keys[0])
	if !ok {
		t.Fatalf("pending acknowledgement %d disappeared", keys[0])
	}
	ack(response, nil)
}

type official258AckResult struct {
	responses []any
	err       error
}

func waitOfficial258Ack(t *testing.T, result <-chan official258AckResult) official258AckResult {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(6 * time.Second):
		t.Fatal("acknowledgement callback was not called")
		return official258AckResult{}
	}
}

func assertOfficial258Rooms(t *testing.T, sockets []*socket.Socket, room socket.Room, expected ...bool) {
	t.Helper()
	for index, socketInstance := range sockets {
		if actual := socketInstance.Rooms().Has(room); actual != expected[index] {
			t.Fatalf("socket %d room %q = %v, want %v; rooms=%v", index, room, actual, expected[index], socketInstance.Rooms().Keys())
		}
	}
}

// TestOfficial258ClusterAdapter maps the executable runtime tests in the
// official packages/socket.io-adapter/test/cluster-adapter.ts file. The
// publish-failure regression has its own focused test in
// cluster-adapter-publish-failure_test.go.
func TestOfficial258ClusterAdapter(t *testing.T) {
	t.Run("broadcasts to all clients", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		events := make([]<-chan []any, 0, 3)
		for _, socketInstance := range fixture.sockets {
			events = append(events, official258Outgoing(socketInstance))
		}
		fixture.servers[0].Emit("test", 1, "2", []byte{3, 4})
		for _, stream := range events {
			event := assertOfficial258TestEvent(t, stream)
			if len(event) != 4 || fmt.Sprint(event[1]) != "1" || event[2] != "2" {
				t.Fatalf("broadcast arguments = %#v", event)
			}
		}
		for index, serverFixture := range fixture.httpServers {
			connection := serverFixture.connections[0]
			if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			messageType, header, err := connection.ReadMessage()
			if err != nil || messageType != 1 || !strings.Contains(string(header), `45`) || !strings.Contains(string(header), `"test"`) {
				t.Fatalf("client %d binary event header = type %d, %q/%v", index, messageType, header, err)
			}
			messageType, attachment, err := connection.ReadMessage()
			if err != nil || messageType != 2 || !reflect.DeepEqual(attachment, []byte{3, 4}) {
				t.Fatalf("client %d binary attachment = type %d, %v/%v", index, messageType, attachment, err)
			}
		}
	})

	t.Run("broadcasts to all clients in a namespace", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		namespaces, sockets := fixture.connectNamespace(t, "/custom")
		events := make([]<-chan []any, 0, 3)
		for _, socketInstance := range sockets {
			events = append(events, official258Outgoing(socketInstance))
		}
		if err := namespaces[0].Emit("test"); err != nil {
			t.Fatal(err)
		}
		for _, stream := range events {
			assertOfficial258TestEvent(t, stream)
		}
	})

	t.Run("broadcasts to all clients in a room", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		fixture.sockets[1].Join("room1")
		events := []<-chan []any{
			official258Outgoing(fixture.sockets[0]),
			official258Outgoing(fixture.sockets[1]),
			official258Outgoing(fixture.sockets[2]),
		}
		if err := fixture.servers[0].To("room1").Emit("test"); err != nil {
			t.Fatal(err)
		}
		assertOfficial258NoEvent(t, events[0])
		assertOfficial258TestEvent(t, events[1])
		assertOfficial258NoEvent(t, events[2])
	})

	t.Run("broadcasts to all clients except in room", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		fixture.sockets[1].Join("room1")
		events := []<-chan []any{
			official258Outgoing(fixture.sockets[0]),
			official258Outgoing(fixture.sockets[1]),
			official258Outgoing(fixture.sockets[2]),
		}
		if err := fixture.servers[0].Except("room1").Emit("test"); err != nil {
			t.Fatal(err)
		}
		assertOfficial258TestEvent(t, events[0])
		assertOfficial258NoEvent(t, events[1])
		assertOfficial258TestEvent(t, events[2])
	})

	t.Run("broadcasts to local clients only", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		events := []<-chan []any{
			official258Outgoing(fixture.sockets[0]),
			official258Outgoing(fixture.sockets[1]),
			official258Outgoing(fixture.sockets[2]),
		}
		if err := fixture.servers[0].Local().Emit("test"); err != nil {
			t.Fatal(err)
		}
		assertOfficial258TestEvent(t, events[0])
		assertOfficial258NoEvent(t, events[1])
		assertOfficial258NoEvent(t, events[2])
	})

	t.Run("broadcasts with multiple acknowledgements", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		result := make(chan official258AckResult, 1)
		fixture.servers[0].Timeout(100 * time.Millisecond).EmitWithAck("test")(func(responses []any, err error) {
			result <- official258AckResult{responses: responses, err: err}
		})
		for index, socketInstance := range fixture.sockets {
			invokeOfficial258Ack(t, socketInstance, index+1)
		}
		actual := waitOfficial258Ack(t, result)
		if actual.err != nil || len(actual.responses) != 3 {
			t.Fatalf("multiple acknowledgements = %#v/%v", actual.responses, actual.err)
		}
		time.Sleep(150 * time.Millisecond)
		cluster := fixture.adapters[0].ClusterAdapterWithHeartbeat.(*clusterAdapterWithHeartbeat).ClusterAdapter.(*clusterAdapter)
		if cluster.ackRequests.Len() != 0 {
			t.Fatalf("ackRequests size = %d after cleanup", cluster.ackRequests.Len())
		}
	})

	t.Run("broadcasts with multiple acknowledgements binary content", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		result := make(chan official258AckResult, 1)
		fixture.servers[0].Timeout(time.Second).EmitWithAck("test")(func(responses []any, err error) {
			result <- official258AckResult{responses: responses, err: err}
		})
		for index, socketInstance := range fixture.sockets {
			invokeOfficial258Ack(t, socketInstance, []byte{byte(index + 1)})
		}
		actual := waitOfficial258Ack(t, result)
		if actual.err != nil || len(actual.responses) != 3 {
			t.Fatalf("binary acknowledgements = %#v/%v", actual.responses, actual.err)
		}
		for _, response := range actual.responses {
			if _, ok := response.([]byte); !ok {
				t.Fatalf("binary acknowledgement = %#v", response)
			}
		}
	})

	t.Run("broadcasts with multiple acknowledgements no client", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		result := make(chan official258AckResult, 1)
		fixture.servers[0].To("abc").Timeout(time.Second).EmitWithAck("test")(func(responses []any, err error) {
			result <- official258AckResult{responses: responses, err: err}
		})
		actual := waitOfficial258Ack(t, result)
		if actual.err != nil || len(actual.responses) != 0 {
			t.Fatalf("empty-room acknowledgements = %#v/%v", actual.responses, actual.err)
		}
	})

	t.Run("broadcasts with multiple acknowledgements timeout", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		result := make(chan official258AckResult, 1)
		fixture.servers[0].Timeout(50 * time.Millisecond).EmitWithAck("test")(func(responses []any, err error) {
			result <- official258AckResult{responses: responses, err: err}
		})
		invokeOfficial258Ack(t, fixture.sockets[0], 1)
		invokeOfficial258Ack(t, fixture.sockets[1], 2)
		actual := waitOfficial258Ack(t, result)
		if actual.err == nil || len(actual.responses) != 2 {
			t.Fatalf("timed-out acknowledgements = %#v/%v", actual.responses, actual.err)
		}
	})

	t.Run("broadcasts with a single acknowledgement local", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		result := make(chan official258AckResult, 1)
		fixture.sockets[1].EmitWithAck("test")(func(responses []any, err error) {
			result <- official258AckResult{responses: responses, err: err}
		})
		invokeOfficial258Ack(t, fixture.sockets[1], 2)
		actual := waitOfficial258Ack(t, result)
		if actual.err != nil || len(actual.responses) != 1 || actual.responses[0] != 2 {
			t.Fatalf("local single acknowledgement = %#v/%v", actual.responses, actual.err)
		}
	})

	t.Run("broadcasts with a single acknowledgement remote", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		var remoteSockets []*socket.RemoteSocket
		fixture.servers[0].In(socket.Room(fixture.sockets[1].Id())).FetchSockets()(func(sockets []*socket.RemoteSocket, err error) {
			if err != nil {
				t.Fatal(err)
			}
			remoteSockets = sockets
		})
		if len(remoteSockets) != 1 {
			t.Fatalf("remote FetchSockets length = %d, want 1", len(remoteSockets))
		}
		result := make(chan official258AckResult, 1)
		remoteSockets[0].Timeout(time.Second).EmitWithAck("test")(func(responses []any, err error) {
			result <- official258AckResult{responses: responses, err: err}
		})
		invokeOfficial258Ack(t, fixture.sockets[1], 2)
		actual := waitOfficial258Ack(t, result)
		if actual.err != nil || len(actual.responses) != 1 || fmt.Sprint(actual.responses[0]) != "2" {
			t.Fatalf("remote single acknowledgement = %#v/%v", actual.responses, actual.err)
		}
	})

	t.Run("makes all socket instances join the specified room", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		fixture.servers[0].SocketsJoin("room1")
		assertOfficial258Rooms(t, fixture.sockets, "room1", true, true, true)
	})

	t.Run("makes the matching socket instances join the specified room", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		fixture.sockets[0].Join("room1")
		fixture.sockets[2].Join("room1")
		fixture.servers[0].In("room1").SocketsJoin("room2")
		assertOfficial258Rooms(t, fixture.sockets, "room2", true, false, true)
	})

	t.Run("makes the given socket instance join the specified room", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		fixture.servers[0].In(socket.Room(fixture.sockets[1].Id())).SocketsJoin("room3")
		assertOfficial258Rooms(t, fixture.sockets, "room3", false, true, false)
	})

	t.Run("makes all socket instances leave the specified room", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		fixture.sockets[0].Join("room1")
		fixture.sockets[2].Join("room1")
		fixture.servers[0].SocketsLeave("room1")
		assertOfficial258Rooms(t, fixture.sockets, "room1", false, false, false)
	})

	t.Run("makes the matching socket instances leave the specified room", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		fixture.sockets[0].Join("room1", "room2")
		fixture.sockets[1].Join("room1", "room2")
		fixture.sockets[2].Join("room2")
		fixture.servers[0].In("room1").SocketsLeave("room2")
		assertOfficial258Rooms(t, fixture.sockets, "room2", false, false, true)
	})

	t.Run("makes the given socket instance leave the specified room", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		for _, socketInstance := range fixture.sockets {
			socketInstance.Join("room3")
		}
		fixture.servers[0].In(socket.Room(fixture.sockets[1].Id())).SocketsLeave("room3")
		assertOfficial258Rooms(t, fixture.sockets, "room3", true, false, true)
	})

	t.Run("makes all socket instances disconnect", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		fixture.servers[0].DisconnectSockets(true)
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			allDisconnected := true
			for _, socketInstance := range fixture.sockets {
				allDisconnected = allDisconnected && socketInstance.Disconnected()
			}
			if allDisconnected {
				return
			}
			time.Sleep(time.Millisecond)
		}
		for index, socketInstance := range fixture.sockets {
			if !socketInstance.Disconnected() {
				t.Errorf("socket %d is still connected", index)
			}
		}
	})

	t.Run("sends a packet before all socket instances disconnect", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		fixture.servers[0].Emit("bye")
		fixture.servers[0].DisconnectSockets(true)
		for index, serverFixture := range fixture.httpServers {
			connection := serverFixture.connections[0]
			if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			_, payload, err := connection.ReadMessage()
			if err != nil || string(payload) != `42["bye"]` {
				t.Fatalf("client %d first packet before disconnect = %q/%v", index, payload, err)
			}
		}
	})

	t.Run("fetchSockets returns all socket instances", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		var fetched []*socket.RemoteSocket
		fixture.servers[0].FetchSockets()(func(sockets []*socket.RemoteSocket, err error) {
			if err != nil {
				t.Fatal(err)
			}
			fetched = sockets
		})
		if len(fetched) != 3 {
			t.Fatalf("FetchSockets length = %d, want 3", len(fetched))
		}
		heartbeat := fixture.adapters[0].ClusterAdapterWithHeartbeat.(*clusterAdapterWithHeartbeat)
		if heartbeat.customRequests.Len() != 0 {
			t.Fatalf("custom request count after FetchSockets = %d", heartbeat.customRequests.Len())
		}
	})

	t.Run("fetchSockets returns a single socket instance", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		fixture.sockets[1].SetData("test")
		var fetched []*socket.RemoteSocket
		fixture.servers[0].In(socket.Room(fixture.sockets[1].Id())).FetchSockets()(func(sockets []*socket.RemoteSocket, err error) {
			if err != nil {
				t.Fatal(err)
			}
			fetched = sockets
		})
		if len(fetched) != 1 {
			t.Fatalf("filtered FetchSockets length = %d, want 1", len(fetched))
		}
		remote := fetched[0]
		if !reflect.DeepEqual(remote.Handshake(), fixture.sockets[1].Handshake()) || remote.Data() != "test" || remote.Rooms().Len() != 1 {
			t.Fatalf("remote socket = handshake %#v, data %#v, rooms %v", remote.Handshake(), remote.Data(), remote.Rooms().Keys())
		}
	})

	t.Run("fetchSockets returns only local socket instances", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		var fetched []*socket.RemoteSocket
		fixture.servers[0].Local().FetchSockets()(func(sockets []*socket.RemoteSocket, err error) {
			if err != nil {
				t.Fatal(err)
			}
			fetched = sockets
		})
		if len(fetched) != 1 || fetched[0].Id() != fixture.sockets[0].Id() {
			t.Fatalf("local FetchSockets = %#v", fetched)
		}
	})

	t.Run("serverSideEmit sends an event to other server instances", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		originCalled := atomic.Bool{}
		_ = fixture.servers[0].Sockets().On("hello", func(...any) { originCalled.Store(true) })
		received := make(chan []any, 2)
		for index := 1; index < 3; index++ {
			_ = fixture.servers[index].Sockets().On("hello", func(args ...any) {
				received <- append([]any(nil), args...)
			})
		}
		if err := fixture.servers[0].ServerSideEmit("hello", "world", 1, "2"); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			select {
			case args := <-received:
				if len(args) != 3 || args[0] != "world" || fmt.Sprint(args[1]) != "1" || args[2] != "2" {
					t.Fatalf("server-side event arguments = %#v", args)
				}
			case <-time.After(time.Second):
				t.Fatal("server-side event was not received")
			}
		}
		if originCalled.Load() {
			t.Fatal("server-side event leaked to originating server")
		}
	})

	t.Run("serverSideEmit sends an event and receives responses", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		_ = fixture.servers[1].Sockets().On("hello", func(args ...any) {
			args[len(args)-1].(socket.Ack)([]any{2}, nil)
		})
		_ = fixture.servers[2].Sockets().On("hello", func(args ...any) {
			args[len(args)-1].(socket.Ack)([]any{"3"}, nil)
		})
		result := make(chan official258AckResult, 1)
		if err := fixture.servers[0].ServerSideEmitWithAck("hello")(func(responses []any, err error) {
			result <- official258AckResult{responses: responses, err: err}
		}); err != nil {
			t.Fatal(err)
		}
		actual := waitOfficial258Ack(t, result)
		if actual.err != nil || len(actual.responses) != 2 {
			t.Fatalf("server-side responses = %#v/%v", actual.responses, actual.err)
		}
	})

	t.Run("serverSideEmit times out if one server does not respond", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		_ = fixture.servers[1].Sockets().On("hello", func(args ...any) {
			args[len(args)-1].(socket.Ack)([]any{2}, nil)
		})
		_ = fixture.servers[2].Sockets().On("hello", func(...any) {})
		result := make(chan official258AckResult, 1)
		if err := fixture.servers[0].ServerSideEmitWithAck("hello")(func(responses []any, err error) {
			result <- official258AckResult{responses: responses, err: err}
		}); err != nil {
			t.Fatal(err)
		}
		actual := waitOfficial258Ack(t, result)
		if actual.err == nil || actual.err.Error() != "timeout reached: missing 1 responses" || len(actual.responses) != 1 || fmt.Sprint(actual.responses[0]) != "2" {
			t.Fatalf("timed-out server-side responses = %#v/%v", actual.responses, actual.err)
		}
	})

	t.Run("serverSideEmit succeeds even if an instance leaves the cluster", func(t *testing.T) {
		fixture := newOfficial258ClusterFixture(t)
		_ = fixture.servers[1].Sockets().On("hello", func(args ...any) {
			args[len(args)-1].(socket.Ack)([]any{2}, nil)
		})
		_ = fixture.servers[2].Sockets().On("hello", func(...any) {
			fixture.adapters[2].Close()
		})
		result := make(chan official258AckResult, 1)
		if err := fixture.servers[0].ServerSideEmitWithAck("hello")(func(responses []any, err error) {
			result <- official258AckResult{responses: responses, err: err}
		}); err != nil {
			t.Fatal(err)
		}
		actual := waitOfficial258Ack(t, result)
		if actual.err != nil || len(actual.responses) != 1 || fmt.Sprint(actual.responses[0]) != "2" {
			t.Fatalf("leave-during-request responses = %#v/%v", actual.responses, actual.err)
		}
	})
}
