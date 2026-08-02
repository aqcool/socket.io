package broker

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	clientsocket "github.com/aqcool/socket.io/clients/socket/v3"
	serversocket "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

func TestBrokerClusterContract(t *testing.T) {
	transport := NewMemoryBroker()
	builder := &Builder{Broker: transport, Options: Options{
		DeliverySemantics: AtLeastOnce,
		Ordered:           true,
	}}
	firstOptions, secondOptions := serversocket.DefaultServerOptions(), serversocket.DefaultServerOptions()
	firstOptions.SetAdapter(builder)
	secondOptions.SetAdapter(builder)
	first := serversocket.NewServer(nil, firstOptions)
	second := serversocket.NewServer(nil, secondOptions)
	httpServer := httptest.NewServer(second.ServeHandler(nil))
	t.Cleanup(func() {
		first.Close(nil)
		second.Close(nil)
		httpServer.Close()
	})

	serverSocket := make(chan *serversocket.Socket, 1)
	_ = second.On("connection", func(args ...any) {
		client := args[0].(*serversocket.Socket)
		client.Join("room-1")
		serverSocket <- client
	})
	options := clientsocket.DefaultOptions()
	options.SetAutoConnect(false)
	options.SetTransports(types.NewSet(clientsocket.WebSocket))
	client, err := clientsocket.Connect(httpServer.URL+"/", options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	connected := make(chan struct{}, 1)
	disconnected := make(chan struct{}, 1)
	notice := make(chan string, 1)
	_ = client.On("connect", func(...any) { connected <- struct{}{} })
	_ = client.On("disconnect", func(...any) { disconnected <- struct{}{} })
	_ = client.On("notice", func(args ...any) {
		if len(args) > 0 {
			if value, ok := args[0].(string); ok {
				notice <- value
			}
		}
	})
	_ = client.On("question", func(args ...any) {
		if len(args) > 0 {
			if ack, ok := args[len(args)-1].(serversocket.Ack); ok {
				ack([]any{"answer"}, nil)
			}
		}
	})
	client.Connect()
	waitValue(t, connected, "client connection")
	remoteSocket := waitValue(t, serverSocket, "server socket")
	brokerErrors := make(chan error, 4)
	_ = first.Sockets().Adapter().On("error", func(args ...any) {
		if len(args) > 0 {
			if brokerErr, ok := args[0].(error); ok {
				brokerErrors <- brokerErr
			}
		}
	})
	_ = second.Sockets().Adapter().On("error", func(args ...any) {
		if len(args) > 0 {
			if brokerErr, ok := args[0].(error); ok {
				brokerErrors <- brokerErr
			}
		}
	})
	eventually(t, func() bool {
		return first.Sockets().Adapter().ServerCount() == 2
	}, "node discovery")

	if err = first.Sockets().Emit("notice", "hello"); err != nil {
		t.Fatal(err)
	}
	if value := waitValue(t, notice, "normal broadcast"); value != "hello" {
		t.Fatalf("unexpected normal broadcast value %q", value)
	}

	ackResult := make(chan error, 1)
	if err = first.To("room-1").Timeout(time.Second).Emit("question", func(values []any, ackErr error) {
		if ackErr == nil && (len(values) != 1 || values[0] != "answer") {
			ackErr = errors.New("unexpected broadcast ACK")
		}
		ackResult <- ackErr
	}); err != nil {
		t.Fatal(err)
	}
	if ackErr := waitValue(t, ackResult, "broadcast ACK"); ackErr != nil {
		t.Fatal(ackErr)
	}

	fetched := make(chan struct {
		count int
		err   error
	}, 1)
	first.In("room-1").FetchSockets()(func(sockets []*serversocket.RemoteSocket, fetchErr error) {
		fetched <- struct {
			count int
			err   error
		}{count: len(sockets), err: fetchErr}
	})
	fetchResult := waitValueWithin(t, fetched, "fetchSockets", 7*time.Second)
	if fetchResult.err != nil || fetchResult.count != 1 {
		select {
		case brokerErr := <-brokerErrors:
			t.Fatalf("fetchSockets count=%d err=%v broker=%v", fetchResult.count, fetchResult.err, brokerErr)
		default:
		}
		t.Fatalf("fetchSockets count=%d err=%v", fetchResult.count, fetchResult.err)
	}

	first.In("room-1").SocketsJoin("room-2")
	eventually(t, func() bool { return remoteSocket.Rooms().Has("room-2") }, "socketsJoin")
	first.In("room-2").SocketsLeave("room-1")
	eventually(t, func() bool { return !remoteSocket.Rooms().Has("room-1") }, "socketsLeave")

	first.In("room-2").DisconnectSockets(false)
	waitValue(t, disconnected, "disconnectSockets")
}

func waitValue[T any](t *testing.T, channel <-chan T, name string) T {
	return waitValueWithin(t, channel, name, 3*time.Second)
}

func waitValueWithin[T any](t *testing.T, channel <-chan T, name string, timeout time.Duration) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(timeout):
		var zero T
		t.Fatalf("timed out waiting for %s", name)
		return zero
	}
}

func eventually(t *testing.T, condition func() bool, name string) {
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
