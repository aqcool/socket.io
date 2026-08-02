package reliability

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	clientsocket "github.com/aqcool/socket.io/clients/socket/v3"
	serversocket "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/servers/socket/v3/presence"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

func TestReliableDeliveryAndOfflineReplay(t *testing.T) {
	server := serversocket.NewServer(nil, nil)
	tracker, err := presence.New(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tracker.Close)
	store := NewMemoryStore()
	service, err := New(server, store, &Options{Limits: Limits{Retention: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})

	options := clientsocket.DefaultOptions()
	options.SetAutoConnect(false)
	options.SetTransports(types.NewSet(clientsocket.WebSocket))
	options.SetAuth(map[string]any{"clientId": "device-1", "userId": "user-1"})
	client, err := clientsocket.Connect(httpServer.URL+"/", options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	reliableClient, err := BindClient(client)
	if err != nil || reliableClient == nil {
		t.Fatalf("bind client: %v", err)
	}
	received := make(chan string, 2)
	_ = client.On("notification", func(args ...any) {
		if len(args) > 0 {
			received <- stringValue(args[0])
		}
	})
	connected := make(chan struct{}, 2)
	_ = client.On("connect", func(...any) { connected <- struct{}{} })
	client.Connect()
	waitFor(t, connected, "initial connection")

	if _, err = service.Emit(context.Background(), Target{
		Kind: TargetUser, Namespace: "/", ID: "user-1",
	}, "notification", "online"); err != nil {
		t.Fatal(err)
	}
	if value := waitFor(t, received, "online event"); value != "online" {
		t.Fatalf("unexpected online event %q", value)
	}
	eventually(t, func() bool {
		offset, _ := store.LastAck(context.Background(), "device-1")
		return offset == "1"
	}, "first ACK")

	client.Disconnect()
	if _, err = service.Emit(context.Background(), Target{
		Kind: TargetUser, Namespace: "/", ID: "user-1",
	}, "notification", "offline"); err != nil {
		t.Fatal(err)
	}
	client.Connect()
	waitFor(t, connected, "reconnection")
	if value := waitFor(t, received, "offline replay"); value != "offline" {
		t.Fatalf("unexpected replay %q", value)
	}
}

func waitFor[T any](t *testing.T, channel <-chan T, name string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(3 * time.Second):
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
