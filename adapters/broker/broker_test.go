package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	cluster "github.com/aqcool/socket.io/adapters/adapter/v4"
	socket "github.com/aqcool/socket.io/servers/socket/v4"
)

func TestBuilderDeclaresBrokerCapabilities(t *testing.T) {
	builder := &Builder{Options: Options{Ordered: true}}
	capabilities := builder.Capabilities()
	if !capabilities.Broadcast || !capabilities.RoomBroadcast || !capabilities.BroadcastAck ||
		!capabilities.FetchSockets || !capabilities.SocketManagement ||
		!capabilities.ServerSideEmit || !capabilities.NodeDiscovery ||
		!capabilities.OrderedDelivery || !capabilities.DuplicateSuppression ||
		!capabilities.ExternalEmitter {
		t.Fatalf("incomplete broker capabilities: %#v", capabilities)
	}
	if builder.SupportsConnectionStateRecovery() || capabilities.ConnectionStateRecovery {
		t.Fatal("generic broker must not claim connection state recovery")
	}
}

func TestMemoryBrokerPropagatesServerSideEmit(t *testing.T) {
	transport := NewMemoryBroker()
	builder := &Builder{Broker: transport, Options: Options{DeliverySemantics: AtLeastOnce}}
	firstOptions := socket.DefaultServerOptions()
	firstOptions.SetAdapter(builder)
	secondOptions := socket.DefaultServerOptions()
	secondOptions.SetAdapter(builder)
	first := socket.NewServer(nil, firstOptions)
	second := socket.NewServer(nil, secondOptions)
	t.Cleanup(func() {
		first.Close(nil)
		second.Close(nil)
	})

	received := make(chan string, 1)
	_ = second.On("cluster-event", func(args ...any) {
		if len(args) > 0 {
			if value, ok := args[0].(string); ok {
				received <- value
			}
		}
	})
	if err := first.Sockets().ServerSideEmit("cluster-event", "hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-received:
		if value != "hello" {
			t.Fatalf("unexpected value %q", value)
		}
	case <-time.After(time.Second):
		t.Fatal("server-side event was not propagated")
	}
}

func TestEmitterAndDuplicateSuppression(t *testing.T) {
	transport := NewMemoryBroker()
	builder := &Builder{Broker: transport, Options: Options{
		DeliverySemantics: AtLeastOnce,
		Ordered:           true,
	}}
	firstOptions := socket.DefaultServerOptions()
	firstOptions.SetAdapter(builder)
	server := socket.NewServer(nil, firstOptions)
	t.Cleanup(func() { server.Close(nil) })

	adapter := server.Sockets().Adapter().(*Adapter)
	emitter, err := NewEmitter(transport, "socket.io")
	if err != nil {
		t.Fatal(err)
	}
	external := make(chan string, 1)
	_ = server.On("external", func(args ...any) {
		if len(args) > 0 {
			if value, ok := args[0].(string); ok {
				external <- value
			}
		}
	})
	if err = emitter.ServerSideEmit("external", "hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-external:
		if value != "hello" {
			t.Fatalf("unexpected external value %q", value)
		}
	case <-time.After(time.Second):
		t.Fatal("external emitter event was not delivered")
	}

	var calls atomic.Int64
	_ = server.On("deduplicated", func(...any) { calls.Add(1) })
	payload, err := cluster.EncodeClusterMessage(&cluster.ClusterMessage{
		Uid:  "remote-node",
		Nsp:  "/",
		Type: cluster.SERVER_SIDE_EMIT,
		Data: &cluster.ServerSideEmitMessage{Packet: []any{"deduplicated"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	incoming := Message{ID: "same-delivery", Data: payload}
	if err = adapter.onMessage(context.Background(), incoming); err != nil {
		t.Fatal(err)
	}
	if err = adapter.onMessage(context.Background(), incoming); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("duplicate was dispatched %d times", calls.Load())
	}
}
