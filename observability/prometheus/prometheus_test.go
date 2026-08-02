package prometheus

import (
	"net/http/httptest"
	"testing"
	"time"

	client "github.com/aqcool/socket.io/clients/socket/v3"
	socket "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestServerAndClientMetrics(t *testing.T) {
	registry := prom.NewRegistry()
	collector, err := New(&Options{Registerer: registry, Prefix: "test_socketio"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(collector.Close)

	server := socket.NewServer(nil, nil)
	if err = collector.InstrumentServer(server); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	server.EmitReserved("telemetry", socket.TelemetryEvent{
		Kind: socket.TelemetryConnection, Namespace: "/chat", SocketID: "private-id",
		Transport: "websocket", Recovered: true, Success: true, At: now,
	})
	server.EmitReserved("telemetry", socket.TelemetryEvent{
		Kind: socket.TelemetryEventReceived, Namespace: "/chat", SocketID: "private-id",
		Transport: "websocket", Event: "message", Bytes: 42,
		Duration: time.Millisecond, Success: true, At: now,
	})
	server.EmitReserved("telemetry", socket.TelemetryEvent{
		Kind: socket.TelemetryAckTimeout, Namespace: "/chat", SocketID: "private-id",
		Transport: "websocket", Event: "message", Duration: time.Second, At: now,
	})
	server.Sockets().Adapter().Emit("adapter_operation", socket.AdapterTelemetryEvent{
		Operation: "publish", Namespace: "/", Duration: time.Millisecond, Success: true, At: now,
	})

	managerOptions := client.DefaultManagerOptions()
	managerOptions.SetAutoConnect(false)
	manager := client.NewManager("http://unused.invalid", managerOptions)
	clientSocket := client.NewSocket(manager, "/chat", client.DefaultOptions())
	if err = collector.InstrumentClient(clientSocket); err != nil {
		t.Fatal(err)
	}
	manager.Emit("reconnect_attempt", 1)
	clientSocket.EventEmitter.Emit("overflow", &client.OverflowDetails{
		Buffer: "send", Strategy: client.OverflowReject,
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	required := map[string]bool{
		"test_socketio_connections_total":                  false,
		"test_socketio_events_total":                       false,
		"test_socketio_acks_total":                         false,
		"test_socketio_adapter_operation_duration_seconds": false,
		"test_socketio_client_reconnects_total":            false,
		"test_socketio_client_buffer_overflows_total":      false,
	}
	for _, family := range families {
		if _, ok := required[family.GetName()]; ok {
			required[family.GetName()] = true
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "socket_id" || label.GetValue() == "private-id" {
					t.Fatalf("high-cardinality Socket ID leaked into %s", family.GetName())
				}
			}
		}
	}
	for name, found := range required {
		if !found {
			t.Errorf("metric %s was not exported", name)
		}
	}
}

func TestDuplicateMetricsRegistrationReturnsError(t *testing.T) {
	registry := prom.NewRegistry()
	first, err := New(&Options{Registerer: registry})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	if second, secondErr := New(&Options{Registerer: registry}); secondErr == nil || second != nil {
		t.Fatal("expected duplicate registration error")
	}
}

func TestRuntimeTelemetryHooks(t *testing.T) {
	registry := prom.NewRegistry()
	collector, err := New(&Options{Registerer: registry, Prefix: "runtime_socketio"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(collector.Close)
	server := socket.NewServer(nil, nil)
	if err = collector.InstrumentServer(server); err != nil {
		t.Fatal(err)
	}
	_ = server.On("connection", func(args ...any) {
		s := args[0].(*socket.Socket)
		_ = s.On("ping", func(eventArgs ...any) {
			if ack, ok := eventArgs[len(eventArgs)-1].(socket.Ack); ok {
				ack([]any{"pong"}, nil)
			}
		})
	})
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.CloseClientConnections()
		httpServer.Close()
	})

	options := client.DefaultOptions()
	options.SetTransports(types.NewSet(client.WebSocket))
	options.SetAutoConnect(false)
	clientSocket, err := client.Connect(httpServer.URL+"/", options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clientSocket.Close() })
	connected := make(chan struct{}, 1)
	acknowledged := make(chan struct{}, 1)
	_ = clientSocket.On("connect", func(...any) { signal(connected) })
	clientSocket.Connect()
	wait(t, connected, "connect")
	clientSocket.EmitWithAck("ping")(func(_ []any, ackErr error) {
		if ackErr != nil {
			t.Errorf("ack: %v", ackErr)
		}
		signal(acknowledged)
	})
	wait(t, acknowledged, "ack")
	clientSocket.Disconnect()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		families, gatherErr := registry.Gather()
		if gatherErr != nil {
			t.Fatal(gatherErr)
		}
		if metricValue(families, "runtime_socketio_events_total") >= 1 &&
			metricValue(families, "runtime_socketio_acks_total") >= 1 &&
			metricValue(families, "runtime_socketio_disconnections_total") >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("runtime telemetry metrics were not observed")
}

func metricValue(families []*dto.MetricFamily, name string) float64 {
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		var value float64
		for _, metric := range family.Metric {
			value += metric.GetCounter().GetValue()
		}
		return value
	}
	return 0
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func wait(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
