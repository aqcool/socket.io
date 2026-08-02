package typed

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	socket "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

type request struct {
	ID string `json:"id"`
}

type response struct {
	OK bool `json:"ok"`
}

type messageRequest struct {
	Code   int    `json:"code"`
	Label  string `json:"label"`
	Values []int  `json:"values"`
}

type fakeEndpoint struct {
	types.EventEmitter
}

func (f *fakeEndpoint) Emit(name string, args ...any) error {
	f.EventEmitter.Emit(types.EventName(name), args...)
	return nil
}

func TestTypedHandleAndEmitAck(t *testing.T) {
	endpoint := &fakeEndpoint{EventEmitter: types.NewEventEmitter()}
	event := Event[request, response]{Name: "create"}
	if err := Handle(endpoint, event, func(_ context.Context, value request) (response, error) {
		return response{OK: value.ID == "42"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := EmitAck(context.Background(), endpoint, event, request{ID: "42"})
	if err != nil || !result.OK {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestEmitAckContextCancellation(t *testing.T) {
	endpoint := &fakeEndpoint{EventEmitter: types.NewEventEmitter()}
	event := Event[request, response]{Name: "never"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := EmitAck(ctx, endpoint, event, request{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
}

func TestTypedEmitSupportsFluentServerEmitter(t *testing.T) {
	server := socket.NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })
	event := Event[request, NoAck]{Name: "server-event", Direction: ServerToClient}
	if err := Emit(server, event, request{ID: "42"}); err != nil {
		t.Fatalf("typed Server emit: %v", err)
	}
}

func TestTypedReservedDisconnectReason(t *testing.T) {
	endpoint := &fakeEndpoint{EventEmitter: types.NewEventEmitter()}
	reasons := make(chan DisconnectReason, 1)
	if err := On(endpoint, DisconnectEvent, func(_ context.Context, reason DisconnectReason) error {
		reasons <- reason
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	endpoint.EventEmitter.Emit("disconnect", string(DisconnectTransportClose))
	select {
	case reason := <-reasons:
		if reason != DisconnectTransportClose {
			t.Fatalf("disconnect reason = %q", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("typed disconnect listener was not invoked")
	}
}

func TestTypedReservedConnectEvents(t *testing.T) {
	endpoint := &fakeEndpoint{EventEmitter: types.NewEventEmitter()}
	connected := make(chan struct{}, 1)
	connectErrors := make(chan error, 1)
	if err := OnConnect(endpoint, func(context.Context) error {
		connected <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := OnConnectError(endpoint, func(_ context.Context, err error) error {
		connectErrors <- err
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("namespace rejected")
	endpoint.EventEmitter.Emit("connect")
	endpoint.EventEmitter.Emit("connect_error", wantErr)
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("typed connect listener was not invoked")
	}
	select {
	case got := <-connectErrors:
		if !errors.Is(got, wantErr) {
			t.Fatalf("connect error = %v, want %v", got, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("typed connect_error listener was not invoked")
	}
}

func TestGenerateAllArtifacts(t *testing.T) {
	event := Event[request, response]{
		Name: "create-order", Direction: ClientToServer, Description: "创建订单",
	}
	serverAckEvent := Event[request, response]{
		Name: "confirm-order", Direction: ServerToClient, Description: "确认订单",
	}
	serverEvent := Event[request, NoAck]{
		Name: "order-updated", Direction: ServerToClient, Description: "订单更新",
	}
	clusterAckEvent := Event[request, response]{
		Name: "sync-order", Direction: ServerToServer, Description: "同步订单",
	}
	messageEvent := Event[messageRequest, NoAck]{
		Name: "message", Direction: ServerToClient, Description: "标准 message 事件",
	}
	files, err := Generate([]Namespace{NewNamespace("/orders",
		event.Definition(),
		serverAckEvent.Definition(),
		serverEvent.Definition(),
		clusterAckEvent.Definition(),
		messageEvent.Definition(),
	)}, GenerateOptions{Package: "events"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"socketio_client.gen.go", "socketio_server.gen.go", "socketio_events.gen.ts", "socketio_events.md"} {
		if len(files[name]) == 0 {
			t.Errorf("missing %s", name)
		}
	}
	if !strings.Contains(string(files["socketio_events.gen.ts"]), "interface request") ||
		!strings.Contains(string(files["socketio_server.gen.go"]), "HandleCreateOrder") ||
		!strings.Contains(string(files["socketio_server.gen.go"]), "func SendConfirmOrder(ctx context.Context") ||
		!strings.Contains(string(files["socketio_client.gen.go"]), "func HandleConfirmOrder(") ||
		!strings.Contains(string(files["socketio_server.gen.go"]), "func SendOrderUpdated(") ||
		!strings.Contains(string(files["socketio_client.gen.go"]), "func OnOrderUpdated(") ||
		!strings.Contains(string(files["socketio_server.gen.go"]), "func BroadcastSyncOrder(ctx context.Context") ||
		!strings.Contains(string(files["socketio_server.gen.go"]), "func SendMessage(emitter sockettyped.Emitter, request messageRequest)") ||
		strings.Contains(string(files["socketio_client.gen.go"]), "EmitConfirmOrder") ||
		strings.Contains(string(files["socketio_server.gen.go"]), "HandleConfirmOrder") {
		t.Fatal("generated artifacts do not contain typed definitions")
	}
}
