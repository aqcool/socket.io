package adapter

import (
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/gorilla/websocket"
)

var errPublishFailed = errors.New("publish failed")

type rejectingPublishClusterAdapter struct {
	ClusterAdapter
	publishCalls atomic.Int32
}

func (a *rejectingPublishClusterAdapter) DoPublish(*ClusterMessage) (Offset, error) {
	a.publishCalls.Add(1)
	return "", errPublishFailed
}

func (*rejectingPublishClusterAdapter) DoPublishResponse(ServerId, *ClusterResponse) error {
	return nil
}

type rejectingPublishClusterAdapterBuilder struct {
	adapter *rejectingPublishClusterAdapter
}

func (b *rejectingPublishClusterAdapterBuilder) New(namespace socket.Namespace) Adapter {
	result := &rejectingPublishClusterAdapter{ClusterAdapter: NewClusterAdapter(namespace)}
	result.Prototype(result)
	b.adapter = result
	return result
}

func (*rejectingPublishClusterAdapterBuilder) SupportsConnectionStateRecovery() bool {
	return false
}

// Regression for socket.io-adapter@2.5.7:
// https://github.com/socketio/socket.io/commit/f6301588ca65de270ecfe22da9023d7ec79ba23a
func TestOfficial257BroadcastsToLocalClientWhenPublishAndReturnOffsetFails(t *testing.T) {
	builder := &rejectingPublishClusterAdapterBuilder{}
	options := socket.DefaultServerOptions()
	options.SetAdapter(builder)
	server := socket.NewServer(nil, options)
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})

	websocketURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") +
		"/socket.io/?EIO=4&transport=websocket"
	connection, _, err := websocket.DefaultDialer.Dial(websocketURL, nil)
	if err != nil {
		t.Fatalf("WebSocket handshake: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("setting WebSocket read deadline: %v", err)
	}
	if _, payload, readErr := connection.ReadMessage(); readErr != nil || !strings.HasPrefix(string(payload), "0{") {
		t.Fatalf("Engine.IO OPEN packet = %q/%v", payload, readErr)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
		t.Fatalf("writing Socket.IO CONNECT packet: %v", err)
	}
	if _, payload, readErr := connection.ReadMessage(); readErr != nil || !strings.HasPrefix(string(payload), `40{"sid":"`) {
		t.Fatalf("Socket.IO CONNECT packet = %q/%v", payload, readErr)
	}

	server.Emit("test", 1)
	if _, payload, readErr := connection.ReadMessage(); readErr != nil || string(payload) != `42["test",1]` {
		t.Fatalf("local broadcast after publish failure = %q/%v, want %q", payload, readErr, `42["test",1]`)
	}
	if builder.adapter == nil {
		t.Fatal("rejecting ClusterAdapter was not constructed")
	}
	if calls := builder.adapter.publishCalls.Load(); calls != 1 {
		t.Fatalf("DoPublish calls = %d, want 1", calls)
	}
}
