package config

import (
	"net/http"
	"testing"

	"github.com/aqcool/socket.io/v4/pkg/types"
)

type testWebSocketEngine struct{}

func (*testWebSocketEngine) Upgrade(
	http.ResponseWriter,
	*http.Request,
	http.Header,
	WebSocketUpgradeOptions,
) (types.WebSocketConnection, error) {
	return nil, nil
}

func TestWebSocketEngineOptionAssignment(t *testing.T) {
	engine := &testWebSocketEngine{}
	source := DefaultServerOptions()
	source.SetWebSocketEngine(engine)
	target := DefaultServerOptions()
	target.Assign(source)
	if target.WebSocketEngine() != engine {
		t.Fatal("custom WebSocket engine was not assigned")
	}
}
