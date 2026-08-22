package engine

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/gorilla/websocket"
)

type recordingWebSocketEngine struct {
	called chan config.WebSocketUpgradeOptions
}

func (engine *recordingWebSocketEngine) Upgrade(
	writer http.ResponseWriter,
	request *http.Request,
	headers http.Header,
	options config.WebSocketUpgradeOptions,
) (types.WebSocketConnection, error) {
	engine.called <- options
	return (&GorillaWebSocketEngine{}).Upgrade(writer, request, headers, options)
}

func TestOfficialServerUsesConfiguredWebSocketEngine(t *testing.T) {
	recorder := &recordingWebSocketEngine{called: make(chan config.WebSocketUpgradeOptions, 1)}
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	options.SetWebSocketEngine(recorder)
	server := NewServer(options)
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("custom-engine WebSocket handshake: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, _, err := connection.ReadMessage(); err != nil {
		t.Fatalf("reading custom-engine OPEN: %v", err)
	}
	select {
	case upgradeOptions := <-recorder.called:
		if upgradeOptions.ReadBufferSize != DefaultWSReadBufferSize || upgradeOptions.WriteBufferSize != DefaultWSWriteBufferSize {
			t.Fatalf("custom engine buffer options = %d/%d", upgradeOptions.ReadBufferSize, upgradeOptions.WriteBufferSize)
		}
	case <-time.After(time.Second):
		t.Fatal("configured WebSocket engine was not invoked")
	}
}
