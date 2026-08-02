package engine

import (
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/gorilla/websocket"
)

func TestOfficialServerRemoteAddressIsClientIP(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetAllowUpgrades(false)
			server := NewServer(options)
			remoteAddresses := make(chan string, 1)
			_ = server.On("connection", func(args ...any) {
				remoteAddresses <- args[0].(Socket).RemoteAddress()
			})
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			if transportName == "polling" {
				_, _ = engineHandshake(t, httpServer.URL)
			} else {
				wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
				connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
				if err != nil {
					t.Fatalf("WebSocket handshake: %v", err)
				}
				t.Cleanup(func() { _ = connection.Close() })
				if _, _, err := connection.ReadMessage(); err != nil {
					t.Fatalf("reading WebSocket OPEN: %v", err)
				}
			}

			select {
			case address := <-remoteAddresses:
				if net.ParseIP(address) == nil {
					t.Fatalf("RemoteAddress() = %q, want an IP without a port", address)
				}
			case <-time.After(time.Second):
				t.Fatal("connection event was not emitted")
			}
		})
	}
}
