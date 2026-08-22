package engine

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/v4/pkg/types"
	gorillaws "github.com/gorilla/websocket"
)

func TestOfficialClientExtraHeadersReachServer(t *testing.T) {
	for _, transportName := range []string{"websocket", "polling"} {
		t.Run(transportName, func(t *testing.T) {
			receivedHeaders := make(chan http.Header, 1)
			var first atomic.Bool
			first.Store(true)
			handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if first.CompareAndSwap(true, false) {
					receivedHeaders <- request.Header.Clone()
				}
				if transportName == "websocket" {
					connection, err := (&gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(writer, request, nil)
					if err != nil {
						return
					}
					defer func() { _ = connection.Close() }()
					_ = connection.WriteMessage(gorillaws.TextMessage, []byte(`0{"sid":"official","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}`))
					_, _, _ = connection.ReadMessage()
					return
				}
				writer.Header().Set("Content-Type", "text/plain; charset=UTF-8")
				if request.Method == http.MethodPost {
					_, _ = writer.Write([]byte("ok"))
					return
				}
				if request.URL.Query().Get("sid") == "" {
					_, _ = writer.Write([]byte(`0{"sid":"official","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}`))
					return
				}
				_, _ = writer.Write([]byte("6"))
			})
			httpServer := httptest.NewServer(handler)
			t.Cleanup(httpServer.Close)

			options := DefaultSocketOptions()
			options.SetUpgrade(false)
			if transportName == "websocket" {
				options.SetTransports(types.NewSet[TransportCtor](&WebSocketBuilder{}))
			} else {
				options.SetTransports(types.NewSet[TransportCtor](&PollingBuilder{}))
			}
			options.SetExtraHeaders(http.Header{
				"X-Custom-Header-For-My-Project": []string{"my-secret-access-token"},
				"Cookie":                         []string{"user_session=official-session"},
			})
			client := NewSocket(httpServer.URL, options)
			t.Cleanup(func() { client.Close() })
			opened := make(chan struct{}, 1)
			_ = client.Once("open", func(...any) { opened <- struct{}{} })

			select {
			case headers := <-receivedHeaders:
				if got := headers.Get("X-Custom-Header-For-My-Project"); got != "my-secret-access-token" {
					t.Fatalf("custom header = %q", got)
				}
				if got := headers.Get("Cookie"); !strings.Contains(got, "user_session=official-session") {
					t.Fatalf("Cookie = %q", got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("server did not receive initial request headers")
			}
			select {
			case <-opened:
			case <-time.After(2 * time.Second):
				t.Fatal("client did not open")
			}
		})
	}
}

func TestOfficialClientTransportSpecificExtraHeadersReachServer(t *testing.T) {
	received := make(chan http.Header, 1)
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case received <- request.Header.Clone():
		default:
		}
		writer.Header().Set("Content-Type", "text/plain; charset=UTF-8")
		if request.Method == http.MethodPost {
			_, _ = writer.Write([]byte("ok"))
			return
		}
		if request.URL.Query().Get("sid") == "" {
			_, _ = writer.Write([]byte(officialClientHandshake))
			return
		}
		_, _ = writer.Write([]byte("6"))
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	transportOptions := DefaultSocketOptions()
	transportOptions.SetExtraHeaders(http.Header{"X-Transport-Specific": {"official"}})
	options := DefaultSocketOptions()
	options.SetTransportList([]TransportCtor{&PollingBuilder{}})
	options.SetUpgrade(false)
	options.SetTransportOptions(map[string]SocketOptionsInterface{
		"polling": transportOptions,
	})
	client := NewSocket(server.URL, options)
	t.Cleanup(func() { client.Close() })

	select {
	case headers := <-received:
		if got := headers.Get("X-Transport-Specific"); got != "official" {
			t.Fatalf("transport-specific header = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive transport-specific headers")
	}
}
