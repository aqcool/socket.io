package engine

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/v4/pkg/types"
	gorillaws "github.com/gorilla/websocket"
)

func TestOfficialClientPollingHTTPErrorDetails(t *testing.T) {
	for _, test := range []struct {
		name       string
		statusCode int
		body       string
		handshake  bool
		wantReason string
	}{
		{name: "unknown session", statusCode: http.StatusBadRequest, body: `{"code":1,"message":"Session ID unknown"}`, wantReason: "fetch read error"},
		{name: "max payload", statusCode: http.StatusRequestEntityTooLarge, body: "payload too large", handshake: true, wantReason: "fetch write error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var opened atomic.Bool
			var failedPost atomic.Bool
			handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if test.handshake {
					if request.Method == http.MethodGet {
						if opened.CompareAndSwap(false, true) {
							_, _ = writer.Write([]byte(officialClientHandshake))
							return
						}
						<-request.Context().Done()
						return
					}
					if !failedPost.CompareAndSwap(false, true) {
						writer.WriteHeader(http.StatusOK)
						return
					}
				}
				writer.WriteHeader(test.statusCode)
				_, _ = writer.Write([]byte(test.body))
			})
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)

			opts := DefaultSocketOptions()
			opts.SetTransportList([]TransportCtor{&PollingBuilder{}})
			opts.SetUpgrade(false)
			client := NewSocket(server.URL, opts)
			t.Cleanup(func() { client.Close() })
			seen := make(chan error, 1)
			_ = client.Once("error", func(args ...any) { seen <- args[0].(error) })
			if test.handshake {
				_ = client.Once("open", func(...any) {
					client.Send(types.NewStringBufferString(strings.Repeat("a", 101)), nil, nil)
				})
			}

			select {
			case err := <-seen:
				var transportError *Error
				if !errors.As(err, &transportError) {
					t.Fatalf("error type = %T, want *engine.Error", err)
				}
				if transportError.Type != "TransportError" || transportError.Message != test.wantReason {
					t.Fatalf("transport error = %#v", transportError)
				}
				var statusError *HTTPStatusError
				if !errors.As(transportError.Description, &statusError) {
					t.Fatalf("description type = %T, want *HTTPStatusError", transportError.Description)
				}
				if statusError.StatusCode != test.statusCode || string(statusError.Body) != test.body {
					t.Fatalf("HTTP details = %d/%q", statusError.StatusCode, statusError.Body)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("client did not emit polling transport error")
			}
		})
	}
}

func TestOfficialClientWebSocketHandshakeErrorIsTransportError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, `{"code":1,"message":"Session ID unknown"}`, http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)
	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&WebSocketBuilder{}})
	client := NewSocket(server.URL, opts)
	t.Cleanup(func() { client.Close() })
	seen := make(chan error, 1)
	_ = client.Once("error", func(args ...any) { seen <- args[0].(error) })
	select {
	case err := <-seen:
		var transportError *Error
		if !errors.As(err, &transportError) || transportError.Type != "TransportError" || transportError.Message != "websocket error" {
			t.Fatalf("error = %#v (%T)", err, err)
		}
		if transportError.Description == nil {
			t.Fatal("websocket handshake error lost its description")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not emit websocket handshake error")
	}
}

func TestOfficialClientWebSocketCloseDetails(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		if err := connection.WriteMessage(gorillaws.TextMessage, []byte(officialClientHandshake)); err != nil {
			return
		}
		_ = connection.WriteControl(
			gorillaws.CloseMessage,
			gorillaws.FormatCloseMessage(gorillaws.CloseMessageTooBig, "too large"),
			time.Now().Add(time.Second),
		)
	}))
	t.Cleanup(server.Close)
	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&WebSocketBuilder{}})
	client := NewSocket(server.URL, opts)
	t.Cleanup(func() { client.Close() })
	closed := make(chan []any, 1)
	_ = client.Once("close", func(args ...any) { closed <- args })
	select {
	case args := <-closed:
		if args[0].(string) != "transport close" {
			t.Fatalf("close reason = %q", args[0])
		}
		details, ok := args[1].(*Error)
		if !ok {
			t.Fatalf("close details = %T", args[1])
		}
		var closeError *gorillaws.CloseError
		if !errors.As(details.Description, &closeError) {
			t.Fatalf("close description = %T", details.Description)
		}
		if closeError.Code != gorillaws.CloseMessageTooBig || closeError.Text != "too large" {
			t.Fatalf("websocket close = %d/%q", closeError.Code, closeError.Text)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not preserve websocket close details")
	}
}
