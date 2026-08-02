package socket

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/gorilla/websocket"
)

func newOfficialPollingSocket(t *testing.T) (*httptest.Server, *Socket, string) {
	t.Helper()

	server, httpServer := newOfficialCloseTestServer(t)
	connected := make(chan *Socket, 1)
	_ = server.On("connection", func(args ...any) { connected <- args[0].(*Socket) })
	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40")
	if payload := socketIOPollingPoll(t, httpServer.URL, sid); !strings.HasPrefix(payload, `40{"sid":"`) {
		t.Fatalf("Socket.IO CONNECT packet = %q", payload)
	}

	select {
	case socket := <-connected:
		return httpServer, socket, sid
	case <-time.After(time.Second):
		t.Fatal("Socket.IO connection event was not emitted")
		return nil, nil, ""
	}
}

func assertOfficialPendingPoll(t *testing.T, cancel context.CancelFunc, result <-chan namespacePendingPollResult, want string) {
	t.Helper()
	defer cancel()
	select {
	case response := <-result:
		if response.err != nil || response.status != 200 || response.payload != want {
			t.Fatalf("Polling response = status %d, payload %q, err %v; want 200/%q", response.status, response.payload, response.err, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Polling response timed out; want %q", want)
	}
}

func TestOfficialSocketPollingVolatileBehavior(t *testing.T) {
	t.Run("regular then volatile", func(t *testing.T) {
		httpServer, socket, sid := newOfficialPollingSocket(t)
		if err := socket.Emit("ev", "data"); err != nil {
			t.Fatalf("regular emit: %v", err)
		}
		if err := socket.Volatile().Emit("ev", "data"); err != nil {
			t.Fatalf("volatile emit: %v", err)
		}
		if payload := socketIOPollingPoll(t, httpServer.URL, sid); payload != `42["ev","data"]` {
			t.Fatalf("Polling payload = %q, want one event", payload)
		}
	})

	t.Run("idle volatile", func(t *testing.T) {
		httpServer, socket, sid := newOfficialPollingSocket(t)
		cancel, result := startNamespacePendingPoll(t, httpServer.URL, sid, socket)
		if err := socket.Volatile().Emit("ev", "data"); err != nil {
			cancel()
			t.Fatalf("volatile emit: %v", err)
		}
		assertOfficialPendingPoll(t, cancel, result, `42["ev","data"]`)
	})

	t.Run("consecutive volatile", func(t *testing.T) {
		httpServer, socket, sid := newOfficialPollingSocket(t)
		cancel, result := startNamespacePendingPoll(t, httpServer.URL, sid, socket)
		if err := socket.Volatile().Emit("ev", "data"); err != nil {
			cancel()
			t.Fatalf("first volatile emit: %v", err)
		}
		if err := socket.Volatile().Emit("ev", "data"); err != nil {
			cancel()
			t.Fatalf("second volatile emit: %v", err)
		}
		assertOfficialPendingPoll(t, cancel, result, `42["ev","data"]`)
	})

	t.Run("regular after failed volatile", func(t *testing.T) {
		httpServer, socket, sid := newOfficialPollingSocket(t)
		if err := socket.Emit("ev", "data"); err != nil {
			t.Fatalf("first regular emit: %v", err)
		}
		if err := socket.Volatile().Emit("ev", "data"); err != nil {
			t.Fatalf("volatile emit: %v", err)
		}
		if err := socket.Emit("ev", "data"); err != nil {
			t.Fatalf("second regular emit: %v", err)
		}
		if payload := socketIOPollingPoll(t, httpServer.URL, sid); payload != "42[\"ev\",\"data\"]\x1e42[\"ev\",\"data\"]" {
			t.Fatalf("Polling payload = %q, want two regular events", payload)
		}
	})
}

func newOfficialWebSocketSocket(t *testing.T) (*Server, *Socket, *websocket.Conn) {
	return newOfficialWebSocketSocketWithRequest(t, "", nil)
}

func newOfficialWebSocketSocketWithRequest(t *testing.T, rawQuery string, headers http.Header) (*Server, *Socket, *websocket.Conn) {
	t.Helper()

	server := NewServer(nil, nil)
	connected := make(chan *Socket, 1)
	_ = server.On("connection", func(args ...any) { connected <- args[0].(*Socket) })
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/socket.io/?EIO=4&transport=websocket"
	if rawQuery != "" {
		wsURL += "&" + rawQuery
	}
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, headers)
	if err != nil {
		t.Fatalf("WebSocket handshake: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, payload, readErr := connection.ReadMessage(); readErr != nil || !strings.HasPrefix(string(payload), "0{") || !strings.Contains(string(payload), `"sid":"`) {
		t.Fatalf("Engine.IO OPEN packet = %q/%v", payload, readErr)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
		t.Fatalf("writing Socket.IO CONNECT: %v", err)
	}
	if _, payload, readErr := connection.ReadMessage(); readErr != nil || !strings.HasPrefix(string(payload), `40{"sid":"`) {
		t.Fatalf("Socket.IO CONNECT packet = %q/%v", payload, readErr)
	}

	var socket *Socket
	select {
	case socket = <-connected:
	case <-time.After(time.Second):
		t.Fatal("Socket.IO connection event was not emitted")
	}
	deadline := time.Now().Add(time.Second)
	for !socket.Conn().Transport().Writable() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !socket.Conn().Transport().Writable() {
		t.Fatal("WebSocket transport did not become writable")
	}
	return server, socket, connection
}

func connectOfficialWebSocketAt(t *testing.T, baseURL string, connected <-chan *Socket) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(baseURL, "http") + "/socket.io/?EIO=4&transport=websocket"
	deadline := time.Now().Add(2 * time.Second)
	var (
		connection *websocket.Conn
		err        error
	)
	for time.Now().Before(deadline) {
		connection, _, err = websocket.DefaultDialer.Dial(wsURL, nil)
		if err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("WebSocket handshake at %s: %v", wsURL, err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, payload, readErr := connection.ReadMessage(); readErr != nil || !strings.HasPrefix(string(payload), "0{") {
		t.Fatalf("Engine.IO OPEN packet = %q/%v", payload, readErr)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
		t.Fatalf("writing Socket.IO CONNECT: %v", err)
	}
	if _, payload, readErr := connection.ReadMessage(); readErr != nil || !strings.HasPrefix(string(payload), `40{"sid":"`) {
		t.Fatalf("Socket.IO CONNECT packet = %q/%v", payload, readErr)
	}
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("Socket.IO connection event was not emitted")
	}
	return connection
}

func TestOfficialSocketServerCanEmitAfterCloseAndRestart(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving restart port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("releasing restart port: %v", err)
	}

	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })
	connected := make(chan *Socket, 2)
	received := make(chan string, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(*Socket)
		_ = socket.On("ev", func(values ...any) { received <- values[0].(string) })
		connected <- socket
	})
	server.Listen(address, nil)
	first := connectOfficialWebSocketAt(t, "http://"+address, connected)

	closed := make(chan error, 1)
	server.Close(func(err error) { closed <- err })
	select {
	case closeErr := <-closed:
		if closeErr != nil {
			t.Fatalf("closing server before restart: %v", closeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server close before restart timed out")
	}
	_ = first.Close()

	server.Listen(address, nil)
	second := connectOfficialWebSocketAt(t, "http://"+address, connected)
	if err := second.WriteMessage(websocket.TextMessage, []byte(`42["ev","payload"]`)); err != nil {
		t.Fatalf("emitting after server restart: %v", err)
	}
	select {
	case value := <-received:
		if value != "payload" {
			t.Fatalf("event after restart = %q", value)
		}
	case <-time.After(time.Second):
		t.Fatal("event after server restart was not received")
	}
}

func TestOfficialSocketHandshakeAndAccessors(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Matrix-Header", "header-value")
	server, socket, connection := newOfficialWebSocketSocketWithRequest(
		t,
		"key1=1&key2=2&key2=3",
		headers,
	)

	if socket.Client() == nil || socket.Conn() == nil || socket.Request() == nil {
		t.Fatal("Socket client, connection and request accessors must be available")
	}
	if socket.Request() != socket.Client().Request() || socket.Conn() != socket.Client().Conn() {
		t.Fatal("Socket accessors do not reference the owning Client")
	}
	handshake := socket.Handshake()
	if got := handshake.Headers["x-matrix-header"]; got != "header-value" {
		t.Fatalf("handshake header = %#v, want header-value string", got)
	}
	if got := handshake.Query["EIO"]; got != "4" {
		t.Fatalf("handshake EIO query = %#v, want string 4", got)
	}
	if got := handshake.Query["key1"]; got != "1" {
		t.Fatalf("handshake key1 query = %#v, want string 1", got)
	}
	key2, ok := handshake.Query["key2"].([]string)
	if !ok || len(key2) != 2 || key2[0] != "2" || key2[1] != "3" {
		t.Fatalf("handshake repeated key2 query = %#v, want [2 3]", handshake.Query["key2"])
	}
	if handshake.Address == "" || handshake.Url == "" || handshake.Issued == 0 || handshake.Time == "" {
		t.Fatalf("incomplete handshake metadata: %#v", handshake)
	}

	secondaryConnected := make(chan *Socket, 1)
	server.Of("/connection2", func(args ...any) { secondaryConnected <- args[0].(*Socket) })
	if err := connection.WriteMessage(websocket.TextMessage, []byte(`40/connection2,{"key1":"aa","key2":"&=bb"}`)); err != nil {
		t.Fatalf("writing secondary namespace CONNECT: %v", err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	_, payload, err := connection.ReadMessage()
	if err != nil || !strings.HasPrefix(string(payload), `40/connection2,{"sid":"`) {
		t.Fatalf("secondary namespace CONNECT packet = %q/%v", payload, err)
	}
	select {
	case secondary := <-secondaryConnected:
		if secondary.Handshake().Query["key1"] != "1" {
			t.Fatalf("secondary handshake query = %#v", secondary.Handshake().Query)
		}
		if secondary.Handshake().Auth["key1"] != "aa" || secondary.Handshake().Auth["key2"] != "&=bb" {
			t.Fatalf("secondary handshake auth = %#v", secondary.Handshake().Auth)
		}
	case <-time.After(time.Second):
		t.Fatal("secondary namespace did not connect")
	}
}

func TestOfficialSocketHandshakeWithoutEngineRequest(t *testing.T) {
	auth := map[string]any{"token": "webtransport"}
	handshake := buildHandshake(nil, "127.0.0.1:443", auth)
	if !handshake.Secure || handshake.Address != "127.0.0.1:443" {
		t.Fatalf("requestless handshake secure/address = %t/%q", handshake.Secure, handshake.Address)
	}
	if handshake.Headers == nil || len(handshake.Headers) != 0 || handshake.Query == nil || len(handshake.Query) != 0 {
		t.Fatalf("requestless handshake headers/query = %#v/%#v, want empty maps", handshake.Headers, handshake.Query)
	}
	if handshake.Auth["token"] != "webtransport" {
		t.Fatalf("requestless handshake auth = %#v", handshake.Auth)
	}
}

func assertSingleOfficialBinaryVolatileEvent(t *testing.T, connection *websocket.Conn) {
	t.Helper()
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	messageType, header, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("reading binary event header: %v", err)
	}
	if messageType != websocket.TextMessage || !strings.HasPrefix(string(header), `451-["ev",`) || !strings.Contains(string(header), `"_placeholder":true`) {
		t.Fatalf("binary event header = type %d, payload %q", messageType, header)
	}
	messageType, attachment, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("reading binary event attachment: %v", err)
	}
	if messageType != websocket.BinaryMessage || (!bytes.Equal(attachment, []byte{1, 2, 3}) && !bytes.Equal(attachment, []byte{4, 1, 2, 3})) {
		t.Fatalf("binary attachment = type %d, payload %v", messageType, attachment)
	}

	_ = connection.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	_, payload, err := connection.ReadMessage()
	if err == nil {
		t.Fatalf("unexpected second binary volatile event: %q", payload)
	}
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("checking for second binary volatile event: %v", err)
	}
}

func TestOfficialSocketWebSocketConsecutiveBinaryVolatileBehavior(t *testing.T) {
	t.Run("socket emit", func(t *testing.T) {
		_, socket, connection := newOfficialWebSocketSocket(t)
		time.Sleep(20 * time.Millisecond)
		if err := socket.Volatile().Emit("ev", types.NewBytesBuffer([]byte{1, 2, 3})); err != nil {
			t.Fatalf("first binary volatile emit: %v", err)
		}
		if err := socket.Volatile().Emit("ev", types.NewBytesBuffer([]byte{4, 5, 6})); err != nil {
			t.Fatalf("second binary volatile emit: %v", err)
		}
		assertSingleOfficialBinaryVolatileEvent(t, connection)
	})

	t.Run("server broadcast", func(t *testing.T) {
		server, _, connection := newOfficialWebSocketSocket(t)
		time.Sleep(20 * time.Millisecond)
		if err := server.Volatile().Emit("ev", types.NewBytesBuffer([]byte{1, 2, 3})); err != nil {
			t.Fatalf("first binary volatile broadcast: %v", err)
		}
		if err := server.Volatile().Emit("ev", types.NewBytesBuffer([]byte{4, 5, 6})); err != nil {
			t.Fatalf("second binary volatile broadcast: %v", err)
		}
		assertSingleOfficialBinaryVolatileEvent(t, connection)
	})
}

func TestOfficialSocketWebSocketVolatileBehavior(t *testing.T) {
	tests := []struct {
		name   string
		settle bool
		emit   func(*Socket) error
		want   int
	}{
		{
			name: "regular then volatile",
			emit: func(socket *Socket) error {
				if err := socket.Emit("ev", "data"); err != nil {
					return err
				}
				return socket.Volatile().Emit("ev", "data")
			},
			want: 1,
		},
		{
			name:   "idle volatile",
			settle: true,
			emit: func(socket *Socket) error {
				return socket.Volatile().Emit("ev", "data")
			},
			want: 1,
		},
		{
			name:   "consecutive volatile",
			settle: true,
			emit: func(socket *Socket) error {
				if err := socket.Volatile().Emit("ev", "data"); err != nil {
					return err
				}
				return socket.Volatile().Emit("ev", "data")
			},
			want: 1,
		},
		{
			name: "regular after failed volatile",
			emit: func(socket *Socket) error {
				if err := socket.Emit("ev", "data"); err != nil {
					return err
				}
				if err := socket.Volatile().Emit("ev", "data"); err != nil {
					return err
				}
				return socket.Emit("ev", "data")
			},
			want: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, socket, connection := newOfficialWebSocketSocket(t)
			if test.settle {
				time.Sleep(20 * time.Millisecond)
			}
			if err := test.emit(socket); err != nil {
				t.Fatalf("emitting events: %v", err)
			}
			for index := range test.want {
				_ = connection.SetReadDeadline(time.Now().Add(time.Second))
				_, payload, err := connection.ReadMessage()
				if err != nil {
					t.Fatalf("reading event %d: %v", index, err)
				}
				if string(payload) != `42["ev","data"]` {
					t.Fatalf("event %d payload = %q", index, payload)
				}
			}

			_ = connection.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
			_, payload, err := connection.ReadMessage()
			if err == nil {
				t.Fatalf("unexpected additional volatile event: %q", payload)
			}
			if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
				t.Fatalf("checking for extra event: %v", err)
			}
		})
	}
}

func TestOfficialSocketMalformedBinaryPackets(t *testing.T) {
	for _, payload := range []string{"45woooot", "45"} {
		t.Run(payload, func(t *testing.T) {
			_, socket, connection := newOfficialWebSocketSocket(t)
			errors := make(chan error, 1)
			_ = socket.Once("error", func(args ...any) {
				err, _ := args[0].(error)
				errors <- err
			})

			if err := connection.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
				t.Fatalf("writing malformed binary packet: %v", err)
			}
			select {
			case err := <-errors:
				if err == nil || err.Error() != "Illegal attachments" {
					t.Fatalf("Socket error = %v, want Illegal attachments", err)
				}
			case <-time.After(time.Second):
				t.Fatal("malformed binary packet did not emit a Socket error")
			}
		})
	}
}

func TestOfficialSocketErrorPacketWithoutApplicationHandlerDoesNotCrash(t *testing.T) {
	_, _, connection := newOfficialWebSocketSocket(t)
	if err := connection.WriteMessage(websocket.TextMessage, []byte(`444["handle me please"]`)); err != nil {
		t.Fatalf("writing Socket.IO ERROR packet: %v", err)
	}
	// The Socket installs the same no-op error listener as the official server,
	// so an application is not required to register one before receiving ERROR.
	// The protocol permits the invalid state to close this connection; the
	// official assertion is specifically that it must not panic the server.
	time.Sleep(100 * time.Millisecond)
}
