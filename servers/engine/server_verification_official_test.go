package engine

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/gorilla/websocket"
)

func TestOfficialServerWebSocketVerificationErrors(t *testing.T) {
	for _, test := range []struct {
		name        string
		transport   string
		configure   func(*config.ServerOptions)
		wantCode    int
		wantMessage string
		wantEvent   string
		wantContext map[string]any
	}{
		{
			name:      "AllowRequest rejection",
			transport: "websocket",
			configure: func(options *config.ServerOptions) {
				options.SetAllowRequest(func(*types.HttpContext) error { return errors.New("Thou shall not pass") })
			},
			wantCode:    4,
			wantMessage: "Thou shall not pass",
			wantEvent:   "Forbidden",
			wantContext: map[string]any{"message": "Thou shall not pass"},
		},
		{
			name:        "prototype transport",
			transport:   "__proto__",
			wantCode:    0,
			wantMessage: "Transport unknown",
			wantEvent:   "Transport unknown",
			wantContext: map[string]any{"transport": "__proto__"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := config.DefaultServerOptions()
			if test.configure != nil {
				test.configure(options)
			}
			server := NewServer(options)
			errorEvents := make(chan *types.ErrorMessage, 1)
			_ = server.On("connection_error", func(args ...any) {
				errorEvents <- args[0].(*types.ErrorMessage)
			})
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=" + url.QueryEscape(test.transport)
			connection, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if connection != nil {
				_ = connection.Close()
			}
			if err == nil || response == nil || response.StatusCode != http.StatusBadRequest {
				t.Fatalf("WebSocket rejection error/response = %v/%v", err, response)
			}
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil || string(body) != test.wantMessage {
				t.Fatalf("WebSocket rejection body = %q, error = %v", body, readErr)
			}
			select {
			case event := <-errorEvents:
				if event.Code != test.wantCode || event.Message != test.wantEvent {
					t.Fatalf("connection_error = %#v", event)
				}
				for key, want := range test.wantContext {
					if event.Context[key] != want {
						t.Fatalf("connection_error context[%q] = %#v, want %#v", key, event.Context[key], want)
					}
				}
			case <-time.After(time.Second):
				t.Fatal("connection_error was not emitted")
			}
			if server.ClientsCount() != 0 {
				t.Fatalf("rejected handshake registered %d clients", server.ClientsCount())
			}
		})
	}
}

func sendMalformedWebSocketHandshake(t *testing.T, serverURL, sid string) {
	t.Helper()
	target := "ws" + strings.TrimPrefix(serverURL, "http") + "/engine.io/?EIO=4&transport=websocket"
	if sid != "" {
		target += "&sid=" + url.QueryEscape(sid)
	}
	connection, _, err := websocket.DefaultDialer.Dial(target, nil)
	if err != nil {
		t.Fatalf("opening WebSocket before malformed frame: %v", err)
	}
	defer func() { _ = connection.Close() }()
	underlying := connection.UnderlyingConn()
	_ = underlying.SetWriteDeadline(time.Now().Add(time.Second))
	// These bytes are deliberately not a valid masked client WebSocket frame.
	if _, err := underlying.Write([]byte("test")); err != nil {
		t.Fatalf("writing malformed WebSocket frame: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
}

func TestOfficialServerSurvivesInvalidWebSocketHandshakeData(t *testing.T) {
	for _, test := range []struct {
		name        string
		existingSID bool
	}{
		{name: "direct WebSocket"},
		{name: "polling upgrade", existingSID: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer(config.DefaultServerOptions())
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			sid := ""
			if test.existingSID {
				_, sid = engineHandshake(t, httpServer.URL)
			}
			sendMalformedWebSocketHandshake(t, httpServer.URL, sid)
			if sid != "" {
				if _, ok := server.Clients().Load(sid); !ok {
					t.Fatal("malformed upgrade closed the existing polling session")
				}
			}
			response, _ := engineHandshake(t, httpServer.URL)
			if response.StatusCode != http.StatusOK {
				t.Fatalf("server did not accept a handshake after malformed data: %d", response.StatusCode)
			}
		})
	}
}

func TestOfficialServerPreventsSecondWebSocketUpgrade(t *testing.T) {
	server := NewServer(config.DefaultServerOptions())
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.On("message", func(messageArgs ...any) {
			data, ok := messageArgs[0].(io.Reader)
			if !ok {
				return
			}
			payload, err := io.ReadAll(data)
			if err == nil {
				socket.Send(strings.NewReader(string(payload)), nil, nil)
			}
		})
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})
	_, sid := engineHandshake(t, httpServer.URL)
	upgradeURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket&sid=" + url.QueryEscape(sid)

	first, _, err := websocket.DefaultDialer.Dial(upgradeURL, nil)
	if err != nil {
		t.Fatalf("first WebSocket upgrade: %v", err)
	}
	defer func() { _ = first.Close() }()
	if writeErr := first.WriteMessage(websocket.TextMessage, []byte("2probe")); writeErr != nil {
		t.Fatalf("writing upgrade probe: %v", writeErr)
	}
	_, probe, err := first.ReadMessage()
	if err != nil || string(probe) != "3probe" {
		t.Fatalf("upgrade probe response = %q, error = %v", probe, err)
	}
	if writeErr := first.WriteMessage(websocket.TextMessage, []byte("5")); writeErr != nil {
		t.Fatalf("completing first upgrade: %v", writeErr)
	}
	deadline := time.Now().Add(time.Second)
	for {
		socket, ok := server.Clients().Load(sid)
		if ok && socket.Upgraded() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first WebSocket upgrade did not complete")
		}
		time.Sleep(time.Millisecond)
	}

	second, _, err := websocket.DefaultDialer.Dial(upgradeURL, nil)
	if err != nil {
		t.Fatalf("second upgrade HTTP handshake: %v", err)
	}
	defer func() { _ = second.Close() }()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, readErr := second.ReadMessage(); readErr == nil {
		t.Fatal("second WebSocket upgrade remained open")
	}

	_ = first.SetReadDeadline(time.Now().Add(time.Second))
	if writeErr := first.WriteMessage(websocket.TextMessage, []byte("4echo")); writeErr != nil {
		t.Fatalf("writing on original upgraded transport: %v", writeErr)
	}
	_, echo, err := first.ReadMessage()
	if err != nil || string(echo) != "4echo" {
		t.Fatalf("original upgraded transport echo = %q, error = %v", echo, err)
	}
}

func TestOfficialServerRegistersNewClientBeforeConnectionEvent(t *testing.T) {
	server := NewServer(config.DefaultServerOptions())
	observed := make(chan string, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		if registered, ok := server.Clients().Load(socket.Id()); ok && registered == socket && server.ClientsCount() == 1 {
			observed <- socket.Id()
			return
		}
		observed <- ""
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})
	if server.ClientsCount() != 0 {
		t.Fatalf("initial clients count = %d", server.ClientsCount())
	}
	_, sid := engineHandshake(t, httpServer.URL)
	select {
	case registeredSID := <-observed:
		if registeredSID != sid {
			t.Fatalf("registered SID = %q, handshake SID = %q", registeredSID, sid)
		}
	case <-time.After(time.Second):
		t.Fatal("connection event was not emitted")
	}
}

func TestOfficialServerWaitsForGeneratedID(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	options := config.DefaultServerOptions()
	options.SetGenerateId(func(*types.HttpContext) (string, error) {
		close(started)
		<-release
		return "AwaitedCustomID", nil
	})
	server := NewServer(options)
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	type handshakeResult struct {
		status  int
		payload []byte
		err     error
	}
	results := make(chan handshakeResult, 1)
	go func() {
		response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling")
		if err != nil {
			results <- handshakeResult{err: err}
			return
		}
		payload, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		results <- handshakeResult{status: response.StatusCode, payload: payload, err: readErr}
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("ID generator was not called")
	}
	select {
	case result := <-results:
		t.Fatalf("handshake completed before ID generator: %#v", result)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case result := <-results:
		if result.err != nil || result.status != http.StatusOK {
			t.Fatalf("handshake after ID generation = %d/%v", result.status, result.err)
		}
		opened := decodeOfficialOpenPacket(t, result.payload)
		if opened.SID != "AwaitedCustomID" {
			t.Fatalf("generated SID = %q", opened.SID)
		}
	case <-time.After(time.Second):
		t.Fatal("handshake did not resume after ID generation")
	}
}

func TestOfficialServerRejectsFailedWebSocketUpgrade(t *testing.T) {
	server := NewServer(config.DefaultServerOptions())
	errorEvents := make(chan *types.ErrorMessage, 1)
	_ = server.On("connection_error", func(args ...any) {
		errorEvents <- args[0].(*types.ErrorMessage)
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	request, err := http.NewRequest(http.MethodGet, httpServer.URL+"/engine.io/?EIO=4&transport=websocket", nil)
	if err != nil {
		t.Fatalf("creating failed upgrade request: %v", err)
	}
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("failed WebSocket upgrade request: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("failed WebSocket upgrade status = %d, want 400", response.StatusCode)
	}
	select {
	case event := <-errorEvents:
		if event.Code != BAD_REQUEST.Code || event.Context["name"] != "UPGRADE_FAILURE" {
			t.Fatalf("failed upgrade connection_error = %#v context=%#v", event.CodeMessage, event.Context)
		}
	case <-time.After(time.Second):
		t.Fatal("failed upgrade connection_error was not emitted")
	}
	if server.ClientsCount() != 0 {
		t.Fatalf("failed upgrade registered %d clients", server.ClientsCount())
	}
	valid, _ := engineHandshake(t, httpServer.URL)
	if valid.StatusCode != http.StatusOK {
		t.Fatalf("server did not recover after failed upgrade: %d", valid.StatusCode)
	}
}
