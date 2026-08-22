package engine

import (
	"encoding/json"
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

type officialOpenPacket struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int64    `json:"pingInterval"`
	PingTimeout  int64    `json:"pingTimeout"`
	MaxPayload   int64    `json:"maxPayload"`
}

func decodeOfficialOpenPacket(t *testing.T, payload []byte) officialOpenPacket {
	t.Helper()
	if len(payload) == 0 || payload[0] != '0' {
		t.Fatalf("OPEN payload = %q", payload)
	}
	end := strings.IndexByte(string(payload), '\x1e')
	if end == -1 {
		end = len(payload)
	}
	var opened officialOpenPacket
	if err := json.Unmarshal(payload[1:end], &opened); err != nil {
		t.Fatalf("decoding OPEN payload %q: %v", payload, err)
	}
	return opened
}

func TestOfficialServerHandshakeDataAndUpgradeSuggestions(t *testing.T) {
	for _, test := range []struct {
		name         string
		configure    func(*config.ServerOptions)
		transport    string
		wantUpgrades []string
	}{
		{name: "default polling", transport: "polling", wantUpgrades: []string{"websocket"}},
		{name: "polling only", transport: "polling", configure: func(options *config.ServerOptions) {
			options.SetTransports(types.NewSet(Polling))
		}},
		{name: "upgrades disabled", transport: "polling", configure: func(options *config.ServerOptions) {
			options.SetAllowUpgrades(false)
		}},
		{name: "direct websocket", transport: "websocket", configure: func(options *config.ServerOptions) {
			options.SetTransports(types.NewSet(WebSocket))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetPingInterval(321 * time.Millisecond)
			options.SetPingTimeout(123 * time.Millisecond)
			if test.configure != nil {
				test.configure(options)
			}
			server := NewServer(options)
			connections := make(chan Socket, 1)
			_ = server.On("connection", func(args ...any) { connections <- args[0].(Socket) })
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			var opened officialOpenPacket
			if test.transport == "polling" {
				response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling")
				if err != nil {
					t.Fatalf("polling handshake: %v", err)
				}
				payload, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if readErr != nil {
					t.Fatalf("reading polling handshake: %v", readErr)
				}
				opened = decodeOfficialOpenPacket(t, payload)
			} else {
				connection, _, err := websocket.DefaultDialer.Dial(
					"ws"+strings.TrimPrefix(httpServer.URL, "http")+"/engine.io/?EIO=4&transport=websocket",
					nil,
				)
				if err != nil {
					t.Fatalf("WebSocket handshake: %v", err)
				}
				defer func() { _ = connection.Close() }()
				_, payload, readErr := connection.ReadMessage()
				if readErr != nil {
					t.Fatalf("reading WebSocket OPEN: %v", readErr)
				}
				opened = decodeOfficialOpenPacket(t, payload)
			}

			if opened.SID == "" || opened.PingInterval != 321 || opened.PingTimeout != 123 || opened.MaxPayload != 1_000_000 {
				t.Fatalf("OPEN data = %#v", opened)
			}
			if strings.Join(opened.Upgrades, ",") != strings.Join(test.wantUpgrades, ",") {
				t.Fatalf("upgrades = %#v, want %#v", opened.Upgrades, test.wantUpgrades)
			}
			select {
			case socket := <-connections:
				if socket == nil || socket.Transport().Name() != test.transport {
					t.Fatalf("connection transport = %#v, want %s", socket, test.transport)
				}
			case <-time.After(time.Second):
				t.Fatal("connection event was not emitted")
			}
		})
	}
}

func TestOfficialServerHandshakePreservesArbitraryQueryData(t *testing.T) {
	server := NewServer(config.DefaultServerOptions())
	queryValues := make(chan map[string][]string, 1)
	_ = server.On("connection", func(args ...any) {
		queryValues <- args[0].(Socket).Request().Query().All()
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling&a=b&c=d")
	if err != nil {
		t.Fatalf("polling handshake: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	select {
	case query := <-queryValues:
		if strings.Join(query["a"], ",") != "b" || strings.Join(query["c"], ",") != "d" || strings.Join(query["EIO"], ",") != "4" {
			t.Fatalf("handshake query = %#v", query)
		}
	case <-time.After(time.Second):
		t.Fatal("connection query was not observed")
	}
}

func TestOfficialServerInitialPacketIsSentOnEveryHandshake(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetInitialPacket(strings.NewReader("faster!"))
	server := NewServer(options)
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	for connectionNumber := 1; connectionNumber <= 2; connectionNumber++ {
		response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling")
		if err != nil {
			t.Fatalf("handshake %d: %v", connectionNumber, err)
		}
		payload, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			t.Fatalf("reading handshake %d: %v", connectionNumber, readErr)
		}
		if !strings.Contains(string(payload), "\x1e4faster!") {
			t.Fatalf("handshake %d payload = %q, want initial packet", connectionNumber, payload)
		}
	}
}

func TestOfficialServerRejectsTransportMismatchWithoutClosingSession(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	server := NewServer(options)
	errorEvents := make(chan *types.ErrorMessage, 1)
	_ = server.On("connection_error", func(args ...any) {
		errorEvents <- args[0].(*types.ErrorMessage)
	})
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.On("message", func(messageArgs ...any) {
			if data, ok := messageArgs[0].(io.Reader); ok {
				socket.Send(data, nil, nil)
			}
		})
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling")
	if err != nil {
		t.Fatalf("polling handshake: %v", err)
	}
	payload, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("reading polling handshake: %v", readErr)
	}
	opened := decodeOfficialOpenPacket(t, payload)

	mismatchTarget := httpServer.URL + "/engine.io/?EIO=4&transport=websocket&sid=" + url.QueryEscape(opened.SID)
	mismatch, err := http.Get(mismatchTarget)
	if err != nil {
		t.Fatalf("transport mismatch request: %v", err)
	}
	mismatchBody, readErr := io.ReadAll(mismatch.Body)
	_ = mismatch.Body.Close()
	if readErr != nil {
		t.Fatalf("reading transport mismatch response: %v", readErr)
	}
	var codeMessage types.CodeMessage
	if decodeErr := json.Unmarshal(mismatchBody, &codeMessage); decodeErr != nil {
		t.Fatalf("decoding transport mismatch response %q: %v", mismatchBody, decodeErr)
	}
	if mismatch.StatusCode != http.StatusBadRequest || codeMessage.Code != 3 || codeMessage.Message != "Bad request" {
		t.Fatalf("transport mismatch response = %d/%#v", mismatch.StatusCode, codeMessage)
	}
	select {
	case event := <-errorEvents:
		if event.Context["name"] != "TRANSPORT_MISMATCH" || event.Context["transport"] != "websocket" || event.Context["previousTransport"] != "polling" {
			t.Fatalf("transport mismatch context = %#v", event.Context)
		}
	case <-time.After(time.Second):
		t.Fatal("transport mismatch connection_error was not emitted")
	}
	if _, ok := server.Clients().Load(opened.SID); !ok {
		t.Fatal("transport mismatch closed the existing polling session")
	}

	pollingTarget := httpServer.URL + "/engine.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(opened.SID)
	post, err := http.Post(pollingTarget, "text/plain;charset=UTF-8", strings.NewReader("4echo"))
	if err != nil {
		t.Fatalf("sending message after transport mismatch: %v", err)
	}
	postBody, readErr := io.ReadAll(post.Body)
	_ = post.Body.Close()
	if readErr != nil || post.StatusCode != http.StatusOK || string(postBody) != "ok" {
		t.Fatalf("post-mismatch upload = %d/%q, read error %v", post.StatusCode, postBody, readErr)
	}
	poll, err := http.Get(pollingTarget)
	if err != nil {
		t.Fatalf("polling after transport mismatch: %v", err)
	}
	echo, readErr := io.ReadAll(poll.Body)
	_ = poll.Body.Close()
	if readErr != nil || string(echo) != "4echo" {
		t.Fatalf("post-mismatch echo = %q, read error %v", echo, readErr)
	}
}

func TestOfficialServerRejectsWebSocketTransportWithoutUpgrade(t *testing.T) {
	response, body, event := performVerificationRequest(
		t,
		config.DefaultServerOptions(),
		http.MethodGet,
		"/engine.io/?EIO=4&transport=websocket",
	)
	if response.StatusCode != http.StatusBadRequest || body.Code != 3 || body.Message != "Bad request" {
		t.Fatalf("handshake error response = %d/%#v", response.StatusCode, body)
	}
	if event.Context["name"] != "TRANSPORT_HANDSHAKE_ERROR" {
		t.Fatalf("handshake error context = %#v", event.Context)
	}
}
