package engine

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/aqcool/socket.io/servers/engine/v3/transports"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/gorilla/websocket"
)

func TestOfficialEngineEntryPointsAndOptions(t *testing.T) {
	if Protocol != 4 {
		t.Fatalf("Protocol = %d, want 4", Protocol)
	}

	server := NewServer(nil)
	if server == nil || !server.Transports().Has(transports.WEBSOCKET) {
		t.Fatal("NewServer(nil) did not create a server with a WebSocket engine")
	}
	server.Close()

	cors := &types.Cors{Origin: "https://example.com"}
	options := config.DefaultServerOptions()
	options.SetCors(cors)
	configured := NewServer(options)
	if configured.Opts().Cors() != cors {
		t.Fatal("NewServer did not preserve the supplied options")
	}
	configured.Close()

	for _, construct := range []struct {
		name string
		make func(*types.HttpServer) Server
	}{
		{name: "New with HTTP server", make: func(httpServer *types.HttpServer) Server { return New(httpServer) }},
		{name: "Attach", make: func(httpServer *types.HttpServer) Server { return Attach(httpServer, nil) }},
	} {
		t.Run(construct.name, func(t *testing.T) {
			httpServer := types.NewWebServer(http.NotFoundHandler())
			engineServer := construct.make(httpServer)
			if engineServer == nil {
				t.Fatal("entry point returned nil")
			}
			engineServer.Close()
		})
	}
}

func TestOfficialEngineListenUses501Fallback(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving address: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()

	server := Listen(address, nil, nil)
	t.Cleanup(func() {
		server.Close()
		if server.HttpServer() != nil {
			_ = server.HttpServer().Close(nil)
		}
	})

	var response *http.Response
	deadline := time.Now().Add(2 * time.Second)
	for {
		response, err = http.Get("http://" + address + "/")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Listen did not start HTTP server: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotImplemented {
		t.Fatalf("fallback status = %d, want 501", response.StatusCode)
	}
}

func TestOfficialEngineAttachHandlesItsPathAndPreservesFallback(t *testing.T) {
	var fallbackRequests int
	httpServer := types.NewWebServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackRequests++
		w.WriteHeader(http.StatusOK)
	}))
	server := Attach(httpServer, nil)
	transport := httptest.NewServer(httpServer)
	t.Cleanup(func() {
		server.Close()
		transport.Close()
	})

	response, err := http.Get(transport.URL + "/engine.io/default/")
	if err != nil {
		t.Fatalf("Engine.IO request: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("reading Engine.IO error: %v", readErr)
	}
	var codeMessage types.CodeMessage
	if decodeErr := json.Unmarshal(body, &codeMessage); decodeErr != nil {
		t.Fatalf("decoding Engine.IO error %q: %v", body, decodeErr)
	}
	if response.StatusCode != http.StatusBadRequest || codeMessage.Code != 0 || codeMessage.Message != "Transport unknown" {
		t.Fatalf("Engine.IO status/body = %d/%#v", response.StatusCode, codeMessage)
	}
	if fallbackRequests != 0 {
		t.Fatalf("fallback handled Engine.IO path %d times", fallbackRequests)
	}

	response, err = http.Get(transport.URL + "/test")
	if err != nil {
		t.Fatalf("fallback request: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || fallbackRequests != 1 {
		t.Fatalf("fallback status/count = %d/%d, want 200/1", response.StatusCode, fallbackRequests)
	}
}

func TestOfficialEngineAttachRejectsUnhandledUpgrade(t *testing.T) {
	httpServer := types.NewWebServer(http.NotFoundHandler())
	options := config.DefaultAttachOptions()
	options.SetDestroyUpgradeTimeout(50 * time.Millisecond)
	server := Attach(httpServer, options)
	transport := httptest.NewServer(httpServer)
	t.Cleanup(func() {
		server.Close()
		transport.Close()
	})

	connection, response, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(transport.URL, "http")+"/not-engine-io",
		nil,
	)
	if connection != nil {
		_ = connection.Close()
	}
	if err == nil {
		t.Fatal("unhandled upgrade unexpectedly succeeded")
	}
	if response == nil {
		t.Fatal("unhandled upgrade did not receive an HTTP response")
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unhandled upgrade status = %d, want 404", response.StatusCode)
	}
}

func TestOfficialEngineAttachPreservesHandledUpgrade(t *testing.T) {
	httpServer := types.NewWebServer(http.NotFoundHandler())
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	httpServer.HandleFunc("/other-upgrade", func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		messageType, payload, err := connection.ReadMessage()
		if err == nil {
			_ = connection.WriteMessage(messageType, payload)
		}
	})
	options := config.DefaultAttachOptions()
	options.SetDestroyUpgradeTimeout(50 * time.Millisecond)
	server := Attach(httpServer, options)
	transport := httptest.NewServer(httpServer)
	t.Cleanup(func() {
		server.Close()
		transport.Close()
	})

	connection, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(transport.URL, "http")+"/other-upgrade",
		nil,
	)
	if err != nil {
		t.Fatalf("handled upgrade: %v", err)
	}
	defer func() { _ = connection.Close() }()
	time.Sleep(100 * time.Millisecond)
	if writeErr := connection.WriteMessage(websocket.TextMessage, []byte("still-open")); writeErr != nil {
		t.Fatalf("writing after destroy-upgrade timeout: %v", writeErr)
	}
	_, payload, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("reading handled upgrade echo: %v", err)
	}
	if string(payload) != "still-open" {
		t.Fatalf("handled upgrade echo = %q", payload)
	}
}

func TestOfficialEngineAttachWithoutTrailingSlashUsesPrefixRouting(t *testing.T) {
	httpServer := types.NewWebServer(http.NotFoundHandler())
	options := config.DefaultAttachOptions()
	options.SetAddTrailingSlash(false)
	server := Attach(httpServer, options)
	transport := httptest.NewServer(httpServer)
	t.Cleanup(func() {
		server.Close()
		transport.Close()
	})

	for _, path := range []string{"/engine.io", "/engine.io/foo/bar/"} {
		response, err := http.Get(transport.URL + path + "?EIO=4&transport=polling")
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, response.StatusCode)
		}
	}
}
