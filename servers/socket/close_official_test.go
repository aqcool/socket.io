package socket

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/v4/pkg/types"
)

func socketIOPollingHandshake(t *testing.T, baseURL string) string {
	t.Helper()

	response, err := http.Get(baseURL + "/socket.io/?EIO=4&transport=polling")
	if err != nil {
		t.Fatalf("polling handshake: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("reading handshake: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("handshake status/body = %d/%q", response.StatusCode, body)
	}

	const marker = `"sid":"`
	start := bytes.Index(body, []byte(marker))
	if start == -1 {
		t.Fatalf("handshake has no sid: %q", body)
	}
	start += len(marker)
	end := bytes.IndexByte(body[start:], '"')
	if end == -1 {
		t.Fatalf("invalid sid in handshake: %q", body)
	}
	return string(body[start : start+end])
}

func socketIOPollingPush(t *testing.T, baseURL, sid, payload string) {
	t.Helper()

	request, err := http.NewRequest(
		http.MethodPost,
		baseURL+"/socket.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid),
		strings.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("creating polling POST: %v", err)
	}
	request.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("polling POST: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("reading polling POST response: %v", readErr)
	}
	if response.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("polling POST status/body = %d/%q, want 200/ok", response.StatusCode, body)
	}
}

func socketIOPollingPoll(t *testing.T, baseURL, sid string) string {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(baseURL + "/socket.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(sid))
	if err != nil {
		t.Fatalf("polling GET: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("reading polling GET response: %v", readErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("polling GET status/body = %d/%q", response.StatusCode, body)
	}
	return string(body)
}

func newOfficialCloseTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()

	server := NewServer(nil, nil)
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})
	return server, httpServer
}

func TestOfficialCloseProtocolViolations(t *testing.T) {
	tests := []struct {
		name             string
		packets          []string
		wantConnectReply bool
	}{
		{
			name:             "several CONNECT packets",
			packets:          []string{"40", "40"},
			wantConnectReply: true,
		},
		{
			name:    "EVENT packet before CONNECT",
			packets: []string{`42["some event"]`},
		},
		{
			name:             "invalid Socket.IO packet",
			packets:          []string{"40", "4abc"},
			wantConnectReply: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, httpServer := newOfficialCloseTestServer(t)
			sid := socketIOPollingHandshake(t, httpServer.URL)
			for _, packet := range test.packets {
				socketIOPollingPush(t, httpServer.URL, sid, packet)
			}

			if test.wantConnectReply {
				connectReply := socketIOPollingPoll(t, httpServer.URL, sid)
				if !strings.HasPrefix(connectReply, `40{"sid":"`) {
					t.Fatalf("CONNECT reply = %q", connectReply)
				}
			}
			if closePayload := socketIOPollingPoll(t, httpServer.URL, sid); closePayload != "6\x1e1" {
				t.Fatalf("close payload = %q, want %q", closePayload, "6\x1e1")
			}
			if clients := server.Engine().ClientsCount(); clients != 0 {
				t.Fatalf("Engine.IO client count = %d, want 0", clients)
			}
		})
	}
}

func TestOfficialCloseStopsConnectedSocketsAndEngineClients(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40")
	_ = socketIOPollingPoll(t, httpServer.URL, sid)

	if sockets := server.Sockets().Sockets().Len(); sockets != 1 {
		t.Fatalf("Socket.IO socket count before close = %d, want 1", sockets)
	}
	if clients := server.Engine().ClientsCount(); clients != 1 {
		t.Fatalf("Engine.IO client count before close = %d, want 1", clients)
	}

	server.Close(nil)

	if sockets := server.Sockets().Sockets().Len(); sockets != 0 {
		t.Fatalf("Socket.IO socket count after close = %d, want 0", sockets)
	}
	if clients := server.Engine().ClientsCount(); clients != 0 {
		t.Fatalf("Engine.IO client count after close = %d, want 0", clients)
	}
}

func TestOfficialCloseWithHTTPServerThatIsNotRunning(t *testing.T) {
	httpServer := types.NewWebServer(nil)
	server := NewServer(httpServer, nil)
	var callbackErr error

	server.Close(func(closeErr error) {
		callbackErr = closeErr
	})

	if !errors.Is(callbackErr, types.ErrServerNotRunning) {
		t.Fatalf("Close callback error = %v, want ErrServerNotRunning", callbackErr)
	}

	// The no-callback form is the Go equivalent of the official Promise form:
	// it must not panic even when the underlying server is not running.
	server.Close(nil)
}

func TestOfficialCloseReleasesHTTPPort(t *testing.T) {
	tests := []struct {
		name  string
		start func(string) *Server
	}{
		{
			name: "attached HTTP server",
			start: func(address string) *Server {
				httpServer := types.NewWebServer(nil)
				server := NewServer(httpServer, nil)
				httpServer.Listen(address, nil)
				return server
			},
		},
		{
			name: "Socket.IO-created HTTP server",
			start: func(address string) *Server {
				return NewServer(address, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reserved, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("reserving TCP port: %v", err)
			}
			address := reserved.Addr().String()
			_ = reserved.Close()

			server := test.start(address)
			deadline := time.Now().Add(2 * time.Second)
			for {
				connection, dialErr := net.DialTimeout("tcp", address, 50*time.Millisecond)
				if dialErr == nil {
					_ = connection.Close()
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("HTTP server did not start: %v", dialErr)
				}
				time.Sleep(10 * time.Millisecond)
			}

			baseURL := "http://" + address
			sid := socketIOPollingHandshake(t, baseURL)
			socketIOPollingPush(t, baseURL, sid, "40")
			_ = socketIOPollingPoll(t, baseURL, sid)

			var closeErr error
			server.Close(func(err error) { closeErr = err })
			if closeErr != nil {
				t.Fatalf("closing Socket.IO server: %v", closeErr)
			}
			if sockets := server.Sockets().Sockets().Len(); sockets != 0 {
				t.Fatalf("Socket.IO socket count after close = %d, want 0", sockets)
			}

			rebound, listenErr := net.Listen("tcp", address)
			if listenErr != nil {
				t.Fatalf("HTTP port was not released: %v", listenErr)
			}
			_ = rebound.Close()
		})
	}
}
