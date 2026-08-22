package engine

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/gorilla/websocket"
)

const officialMiddlewarePollingPath = "/engine.io/?EIO=4&transport=polling"

func newOfficialMiddlewareServer(t *testing.T) (Server, *httptest.Server) {
	t.Helper()
	server := NewServer(config.DefaultServerOptions())
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})
	return server, httpServer
}

func officialMiddlewareWebSocketURL(baseURL string) string {
	return "ws" + strings.TrimPrefix(baseURL, "http") + "/engine.io/?EIO=4&transport=websocket"
}

func TestOfficialEngineMiddlewareAppliesToPollingAndWebSocket(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			server, httpServer := newOfficialMiddlewareServer(t)
			server.Use(func(ctx *types.HttpContext, next func(error)) {
				ctx.ResponseHeaders().Set("Foo", "bar")
				next(nil)
			})

			if transportName == "polling" {
				response, err := http.Get(httpServer.URL + officialMiddlewarePollingPath)
				if err != nil {
					t.Fatalf("polling handshake: %v", err)
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if response.StatusCode != http.StatusOK || response.Header.Get("Foo") != "bar" {
					t.Fatalf("polling status/header = %d/%q, want 200/bar", response.StatusCode, response.Header.Get("Foo"))
				}
				return
			}

			connection, response, err := websocket.DefaultDialer.Dial(officialMiddlewareWebSocketURL(httpServer.URL), nil)
			if err != nil {
				t.Fatalf("WebSocket handshake: %v", err)
			}
			_ = connection.Close()
			if response == nil || response.Header.Get("Foo") != "bar" {
				t.Fatalf("WebSocket upgrade header = %q, want bar", response.Header.Get("Foo"))
			}
		})
	}
}

func TestOfficialEngineMiddlewareRunsAllFunctionsInOrder(t *testing.T) {
	server, httpServer := newOfficialMiddlewareServer(t)
	var nextIndex atomic.Int64
	for want := int64(1); want <= 3; want++ {
		server.Use(func(_ *types.HttpContext, next func(error)) {
			if got := nextIndex.Add(1); got != want {
				t.Errorf("middleware order = %d, want %d", got, want)
			}
			next(nil)
		})
	}

	response, err := http.Get(httpServer.URL + officialMiddlewarePollingPath)
	if err != nil {
		t.Fatalf("polling handshake: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || nextIndex.Load() != 3 {
		t.Fatalf("status/count = %d/%d, want 200/3", response.StatusCode, nextIndex.Load())
	}
}

func TestOfficialEngineMiddlewareCanEndPollingAndWebSocketRequests(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			server, httpServer := newOfficialMiddlewareServer(t)
			var connections atomic.Int64
			_ = server.On("connection", func(...any) { connections.Add(1) })
			server.Use(func(ctx *types.HttpContext, _ func(error)) {
				_ = ctx.SetStatusCode(http.StatusServiceUnavailable)
				_, _ = ctx.Write(nil)
			})

			var response *http.Response
			if transportName == "polling" {
				var err error
				response, err = http.Get(httpServer.URL + officialMiddlewarePollingPath)
				if err != nil {
					t.Fatalf("polling request: %v", err)
				}
			} else {
				connection, upgradeResponse, err := websocket.DefaultDialer.Dial(officialMiddlewareWebSocketURL(httpServer.URL), nil)
				if connection != nil {
					_ = connection.Close()
				}
				if err == nil {
					t.Fatal("WebSocket handshake unexpectedly succeeded")
				}
				response = upgradeResponse
			}
			if response == nil {
				t.Fatal("middleware response is nil")
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", response.StatusCode)
			}
			if connections.Load() != 0 {
				t.Fatalf("connection count = %d, want 0", connections.Load())
			}
		})
	}
}

func TestOfficialEngineMiddlewareSupportsSecurityAndSessionHeaders(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			server, httpServer := newOfficialMiddlewareServer(t)
			server.Use(func(ctx *types.HttpContext, next func(error)) {
				headers := ctx.ResponseHeaders()
				headers.Set("X-Download-Options", "noopen")
				headers.Set("X-Content-Type-Options", "nosniff")
				headers.Set("Set-Cookie", "connect.sid=go-session; Path=/; HttpOnly")
				next(nil)
			})

			var response *http.Response
			if transportName == "polling" {
				var err error
				response, err = http.Get(httpServer.URL + officialMiddlewarePollingPath)
				if err != nil {
					t.Fatalf("polling handshake: %v", err)
				}
			} else {
				connection, upgradeResponse, err := websocket.DefaultDialer.Dial(officialMiddlewareWebSocketURL(httpServer.URL), nil)
				if err != nil {
					t.Fatalf("WebSocket handshake: %v", err)
				}
				_ = connection.Close()
				response = upgradeResponse
			}
			if response == nil {
				t.Fatal("handshake response is nil")
			}
			if response.Body != nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			}
			if response.Header.Get("X-Download-Options") != "noopen" || response.Header.Get("X-Content-Type-Options") != "nosniff" {
				t.Fatalf("security headers = %q/%q", response.Header.Get("X-Download-Options"), response.Header.Get("X-Content-Type-Options"))
			}
			cookies := response.Header.Values("Set-Cookie")
			if len(cookies) == 0 || !strings.HasPrefix(cookies[0], "connect.sid=") {
				t.Fatalf("Set-Cookie = %#v, want connect.sid", cookies)
			}
		})
	}
}

func TestOfficialEngineMiddlewareErrorsRejectPollingAndWebSocket(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			server, httpServer := newOfficialMiddlewareServer(t)
			var connections atomic.Int64
			_ = server.On("connection", func(...any) { connections.Add(1) })
			server.Use(func(_ *types.HttpContext, next func(error)) {
				next(errors.New("will always fail"))
			})

			var response *http.Response
			if transportName == "polling" {
				var err error
				response, err = http.Get(httpServer.URL + officialMiddlewarePollingPath)
				if err != nil {
					t.Fatalf("polling request: %v", err)
				}
			} else {
				connection, upgradeResponse, err := websocket.DefaultDialer.Dial(officialMiddlewareWebSocketURL(httpServer.URL), nil)
				if connection != nil {
					_ = connection.Close()
				}
				if err == nil {
					t.Fatal("WebSocket handshake unexpectedly succeeded")
				}
				response = upgradeResponse
			}
			if response == nil {
				t.Fatal("middleware error response is nil")
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", response.StatusCode)
			}
			if connections.Load() != 0 {
				t.Fatalf("connection count = %d, want 0", connections.Load())
			}
		})
	}
}
