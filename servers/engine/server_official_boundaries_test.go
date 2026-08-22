package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/aqcool/socket.io/servers/engine/v4/transports"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/gorilla/websocket"
)

type blockingRequestBody struct {
	started chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (b *blockingRequestBody) Read([]byte) (int, error) {
	if b.once.CompareAndSwap(false, true) {
		close(b.started)
	}
	<-b.release
	return 0, io.EOF
}

func (*blockingRequestBody) Close() error { return nil }

func engineHandshake(t *testing.T, baseURL string) (*http.Response, string) {
	t.Helper()
	response, err := http.Get(baseURL + "/engine.io/?EIO=4&transport=polling")
	if err != nil {
		t.Fatalf("polling handshake: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("reading handshake: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("handshake status = %d, body = %q", response.StatusCode, body)
	}
	marker := `"sid":"`
	start := bytes.Index(body, []byte(marker))
	if start == -1 {
		t.Fatalf("handshake has no sid: %q", body)
	}
	start += len(marker)
	end := bytes.IndexByte(body[start:], '"')
	if end == -1 {
		t.Fatalf("invalid sid in handshake: %q", body)
	}
	return response, string(body[start : start+end])
}

func TestServerPollingCookieAndHeaderEventsMatchOfficialBehavior(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	options.SetCookie(&http.Cookie{})
	server := NewServer(options)
	var initialHeaders atomic.Int32
	var allHeaders atomic.Int32
	_ = server.On("initial_headers", func(args ...any) {
		initialHeaders.Add(1)
		headers := args[0].(*types.ParameterBag)
		request := args[1].(*types.HttpContext)
		if request.Method() != http.MethodGet {
			t.Errorf("initial_headers method = %s, want GET", request.Method())
		}
		headers.Set("X-Initial", "123")
		headers.Add("Set-Cookie", "mycookie=456")
	})
	_ = server.On("headers", func(args ...any) {
		allHeaders.Add(1)
		args[0].(*types.ParameterBag).Set("X-All", "456")
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	response, sid := engineHandshake(t, httpServer.URL)
	if got := response.Header.Get("X-Initial"); got != "123" {
		t.Fatalf("X-Initial = %q, want 123", got)
	}
	if got := response.Header.Get("X-All"); got != "456" {
		t.Fatalf("X-All = %q, want 456", got)
	}
	cookies := response.Cookies()
	if len(cookies) != 2 {
		t.Fatalf("Set-Cookie count = %d, want 2: %v", len(cookies), response.Header.Values("Set-Cookie"))
	}
	if cookies[0].Name != "io" || cookies[0].Value != sid || cookies[0].Path != "/" || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode {
		t.Fatalf("session cookie = %#v, want official defaults with sid %q", cookies[0], sid)
	}
	if cookies[1].Name != "mycookie" || cookies[1].Value != "456" {
		t.Fatalf("custom cookie = %#v", cookies[1])
	}

	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), strings.NewReader("4a"))
	if err != nil {
		t.Fatalf("creating polling POST: %v", err)
	}
	request.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	postResponse, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("polling POST: %v", err)
	}
	_, _ = io.Copy(io.Discard, postResponse.Body)
	_ = postResponse.Body.Close()
	if postResponse.StatusCode != http.StatusOK || postResponse.Header.Get("X-All") != "456" {
		t.Fatalf("POST status/headers = %d/%q", postResponse.StatusCode, postResponse.Header.Get("X-All"))
	}
	if postResponse.Header.Get("X-Initial") != "" || len(postResponse.Header.Values("Set-Cookie")) != 0 {
		t.Fatalf("initial-only headers leaked to subsequent request: %v", postResponse.Header)
	}
	if initialHeaders.Load() != 1 || allHeaders.Load() != 2 {
		t.Fatalf("event counts initial/all = %d/%d, want 1/2", initialHeaders.Load(), allHeaders.Load())
	}
}

func TestServerPollingCookieVariantsMatchOfficialBehavior(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*config.ServerOptions)
		want      func(string) string
	}{
		{
			name: "disabled by default",
			configure: func(*config.ServerOptions) {
			},
		},
		{
			name:      "custom name with defaults",
			configure: func(options *config.ServerOptions) { options.SetCookie(&http.Cookie{Name: "woot"}) },
			want:      func(sid string) string { return "woot=" + sid + "; Path=/; HttpOnly; SameSite=Lax" },
		},
		{
			name: "custom path",
			configure: func(options *config.ServerOptions) {
				options.SetCookie(&http.Cookie{})
				options.SetCookiePath("/custom")
			},
			want: func(sid string) string { return "io=" + sid + "; Path=/custom; HttpOnly; SameSite=Lax" },
		},
		{
			name: "path disabled",
			configure: func(options *config.ServerOptions) {
				options.SetCookie(&http.Cookie{})
				options.SetCookiePath("")
			},
			want: func(sid string) string { return "io=" + sid + "; HttpOnly; SameSite=Lax" },
		},
		{
			name: "HttpOnly disabled",
			configure: func(options *config.ServerOptions) {
				options.SetCookie(&http.Cookie{})
				options.SetCookieHttpOnly(false)
			},
			want: func(sid string) string { return "io=" + sid + "; Path=/; SameSite=Lax" },
		},
		{
			name: "SameSite strict",
			configure: func(options *config.ServerOptions) {
				options.SetCookie(&http.Cookie{SameSite: http.SameSiteStrictMode})
			},
			want: func(sid string) string { return "io=" + sid + "; Path=/; HttpOnly; SameSite=Strict" },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := config.DefaultServerOptions()
			test.configure(options)
			server := NewServer(options)
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			response, sid := engineHandshake(t, httpServer.URL)
			values := response.Header.Values("Set-Cookie")
			if test.want == nil {
				if len(values) != 0 {
					t.Fatalf("Set-Cookie = %v, want none", values)
				}
				return
			}
			if len(values) != 1 || values[0] != test.want(sid) {
				t.Fatalf("Set-Cookie = %v, want %q", values, test.want(sid))
			}
		})
	}
}

func TestServerCustomAndRejectedGeneratedIDsMatchOfficialBehavior(t *testing.T) {
	t.Run("custom ID polling", func(t *testing.T) {
		options := config.DefaultServerOptions()
		options.SetGenerateId(func(*types.HttpContext) (string, error) { return "CustomPollingId", nil })
		server := NewServer(options)
		httpServer := httptest.NewServer(server)
		t.Cleanup(func() {
			server.Close()
			httpServer.Close()
		})
		_, sid := engineHandshake(t, httpServer.URL)
		if sid != "CustomPollingId" {
			t.Fatalf("sid = %q, want CustomPollingId", sid)
		}
		if _, ok := server.Clients().Load(sid); !ok {
			t.Fatal("custom polling ID was not registered")
		}
	})

	t.Run("custom ID WebSocket", func(t *testing.T) {
		options := config.DefaultServerOptions()
		options.SetGenerateId(func(*types.HttpContext) (string, error) { return "CustomWebSocketId", nil })
		server := NewServer(options)
		httpServer := httptest.NewServer(server)
		t.Cleanup(func() {
			server.Close()
			httpServer.Close()
		})
		wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
		connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("WebSocket handshake: %v", err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		_, payload, err := connection.ReadMessage()
		if err != nil {
			t.Fatalf("reading open packet: %v", err)
		}
		if !bytes.Contains(payload, []byte(`"sid":"CustomWebSocketId"`)) {
			t.Fatalf("open packet = %q", payload)
		}
		if _, ok := server.Clients().Load("CustomWebSocketId"); !ok {
			t.Fatal("custom WebSocket ID was not registered")
		}
	})

	for _, transport := range []string{"polling", "websocket"} {
		t.Run("rejected "+transport, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetGenerateId(func(*types.HttpContext) (string, error) { return "", errors.New("nope") })
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

			if transport == "polling" {
				response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling")
				if err != nil {
					t.Fatalf("polling handshake: %v", err)
				}
				body, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if readErr != nil {
					t.Fatalf("reading polling rejection: %v", readErr)
				}
				if response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte(`"code":3`)) {
					t.Fatalf("polling rejection = %d %q", response.StatusCode, body)
				}
			} else {
				wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
				connection, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
				if connection != nil {
					_ = connection.Close()
				}
				if err == nil || response == nil || response.StatusCode != http.StatusBadRequest {
					t.Fatalf("WebSocket rejection error/response = %v/%v", err, response)
				}
				_ = response.Body.Close()
			}

			select {
			case event := <-errorEvents:
				if event.Code != BAD_REQUEST.Code || event.Context["name"] != "ID_GENERATION_ERROR" {
					t.Fatalf("connection_error = %#v context=%#v", event.CodeMessage, event.Context)
				}
			case <-time.After(time.Second):
				t.Fatal("connection_error event was not emitted")
			}
			if server.ClientsCount() != 0 {
				t.Fatalf("clients count = %d, want 0", server.ClientsCount())
			}
		})
	}
}

func TestServerWebSocketHeaderEventsMatchOfficialBehavior(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetCookie(&http.Cookie{Name: "engine", Path: "/custom", SameSite: http.SameSiteStrictMode})
	server := NewServer(options)
	var initialHeaders atomic.Int32
	var allHeaders atomic.Int32
	_ = server.On("initial_headers", func(args ...any) {
		initialHeaders.Add(1)
		args[0].(*types.ParameterBag).Set("X-Initial", "direct")
	})
	_ = server.On("headers", func(args ...any) {
		allHeaders.Add(1)
		args[0].(*types.ParameterBag).Set("X-All", "websocket")
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
	connection, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("direct WebSocket handshake: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	_, payload, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("reading WebSocket open packet: %v", err)
	}
	marker := `"sid":"`
	start := bytes.Index(payload, []byte(marker))
	if start == -1 {
		t.Fatalf("WebSocket open packet has no sid: %q", payload)
	}
	start += len(marker)
	end := bytes.IndexByte(payload[start:], '"')
	sid := string(payload[start : start+end])
	if response.Header.Get("X-Initial") != "direct" || response.Header.Get("X-All") != "websocket" {
		t.Fatalf("WebSocket response headers = %v", response.Header)
	}
	cookies := response.Cookies()
	if len(cookies) != 1 || cookies[0].Name != "engine" || cookies[0].Value != sid || cookies[0].Path != "/custom" || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("WebSocket session cookie = %#v, sid = %q", cookies, sid)
	}
	if initialHeaders.Load() != 1 || allHeaders.Load() != 1 {
		t.Fatalf("event counts initial/all = %d/%d, want 1/1", initialHeaders.Load(), allHeaders.Load())
	}
}

func TestServerUpgradeEmitsHeadersButNotInitialHeaders(t *testing.T) {
	server := NewServer(config.DefaultServerOptions())
	var initialHeaders atomic.Int32
	var allHeaders atomic.Int32
	_ = server.On("initial_headers", func(args ...any) {
		initialHeaders.Add(1)
		args[0].(*types.ParameterBag).Set("X-Initial", "only-once")
	})
	_ = server.On("headers", func(args ...any) {
		allHeaders.Add(1)
		args[0].(*types.ParameterBag).Set("X-All", "every-time")
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	_, sid := engineHandshake(t, httpServer.URL)
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket&sid=" + url.QueryEscape(sid)
	connection, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket upgrade: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if response.Header.Get("X-All") != "every-time" {
		t.Fatalf("upgrade X-All = %q", response.Header.Get("X-All"))
	}
	if response.Header.Get("X-Initial") != "" || len(response.Header.Values("Set-Cookie")) != 0 {
		t.Fatalf("initial-only headers leaked to upgrade: %v", response.Header)
	}
	if writeErr := connection.WriteMessage(websocket.TextMessage, []byte("2probe")); writeErr != nil {
		t.Fatalf("writing upgrade probe: %v", writeErr)
	}
	_, pong, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("reading upgrade probe response: %v", err)
	}
	if string(pong) != "3probe" {
		t.Fatalf("probe response = %q, want 3probe", pong)
	}
	if writeErr := connection.WriteMessage(websocket.TextMessage, []byte("5")); writeErr != nil {
		t.Fatalf("completing upgrade: %v", writeErr)
	}
	if initialHeaders.Load() != 1 || allHeaders.Load() != 2 {
		t.Fatalf("event counts initial/all = %d/%d, want 1/2", initialHeaders.Load(), allHeaders.Load())
	}
}

func TestServerPollingMaxPayloadRejectsContentLengthAndChunkedBodies(t *testing.T) {
	for _, test := range []struct {
		name    string
		chunked bool
	}{
		{name: "content length"},
		{name: "chunked", chunked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetAllowUpgrades(false)
			options.SetMaxHttpBufferSize(5)
			options.SetPingInterval(time.Second)
			server := NewServer(options)
			server.Use(func(_ *types.HttpContext, next func(error)) { next(nil) })
			messages := make(chan struct{}, 1)
			closed := make(chan string, 1)
			_ = server.On("connection", func(args ...any) {
				socket := args[0].(Socket)
				_ = socket.On("message", func(...any) { messages <- struct{}{} })
				_ = socket.Once("close", func(args ...any) { closed <- args[0].(string) })
			})
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			_, sid := engineHandshake(t, httpServer.URL)
			request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), io.NopCloser(strings.NewReader("4abcdef")))
			if err != nil {
				t.Fatalf("creating oversized request: %v", err)
			}
			request.Header.Set("Content-Type", "text/plain;charset=UTF-8")
			if test.chunked {
				request.ContentLength = -1
				request.TransferEncoding = []string{"chunked"}
			} else {
				request.ContentLength = int64(len("4abcdef"))
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatalf("oversized polling POST: %v", err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d, want 413", response.StatusCode)
			}
			select {
			case <-messages:
				t.Fatal("oversized payload reached message listener")
			default:
			}
			select {
			case reason := <-closed:
				if reason != "transport error" {
					t.Fatalf("close reason = %q, want transport error", reason)
				}
			case <-time.After(time.Second):
				t.Fatal("oversized payload did not close the session")
			}
		})
	}
}

func TestServerPollingAcceptsPayloadBelowMaximum(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	options.SetMaxHttpBufferSize(5)
	server := NewServer(options)
	messages := make(chan string, 1)
	_ = server.On("connection", func(args ...any) {
		_ = args[0].(Socket).On("message", func(args ...any) {
			messages <- args[0].(types.BufferInterface).String()
		})
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	_, sid := engineHandshake(t, httpServer.URL)
	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), strings.NewReader("4a"))
	if err != nil {
		t.Fatalf("creating polling POST: %v", err)
	}
	request.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("polling POST: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	select {
	case message := <-messages:
		if message != "a" {
			t.Fatalf("message = %q, want a", message)
		}
	case <-time.After(time.Second):
		t.Fatal("payload below maximum was not delivered")
	}
}

func TestServerWebSocketRejectsPayloadAboveMaximum(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetMaxHttpBufferSize(5)
	server := NewServer(options)
	messages := make(chan struct{}, 1)
	closed := make(chan string, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.On("message", func(...any) { messages <- struct{}{} })
		_ = socket.Once("close", func(args ...any) { closed <- args[0].(string) })
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket handshake: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, _, readErr := connection.ReadMessage(); readErr != nil {
		t.Fatalf("reading open packet: %v", readErr)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte("4abcdef")); err != nil {
		t.Fatalf("writing oversized message: %v", err)
	}
	select {
	case <-messages:
		t.Fatal("oversized WebSocket payload reached message listener")
	case reason := <-closed:
		if reason != "transport error" && reason != "transport close" {
			t.Fatalf("close reason = %q", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("oversized WebSocket payload did not close the session")
	}
}

func TestServerClosesTransportOnParseError(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetAllowUpgrades(false)
			options.SetPingInterval(time.Second)
			server := NewServer(options)
			sockets := make(chan Socket, 1)
			transportClosed := make(chan struct{}, 1)
			socketClosed := make(chan string, 1)
			messages := make(chan struct{}, 1)
			_ = server.On("connection", func(args ...any) {
				socket := args[0].(Socket)
				_ = socket.Transport().Once("close", func(...any) { transportClosed <- struct{}{} })
				_ = socket.Once("close", func(args ...any) { socketClosed <- args[0].(string) })
				_ = socket.On("message", func(...any) { messages <- struct{}{} })
				sockets <- socket
			})
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			var pollDone <-chan struct{}
			if transportName == "websocket" {
				wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
				connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
				if err != nil {
					t.Fatalf("WebSocket handshake: %v", err)
				}
				t.Cleanup(func() { _ = connection.Close() })
				if _, _, err := connection.ReadMessage(); err != nil {
					t.Fatalf("reading open packet: %v", err)
				}
				<-sockets
				if err := connection.WriteMessage(websocket.TextMessage, []byte("invalid")); err != nil {
					t.Fatalf("writing invalid packet: %v", err)
				}
			} else {
				_, sid := engineHandshake(t, httpServer.URL)
				socket := <-sockets
				pollFinished := make(chan struct{})
				pollDone = pollFinished
				go func() {
					defer close(pollFinished)
					response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(sid))
					if err == nil {
						_, _ = io.Copy(io.Discard, response.Body)
						_ = response.Body.Close()
					}
				}()
				deadline := time.Now().Add(time.Second)
				for !socket.Transport().Writable() && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if !socket.Transport().Writable() {
					t.Fatal("poll request did not become writable")
				}
				request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), strings.NewReader("invalid"))
				if err != nil {
					t.Fatalf("creating invalid POST: %v", err)
				}
				request.Header.Set("Content-Type", "text/plain;charset=UTF-8")
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatalf("invalid polling POST: %v", err)
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			}

			select {
			case reason := <-socketClosed:
				if reason != "parse error" {
					t.Fatalf("socket close reason = %q, want parse error", reason)
				}
			case <-time.After(time.Second):
				t.Fatal("socket did not close after parse error")
			}
			select {
			case <-transportClosed:
			case <-time.After(time.Second):
				t.Fatal("transport did not close after parse error")
			}
			if pollDone != nil {
				select {
				case <-pollDone:
				case <-time.After(time.Second):
					t.Fatal("outstanding poll did not close after parse error")
				}
			}
			select {
			case <-messages:
				t.Fatal("invalid packet reached message listener")
			default:
			}
		})
	}
}

func TestServerClosesUpgradingTransportOnTimeout(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetUpgradeTimeout(50 * time.Millisecond)
	server := NewServer(options)
	upgradingSocket := make(chan Socket, 1)
	upgradeClosed := make(chan struct{}, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.On("upgrading", func(args ...any) {
			candidate := args[0].(transports.Transport)
			_ = candidate.Once("close", func(...any) { upgradeClosed <- struct{}{} })
			upgradingSocket <- socket
		})
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	_, sid := engineHandshake(t, httpServer.URL)
	connection := beginWebSocketProbe(t, httpServer.URL, sid)
	t.Cleanup(func() { _ = connection.Close() })

	var socket Socket
	select {
	case socket = <-upgradingSocket:
	case <-time.After(time.Second):
		t.Fatal("server did not enter upgrading state")
	}
	select {
	case <-upgradeClosed:
	case <-time.After(time.Second):
		t.Fatal("candidate transport was not closed after upgrade timeout")
	}
	if socket.Upgrading() {
		t.Fatal("socket remained in upgrading state after timeout")
	}
	if socket.ReadyState() != "open" {
		t.Fatalf("original polling socket state = %q, want open", socket.ReadyState())
	}
}

func TestServerClosesUpgradingTransportWhenSocketCloses(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetUpgradeTimeout(time.Second)
	server := NewServer(options)
	upgradeClosed := make(chan struct{}, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.On("upgrading", func(args ...any) {
			candidate := args[0].(transports.Transport)
			_ = candidate.Once("close", func(...any) { upgradeClosed <- struct{}{} })
			socket.Close(true)
		})
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	_, sid := engineHandshake(t, httpServer.URL)
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket&sid=" + url.QueryEscape(sid)
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket upgrade: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	// The official Engine.IO assertion only requires the candidate transport
	// to close when its socket closes. Do not require a 3probe response here:
	// the upgrading listener closes the socket synchronously, and the close is
	// allowed to overtake the asynchronously queued pong frame.
	if writeErr := connection.WriteMessage(websocket.TextMessage, []byte("2probe")); writeErr != nil {
		t.Fatalf("writing upgrade probe: %v", writeErr)
	}
	select {
	case <-upgradeClosed:
	case <-time.After(time.Second):
		t.Fatal("candidate transport was not closed with its socket")
	}
}

func beginWebSocketProbe(t *testing.T, baseURL, sid string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(baseURL, "http") + "/engine.io/?EIO=4&transport=websocket&sid=" + url.QueryEscape(sid)
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket upgrade: %v", err)
	}
	if writeErr := connection.WriteMessage(websocket.TextMessage, []byte("2probe")); writeErr != nil {
		_ = connection.Close()
		t.Fatalf("writing upgrade probe: %v", writeErr)
	}
	_, response, err := connection.ReadMessage()
	if err != nil {
		_ = connection.Close()
		t.Fatalf("reading upgrade probe response: %v", err)
	}
	if string(response) != "3probe" {
		_ = connection.Close()
		t.Fatalf("upgrade probe response = %q, want 3probe", response)
	}
	return connection
}

func TestServerSendCallbacksExecuteInOrder(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetAllowUpgrades(false)
			server := NewServer(options)
			callbacks := make(chan int, 4)
			_ = server.On("connection", func(args ...any) {
				socket := args[0].(Socket)
				for index, value := range []string{"d", "c", "b", "a"} {
					callbackIndex := index
					socket.Send(strings.NewReader(value), nil, func(transports.Transport) {
						callbacks <- callbackIndex
					})
				}
			})
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			if transportName == "polling" {
				_, sid := engineHandshake(t, httpServer.URL)
				response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(sid))
				if err != nil {
					t.Fatalf("polling receive: %v", err)
				}
				body, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if readErr != nil {
					t.Fatalf("reading polling payload: %v", readErr)
				}
				for _, packet := range []string{"4d", "4c", "4b", "4a"} {
					if !bytes.Contains(body, []byte(packet)) {
						t.Fatalf("polling payload %q does not contain %q", body, packet)
					}
				}
			} else {
				wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
				connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
				if err != nil {
					t.Fatalf("WebSocket handshake: %v", err)
				}
				t.Cleanup(func() { _ = connection.Close() })
				for range 5 { // OPEN followed by four MESSAGE packets.
					if _, _, err := connection.ReadMessage(); err != nil {
						t.Fatalf("reading WebSocket packet: %v", err)
					}
				}
			}

			for want := range 4 {
				select {
				case got := <-callbacks:
					if got != want {
						t.Fatalf("callback order = %d at position %d", got, want)
					}
				case <-time.After(time.Second):
					t.Fatalf("callback %d did not execute", want)
				}
			}
		})
	}
}

func TestServerSendCallbackDuringPollingUpgradeWindow(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetUpgradeTimeout(time.Second)
	server := NewServer(options)
	connected := make(chan Socket, 1)
	callback := make(chan string, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		connected <- socket
		_ = socket.Once("upgrading", func(...any) {
			socket.Send(strings.NewReader("during-upgrade"), nil, func(transport transports.Transport) {
				callback <- transport.Name()
			})
		})
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	_, sid := engineHandshake(t, httpServer.URL)
	socket := <-connected
	pollResult := make(chan []byte, 1)
	pollError := make(chan error, 1)
	go func() {
		response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(sid))
		if err != nil {
			pollError <- err
			return
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			pollError <- readErr
			return
		}
		pollResult <- body
	}()
	deadline := time.Now().Add(time.Second)
	for !socket.Transport().Writable() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !socket.Transport().Writable() {
		t.Fatal("Polling transport did not enter writable state")
	}

	upgrade := beginWebSocketProbe(t, httpServer.URL, sid)
	t.Cleanup(func() { _ = upgrade.Close() })
	select {
	case err := <-pollError:
		t.Fatalf("Polling during upgrade: %v", err)
	case body := <-pollResult:
		if !bytes.Contains(body, []byte("4during-upgrade")) {
			t.Fatalf("Polling upgrade-window payload = %q", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message was not delivered during upgrade window")
	}
	select {
	case transportName := <-callback:
		if transportName != "polling" {
			t.Fatalf("callback transport = %q, want polling", transportName)
		}
	case <-time.After(time.Second):
		t.Fatal("send callback did not execute during upgrade window")
	}
	select {
	case duplicate := <-callback:
		t.Fatalf("send callback executed twice on %s", duplicate)
	default:
	}
	if err := upgrade.WriteMessage(websocket.TextMessage, []byte("5")); err != nil {
		t.Fatalf("completing WebSocket upgrade: %v", err)
	}
}

func TestServerDiscardClearsPendingCallbacksAfterCloseEvent(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	server := NewServer(options)
	sockets := make(chan *socket, 1)
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})
	_ = server.On("connection", func(args ...any) {
		sockets <- args[0].(*socket)
	})

	_, _ = engineHandshake(t, httpServer.URL)
	engineSocket := <-sockets
	callbackCalled := make(chan struct{}, 1)
	bufferAtClose := make(chan int, 1)
	closed := make(chan struct{}, 1)
	_ = engineSocket.Once("close", func(...any) {
		bufferAtClose <- engineSocket.writeBuffer.Len()
		closed <- struct{}{}
	})
	engineSocket.Send(strings.NewReader("not-sent"), nil, func(transports.Transport) {
		callbackCalled <- struct{}{}
	})
	if engineSocket.writeBuffer.Len() == 0 || engineSocket.packetsFn.Len() == 0 {
		t.Fatal("send was not pending before discard")
	}
	engineSocket.Close(true)

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("discarded socket did not close")
	}
	if got := <-bufferAtClose; got == 0 {
		t.Fatal("write buffer was cleared before close listeners ran")
	}
	if engineSocket.writeBuffer.Len() != 0 || engineSocket.packetsFn.Len() != 0 || engineSocket.sentCallbackFn.Len() != 0 {
		t.Fatalf("pending state after close: write=%d packetsFn=%d sentFn=%d", engineSocket.writeBuffer.Len(), engineSocket.packetsFn.Len(), engineSocket.sentCallbackFn.Len())
	}
	select {
	case <-callbackCalled:
		t.Fatal("callback ran for a packet that was discarded")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestServerGracefulCloseFlushesPendingPacketAndCallback(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	server := NewServer(options)
	sockets := make(chan Socket, 1)
	_ = server.On("connection", func(args ...any) { sockets <- args[0].(Socket) })
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	_, sid := engineHandshake(t, httpServer.URL)
	engineSocket := <-sockets
	order := make(chan string, 2)
	_ = engineSocket.Once("close", func(...any) { order <- "close" })
	engineSocket.Send(strings.NewReader("hello"), nil, func(transports.Transport) { order <- "callback" })
	engineSocket.Close(false)

	response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(sid))
	if err != nil {
		t.Fatalf("polling graceful close: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("reading graceful close payload: %v", readErr)
	}
	if !bytes.Contains(body, []byte("4hello")) {
		t.Fatalf("graceful close payload = %q, want pending message", body)
	}
	select {
	case got := <-order:
		if got != "callback" {
			t.Fatalf("first event = %q, want callback", got)
		}
	case <-time.After(time.Second):
		t.Fatal("missing callback event")
	}
	select {
	case got := <-order:
		if got != "close" {
			t.Fatalf("second event = %q, want close", got)
		}
	default:
		// The close request can race with the first Polling write. When that
		// write has already started, the official protocol delivers CLOSE in
		// the next poll instead of mutating the in-flight payload.
		closeResponse, closeErr := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(sid))
		if closeErr != nil {
			t.Fatalf("receiving graceful close packet: %v", closeErr)
		}
		_, _ = io.Copy(io.Discard, closeResponse.Body)
		_ = closeResponse.Body.Close()
		select {
		case got := <-order:
			if got != "close" {
				t.Fatalf("second event = %q, want close", got)
			}
		case <-time.After(time.Second):
			t.Fatal("missing close event")
		}
	}
	if engineSocket.ReadyState() != "closed" {
		t.Fatalf("socket state = %q, want closed", engineSocket.ReadyState())
	}
}

func TestServerPacketEventsAreObservableWithoutConsumingData(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	server := NewServer(options)
	incomingPacket := make(chan string, 1)
	incomingMessage := make(chan string, 1)
	createdPacket := make(chan string, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.On("packet", func(args ...any) {
			observed := args[0].(*packet.Packet)
			if observed.Type == packet.MESSAGE {
				data, _ := io.ReadAll(observed.Data)
				incomingPacket <- string(data)
			}
		})
		_ = socket.On("message", func(args ...any) {
			incomingMessage <- args[0].(types.BufferInterface).String()
		})
		_ = socket.On("packetCreate", func(args ...any) {
			observed := args[0].(*packet.Packet)
			if observed.Type == packet.MESSAGE {
				data, _ := io.ReadAll(observed.Data)
				createdPacket <- string(data)
			}
		})
		socket.Send(strings.NewReader("server"), nil, nil)
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket handshake: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, _, readErr := connection.ReadMessage(); readErr != nil {
		t.Fatalf("reading open packet: %v", readErr)
	}
	_, outgoing, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("reading server message: %v", err)
	}
	if string(outgoing) != "4server" {
		t.Fatalf("server message = %q, want 4server", outgoing)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte("4client")); err != nil {
		t.Fatalf("writing client message: %v", err)
	}

	for name, channel := range map[string]<-chan string{
		"packetCreate": createdPacket,
		"packet":       incomingPacket,
		"message":      incomingMessage,
	} {
		select {
		case got := <-channel:
			want := "client"
			if name == "packetCreate" {
				want = "server"
			}
			if got != want {
				t.Fatalf("%s data = %q, want %q", name, got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s event was not emitted", name)
		}
	}
}

func TestServerHeartbeatPacketEventsMatchOfficialBehavior(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	options.SetPingInterval(20 * time.Millisecond)
	options.SetPingTimeout(time.Second)
	server := NewServer(options)
	createdPing := make(chan struct{}, 1)
	receivedPong := make(chan struct{}, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.On("packetCreate", func(args ...any) {
			if args[0].(*packet.Packet).Type == packet.PING {
				createdPing <- struct{}{}
			}
		})
		_ = socket.On("packet", func(args ...any) {
			if args[0].(*packet.Packet).Type == packet.PONG {
				receivedPong <- struct{}{}
			}
		})
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket handshake: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, _, readErr := connection.ReadMessage(); readErr != nil {
		t.Fatalf("reading open packet: %v", readErr)
	}
	_, ping, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("reading ping: %v", err)
	}
	if string(ping) != "2" {
		t.Fatalf("ping packet = %q, want 2", ping)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte("3")); err != nil {
		t.Fatalf("writing pong: %v", err)
	}
	for name, event := range map[string]<-chan struct{}{"packetCreate ping": createdPing, "packet pong": receivedPong} {
		select {
		case <-event:
		case <-time.After(time.Second):
			t.Fatalf("%s event was not emitted", name)
		}
	}
}

func TestServerV3PongPacketCreateEventMatchesOfficialBehavior(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowEIO3(true)
	options.SetAllowUpgrades(false)
	options.SetPingInterval(time.Second)
	options.SetPingTimeout(time.Second)
	server := NewServer(options)
	createdPong := make(chan struct{}, 1)
	_ = server.On("connection", func(args ...any) {
		_ = args[0].(Socket).On("packetCreate", func(packetArgs ...any) {
			if packetArgs[0].(*packet.Packet).Type == packet.PONG {
				createdPong <- struct{}{}
			}
		})
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=3&transport=websocket"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("EIO 3 WebSocket handshake: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, _, readErr := connection.ReadMessage(); readErr != nil {
		t.Fatalf("reading EIO 3 OPEN: %v", readErr)
	}
	if writeErr := connection.WriteMessage(websocket.TextMessage, []byte("2")); writeErr != nil {
		t.Fatalf("writing EIO 3 PING: %v", writeErr)
	}
	_, pong, err := connection.ReadMessage()
	if err != nil || string(pong) != "3" {
		t.Fatalf("EIO 3 PONG = %q, error=%v", pong, err)
	}
	select {
	case <-createdPong:
	case <-time.After(time.Second):
		t.Fatal("packetCreate was not emitted for EIO 3 PONG")
	}
}

func TestServerWebSocketPreEncodedContentMatchesOfficialBehavior(t *testing.T) {
	for _, test := range []struct {
		name        string
		compression bool
		want        string
	}{
		{name: "uses pre-encoded content", want: "4123"},
		{name: "ignores pre-encoded content with compression", compression: true, want: "4test"},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetAllowUpgrades(false)
			if test.compression {
				options.SetPerMessageDeflate(&types.PerMessageDeflate{Threshold: 0})
			}
			server := NewServer(options)
			_ = server.On("connection", func(args ...any) {
				args[0].(Socket).Send(strings.NewReader("test"), &packet.Options{
					WsPreEncodedFrame: types.NewStringBufferString("4123"),
				}, nil)
			})
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			dialer := *websocket.DefaultDialer
			dialer.EnableCompression = test.compression
			wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=4&transport=websocket"
			connection, _, err := dialer.Dial(wsURL, nil)
			if err != nil {
				t.Fatalf("WebSocket handshake: %v", err)
			}
			t.Cleanup(func() { _ = connection.Close() })
			if _, _, readErr := connection.ReadMessage(); readErr != nil {
				t.Fatalf("reading open packet: %v", readErr)
			}
			_, payload, err := connection.ReadMessage()
			if err != nil {
				t.Fatalf("reading message: %v", err)
			}
			if string(payload) != test.want {
				t.Fatalf("message = %q, want %q", payload, test.want)
			}
		})
	}
}

func TestServerPollingRequestCancellationClosesTransport(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	server := NewServer(options)
	sockets := make(chan Socket, 1)
	closed := make(chan struct {
		reason string
		err    error
	}, 1)
	transportClosed := make(chan struct{}, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.Transport().Once("close", func(...any) { transportClosed <- struct{}{} })
		_ = socket.Once("close", func(args ...any) {
			closed <- struct {
				reason string
				err    error
			}{reason: args[0].(string), err: args[1].(error)}
		})
		sockets <- socket
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	_, sid := engineHandshake(t, httpServer.URL)
	socket := <-sockets
	requestContext, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), nil)
	if err != nil {
		t.Fatalf("creating cancellable poll: %v", err)
	}
	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := http.DefaultClient.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- requestErr
	}()
	deadline := time.Now().Add(time.Second)
	for !socket.Transport().Writable() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !socket.Transport().Writable() {
		t.Fatal("poll request did not become writable")
	}
	cancel()
	select {
	case requestErr := <-requestDone:
		if requestErr == nil || !errors.Is(requestErr, context.Canceled) {
			t.Fatalf("poll cancellation error = %v", requestErr)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled poll did not return")
	}
	select {
	case event := <-closed:
		if event.reason != "transport error" || event.err == nil || !strings.Contains(event.err.Error(), "poll connection closed prematurely") {
			t.Fatalf("close event = %q/%v", event.reason, event.err)
		}
	case <-time.After(time.Second):
		t.Fatal("socket did not close after poll cancellation")
	}
	select {
	case <-transportClosed:
	case <-time.After(time.Second):
		t.Fatal("transport did not close after poll cancellation")
	}
	if server.ClientsCount() != 0 {
		t.Fatalf("clients count = %d, want 0", server.ClientsCount())
	}
}

func TestServerRejectsOverlappingPollingRequests(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	server := NewServer(options)
	sockets := make(chan Socket, 1)
	closed := make(chan error, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.Once("close", func(args ...any) { closed <- args[1].(error) })
		sockets <- socket
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	_, sid := engineHandshake(t, httpServer.URL)
	socket := <-sockets
	firstDone := make(chan *http.Response, 1)
	go func() {
		response, _ := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(sid))
		firstDone <- response
	}()
	deadline := time.Now().Add(time.Second)
	for !socket.Transport().Writable() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !socket.Transport().Writable() {
		t.Fatal("first poll did not become writable")
	}
	response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(sid))
	if err != nil {
		t.Fatalf("overlapping poll: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("overlapping poll status = %d, want 400", response.StatusCode)
	}
	select {
	case first := <-firstDone:
		if first == nil {
			t.Fatal("first poll failed without response")
		}
		_, _ = io.Copy(io.Discard, first.Body)
		_ = first.Body.Close()
	case <-time.After(time.Second):
		t.Fatal("first poll was not released after overlap")
	}
	select {
	case closeErr := <-closed:
		if closeErr == nil || !strings.Contains(closeErr.Error(), "overlap from client") {
			t.Fatalf("close error = %v", closeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("socket did not close after overlapping poll")
	}
}

func TestServerCloseAbortsInProgressPollingDataRequest(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	server := NewServer(options)
	sockets := make(chan Socket, 1)
	_ = server.On("connection", func(args ...any) { sockets <- args[0].(Socket) })
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	_, sid := engineHandshake(t, httpServer.URL)
	engineSocket := <-sockets
	body := &blockingRequestBody{started: make(chan struct{}), release: make(chan struct{})}
	request := httptest.NewRequest(http.MethodPost, "/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), body)
	request.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	recorder := httptest.NewRecorder()
	ctx := types.NewHttpContext(recorder, request)
	requestDone := make(chan struct{})
	go func() {
		engineSocket.Transport().OnRequest(ctx)
		close(requestDone)
	}()
	select {
	case <-body.started:
	case <-time.After(time.Second):
		t.Fatal("polling data request did not start reading")
	}

	engineSocket.Close(true)
	close(body.release)
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("polling data request remained blocked after server close")
	}
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("aborted data request status = %d, want 429", recorder.Code)
	}
	if engineSocket.ReadyState() != "closed" {
		t.Fatalf("socket state = %q, want closed", engineSocket.ReadyState())
	}
}

func TestServerConnectionCloseHeaderDoesNotAbortPollingSession(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	server := NewServer(options)
	closed := make(chan struct{}, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.Once("close", func(...any) { closed <- struct{}{} })
		_ = socket.On("message", func(...any) {
			socket.Send(strings.NewReader("woot"), nil, nil)
		})
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	t.Cleanup(client.CloseIdleConnections)

	handshakeRequest, err := http.NewRequest(http.MethodGet, httpServer.URL+"/engine.io/?EIO=4&transport=polling", nil)
	if err != nil {
		t.Fatalf("creating handshake: %v", err)
	}
	handshakeRequest.Header.Set("Connection", "close")
	handshakeResponse, err := client.Do(handshakeRequest)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	handshakeBody, readErr := io.ReadAll(handshakeResponse.Body)
	_ = handshakeResponse.Body.Close()
	if readErr != nil {
		t.Fatalf("reading handshake: %v", readErr)
	}
	marker := []byte(`"sid":"`)
	start := bytes.Index(handshakeBody, marker)
	if start == -1 {
		t.Fatalf("handshake has no sid: %q", handshakeBody)
	}
	start += len(marker)
	end := bytes.IndexByte(handshakeBody[start:], '"')
	sid := string(handshakeBody[start : start+end])

	postRequest, err := http.NewRequest(http.MethodPost, httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), strings.NewReader("4test"))
	if err != nil {
		t.Fatalf("creating POST: %v", err)
	}
	postRequest.Header.Set("Connection", "close")
	postRequest.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	postResponse, err := client.Do(postRequest)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_, _ = io.Copy(io.Discard, postResponse.Body)
	_ = postResponse.Body.Close()
	if postResponse.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d", postResponse.StatusCode)
	}

	pollRequest, err := http.NewRequest(http.MethodGet, httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), nil)
	if err != nil {
		t.Fatalf("creating poll: %v", err)
	}
	pollRequest.Header.Set("Connection", "close")
	pollResponse, err := client.Do(pollRequest)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	payload, readErr := io.ReadAll(pollResponse.Body)
	_ = pollResponse.Body.Close()
	if readErr != nil || !bytes.Contains(payload, []byte("4woot")) {
		t.Fatalf("polling echo = %q, error = %v", payload, readErr)
	}
	select {
	case <-closed:
		t.Fatal("Connection: close aborted a healthy Engine.IO session")
	case <-time.After(20 * time.Millisecond):
	}
	if server.ClientsCount() != 1 {
		t.Fatalf("clients count = %d, want 1", server.ClientsCount())
	}
}
