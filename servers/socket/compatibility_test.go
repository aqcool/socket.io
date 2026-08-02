package socket

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	socketlog "github.com/aqcool/socket.io/v3/pkg/log"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

const compatibilityTestEnv = "SOCKET_IO_COMPAT_TEST"

func registerCompatibilitySocketHandlers(server *Server) {
	var recoveryMiddlewareCalls atomic.Int64
	var slowNamespaceConnections atomic.Int64
	register := func(args ...any) {
		client := args[0].(*Socket)
		var anyMu sync.Mutex
		anyIncoming := make([]string, 0)
		anyOutgoing := make([]string, 0)
		client.OnAny(func(args ...any) {
			anyMu.Lock()
			anyIncoming = append(anyIncoming, fmt.Sprint(args[0]))
			anyMu.Unlock()
		})
		client.OnAnyOutgoing(func(args ...any) {
			anyMu.Lock()
			anyOutgoing = append(anyOutgoing, fmt.Sprint(args[0]))
			anyMu.Unlock()
		})
		if server.Opts().ConnectionStateRecovery() != nil {
			if !client.Recovered() {
				client.SetData(map[string]any{"marker": "persisted-socket-data"})
			}
			client.Join("recovery-room")
			_ = client.Emit(
				"recovery-ready",
				"ready",
				client.Recovered(),
				compatibilitySocketDataMarker(client.Data()),
				client.Rooms().Has("recovery-room"),
			)
		}

		_ = client.On("echo", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack([]any{args[0]}, nil)
		})
		_ = client.On("server-socket-id", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack([]any{string(client.Id())}, nil)
		})
		_ = client.On("binary", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack([]any{args[0]}, nil)
		})
		_ = client.On("multi-args", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack(args[:len(args)-1], nil)
		})
		_ = client.On("message", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack(args[:len(args)-1], nil)
		})
		_ = client.On("trigger-server-ack", func(...any) {
			_ = client.Emit("server-ack", "server-value", func(args []any, err error) {
				if err == nil && len(args) > 0 {
					_ = client.Emit("server-ack-result", args[0])
				}
			})
		})
		_ = client.On("trigger-server-binary-ack", func(...any) {
			_ = client.Emit("server-binary-ack", []byte{1, 2, 3}, func(args []any, err error) {
				if err == nil && len(args) > 0 {
					_ = client.Emit("server-binary-ack-result", args[0])
				}
			})
		})
		_ = client.On("any-incoming", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack([]any{"first", 2, true}, nil)
		})
		_ = client.On("trigger-any-outgoing", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = client.Emit("any-outgoing", "server-value", func(response []any, err error) {
				if err == nil {
					_ = client.Emit("any-outgoing-ack-observed", response...)
				}
			})
			ack(nil, nil)
		})
		_ = client.On("any-observer-snapshot", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			anyMu.Lock()
			incoming := append([]string(nil), anyIncoming...)
			outgoing := append([]string(nil), anyOutgoing...)
			anyMu.Unlock()
			ack([]any{incoming, outgoing}, nil)
		})
		_ = client.On("trigger-ack-lifecycle", func(args ...any) {
			controlAck := args[len(args)-1].(Ack)
			var timeoutCallbacks atomic.Int64
			var zeroTimeoutCallbacks atomic.Int64
			_ = client.Timeout(500*time.Millisecond).Emit("ack-fast", "fast-value", func(response []any, err error) {
				_ = client.Emit("ack-fast-result", response, err != nil)
			})
			_ = client.Timeout(75*time.Millisecond).Emit("ack-never", "late-value", func(response []any, err error) {
				count := timeoutCallbacks.Add(1)
				_ = client.Emit("ack-timeout-result", response, err != nil, count)
				time.AfterFunc(300*time.Millisecond, func() {
					_ = client.Emit("ack-timeout-final-count", timeoutCallbacks.Load())
				})
			})
			_ = client.Timeout(0).Emit("ack-zero", "zero-value", func(response []any, err error) {
				count := zeroTimeoutCallbacks.Add(1)
				_ = client.Emit("ack-zero-result", response, err != nil, count)
				time.AfterFunc(200*time.Millisecond, func() {
					_ = client.Emit("ack-zero-final-count", zeroTimeoutCallbacks.Load())
				})
			})
			controlAck(nil, nil)
		})
		_ = client.On("request-server-disconnect", func(...any) {
			client.Disconnect(false)
		})
		client.Use(func(event []any, next func(error)) {
			switch fmt.Sprint(event[0]) {
			case "middleware-mutate":
				if len(event) > 1 {
					event[1] = "modified-by-middleware"
				}
				next(nil)
			case "middleware-blocked":
				next(errors.New("blocked by socket middleware"))
			default:
				next(nil)
			}
		})
		_ = client.On("middleware-mutate", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack([]any{args[0]}, nil)
		})
		_ = client.On("middleware-blocked", func(args ...any) {
			if ack, ok := args[len(args)-1].(Ack); ok {
				ack([]any{"unexpected-handler-call"}, nil)
			}
		})
		_ = client.On("error", func(args ...any) {
			if len(args) > 0 {
				_ = client.Emit("middleware-error-observed", fmt.Sprint(args[0]))
			}
		})
		_ = client.On("dynamic-identity", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack([]any{client.Nsp().Name()}, nil)
		})
		_ = client.On("namespace-present", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			name := fmt.Sprint(args[0])
			present := false
			for _, namespace := range server.Namespaces() {
				if namespace.Name() == name {
					present = true
					break
				}
			}
			ack([]any{present}, nil)
		})
		_ = client.On("trigger-dynamic-broadcast", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = client.Nsp().Emit("dynamic-broadcast", args[0])
			ack(nil, nil)
		})
		_ = client.On("join-matrix-rooms", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			rooms := make([]Room, 0, len(args)-1)
			for _, value := range args[:len(args)-1] {
				rooms = append(rooms, Room(fmt.Sprint(value)))
			}
			client.Join(rooms...)
			ack(nil, nil)
		})
		_ = client.On("leave-matrix-room", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			client.Leave(Room(fmt.Sprint(args[0])))
			ack(nil, nil)
		})
		_ = client.On("trigger-union-broadcast", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = server.To("matrix-room-a", "matrix-room-b").Emit("union-broadcast", args[0])
			ack(nil, nil)
		})
		_ = client.On("trigger-binary-room-broadcast", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = server.To("matrix-room-a", "matrix-room-b").Emit("binary-room-broadcast", types.NewBytesBuffer([]byte{1, 2, 3}))
			ack(nil, nil)
		})
		_ = client.On("trigger-namespace-broadcast", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			server.Emit("namespace-broadcast", args[0])
			ack(nil, nil)
		})
		_ = client.On("trigger-except-broadcast", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = server.To("matrix-room-a", "matrix-room-b").Except("matrix-excluded").Emit("except-broadcast", args[0])
			ack(nil, nil)
		})
		_ = client.On("trigger-socket-broadcast", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = client.To("matrix-room-a").Emit("socket-broadcast", args[0])
			ack(nil, nil)
		})
		_ = client.On("deployment-trigger-broadcast", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			server.Emit("deployment-broadcast", args[0])
			ack(nil, nil)
		})
		_ = client.On("deployment-trigger-namespace-broadcast", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = client.Nsp().Emit("deployment-namespace-broadcast", args[0])
			ack(nil, nil)
		})
		_ = client.On("deployment-trigger-volatile-binary", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = server.Volatile().Emit("deployment-volatile-binary", types.NewBytesBuffer([]byte{1, 2, 3}))
			ack(nil, nil)
		})
		_ = client.On("deployment-trigger-room-broadcast", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = server.To("deployment-room-1").Emit("deployment-room-broadcast")
			ack(nil, nil)
		})
		_ = client.On("deployment-trigger-multiple-rooms", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = server.To("deployment-room-a", "deployment-room-b").Emit("deployment-multiple-rooms")
			ack(nil, nil)
		})
		_ = client.On("deployment-trigger-except-room", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = server.Except("deployment-excluded-room").Emit("deployment-except-room")
			ack(nil, nil)
		})
		_ = client.On("deployment-trigger-middleware-room", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = server.To("deployment-middleware-room").Emit("deployment-middleware-room")
			ack(nil, nil)
		})
		_ = client.On("deployment-trigger-after-leave", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			_ = server.To("deployment-leave-room").Emit("deployment-after-leave")
			ack(nil, nil)
		})
		_ = client.On("trigger-broadcast-ack", func(args ...any) {
			controlAck := args[len(args)-1].(Ack)
			mode := fmt.Sprint(args[0])
			operator := server.Timeout(250 * time.Millisecond)
			if mode == "zero-clients" {
				operator = operator.To("matrix-empty-room")
			}
			operator.EmitWithAck("broadcast-ack-request", mode)(func(responses []any, err error) {
				controlAck([]any{err != nil, responses}, nil)
			})
		})
		_ = client.On("begin-recovery", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack([]any{"closing transport"}, nil)
			time.AfterFunc(30*time.Millisecond, func() {
				client.Conn().Transport().Close()
				time.AfterFunc(50*time.Millisecond, func() {
					_ = server.To("recovery-room").Emit("missed-event", "stored while disconnected")
				})
			})
		})
		_ = client.On("recovery-state", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack([]any{
				client.Recovered(),
				compatibilitySocketDataMarker(client.Data()),
				client.Rooms().Has("recovery-room"),
				recoveryMiddlewareCalls.Load(),
			}, nil)
		})
		_ = client.On("prepare-unrecoverable-disconnect", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack([]any{"disconnecting namespace"}, nil)
			time.AfterFunc(20*time.Millisecond, func() {
				client.Disconnect(false)
			})
		})
	}

	root := server.Of("/", register)
	root.Use(func(client *Socket, next func(*ExtendedError)) {
		// The official uws.ts suite verifies that rooms joined from a
		// namespace middleware are immediately visible to broadcasts. The Go
		// HTTP engine must provide the same Socket.IO behavior independently
		// of the Node-specific uWebSockets.js attachment API.
		client.Join("deployment-middleware-room")
		next(nil)
	})
	_ = root.On("connection", func(args ...any) {
		client := args[0].(*Socket)
		_ = client.On("slow-namespace-connections", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			ack([]any{slowNamespaceConnections.Load()}, nil)
		})
	})
	server.Of("/custom", register)
	ordered := server.Of("/ordered", nil)
	_ = ordered.On("connection", func(args ...any) {
		client := args[0].(*Socket)
		values := make([]string, 0, 2)
		_ = client.On("ordered", func(args ...any) {
			values = append(values, fmt.Sprint(args[0]))
			if len(values) == 2 {
				_ = client.Emit("ordered-result", append([]string(nil), values...))
			}
		})
	})
	if server.Opts().ConnectionStateRecovery() != nil {
		root.Use(func(_ *Socket, next func(*ExtendedError)) {
			recoveryMiddlewareCalls.Add(1)
			next(nil)
		})
	}
	dynamicMatcher := func(name string, _ map[string]any, next func(error, bool)) {
		time.AfterFunc(20*time.Millisecond, func() {
			next(nil, strings.HasPrefix(name, "/dynamic-") && len(name) > len("/dynamic-"))
		})
	}
	server.Of(ParentNspNameMatchFn(&dynamicMatcher), register)
	secure := server.Of("/secure", register)
	secure.Use(func(client *Socket, next func(*ExtendedError)) {
		if compatibilityAuthToken(client.Handshake().Auth) == "matrix-secret" {
			next(nil)
			return
		}
		next(NewExtendedError("unauthorized", nil))
	})
	_ = secure.On("connect", func(args ...any) {
		client := args[0].(*Socket)
		_ = client.On("identity", func(args ...any) {
			ack := args[len(args)-1].(Ack)
			major := strings.TrimPrefix(compatibilityAuthValue(client.Handshake().Auth, "clientMajor"), "v")
			if major == "" {
				major = "unknown"
			}
			ack([]any{"v" + major + "-authorized"}, nil)
		})
	})
	slow := server.Of("/slow", nil)
	slow.Use(func(_ *Socket, next func(*ExtendedError)) {
		time.AfterFunc(150*time.Millisecond, func() {
			next(nil)
		})
	})
	_ = slow.On("connection", func(...any) {
		slowNamespaceConnections.Add(1)
	})
}

func compatibilitySocketDataMarker(data any) string {
	values, ok := data.(map[string]any)
	if !ok {
		return ""
	}
	return fmt.Sprint(values["marker"])
}

func compatibilityAuthToken(auth map[string]any) string {
	return compatibilityAuthValue(auth, "token")
}

func compatibilityAuthValue(auth map[string]any, key string) string {
	switch value := auth[key].(type) {
	case string:
		return value
	case []string:
		if len(value) > 0 {
			return value[0]
		}
	case []any:
		if len(value) > 0 {
			return fmt.Sprint(value[0])
		}
	}
	return ""
}

func newCompatibilityServer(t *testing.T, allowEIO3 bool, recovery bool) *httptest.Server {
	t.Helper()
	options := DefaultServerOptions()
	options.SetAllowEIO3(allowEIO3)
	if recovery {
		options.SetConnectionStateRecovery(DefaultConnectionStateRecovery())
	}
	return newCompatibilityServerWithOptions(t, options)
}

func newCompatibilityRecoveryServer(t *testing.T, skipMiddlewares bool) *httptest.Server {
	t.Helper()
	options := DefaultServerOptions()
	recovery := DefaultConnectionStateRecovery()
	recovery.SetSkipMiddlewares(skipMiddlewares)
	options.SetConnectionStateRecovery(recovery)
	return newCompatibilityServerWithOptions(t, options)
}

func newCompatibilityCleanupServer(t *testing.T) *httptest.Server {
	t.Helper()
	options := DefaultServerOptions()
	options.SetAllowEIO3(true)
	options.SetCleanupEmptyChildNamespaces(true)
	return newCompatibilityServerWithOptions(t, options)
}

func newCompatibilityLargePayloadServer(t *testing.T) *httptest.Server {
	t.Helper()
	options := DefaultServerOptions()
	// The Engine.IO limit is deliberately explicit: the official Socket.IO
	// large-payload tests exercise multi-megabyte parser round trips, while the
	// production default must remain the safer 1 MB limit.
	options.SetMaxHttpBufferSize(8 << 20)
	return newCompatibilityServerWithOptions(t, options)
}

func newCompatibilityLifecycleServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving lifecycle server port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("releasing lifecycle server port: %v", err)
	}

	options := DefaultServerOptions()
	options.SetAllowEIO3(true)
	server := NewServer(nil, options)
	registerCompatibilitySocketHandlers(server)
	var shuttingDown atomic.Bool
	_ = server.On("connection", func(args ...any) {
		client := args[0].(*Socket)
		_ = client.On("request-server-restart", func(values ...any) {
			ack := values[len(values)-1].(Ack)
			ack([]any{"restarting"}, nil)
			time.AfterFunc(20*time.Millisecond, func() {
				closed := make(chan struct{})
				server.Close(func(error) { close(closed) })
				<-closed
				if !shuttingDown.Load() {
					server.Listen(address, nil)
				}
			})
		})
		_ = client.On("request-server-shutdown", func(values ...any) {
			ack := values[len(values)-1].(Ack)
			ack([]any{"shutting-down"}, nil)
			shuttingDown.Store(true)
			time.AfterFunc(20*time.Millisecond, func() { server.Close(nil) })
		})
	})
	server.Listen(address, nil)
	t.Cleanup(func() {
		shuttingDown.Store(true)
		server.Close(nil)
	})
	return "http://" + address
}

func newCompatibilityServerWithOptions(t *testing.T, options ServerOptionsInterface) *httptest.Server {
	t.Helper()
	server := NewServer(nil, options)
	registerCompatibilitySocketHandlers(server)
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})
	return httpServer
}

func TestOfficialJavaScriptClientCompatibilityMatrix(t *testing.T) {
	if os.Getenv(compatibilityTestEnv) != "1" {
		t.Skip("set SOCKET_IO_COMPAT_TEST=1 after running npm ci in testdata/compatibility")
	}
	if os.Getenv("SOCKET_IO_COMPAT_DEBUG") == "1" {
		socketlog.DEBUG.Store(true)
		defer socketlog.DEBUG.Store(false)
	}

	workingDirectory := filepath.Join("testdata", "compatibility")
	for _, major := range []string{"2", "3", "4"} {
		t.Run("v"+major, func(t *testing.T) {
			strictServer := newCompatibilityServer(t, false, false)
			compatibilityServer := newCompatibilityServer(t, true, false)
			recoveryServer := newCompatibilityServer(t, false, true)
			recoveryWithMiddlewareServer := newCompatibilityRecoveryServer(t, false)
			cleanupServer := newCompatibilityCleanupServer(t)
			largePayloadServer := newCompatibilityLargePayloadServer(t)
			lifecycleServerURL := newCompatibilityLifecycleServer(t)

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "node", "matrix.cjs", strictServer.URL, compatibilityServer.URL, recoveryServer.URL, recoveryWithMiddlewareServer.URL, cleanupServer.URL, largePayloadServer.URL, lifecycleServerURL, major)
			command.Dir = workingDirectory
			command.Stdout = os.Stdout
			command.Stderr = os.Stderr
			err := command.Run()
			if err != nil {
				if ctx.Err() != nil {
					t.Fatalf("official JavaScript v%s client compatibility matrix timed out: %v", major, ctx.Err())
				}
				t.Fatalf("official JavaScript v%s client compatibility matrix failed: %v", major, err)
			}
		})
	}
}
