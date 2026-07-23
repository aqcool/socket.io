package main

import (
	"net"
	"net/http"
	"testing"
	"time"

	io_client "github.com/aqcool/socket.io/clients/socket/v3"
	io "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

func setupAuthServer(t *testing.T) (*io.Server, string) {
	t.Helper()

	config := io.DefaultServerOptions()
	config.SetCors(&types.Cors{Origin: "*"})

	srv := io.NewServer(nil, config)

	// Authentication middleware
	srv.Use(func(s *io.Socket, next func(*io.ExtendedError)) {
		auth := s.Handshake().Auth
		if auth == nil {
			next(io.NewExtendedError("authentication error", map[string]any{
				"message": "no auth data provided",
			}))
			return
		}

		token, ok := auth["token"].(string)
		if !ok || token == "" {
			next(io.NewExtendedError("authentication error", map[string]any{
				"message": "no token provided",
			}))
			return
		}

		username, valid := validTokens[token]
		if !valid {
			next(io.NewExtendedError("authentication error", map[string]any{
				"message": "invalid token",
			}))
			return
		}

		s.SetData(map[string]any{
			"username": username,
			"token":    token,
			"isAdmin":  adminTokens[token],
		})

		next(nil)
	})

	srv.On("connection", func(clients ...any) {
		if len(clients) == 0 {
			return
		}
		client, ok := clients[0].(*io.Socket)
		if !ok {
			return
		}

		userData := client.Data().(map[string]any)
		username := userData["username"].(string)

		client.Emit("welcome", map[string]any{
			"message":  "Welcome, " + username + "!",
			"username": username,
		})

		client.On("profile", func(args ...any) {
			if len(args) > 0 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack([]any{userData}, nil)
				}
			}
		})

		client.On("disconnect", func(args ...any) {
			client.Broadcast().Emit("user:disconnected", map[string]any{
				"username": username,
			})
		})
	})

	// Admin namespace
	adminNsp := srv.Of("/admin", nil)
	adminNsp.Use(func(s *io.Socket, next func(*io.ExtendedError)) {
		auth := s.Handshake().Auth
		if auth == nil {
			next(io.NewExtendedError("authorization error", map[string]any{
				"message": "not authorized",
			}))
			return
		}
		token, ok := auth["token"].(string)
		if !ok || !adminTokens[token] {
			next(io.NewExtendedError("authorization error", map[string]any{
				"message": "admin access required",
			}))
			return
		}

		username := validTokens[token]
		s.SetData(map[string]any{
			"username": username,
			"isAdmin":  true,
		})
		next(nil)
	})

	adminNsp.On("connection", func(clients ...any) {
		if len(clients) == 0 {
			return
		}
		client, ok := clients[0].(*io.Socket)
		if !ok {
			return
		}
		userData := client.Data().(map[string]any)
		client.Emit("admin:welcome", map[string]any{
			"message": "Welcome to admin panel, " + userData["username"].(string) + "!",
		})

		client.On("admin:action", func(args ...any) {
			if len(args) > 0 {
				client.Emit("admin:action:result", map[string]any{
					"success": true,
					"action":  args[0],
				})
			}
		})
	})

	httpServer := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: srv.ServeHandler(nil),
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	go httpServer.Serve(ln)

	// Poll for server readiness
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond)
		srv.Close(nil)
		httpServer.Close()
	})

	return srv, addr
}

func connectWithAuth(t *testing.T, addr, nsp string, auth map[string]any, waitEvents ...string) (*io_client.Socket, map[string][]any) {
	t.Helper()

	const maxAttempts = 3
	const timeout = 5 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		managerOpts := io_client.DefaultManagerOptions()
		managerOpts.SetAutoConnect(false)
		managerOpts.SetReconnection(false)

		sockOpts := io_client.DefaultSocketOptions()
		sockOpts.SetAuth(auth)

		manager := io_client.NewManager("http://"+addr, managerOpts)
		client := manager.Socket(nsp, sockOpts)

		connectCh := make(chan struct{}, 1)
		eventChannels := make(map[string]chan []any)
		for _, evt := range waitEvents {
			eventChannels[evt] = make(chan []any, 1)
		}

		client.On("connect", func(args ...any) {
			select {
			case connectCh <- struct{}{}:
			default:
			}
		})

		for evt, ch := range eventChannels {
			e := evt
			c := ch
			client.On(types.EventName(e), func(args ...any) {
				select {
				case c <- args:
				default:
				}
			})
		}

		client.Connect()

		select {
		case <-connectCh:
			results := make(map[string][]any)
			allGood := true
			for evt, ch := range eventChannels {
				select {
				case args := <-ch:
					results[evt] = args
				case <-time.After(timeout):
					t.Logf("attempt %d: connected but timed out waiting for %q", attempt, evt)
					allGood = false
				}
				if !allGood {
					break
				}
			}
			if allGood {
				t.Cleanup(func() { client.Disconnect() })
				return client, results
			}
			client.Disconnect()
		case <-time.After(timeout):
			client.Disconnect()
			if attempt < maxAttempts {
				t.Logf("connect attempt %d failed, retrying...", attempt)
			}
		}
	}
	t.Fatal("failed to connect after 3 attempts")
	return nil, nil
}

func newClientWithAuth(t *testing.T, addr, nsp string, auth map[string]any) *io_client.Socket {
	t.Helper()

	managerOpts := io_client.DefaultManagerOptions()
	managerOpts.SetAutoConnect(false)
	managerOpts.SetReconnection(false)

	sockOpts := io_client.DefaultSocketOptions()
	if auth != nil {
		sockOpts.SetAuth(auth)
	}

	manager := io_client.NewManager("http://"+addr, managerOpts)
	client := manager.Socket(nsp, sockOpts)
	t.Cleanup(func() { client.Disconnect() })
	return client
}

func TestAuthSuccessfulConnection(t *testing.T) {
	_, addr := setupAuthServer(t)

	_, events := connectWithAuth(t, addr, "/", map[string]any{"token": "token-alice"}, "welcome")

	args := events["welcome"]
	if len(args) == 0 {
		t.Fatal("expected welcome data")
	}

	data, ok := args[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", args[0])
	}

	if data["username"] != "Alice" {
		t.Fatalf("expected username Alice, got %v", data["username"])
	}
}

func TestAuthNoToken(t *testing.T) {
	_, addr := setupAuthServer(t)

	client := newClientWithAuth(t, addr, "/", map[string]any{})

	errCh := make(chan []any, 1)
	client.On("connect_error", func(args ...any) {
		select {
		case errCh <- args:
		default:
		}
	})
	client.Connect()

	select {
	case args := <-errCh:
		if len(args) == 0 {
			t.Fatal("expected error data")
		}
		t.Logf("Connection correctly rejected: %v", args[0])
	case <-time.After(5 * time.Second):
		t.Fatal("expected connect_error, but timed out")
	}
}

func TestAuthInvalidToken(t *testing.T) {
	_, addr := setupAuthServer(t)

	client := newClientWithAuth(t, addr, "/", map[string]any{"token": "invalid-token"})

	errCh := make(chan []any, 1)
	client.On("connect_error", func(args ...any) {
		select {
		case errCh <- args:
		default:
		}
	})
	client.Connect()

	select {
	case args := <-errCh:
		if len(args) == 0 {
			t.Fatal("expected error data")
		}
		t.Logf("Connection correctly rejected: %v", args[0])
	case <-time.After(5 * time.Second):
		t.Fatal("expected connect_error, but timed out")
	}
}

func TestAuthNoAuthData(t *testing.T) {
	_, addr := setupAuthServer(t)

	client := newClientWithAuth(t, addr, "/", nil)

	errCh := make(chan []any, 1)
	client.On("connect_error", func(args ...any) {
		select {
		case errCh <- args:
		default:
		}
	})
	client.Connect()

	select {
	case args := <-errCh:
		if len(args) == 0 {
			t.Fatal("expected error data")
		}
		t.Logf("Connection correctly rejected: %v", args[0])
	case <-time.After(5 * time.Second):
		t.Fatal("expected connect_error, but timed out")
	}
}

func TestAuthProfileWithAck(t *testing.T) {
	_, addr := setupAuthServer(t)

	client, _ := connectWithAuth(t, addr, "/", map[string]any{"token": "token-bob"}, "welcome")

	ackCh := make(chan []any, 1)
	client.EmitWithAck("profile")(func(args []any, err error) {
		if err != nil {
			t.Fatalf("ack error: %v", err)
		}
		ackCh <- args
	})

	select {
	case args := <-ackCh:
		if len(args) == 0 {
			t.Fatal("expected profile data")
		}
		data, ok := args[0].(map[string]any)
		if !ok {
			t.Fatalf("expected map, got %T", args[0])
		}
		if data["username"] != "Bob" {
			t.Fatalf("expected username Bob, got %v", data["username"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for profile ack")
	}
}

func TestAuthAdminNamespace(t *testing.T) {
	_, addr := setupAuthServer(t)

	_, events := connectWithAuth(t, addr, "/admin", map[string]any{"token": "token-admin"}, "admin:welcome")

	args := events["admin:welcome"]
	if len(args) == 0 {
		t.Fatal("expected admin welcome data")
	}

	data, ok := args[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", args[0])
	}

	if data["message"] != "Welcome to admin panel, Admin!" {
		t.Fatalf("unexpected admin welcome message: %v", data["message"])
	}
}

func TestAuthAdminNamespaceRejection(t *testing.T) {
	_, addr := setupAuthServer(t)

	client := newClientWithAuth(t, addr, "/admin", map[string]any{"token": "token-alice"})

	errCh := make(chan []any, 1)
	client.On("connect_error", func(args ...any) {
		select {
		case errCh <- args:
		default:
		}
	})
	client.Connect()

	select {
	case args := <-errCh:
		if len(args) == 0 {
			t.Fatal("expected error data")
		}
		t.Logf("Admin access correctly rejected for non-admin: %v", args[0])
	case <-time.After(5 * time.Second):
		t.Fatal("expected connect_error for non-admin on /admin namespace")
	}
}

func TestAuthAdminAction(t *testing.T) {
	_, addr := setupAuthServer(t)

	client, _ := connectWithAuth(t, addr, "/admin", map[string]any{"token": "token-admin"}, "admin:welcome")

	resultCh := make(chan map[string]any, 1)
	client.On("admin:action:result", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				resultCh <- data
			}
		}
	})

	client.Emit("admin:action", "reset_cache")

	select {
	case result := <-resultCh:
		if result["success"] != true {
			t.Fatalf("expected success=true, got %v", result["success"])
		}
		if result["action"] != "reset_cache" {
			t.Fatalf("expected action 'reset_cache', got %v", result["action"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for admin:action:result")
	}
}

func TestAuthMultipleUsersConnection(t *testing.T) {
	_, addr := setupAuthServer(t)

	// Connect Alice
	alice, _ := connectWithAuth(t, addr, "/", map[string]any{"token": "token-alice"}, "welcome")

	// Alice listens for user disconnected
	disconnectedCh := make(chan map[string]any, 1)
	alice.On("user:disconnected", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				disconnectedCh <- data
			}
		}
	})

	// Connect Bob
	bob, _ := connectWithAuth(t, addr, "/", map[string]any{"token": "token-bob"}, "welcome")

	// Bob disconnects
	bob.Disconnect()

	// Alice should receive user:disconnected
	select {
	case data := <-disconnectedCh:
		if data["username"] != "Bob" {
			t.Fatalf("expected username Bob, got %v", data["username"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for user:disconnected event")
	}
}
