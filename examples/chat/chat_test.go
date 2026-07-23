package main

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	io_client "github.com/aqcool/socket.io/clients/socket/v3"
	io "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

// setupServer creates a chat server for testing and returns the server, address, and cleanup function.
func setupServer(t *testing.T) (*io.Server, string) {
	t.Helper()

	var userCount int64

	config := io.DefaultServerOptions()
	config.SetCors(&types.Cors{Origin: "*"})

	srv := io.NewServer(nil, config)

	srv.On("connection", func(clients ...any) {
		if len(clients) == 0 {
			return
		}
		client, ok := clients[0].(*io.Socket)
		if !ok {
			return
		}

		addedUser := false

		client.On("new message", func(args ...any) {
			if !addedUser || len(args) == 0 {
				return
			}
			client.Broadcast().Emit("new message", map[string]any{
				"username": client.Data(),
				"message":  args[0],
			})
		})

		client.On("add user", func(args ...any) {
			if addedUser || len(args) == 0 {
				return
			}
			username, ok := args[0].(string)
			if !ok {
				return
			}
			client.SetData(username)
			current := atomic.AddInt64(&userCount, 1)
			addedUser = true
			client.Emit("login", map[string]any{
				"numUsers": current,
			})
			client.Broadcast().Emit("user joined", map[string]any{
				"username": username,
				"numUsers": current,
			})
		})

		client.On("typing", func(args ...any) {
			if !addedUser {
				return
			}
			client.Broadcast().Emit("typing", map[string]any{
				"username": client.Data(),
			})
		})

		client.On("stop typing", func(args ...any) {
			if !addedUser {
				return
			}
			client.Broadcast().Emit("stop typing", map[string]any{
				"username": client.Data(),
			})
		})

		client.On("disconnect", func(args ...any) {
			if addedUser {
				current := atomic.AddInt64(&userCount, -1)
				client.Broadcast().Emit("user left", map[string]any{
					"username": client.Data(),
					"numUsers": current,
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

	// Wait for server to start
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		srv.Close(nil)
		httpServer.Close()
		time.Sleep(100 * time.Millisecond)
	})

	return srv, addr
}

func connectClient(t *testing.T, addr, nsp string) *io_client.Socket {
	t.Helper()

	var client *io_client.Socket
	const maxRetries = 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		opts := io_client.DefaultManagerOptions()
		opts.SetAutoConnect(false)
		opts.SetReconnection(false)

		manager := io_client.NewManager("http://"+addr, opts)
		client = manager.Socket(nsp, nil)

		connected := make(chan struct{}, 1)
		client.On("connect", func(args ...any) {
			select {
			case connected <- struct{}{}:
			default:
			}
		})

		client.Connect()

		select {
		case <-connected:
			t.Cleanup(func() {
				client.Disconnect()
				time.Sleep(50 * time.Millisecond)
			})
			return client
		case <-time.After(2 * time.Second):
			client.Disconnect()
			time.Sleep(50 * time.Millisecond)
			if attempt < maxRetries-1 {
				t.Logf("connect attempt %d failed, retrying...", attempt+1)
			}
		}
	}

	t.Fatal("failed to connect after retries")
	return nil
}

func waitForEvent(t *testing.T, client *io_client.Socket, event string, timeout time.Duration) []any {
	t.Helper()
	done := make(chan []any, 1)
	client.On(types.EventName(event), func(args ...any) {
		done <- args
	})
	select {
	case args := <-done:
		return args
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for event %q", event)
		return nil
	}
}

func TestAddUser(t *testing.T) {
	_, addr := setupServer(t)

	client := connectClient(t, addr, "/")

	// Add user
	client.Emit("add user", "Alice")

	// Wait for login response
	args := waitForEvent(t, client, "login", 2*time.Second)
	if len(args) == 0 {
		t.Fatal("expected login data")
	}

	data, ok := args[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", args[0])
	}

	if numUsers, ok := data["numUsers"]; !ok || numUsers == nil {
		t.Fatal("expected numUsers in login data")
	}
}

func TestNewMessage(t *testing.T) {
	_, addr := setupServer(t)

	client1 := connectClient(t, addr, "/")

	client2 := connectClient(t, addr, "/")

	// Add users
	client1.Emit("add user", "Alice")
	waitForEvent(t, client1, "login", 2*time.Second)

	client2.Emit("add user", "Bob")
	waitForEvent(t, client2, "login", 2*time.Second)

	// Client2 listens for new messages
	msgCh := make(chan map[string]any, 1)
	client2.On("new message", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				msgCh <- data
			}
		}
	})

	// Client1 sends a message
	client1.Emit("new message", "Hello everyone!")

	select {
	case msg := <-msgCh:
		if msg["username"] != "Alice" {
			t.Fatalf("expected username Alice, got %v", msg["username"])
		}
		if msg["message"] != "Hello everyone!" {
			t.Fatalf("expected message 'Hello everyone!', got %v", msg["message"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for new message")
	}
}

func TestTypingIndicator(t *testing.T) {
	_, addr := setupServer(t)

	client1 := connectClient(t, addr, "/")

	client2 := connectClient(t, addr, "/")

	client1.Emit("add user", "Alice")
	waitForEvent(t, client1, "login", 2*time.Second)

	client2.Emit("add user", "Bob")
	waitForEvent(t, client2, "login", 2*time.Second)

	// Client2 listens for typing events
	typingCh := make(chan map[string]any, 1)
	client2.On("typing", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				typingCh <- data
			}
		}
	})

	stopTypingCh := make(chan map[string]any, 1)
	client2.On("stop typing", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				stopTypingCh <- data
			}
		}
	})

	client1.Emit("typing")

	select {
	case data := <-typingCh:
		if data["username"] != "Alice" {
			t.Fatalf("expected username Alice, got %v", data["username"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for typing event")
	}

	client1.Emit("stop typing")

	select {
	case data := <-stopTypingCh:
		if data["username"] != "Alice" {
			t.Fatalf("expected username Alice, got %v", data["username"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for stop typing event")
	}
}

func TestUserJoinedAndLeft(t *testing.T) {
	_, addr := setupServer(t)

	client1 := connectClient(t, addr, "/")

	client1.Emit("add user", "Alice")
	waitForEvent(t, client1, "login", 2*time.Second)

	// Listen for user joined
	joinedCh := make(chan map[string]any, 1)
	client1.On("user joined", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				joinedCh <- data
			}
		}
	})

	// Listen for user left
	leftCh := make(chan map[string]any, 1)
	client1.On("user left", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				leftCh <- data
			}
		}
	})

	// Client2 connects and adds user
	client2 := connectClient(t, addr, "/")
	client2.Emit("add user", "Bob")

	select {
	case data := <-joinedCh:
		if data["username"] != "Bob" {
			t.Fatalf("expected username Bob, got %v", data["username"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for user joined event")
	}

	// Client2 disconnects
	client2.Disconnect()

	select {
	case data := <-leftCh:
		if data["username"] != "Bob" {
			t.Fatalf("expected username Bob, got %v", data["username"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for user left event")
	}
}

func TestMultipleUsers(t *testing.T) {
	_, addr := setupServer(t)

	const userCount = 5
	clients := make([]*io_client.Socket, userCount)
	var wg sync.WaitGroup

	for i := 0; i < userCount; i++ {
		clients[i] = connectClient(t, addr, "/")
	}

	// Add all users
	for i := 0; i < userCount; i++ {
		clients[i].Emit("add user", fmt.Sprintf("User%d", i))
		waitForEvent(t, clients[i], "login", 2*time.Second)
	}

	// Client0 listens for messages from all others
	msgCount := int32(0)
	wg.Add(1)
	clients[0].On("new message", func(args ...any) {
		if atomic.AddInt32(&msgCount, 1) == int32(userCount-1) {
			wg.Done()
		}
	})

	// All other clients send messages
	for i := 1; i < userCount; i++ {
		clients[i].Emit("new message", fmt.Sprintf("Hello from User%d", i))
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All messages received
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout: only received %d/%d messages", atomic.LoadInt32(&msgCount), userCount-1)
	}

	// Cleanup handled by t.Cleanup
}

func TestDuplicateAddUser(t *testing.T) {
	_, addr := setupServer(t)

	client := connectClient(t, addr, "/")

	loginCount := int32(0)
	client.On("login", func(args ...any) {
		atomic.AddInt32(&loginCount, 1)
	})

	// Add user twice
	client.Emit("add user", "Alice")
	time.Sleep(500 * time.Millisecond)
	client.Emit("add user", "Alice")
	time.Sleep(500 * time.Millisecond)

	// Should only receive one login event
	if count := atomic.LoadInt32(&loginCount); count != 1 {
		t.Fatalf("expected 1 login event, got %d", count)
	}
}
