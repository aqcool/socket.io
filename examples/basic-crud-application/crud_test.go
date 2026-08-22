package main

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	io_client "github.com/aqcool/socket.io/clients/socket/v4"
	io "github.com/aqcool/socket.io/servers/socket/v4"
	types "github.com/aqcool/socket.io/v4/pkg/types"
)

func setupCRUDServer(t *testing.T) (*io.Server, *TodoStore, string) {
	t.Helper()

	store := NewTodoStore()

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

		client.Emit("todo:list", store.List())

		client.On("todo:create", func(args ...any) {
			if len(args) == 0 {
				return
			}
			data, ok := args[0].(map[string]any)
			if !ok {
				return
			}
			title, ok := data["title"].(string)
			if !ok || title == "" {
				return
			}
			item := store.Create(title)
			srv.Emit("todo:created", item)
			if len(args) > 1 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack([]any{item}, nil)
				}
			}
		})

		client.On("todo:read", func(args ...any) {
			items := store.List()
			if len(args) > 0 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack([]any{items}, nil)
				}
			}
		})

		client.On("todo:update", func(args ...any) {
			if len(args) == 0 {
				return
			}
			data, ok := args[0].(map[string]any)
			if !ok {
				return
			}
			idFloat, ok := data["id"].(float64)
			if !ok {
				return
			}
			id := int64(idFloat)
			title, _ := data["title"].(string)
			completed, _ := data["completed"].(bool)
			item, found := store.Update(id, title, completed)
			if !found {
				return
			}
			srv.Emit("todo:updated", item)
			if len(args) > 1 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack([]any{item}, nil)
				}
			}
		})

		client.On("todo:delete", func(args ...any) {
			if len(args) == 0 {
				return
			}
			data, ok := args[0].(map[string]any)
			if !ok {
				return
			}
			idFloat, ok := data["id"].(float64)
			if !ok {
				return
			}
			id := int64(idFloat)
			if !store.Delete(id) {
				return
			}
			srv.Emit("todo:deleted", map[string]any{"id": id})
			if len(args) > 1 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack([]any{map[string]any{"id": id}}, nil)
				}
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

	// Wait until the server is actually accepting connections
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Server cleanup registered FIRST, so it runs LAST (after client cleanups)
	t.Cleanup(func() {
		srv.Close(nil)
		httpServer.Close()
		time.Sleep(100 * time.Millisecond)
	})

	return srv, store, addr
}

func connectCRUDClient(t *testing.T, addr string) *io_client.Socket {
	t.Helper()

	var client *io_client.Socket
	const maxRetries = 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		opts := io_client.DefaultManagerOptions()
		opts.SetAutoConnect(false)
		opts.SetReconnection(false)

		manager := io_client.NewManager("http://"+addr, opts)
		client = manager.Socket("/", nil)

		connected := make(chan struct{}, 1)
		client.On("connect", func(args ...any) {
			select {
			case connected <- struct{}{}:
			default:
			}
		})

		listDone := make(chan struct{}, 1)
		client.On(types.EventName("todo:list"), func(args ...any) {
			select {
			case listDone <- struct{}{}:
			default:
			}
		})

		client.Connect()

		ok := true
		select {
		case <-connected:
		case <-time.After(2 * time.Second):
			client.Disconnect()
			time.Sleep(50 * time.Millisecond)
			ok = false
		}

		if ok {
			select {
			case <-listDone:
			case <-time.After(2 * time.Second):
				client.Disconnect()
				time.Sleep(50 * time.Millisecond)
				ok = false
			}
		}

		if ok {
			t.Cleanup(func() {
				client.Disconnect()
				time.Sleep(50 * time.Millisecond)
			})
			return client
		}

		if attempt < maxRetries-1 {
			t.Logf("connect attempt %d failed, retrying...", attempt+1)
		}
	}

	t.Fatal("failed to connect after retries")
	return nil
}

func waitEvent(t *testing.T, client *io_client.Socket, event string, timeout time.Duration) []any {
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

func TestCRUDCreate(t *testing.T) {
	_, _, addr := setupCRUDServer(t)

	client := connectCRUDClient(t, addr)

	// Create a todo
	createdCh := make(chan map[string]any, 1)
	client.On("todo:created", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				createdCh <- data
			}
		}
	})

	client.Emit("todo:create", map[string]any{"title": "Buy groceries"})

	select {
	case item := <-createdCh:
		if item["title"] != "Buy groceries" {
			t.Fatalf("expected title 'Buy groceries', got %v", item["title"])
		}
		if item["completed"] != false {
			t.Fatalf("expected completed=false, got %v", item["completed"])
		}
		if item["id"] == nil {
			t.Fatal("expected id to be set")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for todo:created")
	}
}

func TestCRUDCreateWithAck(t *testing.T) {
	_, _, addr := setupCRUDServer(t)

	client := connectCRUDClient(t, addr)

	// Create with ack
	ackCh := make(chan []any, 1)
	client.EmitWithAck("todo:create", map[string]any{"title": "Walk the dog"})(func(args []any, err error) {
		if err != nil {
			t.Fatalf("ack error: %v", err)
		}
		ackCh <- args
	})

	select {
	case args := <-ackCh:
		if len(args) == 0 {
			t.Fatal("expected ack data")
		}
		item, ok := args[0].(map[string]any)
		if !ok {
			t.Fatalf("expected map, got %T", args[0])
		}
		if item["title"] != "Walk the dog" {
			t.Fatalf("expected title 'Walk the dog', got %v", item["title"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack")
	}
}

func TestCRUDUpdate(t *testing.T) {
	_, store, addr := setupCRUDServer(t)

	// Pre-populate store
	item := store.Create("Learn Go")

	client := connectCRUDClient(t, addr)

	updatedCh := make(chan map[string]any, 1)
	client.On("todo:updated", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				updatedCh <- data
			}
		}
	})

	client.Emit("todo:update", map[string]any{
		"id":        float64(item.ID),
		"title":     "Learn Go (done!)",
		"completed": true,
	})

	select {
	case updated := <-updatedCh:
		if updated["title"] != "Learn Go (done!)" {
			t.Fatalf("expected updated title, got %v", updated["title"])
		}
		if updated["completed"] != true {
			t.Fatalf("expected completed=true, got %v", updated["completed"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for todo:updated")
	}
}

func TestCRUDDelete(t *testing.T) {
	_, store, addr := setupCRUDServer(t)

	item := store.Create("Delete me")

	client := connectCRUDClient(t, addr)

	deletedCh := make(chan map[string]any, 1)
	client.On("todo:deleted", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				deletedCh <- data
			}
		}
	})

	client.Emit("todo:delete", map[string]any{"id": float64(item.ID)})

	select {
	case deleted := <-deletedCh:
		if deleted["id"] != float64(item.ID) {
			t.Fatalf("expected id %d, got %v", item.ID, deleted["id"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for todo:deleted")
	}

	// Verify item is gone
	if _, found := store.Get(item.ID); found {
		t.Fatal("item should have been deleted")
	}
}

func TestCRUDBroadcastToMultipleClients(t *testing.T) {
	_, _, addr := setupCRUDServer(t)

	client1 := connectCRUDClient(t, addr)

	client2 := connectCRUDClient(t, addr)

	// Both clients listen for created events
	var wg sync.WaitGroup
	received := int32(0)

	wg.Add(2)
	for _, c := range []*io_client.Socket{client1, client2} {
		c.On("todo:created", func(args ...any) {
			if atomic.AddInt32(&received, 1) <= 2 {
				wg.Done()
			}
		})
	}

	// Client1 creates a todo
	client1.Emit("todo:create", map[string]any{"title": "Shared task"})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Both clients received the broadcast
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout: only %d/2 clients received the event", atomic.LoadInt32(&received))
	}
}

func TestCRUDReadWithAck(t *testing.T) {
	_, store, addr := setupCRUDServer(t)

	store.Create("Item 1")
	store.Create("Item 2")

	client := connectCRUDClient(t, addr)

	ackCh := make(chan []any, 1)
	client.EmitWithAck("todo:read")(func(args []any, err error) {
		if err != nil {
			t.Fatalf("ack error: %v", err)
		}
		ackCh <- args
	})

	select {
	case args := <-ackCh:
		if len(args) == 0 {
			t.Fatal("expected items list")
		}
		items, ok := args[0].([]any)
		if !ok {
			t.Fatalf("expected []any, got %T", args[0])
		}
		if len(items) != 2 {
			t.Fatalf("expected 2 items, got %d", len(items))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for read ack")
	}
}

func TestCRUDFullWorkflow(t *testing.T) {
	_, _, addr := setupCRUDServer(t)

	client := connectCRUDClient(t, addr)

	// Step 1: Create
	createCh := make(chan map[string]any, 1)
	client.On("todo:created", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				createCh <- data
			}
		}
	})
	client.Emit("todo:create", map[string]any{"title": "Full workflow test"})

	var createdID float64
	select {
	case item := <-createCh:
		createdID = item["id"].(float64)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout creating item")
	}

	// Step 2: Update
	updateCh := make(chan map[string]any, 1)
	client.On("todo:updated", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				updateCh <- data
			}
		}
	})
	client.Emit("todo:update", map[string]any{
		"id":        createdID,
		"title":     "Full workflow test (updated)",
		"completed": true,
	})

	select {
	case item := <-updateCh:
		if item["title"] != "Full workflow test (updated)" {
			t.Fatalf("unexpected title: %v", item["title"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout updating item")
	}

	// Step 3: Delete
	deleteCh := make(chan map[string]any, 1)
	client.On("todo:deleted", func(args ...any) {
		if len(args) > 0 {
			if data, ok := args[0].(map[string]any); ok {
				deleteCh <- data
			}
		}
	})
	client.Emit("todo:delete", map[string]any{"id": createdID})

	select {
	case deleted := <-deleteCh:
		if deleted["id"] != createdID {
			t.Fatalf("expected deleted id %v, got %v", createdID, deleted["id"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout deleting item")
	}
}
