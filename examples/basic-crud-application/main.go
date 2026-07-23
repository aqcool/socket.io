package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"

	io "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

// Basic CRUD application - demonstrates real-time CRUD operations with Socket.IO.
//
// Features:
//   - Create, read, update, delete TODO items
//   - All changes are broadcast to connected clients in real-time
//   - Each item has an ID, title, and completed status

type TodoItem struct {
	ID        int64  `json:"id"`
	Title     string `json:"title"`
	Completed bool   `json:"completed"`
}

type TodoStore struct {
	mu     sync.RWMutex
	items  map[int64]*TodoItem
	nextID int64
}

func NewTodoStore() *TodoStore {
	return &TodoStore{
		items: make(map[int64]*TodoItem),
	}
}

func (s *TodoStore) Create(title string) *TodoItem {
	id := atomic.AddInt64(&s.nextID, 1)
	item := &TodoItem{
		ID:        id,
		Title:     title,
		Completed: false,
	}
	s.mu.Lock()
	s.items[id] = item
	s.mu.Unlock()
	return item
}

func (s *TodoStore) List() []*TodoItem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*TodoItem, 0, len(s.items))
	for _, item := range s.items {
		result = append(result, item)
	}
	return result
}

func (s *TodoStore) Update(id int64, title string, completed bool) (*TodoItem, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[id]
	if !ok {
		return nil, false
	}
	item.Title = title
	item.Completed = completed
	return item, true
}

func (s *TodoStore) Delete(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[id]; !ok {
		return false
	}
	delete(s.items, id)
	return true
}

func (s *TodoStore) Get(id int64) (*TodoItem, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.items[id]
	return item, ok
}

func main() {
	store := NewTodoStore()

	config := io.DefaultServerOptions()
	config.SetCors(&types.Cors{Origin: "*"})

	httpServer := types.NewWebServer(nil)
	server := io.NewServer(httpServer, config)

	server.On("connection", func(clients ...any) {
		if len(clients) == 0 {
			return
		}
		client, ok := clients[0].(*io.Socket)
		if !ok {
			return
		}

		// Send current items on connection
		client.Emit("todo:list", store.List())

		// Create a new todo item
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

			// Broadcast to all clients including sender
			server.Emit("todo:created", item)

			// Send ack if requested
			if len(args) > 1 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack([]any{item}, nil)
				}
			}
		})

		// Read/list all items
		client.On("todo:read", func(args ...any) {
			items := store.List()
			if len(args) > 0 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack([]any{items}, nil)
				}
			}
		})

		// Update a todo item
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
				if len(args) > 1 {
					if ack, ok := args[len(args)-1].(func([]any, error)); ok {
						ack(nil, fmt.Errorf("item not found"))
					}
				}
				return
			}

			// Broadcast update to all clients
			server.Emit("todo:updated", item)

			if len(args) > 1 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack([]any{item}, nil)
				}
			}
		})

		// Delete a todo item
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
				if len(args) > 1 {
					if ack, ok := args[len(args)-1].(func([]any, error)); ok {
						ack(nil, fmt.Errorf("item not found"))
					}
				}
				return
			}

			// Broadcast deletion to all clients
			server.Emit("todo:deleted", map[string]any{"id": id})

			if len(args) > 1 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack([]any{map[string]any{"id": id}}, nil)
				}
			}
		})
	})

	addr := ":3000"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}

	httpServer.Listen(addr, nil)
	fmt.Printf("CRUD server listening on %s\n", addr)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Println("Shutting down server...")
	server.Close(nil)
}
