package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"

	io "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

// Chat room example - Go implementation of the classic Socket.IO chat demo.
//
// Features:
//   - Multiple users join a chat room with unique usernames
//   - Users send chat messages to all connected users
//   - Typing indicator broadcasts
//   - Join/leave notifications with user count

var numUsers int64

func main() {
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

		addedUser := false

		// When the client emits 'new message', broadcast it to others
		client.On("new message", func(args ...any) {
			if !addedUser || len(args) == 0 {
				return
			}
			client.Broadcast().Emit("new message", map[string]any{
				"username": client.Data(),
				"message":  args[0],
			})
		})

		// When the client emits 'add user', register the username
		client.On("add user", func(args ...any) {
			if addedUser || len(args) == 0 {
				return
			}

			username, ok := args[0].(string)
			if !ok {
				return
			}

			// Store username in socket data
			client.SetData(username)
			current := atomic.AddInt64(&numUsers, 1)
			addedUser = true

			client.Emit("login", map[string]any{
				"numUsers": current,
			})

			// Broadcast that a new user has joined
			client.Broadcast().Emit("user joined", map[string]any{
				"username": username,
				"numUsers": current,
			})
		})

		// When the client emits 'typing', broadcast to others
		client.On("typing", func(args ...any) {
			if !addedUser {
				return
			}
			client.Broadcast().Emit("typing", map[string]any{
				"username": client.Data(),
			})
		})

		// When the client emits 'stop typing', broadcast to others
		client.On("stop typing", func(args ...any) {
			if !addedUser {
				return
			}
			client.Broadcast().Emit("stop typing", map[string]any{
				"username": client.Data(),
			})
		})

		// When the user disconnects
		client.On("disconnect", func(args ...any) {
			if addedUser {
				current := atomic.AddInt64(&numUsers, -1)
				client.Broadcast().Emit("user left", map[string]any{
					"username": client.Data(),
					"numUsers": current,
				})
			}
		})
	})

	addr := ":3000"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}

	httpServer.Listen(addr, nil)
	fmt.Printf("Chat server listening on %s\n", addr)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Println("Shutting down server...")
	server.Close(nil)
}
