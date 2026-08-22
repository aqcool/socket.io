package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	io "github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

// Middleware authentication example - demonstrates server-side middleware
// for authenticating Socket.IO connections using token-based auth.
//
// Features:
//   - Namespace-level middleware for authentication
//   - Socket-level middleware for event authorization
//   - Token validation before connection is established
//   - Private namespace accessible only to authenticated users
//   - Room-based authorization

// validTokens simulates a set of valid authentication tokens.
var validTokens = map[string]string{
	"token-alice": "Alice",
	"token-bob":   "Bob",
	"token-admin": "Admin",
}

// adminTokens simulates tokens with admin privileges.
var adminTokens = map[string]bool{
	"token-admin": true,
}

func main() {
	config := io.DefaultServerOptions()
	config.SetCors(&types.Cors{Origin: "*"})

	httpServer := types.NewWebServer(nil)
	server := io.NewServer(httpServer, config)

	// Namespace-level middleware: authenticate the connection
	server.Use(func(s *io.Socket, next func(*io.ExtendedError)) {
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

		// Store the authenticated username in socket data
		s.SetData(map[string]any{
			"username": username,
			"token":    token,
			"isAdmin":  adminTokens[token],
		})

		next(nil)
	})

	server.On("connection", func(clients ...any) {
		if len(clients) == 0 {
			return
		}
		client, ok := clients[0].(*io.Socket)
		if !ok {
			return
		}

		userData := client.Data().(map[string]any)
		username := userData["username"].(string)

		fmt.Printf("User %q connected (id: %s)\n", username, client.Id())

		// Emit welcome message
		client.Emit("welcome", map[string]any{
			"message":  fmt.Sprintf("Welcome, %s!", username),
			"username": username,
		})

		// Broadcast to other users
		client.Broadcast().Emit("user:connected", map[string]any{
			"username": username,
		})

		// Handle profile request
		client.On("profile", func(args ...any) {
			if len(args) > 0 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack([]any{userData}, nil)
				}
			}
		})

		// Handle disconnect
		client.On("disconnect", func(args ...any) {
			fmt.Printf("User %q disconnected\n", username)
			client.Broadcast().Emit("user:disconnected", map[string]any{
				"username": username,
			})
		})
	})

	// Admin-only namespace with additional middleware
	adminNsp := server.Of("/admin", nil)

	adminNsp.Use(func(s *io.Socket, next func(*io.ExtendedError)) {
		auth := s.Handshake().Auth
		if auth == nil {
			next(io.NewExtendedError("authorization error", map[string]any{
				"message": "not authorized",
			}))
			return
		}

		token, ok := auth["token"].(string)
		if !ok {
			next(io.NewExtendedError("authorization error", map[string]any{
				"message": "not authorized",
			}))
			return
		}

		if !adminTokens[token] {
			next(io.NewExtendedError("authorization error", map[string]any{
				"message": "admin access required",
			}))
			return
		}

		username := validTokens[token]
		s.SetData(map[string]any{
			"username": username,
			"token":    token,
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
		username := userData["username"].(string)

		fmt.Printf("Admin %q connected to /admin namespace\n", username)

		client.Emit("admin:welcome", map[string]any{
			"message": fmt.Sprintf("Welcome to admin panel, %s!", username),
		})

		client.On("admin:action", func(args ...any) {
			if len(args) > 0 {
				fmt.Printf("Admin action by %s: %v\n", username, args[0])
				client.Emit("admin:action:result", map[string]any{
					"success": true,
					"action":  args[0],
				})
			}
		})
	})

	addr := ":3000"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}

	httpServer.Listen(addr, nil)
	fmt.Printf("Auth server listening on %s\n", addr)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Println("Shutting down server...")
	server.Close(nil)
}
