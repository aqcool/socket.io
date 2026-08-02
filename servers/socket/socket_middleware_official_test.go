package socket

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestOfficialSocketMiddlewareOrderMutationAndError(t *testing.T) {
	_, socket, connection := newOfficialWebSocketSocket(t)
	wrapped := make(chan []any, 1)
	middlewareErr := make(chan error, 1)
	secondBlockedCalled := make(chan struct{}, 1)

	socket.Use(func(event []any, next func(error)) {
		if len(event) != 2 {
			next(fmt.Errorf("unexpected event: %#v", event))
			return
		}
		switch fmt.Sprint(event[0]) {
		case "join":
			if event[1] != "woot" {
				next(fmt.Errorf("unexpected join data: %#v", event[1]))
				return
			}
			event[0] = "wrap"
			event[1] = "join:woot"
			next(nil)
		case "blocked":
			next(errors.New("Authentication error"))
		default:
			next(nil)
		}
	})
	socket.Use(func(event []any, next func(error)) {
		if fmt.Sprint(event[0]) == "blocked" {
			secondBlockedCalled <- struct{}{}
		}
		if fmt.Sprint(event[0]) == "wrap" && event[1] != "join:woot" {
			next(fmt.Errorf("mutation was not visible to second middleware: %#v", event))
			return
		}
		next(nil)
	})
	_ = socket.On("wrap", func(args ...any) { wrapped <- args })
	_ = socket.On("blocked", func(...any) { t.Error("blocked event reached its handler") })
	_ = socket.On("error", func(args ...any) {
		middlewareErr <- args[0].(error)
	})

	if err := connection.WriteMessage(websocket.TextMessage, []byte(`42["join","woot"]`)); err != nil {
		t.Fatalf("writing middleware mutation event: %v", err)
	}
	select {
	case args := <-wrapped:
		if len(args) != 1 || args[0] != "join:woot" {
			t.Fatalf("wrapped handler args = %#v", args)
		}
	case <-time.After(time.Second):
		t.Fatal("mutated event did not reach wrapped handler")
	}

	if err := connection.WriteMessage(websocket.TextMessage, []byte(`42["blocked","woot"]`)); err != nil {
		t.Fatalf("writing middleware error event: %v", err)
	}
	select {
	case err := <-middlewareErr:
		if err.Error() != "Authentication error" {
			t.Fatalf("middleware error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("middleware error event was not emitted")
	}
	select {
	case <-secondBlockedCalled:
		t.Fatal("middleware chain did not short-circuit after error")
	default:
	}
}
