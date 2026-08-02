package socket

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestOfficialNamespaceMiddlewareOrderAndConnectionState(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	var order atomic.Int32
	firstCompleted := make(chan struct{})
	server.Use(func(socket *Socket, next func(*ExtendedError)) {
		if got := order.Add(1); got != 1 {
			t.Errorf("first middleware order = %d, want 1", got)
		}
		if socket.Connected() || !socket.Disconnected() || server.Sockets().Sockets().Len() != 0 {
			t.Errorf("pre-connect state = connected %t, disconnected %t, sockets %d", socket.Connected(), socket.Disconnected(), server.Sockets().Sockets().Len())
		}
		socket.SetData(map[string]any{"name": "guillermo"})
		time.AfterFunc(20*time.Millisecond, func() {
			close(firstCompleted)
			next(nil)
		})
	})
	server.Use(func(_ *Socket, next func(*ExtendedError)) {
		select {
		case <-firstCompleted:
		default:
			t.Error("second middleware ran before the asynchronous first middleware completed")
		}
		if got := order.Add(1); got != 2 {
			t.Errorf("second middleware order = %d, want 2", got)
		}
		next(nil)
	})

	connected := make(chan *Socket, 1)
	_ = server.On("connection", func(args ...any) { connected <- args[0].(*Socket) })
	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40")
	if payload := socketIOPollingPoll(t, httpServer.URL, sid); len(payload) < 2 || payload[:2] != "40" {
		t.Fatalf("CONNECT reply = %q", payload)
	}

	select {
	case socket := <-connected:
		if !socket.Connected() || socket.Disconnected() || server.Sockets().Sockets().Len() != 1 {
			t.Fatalf("connected state = connected %t, disconnected %t, sockets %d", socket.Connected(), socket.Disconnected(), server.Sockets().Sockets().Len())
		}
		data, ok := socket.Data().(map[string]any)
		if !ok || data["name"] != "guillermo" {
			t.Fatalf("middleware Socket data = %#v", socket.Data())
		}
	case <-time.After(time.Second):
		t.Fatal("connection event was not emitted after middleware completion")
	}
}

func TestOfficialNamespaceMiddlewareStructuredErrorShortCircuits(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	var secondCalled atomic.Bool
	server.Use(func(_ *Socket, next func(*ExtendedError)) {
		next(NewExtendedError("Authentication error", map[string]any{"a": "b", "c": 3}))
	})
	server.Use(func(_ *Socket, next func(*ExtendedError)) {
		secondCalled.Store(true)
		next(nil)
	})
	var connected atomic.Bool
	_ = server.On("connection", func(...any) { connected.Store(true) })

	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40")
	payload := socketIOPollingPoll(t, httpServer.URL, sid)
	if len(payload) < 2 || payload[:2] != "44" {
		t.Fatalf("CONNECT_ERROR reply = %q", payload)
	}
	var body struct {
		Message string         `json:"message"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(payload[2:]), &body); err != nil {
		t.Fatalf("decoding CONNECT_ERROR: %v", err)
	}
	if body.Message != "Authentication error" || body.Data["a"] != "b" || body.Data["c"] != float64(3) {
		t.Fatalf("CONNECT_ERROR body = %#v", body)
	}
	if secondCalled.Load() || connected.Load() || server.Sockets().Sockets().Len() != 0 {
		t.Fatalf("rejected middleware state = second %t, connected %t, sockets %d", secondCalled.Load(), connected.Load(), server.Sockets().Sockets().Len())
	}
}

func TestOfficialCustomNamespaceMiddlewareIsolation(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	chat := server.Of("/chat", nil)
	var rootCalls atomic.Int32
	var chatCalls atomic.Int32
	server.Use(func(_ *Socket, next func(*ExtendedError)) {
		rootCalls.Add(1)
		next(nil)
	})
	for range 3 {
		chat.Use(func(_ *Socket, next func(*ExtendedError)) {
			chatCalls.Add(1)
			next(nil)
		})
	}

	rootSID := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, rootSID, "40")
	_ = socketIOPollingPoll(t, httpServer.URL, rootSID)
	chatSID := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, chatSID, "40/chat,")
	_ = socketIOPollingPoll(t, httpServer.URL, chatSID)

	if rootCalls.Load() != 1 || chatCalls.Load() != 3 {
		t.Fatalf("root/chat middleware calls = %d/%d, want 1/3", rootCalls.Load(), chatCalls.Load())
	}
}
