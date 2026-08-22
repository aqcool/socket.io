package socket

import (
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/gorilla/websocket"
)

func TestOfficialSocketIncomingAnyListenerLifecycle(t *testing.T) {
	_, socket, connection := newOfficialWebSocketSocket(t)
	called := make(chan []string, 1)
	order := make([]string, 0, 3)
	fail := func(...any) { t.Error("removed onAny listener was called") }
	socket.OnAny(fail)
	socket.OffAny(fail)
	if len(socket.ListenersAny()) != 0 {
		t.Fatalf("listenersAny after removal = %d, want 0", len(socket.ListenersAny()))
	}
	socket.OnAny(func(args ...any) {
		if args[0] != "my-event" || args[1] != "123" {
			t.Errorf("onAny args = %#v", args)
		}
		order = append(order, "normal")
		called <- append([]string(nil), order...)
	})
	socket.PrependAny(func(...any) { order = append(order, "prepend-1") })
	socket.PrependAny(func(...any) { order = append(order, "prepend-2") })

	if err := connection.WriteMessage(websocket.TextMessage, []byte(`42["my-event","123"]`)); err != nil {
		t.Fatalf("writing incoming event: %v", err)
	}
	select {
	case got := <-called:
		want := []string{"prepend-2", "prepend-1", "normal"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("onAny listener order = %v, want %v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("onAny listeners were not called")
	}
	socket.OffAny(nil)
	if len(socket.ListenersAny()) != 0 {
		t.Fatalf("listenersAny after clearing = %d, want 0", len(socket.ListenersAny()))
	}
}

func TestOfficialSocketOutgoingAnyListenerLifecycle(t *testing.T) {
	server, socket, connection := newOfficialWebSocketSocket(t)
	order := make([]string, 0, 3)
	fail := func(...any) { t.Error("removed onAnyOutgoing listener was called") }
	socket.OnAnyOutgoing(fail)
	socket.OffAnyOutgoing(fail)
	if len(socket.ListenersAnyOutgoing()) != 0 {
		t.Fatalf("listenersAnyOutgoing after removal = %d, want 0", len(socket.ListenersAnyOutgoing()))
	}
	socket.OnAnyOutgoing(func(args ...any) {
		if args[0] != "my-event" || args[1] != "123" {
			t.Errorf("onAnyOutgoing args = %#v", args)
		}
		order = append(order, "normal")
	})
	socket.PrependAnyOutgoing(func(...any) { order = append(order, "prepend-1") })
	socket.PrependAnyOutgoing(func(...any) { order = append(order, "prepend-2") })
	if err := socket.Emit("my-event", "123"); err != nil {
		t.Fatalf("direct outgoing event: %v", err)
	}
	want := []string{"prepend-2", "prepend-1", "normal"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("onAnyOutgoing listener order = %v, want %v", order, want)
	}
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	if _, payload, err := connection.ReadMessage(); err != nil || string(payload) != `42["my-event","123"]` {
		t.Fatalf("direct outgoing packet = %q/%v", payload, err)
	}

	socket.OffAnyOutgoing(nil)
	broadcast := make(chan []any, 1)
	socket.OnAnyOutgoing(func(args ...any) { broadcast <- args })
	server.Emit("binary-broadcast", types.NewBytesBuffer([]byte{1, 2, 3}))
	select {
	case args := <-broadcast:
		if args[0] != "binary-broadcast" {
			t.Fatalf("binary broadcast outgoing args = %#v", args)
		}
		if _, ok := args[1].(types.BufferInterface); !ok {
			t.Fatalf("binary broadcast value = %T, want BufferInterface", args[1])
		}
	case <-time.After(time.Second):
		t.Fatal("binary broadcast did not notify onAnyOutgoing")
	}
}

func TestOfficialSocketTransportNullMessageClosesCleanly(t *testing.T) {
	_, socket, _ := newOfficialWebSocketSocket(t)
	errorsSeen := make(chan error, 1)
	disconnected := make(chan string, 1)
	_ = socket.Once("error", func(args ...any) {
		err, _ := args[0].(error)
		errorsSeen <- err
	})
	_ = socket.Once("disconnect", func(args ...any) { disconnected <- args[0].(string) })

	socket.Client().ondata(nil)
	select {
	case err := <-errorsSeen:
		if err == nil {
			t.Fatal("transport null message emitted a nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("transport null message did not emit an error")
	}
	select {
	case reason := <-disconnected:
		if reason != "forced close" {
			t.Fatalf("disconnect reason = %q, want forced close", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("transport null message did not disconnect the Socket")
	}
}

func TestOfficialSocketIgnoresBinaryPacketAfterNamespaceDisconnect(t *testing.T) {
	_, socket, connection := newOfficialWebSocketSocket(t)
	called := make(chan struct{}, 1)
	_ = socket.On("test", func(...any) { called <- struct{}{} })
	socket.Disconnect(false)
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	if _, payload, err := connection.ReadMessage(); err != nil || string(payload) != "41" {
		t.Fatalf("namespace DISCONNECT packet = %q/%v", payload, err)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte(`451-["test",{"_placeholder":true,"num":0}]`)); err != nil {
		t.Fatalf("writing disconnected binary event header: %v", err)
	}
	if err := connection.WriteMessage(websocket.BinaryMessage, []byte{1, 2, 3}); err != nil {
		t.Fatalf("writing disconnected binary attachment: %v", err)
	}
	select {
	case <-called:
		t.Fatal("event handler ran after namespace disconnection")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestOfficialSocketRoomsAreCleanedAfterMiddlewareFailure(t *testing.T) {
	server, httpServer := newOfficialCloseTestServer(t)
	rejected := make(chan *Socket, 1)
	server.Use(func(socket *Socket, next func(*ExtendedError)) {
		socket.Join("room1")
		rejected <- socket
		next(NewExtendedError("nope", nil))
	})

	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, "40")
	if payload := socketIOPollingPoll(t, httpServer.URL, sid); !strings.HasPrefix(payload, "44{") || !strings.Contains(payload, `"message":"nope"`) {
		t.Fatalf("middleware CONNECT_ERROR packet = %q", payload)
	}
	select {
	case socket := <-rejected:
		if rooms := socket.Rooms().Keys(); len(rooms) != 0 {
			t.Fatalf("rooms after middleware failure = %v, want empty", rooms)
		}
	case <-time.After(time.Second):
		t.Fatal("namespace middleware was not called")
	}
}

func TestOfficialSocketCannotJoinRoomsAfterDisconnection(t *testing.T) {
	_, socket, _ := newOfficialPollingSocket(t)
	socket.Disconnect(false)
	if err := socket.TryJoin("room1"); err == nil || err.Error() != "socket.io: socket is closing" {
		t.Fatalf("TryJoin after disconnect error = %v", err)
	}
	if rooms := socket.Rooms().Keys(); len(rooms) != 0 {
		t.Fatalf("rooms after disconnected Join = %v, want empty", rooms)
	}
}

func TestOfficialSocketRejectsReservedEvent(t *testing.T) {
	_, socket, _ := newOfficialWebSocketSocket(t)
	err := socket.Emit("connect_error")
	if err == nil || err.Error() != `"connect_error" is a reserved event name` {
		t.Fatalf("reserved Socket event error = %v", err)
	}
}
