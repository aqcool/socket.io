package socket

import (
	"strings"
	"testing"
	"time"

	enginepacket "github.com/aqcool/socket.io/parsers/engine/v3/packet"
)

func TestOfficialMessagingManyRoomLifecycle(t *testing.T) {
	_, socket, _ := newOfficialWebSocketSocket(t)
	adapter := socket.Nsp().Adapter()

	socket.Join("a")
	socket.Join("b")
	socket.Join("c")
	for _, room := range []Room{Room(socket.Id()), "a", "b", "c"} {
		if !socket.Rooms().Has(room) {
			t.Fatalf("rooms after Join = %v, missing %s", socket.Rooms().Keys(), room)
		}
	}

	socket.Leave("b")
	socket.Leave("unknown")
	if socket.Rooms().Has("b") || !socket.Rooms().Has("a") || !socket.Rooms().Has("c") {
		t.Fatalf("rooms after Leave = %v, want socket ID, a and c", socket.Rooms().Keys())
	}
	if _, exists := adapter.Rooms().Load("b"); exists {
		t.Fatal("empty room b was not deleted from the Adapter")
	}

	socket.leaveAll()
	if socket.Rooms().Len() != 0 {
		t.Fatalf("rooms after leaveAll = %v, want empty", socket.Rooms().Keys())
	}
	for _, room := range []Room{Room(socket.Id()), "a", "c"} {
		if _, exists := adapter.Rooms().Load(room); exists {
			t.Fatalf("empty room %s remains in the Adapter", room)
		}
	}
}

func TestOfficialMessagingManySocketBroadcastOperatorIsImmutable(t *testing.T) {
	_, socket, _ := newOfficialWebSocketSocket(t)
	operator := socket.Local().Compress(false).To("room1", "room2").Except("room3")

	_ = operator.Compress(true).Emit("hello")
	_ = operator.Volatile().Emit("hello")
	_ = operator.To("room4").Emit("hello")
	_ = operator.Except("room5").Emit("hello")
	_ = socket.To("room6").Emit("hello")

	if !operator.rooms.Has("room1") || !operator.rooms.Has("room2") || operator.rooms.Has("room4") || operator.rooms.Has("room6") {
		t.Fatalf("original operator rooms mutated: %v", operator.rooms.Keys())
	}
	if !operator.exceptRooms.Has("room3") || !operator.exceptRooms.Has(Room(socket.Id())) || operator.exceptRooms.Has("room5") {
		t.Fatalf("original operator exclusions mutated: %v", operator.exceptRooms.Keys())
	}
	if !operator.flags.Local || operator.flags.Compress == nil || *operator.flags.Compress {
		t.Fatalf("original operator flags mutated: %#v", operator.flags)
	}
}

func TestOfficialMessagingManyPrecomputesWebSocketFrame(t *testing.T) {
	server, socket, connection := newOfficialWebSocketSocket(t)
	created := make(chan *enginepacket.Packet, 1)
	_ = socket.Conn().Once("packetCreate", func(args ...any) {
		created <- args[0].(*enginepacket.Packet)
	})

	if err := server.Compress(false).Emit("woot", "hi"); err != nil {
		t.Fatalf("broadcasting WebSocket event: %v", err)
	}
	select {
	case packet := <-created:
		if packet.Options == nil || packet.Options.WsPreEncodedFrame == nil {
			t.Fatalf("packet options = %#v, want precomputed WebSocket frame", packet.Options)
		}
		if got := string(packet.Options.WsPreEncodedFrame.Bytes()); got != `42["woot","hi"]` {
			t.Fatalf("precomputed WebSocket frame = %q, want Engine.IO message + Socket.IO event", got)
		}
	case <-time.After(time.Second):
		t.Fatal("packetCreate event was not emitted")
	}

	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	_, payload, err := connection.ReadMessage()
	if err != nil || !strings.Contains(string(payload), `["woot","hi"]`) {
		t.Fatalf("broadcast WebSocket payload = %q/%v", payload, err)
	}
}
