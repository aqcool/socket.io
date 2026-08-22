package emitter

import (
	"testing"

	"github.com/aqcool/socket.io/servers/socket/v4"
)

func TestEmitterOptionsAndNamespace(t *testing.T) {
	options := DefaultEmitterOptions()
	if options.AddCreatedAtField() {
		t.Fatal("createdAt must be disabled by default")
	}
	options.SetAddCreatedAtField(true)
	clone := DefaultEmitterOptions()
	clone.Assign(options)
	if !clone.AddCreatedAtField() {
		t.Fatal("emitter options were not assigned")
	}

	emitter := NewEmitter(nil, clone).Of("orders")
	if emitter.nsp != "/orders" || emitter.broadcastOptions.Nsp != "/orders" ||
		!emitter.broadcastOptions.AddCreatedAtField {
		t.Fatalf("unexpected emitter configuration: %#v", emitter.broadcastOptions)
	}
}

func TestBroadcastOperatorFluentStateAndValidation(t *testing.T) {
	operator := MakeBroadcastOperator()
	operator.Construct(nil, &BroadcastOptions{Nsp: "/"}, nil, nil, nil)
	targeted := operator.
		To(socket.Room("room-a")).
		Except(socket.Room("room-b")).
		Volatile().
		Compress(true).(*BroadcastOperator)
	if !targeted.rooms.Has("room-a") || !targeted.exceptRooms.Has("room-b") ||
		!targeted.flags.Volatile || targeted.flags.Compress == nil || !*targeted.flags.Compress {
		t.Fatal("fluent broadcast state was not preserved")
	}
	if err := targeted.Emit("connect"); err == nil {
		t.Fatal("reserved event was accepted")
	}
	if err := targeted.ServerSideEmit("event", func([]any, error) {}); err == nil {
		t.Fatal("server-side acknowledgement was accepted")
	}
}
