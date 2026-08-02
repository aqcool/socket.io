package emitter

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/aqcool/socket.io/adapters/adapter/v3"
	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
	"github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/utils"
)

func TestRedisStreamsEmitterOfficialDefaultsAndFluentState(t *testing.T) {
	opts := DefaultRedisStreamsEmitterOptions()
	if opts.StreamName != "socket.io" || opts.MaxLen != 10_000 {
		t.Fatalf("unexpected official defaults: %#v", opts)
	}

	emitter := NewRedisStreamsEmitter(nil, nil)
	if emitter.nsp != "/" || emitter.opts != *opts {
		t.Fatalf("unexpected root emitter: %#v", emitter)
	}
	if custom := emitter.Of("custom"); custom.nsp != "/custom" {
		t.Fatalf("namespace was not normalized: %q", custom.nsp)
	}

	base := emitter.newBroadcastOperator()
	operator := base.To("room1").Except("room2").Volatile().Compress(false)
	if base.rooms.Len() != 0 || base.exceptRooms.Len() != 0 || base.flags.Volatile {
		t.Fatal("fluent operations mutated the source operator")
	}
	if !operator.rooms.Has("room1") || !operator.exceptRooms.Has("room2") ||
		!operator.flags.Volatile || operator.flags.Compress == nil || *operator.flags.Compress {
		t.Fatalf("unexpected fluent operator state: %#v", operator)
	}
}

func TestRedisStreamsEmitterValidationWithoutRedis(t *testing.T) {
	emitter := NewRedisStreamsEmitter(nil, nil)
	if err := emitter.Emit("connect"); err == nil {
		t.Fatal("reserved event must be rejected before publishing")
	}
	if err := emitter.Emit("event"); err == nil {
		t.Fatal("missing Redis client must be reported")
	}
	if err := emitter.ServerSideEmit("event", func([]any, error) {}); err == nil {
		t.Fatal("emitter acknowledgement callback must be rejected")
	}
}

func TestEncodeRedisStreamsEmitterMessageOfficialWireFormat(t *testing.T) {
	textMessage := &adapter.ClusterMessage{
		Uid:  "emitter",
		Nsp:  "/custom",
		Type: adapter.BROADCAST,
		Data: &adapter.BroadcastMessage{
			Packet: &parser.Packet{Type: parser.EVENT, Nsp: "/custom", Data: []any{"event", "value"}},
			Opts:   &adapter.PacketOptions{Rooms: []socket.Room{"room1"}, Except: []socket.Room{}, Flags: &socket.BroadcastFlags{}},
		},
	}
	values, err := encodeRedisStreamsEmitterMessage(textMessage)
	if err != nil {
		t.Fatal(err)
	}
	if values["uid"] != "emitter" || values["nsp"] != "/custom" || values["type"] != "3" {
		t.Fatalf("unexpected flattened fields: %#v", values)
	}
	data, ok := values["data"].(string)
	if !ok || len(data) == 0 || data[0] != '{' {
		t.Fatalf("plaintext payload must be JSON: %#v", values["data"])
	}
	var decoded adapter.BroadcastMessage
	if err = json.Unmarshal([]byte(data), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Packet == nil || decoded.Packet.Nsp != "/custom" || len(decoded.Opts.Rooms) != 1 {
		t.Fatalf("unexpected decoded JSON message: %#v", decoded)
	}

	binaryMessage := &adapter.ClusterMessage{
		Uid:  "emitter",
		Nsp:  "/",
		Type: adapter.BROADCAST,
		Data: &adapter.BroadcastMessage{
			Packet: &parser.Packet{Type: parser.EVENT, Nsp: "/", Data: []any{"event", []byte{1, 2, 3}}},
			Opts:   &adapter.PacketOptions{Flags: &socket.BroadcastFlags{}},
		},
	}
	values, err = encodeRedisStreamsEmitterMessage(binaryMessage)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := base64.StdEncoding.DecodeString(values["data"].(string))
	if err != nil {
		t.Fatal(err)
	}
	var binaryDecoded adapter.BroadcastMessage
	if err = utils.MsgPack().Decode(encoded, &binaryDecoded); err != nil {
		t.Fatal(err)
	}
	if binaryDecoded.Packet == nil || !parser.HasBinary(binaryDecoded.Packet.Data) {
		t.Fatalf("binary payload was not preserved: %#v", binaryDecoded)
	}
}
