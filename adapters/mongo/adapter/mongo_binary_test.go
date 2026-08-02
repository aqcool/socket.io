package adapter

import (
	"bytes"
	"testing"

	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNormalizeMongoBinaryAcknowledgement(t *testing.T) {
	message := &BroadcastAck{Packet: types.NewBytesBuffer([]byte{7, 8, 9})}

	normalizeMongoMessage(message)

	packet, ok := message.Packet.([]byte)
	if !ok {
		t.Fatalf("binary ACK was normalized as %T instead of []byte", message.Packet)
	}
	if !bytes.Equal(packet, []byte{7, 8, 9}) {
		t.Fatalf("binary ACK payload = %v", packet)
	}
}

func TestNormalizeMongoBroadcastPacketDataRecursively(t *testing.T) {
	type namedValues []any
	type namedMap map[string]any

	first := types.NewBytesBuffer([]byte{1, 2, 3})
	second := types.NewBytesBuffer([]byte{4, 5, 6})
	third := types.NewBytesBuffer([]byte{7, 8, 9})
	fourth := types.NewBytesBuffer([]byte{13, 14, 15})
	message := &BroadcastMessage{Packet: &parser.Packet{
		Type: parser.BINARY_EVENT,
		Data: namedValues{
			"event",
			namedMap{
				"plain": []any{map[string]any{"binary": first}},
				"typed": []types.BufferInterface{second},
				"typedMap": map[string]types.BufferInterface{
					"binary": fourth,
				},
				"bson": bson.D{{Key: "nested", Value: bson.A{
					bson.M{"binary": third},
					bson.Binary{Subtype: 0x00, Data: []byte{10, 11, 12}},
				}}},
			},
		},
	}}

	normalizeMongoMessage(message)

	packetData, ok := message.Packet.Data.([]any)
	if !ok || len(packetData) != 2 {
		t.Fatalf("Packet.Data = %#v (%T), want []any with 2 elements", message.Packet.Data, message.Packet.Data)
	}
	root, ok := packetData[1].(map[string]any)
	if !ok {
		t.Fatalf("nested Packet.Data = %#v (%T), want map[string]any", packetData[1], packetData[1])
	}
	plain := root["plain"].([]any)[0].(map[string]any)["binary"]
	typed := root["typed"].([]any)[0]
	bsonNested := root["bson"].(map[string]any)["nested"].([]any)

	for name, actual := range map[string]struct {
		value any
		want  []byte
	}{
		"plain map/slice":     {value: plain, want: []byte{1, 2, 3}},
		"named typed slice":   {value: typed, want: []byte{4, 5, 6}},
		"typed map":           {value: root["typedMap"].(map[string]any)["binary"], want: []byte{13, 14, 15}},
		"bson document/array": {value: bsonNested[0].(map[string]any)["binary"], want: []byte{7, 8, 9}},
		"bson binary":         {value: bsonNested[1], want: []byte{10, 11, 12}},
	} {
		binary, ok := actual.value.([]byte)
		if !ok {
			t.Fatalf("%s value = %T, want []byte", name, actual.value)
		}
		if !bytes.Equal(binary, actual.want) {
			t.Fatalf("%s value = %v, want %v", name, binary, actual.want)
		}
	}

	// Conversion must not alias a mutable BufferInterface backing slice.
	first.Bytes()[0] = 99
	if got := plain.([]byte)[0]; got != 1 {
		t.Fatalf("normalized binary aliases source buffer: first byte = %d", got)
	}
}

func TestNormalizeMongoValuePreservesByteAndBSONScalarTypes(t *testing.T) {
	raw := bson.Raw{1, 2, 3}
	plain := []byte{4, 5, 6}
	objectID := bson.NewObjectID()

	if got, ok := normalizeMongoValue(raw).(bson.Raw); !ok || !bytes.Equal(got, raw) {
		t.Fatalf("bson.Raw changed to %#v (%T)", got, got)
	}
	if got, ok := normalizeMongoValue(plain).([]byte); !ok || !bytes.Equal(got, plain) {
		t.Fatalf("[]byte changed to %#v (%T)", got, got)
	}
	if got := normalizeMongoValue(objectID); got != objectID {
		t.Fatalf("ObjectID changed to %#v (%T)", got, got)
	}
}
