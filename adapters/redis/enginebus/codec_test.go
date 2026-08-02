package enginebus

import (
	"bytes"
	"fmt"
	"testing"

	engine "github.com/aqcool/socket.io/servers/engine/v3"
	"github.com/vmihailenco/msgpack/v5"
)

func TestOfficial010WireEnvelope(t *testing.T) {
	message := engine.ClusterMessage{
		Source:    "",
		SenderID:  "go-node",
		RequestID: 7,
		Type:      engine.ClusterMessageAcquireLock,
		SID:       "01234567890123456789",
		Transport: "polling",
		LockType:  engine.ClusterReadLock,
	}
	payload, err := msgpack.Marshal(encodeOfficialMessage(&message))
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := msgpack.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["_source"] != "_eio" {
		t.Fatalf("_source = %#v", raw["_source"])
	}
	if _, exists := raw["source"]; exists {
		t.Fatal("non-official top-level source field was encoded")
	}
	for _, field := range []string{"sid", "transport", "transportName", "lockType"} {
		if _, exists := raw[field]; exists {
			t.Fatalf("field %q must not be flattened at the top level", field)
		}
	}
	data, ok := raw["data"].(map[string]any)
	if !ok {
		t.Fatalf("data = %#v", raw["data"])
	}
	if data["sid"] != message.SID || data["transportName"] != "polling" || data["type"] != "read" {
		t.Fatalf("official nested data = %#v", data)
	}
}

func TestOfficial010AllMessageShapes(t *testing.T) {
	packet := engine.ClusterPacket{Type: "message", HasData: true, Data: []byte("hello")}
	tests := []struct {
		message  engine.ClusterMessage
		topKeys  []string
		dataKeys []string
	}{
		{
			message:  engine.ClusterMessage{Source: "_eio", SenderID: "a", RequestID: 1, Type: engine.ClusterMessageAcquireLock, SID: "01234567890123456789", Transport: "polling", LockType: engine.ClusterReadLock},
			topKeys:  []string{"_source", "requestId", "senderId", "type", "data"},
			dataKeys: []string{"sid", "transportName", "type"},
		},
		{
			message:  engine.ClusterMessage{Source: "_eio", SenderID: "a", RecipientID: "b", RequestID: 1, Type: engine.ClusterMessageAcquireLockResponse, Success: false},
			topKeys:  []string{"_source", "requestId", "senderId", "recipientId", "type", "data"},
			dataKeys: []string{"success"},
		},
		{
			message:  engine.ClusterMessage{Source: "_eio", SenderID: "a", RecipientID: "b", Type: engine.ClusterMessageDrain, SID: "01234567890123456789", Packets: []engine.ClusterPacket{}},
			topKeys:  []string{"_source", "senderId", "recipientId", "type", "data"},
			dataKeys: []string{"sid", "packets"},
		},
		{
			message:  engine.ClusterMessage{Source: "_eio", SenderID: "a", RecipientID: "b", Type: engine.ClusterMessagePacket, SID: "01234567890123456789", Packet: &packet},
			topKeys:  []string{"_source", "senderId", "recipientId", "type", "data"},
			dataKeys: []string{"sid", "packet"},
		},
		{
			message:  engine.ClusterMessage{Source: "_eio", SenderID: "a", RecipientID: "b", RequestID: 1, Type: engine.ClusterMessageUpgrade, SID: "01234567890123456789", Success: false},
			topKeys:  []string{"_source", "requestId", "senderId", "recipientId", "type", "data"},
			dataKeys: []string{"sid", "success"},
		},
		{
			message:  engine.ClusterMessage{Source: "_eio", SenderID: "a", RecipientID: "b", RequestID: 1, Type: engine.ClusterMessageUpgradeResponse, TakeOver: false, Packets: []engine.ClusterPacket{}},
			topKeys:  []string{"_source", "requestId", "senderId", "recipientId", "type", "data"},
			dataKeys: []string{"takeOver", "packets"},
		},
		{
			message:  engine.ClusterMessage{Source: "_eio", SenderID: "a", RecipientID: "b", Type: engine.ClusterMessageClose, SID: "01234567890123456789", Reason: ""},
			topKeys:  []string{"_source", "senderId", "recipientId", "type", "data"},
			dataKeys: []string{"sid", "reason"},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("type_%d", test.message.Type), func(t *testing.T) {
			payload, err := msgpack.Marshal(encodeOfficialMessage(&test.message))
			if err != nil {
				t.Fatal(err)
			}
			var raw map[string]any
			if err := msgpack.Unmarshal(payload, &raw); err != nil {
				t.Fatal(err)
			}
			assertExactWireKeys(t, raw, test.topKeys)
			data, ok := raw["data"].(map[string]any)
			if !ok {
				t.Fatalf("data = %#v", raw["data"])
			}
			assertExactWireKeys(t, data, test.dataKeys)
		})
	}
}

func assertExactWireKeys(t *testing.T, actual map[string]any, expected []string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("wire keys = %#v, want %v", actual, expected)
	}
	for _, key := range expected {
		if _, exists := actual[key]; !exists {
			t.Fatalf("wire key %q missing from %#v", key, actual)
		}
	}
}

func TestOfficial010PacketWireTypes(t *testing.T) {
	textPacket := engine.ClusterPacket{Type: "message", HasData: true, Data: []byte("hello"), HasCompress: true}
	textWire := encodeOfficialPacket(textPacket)
	if textWire.Data == nil {
		t.Fatal("text packet data is absent")
	}
	if text, ok := (*textWire.Data).(string); !ok || text != "hello" {
		t.Fatalf("text packet wire data = %#v", textWire.Data)
	}
	if decoded := decodeOfficialPacket(textWire); decoded.Binary || string(decoded.Data) != "hello" {
		t.Fatalf("decoded text packet = %#v", decoded)
	}
	if textWire.Options == nil || textWire.Options.Compress || !decodeOfficialPacket(textWire).HasCompress {
		t.Fatalf("explicit false packet options were not preserved: %#v", textWire.Options)
	}

	binaryPacket := engine.ClusterPacket{Type: "message", HasData: true, Binary: true, Data: []byte{1, 2, 3, 4}}
	binaryWire := encodeOfficialPacket(binaryPacket)
	if binaryWire.Data == nil {
		t.Fatal("binary packet data is absent")
	}
	data, ok := (*binaryWire.Data).([]byte)
	if !ok || !bytes.Equal(data, []byte{1, 2, 3, 4}) {
		t.Fatalf("binary packet wire data = %#v", binaryWire.Data)
	}
	decoded := decodeOfficialPacket(binaryWire)
	if !decoded.Binary || !bytes.Equal(decoded.Data, []byte{1, 2, 3, 4}) {
		t.Fatalf("decoded binary packet = %#v", decoded)
	}

	emptyBinary := encodeOfficialPacket(engine.ClusterPacket{Type: "message", HasData: true, Binary: true})
	payload, err := msgpack.Marshal(emptyBinary)
	if err != nil {
		t.Fatal(err)
	}
	var decodedWire officialWirePacket
	if decodeErr := msgpack.Unmarshal(payload, &decodedWire); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if decodedWire.Data == nil {
		t.Fatal("empty binary data field was omitted")
	}
	if data, ok := (*decodedWire.Data).([]byte); !ok || len(data) != 0 {
		t.Fatalf("empty binary must remain bin(0), got %#v", decodedWire.Data)
	}

	emptyText := encodeOfficialPacket(engine.ClusterPacket{Type: "message", HasData: true})
	payload, err = msgpack.Marshal(emptyText)
	if err != nil {
		t.Fatal(err)
	}
	decodedWire = officialWirePacket{}
	if err := msgpack.Unmarshal(payload, &decodedWire); err != nil {
		t.Fatal(err)
	}
	if decodedWire.Data == nil {
		t.Fatal("empty text data field was omitted")
	}
	if data, ok := (*decodedWire.Data).(string); !ok || data != "" {
		t.Fatalf("empty text must remain string(0), got %#v", decodedWire.Data)
	}
	decodedText := decodeOfficialPacket(decodedWire)
	if !decodedText.HasData || decodedText.Binary || len(decodedText.Data) != 0 {
		t.Fatalf("decoded empty text = %#v", decodedText)
	}
}
