package adapter

import (
	"encoding/json"
	"testing"

	socket "github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/vmihailenco/msgpack/v5"
)

func TestClusterMessageCodecPreservesConcreteData(t *testing.T) {
	message := &ClusterMessage{
		Uid:  "node-1",
		Nsp:  "/chat",
		Type: SOCKETS_JOIN,
		Data: &SocketsJoinLeaveMessage{Rooms: []socket.Room{"room-1"}},
	}
	payload, err := EncodeClusterMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeClusterMessage(payload)
	if err != nil {
		t.Fatal(err)
	}
	data, ok := decoded.Data.(*SocketsJoinLeaveMessage)
	if !ok || len(data.Rooms) != 1 || data.Rooms[0] != "room-1" {
		t.Fatalf("unexpected decoded data %#v", decoded.Data)
	}
}

func TestBroadcastClientCountCodecPreservesZero(t *testing.T) {
	data := &BroadcastClientCount{RequestId: "empty-room", ClientCount: 0}
	jsonPayload, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if string(jsonPayload) != `{"requestId":"empty-room","clientCount":0}` {
		t.Fatalf("zero client count missing from JSON: %s", jsonPayload)
	}

	payload, err := EncodeClusterMessage(&ClusterMessage{
		Uid:  "node-1",
		Nsp:  "/",
		Type: BROADCAST_CLIENT_COUNT,
		Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := msgpack.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	rawData, ok := raw["data"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected raw MessagePack data: %#v", raw["data"])
	}
	if value, exists := rawData["clientCount"]; !exists || value != int8(0) && value != uint8(0) && value != int64(0) && value != uint64(0) {
		t.Fatalf("zero clientCount missing from MessagePack: %#v", rawData)
	}
}

func TestFetchSocketsResponseCodecPreservesEmptyList(t *testing.T) {
	data := &FetchSocketsResponse{
		RequestId: "no-match",
		Sockets:   []*SocketResponse{},
	}
	jsonPayload, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if string(jsonPayload) != `{"requestId":"no-match","sockets":[]}` {
		t.Fatalf("empty socket list missing from JSON: %s", jsonPayload)
	}

	payload, err := EncodeClusterMessage(&ClusterMessage{
		Uid:  "node-1",
		Nsp:  "/",
		Type: FETCH_SOCKETS_RESPONSE,
		Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := msgpack.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	rawData, ok := raw["data"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected raw MessagePack data: %#v", raw["data"])
	}
	if sockets, exists := rawData["sockets"]; !exists || sockets == nil {
		t.Fatalf("empty sockets missing from MessagePack: %#v", rawData)
	}
}
