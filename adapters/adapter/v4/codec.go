package adapter

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

func EncodeClusterMessage(message *ClusterMessage) ([]byte, error) {
	payload, err := msgpack.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode cluster message: %w", err)
	}
	return payload, nil
}

func DecodeClusterMessage(payload []byte) (*ClusterMessage, error) {
	var raw struct {
		UID    ServerID           `msgpack:"uid,omitempty"`
		NSP    string             `msgpack:"nsp,omitempty"`
		Type   MessageType        `msgpack:"type,omitempty"`
		Data   msgpack.RawMessage `msgpack:"data,omitempty"`
		Offset Offset             `msgpack:"offset,omitempty"`
	}
	if err := msgpack.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("decode cluster message: %w", err)
	}
	if !raw.Type.Valid() {
		return nil, fmt.Errorf("decode cluster message: invalid message type %d", raw.Type)
	}
	message := &ClusterMessage{UID: raw.UID, NSP: raw.NSP, Type: raw.Type, Offset: raw.Offset}
	if len(raw.Data) == 0 {
		return message, nil
	}
	target := messageData(raw.Type)
	if target == nil {
		return message, nil
	}
	if err := msgpack.Unmarshal(raw.Data, target); err != nil {
		return nil, fmt.Errorf("decode cluster message data: %w", err)
	}
	message.Data = target
	return message, nil
}

func messageData(messageType MessageType) any {
	switch messageType {
	case InitialHeartbeat, Heartbeat, AdapterClose:
		return nil
	case Broadcast:
		return &BroadcastMessage{}
	case SocketsJoin, SocketsLeave:
		return &SocketsJoinLeaveMessage{}
	case DisconnectSockets:
		return &DisconnectSocketsMessage{}
	case FetchSockets:
		return &FetchSocketsMessage{}
	case FetchSocketsResponse:
		return &FetchSocketsResponseData{}
	case CountSockets:
		return &CountSocketsMessage{}
	case CountSocketsResponse:
		return &CountSocketsResponse{}
	case ListRooms:
		return &ListRoomsMessage{}
	case ListRoomsResponse:
		return &ListRoomsResponse{}
	case ServerSideEmit:
		return &ServerSideEmitMessage{}
	case ServerSideEmitResponse:
		return &ServerSideEmitResponse{}
	case BroadcastClientCount:
		return &BroadcastClientCount{}
	case BroadcastAck:
		return &BroadcastAckResponse{}
	default:
		return nil
	}
}
