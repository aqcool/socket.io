package adapter

import (
	"fmt"

	"github.com/aqcool/socket.io/v3/pkg/utils"
	"github.com/vmihailenco/msgpack/v5"
)

// EncodeClusterMessage serializes a cluster message including binary payloads.
func EncodeClusterMessage(message *ClusterMessage) ([]byte, error) {
	payload, err := utils.MsgPack().Encode(message)
	if err != nil {
		return nil, fmt.Errorf("encode cluster message: %w", err)
	}
	return payload, nil
}

// DecodeClusterMessage restores the concrete Data type required by
// ClusterAdapter.OnMessage.
func DecodeClusterMessage(payload []byte) (*ClusterMessage, error) {
	var raw struct {
		Uid    ServerId           `msgpack:"uid,omitempty"`
		Nsp    string             `msgpack:"nsp,omitempty"`
		Type   MessageType        `msgpack:"type,omitempty"`
		Data   msgpack.RawMessage `msgpack:"data,omitempty"`
		Offset Offset             `msgpack:"offset,omitempty"`
	}
	if err := utils.MsgPack().Decode(payload, &raw); err != nil {
		return nil, fmt.Errorf("decode cluster message: %w", err)
	}
	message := &ClusterMessage{Uid: raw.Uid, Nsp: raw.Nsp, Type: raw.Type, Offset: raw.Offset}
	if len(raw.Data) == 0 {
		return message, nil
	}
	target := clusterMessageData(raw.Type)
	if target == nil {
		return message, nil
	}
	if err := utils.MsgPack().Decode(raw.Data, target); err != nil {
		return nil, fmt.Errorf("decode cluster message data: %w", err)
	}
	message.Data = target
	return message, nil
}

func clusterMessageData(messageType MessageType) any {
	switch messageType {
	case INITIAL_HEARTBEAT, HEARTBEAT, ADAPTER_CLOSE:
		return nil
	case BROADCAST:
		return &BroadcastMessage{}
	case SOCKETS_JOIN, SOCKETS_LEAVE:
		return &SocketsJoinLeaveMessage{}
	case DISCONNECT_SOCKETS:
		return &DisconnectSocketsMessage{}
	case FETCH_SOCKETS:
		return &FetchSocketsMessage{}
	case FETCH_SOCKETS_RESPONSE:
		return &FetchSocketsResponse{}
	case COUNT_SOCKETS:
		return &CountSocketsMessage{}
	case COUNT_SOCKETS_RESPONSE:
		return &CountSocketsResponse{}
	case LIST_ROOMS:
		return &ListRoomsMessage{}
	case LIST_ROOMS_RESPONSE:
		return &ListRoomsResponse{}
	case SERVER_SIDE_EMIT:
		return &ServerSideEmitMessage{}
	case SERVER_SIDE_EMIT_RESPONSE:
		return &ServerSideEmitResponse{}
	case BROADCAST_CLIENT_COUNT:
		return &BroadcastClientCount{}
	case BROADCAST_ACK:
		return &BroadcastAck{}
	default:
		return nil
	}
}
