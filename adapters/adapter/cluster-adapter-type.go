package adapter

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
	"github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/utils"
)

type (
	// ServerId is the unique ID of a server.
	//
	ServerId string

	// Offset is the unique ID of a message (for the connection state recovery feature).
	Offset string

	// MessageType represents the type of cluster message.
	MessageType int

	// ClusterMessage contains common fields for all cluster messages.
	ClusterMessage struct {
		Uid    ServerId    `json:"uid,omitempty" msgpack:"uid,omitempty" bson:"uid,omitempty"`
		Nsp    string      `json:"nsp,omitempty" msgpack:"nsp,omitempty" bson:"nsp,omitempty"`
		Type   MessageType `json:"type,omitempty" msgpack:"type,omitempty" bson:"type,omitempty"`
		Data   any         `json:"data,omitempty" msgpack:"data,omitempty" bson:"data,omitempty"` // Data will hold the specific message data for different types
		Offset Offset      `json:"offset,omitempty" msgpack:"offset,omitempty" bson:"offset,omitempty"`
	}

	// PacketOptions represents the options for broadcasting messages.
	PacketOptions struct {
		Rooms  []socket.Room          `json:"rooms,omitempty" msgpack:"rooms,omitempty" bson:"rooms,omitempty"`
		Except []socket.Room          `json:"except,omitempty" msgpack:"except,omitempty" bson:"except,omitempty"`
		Flags  *socket.BroadcastFlags `json:"flags,omitempty" msgpack:"flags,omitempty" bson:"flags,omitempty"`
	}

	// BroadcastMessage is a message for broadcasting.
	BroadcastMessage struct {
		Opts      *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
		Packet    *parser.Packet `json:"packet,omitempty" msgpack:"packet,omitempty" bson:"packet,omitempty"`
		RequestId *string        `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	}

	// SocketsJoinLeaveMessage is a message for joining or leaving sockets.
	SocketsJoinLeaveMessage struct {
		Opts  *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
		Rooms []socket.Room  `json:"rooms,omitempty" msgpack:"rooms,omitempty" bson:"rooms,omitempty"`
	}

	// DisconnectSocketsMessage is a message for disconnecting sockets.
	DisconnectSocketsMessage struct {
		Opts  *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
		Close bool           `json:"close,omitempty" msgpack:"close,omitempty" bson:"close,omitempty"`
	}

	// FetchSocketsMessage is a message for fetching sockets.
	FetchSocketsMessage struct {
		Opts      *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
		RequestId string         `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	}

	CountSocketsMessage struct {
		Opts      *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
		RequestId string         `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	}

	CountSocketsResponse struct {
		RequestId string `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
		Count     uint64 `json:"count" msgpack:"count" bson:"count"`
	}

	ListRoomsMessage struct {
		Opts      *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
		RequestId string         `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	}

	ListRoomsResponse struct {
		RequestId string                 `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
		Rooms     map[socket.Room]uint64 `json:"rooms" msgpack:"rooms" bson:"rooms"`
	}

	// ServerSideEmitMessage is a message for server-side emit.
	ServerSideEmitMessage struct {
		RequestId *string `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
		Packet    []any   `json:"packet,omitempty" msgpack:"packet,omitempty" bson:"packet,omitempty"`
	}

	// ClusterRequest represents a cluster request.
	ClusterRequest struct {
		Type      MessageType
		Resolve   func(*types.Slice[any])
		Timeout   *atomic.Pointer[utils.Timer]
		Expected  int64
		Current   *atomic.Int64
		Responses *types.Slice[any]
		Once      sync.Once // guards against double callback invocation (timeout vs response race)
	}

	ClusterResponse = ClusterMessage

	// SocketResponse represents a socket response.
	SocketResponse struct {
		Id        socket.SocketId   `json:"id,omitempty" msgpack:"id,omitempty" bson:"id,omitempty"`
		Handshake *socket.Handshake `json:"handshake,omitempty" msgpack:"handshake,omitempty" bson:"handshake,omitempty"`
		Rooms     []socket.Room     `json:"rooms,omitempty" msgpack:"rooms,omitempty" bson:"rooms,omitempty"`
		Data      any               `json:"data,omitempty" msgpack:"data,omitempty" bson:"data,omitempty"`
	}

	// FetchSocketsResponse represents a response for fetching sockets.
	FetchSocketsResponse struct {
		RequestId string            `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
		Sockets   []*SocketResponse `json:"sockets" msgpack:"sockets" bson:"sockets"`
	}

	// ServerSideEmitResponse represents a response for server-side emit.
	ServerSideEmitResponse struct {
		RequestId string `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
		Packet    any    `json:"packet,omitempty" msgpack:"packet,omitempty" bson:"packet,omitempty"`
	}

	// BroadcastClientCount represents a broadcast client count.
	BroadcastClientCount struct {
		RequestId   string `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
		ClientCount uint64 `json:"clientCount" msgpack:"clientCount" bson:"clientCount"`
	}

	// BroadcastAck represents a broadcast acknowledgment.
	BroadcastAck struct {
		RequestId string `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
		Packet    any    `json:"packet,omitempty" msgpack:"packet,omitempty" bson:"packet,omitempty"`
	}

	// ClusterAckRequest represents a cluster acknowledgment request.
	ClusterAckRequest struct {
		ClientCountCallback func(uint64)
		Ack                 socket.Ack
	}

	// ClusterAdapter is an interface for a cluster-ready adapter.
	// Any implementation must provide methods for publishing messages and responses across the cluster.
	ClusterAdapter interface {
		Adapter

		// Uid returns the unique server ID.
		Uid() ServerId
		// OnMessage handles an incoming cluster message with its offset.
		OnMessage(*ClusterMessage, Offset)
		// OnResponse handles an incoming cluster response.
		OnResponse(*ClusterResponse)
		// Publish sends a cluster message to other nodes.
		Publish(*ClusterMessage)
		// PublishAndReturnOffset sends a message and returns its offset.
		PublishAndReturnOffset(*ClusterMessage) (Offset, error)
		// DoPublish performs the actual publish operation and returns the offset.
		DoPublish(*ClusterMessage) (Offset, error)
		// PublishResponse sends a response to a specific server.
		PublishResponse(ServerId, *ClusterResponse)
		// DoPublishResponse performs the actual publish response operation.
		DoPublishResponse(ServerId, *ClusterResponse) error
	}
)

const (
	EMITTER_UID     ServerId      = "emitter"
	DEFAULT_TIMEOUT time.Duration = 5_000 * time.Millisecond
)

const (
	INITIAL_HEARTBEAT MessageType = iota + 1
	HEARTBEAT
	BROADCAST
	SOCKETS_JOIN
	SOCKETS_LEAVE
	DISCONNECT_SOCKETS
	FETCH_SOCKETS
	FETCH_SOCKETS_RESPONSE
	SERVER_SIDE_EMIT
	SERVER_SIDE_EMIT_RESPONSE
	BROADCAST_CLIENT_COUNT
	BROADCAST_ACK
	ADAPTER_CLOSE
	COUNT_SOCKETS
	COUNT_SOCKETS_RESPONSE
	LIST_ROOMS
	LIST_ROOMS_RESPONSE
)

// IsValid performs a fast bounds check to ensure the MessageType is within defined enum constants.
func (m MessageType) IsValid() bool {
	return m >= INITIAL_HEARTBEAT && m <= LIST_ROOMS_RESPONSE
}
