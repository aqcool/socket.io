package socket

import (
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

type (
	// AdapterCapabilities declares optional and distributed behavior without
	// requiring callers to infer support from concrete Adapter types.
	AdapterCapabilities struct {
		Broadcast               bool `json:"broadcast"`
		RoomBroadcast           bool `json:"roomBroadcast"`
		BroadcastAck            bool `json:"broadcastAck"`
		FetchSockets            bool `json:"fetchSockets"`
		SocketManagement        bool `json:"socketManagement"`
		ServerSideEmit          bool `json:"serverSideEmit"`
		NodeDiscovery           bool `json:"nodeDiscovery"`
		OrderedDelivery         bool `json:"orderedDelivery"`
		DuplicateSuppression    bool `json:"duplicateSuppression"`
		ConnectionStateRecovery bool `json:"connectionStateRecovery"`
		ExternalEmitter         bool `json:"externalEmitter"`
	}

	AdapterCapabilityProvider interface {
		Capabilities() AdapterCapabilities
	}

	// A public ID, sent by the server at the beginning of the Socket.IO session and which can be used for private messaging
	SocketId string

	// A private ID, sent by the server at the beginning of the Socket.IO session and used for connection state recovery
	// upon reconnection
	PrivateSessionId string

	// we could extend the Room type to "string", but that would be a breaking change
	// Related: https://github.com/socketio/socket.io-redis-adapter/issues/418
	Room string

	WriteOptions struct {
		packet.Options

		Volatile   bool `json:"volatile" msgpack:"volatile" bson:"volatile"`
		PreEncoded bool `json:"preEncoded" msgpack:"preEncoded" bson:"preEncoded"`
	}

	BroadcastFlags struct {
		WriteOptions

		Local     bool           `json:"local" msgpack:"local" bson:"local"`
		Broadcast bool           `json:"broadcast" msgpack:"broadcast" bson:"broadcast"`
		Binary    bool           `json:"binary" msgpack:"binary" bson:"binary"`
		Timeout   *time.Duration `json:"timeout,omitempty" msgpack:"timeout,omitempty" bson:"timeout,omitempty"`

		ExpectSingleResponse bool `json:"expectSingleResponse" msgpack:"expectSingleResponse" bson:"expectSingleResponse"`
	}

	BroadcastOptions struct {
		Rooms  *types.Set[Room] `json:"rooms,omitempty" msgpack:"rooms,omitempty"`
		Except *types.Set[Room] `json:"except,omitempty" msgpack:"except,omitempty"`
		Flags  *BroadcastFlags  `json:"flags,omitempty" msgpack:"flags,omitempty"`
	}

	SessionToPersist struct {
		Sid   SocketId         `json:"sid" msgpack:"sid"`
		Pid   PrivateSessionId `json:"pid" msgpack:"pid"`
		Rooms *types.Set[Room] `json:"rooms,omitempty" msgpack:"rooms,omitempty"`
		Data  any              `json:"data" msgpack:"data"`
	}

	Session struct {
		*SessionToPersist

		MissedPackets []any `json:"missedPackets" msgpack:"missedPackets"`
	}

	PersistedPacket struct {
		Id        string            `json:"id" msgpack:"id"`
		EmittedAt int64             `json:"emittedAt" msgpack:"emittedAt"`
		Data      any               `json:"data" msgpack:"data"`
		Opts      *BroadcastOptions `json:"opts,omitempty" msgpack:"opts,omitempty"`
	}

	SessionWithTimestamp struct {
		*SessionToPersist

		DisconnectedAt int64 `json:"disconnectedAt" msgpack:"disconnectedAt"`
	}

	Adapter interface {
		types.EventEmitter

		// SupportsConnectionStateRecovery reports whether sessions and missed
		// packets can be restored by this adapter.
		SupportsConnectionStateRecovery() bool

		// #prototype

		Prototype(Adapter)
		Proto() Adapter

		Rooms() *types.Map[Room, *types.Set[SocketId]]
		Sids() *types.Map[SocketId, *types.Set[Room]]
		Nsp() Namespace

		// Construct() should be called after calling Prototype()
		Construct(Namespace)

		// To be overridden
		Init()

		// To be overridden
		Close()

		// Returns the number of Socket.IO servers in the cluster
		ServerCount() int64

		// CountSockets returns the number of matching sockets without
		// serializing complete socket details.
		CountSockets(*BroadcastOptions) func(func(uint64, error))

		// ListRooms returns matching room names and their socket counts without
		// serializing complete socket details.
		ListRooms(*BroadcastOptions) func(func(map[Room]uint64, error))

		// Adds a socket to a list of room.
		AddAll(SocketId, *types.Set[Room])

		// Removes a socket from a room.
		Del(SocketId, Room)

		// Removes a socket from all rooms it's joined.
		DelAll(SocketId)

		// Broadcasts a packet.
		//
		// Options:
		//  - `Flags` {*BroadcastFlags} flags for this packet
		//  - `Except` {*types.Set[Room]} sids that should be excluded
		//  - `Rooms` {*types.Set[Room]} list of rooms to broadcast to
		Broadcast(*parser.Packet, *BroadcastOptions)

		// Broadcasts a packet and expects multiple acknowledgements.
		//
		// Options:
		//  - `Flags` {*BroadcastFlags} flags for this packet
		//  - `Except` {*types.Set[Room]} sids that should be excluded
		//  - `Rooms` {*types.Set[Room]} list of rooms to broadcast to
		BroadcastWithAck(*parser.Packet, *BroadcastOptions, func(uint64), Ack)

		// Gets a list of sockets by sid.
		Sockets(*types.Set[Room]) *types.Set[SocketId]

		// Gets the list of rooms a given socket has joined.
		SocketRooms(SocketId) *types.Set[Room]

		// Returns the matching socket instances
		FetchSockets(*BroadcastOptions) func(func([]SocketDetails, error))

		// Makes the matching socket instances join the specified rooms
		AddSockets(*BroadcastOptions, []Room)

		// Makes the matching socket instances leave the specified rooms
		DelSockets(*BroadcastOptions, []Room)

		// Makes the matching socket instances disconnect
		DisconnectSockets(*BroadcastOptions, bool)

		// Send a packet to the other Socket.IO servers in the cluster
		ServerSideEmit([]any) error

		// Save the client session in order to restore it upon reconnection.
		PersistSession(*SessionToPersist)

		// Restore the session and find the packets that were missed by the client.
		RestoreSession(PrivateSessionId, string) (*Session, error)
	}

	SessionAwareAdapter interface {
		Adapter
	}

	ParentBroadcastAdapter interface {
		Adapter
	}

	AdapterConstructor interface {
		New(Namespace) Adapter
		SupportsConnectionStateRecovery() bool
	}
)

// CapabilitiesOf returns a standardized declaration, with a conservative
// legacy fallback for third-party adapters and builders.
func CapabilitiesOf(value any) AdapterCapabilities {
	if provider, ok := value.(AdapterCapabilityProvider); ok {
		return provider.Capabilities()
	}
	recovery := false
	if legacy, ok := value.(interface{ SupportsConnectionStateRecovery() bool }); ok {
		recovery = legacy.SupportsConnectionStateRecovery()
	}
	return AdapterCapabilities{ConnectionStateRecovery: recovery}
}
