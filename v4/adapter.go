package socketio

import (
	"context"
	"time"
)

type AdapterCapabilities struct {
	Broadcast               bool
	RoomBroadcast           bool
	BroadcastAck            bool
	FetchSockets            bool
	SocketManagement        bool
	ServerSideEmit          bool
	NodeDiscovery           bool
	OrderedDelivery         bool
	DuplicateSuppression    bool
	ConnectionStateRecovery bool
	ExternalEmitter         bool
}

type BroadcastFlags struct {
	Volatile             bool
	Compress             *bool
	Local                bool
	Broadcast            bool
	Binary               bool
	Timeout              *time.Duration
	ExpectSingleResponse bool
}

type BroadcastOptions struct {
	Rooms  []Room
	Except []Room
	Flags  BroadcastFlags
}

type SocketDetails struct {
	ID        SocketID
	Handshake Handshake
	Rooms     []Room
	Data      any
}

type Session struct {
	SID   SocketID
	PID   PrivateSessionID
	Rooms []Room
	Data  any
}

type RecoveredSession struct {
	Session
	MissedPackets []any
}

type Adapter interface {
	Init(context.Context) error
	Close() error

	Capabilities() AdapterCapabilities
	ServerCount(context.Context) (int64, error)

	AddAll(context.Context, SocketID, ...Room) error
	Delete(context.Context, SocketID, Room) error
	DeleteAll(context.Context, SocketID) error
	SocketRooms(context.Context, SocketID) ([]Room, error)

	Broadcast(context.Context, Packet, *BroadcastOptions) error
	BroadcastWithAck(context.Context, Packet, *BroadcastOptions, func(uint64), Ack) error

	FetchSockets(context.Context, *BroadcastOptions) ([]SocketDetails, error)
	CountSockets(context.Context, *BroadcastOptions) (uint64, error)
	ListRooms(context.Context, *BroadcastOptions) (map[Room]uint64, error)

	AddSockets(context.Context, *BroadcastOptions, ...Room) error
	DeleteSockets(context.Context, *BroadcastOptions, ...Room) error
	DisconnectSockets(context.Context, *BroadcastOptions, bool) error

	ServerSideEmit(context.Context, []any) error

	PersistSession(context.Context, Session) error
	RestoreSession(context.Context, PrivateSessionID, string) (*RecoveredSession, error)
}

type AdapterFactory interface {
	New(*Namespace) (Adapter, error)
}
