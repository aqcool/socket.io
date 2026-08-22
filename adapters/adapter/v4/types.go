package adapter

import (
	"net/http"
	"net/url"
	"time"

	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
	socketio "github.com/aqcool/socket.io/v4"
)

type ServerID string
type Offset string
type MessageType int

const (
	InitialHeartbeat MessageType = iota + 1
	Heartbeat
	Broadcast
	SocketsJoin
	SocketsLeave
	DisconnectSockets
	FetchSockets
	FetchSocketsResponse
	ServerSideEmit
	ServerSideEmitResponse
	BroadcastClientCount
	BroadcastAck
	AdapterClose
	CountSockets
	CountSocketsResponse
	ListRooms
	ListRoomsResponse
)

const DefaultTimeout = 5 * time.Second

func (m MessageType) Valid() bool {
	return m >= InitialHeartbeat && m <= ListRoomsResponse
}

type ClusterMessage struct {
	UID    ServerID    `json:"uid,omitempty" msgpack:"uid,omitempty" bson:"uid,omitempty"`
	NSP    string      `json:"nsp,omitempty" msgpack:"nsp,omitempty" bson:"nsp,omitempty"`
	Type   MessageType `json:"type,omitempty" msgpack:"type,omitempty" bson:"type,omitempty"`
	Data   any         `json:"data,omitempty" msgpack:"data,omitempty" bson:"data,omitempty"`
	Offset Offset      `json:"offset,omitempty" msgpack:"offset,omitempty" bson:"offset,omitempty"`
}

type WireFlags struct {
	Compress             *bool          `json:"compress,omitempty" msgpack:"compress,omitempty" bson:"compress,omitempty"`
	Volatile             bool           `json:"volatile" msgpack:"volatile" bson:"volatile"`
	Local                bool           `json:"local" msgpack:"local" bson:"local"`
	Broadcast            bool           `json:"broadcast" msgpack:"broadcast" bson:"broadcast"`
	Binary               bool           `json:"binary" msgpack:"binary" bson:"binary"`
	Timeout              *time.Duration `json:"timeout,omitempty" msgpack:"timeout,omitempty" bson:"timeout,omitempty"`
	ExpectSingleResponse bool           `json:"expectSingleResponse" msgpack:"expectSingleResponse" bson:"expectSingleResponse"`
}

type PacketOptions struct {
	Rooms  []socketio.Room `json:"rooms,omitempty" msgpack:"rooms,omitempty" bson:"rooms,omitempty"`
	Except []socketio.Room `json:"except,omitempty" msgpack:"except,omitempty" bson:"except,omitempty"`
	Flags  *WireFlags      `json:"flags,omitempty" msgpack:"flags,omitempty" bson:"flags,omitempty"`
}

type BroadcastMessage struct {
	Opts      *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
	Packet    *parser.Packet `json:"packet,omitempty" msgpack:"packet,omitempty" bson:"packet,omitempty"`
	RequestID *string        `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
}

type SocketsJoinLeaveMessage struct {
	Opts  *PacketOptions  `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
	Rooms []socketio.Room `json:"rooms,omitempty" msgpack:"rooms,omitempty" bson:"rooms,omitempty"`
}

type DisconnectSocketsMessage struct {
	Opts  *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
	Close bool           `json:"close,omitempty" msgpack:"close,omitempty" bson:"close,omitempty"`
}

type FetchSocketsMessage struct {
	Opts      *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
	RequestID string         `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
}

type CountSocketsMessage struct {
	Opts      *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
	RequestID string         `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
}

type CountSocketsResponse struct {
	RequestID string `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	Count     uint64 `json:"count" msgpack:"count" bson:"count"`
}

type ListRoomsMessage struct {
	Opts      *PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty" bson:"opts,omitempty"`
	RequestID string         `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
}

type ListRoomsResponse struct {
	RequestID string                   `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	Rooms     map[socketio.Room]uint64 `json:"rooms" msgpack:"rooms" bson:"rooms"`
}

type ServerSideEmitMessage struct {
	RequestID *string `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	Packet    []any   `json:"packet,omitempty" msgpack:"packet,omitempty" bson:"packet,omitempty"`
}

type ServerSideEmitResponse struct {
	RequestID string `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	Packet    any    `json:"packet,omitempty" msgpack:"packet,omitempty" bson:"packet,omitempty"`
}

type BroadcastClientCount struct {
	RequestID   string `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	ClientCount uint64 `json:"clientCount" msgpack:"clientCount" bson:"clientCount"`
}

type BroadcastAckResponse struct {
	RequestID string `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	Packet    any    `json:"packet,omitempty" msgpack:"packet,omitempty" bson:"packet,omitempty"`
}

type WireHandshake struct {
	Headers http.Header    `json:"headers" msgpack:"headers" bson:"headers"`
	Time    string         `json:"time" msgpack:"time" bson:"time"`
	Address string         `json:"address" msgpack:"address" bson:"address"`
	XDomain bool           `json:"xdomain" msgpack:"xdomain" bson:"xdomain"`
	Secure  bool           `json:"secure" msgpack:"secure" bson:"secure"`
	Issued  int64          `json:"issued" msgpack:"issued" bson:"issued"`
	URL     string         `json:"url" msgpack:"url" bson:"url"`
	Query   url.Values     `json:"query" msgpack:"query" bson:"query"`
	Auth    map[string]any `json:"auth" msgpack:"auth" bson:"auth"`
}

type SocketResponse struct {
	ID        socketio.SocketID `json:"id,omitempty" msgpack:"id,omitempty" bson:"id,omitempty"`
	Handshake *WireHandshake    `json:"handshake,omitempty" msgpack:"handshake,omitempty" bson:"handshake,omitempty"`
	Rooms     []socketio.Room   `json:"rooms,omitempty" msgpack:"rooms,omitempty" bson:"rooms,omitempty"`
	Data      any               `json:"data,omitempty" msgpack:"data,omitempty" bson:"data,omitempty"`
}

type FetchSocketsResponseData struct {
	RequestID string            `json:"requestId,omitempty" msgpack:"requestId,omitempty" bson:"requestId,omitempty"`
	Sockets   []*SocketResponse `json:"sockets" msgpack:"sockets" bson:"sockets"`
}

func EncodeOptions(opts *socketio.BroadcastOptions) *PacketOptions {
	if opts == nil {
		return nil
	}
	flags := &WireFlags{
		Compress:             cloneBool(opts.Flags.Compress),
		Volatile:             opts.Flags.Volatile,
		Local:                opts.Flags.Local,
		Broadcast:            opts.Flags.Broadcast,
		Binary:               opts.Flags.Binary,
		Timeout:              cloneDuration(opts.Flags.Timeout),
		ExpectSingleResponse: opts.Flags.ExpectSingleResponse,
	}
	return &PacketOptions{
		Rooms:  append([]socketio.Room(nil), opts.Rooms...),
		Except: append([]socketio.Room(nil), opts.Except...),
		Flags:  flags,
	}
}

func DecodeOptions(opts *PacketOptions) *socketio.BroadcastOptions {
	if opts == nil {
		return &socketio.BroadcastOptions{}
	}
	result := &socketio.BroadcastOptions{
		Rooms:  append([]socketio.Room(nil), opts.Rooms...),
		Except: append([]socketio.Room(nil), opts.Except...),
	}
	if opts.Flags != nil {
		result.Flags = socketio.BroadcastFlags{
			Compress:             cloneBool(opts.Flags.Compress),
			Volatile:             opts.Flags.Volatile,
			Local:                opts.Flags.Local,
			Broadcast:            opts.Flags.Broadcast,
			Binary:               opts.Flags.Binary,
			Timeout:              cloneDuration(opts.Flags.Timeout),
			ExpectSingleResponse: opts.Flags.ExpectSingleResponse,
		}
	}
	return result
}

func EncodeHandshake(handshake socketio.Handshake) *WireHandshake {
	result := &WireHandshake{
		Headers: handshake.Headers.Clone(),
		Address: handshake.Address,
		Secure:  handshake.Secure,
		Auth:    cloneMap(handshake.Auth),
	}
	if !handshake.Time.IsZero() {
		result.Time = handshake.Time.Format(time.RFC1123)
		result.Issued = handshake.Time.UnixMilli()
	}
	if handshake.URL != nil {
		result.URL = handshake.URL.String()
		result.Query = handshake.URL.Query()
	}
	return result
}

func DecodeHandshake(handshake *WireHandshake) socketio.Handshake {
	if handshake == nil {
		return socketio.Handshake{}
	}
	result := socketio.Handshake{
		Headers: handshake.Headers.Clone(),
		Address: handshake.Address,
		Secure:  handshake.Secure,
		Auth:    cloneMap(handshake.Auth),
	}
	if handshake.Issued != 0 {
		result.Time = time.UnixMilli(handshake.Issued)
	} else if handshake.Time != "" {
		if parsed, err := time.Parse(time.RFC1123, handshake.Time); err == nil {
			result.Time = parsed
		}
	}
	if handshake.URL != "" {
		if parsed, err := url.Parse(handshake.URL); err == nil {
			result.URL = parsed
		}
	}
	return result
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneDuration(value *time.Duration) *time.Duration {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
