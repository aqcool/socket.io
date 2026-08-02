package typed

import (
	"reflect"
	"strings"
)

type Direction string

const (
	ClientToServer Direction = "client-to-server"
	ServerToClient Direction = "server-to-client"
	ServerToServer Direction = "server-to-server"
	Bidirectional  Direction = "bidirectional"
)

type NoAck struct{}

// NoRequest marks a lifecycle event which does not carry an application
// payload. It is mainly used by the reserved "connect" event.
type NoRequest struct{}

// DisconnectReason is the Go equivalent of the official Socket.IO
// DisconnectReason string union.
type DisconnectReason string

const (
	DisconnectTransportError     DisconnectReason = "transport error"
	DisconnectTransportClose     DisconnectReason = "transport close"
	DisconnectForcedClose        DisconnectReason = "forced close"
	DisconnectPingTimeout        DisconnectReason = "ping timeout"
	DisconnectParseError         DisconnectReason = "parse error"
	DisconnectServerShuttingDown DisconnectReason = "server shutting down"
	DisconnectForcedServerClose  DisconnectReason = "forced server close"
	DisconnectClientNamespace    DisconnectReason = "client namespace disconnect"
	DisconnectServerNamespace    DisconnectReason = "server namespace disconnect"
)

var (
	// ConnectEvent, ConnectErrorEvent, DisconnectEvent and DisconnectingEvent
	// describe the reserved Socket.IO lifecycle events. Use OnConnect for the
	// zero-argument connect event and the regular On helper for the others.
	ConnectEvent       = Event[NoRequest, NoAck]{Name: "connect"}
	ConnectErrorEvent  = Event[error, NoAck]{Name: "connect_error"}
	DisconnectEvent    = Event[DisconnectReason, NoAck]{Name: "disconnect"}
	DisconnectingEvent = Event[DisconnectReason, NoAck]{Name: "disconnecting"}
)

type Event[Request, Response any] struct {
	Name        string
	Direction   Direction
	Description string
}

func (e Event[Request, Response]) Definition() Definition {
	return Definition{
		Name:         e.Name,
		Direction:    e.Direction,
		Description:  e.Description,
		RequestType:  reflect.TypeFor[Request](),
		ResponseType: reflect.TypeFor[Response](),
	}
}

type Definition struct {
	Name         string
	Direction    Direction
	Description  string
	RequestType  reflect.Type
	ResponseType reflect.Type
}

type Namespace struct {
	Name   string
	Events []Definition
}

func NewNamespace(name string, definitions ...Definition) Namespace {
	if name == "" {
		name = "/"
	}
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	return Namespace{Name: name, Events: append([]Definition(nil), definitions...)}
}
