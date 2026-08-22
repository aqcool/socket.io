package socket

import (
	"github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/parsers/socket/v4/parser"
)

// ReadyState represents the state of the connection.
type (
	ReadyState string

	// Packet represents a Socket.IO packet, including options for encoding.
	Packet struct {
		*parser.Packet

		Options *packet.Options `json:"options,omitempty" msgpack:"options,omitempty"`
	}

	// Handshake contains information from the server handshake.
	Handshake struct {
		Sid string `json:"sid" msgpack:"sid"`
		Pid string `json:"pid,omitempty" msgpack:"pid,omitempty"`
	}
)

const (
	// ReadyStateOpen indicates the connection is open.
	ReadyStateOpen ReadyState = "open"
	// ReadyStateOpening indicates the connection is in the process of opening.
	ReadyStateOpening ReadyState = "opening"
	// ReadyStateClosed indicates the connection is closed.
	ReadyStateClosed ReadyState = "closed"
)
