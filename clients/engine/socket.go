// Package engine implements a client-side Engine.IO transport layer.
// It provides real-time bidirectional communication between clients and servers
// using various transport mechanisms including WebSocket, HTTP long-polling,
// and WebTransport.
//
// The package supports automatic transport upgrade, binary data transmission,
// heartbeat handling, and graceful close. Reconnection belongs to the higher-level
// Socket.IO Manager, not the Engine.IO socket itself.
package engine

import "github.com/aqcool/socket.io/v4/pkg/types"

// Socket provides a WebSocket-like interface to connect to an Engine.IO server.
// It supports multiple transport protocols including HTTP long-polling, WebSocket,
// and WebTransport.
//
// Key features:
//   - Automatic transport upgrade mechanism
//   - Fallback to lower-level transports if higher-level ones fail
//   - Event-based communication
//   - Support for binary data
//
// Example usage:
//
//	import (
//		"strings"
//
//		"github.com/aqcool/socket.io/clients/engine/v4"
//	)
//
//	func main() {
//		opts := engine.DefaultSocketOptions()
//		opts.SetTransportList([]engine.TransportCtor{
//			&engine.PollingBuilder{},
//			&engine.WebSocketBuilder{},
//		})
//		socket := engine.NewSocket("http://localhost:8080", opts)
//		socket.On("open", func(...any) {
//			socket.Send(strings.NewReader("hello"), nil, nil)
//		})
//	}
//
// See: [SocketWithoutUpgrade] for a simpler implementation without transport upgrade
//
// See: [SocketWithUpgrade] for the base implementation with upgrade support
type socket struct {
	SocketWithUpgrade
}

// MakeSocket creates a new Socket instance with default settings.
// This is the factory function for creating a new socket.
//
// Returns:
//   - Socket: A new Socket instance initialized with default settings.
func MakeSocket() Socket {
	s := &socket{
		SocketWithUpgrade: MakeSocketWithUpgrade(),
	}

	s.Prototype(s)

	return s
}

// NewSocket creates a new Socket instance with the specified URI and options.
//
// Parameters:
//   - uri: The URI to connect to (e.g., "http://localhost:8080")
//   - opts: Socket configuration options. If nil, default options will be used
//
// Returns:
//   - Socket: A new Socket instance configured with the specified options
func NewSocket(uri string, opts SocketOptionsInterface) Socket {
	s := MakeSocket()

	s.Construct(uri, opts)

	return s
}

// Construct initializes the socket with the given URI and options.
// This is an internal method used by NewSocket to set up the connection.
//
// Parameters:
//   - uri: The URI to connect to
//   - opts: Socket configuration options. If nil, default options will be used
func (s *socket) Construct(uri string, opts SocketOptionsInterface) {
	if opts == nil {
		opts = DefaultSocketOptions()
	}

	if opts.Transports() == nil {
		defaults := []TransportCtor{&PollingBuilder{}, &WebSocketBuilder{}, &WebTransportBuilder{}}
		if ordered, ok := opts.(interface{ SetTransportList([]TransportCtor) }); ok {
			ordered.SetTransportList(defaults)
		} else {
			opts.SetTransports(types.NewSet(defaults...))
		}
	}

	s.SocketWithUpgrade.Construct(uri, opts)
}
