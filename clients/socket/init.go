// Package socket provides a Socket.IO client implementation in Go.
// It enables real-time, bidirectional event-based communication between web clients and servers.
//
// Example usage:
//
//	socket, err := socket.Connect("http://localhost:8080", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	socket.On("connect", func() {
//	    socket.Emit("hello", "world")
//	})
package socket

import (
	"sync"

	"github.com/aqcool/socket.io/clients/engine/v3/transports"
	"github.com/aqcool/socket.io/v3/pkg/log"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/utils"
)

var (
	managerLog = log.NewLog("socket.io-client:manager")
	socketLog  = log.NewLog("socket.io-client:socket")
	clientLog  = log.NewLog("socket.io-client")

	RESERVED_EVENTS = types.NewSet(
		"connect", "connect_error", "disconnect", "disconnecting",
		"drain", "overflow", "slow_consumer",
		"newListener", "removeListener",
	)

	Polling      = transports.Polling
	WebSocket    = transports.WebSocket
	WebTransport = transports.WebTransport

	cache   types.Map[string, *Manager]
	cacheMu sync.Mutex
)

func init() {
	cache = types.Map[string, *Manager]{}
}

// lookup returns a Socket instance for the given URI and options.
// It manages socket caching and multiplexing according to the options provided.
func lookup(uri string, opts OptionsInterface) (*Socket, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	path := "/socket.io"
	if opts.GetRawPath() != nil {
		path = opts.Path()
	}
	parsed, err := utils.Url(uri, path)
	if err != nil {
		return nil, err
	}
	if opts.Query() == nil && parsed.RawQuery != "" {
		// The Engine.IO connection is created by NewManager, so the query from
		// the URI must be copied before constructing the Manager.
		opts.SetQuery(parsed.Query())
	}

	source := parsed.String()
	id := parsed.Id
	multiplex := true
	if opts.GetRawMultiplex() != nil {
		multiplex = opts.Multiplex()
	}
	if opts.ForceNew() || !multiplex {
		clientLog.Debug("ignoring socket cache for %s", source)
		return NewManager(source, opts).Socket(parsed.Path, opts), nil
	}

	// NewManager has the side effect of opening an Engine.IO connection when
	// autoConnect is enabled. Keep cache lookup, Manager creation and namespace
	// reservation in one critical section so a cache hit never constructs a
	// throwaway Manager and concurrent lookups of the same namespace still use
	// separate Managers, like the official client.
	cacheMu.Lock()
	defer cacheMu.Unlock()
	manager, ok := cache.Load(id)
	if ok {
		if _, sameNamespace := manager.nsps.Load(parsed.Path); sameNamespace {
			clientLog.Debug("ignoring socket cache for %s", source)
			return NewManager(source, opts).Socket(parsed.Path, opts), nil
		}
	} else {
		manager = NewManager(source, opts)
		cache.Store(id, manager)
		clientLog.Debug("new io instance for %s", source)
	}
	return manager.Socket(parsed.Path, opts), nil
}

// Io returns a Socket instance for the given URI and options.
// It is an alias for Connect.
func Io(uri string, opts OptionsInterface) (*Socket, error) {
	return lookup(uri, opts)
}

// Connect returns a Socket instance for the given URI and options.
// It is the main entry point for establishing a new connection.
func Connect(uri string, opts OptionsInterface) (*Socket, error) {
	return lookup(uri, opts)
}
