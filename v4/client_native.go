package socketio

import (
	"context"
	"fmt"
	"sync"

	enginepacket "github.com/aqcool/socket.io/parsers/engine/v3/packet"
	engine "github.com/aqcool/socket.io/servers/engine/v3"
)

type client struct {
	server  *Server
	conn    engine.Socket
	decoder PacketDecoder
	encoder PacketEncoder

	mu      sync.RWMutex
	sockets map[string]*Socket
	closed  bool
}

func newClient(server *Server, conn engine.Socket) *client {
	return &client{
		server:  server,
		conn:    conn,
		decoder: server.codec.NewDecoder(),
		encoder: server.codec.NewEncoder(),
		sockets: make(map[string]*Socket),
	}
}

func (c *client) start() {
	_ = c.conn.On("message", func(args ...any) {
		if len(args) == 0 {
			return
		}
		packets, err := c.decoder.Add(args[0])
		if err != nil {
			c.close("parse error")
			return
		}
		for _, packet := range packets {
			c.onPacket(packet)
		}
	})
	_ = c.conn.Once("close", func(args ...any) {
		reason := "transport close"
		if len(args) > 0 {
			if value, ok := args[0].(string); ok && value != "" {
				reason = value
			}
		}
		c.close(reason)
	})
	_ = c.conn.Once("error", func(...any) {
		c.close("transport error")
	})
	if c.conn.Protocol() == 3 {
		c.connect("/", nil)
	}
}

func (c *client) onPacket(packet Packet) {
	nsp := normalizeNamespace(packet.Namespace)
	switch packet.Type {
	case PacketConnect:
		auth, _ := packet.Data.(map[string]any)
		c.connect(nsp, auth)
	case PacketEvent, PacketBinaryEvent, PacketAck, PacketBinaryAck, PacketDisconnect:
		c.mu.RLock()
		socket := c.sockets[nsp]
		c.mu.RUnlock()
		if socket != nil {
			socket.onPacket(packet)
		}
	}
}

func (c *client) connect(name string, auth map[string]any) {
	c.mu.RLock()
	existing := c.sockets[name]
	closed := c.closed
	c.mu.RUnlock()
	if closed || existing != nil {
		return
	}

	nsp, err := c.server.resolveNamespace(name, auth)
	if err != nil || nsp == nil {
		message := "Invalid namespace"
		if err != nil {
			message = err.Error()
		}
		_ = c.writePacket(Packet{
			Type:      PacketConnectError,
			Namespace: name,
			Data:      map[string]any{"message": message},
		}, BroadcastFlags{})
		return
	}

	if _, err = nsp.connect(c, auth); err != nil {
		data := map[string]any{"message": err.Error()}
		var connectErr *ConnectError
		if errorsAs(err, &connectErr) && connectErr.Data != nil {
			data["data"] = connectErr.Data
		}
		_ = c.writePacket(Packet{
			Type:      PacketConnectError,
			Namespace: name,
			Data:      data,
		}, BroadcastFlags{})
	}
}

func errorsAs(err error, target any) bool {
	return errorAs(err, target)
}

func (c *client) register(namespace string, socket *Socket) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if existing := c.sockets[namespace]; existing != nil && existing != socket {
		return ErrAlreadyConnected
	}
	c.sockets[namespace] = socket
	return nil
}

func (c *client) remove(namespace string, socket *Socket) {
	c.mu.Lock()
	if c.sockets[namespace] == socket {
		delete(c.sockets, namespace)
	}
	c.mu.Unlock()
}

func (c *client) writePacket(packet Packet, flags BroadcastFlags) error {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if flags.Volatile && !c.conn.Transport().Writable() {
		return nil
	}

	frames, err := c.encoder.Encode(packet)
	if err != nil {
		return err
	}
	options := &enginepacket.Options{Compress: flags.Compress}
	for _, frame := range frames {
		if frame == nil {
			continue
		}
		c.conn.Send(frame, options, nil)
	}
	return nil
}

func (c *client) close(reason string) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	sockets := make([]*Socket, 0, len(c.sockets))
	for _, socket := range c.sockets {
		sockets = append(sockets, socket)
	}
	clear(c.sockets)
	c.mu.Unlock()

	_ = c.decoder.Close()
	for _, socket := range sockets {
		socket.close(reason)
	}
	c.server.dropClient(c)
}

func (c *client) disconnectAll() {
	c.close("server shutting down")
	c.conn.Close(true)
}

func (c *client) Context() context.Context { return c.server.Context() }
func (c *client) String() string           { return fmt.Sprintf("client(%s)", c.conn.Id()) }
