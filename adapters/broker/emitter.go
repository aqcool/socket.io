package broker

import (
	"context"
	"errors"
	"strings"

	cluster "github.com/aqcool/socket.io/adapters/adapter/v3"
	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
	socket "github.com/aqcool/socket.io/servers/socket/v3"
)

// Emitter publishes Socket.IO cluster commands without hosting a Socket.IO
// server. It is shared by every concrete Broker-backed Adapter.
type Emitter struct {
	broker    Broker
	prefix    string
	namespace string
	rooms     []socket.Room
	except    []socket.Room
	flags     socket.BroadcastFlags
}

func NewEmitter(transport Broker, channelPrefix string) (*Emitter, error) {
	if transport == nil {
		return nil, errors.New("broker emitter: broker is required")
	}
	if channelPrefix == "" {
		channelPrefix = "socket.io"
	}
	return &Emitter{broker: transport, prefix: channelPrefix, namespace: "/"}, nil
}

func (e *Emitter) Of(namespace string) *Emitter {
	clone := e.clone()
	if namespace == "" {
		namespace = "/"
	}
	if !strings.HasPrefix(namespace, "/") {
		namespace = "/" + namespace
	}
	clone.namespace = namespace
	return clone
}

func (e *Emitter) To(rooms ...socket.Room) *Emitter {
	clone := e.clone()
	clone.rooms = append(clone.rooms, rooms...)
	return clone
}

func (e *Emitter) In(rooms ...socket.Room) *Emitter {
	return e.To(rooms...)
}

func (e *Emitter) Except(rooms ...socket.Room) *Emitter {
	clone := e.clone()
	clone.except = append(clone.except, rooms...)
	return clone
}

func (e *Emitter) Volatile() *Emitter {
	clone := e.clone()
	clone.flags.Volatile = true
	return clone
}

func (e *Emitter) Compress(enabled bool) *Emitter {
	clone := e.clone()
	clone.flags.Compress = &enabled
	return clone
}

func (e *Emitter) Emit(event string, args ...any) error {
	packet := &parser.Packet{Type: parser.EVENT, Data: append([]any{event}, args...)}
	return e.publish(&cluster.ClusterMessage{
		Type: cluster.BROADCAST,
		Data: &cluster.BroadcastMessage{
			Packet: packet,
			Opts:   e.packetOptions(),
		},
	})
}

func (e *Emitter) SocketsJoin(rooms ...socket.Room) error {
	return e.publish(&cluster.ClusterMessage{
		Type: cluster.SOCKETS_JOIN,
		Data: &cluster.SocketsJoinLeaveMessage{Opts: e.packetOptions(), Rooms: rooms},
	})
}

func (e *Emitter) SocketsLeave(rooms ...socket.Room) error {
	return e.publish(&cluster.ClusterMessage{
		Type: cluster.SOCKETS_LEAVE,
		Data: &cluster.SocketsJoinLeaveMessage{Opts: e.packetOptions(), Rooms: rooms},
	})
}

func (e *Emitter) DisconnectSockets(close bool) error {
	return e.publish(&cluster.ClusterMessage{
		Type: cluster.DISCONNECT_SOCKETS,
		Data: &cluster.DisconnectSocketsMessage{Opts: e.packetOptions(), Close: close},
	})
}

func (e *Emitter) ServerSideEmit(event string, args ...any) error {
	return e.publish(&cluster.ClusterMessage{
		Type: cluster.SERVER_SIDE_EMIT,
		Data: &cluster.ServerSideEmitMessage{Packet: append([]any{event}, args...)},
	})
}

func (e *Emitter) publish(message *cluster.ClusterMessage) error {
	message.Uid = cluster.EMITTER_UID
	message.Nsp = e.namespace
	payload, err := cluster.EncodeClusterMessage(message)
	if err != nil {
		return err
	}
	_, err = e.broker.Publish(context.Background(), subject(e.prefix, e.namespace), payload)
	return err
}

func (e *Emitter) packetOptions() *cluster.PacketOptions {
	return &cluster.PacketOptions{
		Rooms:  append([]socket.Room(nil), e.rooms...),
		Except: append([]socket.Room(nil), e.except...),
		Flags:  &e.flags,
	}
}

func (e *Emitter) clone() *Emitter {
	clone := *e
	clone.rooms = append([]socket.Room(nil), e.rooms...)
	clone.except = append([]socket.Room(nil), e.except...)
	return &clone
}
