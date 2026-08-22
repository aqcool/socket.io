package emitter

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aqcool/socket.io/adapters/adapter/v4"
	redisbridge "github.com/aqcool/socket.io/adapters/redis/v4"
	"github.com/aqcool/socket.io/parsers/socket/v4/parser"
	"github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/aqcool/socket.io/v4/pkg/utils"
	rds "github.com/redis/go-redis/v9"
)

const (
	// DefaultRedisStreamsEmitterStreamName matches
	// @socket.io/redis-streams-emitter@0.1.1.
	DefaultRedisStreamsEmitterStreamName = "socket.io"

	// DefaultRedisStreamsEmitterMaxLen is the approximate maximum stream size.
	DefaultRedisStreamsEmitterMaxLen int64 = 10_000
)

// RedisStreamsEmitterOptions configures a Redis Streams external emitter.
type RedisStreamsEmitterOptions struct {
	StreamName string
	MaxLen     int64
}

// DefaultRedisStreamsEmitterOptions returns the official defaults.
func DefaultRedisStreamsEmitterOptions() *RedisStreamsEmitterOptions {
	return &RedisStreamsEmitterOptions{
		StreamName: DefaultRedisStreamsEmitterStreamName,
		MaxLen:     DefaultRedisStreamsEmitterMaxLen,
	}
}

func normalizeRedisStreamsEmitterOptions(opts *RedisStreamsEmitterOptions) RedisStreamsEmitterOptions {
	normalized := *DefaultRedisStreamsEmitterOptions()
	if opts == nil {
		return normalized
	}
	if opts.StreamName != "" {
		normalized.StreamName = opts.StreamName
	}
	if opts.MaxLen > 0 {
		normalized.MaxLen = opts.MaxLen
	}
	return normalized
}

// RedisStreamsEmitter publishes external Socket.IO cluster messages with XADD.
// Its wire format matches @socket.io/redis-streams-emitter@0.1.1.
type RedisStreamsEmitter struct {
	redisClient *redisbridge.RedisClient
	opts        RedisStreamsEmitterOptions
	nsp         string
}

// NewRedisStreamsEmitter creates an external emitter for the root namespace.
func NewRedisStreamsEmitter(client *redisbridge.RedisClient, opts *RedisStreamsEmitterOptions, nsps ...string) *RedisStreamsEmitter {
	nsp := defaultNamespace
	if len(nsps) > 0 && nsps[0] != "" {
		nsp = nsps[0]
	}
	if !strings.HasPrefix(nsp, "/") {
		nsp = "/" + nsp
	}
	return &RedisStreamsEmitter{
		redisClient: client,
		opts:        normalizeRedisStreamsEmitterOptions(opts),
		nsp:         nsp,
	}
}

// Of returns a new emitter for the given namespace.
func (e *RedisStreamsEmitter) Of(nsp string) *RedisStreamsEmitter {
	return NewRedisStreamsEmitter(e.redisClient, &e.opts, nsp)
}

// Emit broadcasts an event to all clients in the namespace.
func (e *RedisStreamsEmitter) Emit(ev string, args ...any) error {
	return e.newBroadcastOperator().Emit(ev, args...)
}

// To targets one or more rooms.
func (e *RedisStreamsEmitter) To(rooms ...socket.Room) *RedisStreamsBroadcastOperator {
	return e.newBroadcastOperator().To(rooms...)
}

// In is an alias for To.
func (e *RedisStreamsEmitter) In(rooms ...socket.Room) *RedisStreamsBroadcastOperator {
	return e.To(rooms...)
}

// Except excludes one or more rooms.
func (e *RedisStreamsEmitter) Except(rooms ...socket.Room) *RedisStreamsBroadcastOperator {
	return e.newBroadcastOperator().Except(rooms...)
}

// Volatile marks the next broadcast as volatile.
func (e *RedisStreamsEmitter) Volatile() *RedisStreamsBroadcastOperator {
	return e.newBroadcastOperator().Volatile()
}

// Compress sets the Socket.IO compress flag.
func (e *RedisStreamsEmitter) Compress(compress bool) *RedisStreamsBroadcastOperator {
	return e.newBroadcastOperator().Compress(compress)
}

// SocketsJoin makes matching sockets join the given rooms.
func (e *RedisStreamsEmitter) SocketsJoin(rooms ...socket.Room) error {
	return e.newBroadcastOperator().SocketsJoin(rooms...)
}

// SocketsLeave makes matching sockets leave the given rooms.
func (e *RedisStreamsEmitter) SocketsLeave(rooms ...socket.Room) error {
	return e.newBroadcastOperator().SocketsLeave(rooms...)
}

// DisconnectSockets disconnects all matching sockets.
func (e *RedisStreamsEmitter) DisconnectSockets(close bool) error {
	return e.newBroadcastOperator().DisconnectSockets(close)
}

// ServerSideEmit sends an event to Socket.IO server instances.
func (e *RedisStreamsEmitter) ServerSideEmit(args ...any) error {
	return e.newBroadcastOperator().ServerSideEmit(args...)
}

func (e *RedisStreamsEmitter) newBroadcastOperator() *RedisStreamsBroadcastOperator {
	return newRedisStreamsBroadcastOperator(e.redisClient, e.opts, e.nsp, nil, nil, nil)
}

// RedisStreamsBroadcastOperator is the immutable fluent operator used by
// RedisStreamsEmitter.
type RedisStreamsBroadcastOperator struct {
	redisClient *redisbridge.RedisClient
	opts        RedisStreamsEmitterOptions
	nsp         string
	rooms       *types.Set[socket.Room]
	exceptRooms *types.Set[socket.Room]
	flags       *socket.BroadcastFlags
}

func newRedisStreamsBroadcastOperator(
	client *redisbridge.RedisClient,
	opts RedisStreamsEmitterOptions,
	nsp string,
	rooms *types.Set[socket.Room],
	exceptRooms *types.Set[socket.Room],
	flags *socket.BroadcastFlags,
) *RedisStreamsBroadcastOperator {
	if rooms == nil {
		rooms = types.NewSet[socket.Room]()
	}
	if exceptRooms == nil {
		exceptRooms = types.NewSet[socket.Room]()
	}
	if flags == nil {
		flags = &socket.BroadcastFlags{}
	}
	return &RedisStreamsBroadcastOperator{
		redisClient: client,
		opts:        opts,
		nsp:         nsp,
		rooms:       rooms,
		exceptRooms: exceptRooms,
		flags:       flags,
	}
}

// To targets one or more rooms and returns a new operator.
func (b *RedisStreamsBroadcastOperator) To(rooms ...socket.Room) *RedisStreamsBroadcastOperator {
	next := types.NewSet(b.rooms.Keys()...)
	next.Add(rooms...)
	return newRedisStreamsBroadcastOperator(b.redisClient, b.opts, b.nsp, next, b.exceptRooms, b.flags)
}

// In is an alias for To.
func (b *RedisStreamsBroadcastOperator) In(rooms ...socket.Room) *RedisStreamsBroadcastOperator {
	return b.To(rooms...)
}

// Except excludes one or more rooms and returns a new operator.
func (b *RedisStreamsBroadcastOperator) Except(rooms ...socket.Room) *RedisStreamsBroadcastOperator {
	next := types.NewSet(b.exceptRooms.Keys()...)
	next.Add(rooms...)
	return newRedisStreamsBroadcastOperator(b.redisClient, b.opts, b.nsp, b.rooms, next, b.flags)
}

// Compress sets the compress flag and returns a new operator.
func (b *RedisStreamsBroadcastOperator) Compress(compress bool) *RedisStreamsBroadcastOperator {
	flags := *b.flags
	flags.Compress = &compress
	return newRedisStreamsBroadcastOperator(b.redisClient, b.opts, b.nsp, b.rooms, b.exceptRooms, &flags)
}

// Volatile sets the volatile flag and returns a new operator.
func (b *RedisStreamsBroadcastOperator) Volatile() *RedisStreamsBroadcastOperator {
	flags := *b.flags
	flags.Volatile = true
	return newRedisStreamsBroadcastOperator(b.redisClient, b.opts, b.nsp, b.rooms, b.exceptRooms, &flags)
}

// Emit broadcasts an event through the Redis stream.
func (b *RedisStreamsBroadcastOperator) Emit(ev string, args ...any) error {
	if reservedEvents.Has(ev) {
		return fmt.Errorf(`"%s" is a reserved event name`, ev)
	}
	return b.publish(&adapter.ClusterMessage{
		Type: adapter.BROADCAST,
		Data: &adapter.BroadcastMessage{
			Packet: &parser.Packet{
				Type: parser.EVENT,
				Nsp:  b.nsp,
				Data: append([]any{ev}, args...),
			},
			Opts: b.packetOptions(true),
		},
	})
}

// SocketsJoin makes matching sockets join the given rooms.
func (b *RedisStreamsBroadcastOperator) SocketsJoin(rooms ...socket.Room) error {
	return b.publish(&adapter.ClusterMessage{
		Type: adapter.SOCKETS_JOIN,
		Data: &adapter.SocketsJoinLeaveMessage{
			Opts:  b.packetOptions(false),
			Rooms: rooms,
		},
	})
}

// SocketsLeave makes matching sockets leave the given rooms.
func (b *RedisStreamsBroadcastOperator) SocketsLeave(rooms ...socket.Room) error {
	return b.publish(&adapter.ClusterMessage{
		Type: adapter.SOCKETS_LEAVE,
		Data: &adapter.SocketsJoinLeaveMessage{
			Opts:  b.packetOptions(false),
			Rooms: rooms,
		},
	})
}

// DisconnectSockets disconnects matching sockets.
func (b *RedisStreamsBroadcastOperator) DisconnectSockets(close bool) error {
	return b.publish(&adapter.ClusterMessage{
		Type: adapter.DISCONNECT_SOCKETS,
		Data: &adapter.DisconnectSocketsMessage{
			Opts:  b.packetOptions(false),
			Close: close,
		},
	})
}

// ServerSideEmit sends an event to Socket.IO server instances.
func (b *RedisStreamsBroadcastOperator) ServerSideEmit(args ...any) error {
	if len(args) > 0 {
		if _, withAck := args[len(args)-1].(socket.Ack); withAck {
			return errors.New("acknowledgements are not supported when using emitter")
		}
	}
	return b.publish(&adapter.ClusterMessage{
		Type: adapter.SERVER_SIDE_EMIT,
		Data: &adapter.ServerSideEmitMessage{Packet: args},
	})
}

func (b *RedisStreamsBroadcastOperator) packetOptions(includeFlags bool) *adapter.PacketOptions {
	opts := &adapter.PacketOptions{
		Rooms:  b.rooms.Keys(),
		Except: b.exceptRooms.Keys(),
	}
	if includeFlags {
		opts.Flags = b.flags
	}
	return opts
}

func (b *RedisStreamsBroadcastOperator) publish(message *adapter.ClusterMessage) error {
	if b.redisClient == nil || b.redisClient.Client == nil {
		return errors.New("redis streams emitter requires a Redis client")
	}

	message.Uid = emitterUID
	message.Nsp = b.nsp
	values, err := encodeRedisStreamsEmitterMessage(message)
	if err != nil {
		return err
	}

	return b.redisClient.Client.XAdd(b.redisClient.Context, &rds.XAddArgs{
		Stream: b.opts.StreamName,
		MaxLen: b.opts.MaxLen,
		Approx: true,
		ID:     "*",
		Values: values,
	}).Err()
}

func encodeRedisStreamsEmitterMessage(message *adapter.ClusterMessage) (map[string]any, error) {
	values := map[string]any{
		"uid":  string(message.Uid),
		"nsp":  message.Nsp,
		"type": strconv.Itoa(int(message.Type)),
	}
	if message.Data != nil {
		mayContainBinary := message.Type == adapter.BROADCAST ||
			message.Type == adapter.FETCH_SOCKETS_RESPONSE ||
			message.Type == adapter.SERVER_SIDE_EMIT ||
			message.Type == adapter.SERVER_SIDE_EMIT_RESPONSE ||
			message.Type == adapter.BROADCAST_ACK
		if mayContainBinary && parser.HasBinary(message.Data) {
			encoded, err := utils.MsgPack().Encode(message.Data)
			if err != nil {
				return nil, fmt.Errorf("encode Redis Streams emitter MessagePack payload: %w", err)
			}
			values["data"] = base64.StdEncoding.EncodeToString(encoded)
		} else {
			encoded, err := json.Marshal(message.Data)
			if err != nil {
				return nil, fmt.Errorf("encode Redis Streams emitter JSON payload: %w", err)
			}
			values["data"] = string(encoded)
		}
	}
	return values, nil
}
