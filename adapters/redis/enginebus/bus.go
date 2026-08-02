// Package enginebus provides the Redis Pub/Sub transport for the distributed
// Engine.IO session protocol implemented by servers/engine ClusterServer.
package enginebus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	engine "github.com/aqcool/socket.io/servers/engine/v3"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

const DefaultChannelPrefix = "engine.io"

const officialMessageSource = "_eio"

var ErrRedisClientRequired = errors.New("enginebus: Redis publish and subscribe clients are required")

type Options struct {
	ChannelPrefix string
}

// Bus maps cluster broadcasts to <prefix># and directed messages to
// <prefix>#<nodeID># exactly like @socket.io/cluster-engine@0.1.0.
type Bus struct {
	pubClient redis.UniversalClient
	subClient redis.UniversalClient
	prefix    string
}

func New(pubClient, subClient redis.UniversalClient, options *Options) (*Bus, error) {
	if pubClient == nil || subClient == nil {
		return nil, ErrRedisClientRequired
	}
	prefix := DefaultChannelPrefix
	if options != nil && options.ChannelPrefix != "" {
		prefix = options.ChannelPrefix
	}
	return &Bus{pubClient: pubClient, subClient: subClient, prefix: prefix}, nil
}

func channelName(prefix, nodeID string) string {
	if nodeID == "" {
		return prefix + "#"
	}
	return prefix + "#" + nodeID + "#"
}

func (b *Bus) Subscribe(nodeID string, listener func(*engine.ClusterMessage)) (func(), error) {
	if listener == nil {
		return nil, errors.New("enginebus: listener is nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	pubsub := b.subClient.Subscribe(ctx, channelName(b.prefix, ""), channelName(b.prefix, nodeID))
	if _, err := pubsub.Receive(ctx); err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, fmt.Errorf("enginebus: subscribe: %w", err)
	}

	messages := pubsub.Channel(redis.WithChannelSize(256))
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case raw, open := <-messages:
				if !open {
					return
				}
				var wire officialWireMessage
				if err := msgpack.Unmarshal([]byte(raw.Payload), &wire); err != nil {
					continue
				}
				if wire.Source != officialMessageSource {
					continue
				}
				message := decodeOfficialMessage(&wire)
				listener(&message)
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			_ = pubsub.Close()
		})
	}, nil
}

func (b *Bus) Publish(ctx context.Context, message *engine.ClusterMessage) error {
	if message == nil {
		return errors.New("enginebus: message is nil")
	}
	payload, err := msgpack.Marshal(encodeOfficialMessage(message))
	if err != nil {
		return fmt.Errorf("enginebus: encode message: %w", err)
	}
	channel := channelName(b.prefix, message.RecipientID)
	if err := b.pubClient.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("enginebus: publish: %w", err)
	}
	return nil
}

var _ engine.ClusterBus = (*Bus)(nil)

// officialWireMessage mirrors the exact @socket.io/cluster-engine@0.1.0
// MessagePack envelope. Session fields belong under data, and _source keeps
// unrelated Redis traffic from being interpreted as Engine.IO cluster data.
type officialWireMessage struct {
	Source      string                    `msgpack:"_source"`
	RequestID   uint64                    `msgpack:"requestId,omitempty"`
	SenderID    string                    `msgpack:"senderId"`
	RecipientID string                    `msgpack:"recipientId,omitempty"`
	Type        engine.ClusterMessageType `msgpack:"type"`
	Data        officialWireData          `msgpack:"data"`
}

type officialWireData struct {
	SID           string                 `msgpack:"sid,omitempty"`
	TransportName string                 `msgpack:"transportName,omitempty"`
	LockType      engine.ClusterLockType `msgpack:"type,omitempty"`
	Success       *bool                  `msgpack:"success,omitempty"`
	TakeOver      *bool                  `msgpack:"takeOver,omitempty"`
	Reason        *string                `msgpack:"reason,omitempty"`
	Packet        *officialWirePacket    `msgpack:"packet,omitempty"`
	Packets       *[]officialWirePacket  `msgpack:"packets,omitempty"`
}

type officialWirePacket struct {
	Type    string                     `msgpack:"type"`
	Data    *any                       `msgpack:"data,omitempty"`
	Options *officialWirePacketOptions `msgpack:"options,omitempty"`
}

type officialWirePacketOptions struct {
	Compress bool `msgpack:"compress"`
}

func encodeOfficialMessage(message *engine.ClusterMessage) officialWireMessage {
	wire := officialWireMessage{
		Source:      officialMessageSource,
		RequestID:   message.RequestID,
		SenderID:    message.SenderID,
		RecipientID: message.RecipientID,
		Type:        message.Type,
		Data: officialWireData{
			SID:           message.SID,
			TransportName: message.Transport,
			LockType:      message.LockType,
		},
	}
	switch message.Type {
	case engine.ClusterMessageAcquireLockResponse, engine.ClusterMessageUpgrade:
		success := message.Success
		wire.Data.Success = &success
	case engine.ClusterMessageUpgradeResponse:
		takeOver := message.TakeOver
		wire.Data.TakeOver = &takeOver
	case engine.ClusterMessageClose:
		reason := message.Reason
		wire.Data.Reason = &reason
	}
	if message.Packet != nil {
		packet := encodeOfficialPacket(*message.Packet)
		wire.Data.Packet = &packet
	}
	if message.Type == engine.ClusterMessageDrain || message.Type == engine.ClusterMessageUpgradeResponse {
		packets := make([]officialWirePacket, 0, len(message.Packets))
		for _, packet := range message.Packets {
			packets = append(packets, encodeOfficialPacket(packet))
		}
		wire.Data.Packets = &packets
	}
	return wire
}

func encodeOfficialPacket(packet engine.ClusterPacket) officialWirePacket {
	wire := officialWirePacket{Type: packet.Type}
	if packet.HasCompress {
		wire.Options = &officialWirePacketOptions{Compress: packet.Compress}
	}
	if !packet.HasData {
		return wire
	}
	if packet.Binary {
		data := make([]byte, len(packet.Data))
		copy(data, packet.Data)
		value := any(data)
		wire.Data = &value
	} else {
		value := any(string(packet.Data))
		wire.Data = &value
	}
	return wire
}

func decodeOfficialMessage(wire *officialWireMessage) engine.ClusterMessage {
	message := engine.ClusterMessage{
		Source:      wire.Source,
		RequestID:   wire.RequestID,
		SenderID:    wire.SenderID,
		RecipientID: wire.RecipientID,
		Type:        wire.Type,
		SID:         wire.Data.SID,
		Transport:   wire.Data.TransportName,
		LockType:    wire.Data.LockType,
	}
	if wire.Data.Success != nil {
		message.Success = *wire.Data.Success
	}
	if wire.Data.TakeOver != nil {
		message.TakeOver = *wire.Data.TakeOver
	}
	if wire.Data.Reason != nil {
		message.Reason = *wire.Data.Reason
	}
	if wire.Data.Packet != nil {
		packet := decodeOfficialPacket(*wire.Data.Packet)
		message.Packet = &packet
	}
	if wire.Data.Packets != nil {
		message.Packets = make([]engine.ClusterPacket, 0, len(*wire.Data.Packets))
		for _, packet := range *wire.Data.Packets {
			message.Packets = append(message.Packets, decodeOfficialPacket(packet))
		}
	}
	return message
}

func decodeOfficialPacket(packet officialWirePacket) engine.ClusterPacket {
	result := engine.ClusterPacket{Type: packet.Type}
	if packet.Options != nil {
		result.HasCompress = true
		result.Compress = packet.Options.Compress
	}
	if packet.Data == nil {
		return result
	}
	switch data := (*packet.Data).(type) {
	case nil:
	case string:
		result.HasData = true
		result.Data = []byte(data)
	case []byte:
		result.HasData = true
		result.Binary = true
		result.Data = append([]byte(nil), data...)
	default:
		// @msgpack/msgpack emits only string or bin for Engine.IO packet data.
		// Unknown shapes are treated as absent instead of widening the protocol.
	}
	return result
}
