package socketio

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	socketparser "github.com/aqcool/socket.io/parsers/socket/v3/parser"
)

type ValueCodec interface {
	Decode(src any, dst any) error
}

type JSONValueCodec struct{}

func (JSONValueCodec) Decode(src any, dst any) error {
	payload, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, dst)
}

const (
	PacketConnect uint8 = iota
	PacketDisconnect
	PacketEvent
	PacketAck
	PacketConnectError
	PacketBinaryEvent
	PacketBinaryAck
)

type Packet struct {
	Type      uint8
	Namespace string
	ID        *uint64
	Data      any
}

type PacketEncoder interface {
	Encode(Packet) ([]io.Reader, error)
}

type PacketDecoder interface {
	Add(any) ([]Packet, error)
	Close() error
}

type PacketCodec interface {
	NewEncoder() PacketEncoder
	NewDecoder() PacketDecoder
}

type DefaultPacketCodec struct{}

func (DefaultPacketCodec) NewEncoder() PacketEncoder { return &defaultPacketEncoder{raw: socketparser.NewEncoder()} }
func (DefaultPacketCodec) NewDecoder() PacketDecoder { return newDefaultPacketDecoder() }

type defaultPacketEncoder struct{ raw socketparser.Encoder }

func (e *defaultPacketEncoder) Encode(packet Packet) (readers []io.Reader, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			readers = nil
			err = fmt.Errorf("socket.io: encode packet: %v", recovered)
		}
	}()
	raw, err := toParserPacket(packet)
	if err != nil {
		return nil, err
	}
	encoded := e.raw.Encode(raw)
	readers = make([]io.Reader, 0, len(encoded))
	for _, item := range encoded {
		readers = append(readers, item)
	}
	return readers, nil
}

type defaultPacketDecoder struct {
	raw socketparser.Decoder
	mu sync.Mutex
	decoded []Packet
}

func newDefaultPacketDecoder() *defaultPacketDecoder {
	d := &defaultPacketDecoder{raw: socketparser.NewDecoder()}
	_ = d.raw.On("decoded", func(args ...any) {
		if len(args) == 0 {
			return
		}
		if raw, ok := args[0].(*socketparser.Packet); ok && raw != nil {
			d.mu.Lock()
			d.decoded = append(d.decoded, fromParserPacket(raw))
			d.mu.Unlock()
		}
	})
	return d
}

func (d *defaultPacketDecoder) Add(data any) ([]Packet, error) {
	if err := d.raw.Add(data); err != nil {
		return nil, err
	}
	d.mu.Lock()
	result := append([]Packet(nil), d.decoded...)
	d.decoded = d.decoded[:0]
	d.mu.Unlock()
	return result, nil
}

func (d *defaultPacketDecoder) Close() error {
	d.raw.Destroy()
	return nil
}

func toParserPacket(packet Packet) (*socketparser.Packet, error) {
	raw := &socketparser.Packet{Nsp: packet.Namespace, Id: packet.ID, Data: packet.Data}
	switch packet.Type {
	case PacketConnect:
		raw.Type = socketparser.CONNECT
	case PacketDisconnect:
		raw.Type = socketparser.DISCONNECT
	case PacketEvent:
		raw.Type = socketparser.EVENT
	case PacketAck:
		raw.Type = socketparser.ACK
	case PacketConnectError:
		raw.Type = socketparser.CONNECT_ERROR
	case PacketBinaryEvent:
		raw.Type = socketparser.BINARY_EVENT
	case PacketBinaryAck:
		raw.Type = socketparser.BINARY_ACK
	default:
		return nil, fmt.Errorf("%w: packet type %d", ErrInvalidArgument, packet.Type)
	}
	return raw, nil
}

func fromParserPacket(packet *socketparser.Packet) Packet {
	result := Packet{Namespace: packet.Nsp, ID: packet.Id, Data: packet.Data}
	switch packet.Type {
	case socketparser.CONNECT:
		result.Type = PacketConnect
	case socketparser.DISCONNECT:
		result.Type = PacketDisconnect
	case socketparser.EVENT:
		result.Type = PacketEvent
	case socketparser.ACK:
		result.Type = PacketAck
	case socketparser.CONNECT_ERROR:
		result.Type = PacketConnectError
	case socketparser.BINARY_EVENT:
		result.Type = PacketBinaryEvent
	case socketparser.BINARY_ACK:
		result.Type = PacketBinaryAck
	}
	return result
}
