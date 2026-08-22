package socketio

import (
	"fmt"
	"io"

	coreparser "github.com/aqcool/socket.io/parsers/socket/v4/parser"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

type nativePacketParser struct {
	codec PacketCodec
}

func newNativePacketParser(codec PacketCodec) coreparser.Parser {
	return &nativePacketParser{codec: codec}
}

func (p *nativePacketParser) NewEncoder() coreparser.Encoder {
	return &nativePacketEncoder{encoder: p.codec.NewEncoder()}
}

func (p *nativePacketParser) NewDecoder() coreparser.Decoder {
	return &nativePacketDecoder{
		EventEmitter: types.NewEventEmitter(),
		decoder:      p.codec.NewDecoder(),
	}
}

type nativePacketEncoder struct {
	encoder PacketEncoder
}

func (e *nativePacketEncoder) Encode(packet *coreparser.Packet) []types.BufferInterface {
	if e == nil || e.encoder == nil {
		panic("socketio: packet encoder is nil")
	}
	buffers, err := e.encoder.Encode(fromCorePacket(packet))
	if err != nil {
		panic(fmt.Errorf("socketio: encode packet: %w", err))
	}
	result := make([]types.BufferInterface, 0, len(buffers))
	for index, buffer := range buffers {
		data := append([]byte(nil), buffer...)
		if index == 0 {
			result = append(result, types.NewStringBuffer(data))
		} else {
			result = append(result, types.NewBytesBuffer(data))
		}
	}
	return result
}

type nativePacketDecoder struct {
	types.EventEmitter
	decoder PacketDecoder
}

func (d *nativePacketDecoder) Add(data any) error {
	if d == nil || d.decoder == nil {
		return fmt.Errorf("%w: packet decoder is nil", ErrInvalidArgument)
	}
	payload, binary, err := packetCodecInput(data)
	if err != nil {
		return err
	}
	packet, err := d.decoder.Add(payload, binary)
	if err != nil {
		return err
	}
	if packet != nil {
		d.EventEmitter.Emit(types.EventName("decoded"), toCorePacket(*packet))
	}
	return nil
}

func (d *nativePacketDecoder) Destroy() {
	if d != nil && d.decoder != nil {
		_ = d.decoder.Close()
	}
}

func packetCodecInput(data any) ([]byte, bool, error) {
	binary := coreparser.IsBinary(data)
	switch value := data.(type) {
	case string:
		return []byte(value), false, nil
	case []byte:
		return append([]byte(nil), value...), true, nil
	case interface{ Bytes() []byte }:
		return append([]byte(nil), value.Bytes()...), binary, nil
	case fmt.Stringer:
		return []byte(value.String()), binary, nil
	case io.Reader:
		payload, err := io.ReadAll(value)
		return payload, binary, err
	default:
		return nil, false, fmt.Errorf("socketio: unsupported packet codec input %T", data)
	}
}

func fromCorePacket(packet *coreparser.Packet) Packet {
	if packet == nil {
		return Packet{}
	}
	return Packet{
		Type:        uint8(packet.Type),
		Namespace:   packet.Nsp,
		ID:          packet.Id,
		Data:        packet.Data,
		Attachments: packet.Attachments,
	}
}

func toCorePacket(packet Packet) *coreparser.Packet {
	return &coreparser.Packet{
		Type:        coreparser.PacketType(packet.Type),
		Nsp:         packet.Namespace,
		Id:          packet.ID,
		Data:        packet.Data,
		Attachments: packet.Attachments,
	}
}
