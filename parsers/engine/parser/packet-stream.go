package parser

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

// maxSafePayloadLength mirrors Number.MAX_SAFE_INTEGER, which is the largest
// payload length accepted by engine.io-parser's WebTransport decoder.
const maxSafePayloadLength uint64 = 1<<53 - 1

// PacketStreamEncoder writes Engine.IO v4 packets using the length-prefixed
// framing used on a WebTransport bidirectional stream.
type PacketStreamEncoder struct {
	writer io.Writer
}

// NewPacketStreamEncoder creates a WebTransport packet-stream encoder.
func NewPacketStreamEncoder(writer io.Writer) *PacketStreamEncoder {
	return &PacketStreamEncoder{writer: writer}
}

// Encode writes one framed packet. Multiple calls produce a payload stream
// that PacketStreamDecoder can consume one packet at a time.
func (e *PacketStreamEncoder) Encode(pkt *packet.Packet) error {
	if e == nil || e.writer == nil {
		return ErrWriterNil
	}

	header, payload, err := EncodePacketFrame(pkt)
	if err != nil {
		return err
	}
	if err := writeAll(e.writer, header); err != nil {
		return err
	}
	return writeAll(e.writer, payload)
}

// EncodePacketToBinary returns the Engine.IO v4 wire representation as bytes.
// Text packets include their packet-type byte; binary packets are returned
// unchanged, matching engine.io-parser's encodePacketToBinary helper.
func EncodePacketToBinary(pkt *packet.Packet) ([]byte, error) {
	encoded, err := Parserv4().EncodePacket(pkt, true)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), encoded.Bytes()...), nil
}

// EncodePacketFrame returns the two chunks emitted by the official
// createPacketEncoderStream implementation: a length header and the packet
// payload. The high bit of the first header byte marks binary data.
func EncodePacketFrame(pkt *packet.Packet) (header, payload []byte, err error) {
	if pkt == nil {
		return nil, nil, ErrPacketNil
	}

	isBinary := isBinaryPacketData(pkt.Data)
	payload, err = EncodePacketToBinary(pkt)
	if err != nil {
		return nil, nil, err
	}
	header = encodePacketHeader(uint64(len(payload)), isBinary)
	return header, payload, nil
}

func isBinaryPacketData(data io.Reader) bool {
	switch data.(type) {
	case nil, *types.StringBuffer, *strings.Reader:
		return false
	default:
		return true
	}
}

func encodePacketHeader(payloadLength uint64, isBinary bool) []byte {
	var header []byte
	switch {
	case payloadLength < 126:
		header = []byte{byte(payloadLength)}
	case payloadLength < 1<<16:
		header = make([]byte, 3)
		header[0] = 126
		binary.BigEndian.PutUint16(header[1:], uint16(payloadLength))
	default:
		header = make([]byte, 9)
		header[0] = 127
		binary.BigEndian.PutUint64(header[1:], payloadLength)
	}
	if isBinary {
		header[0] |= 0x80
	}
	return header
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

// PacketStreamDecoder reads the length-prefixed Engine.IO v4 packet stream
// used by WebTransport. It accepts any io.Reader, including readers that
// return a frame one byte at a time.
type PacketStreamDecoder struct {
	reader     io.Reader
	maxPayload uint64
}

// NewPacketStreamDecoder creates a WebTransport packet-stream decoder.
// maxPayload is checked before allocating or reading the declared payload.
func NewPacketStreamDecoder(reader io.Reader, maxPayload uint64) *PacketStreamDecoder {
	return &PacketStreamDecoder{reader: reader, maxPayload: maxPayload}
}

// Decode reads and decodes one framed packet. io.EOF with a nil packet means
// the stream ended cleanly. Protocol failures return the standard Engine.IO
// error packet together with a descriptive Go error.
func (d *PacketStreamDecoder) Decode() (*packet.Packet, error) {
	if d == nil || d.reader == nil {
		return newErrorPacket(), ErrReaderNil
	}

	var first [1]byte
	if _, err := io.ReadFull(d.reader, first[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return newErrorPacket(), err
	}

	isBinary := first[0]&0x80 != 0
	payloadLength, err := decodePacketLength(d.reader, first[0]&0x7f)
	if err != nil {
		return newErrorPacket(), err
	}
	if payloadLength == 0 {
		return newErrorPacket(), ErrInvalidDataLength
	}
	if payloadLength > d.maxPayload {
		return newErrorPacket(), fmt.Errorf("%w: %d exceeds %d", ErrPayloadTooLarge, payloadLength, d.maxPayload)
	}
	if payloadLength > uint64(maxIntValue) {
		return newErrorPacket(), fmt.Errorf("%w: %d", ErrPayloadLengthUnsafe, payloadLength)
	}

	payload := make([]byte, int(payloadLength))
	if _, err := io.ReadFull(d.reader, payload); err != nil {
		return newErrorPacket(), err
	}
	if isBinary {
		return &packet.Packet{Type: packet.MESSAGE, Data: types.NewBytesBuffer(payload)}, nil
	}

	return Parserv4().DecodePacket(types.NewStringBuffer(payload))
}

const maxIntValue = int(^uint(0) >> 1)

func decodePacketLength(reader io.Reader, lengthCode byte) (uint64, error) {
	switch lengthCode {
	case 126:
		var extended [2]byte
		if _, err := io.ReadFull(reader, extended[:]); err != nil {
			return 0, err
		}
		return uint64(binary.BigEndian.Uint16(extended[:])), nil
	case 127:
		var extended [8]byte
		if _, err := io.ReadFull(reader, extended[:]); err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint64(extended[:])
		if length > maxSafePayloadLength {
			return 0, fmt.Errorf("%w: %d", ErrPayloadLengthUnsafe, length)
		}
		return length, nil
	default:
		return uint64(lengthCode), nil
	}
}
