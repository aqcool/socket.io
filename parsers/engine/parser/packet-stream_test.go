package parser

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
)

func TestPacketStreamRoundTrip(t *testing.T) {
	var wire bytes.Buffer
	encoder := NewPacketStreamEncoder(&wire)
	for _, pkt := range []*packet.Packet{
		{Type: packet.PING},
		{Type: packet.MESSAGE, Data: strings.NewReader("hello €")},
		{Type: packet.MESSAGE, Data: bytes.NewReader([]byte{0, 1, 2, 255})},
	} {
		if err := encoder.Encode(pkt); err != nil {
			t.Fatalf("Encode(%q): %v", pkt.Type, err)
		}
	}

	decoder := NewPacketStreamDecoder(&wire, 1024)
	decoded, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Decode ping: %v", err)
	}
	assertPacket(t, decoded, packet.PING, nil, true)

	decoded, err = decoder.Decode()
	if err != nil {
		t.Fatalf("Decode text: %v", err)
	}
	assertPacket(t, decoded, packet.MESSAGE, []byte("hello €"), false)

	decoded, err = decoder.Decode()
	if err != nil {
		t.Fatalf("Decode binary: %v", err)
	}
	assertBinaryPacket(t, decoded, []byte{0, 1, 2, 255})

	decoded, err = decoder.Decode()
	if !errors.Is(err, io.EOF) || decoded != nil {
		t.Fatalf("Decode end = (%v, %v), want (nil, io.EOF)", decoded, err)
	}
}

func TestPacketStreamHeaderBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		length   uint64
		binary   bool
		expected []byte
	}{
		{"text-125", 125, false, []byte{125}},
		{"text-126", 126, false, []byte{126, 0, 126}},
		{"binary-125", 125, true, []byte{253}},
		{"binary-126", 126, true, []byte{254, 0, 126}},
		{"binary-65535", 65535, true, []byte{254, 255, 255}},
		{"binary-65536", 65536, true, []byte{255, 0, 0, 0, 0, 0, 1, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertBytes(t, encodePacketHeader(tt.length, tt.binary), tt.expected)
		})
	}
}

func TestPacketStreamErrors(t *testing.T) {
	t.Run("nil writer", func(t *testing.T) {
		err := NewPacketStreamEncoder(nil).Encode(&packet.Packet{Type: packet.PING})
		if !errors.Is(err, ErrWriterNil) {
			t.Fatalf("Encode error = %v, want ErrWriterNil", err)
		}
	})

	t.Run("nil packet", func(t *testing.T) {
		var wire bytes.Buffer
		err := NewPacketStreamEncoder(&wire).Encode(nil)
		if !errors.Is(err, ErrPacketNil) {
			t.Fatalf("Encode error = %v, want ErrPacketNil", err)
		}
	})

	t.Run("short writer", func(t *testing.T) {
		err := NewPacketStreamEncoder(zeroWriter{}).Encode(&packet.Packet{Type: packet.PING})
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("Encode error = %v, want io.ErrShortWrite", err)
		}
	})

	t.Run("nil reader", func(t *testing.T) {
		decoded, err := NewPacketStreamDecoder(nil, 1).Decode()
		if !errors.Is(err, ErrReaderNil) {
			t.Fatalf("Decode error = %v, want ErrReaderNil", err)
		}
		assertErrorPacket(t, decoded)
	})

	t.Run("truncated extended header", func(t *testing.T) {
		decoded, err := NewPacketStreamDecoder(bytes.NewReader([]byte{126, 1}), 1024).Decode()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("Decode error = %v, want io.ErrUnexpectedEOF", err)
		}
		assertErrorPacket(t, decoded)
	})

	t.Run("truncated payload", func(t *testing.T) {
		decoded, err := NewPacketStreamDecoder(bytes.NewReader([]byte{3, '4', 'a'}), 1024).Decode()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("Decode error = %v, want io.ErrUnexpectedEOF", err)
		}
		assertErrorPacket(t, decoded)
	})

	t.Run("max payload is inclusive", func(t *testing.T) {
		decoded, err := NewPacketStreamDecoder(bytes.NewReader([]byte{2, '4', 'a'}), 2).Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		assertPacket(t, decoded, packet.MESSAGE, []byte("a"), false)
	})

	t.Run("malformed text packet", func(t *testing.T) {
		decoded, err := NewPacketStreamDecoder(bytes.NewReader([]byte{1, 'x'}), 1024).Decode()
		if !errors.Is(err, ErrUnknownPacketType) {
			t.Fatalf("Decode error = %v, want ErrUnknownPacketType", err)
		}
		assertErrorPacket(t, decoded)
	})
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }
