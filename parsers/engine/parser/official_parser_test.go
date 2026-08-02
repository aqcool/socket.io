package parser

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

// These tests are a behavior-by-behavior port of engine.io-parser@5.2.3 at
// gitHead 0692bed4629047a26ae8fc96e3f8636e0a4d4b57. The upstream denominator is
// 19 shared tests, 10 Node.js tests, and 9 browser tests.

func TestOfficialEngineIOParser523Shared(t *testing.T) {
	t.Run("single packet/should encode/decode a string", func(t *testing.T) {
		encoded, err := Parserv4().EncodePacket(&packet.Packet{
			Type: packet.MESSAGE,
			Data: strings.NewReader("test"),
		}, true)
		if err != nil {
			t.Fatalf("EncodePacket: %v", err)
		}
		if got := encoded.String(); got != "4test" {
			t.Fatalf("encoded packet = %q, want %q", got, "4test")
		}

		decoded, err := Parserv4().DecodePacket(encoded.Clone())
		if err != nil {
			t.Fatalf("DecodePacket: %v", err)
		}
		assertPacket(t, decoded, packet.MESSAGE, []byte("test"), false)
	})

	t.Run("single packet/should fail to decode a malformed packet", func(t *testing.T) {
		for _, malformed := range []string{"", "a123"} {
			decoded, err := Parserv4().DecodePacket(types.NewStringBufferString(malformed))
			if err == nil {
				t.Fatalf("DecodePacket(%q) error = nil", malformed)
			}
			assertErrorPacket(t, decoded)
		}
	})

	t.Run("payload/should encode/decode all packet types", func(t *testing.T) {
		packets := []*packet.Packet{
			{Type: packet.OPEN},
			{Type: packet.CLOSE},
			{Type: packet.PING, Data: strings.NewReader("probe")},
			{Type: packet.PONG, Data: strings.NewReader("probe")},
			{Type: packet.MESSAGE, Data: strings.NewReader("test")},
		}
		encoded, err := Parserv4().EncodePayload(packets)
		if err != nil {
			t.Fatalf("EncodePayload: %v", err)
		}
		const want = "0\x1e1\x1e2probe\x1e3probe\x1e4test"
		if got := encoded.String(); got != want {
			t.Fatalf("encoded payload = %q, want %q", got, want)
		}

		decoded, err := Parserv4().DecodePayload(encoded.Clone())
		if err != nil {
			t.Fatalf("DecodePayload: %v", err)
		}
		if len(decoded) != 5 {
			t.Fatalf("decoded packet count = %d, want 5", len(decoded))
		}
		assertPacket(t, decoded[0], packet.OPEN, nil, true)
		assertPacket(t, decoded[1], packet.CLOSE, nil, true)
		assertPacket(t, decoded[2], packet.PING, []byte("probe"), false)
		assertPacket(t, decoded[3], packet.PONG, []byte("probe"), false)
		assertPacket(t, decoded[4], packet.MESSAGE, []byte("test"), false)
	})

	t.Run("payload/should fail to decode a malformed payload", func(t *testing.T) {
		for _, malformed := range []string{"{", "{}", `["a123", "a456"]`} {
			decoded, err := Parserv4().DecodePayload(types.NewStringBufferString(malformed))
			if err == nil {
				t.Fatalf("DecodePayload(%q) error = nil", malformed)
			}
			if len(decoded) != 1 {
				t.Fatalf("DecodePayload(%q) packet count = %d, want 1", malformed, len(decoded))
			}
			assertErrorPacket(t, decoded[0])
		}
	})

	t.Run("createPacketEncoderStream/should encode a plaintext packet", func(t *testing.T) {
		header, payload, err := EncodePacketFrame(&packet.Packet{
			Type: packet.MESSAGE,
			Data: strings.NewReader("1€"),
		})
		if err != nil {
			t.Fatalf("EncodePacketFrame: %v", err)
		}
		assertBytes(t, header, []byte{5})
		assertBytes(t, payload, []byte{52, 49, 226, 130, 172})
	})

	t.Run("createPacketEncoderStream/should encode a binary packet (Uint8Array)", func(t *testing.T) {
		data := []byte{1, 2, 3}
		header, payload, err := EncodePacketFrame(&packet.Packet{Type: packet.MESSAGE, Data: bytes.NewReader(data)})
		if err != nil {
			t.Fatalf("EncodePacketFrame: %v", err)
		}
		assertBytes(t, header, []byte{131})
		assertBytes(t, payload, data)
	})

	t.Run("createPacketEncoderStream/should encode a binary packet (ArrayBuffer)", func(t *testing.T) {
		header, payload, err := EncodePacketFrame(&packet.Packet{
			Type: packet.MESSAGE,
			Data: types.NewBytesBuffer([]byte{1, 2, 3}),
		})
		if err != nil {
			t.Fatalf("EncodePacketFrame: %v", err)
		}
		assertBytes(t, header, []byte{131})
		assertBytes(t, payload, []byte{1, 2, 3})
	})

	t.Run("createPacketEncoderStream/should encode a binary packet (Uint16Array)", func(t *testing.T) {
		data := make([]byte, 6)
		binary.LittleEndian.PutUint16(data[0:2], 1)
		binary.LittleEndian.PutUint16(data[2:4], 2)
		binary.LittleEndian.PutUint16(data[4:6], 257)
		header, payload, err := EncodePacketFrame(&packet.Packet{Type: packet.MESSAGE, Data: bytes.NewReader(data)})
		if err != nil {
			t.Fatalf("EncodePacketFrame: %v", err)
		}
		assertBytes(t, header, []byte{134})
		assertBytes(t, payload, []byte{1, 0, 2, 0, 1, 1})
	})

	t.Run("createPacketEncoderStream/should encode a binary packet (Uint8Array - medium)", func(t *testing.T) {
		data := make([]byte, 12345)
		header, payload, err := EncodePacketFrame(&packet.Packet{Type: packet.MESSAGE, Data: bytes.NewReader(data)})
		if err != nil {
			t.Fatalf("EncodePacketFrame: %v", err)
		}
		assertBytes(t, header, []byte{254, 48, 57})
		assertBytes(t, payload, data)
	})

	t.Run("createPacketEncoderStream/should encode a binary packet (Uint8Array - big)", func(t *testing.T) {
		// Verify the exact upstream length without retaining a 123 MB fixture,
		// then exercise the same 64-bit branch end-to-end at its boundary.
		assertBytes(t, encodePacketHeader(123456789, true), []byte{255, 0, 0, 0, 0, 7, 91, 205, 21})
		data := make([]byte, 65536)
		header, payload, err := EncodePacketFrame(&packet.Packet{Type: packet.MESSAGE, Data: bytes.NewReader(data)})
		if err != nil {
			t.Fatalf("EncodePacketFrame: %v", err)
		}
		assertBytes(t, header, []byte{255, 0, 0, 0, 0, 0, 1, 0, 0})
		assertBytes(t, payload, data)
	})

	t.Run("createPacketDecoderStream/should decode a plaintext packet", func(t *testing.T) {
		decoder := NewPacketStreamDecoder(bytes.NewReader([]byte{5, 52, 49, 226, 130, 172}), 1e6)
		decoded, err := decoder.Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		assertPacket(t, decoded, packet.MESSAGE, []byte("1€"), false)
	})

	t.Run("createPacketDecoderStream/should decode a plaintext packet (bytes by bytes)", func(t *testing.T) {
		stream := []byte{5, 52, 49, 226, 130, 172, 1, 50, 1, 51}
		decoder := NewPacketStreamDecoder(&oneByteReader{data: stream}, 1e6)
		decoded, err := decoder.Decode()
		if err != nil {
			t.Fatalf("Decode message: %v", err)
		}
		assertPacket(t, decoded, packet.MESSAGE, []byte("1€"), false)
		decoded, err = decoder.Decode()
		if err != nil {
			t.Fatalf("Decode ping: %v", err)
		}
		assertPacket(t, decoded, packet.PING, nil, true)
		decoded, err = decoder.Decode()
		if err != nil {
			t.Fatalf("Decode pong: %v", err)
		}
		assertPacket(t, decoded, packet.PONG, nil, true)
	})

	t.Run("createPacketDecoderStream/should decode a plaintext packet (all bytes at once)", func(t *testing.T) {
		decoder := NewPacketStreamDecoder(bytes.NewReader([]byte{5, 52, 49, 226, 130, 172, 1, 50, 1, 51}), 1e6)
		for i, want := range []struct {
			type_ packet.Type
			data  []byte
			nil   bool
		}{{packet.MESSAGE, []byte("1€"), false}, {packet.PING, nil, true}, {packet.PONG, nil, true}} {
			decoded, err := decoder.Decode()
			if err != nil {
				t.Fatalf("Decode packet %d: %v", i, err)
			}
			assertPacket(t, decoded, want.type_, want.data, want.nil)
		}
	})

	t.Run("createPacketDecoderStream/should decode a binary packet (ArrayBuffer)", func(t *testing.T) {
		decoder := NewPacketStreamDecoder(bytes.NewReader([]byte{131, 1, 2, 3}), 1e6)
		decoded, err := decoder.Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		assertBinaryPacket(t, decoded, []byte{1, 2, 3})
	})

	t.Run("createPacketDecoderStream/should decode a binary packet (ArrayBuffer) (medium)", func(t *testing.T) {
		payload := make([]byte, 12345)
		reader := io.MultiReader(
			bytes.NewReader([]byte{254}),
			bytes.NewReader([]byte{48, 57}),
			bytes.NewReader(payload),
		)
		decoded, err := NewPacketStreamDecoder(reader, 1e6).Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		assertBinaryPacket(t, decoded, payload)
	})

	t.Run("createPacketDecoderStream/should decode a binary packet (ArrayBuffer) (big)", func(t *testing.T) {
		lengthBytes := []byte{0, 0, 0, 0, 7, 91, 205, 21}
		length, err := decodePacketLength(bytes.NewReader(lengthBytes), 127)
		if err != nil {
			t.Fatalf("decodePacketLength: %v", err)
		}
		if length != 123456789 {
			t.Fatalf("decoded length = %d, want 123456789", length)
		}

		payload := make([]byte, 65536)
		frame := append(encodePacketHeader(uint64(len(payload)), true), payload...)
		decoded, err := NewPacketStreamDecoder(bytes.NewReader(frame), 1e10).Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		assertBinaryPacket(t, decoded, payload)
	})

	t.Run("createPacketDecoderStream/should return an error packet if the length of the payload is too big", func(t *testing.T) {
		decoded, err := NewPacketStreamDecoder(bytes.NewReader([]byte{11}), 10).Decode()
		if !errors.Is(err, ErrPayloadTooLarge) {
			t.Fatalf("Decode error = %v, want ErrPayloadTooLarge", err)
		}
		assertErrorPacket(t, decoded)
	})

	t.Run("createPacketDecoderStream/should return an error packet if the length of the payload is invalid", func(t *testing.T) {
		decoded, err := NewPacketStreamDecoder(bytes.NewReader([]byte{0}), 1e6).Decode()
		if !errors.Is(err, ErrInvalidDataLength) {
			t.Fatalf("Decode error = %v, want ErrInvalidDataLength", err)
		}
		assertErrorPacket(t, decoded)
	})

	t.Run("createPacketDecoderStream/should return an error packet if the length is bigger than Number.MAX_SAFE_INTEGER", func(t *testing.T) {
		frame := []byte{255, 1, 0, 0, 0, 0, 0, 0, 0, 0}
		decoded, err := NewPacketStreamDecoder(bytes.NewReader(frame), 1e6).Decode()
		if !errors.Is(err, ErrPayloadLengthUnsafe) {
			t.Fatalf("Decode error = %v, want ErrPayloadLengthUnsafe", err)
		}
		assertErrorPacket(t, decoded)
	})
}

func TestOfficialEngineIOParser523Node(t *testing.T) {
	t.Run("single packet/should encode/decode a Buffer", func(t *testing.T) {
		encoded, err := Parserv4().EncodePacket(&packet.Packet{Type: packet.MESSAGE, Data: bytes.NewReader([]byte{1, 2, 3, 4})}, true)
		if err != nil {
			t.Fatalf("EncodePacket: %v", err)
		}
		if _, ok := encoded.(*types.BytesBuffer); !ok {
			t.Fatalf("encoded type = %T, want *types.BytesBuffer", encoded)
		}
		assertBytes(t, encoded.Bytes(), []byte{1, 2, 3, 4})
		decoded, err := Parserv4().DecodePacket(encoded.Clone())
		if err != nil {
			t.Fatalf("DecodePacket: %v", err)
		}
		assertBinaryPacket(t, decoded, []byte{1, 2, 3, 4})
	})

	t.Run("single packet/should encode/decode a Buffer as base64", func(t *testing.T) {
		assertBase64RoundTrip(t, bytes.NewReader([]byte{1, 2, 3, 4}))
	})

	t.Run("single packet/should encode/decode an ArrayBuffer", func(t *testing.T) {
		encoded, err := Parserv4().EncodePacket(&packet.Packet{Type: packet.MESSAGE, Data: types.NewBytesBuffer([]byte{1, 2, 3, 4})}, true)
		if err != nil {
			t.Fatalf("EncodePacket: %v", err)
		}
		decoded, err := Parserv4().DecodePacket(encoded.Clone())
		if err != nil {
			t.Fatalf("DecodePacket: %v", err)
		}
		assertBinaryPacket(t, decoded, []byte{1, 2, 3, 4})
	})

	t.Run("single packet/should encode/decode an ArrayBuffer as base64", func(t *testing.T) {
		assertBase64RoundTrip(t, types.NewBytesBuffer([]byte{1, 2, 3, 4}))
	})

	t.Run("single packet/should encode a typed array", func(t *testing.T) {
		data := []byte{1, 1, 2, 1, 3, 1, 4, 1}
		encoded, err := Parserv4().EncodePacket(&packet.Packet{Type: packet.MESSAGE, Data: bytes.NewReader(data)}, true)
		if err != nil {
			t.Fatalf("EncodePacket: %v", err)
		}
		assertBytes(t, encoded.Bytes(), data)
	})

	t.Run("single packet/should encode a typed array (with offset and length)", func(t *testing.T) {
		data := []byte{1, 2, 3, 4}
		encoded, err := Parserv4().EncodePacket(&packet.Packet{Type: packet.MESSAGE, Data: bytes.NewReader(data[1:3])}, true)
		if err != nil {
			t.Fatalf("EncodePacket: %v", err)
		}
		assertBytes(t, encoded.Bytes(), []byte{2, 3})
	})

	t.Run("single packet/should decode an ArrayBuffer as ArrayBuffer", func(t *testing.T) {
		decoded, err := Parserv4().DecodePacket(types.NewBytesBuffer([]byte{1, 2, 3, 4}))
		if err != nil {
			t.Fatalf("DecodePacket: %v", err)
		}
		assertBinaryPacket(t, decoded, []byte{1, 2, 3, 4})
	})

	t.Run("payload/should encode/decode a string + Buffer payload", func(t *testing.T) {
		assertMixedPayload(t, bytes.NewReader([]byte{1, 2, 3, 4}))
	})

	t.Run("createPacketEncoderStream/should encode a binary packet (Buffer)", func(t *testing.T) {
		header, payload, err := EncodePacketFrame(&packet.Packet{Type: packet.MESSAGE, Data: bytes.NewReader([]byte{1, 2, 3})})
		if err != nil {
			t.Fatalf("EncodePacketFrame: %v", err)
		}
		assertBytes(t, header, []byte{131})
		assertBytes(t, payload, []byte{1, 2, 3})
	})

	t.Run("createPacketDecoderStream/should decode a binary packet (Buffer)", func(t *testing.T) {
		decoded, err := NewPacketStreamDecoder(bytes.NewReader([]byte{131, 1, 2, 3}), 1e6).Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		assertBinaryPacket(t, decoded, []byte{1, 2, 3})
	})
}

func TestOfficialEngineIOParser523Browser(t *testing.T) {
	t.Run("single packet/should encode/decode an ArrayBuffer", func(t *testing.T) {
		encoded, err := Parserv4().EncodePacket(&packet.Packet{Type: packet.MESSAGE, Data: types.NewBytesBuffer([]byte{1, 2, 3, 4})}, true)
		if err != nil {
			t.Fatalf("EncodePacket: %v", err)
		}
		decoded, err := Parserv4().DecodePacket(encoded.Clone())
		if err != nil {
			t.Fatalf("DecodePacket: %v", err)
		}
		assertBinaryPacket(t, decoded, []byte{1, 2, 3, 4})
	})

	t.Run("single packet/should encode/decode an ArrayBuffer as base64", func(t *testing.T) {
		assertBase64RoundTrip(t, types.NewBytesBuffer([]byte{1, 2, 3, 4}))
	})

	t.Run("single packet/should encode a typed array", func(t *testing.T) {
		data := []byte{1, 2, 3, 4}
		encoded, err := Parserv4().EncodePacket(&packet.Packet{Type: packet.MESSAGE, Data: bytes.NewReader(data[1:3])}, true)
		if err != nil {
			t.Fatalf("EncodePacket: %v", err)
		}
		assertBytes(t, encoded.Bytes(), []byte{2, 3})
	})

	t.Run("single packet/should encode/decode a Blob", func(t *testing.T) {
		data := io.MultiReader(strings.NewReader("1234"), bytes.NewReader([]byte{1, 2, 3, 4}))
		encoded, err := Parserv4().EncodePacket(&packet.Packet{Type: packet.MESSAGE, Data: data}, true)
		if err != nil {
			t.Fatalf("EncodePacket: %v", err)
		}
		decoded, err := Parserv4().DecodePacket(encoded.Clone())
		if err != nil {
			t.Fatalf("DecodePacket: %v", err)
		}
		assertBinaryPacket(t, decoded, []byte{'1', '2', '3', '4', 1, 2, 3, 4})
	})

	t.Run("single packet/should encode/decode a Blob as base64", func(t *testing.T) {
		data := io.MultiReader(strings.NewReader("1234"), bytes.NewReader([]byte{1, 2, 3, 4}))
		encoded, err := Parserv4().EncodePacket(&packet.Packet{Type: packet.MESSAGE, Data: data}, false)
		if err != nil {
			t.Fatalf("EncodePacket: %v", err)
		}
		if got := encoded.String(); got != "bMTIzNAECAwQ=" {
			t.Fatalf("encoded packet = %q, want %q", got, "bMTIzNAECAwQ=")
		}
		decoded, err := Parserv4().DecodePacket(encoded.Clone())
		if err != nil {
			t.Fatalf("DecodePacket: %v", err)
		}
		assertBinaryPacket(t, decoded, []byte{'1', '2', '3', '4', 1, 2, 3, 4})
	})

	t.Run("payload/should encode/decode a string + ArrayBuffer payload", func(t *testing.T) {
		assertMixedPayload(t, types.NewBytesBuffer([]byte{1, 2, 3, 4}))
	})

	t.Run("payload/should encode/decode a string + a 0-length ArrayBuffer payload", func(t *testing.T) {
		encoded, err := Parserv4().EncodePayload([]*packet.Packet{
			{Type: packet.MESSAGE, Data: strings.NewReader("test")},
			{Type: packet.MESSAGE, Data: types.NewBytesBuffer(nil)},
		})
		if err != nil {
			t.Fatalf("EncodePayload: %v", err)
		}
		if got := encoded.String(); got != "4test\x1eb" {
			t.Fatalf("encoded payload = %q, want %q", got, "4test\x1eb")
		}
		decoded, err := Parserv4().DecodePayload(encoded.Clone())
		if err != nil {
			t.Fatalf("DecodePayload: %v", err)
		}
		if len(decoded) != 2 {
			t.Fatalf("decoded packet count = %d, want 2", len(decoded))
		}
		assertPacket(t, decoded[0], packet.MESSAGE, []byte("test"), false)
		assertBinaryPacket(t, decoded[1], []byte{})
	})

	t.Run("createPacketEncoderStream/should encode a binary packet (Blob)", func(t *testing.T) {
		data := io.MultiReader(bytes.NewReader([]byte{1}), bytes.NewReader([]byte{2, 3}))
		header, payload, err := EncodePacketFrame(&packet.Packet{Type: packet.MESSAGE, Data: data})
		if err != nil {
			t.Fatalf("EncodePacketFrame: %v", err)
		}
		assertBytes(t, header, []byte{131})
		assertBytes(t, payload, []byte{1, 2, 3})
	})

	t.Run("createPacketDecoderStream/should decode a binary packet (Blob)", func(t *testing.T) {
		decoded, err := NewPacketStreamDecoder(bytes.NewReader([]byte{131, 1, 2, 3}), 1e6).Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		// Blob/ArrayBuffer/Buffer are represented by the same BytesBuffer in Go.
		assertBinaryPacket(t, decoded, []byte{1, 2, 3})
	})
}

func TestOfficialV3ParserEncodesMixedPayload(t *testing.T) {
	encoded, err := Parserv3().EncodePayload([]*packet.Packet{
		{Type: packet.MESSAGE, Data: types.NewStringBufferString("€€€€")},
		{Type: packet.MESSAGE, Data: types.NewBytesBuffer([]byte{1, 2, 3})},
	}, true)
	if err != nil {
		t.Fatalf("encoding mixed payload: %v", err)
	}
	want := []byte{
		0, 1, 3, 255, '4', 226, 130, 172, 226, 130, 172, 226, 130, 172, 226, 130, 172,
		1, 4, 255, 4, 1, 2, 3,
	}
	assertBytes(t, encoded.Bytes(), want)

	decoded, err := Parserv3().DecodePayload(encoded.Clone())
	if err != nil {
		t.Fatalf("decoding mixed payload: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded packet count = %d, want 2", len(decoded))
	}
	assertPacket(t, decoded[0], packet.MESSAGE, []byte("€€€€"), false)
	assertBinaryPacket(t, decoded[1], []byte{1, 2, 3})
}

func assertBase64RoundTrip(t *testing.T, data io.Reader) {
	t.Helper()
	encoded, err := Parserv4().EncodePacket(&packet.Packet{Type: packet.MESSAGE, Data: data}, false)
	if err != nil {
		t.Fatalf("EncodePacket: %v", err)
	}
	if got := encoded.String(); got != "bAQIDBA==" {
		t.Fatalf("encoded packet = %q, want %q", got, "bAQIDBA==")
	}
	decoded, err := Parserv4().DecodePacket(encoded.Clone())
	if err != nil {
		t.Fatalf("DecodePacket: %v", err)
	}
	assertBinaryPacket(t, decoded, []byte{1, 2, 3, 4})
}

func assertMixedPayload(t *testing.T, binaryData io.Reader) {
	t.Helper()
	encoded, err := Parserv4().EncodePayload([]*packet.Packet{
		{Type: packet.MESSAGE, Data: strings.NewReader("test")},
		{Type: packet.MESSAGE, Data: binaryData},
	})
	if err != nil {
		t.Fatalf("EncodePayload: %v", err)
	}
	if got := encoded.String(); got != "4test\x1ebAQIDBA==" {
		t.Fatalf("encoded payload = %q, want %q", got, "4test\x1ebAQIDBA==")
	}
	decoded, err := Parserv4().DecodePayload(encoded.Clone())
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded packet count = %d, want 2", len(decoded))
	}
	assertPacket(t, decoded[0], packet.MESSAGE, []byte("test"), false)
	assertBinaryPacket(t, decoded[1], []byte{1, 2, 3, 4})
}

func assertBinaryPacket(t *testing.T, pkt *packet.Packet, want []byte) {
	t.Helper()
	if pkt == nil {
		t.Fatal("packet = nil")
	}
	if pkt.Type != packet.MESSAGE {
		t.Fatalf("packet type = %q, want %q", pkt.Type, packet.MESSAGE)
	}
	if _, ok := pkt.Data.(*types.BytesBuffer); !ok {
		t.Fatalf("packet data type = %T, want *types.BytesBuffer", pkt.Data)
	}
	assertPacket(t, pkt, packet.MESSAGE, want, false)
}

func assertErrorPacket(t *testing.T, pkt *packet.Packet) {
	t.Helper()
	assertPacket(t, pkt, packet.ERROR, []byte("parser error"), false)
}

func assertPacket(t *testing.T, pkt *packet.Packet, wantType packet.Type, wantData []byte, wantNil bool) {
	t.Helper()
	if pkt == nil {
		t.Fatal("packet = nil")
	}
	if pkt.Type != wantType {
		t.Fatalf("packet type = %q, want %q", pkt.Type, wantType)
	}
	if wantNil {
		if pkt.Data != nil {
			t.Fatalf("packet data = %T, want nil", pkt.Data)
		}
		return
	}
	if pkt.Data == nil {
		t.Fatal("packet data = nil")
	}
	got, err := io.ReadAll(pkt.Data)
	if err != nil {
		t.Fatalf("read packet data: %v", err)
	}
	assertBytes(t, got, wantData)
}

func assertBytes(t *testing.T, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes = %v, want %v", got, want)
	}
}

type oneByteReader struct {
	data []byte
	off  int
}

func (r *oneByteReader) Read(dst []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	if len(dst) == 0 {
		return 0, nil
	}
	dst[0] = r.data[r.off]
	r.off++
	return 1, nil
}
