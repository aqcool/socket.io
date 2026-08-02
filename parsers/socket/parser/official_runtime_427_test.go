package parser

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/aqcool/socket.io/v3/pkg/types"
)

// TestOfficialRuntimeSuite427 mirrors the 34 runtime test cases declared by
// socket.io-parser@4.2.7 in parser.js, buffer.js, arraybuffer.js and blob.js.
// Browser-native binary containers are represented by their Go transport-level
// equivalents: []byte, map[string]any and io.Reader.
func TestOfficialRuntimeSuite427(t *testing.T) {
	t.Run("parser.js/exposes types", func(t *testing.T) {
		packetTypes := []PacketType{
			CONNECT,
			DISCONNECT,
			EVENT,
			ACK,
			CONNECT_ERROR,
			BINARY_EVENT,
			BINARY_ACK,
		}
		for index, packetType := range packetTypes {
			if int(packetType) != index || !packetType.Valid() {
				t.Fatalf("packet type at index %d = %d (valid=%t)", index, packetType, packetType.Valid())
			}
		}
	})

	t.Run("parser.js/encodes connection", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: CONNECT,
			Nsp:  "/woot",
			Data: map[string]any{"token": "123"},
		})
	})

	t.Run("parser.js/encodes disconnection", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{Type: DISCONNECT, Nsp: "/woot"})
	})

	t.Run("parser.js/encodes an event", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/",
			Data: []any{"a", 1, map[string]any{}},
		})
	})

	t.Run("parser.js/encodes an event (with an integer as event name)", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/",
			Data: []any{1, "a", map[string]any{}},
		})
	})

	t.Run("parser.js/encodes an event (with ack)", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/test",
			Id:   officialUint64(1),
			Data: []any{"a", 1, map[string]any{}},
		})
	})

	t.Run("parser.js/encodes an ack", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: ACK,
			Nsp:  "/",
			Id:   officialUint64(123),
			Data: []any{"a", 1, map[string]any{}},
		})
	})

	t.Run("parser.js/encodes an connect error", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: CONNECT_ERROR,
			Nsp:  "/",
			Data: "Unauthorized",
		})
	})

	t.Run("parser.js/encodes an connect error (with object)", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: CONNECT_ERROR,
			Nsp:  "/",
			Data: map[string]any{"message": "Unauthorized"},
		})
	})

	t.Run("parser.js/throws an error when encoding circular objects", func(t *testing.T) {
		cyclic := map[string]any{}
		cyclic["b"] = cyclic

		defer func() {
			recovered := recover()
			if !errors.Is(officialRecoveredError(recovered), ErrCircularReference) {
				t.Fatalf("Encode() panic = %v, want %v", recovered, ErrCircularReference)
			}
		}()
		NewEncoder().Encode(&Packet{Type: EVENT, Nsp: "/", Id: officialUint64(1), Data: cyclic})
	})

	t.Run("parser.js/decodes a bad binary packet", func(t *testing.T) {
		if err := NewDecoder().Add("5"); !errors.Is(err, ErrInvalidAttachmentCount) {
			t.Fatalf("Add() error = %v, want %v", err, ErrInvalidAttachmentCount)
		}
	})

	t.Run("parser.js/throws an error when receiving too many attachments", func(t *testing.T) {
		opts := DefaultDecoderOptions()
		opts.SetMaxAttachments(2)
		err := NewDecoder(opts).Add(
			`53-["hello",{"_placeholder":true,"num":0},{"_placeholder":true,"num":1},{"_placeholder":true,"num":2}]`,
		)
		if !errors.Is(err, ErrTooManyAttachments) {
			t.Fatalf("Add() error = %v, want %v", err, ErrTooManyAttachments)
		}
	})

	t.Run("parser.js/decodes with a custom reviver", func(t *testing.T) {
		officialAssertReviver(t, NewDecoder(func(key string, value any) any {
			if key == "a" {
				return strings.ToUpper(value.(string))
			}
			return value
		}))
	})

	t.Run("parser.js/decodes with a custom reviver (options object)", func(t *testing.T) {
		opts := DefaultDecoderOptions()
		opts.SetReviver(func(key string, value any) any {
			if key == "a" {
				return strings.ToUpper(value.(string))
			}
			return value
		})
		officialAssertReviver(t, NewDecoder(opts))
	})

	t.Run("parser.js/throw an error upon parsing error", func(t *testing.T) {
		invalidPayloads := []string{
			`442["some","data"`,
			`0/admin,"invalid"`,
			`0[]`,
			`1/admin,{}`,
			`2/admin,"invalid`,
			`2/admin,{}`,
			`2[{"toString":"foo"}]`,
			`2[true,"foo"]`,
			`2[null,"bar"]`,
			`2["connect"]`,
			`2["disconnect","123"]`,
		}
		for _, input := range invalidPayloads {
			if err := NewDecoder().Add(input); !errors.Is(err, ErrInvalidPayload) {
				t.Errorf("Add(%q) error = %v, want %v", input, err, ErrInvalidPayload)
			}
		}

		for _, input := range []string{"5", "51", "50-", "5a-", "51.23-"} {
			if err := NewDecoder().Add(input); !errors.Is(err, ErrInvalidAttachmentCount) {
				t.Errorf("Add(%q) error = %v, want %v", input, err, ErrInvalidAttachmentCount)
			}
		}

		if err := NewDecoder().Add("999"); err == nil || err.Error() != "unknown packet type 9" {
			t.Errorf("Add(%q) error = %v, want %q", "999", err, "unknown packet type 9")
		}
		if err := NewDecoder().Add(999); err == nil || err.Error() != "Unknown type: 999" {
			t.Errorf("Add(%d) error = %v, want %q", 999, err, "Unknown type: 999")
		}
	})

	t.Run("parser.js/should resume decoding after calling destroy()", func(t *testing.T) {
		decoder := NewDecoder()
		var decoded *Packet
		if err := decoder.On("decoded", func(args ...any) {
			decoded, _ = args[0].(*Packet)
		}); err != nil {
			t.Fatalf("On() error = %v", err)
		}

		if err := decoder.Add(`51-["hello"]`); err != nil {
			t.Fatalf("binary header Add() error = %v", err)
		}
		decoder.Destroy()
		if err := decoder.Add(`2["hello"]`); err != nil {
			t.Fatalf("event Add() error = %v", err)
		}
		if decoded == nil || !reflect.DeepEqual(decoded.Data, []any{"hello"}) {
			t.Fatalf("decoded packet = %#v, want data [hello]", decoded)
		}
	})

	t.Run("parser.js/should ensure that a packet is valid", func(t *testing.T) {
		tests := []struct {
			packet *Packet
			valid  bool
		}{
			{&Packet{Type: CONNECT, Nsp: "/"}, true},
			{&Packet{Type: CONNECT, Nsp: "/admin", Data: "invalid"}, false},
			{&Packet{Type: CONNECT, Nsp: "/", Data: []any{}}, false},
			{&Packet{Type: DISCONNECT, Nsp: "/admin", Data: map[string]any{}}, false},
			{&Packet{Type: EVENT, Nsp: "/admin", Data: "invalid"}, false},
			{&Packet{Type: EVENT, Nsp: "/admin", Data: map[string]any{}}, false},
			{&Packet{Type: EVENT, Nsp: "/", Data: []any{map[string]any{"toString": "foo"}}}, false},
			{&Packet{Type: EVENT, Nsp: "/", Data: []any{true, "foo"}}, false},
			{&Packet{Type: EVENT, Nsp: "/", Data: []any{nil, "bar"}}, false},
			{&Packet{Type: EVENT, Nsp: "/", Data: []any{"connect"}}, false},
			{&Packet{Type: EVENT, Nsp: "/", Data: []any{"disconnect", "123"}}, false},
		}
		for index, test := range tests {
			if got := IsPacketValid(test.packet); got != test.valid {
				t.Errorf("case %d: IsPacketValid(%#v) = %t, want %t", index, test.packet, got, test.valid)
			}
		}
	})

	t.Run("buffer.js/encodes a Buffer", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/cool",
			Id:   officialUint64(23),
			Data: []any{"a", []byte("abc")},
		})
	})

	t.Run("buffer.js/encodes a nested Buffer", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/cool",
			Id:   officialUint64(23),
			Data: []any{"a", map[string]any{"b": []any{"c", []byte("abc")}}},
		})
	})

	t.Run("buffer.js/encodes a binary ack with Buffer", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: ACK,
			Nsp:  "/back",
			Id:   officialUint64(127),
			Data: []any{"a", []byte("xxx"), map[string]any{}},
		})
	})

	t.Run("buffer.js/encodes a Buffer nested in an object with a toJSON() method", func(t *testing.T) {
		officialAssertRoundTripData(t, &Packet{
			Type: EVENT,
			Nsp:  "/",
			Data: []any{"a", &messageWithToJSON{internal: types.NewBytesBufferString("abc")}},
		}, []any{"a", map[string]any{"file": []byte("abc")}})
	})

	t.Run("buffer.js/throws an error when adding an attachment with an invalid 'num' attribute (string)", func(t *testing.T) {
		decoder := NewDecoder()
		if err := decoder.Add(`51-["hello",{"_placeholder":true,"num":"splice"}]`); err != nil {
			t.Fatalf("header Add() error = %v", err)
		}
		if err := decoder.Add([]byte("world")); !errors.Is(err, ErrIllegalAttachments) || err.Error() != "illegal attachments" {
			t.Fatalf("attachment Add() error = %v, want %v", err, ErrIllegalAttachments)
		}
	})

	t.Run("buffer.js/throws an error when adding an attachment with an invalid 'num' attribute (out-of-bound)", func(t *testing.T) {
		decoder := NewDecoder()
		if err := decoder.Add(`51-["hello",{"_placeholder":true,"num":1}]`); err != nil {
			t.Fatalf("header Add() error = %v", err)
		}
		if err := decoder.Add([]byte("world")); !errors.Is(err, ErrIllegalAttachments) || err.Error() != "illegal attachments" {
			t.Fatalf("attachment Add() error = %v, want %v", err, ErrIllegalAttachments)
		}
	})

	t.Run("buffer.js/throws an error when adding an attachment without header", func(t *testing.T) {
		if err := NewDecoder().Add([]byte("world")); !errors.Is(err, ErrBinaryWithoutReconstruction) {
			t.Fatalf("Add() error = %v, want %v", err, ErrBinaryWithoutReconstruction)
		}
	})

	t.Run("buffer.js/throws an error when decoding a binary event without attachments", func(t *testing.T) {
		decoder := NewDecoder()
		if err := decoder.Add(`51-["hello",{"_placeholder":true,"num":0}]`); err != nil {
			t.Fatalf("header Add() error = %v", err)
		}
		if err := decoder.Add(`2["hello"]`); !errors.Is(err, ErrPlaintextDuringReconstruction) {
			t.Fatalf("plaintext Add() error = %v, want %v", err, ErrPlaintextDuringReconstruction)
		}
	})

	t.Run("arraybuffer.js/encodes an ArrayBuffer", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/",
			Id:   officialUint64(0),
			Data: []any{"a", []byte{0, 0}},
		})
	})

	t.Run("arraybuffer.js/encodes an ArrayBuffer into an object with a null prototype", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/",
			Id:   officialUint64(0),
			Data: []any{"a", map[string]any{"array": []byte{0, 0}}},
		})
	})

	t.Run("arraybuffer.js/encodes a TypedArray", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/",
			Id:   officialUint64(0),
			Data: []any{"a", []byte{0, 1, 2, 3, 4}},
		})
	})

	t.Run("arraybuffer.js/encodes ArrayBuffers deep in JSON", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/deep",
			Id:   officialUint64(999),
			Data: []any{"a", map[string]any{
				"a": "hi",
				"b": map[string]any{"why": []byte{0, 0, 0}},
				"c": map[string]any{"a": "bye", "b": map[string]any{"a": make([]byte, 6)}},
			}},
		})
	})

	t.Run("arraybuffer.js/encodes deep binary JSON with null values", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/",
			Id:   officialUint64(600),
			Data: []any{"a", map[string]any{
				"a": "b",
				"c": 4,
				"e": map[string]any{"g": nil},
				"h": make([]byte, 9),
			}},
		})
	})

	t.Run("arraybuffer.js/should not modify the input packet", func(t *testing.T) {
		packet := &Packet{
			Type: EVENT,
			Nsp:  "/",
			Data: []any{"a", []byte{1, 2, 3}, []byte{4, 5, 6}},
		}
		before := officialNormalize(packet.Data)
		NewEncoder().Encode(packet)

		if packet.Type != EVENT || packet.Nsp != "/" || packet.Attachments != nil {
			t.Fatalf("Encode() modified packet metadata: %#v", packet)
		}
		if after := officialNormalize(packet.Data); !reflect.DeepEqual(after, before) {
			t.Fatalf("Encode() modified packet data: before=%#v after=%#v", before, after)
		}
	})

	t.Run("blob.js/encodes a Blob", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/",
			Id:   officialUint64(0),
			Data: []any{"a", bytes.NewReader([]byte{0, 0})},
		})
	})

	t.Run("blob.js/encodes an Blob deep in JSON", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: EVENT,
			Nsp:  "/deep",
			Id:   officialUint64(999),
			Data: []any{"a", map[string]any{
				"a": "hi",
				"b": map[string]any{"why": bytes.NewReader([]byte{0, 0})},
				"c": "bye",
			}},
		})
	})

	t.Run("blob.js/encodes a binary ack with a blob", func(t *testing.T) {
		officialAssertRoundTrip(t, &Packet{
			Type: ACK,
			Nsp:  "/deep",
			Id:   officialUint64(999),
			Data: []any{map[string]any{
				"a": "hi ack",
				"b": map[string]any{"why": bytes.NewReader([]byte{0, 0})},
				"c": "bye ack",
			}},
		})
	})
}

func officialAssertReviver(t *testing.T, decoder Decoder) {
	t.Helper()

	var decoded *Packet
	if err := decoder.On("decoded", func(args ...any) {
		decoded, _ = args[0].(*Packet)
	}); err != nil {
		t.Fatalf("On() error = %v", err)
	}
	if err := decoder.Add(`2["b",{"a":"val"}]`); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	want := []any{"b", map[string]any{"a": "VAL"}}
	if decoded == nil || !reflect.DeepEqual(decoded.Data, want) {
		t.Fatalf("decoded data = %#v, want %#v", decoded, want)
	}
}

func officialAssertRoundTrip(t *testing.T, packet *Packet) {
	t.Helper()
	officialAssertRoundTripData(t, packet, officialNormalize(packet.Data))
}

func officialAssertRoundTripData(t *testing.T, packet *Packet, expectedData any) {
	t.Helper()

	encoded := NewEncoder().Encode(packet)
	decoder := NewDecoder()
	var decoded *Packet
	if err := decoder.On("decoded", func(args ...any) {
		decoded, _ = args[0].(*Packet)
	}); err != nil {
		t.Fatalf("On() error = %v", err)
	}
	for _, part := range encoded {
		if err := decoder.Add(part); err != nil {
			t.Fatalf("Add(%T) error = %v", part, err)
		}
	}

	if decoded == nil {
		t.Fatal("decoded event was not emitted")
	}
	if decoded.Type != packet.Type || decoded.Nsp != packet.Nsp {
		t.Fatalf("decoded metadata = {type:%v nsp:%q}, want {type:%v nsp:%q}", decoded.Type, decoded.Nsp, packet.Type, packet.Nsp)
	}
	if !officialIDsEqual(decoded.Id, packet.Id) {
		t.Fatalf("decoded id = %v, want %v", decoded.Id, packet.Id)
	}
	if decoded.Attachments != nil {
		t.Fatalf("decoded attachments = %v, want nil", *decoded.Attachments)
	}
	if got := officialNormalize(decoded.Data); !reflect.DeepEqual(got, expectedData) {
		t.Fatalf("decoded data = %#v, want %#v", got, expectedData)
	}
}

func officialNormalize(value any) any {
	switch typedValue := value.(type) {
	case nil:
		return nil
	case types.BufferInterface:
		return append([]byte(nil), typedValue.Bytes()...)
	case []byte:
		return append([]byte(nil), typedValue...)
	case []any:
		result := make([]any, len(typedValue))
		for index, item := range typedValue {
			result[index] = officialNormalize(item)
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(typedValue))
		for key, item := range typedValue {
			result[key] = officialNormalize(item)
		}
		return result
	case io.ReadSeeker:
		position, err := typedValue.Seek(0, io.SeekCurrent)
		if err != nil {
			panic(err)
		}
		data, err := io.ReadAll(typedValue)
		if err != nil {
			panic(err)
		}
		if _, err := typedValue.Seek(position, io.SeekStart); err != nil {
			panic(err)
		}
		return data
	case int:
		return float64(typedValue)
	case int8:
		return float64(typedValue)
	case int16:
		return float64(typedValue)
	case int32:
		return float64(typedValue)
	case int64:
		return float64(typedValue)
	case uint:
		return float64(typedValue)
	case uint8:
		return float64(typedValue)
	case uint16:
		return float64(typedValue)
	case uint32:
		return float64(typedValue)
	case uint64:
		return float64(typedValue)
	default:
		return value
	}
}

func officialUint64(value uint64) *uint64 {
	return new(value)
}

func officialIDsEqual(left, right *uint64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func officialRecoveredError(value any) error {
	if value == nil {
		return nil
	}
	if err, ok := value.(error); ok {
		return err
	}
	return errors.New("non-error panic")
}
