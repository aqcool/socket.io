package socketio

import "encoding/json"

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

// Packet is the codec-facing Socket.IO packet representation. Binary codecs
// receive complete packets; Attachments is available for codecs that expose
// the standard Socket.IO binary framing model.
type Packet struct {
	Type        uint8
	Namespace   string
	ID          *uint64
	Data        any
	Attachments *uint64
}

// PacketEncoder returns the wire header as the first buffer and optional binary
// attachments as following buffers. For a non-binary packet the result contains
// exactly one buffer.
type PacketEncoder interface {
	Encode(Packet) ([][]byte, error)
}

// PacketDecoder consumes one Engine.IO message at a time. binary identifies
// whether the message was received as a binary transport frame. Returning nil
// means more binary attachments are required before a complete packet exists.
type PacketDecoder interface {
	Add(data []byte, binary bool) (*Packet, error)
	Close() error
}

type PacketCodec interface {
	NewEncoder() PacketEncoder
	NewDecoder() PacketDecoder
}
