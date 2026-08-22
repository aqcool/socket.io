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

type Packet struct {
	Type      uint8
	Namespace string
	ID        *uint64
	Data      any
}

type PacketEncoder interface {
	Encode(Packet) ([][]byte, error)
}

type PacketDecoder interface {
	Add([]byte) error
	Close() error
}

type PacketCodec interface {
	NewEncoder() PacketEncoder
	NewDecoder() PacketDecoder
}
