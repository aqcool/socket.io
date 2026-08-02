package engine

import (
	"strings"
	"testing"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
)

type controlledDrainTransport struct {
	Transport
	name string
	sent chan []*packet.Packet
}

func (t *controlledDrainTransport) Name() string { return t.name }

func (t *controlledDrainTransport) Write(packets []*packet.Packet) {
	t.SetWritable(false)
	t.sent <- packets
}

func newControlledDrainTransport(name string) *controlledDrainTransport {
	base := MakeTransport()
	transport := &controlledDrainTransport{
		Transport: base,
		name:      name,
		sent:      make(chan []*packet.Packet, 2),
	}
	base.Prototype(transport)
	base.SetReadyState(TransportStateOpen)
	base.SetWritable(true)
	return transport
}

func TestOfficialClientWriteBufferDrainsAfterTransportWrite(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			client := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
			client.readyState.Store(SocketStateOpen)
			client._maxPayload.Store(1_000_000)
			transport := newControlledDrainTransport(transportName)
			client.SetTransport(transport)

			client.Send(strings.NewReader("a"), nil, nil)
			client.Send(strings.NewReader("b"), nil, nil)
			if got := client.WriteBuffer().Len(); got != 2 {
				t.Fatalf("writeBuffer before first drain = %d, want 2", got)
			}
			first := <-transport.sent
			if len(first) != 1 {
				t.Fatalf("first transport write contains %d packets, want 1", len(first))
			}

			transport.SetWritable(true)
			transport.Emit("drain")
			if got := client.WriteBuffer().Len(); got != 1 {
				t.Fatalf("writeBuffer after first drain = %d, want 1", got)
			}
			second := <-transport.sent
			if len(second) != 1 {
				t.Fatalf("second transport write contains %d packets, want 1", len(second))
			}

			transport.SetWritable(true)
			transport.Emit("drain")
			if got := client.WriteBuffer().Len(); got != 0 {
				t.Fatalf("writeBuffer after second drain = %d, want 0", got)
			}
		})
	}
}

func TestOfficialClientSendCallbacksFollowFlushOrder(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			client := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
			client.readyState.Store(SocketStateOpen)
			client._maxPayload.Store(1_000_000)
			transport := newControlledDrainTransport(transportName)
			client.SetTransport(transport)
			callbacks := make(chan int, 3)

			client.Send(strings.NewReader("a"), nil, func() { callbacks <- 1 })
			client.Send(strings.NewReader("b"), nil, func() { callbacks <- 2 })
			client.Send(strings.NewReader("c"), nil, func() { callbacks <- 3 })
			if got := <-callbacks; got != 1 {
				t.Fatalf("first callback = %d, want 1", got)
			}
			<-transport.sent

			transport.SetWritable(true)
			transport.Emit("drain")
			for _, want := range []int{2, 3} {
				if got := <-callbacks; got != want {
					t.Fatalf("batched callback = %d, want %d", got, want)
				}
			}
			if packets := <-transport.sent; len(packets) != 2 {
				t.Fatalf("batched transport write contains %d packets, want 2", len(packets))
			}

			select {
			case unexpected := <-callbacks:
				t.Fatalf("callback executed more than once: %d", unexpected)
			default:
			}
		})
	}
}
