package socket

import (
	"errors"
	"testing"
	"time"

	"github.com/aqcool/socket.io/parsers/socket/v4/parser"
)

func newBackpressureTestSocket(t *testing.T, configure func(*SocketOptions)) *Socket {
	t.Helper()
	managerOptions := DefaultManagerOptions()
	managerOptions.SetAutoConnect(false)
	manager := NewManager("http://unused.invalid", managerOptions)
	socketOptions := DefaultSocketOptions()
	configure(socketOptions)
	return NewSocket(manager, "/", socketOptions)
}

func bufferedEventName(t *testing.T, packet *Packet) string {
	t.Helper()
	data, ok := packet.Data.([]any)
	if !ok || len(data) == 0 {
		t.Fatalf("unexpected packet data: %#v", packet.Data)
	}
	return data[0].(string)
}

func TestSendBufferDropOldestAndStats(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(options *SocketOptions) {
		options.SetMaxSendBufferPackets(2)
		options.SetOverflowStrategy(OverflowDropOldest)
	})
	overflows := make(chan *OverflowDetails, 1)
	if err := socket.On("overflow", func(args ...any) {
		overflows <- args[0].(*OverflowDetails)
	}); err != nil {
		t.Fatal(err)
	}

	if err := socket.Emit("one"); err != nil {
		t.Fatal(err)
	}
	if err := socket.Emit("two"); err != nil {
		t.Fatal(err)
	}
	if err := socket.Emit("three"); err != nil {
		t.Fatal(err)
	}

	if socket.SendBuffer().Len() != 2 {
		t.Fatalf("expected two buffered packets, got %d", socket.SendBuffer().Len())
	}
	first, _ := socket.SendBuffer().Get(0)
	second, _ := socket.SendBuffer().Get(1)
	if bufferedEventName(t, first) != "two" || bufferedEventName(t, second) != "three" {
		t.Fatalf("unexpected send buffer contents: %#v", socket.SendBuffer().All())
	}
	stats := socket.BufferStats()
	if stats.SendPackets != 2 || stats.SendBytes <= 0 {
		t.Fatalf("unexpected send buffer stats: %#v", stats)
	}
	select {
	case details := <-overflows:
		if details.Buffer != "send" || details.Strategy != OverflowDropOldest {
			t.Fatalf("unexpected overflow details: %#v", details)
		}
	case <-time.After(time.Second):
		t.Fatal("expected overflow event")
	}
}

func TestSendBufferRejectAndByteLimit(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(options *SocketOptions) {
		options.SetMaxSendBufferBytes(1)
		options.SetOverflowStrategy(OverflowReject)
	})
	if err := socket.Emit("too-large"); !errors.Is(err, ErrBufferOverflow) {
		t.Fatalf("expected buffer overflow error, got %v", err)
	}
	if socket.BufferStats().SendPackets != 0 {
		t.Fatalf("rejected packet was buffered: %#v", socket.BufferStats())
	}
}

func TestReceiveBufferDropOldest(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(options *SocketOptions) {
		options.SetMaxReceiveBufferPackets(1)
		options.SetOverflowStrategy(OverflowDropOldest)
	})
	socket.onevent(&parser.Packet{Type: parser.EVENT, Data: []any{"first"}})
	socket.onevent(&parser.Packet{Type: parser.EVENT, Data: []any{"second"}})

	if socket.BufferStats().ReceivePackets != 1 {
		t.Fatalf("unexpected receive stats: %#v", socket.BufferStats())
	}
	args, _ := socket.ReceiveBuffer().Get(0)
	if args[0] != "second" {
		t.Fatalf("expected newest received event, got %#v", args)
	}
}

func TestRetryQueueDropNewest(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(options *SocketOptions) {
		options.SetRetries(1)
		options.SetMaxRetryQueuePackets(1)
		options.SetOverflowStrategy(OverflowDropNewest)
	})
	if err := socket.Emit("first"); err != nil {
		t.Fatal(err)
	}
	ackResult := make(chan error, 1)
	if err := socket.Emit("second", func(_ []any, err error) {
		ackResult <- err
	}); err != nil {
		t.Fatal(err)
	}
	if socket.BufferStats().RetryPackets != 1 {
		t.Fatalf("unexpected retry queue stats: %#v", socket.BufferStats())
	}
	select {
	case err := <-ackResult:
		if !errors.Is(err, ErrBufferOverflow) {
			t.Fatalf("unexpected dropped packet error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected dropped retry acknowledgement")
	}
}

func TestDisconnectOverflowEmitsSlowConsumer(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(options *SocketOptions) {
		options.SetMaxSendBufferPackets(1)
		options.SetOverflowStrategy(OverflowDisconnect)
	})
	slowConsumer := make(chan string, 1)
	if err := socket.On("slow_consumer", func(args ...any) {
		slowConsumer <- args[0].(string)
	}); err != nil {
		t.Fatal(err)
	}
	if err := socket.Emit("first"); err != nil {
		t.Fatal(err)
	}
	if err := socket.Emit("second"); !errors.Is(err, ErrBufferOverflow) {
		t.Fatalf("expected disconnect overflow error, got %v", err)
	}
	select {
	case buffer := <-slowConsumer:
		if buffer != "send" {
			t.Fatalf("unexpected slow consumer buffer: %s", buffer)
		}
	case <-time.After(time.Second):
		t.Fatal("expected slow_consumer event")
	}
}

func TestBufferDrainEvent(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(options *SocketOptions) {
		options.SetMaxSendBufferPackets(2)
	})
	drained := make(chan string, 1)
	if err := socket.On("drain", func(args ...any) {
		drained <- args[0].(string)
	}); err != nil {
		t.Fatal(err)
	}
	if err := socket.Emit("buffered"); err != nil {
		t.Fatal(err)
	}
	socket.onconnect("sid", "")

	select {
	case buffer := <-drained:
		if buffer != "send" {
			t.Fatalf("unexpected drained buffer: %s", buffer)
		}
	case <-time.After(time.Second):
		t.Fatal("expected drain event")
	}
	if socket.BufferStats().SendPackets != 0 || socket.BufferStats().SendBytes != 0 {
		t.Fatalf("expected empty send buffer: %#v", socket.BufferStats())
	}
}
