package socket

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
)

func newAckTestSocket(t *testing.T) *Socket {
	t.Helper()
	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })
	socket := MakeSocket()
	socket.nsp = server.Sockets()
	socket.server = server
	return socket
}

func TestOnAckConsumesCallbackOnce(t *testing.T) {
	socket := MakeSocket()
	var calls atomic.Int64
	socket.acks.Store(7, func([]any, error) {
		calls.Add(1)
	})

	id := uint64(7)
	packet := &parser.Packet{Type: parser.ACK, Id: &id, Data: []any{"response"}}
	var group sync.WaitGroup
	for range 32 {
		group.Go(func() { socket.onack(packet) })
	}
	group.Wait()

	if calls.Load() != 1 {
		t.Fatalf("ACK callback called %d times, want once", calls.Load())
	}
}

func TestAckTimeoutIgnoresLateResponse(t *testing.T) {
	socket := newAckTestSocket(t)
	timeout := 10 * time.Millisecond
	result := make(chan error, 2)
	socket.registerAckCallback(9, "event", nil, func(_ []any, err error) {
		result <- err
	}, &timeout)

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected timeout error")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ACK timeout")
	}

	id := uint64(9)
	socket.onack(&parser.Packet{Type: parser.ACK, Id: &id, Data: []any{"late"}})
	select {
	case <-result:
		t.Fatal("late ACK called callback a second time")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestZeroAckTimeoutRegistersBeforeTimerAndCallsOnce(t *testing.T) {
	socket := newAckTestSocket(t)

	for iteration := range 250 {
		var calls atomic.Int64
		result := make(chan error, 2)
		timeout := time.Duration(0)
		id := uint64(iteration)
		socket.registerAckCallback(id, "zero-timeout", nil, func(_ []any, err error) {
			calls.Add(1)
			result <- err
		}, &timeout)

		select {
		case err := <-result:
			if err == nil {
				t.Fatalf("iteration %d: expected timeout error", iteration)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: zero timeout did not complete", iteration)
		}

		if _, exists := socket.acks.Load(id); exists {
			t.Fatalf("iteration %d: timed-out ACK remained registered", iteration)
		}
		socket.onack(&parser.Packet{Type: parser.ACK, Id: &id, Data: []any{"late"}})
		select {
		case <-result:
			t.Fatalf("iteration %d: late ACK called callback a second time", iteration)
		case <-time.After(time.Millisecond):
		}
		if calls.Load() != 1 {
			t.Fatalf("iteration %d: callback called %d times, want once", iteration, calls.Load())
		}
	}
}

func TestAckAndTimeoutRaceHasSingleWinner(t *testing.T) {
	socket := newAckTestSocket(t)
	for iteration := range 100 {
		timeout := time.Millisecond
		var calls atomic.Int64
		done := make(chan struct{}, 2)
		socket.registerAckCallback(uint64(iteration), "event", nil, func([]any, error) {
			calls.Add(1)
			done <- struct{}{}
		}, &timeout)

		id := uint64(iteration)
		time.AfterFunc(time.Millisecond, func() {
			socket.onack(&parser.Packet{Type: parser.ACK, Id: &id, Data: []any{"response"}})
		})
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: neither ACK nor timeout completed", iteration)
		}
		time.Sleep(2 * time.Millisecond)
		if calls.Load() != 1 {
			t.Fatalf("iteration %d: callback called %d times, want once", iteration, calls.Load())
		}
	}
}
