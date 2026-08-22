package socketio

import (
	"context"
	"testing"
	"time"
)

type asyncAckAdapter struct {
	Adapter
}

func (a *asyncAckAdapter) ServerCount(context.Context) (int64, error) { return 2, nil }

func (a *asyncAckAdapter) BroadcastWithAck(
	ctx context.Context,
	_ Packet,
	_ *BroadcastOptions,
	clientCount func(uint64),
	ack Ack,
) error {
	go func() {
		clientCount(1)
		select {
		case <-time.After(10 * time.Millisecond):
			ack([]any{"local"}, nil)
		case <-ctx.Done():
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
			clientCount(1)
		case <-ctx.Done():
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
			ack([]any{"remote"}, nil)
		case <-ctx.Done():
		}
	}()
	return nil
}

func TestBroadcastOperatorWaitsForDistributedAcks(t *testing.T) {
	server, err := New(WithClientServing(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	namespace := server.Of("/")
	namespace.adapter = &asyncAckAdapter{Adapter: namespace.adapter}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	responses, err := namespace.EmitAcks(ctx, "cluster-event", "payload")
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses = %#v, want 2 acknowledgements", responses)
	}
	if len(responses[0]) != 1 || len(responses[1]) != 1 {
		t.Fatalf("unexpected acknowledgement shape: %#v", responses)
	}
	if responses[0][0] != "local" || responses[1][0] != "remote" {
		t.Fatalf("responses = %#v, want local then remote", responses)
	}
}

func TestBroadcastOperatorDistributedAckTimeout(t *testing.T) {
	server, err := New(WithClientServing(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	namespace := server.Of("/")
	namespace.adapter = &asyncAckAdapter{Adapter: namespace.adapter}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_, err = namespace.EmitAcks(ctx, "cluster-event", "payload")
	if err == nil {
		t.Fatal("EmitAcks unexpectedly completed before all distributed acknowledgements")
	}
}
