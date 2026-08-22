package socketio

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestEventHubSubscriptionHasExactIdentity(t *testing.T) {
	var bridge func(...any)
	hub := newEventHub(
		func(_ string, listener func(...any)) error {
			bridge = listener
			return nil
		},
		context.Background,
		nil,
		nil,
	)

	var first atomic.Int32
	var second atomic.Int32
	makeListener := func(counter *atomic.Int32) Listener {
		return func(context.Context, ...any) error {
			counter.Add(1)
			return nil
		}
	}

	firstSubscription := hub.On("event", makeListener(&first))
	secondSubscription := hub.On("event", makeListener(&second))
	if bridge == nil {
		t.Fatal("underlying bridge listener was not installed")
	}

	if err := secondSubscription.Close(); err != nil {
		t.Fatal(err)
	}
	bridge("payload")
	if first.Load() != 1 || second.Load() != 0 {
		t.Fatalf("listener calls = %d/%d, want 1/0", first.Load(), second.Load())
	}

	if err := firstSubscription.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstSubscription.Close(); err != nil {
		t.Fatalf("subscription close must be idempotent: %v", err)
	}
}

func TestEventHubOnceRemovesBeforeInvocation(t *testing.T) {
	var bridge func(...any)
	hub := newEventHub(
		func(_ string, listener func(...any)) error {
			bridge = listener
			return nil
		},
		context.Background,
		nil,
		nil,
	)

	var calls atomic.Int32
	hub.Once("once", func(context.Context, ...any) error {
		calls.Add(1)
		bridge()
		return nil
	})
	bridge()
	if calls.Load() != 1 {
		t.Fatalf("once listener calls = %d, want 1", calls.Load())
	}
}
