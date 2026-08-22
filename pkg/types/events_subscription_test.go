package types

import "testing"

func TestSubscriptionRemovesExactClosureInstance(t *testing.T) {
	emitter := NewEventEmitter()
	calls := []int{}

	makeListener := func(value int) EventListener {
		return func(...any) {
			calls = append(calls, value)
		}
	}

	first := makeListener(1)
	second := makeListener(2)

	firstSub, err := Subscribe(emitter, "event", first)
	if err != nil {
		t.Fatal(err)
	}
	secondSub, err := Subscribe(emitter, "event", second)
	if err != nil {
		t.Fatal(err)
	}

	if !secondSub.Close() {
		t.Fatal("expected exact second subscription to be removed")
	}
	if secondSub.Close() {
		t.Fatal("closing a subscription twice should report false")
	}

	emitter.Emit("event")
	if len(calls) != 1 || calls[0] != 1 {
		t.Fatalf("unexpected listeners invoked: %v", calls)
	}

	if !firstSub.Close() {
		t.Fatal("expected first subscription to be removed")
	}
	if emitter.ListenerCount("event") != 0 {
		t.Fatalf("listener count = %d, want 0", emitter.ListenerCount("event"))
	}
}
