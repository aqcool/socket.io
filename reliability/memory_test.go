package reliability

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreReplayAckDeduplicateAndPrune(t *testing.T) {
	store := NewMemoryStore()
	target := Target{Kind: TargetUser, Namespace: "/", ID: "u1"}
	for _, id := range []string{"one", "two", "three"} {
		event := &Event{ID: id, Size: 4, CreatedAt: time.Now()}
		_, err := store.Append(context.Background(), target, event)
		if err != nil {
			t.Fatal(err)
		}
	}
	events, err := store.Replay(context.Background(), target, "1", 10)
	if err != nil || len(events) != 2 || events[0].ID != "two" {
		t.Fatalf("unexpected replay: %#v, %v", events, err)
	}
	if err = store.Ack(context.Background(), "c1", events[0].Offset); err != nil {
		t.Fatal(err)
	}
	if offset, _ := store.LastAck(context.Background(), "c1"); offset != "2" {
		t.Fatalf("unexpected ack offset %q", offset)
	}
	fresh, _ := store.MarkInbound(context.Background(), "c1", "request-1", time.Now().Add(time.Minute))
	duplicate, _ := store.MarkInbound(context.Background(), "c1", "request-1", time.Now().Add(time.Minute))
	if !fresh || duplicate {
		t.Fatal("inbound deduplication did not reject duplicate")
	}
	if err = store.Prune(context.Background(), Limits{MaxEvents: 1, MaxBytes: 4}); err != nil {
		t.Fatal(err)
	}
	events, _ = store.Replay(context.Background(), target, "", 10)
	if len(events) != 1 || events[0].ID != "three" {
		t.Fatalf("unexpected pruned events: %#v", events)
	}
}
