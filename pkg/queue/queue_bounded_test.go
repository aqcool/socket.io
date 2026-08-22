package queue

import (
	"testing"
	"time"
)

func TestBoundedQueueRejectsOverflow(t *testing.T) {
	q := NewBounded(1)
	t.Cleanup(q.TryClose)
	release := make(chan struct{})
	started := make(chan struct{})
	if got := q.TryEnqueue(func() { close(started); <-release }); got != EnqueueAccepted {
		t.Fatalf("first enqueue = %v", got)
	}
	<-started
	if got := q.TryEnqueue(func() {}); got != EnqueueAccepted {
		t.Fatalf("pending enqueue = %v", got)
	}
	if got := q.TryEnqueue(func() {}); got != EnqueueFull {
		t.Fatalf("overflow enqueue = %v, want full", got)
	}
	if q.OverflowCount() != 1 {
		t.Fatalf("overflow count = %d, want 1", q.OverflowCount())
	}
	close(release)
}

func TestQueueCloseDrainsAcceptedWork(t *testing.T) {
	q := NewBounded(4)
	completed := make(chan struct{}, 2)
	q.Enqueue(func() { completed <- struct{}{} })
	q.Enqueue(func() { completed <- struct{}{} })
	done := make(chan struct{})
	go func() { q.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("queue did not drain")
	}
	if len(completed) != 2 {
		t.Fatalf("completed = %d, want 2", len(completed))
	}
}
