package socketio

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestOptionsAndConfigSnapshot(t *testing.T) {
	io, err := New(
		WithPath("realtime/"),
		WithClientServing(false),
		WithConnectTimeout(5*time.Second),
		WithQueue(QueueOptions{MaxPending: 128, Overflow: OverflowReject}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = io.Close() })

	cfg := io.Config()
	if cfg.Path != "/realtime" || cfg.ServeClient || cfg.ConnectTimeout != 5*time.Second {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	if cfg.Queue.MaxPending != 128 || cfg.Queue.Overflow != OverflowReject {
		t.Fatalf("unexpected queue config: %#v", cfg.Queue)
	}
}

func TestConcurrentOfReturnsSameNamespace(t *testing.T) {
	io, err := New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = io.Close() })

	const callers = 64
	results := make(chan *Namespace, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			results <- io.Of("chat")
		}()
	}
	wait.Wait()
	close(results)

	var first *Namespace
	for namespace := range results {
		if namespace == nil {
			t.Fatal("Of returned nil namespace")
		}
		if first == nil {
			first = namespace
			continue
		}
		if namespace != first {
			t.Fatalf("concurrent Of returned different namespace instances: %p != %p", namespace, first)
		}
	}
	if first.Name() != "/chat" {
		t.Fatalf("namespace name = %q, want /chat", first.Name())
	}
}

func TestListenAndServeReturnsBindError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	io, err := New()
	if err != nil {
		t.Fatal(err)
	}
	err = io.ListenAndServe(listener.Addr().String())
	if err == nil {
		t.Fatal("expected bind error")
	}
	if errors.Is(err, ErrClosed) {
		t.Fatalf("bind error was incorrectly reported as closed: %v", err)
	}
	_ = io.Close()
}

func TestCloseCancelsServerContextAndDone(t *testing.T) {
	io, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if err := io.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-io.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("server context was not canceled")
	}
	select {
	case <-io.Done():
	case <-time.After(time.Second):
		t.Fatal("server Done was not closed")
	}
	if err := io.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown must be idempotent: %v", err)
	}
}
