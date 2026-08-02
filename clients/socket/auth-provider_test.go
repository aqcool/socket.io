package socket

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func newAuthTestSocket(t *testing.T, provider AuthProvider) *Socket {
	t.Helper()
	managerOptions := DefaultManagerOptions()
	managerOptions.SetAutoConnect(false)
	managerOptions.SetTimeout(time.Second)
	manager := NewManager("http://unused.invalid", managerOptions)
	socketOptions := DefaultSocketOptions()
	socketOptions.SetAuthProvider(provider)
	return NewSocket(manager, "/", socketOptions)
}

func waitForAuthToken(t *testing.T, socket *Socket, expected string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if socket.Auth()["token"] == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected auth token %q, got %#v", expected, socket.Auth())
}

func TestAuthProviderRunsBeforeEveryConnectionAttempt(t *testing.T) {
	var calls atomic.Int64
	socket := newAuthTestSocket(t, func(ctx context.Context) (map[string]any, error) {
		if ctx == nil {
			t.Fatal("expected a non-nil context")
		}
		call := calls.Add(1)
		return map[string]any{"token": fmt.Sprintf("token-%d", call)}, nil
	})

	socket.onopen()
	waitForAuthToken(t, socket, "token-1")
	socket.onopen()
	waitForAuthToken(t, socket, "token-2")

	if calls.Load() != 2 {
		t.Fatalf("expected provider to run twice, got %d", calls.Load())
	}
}

func TestAuthProviderErrorCanBeRetried(t *testing.T) {
	var calls atomic.Int64
	socket := newAuthTestSocket(t, func(context.Context) (map[string]any, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("expired token")
		}
		return map[string]any{"token": "refreshed"}, nil
	})

	connectErrors := make(chan error, 1)
	if err := socket.On("connect_error", func(args ...any) {
		if len(args) > 0 {
			connectErrors <- args[0].(error)
		}
	}); err != nil {
		t.Fatal(err)
	}

	socket.onopen()
	select {
	case err := <-connectErrors:
		if err.Error() != "expired token" {
			t.Fatalf("unexpected provider error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected connect_error from auth provider")
	}

	socket.onopen()
	waitForAuthToken(t, socket, "refreshed")
}

func TestAuthProviderContextIsCanceledWhenSocketCloses(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	socket := newAuthTestSocket(t, func(ctx context.Context) (map[string]any, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	})

	socket.onopen()
	<-started
	socket.onclose("test close", nil)

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("expected auth provider context cancellation")
	}
}

func TestSetAuthOnlyUpdatesCurrentNamespace(t *testing.T) {
	managerOptions := DefaultManagerOptions()
	managerOptions.SetAutoConnect(false)
	manager := NewManager("http://unused.invalid", managerOptions)
	first := NewSocket(manager, "/first", DefaultSocketOptions())
	second := NewSocket(manager, "/second", DefaultSocketOptions())

	first.SetAuth(map[string]any{"token": "first"})
	second.SetAuth(map[string]any{"token": "second"})
	first._pid.Store("private-id")
	first._sendConnectPacket(first.Auth())

	if first.Auth()["token"] != "first" || first.Auth()["pid"] != nil {
		t.Fatalf("first namespace auth was unexpectedly mutated: %#v", first.Auth())
	}
	if second.Auth()["token"] != "second" {
		t.Fatalf("second namespace auth was unexpectedly changed: %#v", second.Auth())
	}
}
