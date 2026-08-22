package socketio

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDispatchQueueOverflowPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  OverflowPolicy
		wantErr bool
	}{
		{name: "drop newest", policy: OverflowDropNewest},
		{name: "reject", policy: OverflowReject, wantErr: true},
		{name: "disconnect", policy: OverflowDisconnect, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			started := make(chan struct{})
			release := make(chan struct{})
			overflow := make(chan OverflowPolicy, 1)
			queue := newDispatchQueue(QueueOptions{
				MaxPending: 1,
				Overflow:   test.policy,
			}, func(policy OverflowPolicy) {
				overflow <- policy
			})
			queue.Start()

			if err := queue.Enqueue(func() {
				close(started)
				<-release
			}); err != nil {
				t.Fatalf("enqueue running task: %v", err)
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("queue worker did not start")
			}

			if err := queue.Enqueue(func() {}); err != nil {
				t.Fatalf("enqueue pending task: %v", err)
			}
			err := queue.Enqueue(func() {})
			if test.wantErr && !errors.Is(err, errQueueFull) {
				t.Fatalf("overflow error = %v, want %v", err, errQueueFull)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("overflow error = %v, want nil", err)
			}
			if queue.Overflows() != 1 {
				t.Fatalf("overflow count = %d, want 1", queue.Overflows())
			}

			select {
			case policy := <-overflow:
				if policy != test.policy {
					t.Fatalf("overflow callback policy = %d, want %d", policy, test.policy)
				}
			case <-time.After(time.Second):
				t.Fatal("overflow callback was not invoked")
			}

			queue.Close(true)
			close(release)
			select {
			case <-queue.Done():
			case <-time.After(time.Second):
				t.Fatal("queue worker did not stop")
			}
		})
	}
}

func TestNativeClientAssets(t *testing.T) {
	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	handler := server.Handler()
	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/socket.io/socket.io.min.js", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET asset status = %d, want 200", get.Code)
	}
	if !strings.Contains(get.Header().Get("Content-Type"), "javascript") {
		t.Fatalf("GET asset content type = %q", get.Header().Get("Content-Type"))
	}
	etag := get.Header().Get("ETag")
	if etag == "" {
		t.Fatal("GET asset did not return an ETag")
	}
	if get.Body.Len() == 0 {
		t.Fatal("GET asset returned an empty body")
	}

	head := httptest.NewRecorder()
	handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/socket.io/socket.io.min.js", nil))
	if head.Code != http.StatusOK || head.Body.Len() != 0 {
		t.Fatalf("HEAD asset = status %d body %d bytes, want 200/0", head.Code, head.Body.Len())
	}
	if head.Header().Get("Content-Length") == "" {
		t.Fatal("HEAD asset did not return Content-Length")
	}

	notModifiedRequest := httptest.NewRequest(http.MethodGet, "/socket.io/socket.io.min.js", nil)
	notModifiedRequest.Header.Set("If-None-Match", etag)
	notModified := httptest.NewRecorder()
	handler.ServeHTTP(notModified, notModifiedRequest)
	if notModified.Code != http.StatusNotModified {
		t.Fatalf("conditional GET status = %d, want 304", notModified.Code)
	}

	gzipRequest := httptest.NewRequest(http.MethodGet, "/socket.io/socket.io.min.js", nil)
	gzipRequest.Header.Set("Accept-Encoding", "gzip")
	compressed := httptest.NewRecorder()
	handler.ServeHTTP(compressed, gzipRequest)
	if compressed.Code != http.StatusOK {
		t.Fatalf("gzip GET status = %d, want 200", compressed.Code)
	}
	if compressed.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("gzip Content-Encoding = %q", compressed.Header().Get("Content-Encoding"))
	}
	reader, err := gzip.NewReader(compressed.Body)
	if err != nil {
		t.Fatalf("opening gzip asset: %v", err)
	}
	decompressed, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("reading gzip asset: %v", err)
	}
	if len(decompressed) != get.Body.Len() {
		t.Fatalf("gzip asset length = %d, want %d", len(decompressed), get.Body.Len())
	}
}

func TestNativeRecoveryFiltersMissedPackets(t *testing.T) {
	server, err := New(
		WithClientServing(false),
		WithConnectionStateRecovery(RecoveryOptions{
			MaxDisconnectionDuration: time.Minute,
			CleanupInterval:          time.Minute,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	namespace := server.Of("/")
	adapter, ok := namespace.adapter.(*memoryAdapter)
	if !ok {
		t.Fatalf("default adapter = %T, want *memoryAdapter", namespace.adapter)
	}

	baseline := adapter.persistRecoverablePacket(Packet{
		Type:      PacketEvent,
		Namespace: "/",
		Data:      []any{"baseline"},
	}, &BroadcastOptions{Rooms: []Room{"room-a"}})
	baselineOffset := recoveryOffset(t, baseline)

	session := Session{
		SID:   "socket-1",
		PID:   "private-1",
		Rooms: []Room{"socket-1", "room-a"},
		Data:  map[string]any{"user": "alice"},
	}
	if persistErr := adapter.PersistSession(context.Background(), session); persistErr != nil {
		t.Fatalf("persist session: %v", persistErr)
	}

	adapter.persistRecoverablePacket(Packet{
		Type:      PacketEvent,
		Namespace: "/",
		Data:      []any{"match", 1},
	}, &BroadcastOptions{Rooms: []Room{"room-a"}})
	adapter.persistRecoverablePacket(Packet{
		Type:      PacketEvent,
		Namespace: "/",
		Data:      []any{"excluded", 2},
	}, &BroadcastOptions{Rooms: []Room{"room-a"}, Except: []Room{"room-a"}})
	adapter.persistRecoverablePacket(Packet{
		Type:      PacketEvent,
		Namespace: "/",
		Data:      []any{"other-room", 3},
	}, &BroadcastOptions{Rooms: []Room{"room-b"}})
	adapter.persistRecoverablePacket(Packet{
		Type:      PacketEvent,
		Namespace: "/",
		Data:      []any{"volatile", 4},
	}, &BroadcastOptions{Rooms: []Room{"room-a"}, Flags: BroadcastFlags{Volatile: true}})

	recovered, err := adapter.RestoreSession(context.Background(), session.PID, baselineOffset)
	if err != nil {
		t.Fatalf("restore session: %v", err)
	}
	if recovered == nil {
		t.Fatal("restore session returned nil")
	}
	if recovered.SID != session.SID || recovered.PID != session.PID {
		t.Fatalf("restored identity = %q/%q, want %q/%q", recovered.SID, recovered.PID, session.SID, session.PID)
	}
	if len(recovered.MissedPackets) != 1 {
		t.Fatalf("missed packets = %#v, want exactly one matching packet", recovered.MissedPackets)
	}
	values, ok := recovered.MissedPackets[0].([]any)
	if !ok || len(values) < 2 || values[0] != "match" || values[1] != 1 {
		t.Fatalf("recovered packet = %#v, want match/1", recovered.MissedPackets[0])
	}

	adapter.mu.RLock()
	persistedCount := len(adapter.packets)
	adapter.mu.RUnlock()
	if persistedCount != 4 {
		t.Fatalf("persisted recovery packets = %d, want 4 (volatile must not persist)", persistedCount)
	}
}

func recoveryOffset(t *testing.T, packet Packet) string {
	t.Helper()
	values, ok := packet.Data.([]any)
	if !ok || len(values) == 0 {
		t.Fatalf("recovery packet data = %#v", packet.Data)
	}
	offset, ok := values[len(values)-1].(string)
	if !ok || offset == "" {
		t.Fatalf("recovery offset = %#v", values[len(values)-1])
	}
	return offset
}
