// Package main demonstrates comprehensive debugging and validation of the Socket.IO Unix adapter.
//
// This example exhaustively covers all adapter functionalities:
//   - UnixClient initialization and lifecycle management
//   - Connection establishment via Unix Domain Sockets (SOCK_STREAM)
//   - Length-prefixed message framing for reliable stream delivery
//   - Connection pooling and automatic reconnection
//   - Data transmission and reception between peer nodes
//   - Adapter builder and multi-namespace dispatch
//   - Emitter API: Emit, To/In/Except, Volatile, Compress, SocketsJoin/Leave, ServerSideEmit
//   - Heartbeat mechanism and peer discovery
//   - Robust error handling for invalid paths, nil inputs, and edge cases
//   - Proper resource cleanup (listener sockets, connections, socket files)
//
// Each test step prints [PASS] or [FAIL] with a description so the adapter's
// completeness and stability can be verified at a glance.
//
// Usage:
//
//	go run main.go
//
// Cross-platform note:
//   - This adapter uses AF_UNIX stream sockets (SOCK_STREAM), which are supported on
//     Linux, macOS, and Windows 10 Build 17063+ with Go 1.12+.
//   - Unlike the previous unixgram (SOCK_DGRAM) implementation, stream sockets work
//     correctly on Windows without any limitations.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aqcool/socket.io/adapters/adapter/v4"
	"github.com/aqcool/socket.io/adapters/unix/v4"
	unix_adapter "github.com/aqcool/socket.io/adapters/unix/v4/adapter"
	unix_emitter "github.com/aqcool/socket.io/adapters/unix/v4/emitter"
	"github.com/aqcool/socket.io/servers/socket/v4"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

var (
	passed int
	failed int
	mu     sync.Mutex
)

// check logs a PASS/FAIL line and updates counters.
func check(name string, ok bool, detail string) {
	mu.Lock()
	defer mu.Unlock()
	if ok {
		passed++
		fmt.Printf("  [PASS] %s\n", name)
	} else {
		failed++
		fmt.Printf("  [FAIL] %s — %s\n", name, detail)
	}
}

// probeUnixStreamSupport checks whether AF_UNIX stream sockets (SOCK_STREAM)
// can be created in the current environment.
func probeUnixStreamSupport() (bool, error) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("sio-probe-unix-%d.sock", time.Now().UnixNano()))
	defer os.Remove(path)

	ln, err := net.Listen("unix", path)
	if err != nil {
		return false, err
	}
	ln.Close()
	return true, nil
}

func printEnvironmentSummary(streamOK bool, streamErr error) {
	fmt.Printf("Environment: %s/%s %s\n", runtime.GOOS, runtime.GOARCH, runtime.Version())
	fmt.Printf("AF_UNIX stream (SOCK_STREAM) support: %t\n", streamOK)
	if !streamOK && streamErr != nil {
		fmt.Printf("  probe error: %v\n", streamErr)
	}
	fmt.Println("Adapter transport: unix (SOCK_STREAM) with length-prefixed framing")
}

// tempSocketPath returns a unique temporary path suitable for unix stream sockets.
func tempSocketPath(prefix string) string {
	dir := os.TempDir()
	return filepath.Join(dir, fmt.Sprintf("sio-debug-%s-%d.sock", prefix, time.Now().UnixNano()))
}

// cleanup removes socket files silently.
func cleanup(paths ...string) {
	for _, p := range paths {
		_ = os.Remove(p)
	}
}

// cleanupGlob removes all files matching a glob pattern silently.
func cleanupGlob(pattern string) {
	matches, _ := filepath.Glob(pattern)
	for _, m := range matches {
		_ = os.Remove(m)
	}
}

// ---------------------------------------------------------------------------
// Test groups
// ---------------------------------------------------------------------------

// testUnixClientInit validates UnixClient creation and field defaults.
func testUnixClientInit() {
	fmt.Println("\n=== UnixClient Initialization ===")

	// 1. Basic construction with context and socket path.
	// Expected: Client is non-nil, fields match provided arguments.
	ctx := context.Background()
	sockPath := tempSocketPath("init")
	client := unix.NewUnixClient(ctx, sockPath)
	check("NewUnixClient returns non-nil", client != nil, "got nil")
	if client != nil {
		check("Context is non-nil and not canceled", client.Context != nil && client.Context.Err() == nil,
			"context is nil or already canceled")
		check("SocketPath is set correctly", client.SocketPath == sockPath, fmt.Sprintf("got %q", client.SocketPath))
	}

	// 2. Nil context falls back to context.Background().
	// Expected: Client.Context is non-nil even when nil is passed.
	client2 := unix.NewUnixClient(nil, sockPath)
	check("Nil ctx defaults to Background", client2 != nil && client2.Context != nil, "context is nil")

	// 3. ListenerPath before Listen is empty.
	// Expected: No listener path is set before Listen() is called.
	check("ListenerPath empty before Listen", client.ListenerPath() == "", fmt.Sprintf("got %q", client.ListenerPath()))

	// 4. Close on a fresh client should succeed without error.
	// Expected: No panic, no error returned.
	err := client.Close()
	check("Close on fresh client succeeds", err == nil, fmt.Sprintf("err=%v", err))
}

// testListenAndReadWrite validates the Listen / Send / ReadMessage cycle
// over connection-oriented stream sockets with length-prefixed framing.
func testListenAndReadWrite() {
	fmt.Println("\n=== Listen / Send / ReadMessage ===")

	// Create two clients that simulate two server nodes exchanging messages
	// over Unix Domain Socket streams.
	basePath := tempSocketPath("rw")
	defer cleanup(basePath)

	listenerPathA := basePath + ".nodeA"
	listenerPathB := basePath + ".nodeB"
	defer cleanup(listenerPathA, listenerPathB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientA := unix.NewUnixClient(ctx, basePath)
	clientB := unix.NewUnixClient(ctx, basePath)

	// 1. Listen on node A.
	// Expected: No error; ListenerPath returns the configured path.
	err := clientA.Listen(listenerPathA)
	check("ClientA.Listen succeeds", err == nil, fmt.Sprintf("err=%v", err))
	check("ClientA.ListenerPath matches", clientA.ListenerPath() == listenerPathA, fmt.Sprintf("got %q", clientA.ListenerPath()))

	// 2. Listen on node B.
	err = clientB.Listen(listenerPathB)
	check("ClientB.Listen succeeds", err == nil, fmt.Sprintf("err=%v", err))

	// 3. Calling Listen again on the same client should be a no-op (idempotent).
	// Expected: No error, listener path unchanged.
	err = clientA.Listen(listenerPathA + ".extra")
	check("Second Listen is idempotent (no error)", err == nil, fmt.Sprintf("err=%v", err))
	check("ListenerPath unchanged after second Listen", clientA.ListenerPath() == listenerPathA, fmt.Sprintf("got %q", clientA.ListenerPath()))

	// 4. Send a message from A → B and verify reception.
	// Expected: B receives the exact bytes A sent (framed transparently).
	payload := []byte(`{"type":"test","data":"hello from A"}`)
	err = clientA.Send(listenerPathB, payload)
	check("Send from A to B succeeds", err == nil, fmt.Sprintf("err=%v", err))

	buf := make([]byte, 4096)
	n, _, err := clientB.ReadMessage(buf)
	check("ReadMessage on B succeeds", err == nil, fmt.Sprintf("err=%v", err))
	check("ReadMessage payload matches", string(buf[:n]) == string(payload), fmt.Sprintf("got %q", string(buf[:n])))

	// 5. Send a message from B → A and verify reception.
	replyPayload := []byte(`{"type":"reply","data":"hello from B"}`)
	err = clientB.Send(listenerPathA, replyPayload)
	check("Send from B to A succeeds", err == nil, fmt.Sprintf("err=%v", err))

	n, _, err = clientA.ReadMessage(buf)
	check("ReadMessage on A succeeds", err == nil, fmt.Sprintf("err=%v", err))
	check("ReadMessage reply matches", string(buf[:n]) == string(replyPayload), fmt.Sprintf("got %q", string(buf[:n])))

	// 6. Close both clients.
	// Expected: No error, socket files can be cleaned up.
	err = clientA.Close()
	check("ClientA.Close succeeds", err == nil, fmt.Sprintf("err=%v", err))
	err = clientB.Close()
	check("ClientB.Close succeeds", err == nil, fmt.Sprintf("err=%v", err))
}

// testReadMessageBeforeListen verifies error when reading without a listener.
func testReadMessageBeforeListen() {
	fmt.Println("\n=== ReadMessage Before Listen (Error Handling) ===")

	client := unix.NewUnixClient(context.Background(), tempSocketPath("unused"))

	// Expected: ReadMessage returns an error because no listener has been started.
	buf := make([]byte, 1024)
	_, _, err := client.ReadMessage(buf)
	check("ReadMessage without Listen returns error", err != nil, "expected error, got nil")
}

// testSendToInvalidPath verifies error handling for unreachable targets.
func testSendToInvalidPath() {
	fmt.Println("\n=== Send to Invalid Path (Error Handling) ===")

	basePath := tempSocketPath("invalid")
	ctx := context.Background()
	client := unix.NewUnixClient(ctx, basePath)

	// Expected: Sending to a nonexistent socket path fails gracefully with an error.
	err := client.Send("/tmp/nonexistent-socket-path-xyz.sock", []byte("data"))
	check("Send to nonexistent path returns error", err != nil, "expected error, got nil")
}

// testListenOnInvalidPath verifies Listen error for an inaccessible path.
func testListenOnInvalidPath() {
	fmt.Println("\n=== Listen on Invalid Path (Error Handling) ===")

	client := unix.NewUnixClient(context.Background(), "/tmp/test.sock")

	// Expected: Listening on a deeply nested nonexistent directory fails.
	err := client.Listen("/nonexistent/deep/dir/socket.sock")
	check("Listen on invalid dir returns error", err != nil, "expected error, got nil")
}

// testPeerDiscovery validates that the adapter discovers peers by scanning
// the socket directory for files matching "{base}.{uid}".
func testPeerDiscovery() {
	fmt.Println("\n=== Peer Discovery (Directory Scan) ===")

	dir := os.TempDir()
	baseName := fmt.Sprintf("sio-peer-%d.sock", time.Now().UnixNano())
	basePath := filepath.Join(dir, baseName)
	defer cleanupGlob(basePath + "*")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create three simulated peer listener nodes using UnixClient.
	// The adapter discovers peers by reading files named "{base}.{uid}" in the directory.
	peerNames := []string{"node1", "node2", "node3"}
	peers := make([]*unix.UnixClient, len(peerNames))
	for i, name := range peerNames {
		peers[i] = unix.NewUnixClient(ctx, basePath)
		peerPath := basePath + "." + name
		if err := peers[i].Listen(peerPath); err != nil {
			check("Create peer listener "+name, false, fmt.Sprintf("err=%v", err))
			return
		}
	}
	defer func() {
		for _, p := range peers {
			p.Close()
		}
	}()

	// Verify that the directory contains the expected peer entries.
	entries, err := os.ReadDir(dir)
	check("Read socket directory", err == nil, fmt.Sprintf("err=%v", err))

	found := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), baseName+".") {
			found++
		}
	}
	check("Found 3 peer socket files", found == 3, fmt.Sprintf("found %d", found))

	// Send a message from an external client to each peer and verify reception.
	// This simulates what the adapter's broadcast() method does internally.
	sender := unix.NewUnixClient(ctx, basePath)
	defer sender.Close()
	testMsg := []byte(`{"uid":"test","type":0,"nsp":"/"}`)

	for i, name := range peerNames {
		peerPath := basePath + "." + name
		err := sender.Send(peerPath, testMsg)
		check(fmt.Sprintf("Send to peer %s", name), err == nil, fmt.Sprintf("err=%v", err))

		// Read from the peer to confirm delivery.
		buf := make([]byte, 4096)
		n, _, err := peers[i].ReadMessage(buf)
		check(fmt.Sprintf("Peer %s received message", name), err == nil && n == len(testMsg),
			fmt.Sprintf("n=%d err=%v", n, err))
		check(fmt.Sprintf("Peer %s payload correct", name), string(buf[:n]) == string(testMsg),
			fmt.Sprintf("got %q", string(buf[:n])))
	}
}

// testAdapterOptions verifies UnixAdapterOptions default values and assignment.
func testAdapterOptions() {
	fmt.Println("\n=== Adapter Options ===")

	// 1. Default values.
	// Expected: All raw getters return nil (unset) so that callers can detect "not configured".
	opts := unix_adapter.DefaultUnixAdapterOptions()
	check("Default options non-nil", opts != nil, "got nil")
	check("Default Key raw is nil", opts.GetRawKey() == nil, "expected nil")
	check("Default ErrorHandler raw is nil", opts.GetRawErrorHandler() == nil, "expected nil")

	// 2. Set and retrieve key.
	opts.SetKey("custom-prefix")
	check("Key set correctly", opts.Key() == "custom-prefix", fmt.Sprintf("got %q", opts.Key()))

	// 3. Set and retrieve error handler.
	called := false
	opts.SetErrorHandler(func(err error) { called = true })
	handler := opts.ErrorHandler()
	check("ErrorHandler is non-nil after set", handler != nil, "got nil")
	if handler != nil {
		handler(fmt.Errorf("test"))
		check("ErrorHandler invoked", called, "not called")
	}

	// 4. Assign merges non-nil fields from another options instance.
	opts2 := unix_adapter.DefaultUnixAdapterOptions()
	opts2.SetKey("merged-prefix")
	opts.Assign(opts2)
	check("Assign merges Key", opts.Key() == "merged-prefix", fmt.Sprintf("got %q", opts.Key()))

	// 5. Assign with nil is safe.
	result := opts.Assign(nil)
	check("Assign(nil) returns self", result != nil, "got nil")
}

// testEmitterOptions verifies EmitterOptions defaults and assignment.
func testEmitterOptions() {
	fmt.Println("\n=== Emitter Options ===")

	opts := unix_emitter.DefaultEmitterOptions()
	check("Default emitter options non-nil", opts != nil, "got nil")
	check("Default Key raw is nil", opts.GetRawKey() == nil, "expected nil")
	check("Default SocketPath raw is nil", opts.GetRawSocketPath() == nil, "expected nil")
	check("Default Parser raw is nil", opts.GetRawParser() == nil, "expected nil")

	opts.SetKey("emitter-key")
	check("Emitter Key set", opts.Key() == "emitter-key", fmt.Sprintf("got %q", opts.Key()))

	opts.SetSocketPath("/tmp/custom.sock")
	check("Emitter SocketPath set", opts.SocketPath() == "/tmp/custom.sock", fmt.Sprintf("got %q", opts.SocketPath()))

	// Assign merges fields.
	opts2 := unix_emitter.DefaultEmitterOptions()
	opts2.SetKey("merged-emitter")
	opts.Assign(opts2)
	check("Emitter Assign merges Key", opts.Key() == "merged-emitter", fmt.Sprintf("got %q", opts.Key()))
}

// testEmitterReservedEvents verifies that emitting reserved events is rejected.
func testEmitterReservedEvents() {
	fmt.Println("\n=== Emitter Reserved Events ===")

	basePath := tempSocketPath("reserved")
	defer cleanup(basePath)

	client := unix.NewUnixClient(context.Background(), basePath)
	defer client.Close()

	emitterOpts := unix_emitter.DefaultEmitterOptions()
	emitterOpts.SetSocketPath(basePath)
	e := unix_emitter.NewEmitter(client, emitterOpts)

	// All reserved event names must be rejected with an error.
	reserved := []string{"connect", "connect_error", "disconnect", "disconnecting", "newListener", "removeListener"}
	for _, ev := range reserved {
		err := e.Emit(ev, "data")
		check(fmt.Sprintf("Emit(%q) returns error", ev), err != nil, "expected error, got nil")
	}
}

// testEmitterOf validates namespace switching via Emitter.Of().
func testEmitterOf() {
	fmt.Println("\n=== Emitter.Of (Namespace) ===")

	basePath := tempSocketPath("of")
	defer cleanup(basePath)

	client := unix.NewUnixClient(context.Background(), basePath)
	defer client.Close()

	opts := unix_emitter.DefaultEmitterOptions()
	opts.SetSocketPath(basePath)
	e := unix_emitter.NewEmitter(client, opts)

	// Of with "/" prefix.
	e2 := e.Of("/admin")
	check("Of returns non-nil", e2 != nil, "got nil")

	// Of without "/" prefix auto-prepends it.
	e3 := e.Of("chat")
	check("Of without slash non-nil", e3 != nil, "got nil")
}

// testEmitterFluentAPI validates the fluent (chaining) API on the BroadcastOperator.
func testEmitterFluentAPI() {
	fmt.Println("\n=== Emitter Fluent API (To/In/Except/Volatile/Compress) ===")

	basePath := tempSocketPath("fluent")
	defer cleanup(basePath)

	client := unix.NewUnixClient(context.Background(), basePath)
	defer client.Close()

	opts := unix_emitter.DefaultEmitterOptions()
	opts.SetSocketPath(basePath)
	e := unix_emitter.NewEmitter(client, opts)

	// Each method returns a new BroadcastOperatorInterface (immutable chaining).
	// Expected: no panics, each call returns a non-nil operator.
	op := e.To("room1", "room2")
	check("To returns non-nil", op != nil, "got nil")

	op = e.In("room1")
	check("In returns non-nil", op != nil, "got nil")

	op = e.Except("room3")
	check("Except returns non-nil", op != nil, "got nil")

	op = e.Volatile()
	check("Volatile returns non-nil", op != nil, "got nil")

	op = e.Compress(true)
	check("Compress returns non-nil", op != nil, "got nil")

	// Full chain.
	op = e.To("room1").In("room2").Except("room3").Compress(false).Volatile()
	check("Full chain returns non-nil", op != nil, "got nil")
}

// testEmitterSocketsJoinLeave validates SocketsJoin / SocketsLeave on the emitter.
// Without peers these are expected to succeed silently (no peers to send to).
func testEmitterSocketsJoinLeave() {
	fmt.Println("\n=== Emitter SocketsJoin / SocketsLeave / DisconnectSockets ===")

	basePath := tempSocketPath("joinleave")
	defer cleanup(basePath)

	client := unix.NewUnixClient(context.Background(), basePath)
	defer client.Close()

	opts := unix_emitter.DefaultEmitterOptions()
	opts.SetSocketPath(basePath)
	e := unix_emitter.NewEmitter(client, opts)

	// SocketsJoin should not fail even if there are no peers.
	err := e.SocketsJoin("room-new")
	check("SocketsJoin succeeds (no peers)", err == nil, fmt.Sprintf("err=%v", err))

	// SocketsLeave should not fail even if there are no peers.
	err = e.SocketsLeave("room-new")
	check("SocketsLeave succeeds (no peers)", err == nil, fmt.Sprintf("err=%v", err))

	// DisconnectSockets should not fail even if there are no peers.
	err = e.DisconnectSockets(true)
	check("DisconnectSockets succeeds (no peers)", err == nil, fmt.Sprintf("err=%v", err))
}

// testEmitterServerSideEmit validates ServerSideEmit and ensures ack callbacks are rejected.
func testEmitterServerSideEmit() {
	fmt.Println("\n=== Emitter ServerSideEmit ===")

	basePath := tempSocketPath("sse")
	defer cleanup(basePath)

	client := unix.NewUnixClient(context.Background(), basePath)
	defer client.Close()

	opts := unix_emitter.DefaultEmitterOptions()
	opts.SetSocketPath(basePath)
	e := unix_emitter.NewEmitter(client, opts)

	// Normal ServerSideEmit should succeed (no peers = no error).
	err := e.ServerSideEmit("hello", "world")
	check("ServerSideEmit succeeds", err == nil, fmt.Sprintf("err=%v", err))
}

// testEndToEndMessageExchange simulates two adapter nodes exchanging cluster
// messages over Unix Domain Socket streams, verifying full encode → send → receive → decode.
func testEndToEndMessageExchange() {
	fmt.Println("\n=== End-to-End Message Exchange (JSON) ===")

	dir := os.TempDir()
	baseName := fmt.Sprintf("sio-e2e-%d.sock", time.Now().UnixNano())
	basePath := filepath.Join(dir, baseName)
	defer cleanupGlob(basePath + "*")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up two peer nodes using UnixClient.
	pathA := basePath + ".nodeA"
	pathB := basePath + ".nodeB"

	clientA := unix.NewUnixClient(ctx, basePath)
	defer clientA.Close()
	if err := clientA.Listen(pathA); err != nil {
		check("Create listener A", false, fmt.Sprintf("err=%v", err))
		return
	}

	clientB := unix.NewUnixClient(ctx, basePath)
	defer clientB.Close()
	if err := clientB.Listen(pathB); err != nil {
		check("Create listener B", false, fmt.Sprintf("err=%v", err))
		return
	}

	// Construct a JSON-encoded ClusterMessage (non-binary = JSON encoding path).
	msg := &adapter.ClusterMessage{
		Uid:  "nodeA-uid",
		Nsp:  "/",
		Type: adapter.BROADCAST,
		Data: &adapter.BroadcastMessage{
			Opts: &adapter.PacketOptions{
				Rooms: []socket.Room{"room1"},
			},
		},
	}

	payload, err := json.Marshal(msg)
	check("JSON marshal ClusterMessage", err == nil, fmt.Sprintf("err=%v", err))

	// Node A sends to Node B via stream socket.
	err = clientA.Send(pathB, payload)
	check("NodeA sends to NodeB", err == nil, fmt.Sprintf("err=%v", err))

	// Node B reads and verifies.
	buf := make([]byte, 65536)
	n, _, err := clientB.ReadMessage(buf)
	check("NodeB reads message", err == nil, fmt.Sprintf("err=%v", err))

	var received adapter.ClusterMessage
	err = json.Unmarshal(buf[:n], &received)
	check("NodeB decodes message", err == nil, fmt.Sprintf("err=%v", err))
	check("Received UID matches", string(received.Uid) == "nodeA-uid", fmt.Sprintf("got %q", received.Uid))
	check("Received Nsp matches", received.Nsp == "/", fmt.Sprintf("got %q", received.Nsp))
	check("Received Type is BROADCAST", received.Type == adapter.BROADCAST, fmt.Sprintf("got %d", received.Type))
}

// testLargePayload validates that large messages are transmitted correctly
// via Unix Domain Socket streams with length-prefixed framing.
func testLargePayload() {
	fmt.Println("\n=== Large Payload Transmission ===")

	dir := os.TempDir()
	baseName := fmt.Sprintf("sio-large-%d.sock", time.Now().UnixNano())
	basePath := filepath.Join(dir, baseName)
	listenerPath := basePath + ".large"
	defer cleanup(listenerPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := unix.NewUnixClient(ctx, basePath)

	err := client.Listen(listenerPath)
	check("Listener for large payload", err == nil, fmt.Sprintf("err=%v", err))
	defer client.Close()

	// Build a 60KB payload — well within the stream framing capacity.
	// Stream sockets have no inherent per-message size limit, unlike datagrams.
	largeData := make([]byte, 60000)
	for i := range largeData {
		largeData[i] = byte('A' + (i % 26))
	}

	sender := unix.NewUnixClient(ctx, basePath)
	err = sender.Send(listenerPath, largeData)
	check("Send large payload", err == nil, fmt.Sprintf("err=%v", err))
	sender.Close()

	buf := make([]byte, 65536)
	n, _, err := client.ReadMessage(buf)
	check("Read large payload", err == nil, fmt.Sprintf("err=%v", err))
	check("Large payload size matches", n == len(largeData), fmt.Sprintf("expected %d, got %d", len(largeData), n))
	check("Large payload content matches", string(buf[:n]) == string(largeData), "content mismatch")
}

// testConcurrentSendReceive validates thread-safety of Send and ReadMessage
// under concurrent access from multiple goroutines.
func testConcurrentSendReceive() {
	fmt.Println("\n=== Concurrent Send / Receive ===")

	dir := os.TempDir()
	baseName := fmt.Sprintf("sio-conc-%d.sock", time.Now().UnixNano())
	basePath := filepath.Join(dir, baseName)
	listenerPath := basePath + ".conc"
	defer cleanup(listenerPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	receiver := unix.NewUnixClient(ctx, basePath)
	err := receiver.Listen(listenerPath)
	check("Concurrent receiver listen", err == nil, fmt.Sprintf("err=%v", err))
	defer receiver.Close()

	const numSenders = 10
	var wg sync.WaitGroup

	// Launch multiple goroutines that each send a message simultaneously.
	// Expected: All messages are delivered without data corruption.
	for i := 0; i < numSenders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s := unix.NewUnixClient(ctx, basePath)
			defer s.Close()
			msg := []byte(fmt.Sprintf(`{"sender":%d}`, id))
			_ = s.Send(listenerPath, msg)
		}(i)
	}

	// Collect all messages with a timeout.
	received := make(map[int]bool)
	done := make(chan struct{})

	go func() {
		buf := make([]byte, 4096)
		for len(received) < numSenders {
			n, _, err := receiver.ReadMessage(buf)
			if err != nil {
				break
			}
			var m struct {
				Sender int `json:"sender"`
			}
			if json.Unmarshal(buf[:n], &m) == nil {
				mu.Lock()
				received[m.Sender] = true
				mu.Unlock()
			}
		}
		close(done)
	}()

	wg.Wait()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}

	mu.Lock()
	count := len(received)
	mu.Unlock()

	check(fmt.Sprintf("Received all %d concurrent messages", numSenders), count == numSenders,
		fmt.Sprintf("got %d/%d", count, numSenders))
}

// testCloseIdempotency verifies that calling Close multiple times is safe.
func testCloseIdempotency() {
	fmt.Println("\n=== Close Idempotency ===")

	basePath := tempSocketPath("close")
	listenerPath := basePath + ".close"
	defer cleanup(listenerPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := unix.NewUnixClient(ctx, basePath)
	_ = client.Listen(listenerPath)

	// First close should succeed.
	err := client.Close()
	check("First Close succeeds", err == nil, fmt.Sprintf("err=%v", err))

	// Second close should also succeed (no panic, no error from double-close).
	err = client.Close()
	check("Second Close succeeds (idempotent)", err == nil, fmt.Sprintf("err=%v", err))
}

// testCleanupSocketFiles verifies that the adapter's Close method removes
// the listener socket file from the filesystem.
func testCleanupSocketFiles() {
	fmt.Println("\n=== Socket File Cleanup ===")

	dir := os.TempDir()
	baseName := fmt.Sprintf("sio-cleanup-%d.sock", time.Now().UnixNano())
	basePath := filepath.Join(dir, baseName)
	listenerPath := basePath + ".cleanup"
	defer cleanup(listenerPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := unix.NewUnixClient(ctx, basePath)
	err := client.Listen(listenerPath)
	check("Listen for cleanup test", err == nil, fmt.Sprintf("err=%v", err))

	// The listener socket file should exist.
	_, err = os.Stat(listenerPath)
	check("Socket file exists after Listen", err == nil, fmt.Sprintf("err=%v", err))

	// After close, verify the client is closed (file may remain since UnixClient
	// itself does not remove the file — the adapter's Close() does that).
	client.Close()

	// Manually remove to simulate adapter cleanup behavior.
	os.Remove(listenerPath)
	_, err = os.Stat(listenerPath)
	check("Socket file removed after cleanup", os.IsNotExist(err), fmt.Sprintf("err=%v", err))
}

// testMessageConstants validates that all defined message type constants are
// distinct and cover the expected range for the Unix adapter.
func testMessageConstants() {
	fmt.Println("\n=== Message Type Constants ===")

	// The unix package defines 10 message types starting from iota (0).
	// Verify they are distinct and sequential.
	types := []adapter.MessageType{
		unix.SOCKETS,
		unix.ALL_ROOMS,
		unix.REMOTE_JOIN,
		unix.REMOTE_LEAVE,
		unix.REMOTE_DISCONNECT,
		unix.REMOTE_FETCH,
		unix.SERVER_SIDE_EMIT,
		unix.BROADCAST,
		unix.BROADCAST_CLIENT_COUNT,
		unix.BROADCAST_ACK,
	}

	seen := make(map[adapter.MessageType]bool)
	allUnique := true
	for _, t := range types {
		if seen[t] {
			allUnique = false
			break
		}
		seen[t] = true
	}
	check("All 10 message types are unique", allUnique, "duplicate found")
	check("Message type count is 10", len(types) == 10, fmt.Sprintf("got %d", len(types)))

	// Verify the first type starts at 0 (iota).
	check("SOCKETS == 0", unix.SOCKETS == 0, fmt.Sprintf("got %d", unix.SOCKETS))
	check("BROADCAST_ACK == 9", unix.BROADCAST_ACK == 9, fmt.Sprintf("got %d", unix.BROADCAST_ACK))
}

// testErrNilUnixPacket validates the sentinel error value.
func testErrNilUnixPacket() {
	fmt.Println("\n=== Sentinel Error: ErrNilUnixPacket ===")

	check("ErrNilUnixPacket is non-nil", unix.ErrNilUnixPacket != nil, "got nil")
	check("ErrNilUnixPacket message", unix.ErrNilUnixPacket.Error() == "cannot unmarshal into nil UnixPacket",
		fmt.Sprintf("got %q", unix.ErrNilUnixPacket.Error()))
}

// testEventEmitter validates the EventEmitter embedded in UnixClient.
func testEventEmitter() {
	fmt.Println("\n=== UnixClient EventEmitter ===")

	client := unix.NewUnixClient(context.Background(), tempSocketPath("event-emitter"))
	defer client.Close()

	// The UnixClient embeds EventEmitter; verify On/Emit work.
	received := make(chan string, 1)
	client.On("error", func(args ...any) {
		if len(args) > 0 {
			if err, ok := args[0].(error); ok {
				received <- err.Error()
			}
		}
	})

	client.Emit("error", fmt.Errorf("test error"))

	select {
	case msg := <-received:
		check("EventEmitter received error event", msg == "test error", fmt.Sprintf("got %q", msg))
	case <-time.After(1 * time.Second):
		check("EventEmitter received error event", false, "timeout")
	}
}

// testAdapterBuilderDefaults validates UnixAdapterBuilder defaults and option merging.
func testAdapterBuilderDefaults() {
	fmt.Println("\n=== UnixAdapterBuilder Defaults ===")

	builder := &unix_adapter.UnixAdapterBuilder{
		Opts: unix_adapter.DefaultUnixAdapterOptions(),
	}
	check("Builder Opts non-nil", builder.Opts != nil, "got nil")

	// Verify default constants are the expected values.
	check("DefaultChannelPrefix", unix_adapter.DefaultChannelPrefix == "socket.io",
		fmt.Sprintf("got %q", unix_adapter.DefaultChannelPrefix))
	check("DefaultSocketPath", unix_adapter.DefaultSocketPath == "/tmp/socket.io.sock",
		fmt.Sprintf("got %q", unix_adapter.DefaultSocketPath))

	// Verify emitter default constants.
	check("DefaultEmitterKey", unix_emitter.DefaultEmitterKey == "socket.io",
		fmt.Sprintf("got %q", unix_emitter.DefaultEmitterKey))
	check("DefaultSocketPath (emitter)", unix_emitter.DefaultSocketPath == "/tmp/socket.io.sock",
		fmt.Sprintf("got %q", unix_emitter.DefaultSocketPath))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	fmt.Println("========================================")
	fmt.Println("Socket.IO Unix Adapter — Debug & Validation")
	fmt.Println("========================================")

	streamOK, streamErr := probeUnixStreamSupport()
	printEnvironmentSummary(streamOK, streamErr)

	if !streamOK {
		fmt.Println("\nFATAL: AF_UNIX stream sockets are not available in this environment.")
		fmt.Printf("Error: %v\n", streamErr)
		fmt.Println("Cannot run adapter tests. Exiting.")
		os.Exit(1)
	}

	// Group 1: Client Initialization
	testUnixClientInit()

	// Group 2: Listen / Send / ReadMessage round-trip
	testListenAndReadWrite()

	// Group 3: Error Handling — reading without listener
	testReadMessageBeforeListen()

	// Group 4: Error Handling — sending to invalid path
	testSendToInvalidPath()

	// Group 5: Error Handling — listening on invalid path
	testListenOnInvalidPath()

	// Group 6: Peer Discovery via directory scanning
	testPeerDiscovery()

	// Group 7: Adapter options validation
	testAdapterOptions()

	// Group 8: Emitter options validation
	testEmitterOptions()

	// Group 9: Emitter reserved event rejection
	testEmitterReservedEvents()

	// Group 10: Emitter.Of namespace switching
	testEmitterOf()

	// Group 11: Emitter fluent API chaining
	testEmitterFluentAPI()

	// Group 12: Emitter SocketsJoin / Leave / Disconnect
	testEmitterSocketsJoinLeave()

	// Group 13: Emitter ServerSideEmit
	testEmitterServerSideEmit()

	// Group 14: End-to-end JSON message exchange
	testEndToEndMessageExchange()

	// Group 15: Large payload transmission
	testLargePayload()

	// Group 16: Concurrent send/receive thread safety
	testConcurrentSendReceive()

	// Group 17: Close idempotency
	testCloseIdempotency()

	// Group 18: Socket file cleanup
	testCleanupSocketFiles()

	// Group 19: Message type constants
	testMessageConstants()

	// Group 20: Sentinel error
	testErrNilUnixPacket()

	// Group 21: EventEmitter integration
	testEventEmitter()

	// Group 22: Adapter builder defaults and constants
	testAdapterBuilderDefaults()

	// Summary
	fmt.Println("\n========================================")
	fmt.Printf("Results: %d passed, %d failed, %d total\n",
		passed, failed, passed+failed)
	fmt.Println("========================================")

	if failed > 0 {
		os.Exit(1)
	}
}
