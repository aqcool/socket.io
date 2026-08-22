#!/usr/bin/env python3
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
MARKER = ROOT / ".native-v4-core-finalized"
if MARKER.exists():
    raise SystemExit(0)


def replace_once(path: Path, old: str, new: str):
    text = path.read_text(encoding="utf-8")
    if old not in text:
        raise RuntimeError(f"expected snippet not found in {path}: {old[:120]!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def regex_once(path: Path, pattern: str, repl: str):
    text = path.read_text(encoding="utf-8")
    updated, count = re.subn(pattern, repl, text, count=1, flags=re.S | re.M)
    if count != 1:
        raise RuntimeError(f"pattern matched {count} times in {path}: {pattern[:120]!r}")
    path.write_text(updated, encoding="utf-8")


opts = ROOT / "servers/socket/server-options.go"
replace_once(opts, "type (\n\tConnectionStateRecoveryInterface interface {", "type TaskQueueOverflowPolicy uint8\n\nconst (\n\tTaskQueueOverflowDisconnect TaskQueueOverflowPolicy = iota\n\tTaskQueueOverflowDropNewest\n\tTaskQueueOverflowReject\n)\n\ntype (\n\tConnectionStateRecoveryInterface interface {")
replace_once(opts, "\t\tSetCleanupEmptyChildNamespaces(bool)\n\t\tGetRawCleanupEmptyChildNamespaces() types.Optional[bool]\n\t\tCleanupEmptyChildNamespaces() bool\n\t}", "\t\tSetCleanupEmptyChildNamespaces(bool)\n\t\tGetRawCleanupEmptyChildNamespaces() types.Optional[bool]\n\t\tCleanupEmptyChildNamespaces() bool\n\n\t\tSetTaskQueueMaxPending(int)\n\t\tTaskQueueMaxPending() int\n\t\tSetTaskQueueOverflowPolicy(TaskQueueOverflowPolicy)\n\t\tTaskQueueOverflowPolicy() TaskQueueOverflowPolicy\n\t}")
replace_once(opts, "\t\t// Whether to remove child namespaces that have no sockets connected to them\n\t\tcleanupEmptyChildNamespaces types.Optional[bool]\n\t}", "\t\t// Whether to remove child namespaces that have no sockets connected to them\n\t\tcleanupEmptyChildNamespaces types.Optional[bool]\n\n\t\ttaskQueueMaxPending      int\n\t\ttaskQueueOverflowPolicy TaskQueueOverflowPolicy\n\t}")
replace_once(opts, "func DefaultServerOptions() *ServerOptions {\n\treturn &ServerOptions{}\n}", "func DefaultServerOptions() *ServerOptions {\n\treturn &ServerOptions{\n\t\ttaskQueueMaxPending:      4096,\n\t\ttaskQueueOverflowPolicy: TaskQueueOverflowDisconnect,\n\t}\n}")
replace_once(opts, "\tif data.GetRawCleanupEmptyChildNamespaces() != nil {\n\t\ts.SetCleanupEmptyChildNamespaces(data.CleanupEmptyChildNamespaces())\n\t}\n\n\treturn s", "\tif data.GetRawCleanupEmptyChildNamespaces() != nil {\n\t\ts.SetCleanupEmptyChildNamespaces(data.CleanupEmptyChildNamespaces())\n\t}\n\ts.SetTaskQueueMaxPending(data.TaskQueueMaxPending())\n\ts.SetTaskQueueOverflowPolicy(data.TaskQueueOverflowPolicy())\n\n\treturn s")
with opts.open("a", encoding="utf-8") as f:
    f.write("\n\nfunc (s *ServerOptions) SetTaskQueueMaxPending(maxPending int) {\n\tif maxPending < 0 {\n\t\tmaxPending = 0\n\t}\n\ts.taskQueueMaxPending = maxPending\n}\n\nfunc (s *ServerOptions) TaskQueueMaxPending() int {\n\treturn s.taskQueueMaxPending\n}\n\nfunc (s *ServerOptions) SetTaskQueueOverflowPolicy(policy TaskQueueOverflowPolicy) {\n\tif policy > TaskQueueOverflowReject {\n\t\tpolicy = TaskQueueOverflowDisconnect\n\t}\n\ts.taskQueueOverflowPolicy = policy\n}\n\nfunc (s *ServerOptions) TaskQueueOverflowPolicy() TaskQueueOverflowPolicy {\n\treturn s.taskQueueOverflowPolicy\n}\n")

socket = ROOT / "servers/socket/socket.go"
replace_once(socket, "\tRECOVERABLE_DISCONNECT_REASONS = types.NewSet(\"transport error\", \"transport close\", \"forced close\", \"ping timeout\", \"server shutting down\", \"forced server close\")\n)", "\tRECOVERABLE_DISCONNECT_REASONS = types.NewSet(\"transport error\", \"transport close\", \"forced close\", \"ping timeout\", \"server shutting down\", \"forced server close\")\n\tErrTaskQueueOverflow           = errors.New(\"socket.io: inbound event queue overflow\")\n)")
replace_once(socket, "\t\tflags                 atomic.Pointer[BroadcastFlags]\n\t\t_anyListeners", "\t\tflags                 atomic.Pointer[BroadcastFlags]\n\t\tflagsMu               sync.Mutex\n\t\t_anyListeners")
replace_once(socket, "\ts.server = nsp.Server()\n\ts.adapter = s.nsp.Adapter()", "\ts.server = nsp.Server()\n\ts.adapter = s.nsp.Adapter()\n\tif s.server != nil && s.server.Opts() != nil {\n\t\ts.taskQueue.SetMaxPending(s.server.Opts().TaskQueueMaxPending())\n\t}")
replace_once(socket, "\tflags := *s.flags.Swap(&BroadcastFlags{})", "\tflags := s.takeFlags()")
replace_once(socket, "// Emits to this client.\n//", "func (s *Socket) updateFlags(update func(*BroadcastFlags)) *Socket {\n\ts.flagsMu.Lock()\n\tdefer s.flagsMu.Unlock()\n\tflags := s.flags.Load()\n\tif flags == nil {\n\t\tflags = &BroadcastFlags{}\n\t\ts.flags.Store(flags)\n\t}\n\tupdate(flags)\n\treturn s\n}\n\nfunc (s *Socket) takeFlags() BroadcastFlags {\n\ts.flagsMu.Lock()\n\tdefer s.flagsMu.Unlock()\n\tflags := s.flags.Swap(&BroadcastFlags{})\n\tif flags == nil {\n\t\treturn BroadcastFlags{}\n\t}\n\treturn *flags\n}\n\n// Emits to this client.\n//")
regex_once(socket, r"func \(s \*Socket\) Enqueue\(task func\(\)\) \{\n\s*s\.taskQueue\.Enqueue\(task\)\n\}", "func (s *Socket) Enqueue(task func()) {\n\tresult := s.taskQueue.TryEnqueue(task)\n\tif result != queue.EnqueueFull {\n\t\treturn\n\t}\n\n\tpolicy := TaskQueueOverflowDisconnect\n\tif s.server != nil && s.server.Opts() != nil {\n\t\tpolicy = s.server.Opts().TaskQueueOverflowPolicy()\n\t}\n\tswitch policy {\n\tcase TaskQueueOverflowDropNewest:\n\t\tsocketLog.Debug(\"dropping inbound event for socket %s: queue full\", s.id)\n\tcase TaskQueueOverflowReject:\n\t\ts._onerror(ErrTaskQueueOverflow)\n\tdefault:\n\t\tsocketLog.Debug(\"disconnecting socket %s: inbound event queue full\", s.id)\n\t\tgo s.Disconnect(true)\n\t}\n}")
regex_once(socket, r"func \(s \*Socket\) Compress\(compress bool\) \*Socket \{\n\s*s\.flags\.Load\(\)\.Compress = &compress\n\s*return s\n\}", "func (s *Socket) Compress(compress bool) *Socket {\n\treturn s.updateFlags(func(flags *BroadcastFlags) {\n\t\tflags.Compress = &compress\n\t})\n}")
regex_once(socket, r"func \(s \*Socket\) Volatile\(\) \*Socket \{\n\s*s\.flags\.Load\(\)\.Volatile = true\n\s*return s\n\}", "func (s *Socket) Volatile() *Socket {\n\treturn s.updateFlags(func(flags *BroadcastFlags) {\n\t\tflags.Volatile = true\n\t})\n}")
regex_once(socket, r"func \(s \*Socket\) Timeout\(timeout time\.Duration\) \*Socket \{\n\s*s\.flags\.Load\(\)\.Timeout = &timeout\n\s*return s\n\}", "func (s *Socket) Timeout(timeout time.Duration) *Socket {\n\treturn s.updateFlags(func(flags *BroadcastFlags) {\n\t\tflags.Timeout = &timeout\n\t})\n}")
replace_once(socket, "\ts.leaveAll()\n\ts.nsp.Remove(s)\n\t// Clear pending ack callbacks", "\ts.leaveAll()\n\ts.nsp.Remove(s)\n\ts.markConnectReady()\n\t// Clear pending ack callbacks")
replace_once(socket, "\tflags := *s.flags.Swap(&BroadcastFlags{})\n\treturn NewBroadcastOperator", "\tflags := s.takeFlags()\n\treturn NewBroadcastOperator")

namespace = ROOT / "servers/socket/namespace.go"
replace_once(namespace, "\tname    string\n\tsockets *types.Map[SocketId, *Socket]\n\n\tadapter Adapter", "\tname              string\n\tsockets           *types.Map[SocketId, *Socket]\n\tpreConnectSockets *types.Map[SocketId, *Socket]\n\n\tadapter Adapter")
replace_once(namespace, "\t\tsockets:  &types.Map[SocketId, *Socket]{},\n\t\t_fns:", "\t\tsockets:           &types.Map[SocketId, *Socket]{},\n\t\tpreConnectSockets: &types.Map[SocketId, *Socket]{},\n\t\t_fns:")
replace_once(namespace, "\tsocket := n._createSocket(client, auth)\n\tif connectionStateRecovery", "\tsocket := n._createSocket(client, auth)\n\tn.preConnectSockets.Store(socket.Id(), socket)\n\t_ = socket.Conn().Once(\"close\", func(...any) {\n\t\tif _, pending := n.preConnectSockets.Load(socket.Id()); pending {\n\t\t\tsocket._cleanup()\n\t\t}\n\t})\n\tif connectionStateRecovery")
replace_once(namespace, "\t// track socket\n\tn.sockets.Store(socket.Id(), socket)", "\t// move the Socket from pre-connect tracking into the connected registry.\n\tn.preConnectSockets.Delete(socket.Id())\n\tn.sockets.Store(socket.Id(), socket)")
replace_once(namespace, "func (n *namespace) Remove(socket *Socket) {\n\tif _, ok := n.sockets.LoadAndDelete(socket.Id()); !ok {", "func (n *namespace) Remove(socket *Socket) {\n\tn.preConnectSockets.Delete(socket.Id())\n\tif _, ok := n.sockets.LoadAndDelete(socket.Id()); !ok {")

server = ROOT / "servers/socket/server.go"
replace_once(server, "\t\t// dynamicNamespaceMu makes creation of a child namespace atomic when\n\t\t// several clients concurrently match the same parent namespace.\n\t\tdynamicNamespaceMu sync.Mutex", "\t\tnamespaceMu sync.Mutex\n\n\t\t// dynamicNamespaceMu makes creation of a child namespace atomic when\n\t\t// several clients concurrently match the same parent namespace.\n\t\tdynamicNamespaceMu sync.Mutex")
replace_once(server, "\t\tserverLog.Debug(\"initializing namespace %s\", n)\n\t\tnamespace = NewNamespace(s, n)\n\t\ts._nsps.Store(n, namespace)\n\t\tif n != \"/\" {\n\t\t\ts.sockets.EmitReserved(\"new_namespace\", namespace)\n\t\t}\n", "\t\tcreated := false\n\t\ts.namespaceMu.Lock()\n\t\tif existing, exists := s._nsps.Load(n); exists {\n\t\t\tnamespace = existing\n\t\t} else {\n\t\t\tserverLog.Debug(\"initializing namespace %s\", n)\n\t\t\tnamespace = NewNamespace(s, n)\n\t\t\ts._nsps.Store(n, namespace)\n\t\t\tcreated = true\n\t\t}\n\t\ts.namespaceMu.Unlock()\n\t\tif created && n != \"/\" {\n\t\t\ts.sockets.EmitReserved(\"new_namespace\", namespace)\n\t\t}\n")

public_server = ROOT / "server.go"
replace_once(public_server, "\tcoreOptions.SetCleanupEmptyChildNamespaces(cfg.CleanupEmptyChildNamespaces)\n", "\tcoreOptions.SetCleanupEmptyChildNamespaces(cfg.CleanupEmptyChildNamespaces)\n\tcoreOptions.SetTaskQueueMaxPending(cfg.Queue.MaxPending)\n\tcoreOptions.SetTaskQueueOverflowPolicy(core.TaskQueueOverflowPolicy(cfg.Queue.Overflow))\n")

(ROOT / "pkg/queue/queue_bounded_test.go").write_text('''package queue\n\nimport (\n\t"testing"\n\t"time"\n)\n\nfunc TestBoundedQueueRejectsOverflow(t *testing.T) {\n\tq := NewBounded(1)\n\tt.Cleanup(q.TryClose)\n\trelease := make(chan struct{})\n\tstarted := make(chan struct{})\n\tif got := q.TryEnqueue(func() { close(started); <-release }); got != EnqueueAccepted {\n\t\tt.Fatalf("first enqueue = %v", got)\n\t}\n\t<-started\n\tif got := q.TryEnqueue(func() {}); got != EnqueueAccepted {\n\t\tt.Fatalf("pending enqueue = %v", got)\n\t}\n\tif got := q.TryEnqueue(func() {}); got != EnqueueFull {\n\t\tt.Fatalf("overflow enqueue = %v, want full", got)\n\t}\n\tif q.OverflowCount() != 1 {\n\t\tt.Fatalf("overflow count = %d, want 1", q.OverflowCount())\n\t}\n\tclose(release)\n}\n\nfunc TestQueueCloseDrainsAcceptedWork(t *testing.T) {\n\tq := NewBounded(4)\n\tcompleted := make(chan struct{}, 2)\n\tq.Enqueue(func() { completed <- struct{}{} })\n\tq.Enqueue(func() { completed <- struct{}{} })\n\tdone := make(chan struct{})\n\tgo func() { q.Close(); close(done) }()\n\tselect {\n\tcase <-done:\n\tcase <-time.After(time.Second):\n\t\tt.Fatal("queue did not drain")\n\t}\n\tif len(completed) != 2 {\n\t\tt.Fatalf("completed = %d, want 2", len(completed))\n\t}\n}\n''', encoding="utf-8")

(ROOT / "servers/socket/v4_native_concurrency_test.go").write_text('''package socket\n\nimport (\n\t"sync"\n\t"testing"\n\t"time"\n)\n\nfunc TestV4CoreConcurrentNamespaceCreationIsAtomic(t *testing.T) {\n\tserver := NewServer(nil, nil)\n\tt.Cleanup(func() { server.Close(nil) })\n\tconst callers = 64\n\tresults := make(chan Namespace, callers)\n\tvar wg sync.WaitGroup\n\twg.Add(callers)\n\tfor range callers {\n\t\tgo func() { defer wg.Done(); results <- server.Of("/atomic", nil) }()\n\t}\n\twg.Wait()\n\tclose(results)\n\tvar first Namespace\n\tfor nsp := range results {\n\t\tif first == nil { first = nsp; continue }\n\t\tif nsp != first { t.Fatalf("different namespace instances: %p != %p", nsp, first) }\n\t}\n}\n\nfunc TestV4CoreSocketTransientFlagsAreRaceSafe(t *testing.T) {\n\tsocket := MakeSocket()\n\tconst workers = 64\n\tvar wg sync.WaitGroup\n\twg.Add(workers)\n\tfor i := range workers {\n\t\tgo func(i int) { defer wg.Done(); socket.Volatile().Compress(i%2 == 0).Timeout(time.Duration(i+1) * time.Millisecond) }(i)\n\t}\n\twg.Wait()\n\t_ = socket.takeFlags()\n}\n''', encoding="utf-8")

MARKER.write_text("Native v4 core concurrency/lifecycle hardening applied.\n", encoding="utf-8")
