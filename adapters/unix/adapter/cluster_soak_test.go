package adapter

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	enginetransports "github.com/aqcool/socket.io/servers/engine/v4/transports"
	"github.com/aqcool/socket.io/sticky/v4"
)

const clusterSoakEnv = "SOCKET_IO_CLUSTER_SOAK"

type partitionProxy struct {
	server      *http.Server
	listener    net.Listener
	proxy       *httputil.ReverseProxy
	transport   *http.Transport
	partitioned atomic.Bool
	connections sync.Map
	rejected    atomic.Uint64
	closed      atomic.Uint64
}

type partitionTrackedConn struct {
	net.Conn
	owner *partitionProxy
	once  sync.Once
}

func (c *partitionTrackedConn) Close() error {
	c.once.Do(func() { c.owner.connections.Delete(c) })
	return c.Conn.Close()
}

type partitionTrackingListener struct {
	net.Listener
	owner *partitionProxy
}

func (l *partitionTrackingListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tracked := &partitionTrackedConn{Conn: connection, owner: l.owner}
	l.owner.connections.Store(tracked, struct{}{})
	return tracked, nil
}

type partitionProxyStats struct {
	RejectedRequests  uint64
	ForcedConnections uint64
}

func newPartitionProxy(t *testing.T, targetAddress string) *partitionProxy {
	t.Helper()
	target, err := url.Parse("http://" + targetAddress)
	if err != nil {
		t.Fatal(err)
	}
	gate := &partitionProxy{proxy: httputil.NewSingleHostReverseProxy(target)}
	gate.transport = http.DefaultTransport.(*http.Transport).Clone()
	gate.proxy.Transport = gate.transport
	gate.proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gate.listener = &partitionTrackingListener{Listener: listener, owner: gate}
	gate.server = &http.Server{Handler: gate, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = gate.server.Serve(gate.listener) }()
	t.Cleanup(gate.close)
	return gate
}

func (p *partitionProxy) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if p.partitioned.Load() {
		p.rejected.Add(1)
		if hijacker, ok := w.(http.Hijacker); ok {
			connection, _, err := hijacker.Hijack()
			if err == nil {
				_ = connection.Close()
				return
			}
		}
		http.Error(w, "network partition", http.StatusServiceUnavailable)
		return
	}
	p.proxy.ServeHTTP(w, request)
}

func (p *partitionProxy) address() string { return p.listener.Addr().String() }

func (p *partitionProxy) setPartitioned(partitioned bool) {
	previous := p.partitioned.Swap(partitioned)
	if !partitioned || previous {
		return
	}
	p.transport.CloseIdleConnections()
	connections := make([]net.Conn, 0)
	p.connections.Range(func(connection, _ any) bool {
		connections = append(connections, connection.(net.Conn))
		return true
	})
	for _, connection := range connections {
		if err := connection.Close(); err == nil {
			p.closed.Add(1)
		}
	}
}

func (p *partitionProxy) stats() partitionProxyStats {
	return partitionProxyStats{
		RejectedRequests:  p.rejected.Load(),
		ForcedConnections: p.closed.Load(),
	}
}

func (p *partitionProxy) close() {
	p.setPartitioned(true)
	_ = p.server.Close()
	p.transport.CloseIdleConnections()
}

func TestPartitionProxyCutsAndRestoresNetwork(t *testing.T) {
	backend := http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})}
	backendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = backend.Serve(backendListener) }()
	t.Cleanup(func() { _ = backend.Close() })

	gate := newPartitionProxy(t, backendListener.Addr().String())
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + gate.address() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("healthy proxy status = %d, want 200", response.StatusCode)
	}

	gate.setPartitioned(true)
	if response, err = client.Get("http://" + gate.address() + "/health"); err == nil {
		_ = response.Body.Close()
		t.Fatal("partitioned proxy unexpectedly returned an HTTP response")
	}
	if stats := gate.stats(); stats.RejectedRequests == 0 {
		t.Fatalf("partition stats = %+v, want a rejected request", stats)
	}

	gate.setPartitioned(false)
	response, err = client.Get("http://" + gate.address() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("restored proxy status = %d, want 200", response.StatusCode)
	}
}

type workerResources struct {
	Goroutines       uint64 `json:"goroutines"`
	HeapAlloc        uint64 `json:"heapAlloc"`
	HeapObjects      uint64 `json:"heapObjects"`
	StackInuse       uint64 `json:"stackInuse"`
	OpenFDs          uint64 `json:"openFDs"`
	SocketClients    uint64 `json:"socketClients"`
	EngineClients    uint64 `json:"engineClients"`
	AdapterRooms     uint64 `json:"adapterRooms"`
	AdapterSids      uint64 `json:"adapterSids"`
	RawEngineClients uint64 `json:"rawEngineClients"`
}

type soakProcessResources struct {
	Goroutines  uint64
	HeapAlloc   uint64
	HeapObjects uint64
	StackInuse  uint64
	OpenFDs     uint64
}

type timedWorkerResourceSample struct {
	At        time.Time
	Resources workerResources
}

func absoluteDifference(left, right uint64) uint64 {
	if left >= right {
		return left - right
	}
	return right - left
}

type soakJSONRecord struct {
	Type   string          `json:"type"`
	Phase  string          `json:"phase"`
	State  string          `json:"state"`
	Status string          `json:"status"`
	Error  string          `json:"error"`
	Raw    json.RawMessage `json:"-"`
}

type soakOutputLog struct {
	mu      sync.Mutex
	builder strings.Builder
}

func (l *soakOutputLog) append(line string) {
	_, _ = l.Write([]byte(line + "\n"))
}

func (l *soakOutputLog) Write(payload []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.builder.Write(payload)
}

func (l *soakOutputLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.builder.String()
}

type soakResult struct {
	Type                  string `json:"type"`
	DurationMS            int64  `json:"durationMs"`
	ClientCount           int    `json:"clientCount"`
	Connects              int    `json:"connects"`
	Disconnects           int    `json:"disconnects"`
	AckAttempts           int    `json:"ackAttempts"`
	AckSuccess            int    `json:"ackSuccess"`
	AckTimeouts           int    `json:"ackTimeouts"`
	DuplicateAcks         int    `json:"duplicateAcks"`
	AckMismatches         int    `json:"ackMismatches"`
	DuplicateEvents       int    `json:"duplicateEvents"`
	UnknownEvents         int    `json:"unknownEvents"`
	FinalConnected        int    `json:"finalConnected"`
	FinalRecoveredClients int    `json:"finalRecoveredClients"`
	PhasesRequired        bool   `json:"phasesRequired"`
	TransportClasses      map[string]struct {
		Clients           int `json:"clients"`
		Connected         int `json:"connected"`
		PollingObserved   int `json:"pollingObserved"`
		WebSocketObserved int `json:"websocketObserved"`
		UpgradedClients   int `json:"upgradedClients"`
	} `json:"transportClasses"`
	Broadcasts struct {
		DuplicateCallbacks  int     `json:"duplicateCallbacks"`
		DuplicateDeliveries int     `json:"duplicateDeliveries"`
		UnknownDeliveries   int     `json:"unknownDeliveries"`
		DeliveryRatio       float64 `json:"deliveryRatio"`
	} `json:"broadcasts"`
	Phases map[string]struct {
		TargetClientIndexes   []int `json:"targetClientIndexes"`
		AffectedClientIndexes []int `json:"affectedClientIndexes"`
		Recovery              struct {
			Status                         string `json:"status"`
			TargetClients                  []int  `json:"targetClients"`
			AffectedClients                []int  `json:"affectedClients"`
			RequiredAffectedClients        int    `json:"requiredAffectedClients"`
			ExpectedRecoveredNode          string `json:"expectedRecoveredNode"`
			ClientsAssignedToRecoveredNode []int  `json:"clientsAssignedToRecoveredNode"`
		} `json:"recovery"`
	} `json:"phases"`
	Shutdown struct {
		DisconnectedClients int `json:"disconnectedClients"`
		ClosedEngines       int `json:"closedEngines"`
		EngineCount         int `json:"engineCount"`
		AgentHandles        struct {
			Active int `json:"active"`
			Free   int `json:"free"`
			Queued int `json:"queued"`
		} `json:"agentHandles"`
	} `json:"shutdown"`
}

var soakHTTPClient = &http.Client{Timeout: 5 * time.Second}

func fetchWorkerResources(worker *clusterWorker) (workerResources, error) {
	response, err := soakHTTPClient.Get("http://" + worker.address + "/debug/resources")
	if err != nil {
		return workerResources{}, fmt.Errorf("read resources from %s: %w", worker.id, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return workerResources{}, fmt.Errorf("read resources from %s: status %s", worker.id, response.Status)
	}
	var resources workerResources
	if err := json.NewDecoder(response.Body).Decode(&resources); err != nil {
		return workerResources{}, fmt.Errorf("decode resources from %s: %w", worker.id, err)
	}
	return resources, nil
}

func readWorkerResources(t *testing.T, worker *clusterWorker) workerResources {
	t.Helper()
	resources, err := fetchWorkerResources(worker)
	if err != nil {
		t.Fatal(err)
	}
	return resources
}

func currentSoakProcessResources() soakProcessResources {
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return soakProcessResources{
		Goroutines:  uint64(runtime.NumGoroutine()),
		HeapAlloc:   memory.HeapAlloc,
		HeapObjects: memory.HeapObjects,
		StackInuse:  memory.StackInuse,
		OpenFDs:     clusterWorkerOpenFileDescriptors(),
	}
}

func waitForSoakRecord(records <-chan soakJSONRecord, timeout time.Duration, match func(soakJSONRecord) bool) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case record, ok := <-records:
			if !ok {
				return fmt.Errorf("soak process output closed before the expected record")
			}
			if record.Type == "ERROR" || record.Type == "CONTROL_ERROR" {
				return fmt.Errorf("soak process %s: %s", record.Type, record.Error)
			}
			if record.Type == "RECOVERY" && record.Status == "failed" {
				return fmt.Errorf("soak recovery %s failed: %s", record.Phase, record.Error)
			}
			if match(record) {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timed out after %s waiting for soak control record", timeout)
		}
	}
}

func writeSoakPhase(stdin io.Writer, phase, state, target, replacement string) error {
	command := map[string]string{
		"type":   "PHASE",
		"phase":  phase,
		"state":  state,
		"target": target,
	}
	if replacement != "" {
		command["replacement"] = replacement
	}
	payload, err := json.Marshal(command)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdin, "%s\n", payload)
	return err
}

func soakDuration(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid %s=%q", name, value)
	}
	return duration
}

func assertSoakResult(t *testing.T, result *soakResult, clientCount int) {
	t.Helper()
	if result.Type != "RESULT" || result.ClientCount != clientCount || result.FinalConnected != clientCount ||
		result.FinalRecoveredClients != clientCount || !result.PhasesRequired {
		t.Fatalf("invalid soak result summary: %+v", result)
	}
	if result.DuplicateAcks != 0 || result.AckMismatches != 0 || result.DuplicateEvents != 0 ||
		result.UnknownEvents != 0 || result.Broadcasts.DuplicateCallbacks != 0 ||
		result.Broadcasts.DuplicateDeliveries != 0 || result.Broadcasts.UnknownDeliveries != 0 ||
		result.Broadcasts.DeliveryRatio <= 0 {
		t.Fatalf("soak delivery invariants failed: %+v", result.Broadcasts)
	}

	polling := result.TransportClasses["polling-only"]
	webSocket := result.TransportClasses["websocket-only"]
	upgrade := result.TransportClasses["polling-to-websocket"]
	if polling.Clients == 0 || polling.Connected != polling.Clients || polling.PollingObserved != polling.Clients ||
		webSocket.Clients == 0 || webSocket.Connected != webSocket.Clients || webSocket.WebSocketObserved != webSocket.Clients ||
		upgrade.Clients == 0 || upgrade.Connected != upgrade.Clients || upgrade.PollingObserved != upgrade.Clients ||
		upgrade.WebSocketObserved != upgrade.Clients || upgrade.UpgradedClients != upgrade.Clients {
		t.Fatalf("transport matrix incomplete: %+v", result.TransportClasses)
	}
	for _, phaseName := range []string{"partition", "rolling-restart"} {
		phase, ok := result.Phases[phaseName]
		if !ok || phase.Recovery.Status != "passed" || len(phase.TargetClientIndexes) == 0 ||
			len(phase.AffectedClientIndexes) < phase.Recovery.RequiredAffectedClients ||
			phase.Recovery.ExpectedRecoveredNode == "" || len(phase.Recovery.ClientsAssignedToRecoveredNode) == 0 {
			t.Fatalf("%s phase was not proven: %+v", phaseName, phase)
		}
	}
	if result.Shutdown.DisconnectedClients != clientCount || result.Shutdown.ClosedEngines != result.Shutdown.EngineCount ||
		result.Shutdown.AgentHandles.Active != 0 || result.Shutdown.AgentHandles.Free != 0 ||
		result.Shutdown.AgentHandles.Queued != 0 {
		t.Fatalf("Node client resources did not close: %+v", result.Shutdown)
	}
}

func TestOfficialClusterSoakAndResourceLeaks(t *testing.T) {
	if os.Getenv(clusterSoakEnv) != "1" {
		t.Skip("set SOCKET_IO_CLUSTER_SOAK=1 after npm ci in testdata/cluster-official")
	}
	duration := soakDuration(t, "SOCKET_IO_SOAK_DURATION", 30*time.Second)
	if duration < 10*time.Second {
		t.Fatal("SOCKET_IO_SOAK_DURATION must be at least 10s")
	}
	partitionDuration := soakDuration(t, "SOCKET_IO_SOAK_PARTITION_DURATION", duration/5)
	if partitionDuration >= duration/2 {
		t.Fatal("partition duration must be less than half of the soak duration")
	}
	clientCount := 36
	if value := os.Getenv("SOCKET_IO_SOAK_CLIENTS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 3 {
			t.Fatalf("invalid SOCKET_IO_SOAK_CLIENTS=%q", value)
		}
		clientCount = parsed
	}
	pingInterval := min(10*time.Second, max(500*time.Millisecond, partitionDuration/4))
	pingTimeout := min(5*time.Second, max(500*time.Millisecond, partitionDuration/4))
	t.Setenv("SOCKET_IO_UNIX_CLUSTER_WORKER_PING_INTERVAL", pingInterval.String())
	t.Setenv("SOCKET_IO_UNIX_CLUSTER_WORKER_PING_TIMEOUT", pingTimeout.String())

	socketPath := filepath.Join(os.TempDir(), "sio-soak-"+strconv.Itoa(os.Getpid())+".sock")
	t.Cleanup(func() {
		matches, _ := filepath.Glob(socketPath + ".*")
		for _, match := range matches {
			_ = os.Remove(match)
		}
	})
	workers := make([]*clusterWorker, 0, 4)
	workers = append(workers,
		startClusterWorker(t, socketPath, "soak-1"),
		startClusterWorker(t, socketPath, "soak-2"),
		startClusterWorker(t, socketPath, "soak-3"),
	)
	baselines := make(map[string]workerResources)
	gates := make(map[string]*partitionProxy)

	router, err := sticky.New(sticky.Options{
		LoadBalancingMethod: sticky.RoundRobin,
		SessionTTL:          30 * time.Second,
		SweepInterval:       time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	for _, worker := range workers {
		gate := newPartitionProxy(t, worker.address)
		gates[worker.id] = gate
		target, _ := url.Parse("http://" + gate.address())
		if addErr := router.AddBackend(worker.id, target); addErr != nil {
			t.Fatal(addErr)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyServer := &http.Server{Handler: router, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = proxyServer.Serve(listener) }()
	t.Cleanup(func() { _ = proxyServer.Close() })
	time.Sleep(time.Second)
	for _, worker := range workers {
		_ = readWorkerResources(t, worker) // warm the diagnostics and lazy runtime paths
		baselines[worker.id] = readWorkerResources(t, worker)
	}
	parentBaseline := currentSoakProcessResources()

	matrixPath := filepath.Join("..", "testdata", "cluster-official", "soak.cjs")
	commandGrace := 2 * time.Minute
	if duration >= time.Hour {
		commandGrace = 10 * time.Minute
	}
	commandContext, cancelCommand := context.WithTimeout(t.Context(), duration+commandGrace)
	defer cancelCommand()
	command := exec.CommandContext(commandContext, "node", matrixPath)
	command.Env = append(os.Environ(),
		"SOCKET_IO_CLUSTER_URL=http://"+listener.Addr().String(),
		"SOCKET_IO_SOAK_DURATION_MS="+strconv.FormatInt(duration.Milliseconds(), 10),
		"SOCKET_IO_SOAK_CLIENTS="+strconv.Itoa(clientCount),
		"SOCKET_IO_SOAK_REQUIRE_PHASES=1",
		"SOCKET_IO_SOAK_PARTITION_DURATION_MS="+strconv.FormatInt(partitionDuration.Milliseconds(), 10),
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var output soakOutputLog
	var stderr soakOutputLog
	command.Stderr = &stderr
	records := make(chan soakJSONRecord, 64)
	scannerDone := make(chan error, 1)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	go func() {
		defer close(records)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 64*1024), 4<<20)
		for scanner.Scan() {
			line := scanner.Text()
			output.append(line)
			var record soakJSONRecord
			if unmarshalErr := json.Unmarshal([]byte(line), &record); unmarshalErr != nil {
				records <- soakJSONRecord{Type: "ERROR", Error: "invalid JSONL output: " + unmarshalErr.Error()}
				continue
			}
			record.Raw = append(record.Raw[:0], []byte(line)...)
			records <- record
		}
		scannerDone <- scanner.Err()
	}()

	if err := waitForSoakRecord(records, 45*time.Second, func(record soakJSONRecord) bool {
		return record.Type == "READY"
	}); err != nil {
		t.Fatalf("wait for soak READY: %v\nstdout:\n%s\nstderr:\n%s", err, output.String(), stderr.String())
	}

	var sampleWorkers sync.Map
	for _, worker := range workers {
		sampleWorkers.Store(worker.id, worker)
	}
	resourceSamples := make(map[string][]timedWorkerResourceSample)
	var resourceSamplesMu sync.Mutex
	sampleInterval := min(5*time.Minute, max(5*time.Second, duration/12))
	samplerContext, stopSampler := context.WithCancel(t.Context())
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(sampleInterval)
		defer ticker.Stop()
		for {
			select {
			case sampledAt := <-ticker.C:
				sampleWorkers.Range(func(id, value any) bool {
					worker := value.(*clusterWorker)
					resources, sampleErr := fetchWorkerResources(worker)
					if sampleErr == nil {
						resourceSamplesMu.Lock()
						resourceSamples[id.(string)] = append(resourceSamples[id.(string)], timedWorkerResourceSample{
							At: sampledAt, Resources: resources,
						})
						resourceSamplesMu.Unlock()
					}
					return true
				})
			case <-samplerContext.Done():
				return
			}
		}
	}()

	// READY is emitted only after every client is connected and the polling,
	// WebSocket-only and polling-to-WebSocket groups have all proven their transport.
	time.Sleep(duration / 5)
	if err := writeSoakPhase(stdin, "partition", "start", workers[0].id, ""); err != nil {
		t.Fatal(err)
	}
	if err := waitForSoakRecord(records, 10*time.Second, func(record soakJSONRecord) bool {
		return record.Type == "PHASE" && record.Phase == "partition" && record.State == "started"
	}); err != nil {
		t.Fatal(err)
	}
	partitionBefore := gates[workers[0].id].stats()
	gates[workers[0].id].setPartitioned(true)
	time.Sleep(partitionDuration)
	gates[workers[0].id].setPartitioned(false)
	if err := writeSoakPhase(stdin, "partition", "end", workers[0].id, ""); err != nil {
		t.Fatal(err)
	}
	if err := waitForSoakRecord(records, 10*time.Second, func(record soakJSONRecord) bool {
		return record.Type == "PHASE" && record.Phase == "partition" && record.State == "ended"
	}); err != nil {
		t.Fatal(err)
	}
	if err := waitForSoakRecord(records, 45*time.Second, func(record soakJSONRecord) bool {
		return record.Type == "RECOVERY" && record.Phase == "partition" && record.Status == "passed"
	}); err != nil {
		t.Fatalf("partition recovery: %v\n%s", err, output.String())
	}
	partitionAfter := gates[workers[0].id].stats()
	if partitionAfter.ForcedConnections <= partitionBefore.ForcedConnections ||
		partitionAfter.RejectedRequests <= partitionBefore.RejectedRequests {
		t.Fatalf("partition did not cut active and subsequent connections: before=%+v after=%+v", partitionBefore, partitionAfter)
	}

	// Perform a rolling process replacement while the workload continues.
	time.Sleep(duration / 10)
	if err := writeSoakPhase(stdin, "rolling-restart", "start", workers[1].id, ""); err != nil {
		t.Fatal(err)
	}
	if err := waitForSoakRecord(records, 10*time.Second, func(record soakJSONRecord) bool {
		return record.Type == "PHASE" && record.Phase == "rolling-restart" && record.State == "started"
	}); err != nil {
		t.Fatal(err)
	}
	workers[1].stop()
	router.RemoveBackend(workers[1].id)
	replacement := startClusterWorker(t, socketPath, "soak-4")
	workers = append(workers, replacement)
	replacementGate := newPartitionProxy(t, replacement.address)
	gates[replacement.id] = replacementGate
	time.Sleep(time.Second)
	_ = readWorkerResources(t, replacement)
	baselines[replacement.id] = readWorkerResources(t, replacement)
	sampleWorkers.Store(replacement.id, replacement)
	target, _ := url.Parse("http://" + replacementGate.address())
	if err := router.AddBackend(replacement.id, target); err != nil {
		t.Fatal(err)
	}
	if err := writeSoakPhase(stdin, "rolling-restart", "end", workers[1].id, replacement.id); err != nil {
		t.Fatal(err)
	}
	if err := waitForSoakRecord(records, 10*time.Second, func(record soakJSONRecord) bool {
		return record.Type == "PHASE" && record.Phase == "rolling-restart" && record.State == "ended"
	}); err != nil {
		t.Fatal(err)
	}
	if err := waitForSoakRecord(records, 45*time.Second, func(record soakJSONRecord) bool {
		return record.Type == "RECOVERY" && record.Phase == "rolling-restart" && record.Status == "passed"
	}); err != nil {
		t.Fatalf("rolling recovery: %v\n%s", err, output.String())
	}

	waitErr := command.Wait()
	_ = stdin.Close()
	scanErr := <-scannerDone
	stopSampler()
	<-samplerDone
	if waitErr != nil {
		t.Fatalf("soak matrix failed: %v\nstdout:\n%s\nstderr:\n%s", waitErr, output.String(), stderr.String())
	}
	if scanErr != nil {
		t.Fatalf("read soak output: %v", scanErr)
	}
	var resultRecord soakJSONRecord
	for record := range records {
		if record.Type == "ERROR" || record.Type == "CONTROL_ERROR" {
			t.Fatalf("soak process %s: %s\n%s", record.Type, record.Error, output.String())
		}
		if record.Type == "RESULT" {
			resultRecord = record
		}
	}
	if len(resultRecord.Raw) == 0 {
		t.Fatalf("soak process emitted no RESULT record\n%s", output.String())
	}
	var result soakResult
	if err := json.Unmarshal(resultRecord.Raw, &result); err != nil {
		t.Fatalf("decode soak RESULT: %v\n%s", err, resultRecord.Raw)
	}
	assertSoakResult(t, &result, clientCount)

	// The Node process performs a graceful Engine.IO close. Poll until every
	// protocol/adapter/router resource is zero and remains stable for three reads.
	survivors := make([]*clusterWorker, 0, 3)
	for _, worker := range workers {
		if worker.command.ProcessState == nil {
			survivors = append(survivors, worker)
		}
	}
	settleDeadline := time.Now().Add(enginetransports.DefaultPollingCloseTimeout + 20*time.Second)
	stableReads := 0
	lastResources := make(map[string]workerResources)
	for time.Now().Before(settleDeadline) {
		clean := true
		for _, stats := range router.Stats() {
			if stats.Sessions != 0 {
				clean = false
			}
		}
		for _, worker := range survivors {
			current, resourceErr := fetchWorkerResources(worker)
			if resourceErr != nil {
				clean = false
				continue
			}
			lastResources[worker.id] = current
			baseline := baselines[worker.id]
			if current.SocketClients != 0 || current.EngineClients != 0 || current.RawEngineClients != 0 ||
				current.AdapterRooms != 0 || current.AdapterSids != 0 ||
				current.Goroutines > baseline.Goroutines+24 || current.HeapAlloc > baseline.HeapAlloc+(32<<20) ||
				current.HeapObjects > baseline.HeapObjects+20_000 || current.StackInuse > baseline.StackInuse+(8<<20) ||
				(baseline.OpenFDs != 0 && current.OpenFDs > baseline.OpenFDs+16) {
				clean = false
			}
		}
		if clean {
			stableReads++
			if stableReads == 3 {
				break
			}
		} else {
			stableReads = 0
		}
		time.Sleep(time.Second)
	}
	if stableReads != 3 {
		for _, worker := range survivors {
			if response, requestErr := soakHTTPClient.Get("http://" + worker.address + "/debug/goroutines"); requestErr == nil {
				var stacks strings.Builder
				_, _ = io.Copy(&stacks, response.Body)
				_ = response.Body.Close()
				t.Logf("%s residual goroutines:\n%s", worker.id, stacks.String())
			}
		}
		t.Fatalf("cluster resources did not settle: router=%+v workers=%+v", router.Stats(), lastResources)
	}

	if duration >= time.Minute {
		resourceSamplesMu.Lock()
		type aggregateResourceSample struct {
			at        time.Time
			workers   uint64
			resources workerResources
		}
		aggregatesByTime := make(map[time.Time]*aggregateResourceSample)
		for workerID, samples := range resourceSamples {
			t.Logf("%s periodic resource samples: %d", workerID, len(samples))
			for _, sample := range samples {
				aggregate := aggregatesByTime[sample.At]
				if aggregate == nil {
					aggregate = &aggregateResourceSample{at: sample.At}
					aggregatesByTime[sample.At] = aggregate
				}
				aggregate.workers++
				aggregate.resources.Goroutines += sample.Resources.Goroutines
				aggregate.resources.HeapAlloc += sample.Resources.HeapAlloc
				aggregate.resources.HeapObjects += sample.Resources.HeapObjects
				aggregate.resources.StackInuse += sample.Resources.StackInuse
				aggregate.resources.OpenFDs += sample.Resources.OpenFDs
				aggregate.resources.SocketClients += sample.Resources.SocketClients
				aggregate.resources.EngineClients += sample.Resources.EngineClients
				aggregate.resources.AdapterRooms += sample.Resources.AdapterRooms
				aggregate.resources.AdapterSids += sample.Resources.AdapterSids
				aggregate.resources.RawEngineClients += sample.Resources.RawEngineClients
			}
		}
		aggregates := make([]*aggregateResourceSample, 0, len(aggregatesByTime))
		for _, aggregate := range aggregatesByTime {
			aggregates = append(aggregates, aggregate)
		}
		sort.Slice(aggregates, func(left, right int) bool {
			return aggregates[left].at.Before(aggregates[right].at)
		})
		resourceSamplesMu.Unlock()

		// Connections intentionally move between workers after the partition and
		// rolling replacement. Compare the aggregate cluster footprint at two
		// similarly loaded points; a per-worker comparison mistakes redistribution
		// for a leak (for example, 24 -> 50 healthy clients on one survivor).
		if len(aggregates) >= 6 {
			early := aggregates[1]
			late := aggregates[len(aggregates)/2]
			bestClientDelta := absoluteDifference(early.resources.SocketClients, late.resources.SocketClients)
			for _, candidate := range aggregates[len(aggregates)/2:] {
				delta := absoluteDifference(early.resources.SocketClients, candidate.resources.SocketClients)
				if delta < bestClientDelta {
					late = candidate
					bestClientDelta = delta
				}
			}
			loadTolerance := uint64(max(4, clientCount/10))
			if bestClientDelta > loadTolerance {
				t.Fatalf("cluster resource trend has no comparable load samples: early=%+v late=%+v clientDelta=%d samples=%d",
					early.resources, late.resources, bestClientDelta, len(aggregates))
			}
			workerAllowance := max(early.workers, late.workers)
			clientGrowth := uint64(0)
			if late.resources.SocketClients > early.resources.SocketClients {
				clientGrowth = late.resources.SocketClients - early.resources.SocketClients
			}
			if late.resources.HeapAlloc > early.resources.HeapAlloc+workerAllowance*(16<<20)+clientGrowth*(1<<20) ||
				late.resources.HeapObjects > early.resources.HeapObjects+workerAllowance*10_000+clientGrowth*1_000 ||
				late.resources.Goroutines > early.resources.Goroutines+workerAllowance*12+clientGrowth*6 ||
				late.resources.StackInuse > early.resources.StackInuse+workerAllowance*(8<<20) ||
				(early.resources.OpenFDs != 0 && late.resources.OpenFDs > early.resources.OpenFDs+workerAllowance*8+clientGrowth) {
				t.Fatalf("cluster aggregate resource trend grew across soak: early=%+v late=%+v workers=%d/%d samples=%d",
					early.resources, late.resources, early.workers, late.workers, len(aggregates))
			}
			t.Logf("cluster aggregate resource samples: %d, comparable clients=%d/%d",
				len(aggregates), early.resources.SocketClients, late.resources.SocketClients)
		}
	}

	_ = proxyServer.Close()
	for _, gate := range gates {
		gate.close()
	}
	_ = router.Close()
	time.Sleep(500 * time.Millisecond)
	parentCurrent := currentSoakProcessResources()
	if parentCurrent.Goroutines > parentBaseline.Goroutines+12 ||
		parentCurrent.HeapAlloc > parentBaseline.HeapAlloc+(16<<20) ||
		parentCurrent.HeapObjects > parentBaseline.HeapObjects+10_000 ||
		parentCurrent.StackInuse > parentBaseline.StackInuse+(4<<20) ||
		(parentBaseline.OpenFDs != 0 && parentCurrent.OpenFDs > parentBaseline.OpenFDs+12) {
		t.Fatalf("router/proxy process resources did not return: baseline=%+v current=%+v", parentBaseline, parentCurrent)
	}
	if os.Getenv("SOCKET_IO_SOAK_VERBOSE") == "1" {
		t.Logf("soak completed: %s", strings.TrimSpace(string(resultRecord.Raw)))
	} else {
		t.Logf("soak completed: duration=%dms clients=%d connects=%d disconnects=%d ACK=%d/%d timeouts=%d delivery=%.4f phases=%+v",
			result.DurationMS, result.ClientCount, result.Connects, result.Disconnects, result.AckSuccess,
			result.AckAttempts, result.AckTimeouts, result.Broadcasts.DeliveryRatio, result.Phases)
	}
}
