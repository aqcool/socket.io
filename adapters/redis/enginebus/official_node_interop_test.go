package enginebus

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	engine "github.com/aqcool/socket.io/servers/engine/v3"
	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/redis/go-redis/v9"
)

type officialNodeProcess struct {
	baseURL string
	command *exec.Cmd
	done    chan struct{}
	waitMu  sync.Mutex
	waitErr error
}

func (p *officialNodeProcess) finish(err error) {
	p.waitMu.Lock()
	p.waitErr = err
	p.waitMu.Unlock()
	close(p.done)
}

func (p *officialNodeProcess) result() error {
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}

type synchronizedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *synchronizedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(value)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func officialInteropFixtureDir(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(filename), "testdata", "official-interop")
}

func reserveOfficialInteropPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func startOfficialNodeRedisEngine(
	t *testing.T,
	mode, redisURL, channelPrefix string,
) *officialNodeProcess {
	t.Helper()
	fixtureDir := officialInteropFixtureDir(t)
	if _, err := os.Stat(filepath.Join(fixtureDir, "node_modules", "@socket.io", "cluster-engine")); err != nil {
		t.Skip("run npm ci in enginebus/testdata/official-interop for official Node interop")
	}
	port := reserveOfficialInteropPort(t)
	command := exec.Command("node", "server.cjs") //nolint:gosec
	command.Dir = fixtureDir
	command.Env = append(os.Environ(),
		"CLUSTER_ENGINE_REDIS_CLIENT="+mode,
		"CLUSTER_ENGINE_REDIS_URL="+redisURL,
		"CLUSTER_ENGINE_CHANNEL_PREFIX="+channelPrefix,
		"CLUSTER_ENGINE_NODE_PORT="+strconv.Itoa(port),
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr synchronizedBuffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &officialNodeProcess{
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", port),
		command: command,
		done:    make(chan struct{}),
	}
	go func() { process.finish(command.Wait()) }()

	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if !scanner.Scan() {
			ready <- fmt.Errorf("official Node server exited before ready: %s", stderr.String())
			return
		}
		var state struct {
			Ready bool `json:"ready"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &state); err != nil || !state.Ready {
			ready <- fmt.Errorf("invalid official Node ready line %q: %w", scanner.Text(), err)
			return
		}
		ready <- nil
	}()
	select {
	case err := <-ready:
		if err != nil {
			_ = command.Process.Kill()
			<-process.done
			t.Fatal(err)
		}
	case <-process.done:
		t.Fatalf("official Node RedisEngine exited before ready: %v: %s", process.result(), stderr.String())
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		<-process.done
		t.Fatalf("official Node RedisEngine readiness timeout: %s", stderr.String())
	}

	t.Cleanup(func() {
		select {
		case <-process.done:
			return
		default:
			_ = command.Process.Kill()
		}
		select {
		case <-process.done:
		case <-time.After(5 * time.Second):
			t.Errorf("official Node RedisEngine did not exit")
		}
	})
	return process
}

type officialGoRedisEngine struct {
	server  engine.ClusterServer
	http    *httptest.Server
	clients []*redis.Client
}

func newOfficialGoRedisEngine(
	t *testing.T,
	address, password, channelPrefix string,
) *officialGoRedisEngine {
	t.Helper()
	newClient := func() *redis.Client {
		client := redis.NewClient(&redis.Options{Addr: address, Password: password})
		if err := client.Ping(t.Context()).Err(); err != nil {
			_ = client.Close()
			t.Fatal(err)
		}
		return client
	}
	pubClient := newClient()
	subClient := newClient()
	bus, err := New(pubClient, subClient, &Options{ChannelPrefix: channelPrefix})
	if err != nil {
		t.Fatal(err)
	}
	serverOptions := config.DefaultServerOptions()
	serverOptions.SetPingInterval(50 * time.Millisecond)
	server, err := engine.NewClusterServer(bus, serverOptions, &engine.ClusterOptions{
		ResponseTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = server.On("connection", func(values ...any) {
		socket := values[0].(engine.Socket)
		_ = socket.On("message", func(message ...any) {
			reader, _ := message[0].(io.Reader)
			socket.Send(reader, nil, nil)
		})
	})
	fixture := &officialGoRedisEngine{
		server:  server,
		http:    httptest.NewServer(server),
		clients: []*redis.Client{pubClient, subClient},
	}
	t.Cleanup(func() {
		server.Close()
		fixture.http.CloseClientConnections()
		fixture.http.Close()
		for _, client := range fixture.clients {
			_ = client.Close()
		}
	})
	return fixture
}

func officialRedisURL(address, password string) string {
	redisURL := &url.URL{Scheme: "redis", Host: address}
	if password != "" {
		redisURL.User = url.UserPassword("", password)
	}
	return redisURL.String()
}

func interopHandshake(t *testing.T, baseURL string) string {
	t.Helper()
	response, err := http.Get(redisEngineURL(baseURL, "")) //nolint:gosec,noctx
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(payload) == 0 || payload[0] != '0' {
		t.Fatalf("handshake = status %d body %q", response.StatusCode, payload)
	}
	var open struct {
		SID string `json:"sid"`
	}
	if err := json.Unmarshal(payload[1:], &open); err != nil {
		t.Fatal(err)
	}
	if len(open.SID) != 20 {
		t.Fatalf("official cluster SID length = %d, want 20", len(open.SID))
	}
	return open.SID
}

func interopRequest(
	t *testing.T,
	method, baseURL, sid, body string,
) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, redisEngineURL(baseURL, sid), strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, string(payload)
}

func assertOfficialPingAcrossOwners(t *testing.T, nodeURL, goURL string) {
	t.Helper()
	for _, ownerURL := range []string{nodeURL, goURL} {
		sid := interopHandshake(t, ownerURL)
		otherURL := goURL
		if ownerURL == goURL {
			otherURL = nodeURL
		}
		for index := range 10 {
			pollURL := otherURL
			dataURL := ownerURL
			if index%2 == 1 {
				pollURL, dataURL = ownerURL, otherURL
			}
			status, body := interopRequest(t, http.MethodGet, pollURL, sid, "")
			if status != http.StatusOK || body != "2" {
				t.Fatalf("owner %s ping %d = status %d body %q", ownerURL, index, status, body)
			}
			status, _ = interopRequest(t, http.MethodPost, dataURL, sid, "3")
			if status != http.StatusOK {
				t.Fatalf("owner %s pong %d status = %d", ownerURL, index, status)
			}
		}
	}
}

func assertOfficialBinaryAcrossOwners(t *testing.T, nodeURL, goURL string) {
	t.Helper()
	for _, ownerURL := range []string{nodeURL, goURL} {
		sid := interopHandshake(t, ownerURL)
		otherURL := goURL
		if ownerURL == goURL {
			otherURL = nodeURL
		}
		status, _ := interopRequest(t, http.MethodPost, otherURL, sid, "bAQIDBA==")
		if status != http.StatusOK {
			t.Fatalf("owner %s binary POST status = %d", ownerURL, status)
		}
		observed := false
		for range 100 {
			status, body := interopRequest(t, http.MethodGet, otherURL, sid, "")
			if status != http.StatusOK {
				t.Fatalf("owner %s binary poll status = %d", ownerURL, status)
			}
			if body == "bAQIDBA==" {
				observed = true
				break
			}
			if body != "2" {
				t.Fatalf("owner %s binary poll body = %q", ownerURL, body)
			}
		}
		if !observed {
			t.Fatalf("owner %s binary echo was not observed", ownerURL)
		}
	}
}

func TestOfficialNodeClusterEngine010RedisInterop(t *testing.T) {
	if os.Getenv("SOCKET_IO_CLUSTER_ENGINE_OFFICIAL_INTEROP") != "1" {
		t.Skip("set SOCKET_IO_CLUSTER_ENGINE_OFFICIAL_INTEROP=1 for official Node RedisEngine interop")
	}
	address, password := redisClusterTestConfig(t)
	for _, clientMode := range []string{"redis", "ioredis"} {
		t.Run(clientMode, func(t *testing.T) {
			channelPrefix := fmt.Sprintf("engine.io-node-go-%s-%d", clientMode, time.Now().UnixNano())
			node := startOfficialNodeRedisEngine(t, clientMode, officialRedisURL(address, password), channelPrefix)
			goEngine := newOfficialGoRedisEngine(t, address, password, channelPrefix)
			t.Run("should ping/pong", func(t *testing.T) {
				assertOfficialPingAcrossOwners(t, node.baseURL, goEngine.http.URL)
			})
			t.Run("should send and receive binary", func(t *testing.T) {
				assertOfficialBinaryAcrossOwners(t, node.baseURL, goEngine.http.URL)
			})
		})
	}
}
