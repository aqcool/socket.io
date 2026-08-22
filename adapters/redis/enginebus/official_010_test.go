package enginebus

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	engine "github.com/aqcool/socket.io/servers/engine/v4"
	"github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/redis/go-redis/v9"
)

type redisClusterFixture struct {
	engines []engine.ClusterServer
	servers []*httptest.Server
	urls    []string
	clients []*redis.Client
}

func redisClusterTestConfig(t *testing.T) (string, string) {
	t.Helper()
	address := os.Getenv("SOCKET_IO_CLUSTER_ENGINE_REDIS_ADDR")
	if address == "" {
		address = os.Getenv("SOCKET_IO_REDIS_TEST_ADDR")
	}
	if address == "" {
		t.Skip("set SOCKET_IO_CLUSTER_ENGINE_REDIS_ADDR (or SOCKET_IO_REDIS_TEST_ADDR) for the real Redis matrix")
	}
	password := os.Getenv("SOCKET_IO_CLUSTER_ENGINE_REDIS_PASSWORD")
	if password == "" {
		password = os.Getenv("SOCKET_IO_REDIS_TEST_PASSWORD")
	}
	return address, password
}

func newRedisClusterFixture(t *testing.T, sharedPublisher bool) *redisClusterFixture {
	t.Helper()
	address, password := redisClusterTestConfig(t)
	fixture := &redisClusterFixture{}
	newClient := func() *redis.Client {
		client := redis.NewClient(&redis.Options{Addr: address, Password: password})
		if err := client.Ping(t.Context()).Err(); err != nil {
			_ = client.Close()
			t.Fatalf("Redis PING %s: %v", address, err)
		}
		fixture.clients = append(fixture.clients, client)
		return client
	}

	var sharedPub *redis.Client
	if sharedPublisher {
		sharedPub = newClient()
	}
	prefix := fmt.Sprintf("engine.io-official-%d", time.Now().UnixNano())
	for index := range 3 {
		pubClient := sharedPub
		if pubClient == nil {
			pubClient = newClient()
		}
		subClient := newClient()
		bus, err := New(pubClient, subClient, &Options{ChannelPrefix: prefix})
		if err != nil {
			t.Fatal(err)
		}
		serverOptions := config.DefaultServerOptions()
		if index == 2 {
			serverOptions.SetPingInterval(50 * time.Millisecond)
		}
		clusterServer, err := engine.NewClusterServer(bus, serverOptions, &engine.ClusterOptions{
			ResponseTimeout: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewClusterServer() error = %v", err)
		}
		fixture.engines = append(fixture.engines, clusterServer)
		httpServer := httptest.NewServer(clusterServer)
		fixture.servers = append(fixture.servers, httpServer)
		fixture.urls = append(fixture.urls, httpServer.URL)
	}
	t.Cleanup(func() {
		for _, clusterServer := range fixture.engines {
			clusterServer.Close()
		}
		for _, httpServer := range fixture.servers {
			httpServer.CloseClientConnections()
			httpServer.Close()
		}
		for _, client := range fixture.clients {
			_ = client.Close()
		}
	})
	return fixture
}

func redisEngineURL(baseURL, sid string) string {
	value := baseURL + "/engine.io/?EIO=4&transport=polling"
	if sid != "" {
		value += "&sid=" + sid
	}
	return value
}

func redisEngineHandshake(t *testing.T, baseURL string) string {
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
	return open.SID
}

func redisEngineRequest(t *testing.T, method, baseURL, sid, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(method, redisEngineURL(baseURL, sid), strings.NewReader(body))
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

func TestOfficialClusterEngine010Redis(t *testing.T) {
	for _, clientMatrix := range []struct {
		name            string
		sharedPublisher bool
	}{
		{name: "redis package", sharedPublisher: true},
		{name: "ioredis package", sharedPublisher: false},
	} {
		t.Run(clientMatrix.name, func(t *testing.T) {
			t.Run("should ping/pong", func(t *testing.T) {
				fixture := newRedisClusterFixture(t, clientMatrix.sharedPublisher)
				sid := redisEngineHandshake(t, fixture.urls[2])
				for index := range 10 {
					status, body := redisEngineRequest(t, http.MethodGet, fixture.urls[index%3], sid, "")
					if status != http.StatusOK || body != "2" {
						t.Fatalf("ping %d = status %d body %q", index, status, body)
					}
					status, _ = redisEngineRequest(t, http.MethodPost, fixture.urls[(index+1)%3], sid, "3")
					if status != http.StatusOK {
						t.Fatalf("pong %d status = %d", index, status)
					}
				}
			})

			t.Run("should send and receive binary", func(t *testing.T) {
				fixture := newRedisClusterFixture(t, clientMatrix.sharedPublisher)
				_ = fixture.engines[0].On("connection", func(values ...any) {
					socket := values[0].(engine.Socket)
					_ = socket.On("message", func(message ...any) {
						reader, _ := message[0].(io.Reader)
						socket.Send(reader, nil, nil)
					})
				})
				sid := redisEngineHandshake(t, fixture.urls[0])
				status, _ := redisEngineRequest(t, http.MethodPost, fixture.urls[1], sid, "bAQIDBA==")
				if status != http.StatusOK {
					t.Fatalf("binary POST status = %d", status)
				}
				for range 100 {
					status, body := redisEngineRequest(t, http.MethodGet, fixture.urls[2], sid, "")
					if status != http.StatusOK {
						t.Fatalf("binary poll status = %d", status)
					}
					if body == "bAQIDBA==" {
						return
					}
					if body != "2" {
						t.Fatalf("binary poll body = %q", body)
					}
				}
				t.Fatal("binary echo was not observed")
			})
		})
	}
}
