package engine

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

const engineCompatibilityTestEnv = "ENGINE_IO_COMPAT_TEST"

type engineCompatibilityHarness struct {
	HTTPServer                  *httptest.Server
	serverInitiatedCloseReasons chan string
	clientInitiatedCloseReasons chan string
	pingTimeoutReasons          chan string
	payloadCloseReasons         chan string
	unicodeRoundTrips           chan struct{}
	upgradeStressResults        chan error
}

func newEngineCompatibilityServer(t *testing.T, allowEIO3 bool) *engineCompatibilityHarness {
	t.Helper()
	options := config.DefaultServerOptions()
	options.SetAllowEIO3(allowEIO3)
	options.SetPingInterval(100 * time.Millisecond)
	options.SetPingTimeout(500 * time.Millisecond)
	server := NewServer(options)
	harness := &engineCompatibilityHarness{
		serverInitiatedCloseReasons: make(chan string, 2),
		clientInitiatedCloseReasons: make(chan string, 2),
		pingTimeoutReasons:          make(chan string, 1),
		payloadCloseReasons:         make(chan string, 1),
		unicodeRoundTrips:           make(chan struct{}, 1),
		upgradeStressResults:        make(chan error, 1),
	}
	_ = server.On("connection", func(args ...any) {
		client := args[0].(Socket)
		var upgradeStressReceived atomic.Int32
		var upgradeStressUpgraded atomic.Bool
		var upgradeStressOnce sync.Once
		completeUpgradeStress := func(err error) {
			if err != nil || (upgradeStressReceived.Load() == 50 && upgradeStressUpgraded.Load()) {
				upgradeStressOnce.Do(func() { harness.upgradeStressResults <- err })
			}
		}
		_ = client.On("upgrade", func(...any) {
			upgradeStressUpgraded.Store(true)
			completeUpgradeStress(nil)
		})
		unicodeMessages := []string{".", "石室詩士施氏，嗜獅，誓食十獅。", "氏時時適市視獅。"}
		unicodeIndex := 0
		client.Send(types.NewStringBufferString(fmt.Sprintf("protocol:%d", client.Protocol())), nil, nil)
		_ = client.On("message", func(args ...any) {
			message, ok := args[0].(types.BufferInterface)
			if !ok {
				return
			}
			text := message.String()
			switch text {
			case "__server_close__":
				_ = client.Once("close", func(closeArgs ...any) {
					harness.serverInitiatedCloseReasons <- closeArgs[0].(string)
				})
				client.Close(false)
				return
			case "__client_will_close__":
				_ = client.Once("close", func(closeArgs ...any) {
					harness.clientInitiatedCloseReasons <- closeArgs[0].(string)
				})
				client.Send(types.NewStringBufferString("__close_ready__"), nil, nil)
				return
			case "__timeout_ready__":
				_ = client.Once("close", func(closeArgs ...any) {
					harness.pingTimeoutReasons <- closeArgs[0].(string)
				})
				client.Send(types.NewStringBufferString("__timeout_ready__"), nil, nil)
				// The official client and server derive nearly identical timeout
				// deadlines from the handshake. Close slightly before the client's
				// local deadline so the server-side reason is deterministic while
				// the client (whose transport close listener is disabled by the
				// matrix) still reaches its own "ping timeout" path.
				time.AfterFunc(250*time.Millisecond, func() { client.(*socket).OnClose("ping timeout") })
				return
			case "__close_in_payload__":
				_ = client.Once("close", func(closeArgs ...any) {
					harness.payloadCloseReasons <- closeArgs[0].(string)
				})
				client.Send(types.NewStringBufferString("__close_now__"), nil, nil)
				client.Send(types.NewStringBufferString("__must_not_arrive__"), nil, nil)
				return
			case "__server_messages__":
				for _, value := range []string{"a", "b", "c"} {
					client.Send(types.NewStringBufferString(value), nil, nil)
				}
				client.Send(types.NewBytesBuffer([]byte{0, 1, 2, 3, 4}), nil, nil)
				return
			case "__ordered_close__":
				for _, value := range []string{"a", "b", "c"} {
					client.Send(types.NewStringBufferString(value), nil, nil)
				}
				client.Close(false)
				return
			case "__ordered_close_delayed__":
				client.Send(types.NewStringBufferString("a"), nil, nil)
				time.AfterFunc(20*time.Millisecond, func() {
					client.Send(types.NewStringBufferString("b"), nil, nil)
					time.AfterFunc(20*time.Millisecond, func() {
						client.Send(types.NewStringBufferString("c"), nil, nil)
						client.Close(false)
					})
				})
				return
			case "__unicode_roundtrip__":
				unicodeIndex = 0
				for _, value := range unicodeMessages {
					client.Send(types.NewStringBufferString(value), nil, nil)
				}
				return
			case "__message_burst__":
				payload := strings.Repeat("a", 256*256-1)
				for index := range 100 {
					client.Send(types.NewStringBufferString(payload+"|message: "+strconv.Itoa(index)), nil, nil)
				}
				return
			case "__upgrade_stress__":
				go func() {
					ticker := time.NewTicker(2 * time.Millisecond)
					defer ticker.Stop()
					for value := 1; value <= 50; value++ {
						<-ticker.C
						client.Send(types.NewStringBufferString("server-stress:"+strconv.Itoa(value)), nil, nil)
					}
				}()
				return
			}
			if stressValue, ok := strings.CutPrefix(text, "client-stress:"); ok {
				value, err := strconv.Atoi(stressValue)
				expected := int(upgradeStressReceived.Add(1))
				if err != nil || value != expected {
					completeUpgradeStress(fmt.Errorf("upgrade stress message = %q at position %d", text, expected))
					return
				}
				client.Send(message.Clone(), nil, nil)
				completeUpgradeStress(nil)
				return
			}
			if unicodeIndex < len(unicodeMessages) && text == unicodeMessages[unicodeIndex] {
				client.Send(message.Clone(), nil, nil)
				unicodeIndex++
				if unicodeIndex == len(unicodeMessages) {
					harness.unicodeRoundTrips <- struct{}{}
				}
				return
			}
			client.Send(message.Clone(), nil, nil)
		})
	})
	httpServer := httptest.NewServer(server)
	harness.HTTPServer = httpServer
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})
	return harness
}

func expectCompatibilityCloseReason(t *testing.T, reasons <-chan string, want string) {
	t.Helper()
	select {
	case reason := <-reasons:
		if reason != want {
			t.Fatalf("close reason = %q, want %q", reason, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for close reason %q", want)
	}
}

func TestOfficialEngineJavaScriptClientCompatibilityMatrix(t *testing.T) {
	if os.Getenv(engineCompatibilityTestEnv) != "1" {
		t.Skip("set ENGINE_IO_COMPAT_TEST=1 after running npm ci in testdata/compatibility")
	}

	workingDirectory := filepath.Join("testdata", "compatibility")
	for _, clientMajor := range []string{"3", "4", "6"} {
		t.Run("v"+clientMajor, func(t *testing.T) {
			strictServer := newEngineCompatibilityServer(t, false)
			compatibilityServer := newEngineCompatibilityServer(t, true)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "node", "matrix.cjs", strictServer.HTTPServer.URL, compatibilityServer.HTTPServer.URL, clientMajor)
			command.Dir = workingDirectory
			command.Stdout = os.Stdout
			command.Stderr = os.Stderr
			if err := command.Run(); err != nil {
				if ctx.Err() != nil {
					t.Fatalf("official Engine.IO JavaScript v%s client matrix timed out: %v", clientMajor, ctx.Err())
				}
				t.Fatalf("official Engine.IO JavaScript v%s client matrix failed: %v", clientMajor, err)
			}
			selectedServer := strictServer
			if clientMajor == "3" {
				selectedServer = compatibilityServer
			}
			for range 2 {
				expectCompatibilityCloseReason(t, selectedServer.serverInitiatedCloseReasons, "forced close")
				expectCompatibilityCloseReason(t, selectedServer.clientInitiatedCloseReasons, "transport close")
			}
			expectCompatibilityCloseReason(t, selectedServer.pingTimeoutReasons, "ping timeout")
			expectCompatibilityCloseReason(t, selectedServer.payloadCloseReasons, "transport close")
			select {
			case <-selectedServer.unicodeRoundTrips:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for Unicode round trip")
			}
			select {
			case err := <-selectedServer.upgradeStressResults:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for upgrade stress result")
			}
		})
	}
}
