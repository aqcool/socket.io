package socket

import (
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type disabledRecoveryProbeAdapter struct {
	Adapter
	persistCalls atomic.Int32
	restoreCalls atomic.Int32
}

func (a *disabledRecoveryProbeAdapter) PersistSession(*SessionToPersist) {
	a.persistCalls.Add(1)
}

func (a *disabledRecoveryProbeAdapter) RestoreSession(PrivateSessionId, string) (*Session, error) {
	a.restoreCalls.Add(1)
	return nil, nil
}

type disabledRecoveryProbeBuilder struct {
	adapter *disabledRecoveryProbeAdapter
}

func (b *disabledRecoveryProbeBuilder) New(namespace Namespace) Adapter {
	b.adapter = &disabledRecoveryProbeAdapter{Adapter: NewAdapter(namespace)}
	return b.adapter
}

func (*disabledRecoveryProbeBuilder) SupportsConnectionStateRecovery() bool { return false }

func TestOfficialRecoveryDisabledDoesNotCallAdapterSessionMethods(t *testing.T) {
	builder := &disabledRecoveryProbeBuilder{}
	options := DefaultServerOptions()
	options.SetAdapter(builder)
	server := NewServer(nil, options)
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})

	sid := socketIOPollingHandshake(t, httpServer.URL)
	socketIOPollingPush(t, httpServer.URL, sid, `40{"pid":"foo","offset":"bar"}`)
	payload := socketIOPollingPoll(t, httpServer.URL, sid)
	if !strings.HasPrefix(payload, `40{"sid":"`) || strings.Contains(payload, `"pid":`) {
		t.Fatalf("recovery-disabled CONNECT reply = %q", payload)
	}
	socketIOPollingPush(t, httpServer.URL, sid, "1")

	deadline := time.Now().Add(time.Second)
	for server.Engine().ClientsCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if builder.adapter == nil {
		t.Fatal("probe Adapter was not constructed")
	}
	if persist := builder.adapter.persistCalls.Load(); persist != 0 {
		t.Fatalf("PersistSession calls with recovery disabled = %d, want 0", persist)
	}
	if restore := builder.adapter.restoreCalls.Load(); restore != 0 {
		t.Fatalf("RestoreSession calls with recovery disabled = %d, want 0", restore)
	}
}
