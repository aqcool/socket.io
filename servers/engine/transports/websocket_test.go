package transports

import (
	"bytes"
	"io"
	"net"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/v3/pkg/types"
)

type testWebSocketConnection struct {
	closed     chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int64
	readCalls  atomic.Int64
}

func newTestWebSocketConnection() *testWebSocketConnection {
	return &testWebSocketConnection{closed: make(chan struct{})}
}

func (c *testWebSocketConnection) Close() error {
	c.closeCalls.Add(1)
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (*testWebSocketConnection) EnableWriteCompression(bool) {}

func (c *testWebSocketConnection) NextReader() (int, io.Reader, error) {
	c.readCalls.Add(1)
	<-c.closed
	return 0, nil, net.ErrClosed
}

func (*testWebSocketConnection) NextWriter(int) (io.WriteCloser, error) {
	return nopWriteCloser{Writer: &bytes.Buffer{}}, nil
}

func (*testWebSocketConnection) RemoteAddr() net.Addr            { return &net.TCPAddr{} }
func (*testWebSocketConnection) SetReadDeadline(time.Time) error { return nil }
func (*testWebSocketConnection) SetReadLimit(int64)              {}
func (*testWebSocketConnection) WriteMessage(int, []byte) error  { return nil }

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func TestWebSocketOnCloseReleasesUnderlyingConnectionOnce(t *testing.T) {
	connection := newTestWebSocketConnection()
	request := httptest.NewRequest("GET", "http://example.test/socket.io/?EIO=4&transport=websocket", nil)
	ctx := types.NewHttpContext(httptest.NewRecorder(), request)
	ctx.Websocket = &types.WebSocketConn{
		EventEmitter:        types.NewEventEmitter(),
		WebSocketConnection: connection,
	}

	transport := NewWebSocket(ctx).(*websocket)
	transport.Start()
	transport.Start()
	deadline := time.Now().Add(time.Second)
	for connection.readCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := connection.readCalls.Load(); got != 1 {
		t.Fatalf("read loops = %d, want 1", got)
	}
	transport.OnClose()
	transport.OnClose()

	if got := connection.closeCalls.Load(); got != 1 {
		t.Fatalf("underlying Close calls = %d, want 1", got)
	}
	if got := transport.ReadyState(); got != "closed" {
		t.Fatalf("transport state = %q, want closed", got)
	}
	if !transport.writeQueue.IsShuttingDown() {
		t.Fatal("write queue must be shut down with the transport")
	}
}
