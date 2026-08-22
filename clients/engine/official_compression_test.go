package engine

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/aqcool/socket.io/v4/pkg/types"
)

type officialCaptureWebSocket struct {
	compressions []bool
	writes       [][]byte
}

func (socket *officialCaptureWebSocket) Close() error { return nil }
func (socket *officialCaptureWebSocket) EnableWriteCompression(compress bool) {
	socket.compressions = append(socket.compressions, compress)
}
func (*officialCaptureWebSocket) NextReader() (int, io.Reader, error) {
	return 0, nil, io.EOF
}
func (socket *officialCaptureWebSocket) NextWriter(int) (io.WriteCloser, error) {
	return &officialCaptureWriter{onClose: func(data []byte) {
		socket.writes = append(socket.writes, data)
	}}, nil
}
func (*officialCaptureWebSocket) RemoteAddr() net.Addr            { return nil }
func (*officialCaptureWebSocket) SetReadDeadline(time.Time) error { return nil }
func (*officialCaptureWebSocket) SetReadLimit(int64)              {}
func (socket *officialCaptureWebSocket) WriteMessage(_ int, data []byte) error {
	socket.writes = append(socket.writes, append([]byte(nil), data...))
	return nil
}

type officialCaptureWriter struct {
	bytes.Buffer
	onClose func([]byte)
}

func (writer *officialCaptureWriter) Close() error {
	writer.onClose(append([]byte(nil), writer.Bytes()...))
	return nil
}

func TestOfficialClientPerMessageDeflateThreshold(t *testing.T) {
	connection := &officialCaptureWebSocket{}
	options := DefaultSocketOptions()
	options.SetPerMessageDeflate(&types.PerMessageDeflate{Threshold: 3})
	websocket := MakeWebSocket().(*websocket)
	websocket.Transport.Construct(nil, options)
	websocket.socket = &types.WebSocketConn{
		EventEmitter:        types.NewEventEmitter(),
		WebSocketConnection: connection,
	}

	websocket.doWrite(types.NewStringBufferString("hi"), true)
	websocket.doWrite(types.NewStringBufferString("hello"), true)
	if len(connection.compressions) != 2 || connection.compressions[0] || !connection.compressions[1] {
		t.Fatalf("compression decisions = %#v, want [false true]", connection.compressions)
	}
	if len(connection.writes) != 2 || string(connection.writes[0]) != "hi" || string(connection.writes[1]) != "hello" {
		t.Fatalf("writes = %#v", connection.writes)
	}
}
