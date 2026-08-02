package types

import (
	"io"
	"net"
	"sync"
	"time"
)

type WebSocketConnection interface {
	Close() error
	EnableWriteCompression(bool)
	NextReader() (int, io.Reader, error)
	NextWriter(int) (io.WriteCloser, error)
	RemoteAddr() net.Addr
	SetReadDeadline(time.Time) error
	SetReadLimit(int64)
	WriteMessage(int, []byte) error
}

type WebSocketConn struct {
	EventEmitter

	WebSocketConnection
	writeMu sync.Mutex
}

type lockedWebSocketWriter struct {
	io.WriteCloser
	once     sync.Once
	unlock   func()
	closeErr error
}

func (w *lockedWebSocketWriter) Close() error {
	w.once.Do(func() {
		w.closeErr = w.WriteCloser.Close()
		w.unlock()
	})
	return w.closeErr
}

// Gorilla permits one concurrent reader and one concurrent writer, but not
// multiple writers. Engine.IO's transport queue and handshake-abort path can
// otherwise write the same connection concurrently, so all wrapper writes
// share this lock. NextWriter retains ownership until its writer is closed.
func (t *WebSocketConn) WriteMessage(messageType int, data []byte) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	return t.WebSocketConnection.WriteMessage(messageType, data)
}

func (t *WebSocketConn) NextWriter(messageType int) (io.WriteCloser, error) {
	t.writeMu.Lock()
	writer, err := t.WebSocketConnection.NextWriter(messageType)
	if err != nil {
		t.writeMu.Unlock()
		return nil, err
	}
	return &lockedWebSocketWriter{WriteCloser: writer, unlock: t.writeMu.Unlock}, nil
}

func (t *WebSocketConn) EnableWriteCompression(enable bool) {
	t.writeMu.Lock()
	t.WebSocketConnection.EnableWriteCompression(enable)
	t.writeMu.Unlock()
}

func (t *WebSocketConn) Close() error {
	defer t.Emit("close")
	return t.WebSocketConnection.Close()
}
