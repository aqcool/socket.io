// Package transports implements the WebTransport transport for Engine.IO.
package transports

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/v3/pkg/log"
	"github.com/aqcool/socket.io/v3/pkg/queue"
	"github.com/aqcool/socket.io/v3/pkg/slices"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/webtransport"
	wt "github.com/quic-go/webtransport-go"
)

var (
	wtLog = log.NewLog("engine:webtransport")
)

type webTransport struct {
	Transport

	idleTimeout time.Duration

	session    *types.WebTransportConn
	mu         sync.Mutex
	writeQueue *queue.Queue
	closeOnce  sync.Once
	startOnce  sync.Once
}

// WebTransport transport
func MakeWebTransport() WebTransport {
	w := &webTransport{Transport: MakeTransport()}

	w.Prototype(w)

	return w
}

func NewWebTransport(ctx *types.HttpContext) WebTransport {
	w := MakeWebTransport()

	w.Construct(ctx)

	return w
}

func (w *webTransport) Construct(ctx *types.HttpContext) {
	w.Transport.Construct(ctx)

	w.idleTimeout = ctx.IdleTimeout
	w.session = ctx.WebTransport
	w.writeQueue = queue.New()

	_ = w.session.On("error", func(errs ...any) {
		w.OnError("webtransport error", slices.TryGetAny[error](errs, 0))
	})
	_ = w.session.Once("close", func(...any) {
		w.OnClose()
	})

	w.SetWritable(true)
	w.SetPerMessageDeflate(nil)
}

func (w *webTransport) Start() {
	w.startOnce.Do(func() {
		go w.message()
	})
}

// Transport name
func (w *webTransport) Name() string {
	return WEBTRANSPORT
}

// Advertise upgrade support.
func (w *webTransport) HandlesUpgrades() bool {
	return true
}

func (w *webTransport) _error(err error) {
	var sessionError *wt.SessionError
	if webtransport.IsUnexpectedCloseError(err) || errors.Is(err, net.ErrClosed) || errors.As(err, &sessionError) {
		w.session.Emit("close")
	} else {
		w.session.Emit("error", err)
	}
}

// Receiving Messages
func (w *webTransport) message() {
	// Guarantee cleanup on any exit path — covers errors that _error()
	// routes as "error" (not "close") on the socket and any future paths
	// that return without calling _error at all.
	defer func() {
		if !w.writeQueue.IsShuttingDown() {
			w.session.Emit("close")
		}
	}()

	for {
		if w.idleTimeout > 0 {
			_ = w.session.SetReadDeadline(time.Now().Add(w.idleTimeout))
		}
		mt, message, err := w.session.NextReader()
		if err != nil {
			w._error(err)
			return
		}

		switch mt {
		case webtransport.BinaryMessage:
			read := types.NewBytesBuffer(nil)
			if _, err := read.ReadFrom(message); err != nil {
				w._error(err)
			} else {
				w.onMessage(read)
			}
		case webtransport.TextMessage:
			read := types.NewStringBuffer(nil)
			if _, err := read.ReadFrom(message); err != nil {
				w._error(err)
			} else {
				w.onMessage(read)
			}
		}
		if c, ok := message.(io.Closer); ok {
			_ = c.Close()
		}
	}
}

func (w *webTransport) onMessage(data types.BufferInterface) {
	wtLog.Debug(`webTransport received "%s"`, data)
	w.OnData(data)
}

// Writes a packet payload.
func (w *webTransport) Send(packets []*packet.Packet) {
	w.SetWritable(false)
	w.writeQueue.Enqueue(func() { w.send(packets) })
}
func (w *webTransport) send(packets []*packet.Packet) {
	defer func() {
		w.Emit("drain")
		w.SetWritable(true)
		w.Emit("ready")
	}()

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, packet := range packets {
		// always creates a new object since ws modifies it
		compress := true
		if packet.Options != nil {
			if packet.Options.Compress != nil && !*packet.Options.Compress {
				compress = false
			}

			if w.PerMessageDeflate() == nil && packet.Options.WsPreEncodedFrame != nil {
				mt := webtransport.BinaryMessage
				if _, ok := packet.Options.WsPreEncodedFrame.(*types.StringBuffer); ok {
					mt = webtransport.TextMessage
				}
				pm, err := webtransport.NewPreparedMessage(mt, packet.Options.WsPreEncodedFrame.Bytes())
				if err != nil {
					wtLog.Debug(`Send Error "%s"`, err.Error())
					w._error(err)
					return
				}
				if err := w.session.WritePreparedMessage(pm); err != nil {
					wtLog.Debug(`Send Error "%s"`, err.Error())
					w._error(err)
					return
				}
				continue

			}
		}

		data, err := w.Parser().EncodePacket(packet, w.SupportsBinary())
		if err != nil {
			wtLog.Debug(`Send Error "%s"`, err.Error())
			w._error(err)
			return
		}
		w.write(data, compress)
	}
}

func (w *webTransport) write(data types.BufferInterface, _ bool) {
	// if w.PerMessageDeflate() != nil {
	// 	if data.Len() < w.PerMessageDeflate().Threshold {
	// 		compress = false
	// 	}
	// }
	wtLog.Debug(`writing %s`, data)

	// w.session.EnableWriteCompression(compress)
	mt := webtransport.BinaryMessage
	if _, ok := data.(*types.StringBuffer); ok {
		mt = webtransport.TextMessage
	}
	write, err := w.session.NextWriter(mt)
	if err != nil {
		w._error(err)
		return
	}
	defer func() {
		if err := write.Close(); err != nil {
			w._error(err)
			return
		}
	}()
	if _, err := io.Copy(write, data); err != nil {
		w._error(err)
		return
	}
}

// OnClose tears down the write queue regardless of how the transport
// reaches the "closed" state. See the matching override on websocket
// for why the base path leaks the queue.loop goroutine on ungraceful
// disconnects.
func (w *webTransport) OnClose() {
	w.writeQueue.TryClose()
	w.closeConnection()
	w.Transport.OnClose()
}

func (w *webTransport) closeConnection() {
	w.closeOnce.Do(func() {
		if w.session != nil && w.session.Conn != nil {
			_ = w.session.Conn.CloseWithError(0, "")
		}
	})
}

// Closes the transport.
func (w *webTransport) DoClose(fn types.Callable) {
	wtLog.Debug(`closing WebTransport session`)
	w.writeQueue.TryClose()
	defer func() {
		w.closeConnection()
		w.Transport.OnClose()
	}()
	if fn != nil {
		fn()
	}
}
