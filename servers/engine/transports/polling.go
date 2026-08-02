// Package transports implements the HTTP long-polling transport for Engine.IO.
package transports

import (
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/v3/pkg/log"
	"github.com/aqcool/socket.io/v3/pkg/queue"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/utils"
	"github.com/klauspost/compress/zstd"
)

var pollingLog = log.NewLog("engine:polling")

var (
	gzipWriterPool = sync.Pool{
		New: func() any {
			w, _ := gzip.NewWriterLevel(io.Discard, gzip.DefaultCompression)
			return w
		},
	}
	flateWriterPool = sync.Pool{
		New: func() any {
			w, _ := flate.NewWriter(io.Discard, flate.DefaultCompression)
			return w
		},
	}
	brotliWriterPool = sync.Pool{
		New: func() any {
			return brotli.NewWriterLevel(io.Discard, brotli.DefaultCompression)
		},
	}
	zstdWriterPool = sync.Pool{
		New: func() any {
			w, _ := zstd.NewWriter(io.Discard, zstd.WithEncoderLevel(zstd.SpeedDefault))
			return w
		},
	}
)

const (
	// DefaultPollingCloseTimeout is the default time to wait for pending writes before closing a polling transport.
	DefaultPollingCloseTimeout = 30_000 * time.Millisecond
)

type polling struct {
	Transport

	closeTimeout time.Duration

	req     atomic.Pointer[types.HttpContext]
	dataCtx atomic.Pointer[types.HttpContext]
	// dataReading distinguishes an upload whose body is still in progress
	// from the synchronous packet-dispatch phase after EOF. Closing the
	// transport must abort the former, but must let the latter return the
	// official 200/"ok" response for a protocol-violation packet.
	dataReading atomic.Bool

	shouldClose atomic.Pointer[types.Callable]
	mu          sync.Mutex
	writeQueue  *queue.Queue
}

// HTTP polling New.
func MakePolling() Polling {
	p := &polling{Transport: MakeTransport()}

	p.Prototype(p)

	return p
}

func NewPolling(ctx *types.HttpContext) Polling {
	p := MakePolling()

	p.Construct(ctx)

	return p
}

func (p *polling) Construct(ctx *types.HttpContext) {
	p.Transport.Construct(ctx)

	p.closeTimeout = DefaultPollingCloseTimeout
	p.writeQueue = queue.New()
}

func (p *polling) Name() string {
	return POLLING
}

// Overrides onRequest.
func (p *polling) OnRequest(ctx *types.HttpContext) {
	method := ctx.Method()

	switch method {
	case http.MethodGet:
		p.onPollRequest(ctx)
	case http.MethodPost:
		p.onDataRequest(ctx)
	default:
		_ = ctx.SetStatusCode(http.StatusInternalServerError)
		_, _ = ctx.Write(nil)
	}
}

// The client sends a request awaiting for us to send data.
func (p *polling) onPollRequest(ctx *types.HttpContext) {
	if p.req.Load() != nil {
		pollingLog.Debug("request overlap")
		// assert: p.res, '.req should be (un)set together'
		p.OnError("overlap from client", nil)
		_ = ctx.SetStatusCode(http.StatusBadRequest)
		_, _ = ctx.Write(nil)
		return
	}

	p.req.Store(ctx)

	pollingLog.Debug("setting request")

	onClose := types.EventListener(func(...any) {
		p.SetWritable(false)
		p.Discard()
		p.OnError("poll connection closed prematurely", nil)
	})

	ctx.SetCleanup(func() {
		ctx.RemoveListener("close", onClose)
		p.req.Store(nil)
	})

	_ = ctx.Once("close", onClose)

	p.SetWritable(true)
	p.Emit("ready")

	// if we're still writable but had a pending close, trigger an empty send
	if p.Writable() && p.shouldClose.Load() != nil {
		pollingLog.Debug("triggering empty send to append close packet")
		p.Send([]*packet.Packet{
			{
				Type: packet.NOOP,
			},
		})
	}
}

// The client sends a request with data.
func (p *polling) onDataRequest(ctx *types.HttpContext) {
	if p.dataCtx.Load() != nil {
		// assert: p.dataRes, '.dataCtx should be (un)set together'
		p.OnError("data request overlap from client", nil)
		_ = ctx.SetStatusCode(http.StatusBadRequest)
		_, _ = ctx.Write(nil)
		return
	}

	isBinary := ctx.Headers().Peek("Content-Type") == "application/octet-stream"

	if isBinary && p.Protocol() == 4 {
		p.OnError("invalid content", nil)
		_ = ctx.SetStatusCode(http.StatusBadRequest)
		_, _ = ctx.Write(nil)
		return
	}

	p.dataCtx.Store(ctx)
	p.dataReading.Store(true)

	var cleanup types.Callable

	onClose := func(...any) {
		if cleanup != nil {
			cleanup()
		}
		p.Discard()
		p.OnError("data request connection closed prematurely", nil)
	}

	cleanup = func() {
		ctx.RemoveListener("close", onClose)
		p.dataReading.Store(false)
		p.dataCtx.Store(nil)
	}

	_ = ctx.Once("close", onClose)

	maxPayload := p.MaxHttpBufferSize()
	if ctx.Request().ContentLength > maxPayload {
		cleanup()
		p.OnError("payload too large", nil)
		_ = ctx.SetStatusCode(http.StatusRequestEntityTooLarge)
		_, _ = ctx.Write(nil)
		return
	}

	var packet types.BufferInterface
	if isBinary {
		packet = types.NewBytesBuffer(nil)
	} else {
		packet = types.NewStringBuffer(nil)
	}
	if body := ctx.Request().Body; body != nil {
		read, readErr := packet.ReadFrom(io.LimitReader(body, maxPayload+1))
		_ = body.Close()
		if readErr != nil {
			cleanup()
			p.OnError("payload read error", readErr)
			_ = ctx.SetStatusCode(http.StatusBadRequest)
			_, _ = ctx.Write(nil)
			return
		}
		if read > maxPayload {
			cleanup()
			p.OnError("payload too large", nil)
			_ = ctx.SetStatusCode(http.StatusRequestEntityTooLarge)
			_, _ = ctx.Write(nil)
			return
		}
	}
	p.dataReading.Store(false)
	p.Proto().OnData(packet)

	cleanup()

	headers := types.NewParameterBag(map[string][]string{
		// text/html is required instead of text/plain to avoid an
		// unwanted download dialog on certain user-agents (GH-43)
		"Content-Type":   {"text/html"},
		"Content-Length": {"2"},
	})

	// The following process in nodejs is asynchronous.
	ctx.ResponseHeaders().With(p.headers(ctx, headers).All())
	_ = ctx.SetStatusCode(http.StatusOK)
	_, _ = io.WriteString(ctx, "ok")
}

// Processes the incoming data payload.
func (p *polling) OnData(data types.BufferInterface) {
	pollingLog.Debug(`received "%s"`, data)

	packets, err := p.Parser().DecodePayload(data)
	for _, packetData := range packets {
		if packet.CLOSE == packetData.Type {
			pollingLog.Debug("got xhr close packet")
			p.OnClose()
			return
		}

		p.OnPacket(packetData)
	}
	if err != nil {
		p.OnPacket(&packet.Packet{Type: packet.ERROR, Data: strings.NewReader("parser error")})
	}
}

// Overrides onClose.
func (p *polling) OnClose() {
	if p.Writable() {
		// close pending poll request
		p.Send([]*packet.Packet{
			{
				Type: packet.NOOP,
			},
		})
	}
	// Always tear down the write queue, even on ungraceful disconnects
	// where DoClose never runs. See the matching override on websocket
	// for the full rationale.
	p.writeQueue.TryClose()
	p.Transport.OnClose()
}

// Writes a packet payload.
func (p *polling) Send(packets []*packet.Packet) {
	p.SetWritable(false)
	p.writeQueue.Enqueue(func() { p.send(packets) })
}
func (p *polling) send(packets []*packet.Packet) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var closeAfterDrain types.Callable
	if shouldClose := p.shouldClose.Load(); shouldClose != nil {
		pollingLog.Debug("appending close packet to payload")
		packets = append(packets, &packet.Packet{
			Type: packet.CLOSE,
		})
		closeAfterDrain = *shouldClose
		p.shouldClose.Store(nil)
	}
	if closeAfterDrain != nil {
		_ = p.Once("drain", func(...any) { closeAfterDrain() })
	}

	compress := false
	for _, packetData := range packets {
		if packetData.Options != nil && packetData.Options.Compress != nil && *packetData.Options.Compress {
			compress = true
			break
		}
	}
	option := &packet.Options{Compress: utils.Ptr(compress)}

	if p.Protocol() == 3 {
		data, _ := p.Parser().EncodePayload(packets, p.SupportsBinary())
		p.write(data, option)
	} else {
		data, _ := p.Parser().EncodePayload(packets)
		p.write(data, option)
	}
}

// Writes data as response to poll request.
func (p *polling) write(data types.BufferInterface, options *packet.Options) {
	pollingLog.Debug(`writing %#v`, data)
	ctx := p.req.Load()
	if ctx == nil {
		p.OnError("polling write error", nil)
		return
	}
	p.Proto().(Polling).DoWrite(ctx, data, options, func(err error) {
		if err != nil {
			p.OnError("polling write error", err)
			return
		}
		p.Emit("drain")
	})
}

// Performs the write.
func (p *polling) DoWrite(ctx *types.HttpContext, data types.BufferInterface, options *packet.Options, callback func(error)) {
	contentType := "application/octet-stream"
	// explicit UTF-8 is required for pages not served under utf
	switch data.(type) {
	case *types.StringBuffer:
		contentType = "text/plain; charset=UTF-8"
	}

	headers := types.NewParameterBag(map[string][]string{
		"Content-Type": {contentType},
	})

	respond := func(data types.BufferInterface, length string) {
		ctx.RunCleanup()

		headers.Set("Content-Length", length)
		_, err := ctx.WriteResponse(http.StatusOK, p.headers(ctx, headers), data)
		callback(err)
	}

	if p.HttpCompression() == nil || (options != nil && options.Compress != nil && !*options.Compress) {
		respond(data, strconv.Itoa(data.Len()))
		return
	}

	if data.Len() < p.HttpCompression().Threshold {
		respond(data, strconv.Itoa(data.Len()))
		return
	}

	encoding := negotiateEncoding(ctx.Headers().Peek("Accept-Encoding"))
	if encoding == "" {
		respond(data, strconv.Itoa(data.Len()))
		return
	}

	buf, err := p.compress(data, encoding)
	if err != nil {
		ctx.RunCleanup()
		defer callback(err)

		_, _ = ctx.WriteResponse(http.StatusInternalServerError, nil, nil)
		return
	}

	headers.Set("Content-Encoding", encoding)
	respond(buf, strconv.Itoa(buf.Len()))
}

func negotiateEncoding(header string) string {
	if strings.TrimSpace(header) == "" {
		return ""
	}

	type preference struct {
		quality float64
		set     bool
	}
	preferences := make(map[string]preference)
	for value := range strings.SplitSeq(header, ",") {
		parts := strings.Split(value, ";")
		name := strings.ToLower(strings.TrimSpace(parts[0]))
		if name == "" {
			continue
		}
		quality := 1.0
		for _, parameter := range parts[1:] {
			key, raw, found := strings.Cut(strings.TrimSpace(parameter), "=")
			if !found || !strings.EqualFold(strings.TrimSpace(key), "q") {
				continue
			}
			parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
			if err != nil || parsed < 0 || parsed > 1 {
				quality = 0
			} else {
				quality = parsed
			}
		}
		preferences[name] = preference{quality: quality, set: true}
	}

	wildcard := preferences["*"]
	bestEncoding := ""
	bestQuality := 0.0
	for _, encoding := range []string{"gzip", "deflate", "br", "zstd"} {
		candidate := preferences[encoding]
		if !candidate.set {
			candidate = wildcard
		}
		if candidate.set && candidate.quality > bestQuality {
			bestEncoding = encoding
			bestQuality = candidate.quality
		}
	}
	return bestEncoding
}

// Compresses data.
func (p *polling) compress(data types.BufferInterface, encoding string) (types.BufferInterface, error) {
	pollingLog.Debug("compressing")
	buf := types.NewBytesBuffer(nil)
	switch encoding {
	case "gzip":
		gz := gzipWriterPool.Get().(*gzip.Writer)
		gz.Reset(buf)
		_, err := io.Copy(gz, data)
		closeErr := gz.Close()
		gzipWriterPool.Put(gz)
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	case "deflate":
		fl := flateWriterPool.Get().(*flate.Writer)
		fl.Reset(buf)
		_, err := io.Copy(fl, data)
		closeErr := fl.Close()
		flateWriterPool.Put(fl)
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	case "br":
		br := brotliWriterPool.Get().(*brotli.Writer)
		br.Reset(buf)
		_, err := io.Copy(br, data)
		closeErr := br.Close()
		brotliWriterPool.Put(br)
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	case "zstd":
		zd := zstdWriterPool.Get().(*zstd.Encoder)
		zd.Reset(buf)
		_, err := io.Copy(zd, data)
		closeErr := zd.Close()
		zstdWriterPool.Put(zd)
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return buf, nil
}

// Closes the transport.
func (p *polling) DoClose(fn types.Callable) {
	pollingLog.Debug("closing")

	if dataCtx := p.dataCtx.Load(); dataCtx != nil && p.dataReading.Load() && !dataCtx.IsDone() {
		pollingLog.Debug("aborting ongoing data request")
		dataCtx.ResponseHeaders().Set("Connection", "close")
		_ = dataCtx.SetStatusCode(http.StatusTooManyRequests)
		_, _ = dataCtx.Write(nil)
	}

	onClose := func() {
		if fn != nil {
			fn()
		}
		p.OnClose()
	}

	if p.Writable() {
		pollingLog.Debug("transport writable - closing right away")
		// The write queue is asynchronous. Closing the transport immediately
		// after Send() can shut the queue down before the close packet reaches
		// the outstanding poll request. Finalize only after the write drains.
		_ = p.Once("drain", func(...any) { onClose() })
		p.Send([]*packet.Packet{
			{
				Type: packet.CLOSE,
			},
		})
	} else if p.Discarded() {
		pollingLog.Debug("transport discarded - closing right away")
		onClose()
	} else {
		pollingLog.Debug("transport not writable - buffering orderly close")
		closeTimeoutTimer := utils.SetTimeout(onClose, p.closeTimeout)
		shouldClose := func() {
			utils.ClearTimeout(closeTimeoutTimer)
			onClose()
		}
		p.shouldClose.Store(&shouldClose)
	}
}

// Returns headers for a response.
func (p *polling) headers(ctx *types.HttpContext, headers *types.ParameterBag) *types.ParameterBag {
	// prevent XSS warnings on IE
	// https://github.com/socketio/socket.io/pull/1333
	if ua := ctx.UserAgent(); (len(ua) > 0) && (strings.Contains(ua, ";MSIE") || strings.Contains(ua, "Trident/")) {
		headers.Set("X-XSS-Protection", "0")
	}
	headers.Set("Cache-Control", "no-store")
	p.Emit("headers", headers, ctx)
	return headers
}
