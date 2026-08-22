package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	enginepacket "github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/aqcool/socket.io/servers/engine/v4/transports"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

type clusterConversionErrorReader struct {
	err error
}

func (r *clusterConversionErrorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type clusterConversionHTTPResult struct {
	status int
	body   string
	err    error
}

type clusterConversionTransport struct {
	transports.Transport
	closeOnce sync.Once
	closed    chan struct{}
}

type clusterPacketFailureBus struct {
	*MemoryClusterBus
	failure error
}

func (b *clusterPacketFailureBus) Publish(ctx context.Context, message *ClusterMessage) error {
	if message != nil && message.Type == ClusterMessagePacket {
		return b.failure
	}
	return b.MemoryClusterBus.Publish(ctx, message)
}

func newClusterConversionTransport() *clusterConversionTransport {
	return &clusterConversionTransport{
		Transport: transports.MakeTransport(),
		closed:    make(chan struct{}),
	}
}

func (*clusterConversionTransport) Name() string { return transports.WEBSOCKET }

func (t *clusterConversionTransport) Close(callbacks ...types.Callable) {
	t.closeOnce.Do(func() {
		t.SetReadyState("closed")
		for _, callback := range callbacks {
			if callback != nil {
				callback()
			}
		}
		t.Emit("close")
		close(t.closed)
	})
}

func clusterConversionHandshake(
	t *testing.T,
	server ClusterServer,
	baseURL string,
) (string, *socket, <-chan string) {
	t.Helper()
	connected := make(chan *socket, 1)
	closed := make(chan string, 1)
	_ = server.On("connection", func(values ...any) {
		client, _ := values[0].(*socket)
		if client == nil {
			return
		}
		_ = client.On("close", func(values ...any) {
			reason, _ := values[0].(string)
			closed <- reason
		})
		connected <- client
	})

	sid := clusterHandshake(t, baseURL)
	select {
	case client := <-connected:
		return sid, client, closed
	case <-time.After(2 * time.Second):
		t.Fatal("owner connection was not announced")
		return "", nil, nil
	}
}

func TestClusterOwnerConversionFailureClosesRemotePollingRead(t *testing.T) {
	fixture := newOfficialClusterFixture(t, 0)
	owner := fixture.engines[0].(*clusterServer)
	edge := fixture.engines[1].(*clusterServer)
	sid, ownerSocket, ownerClosed := clusterConversionHandshake(t, owner, fixture.urls[0])

	conversionFailure := errors.New("owner packet conversion failure")
	clusterErrors := make(chan error, 1)
	reenteredErrorListener := make(chan struct{}, 1)
	_ = owner.On("cluster_error", func(values ...any) {
		if err, ok := values[0].(error); ok {
			// forwardingTransport.Send runs under socket.flushMu. A synchronous
			// cluster_error emission would deadlock this reentrant Send.
			ownerSocket.Send(strings.NewReader("reentrant"), nil, nil)
			reenteredErrorListener <- struct{}{}
			clusterErrors <- err
		}
	})

	requestContext, cancelRequest := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancelRequest()
	request, err := http.NewRequestWithContext(
		requestContext,
		http.MethodGet,
		clusterPollingURL(fixture.urls[1], sid),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan clusterConversionHTTPResult, 1)
	go func() {
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr != nil {
			result <- clusterConversionHTTPResult{err: requestErr}
			return
		}
		payload, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil {
			requestErr = readErr
		} else if closeErr != nil {
			requestErr = closeErr
		}
		result <- clusterConversionHTTPResult{
			status: response.StatusCode,
			body:   string(payload),
			err:    requestErr,
		}
	}()

	var forwarder *forwardingTransport
	waitClusterCondition(t, func() bool {
		current, ok := ownerSocket.Transport().(*forwardingTransport)
		if !ok || !current.oneShot || current.targetID != edge.NodeID() {
			return false
		}
		forwarder = current
		return edge.RemoteTransportCount() == 1
	})

	callbackCalled := make(chan struct{}, 1)
	ownerSocket.Send(
		&clusterConversionErrorReader{err: conversionFailure},
		nil,
		func(transports.Transport) { callbackCalled <- struct{}{} },
	)

	select {
	case got := <-clusterErrors:
		if !errors.Is(got, conversionFailure) {
			t.Fatalf("cluster error = %v, want wrapped conversion failure", got)
		}
	case <-time.After(time.Second):
		t.Fatal("owner did not report the packet conversion failure")
	}
	select {
	case <-reenteredErrorListener:
	case <-time.After(time.Second):
		t.Fatal("cluster_error listener could not re-enter Socket.Send")
	}

	select {
	case got := <-result:
		if got.err != nil || got.status != http.StatusOK || got.body != "1" {
			t.Fatalf("remote polling result = status %d body %q error %v, want 200 close packet", got.status, got.body, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("remote polling GET remained open after owner conversion failure")
	}

	select {
	case reason := <-ownerClosed:
		if reason != "transport error" {
			t.Fatalf("owner close reason = %q, want transport error", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("owner socket did not close after packet conversion failure")
	}
	select {
	case <-callbackCalled:
		t.Fatal("failed packet executed its success callback")
	default:
	}
	select {
	case <-forwarder.done:
	case <-time.After(time.Second):
		t.Fatal("failed owner forwarder did not finish")
	}

	waitClusterCondition(t, func() bool {
		_, ownerHasClient := owner.Clients().Load(sid)
		return !ownerHasClient && clusterRequestStateClean(edge)
	})
}

func TestClusterEdgeConversionFailureClosesBothHalves(t *testing.T) {
	fixture := newOfficialClusterFixture(t, 0)
	owner := fixture.engines[0].(*clusterServer)
	edge := fixture.engines[1].(*clusterServer)
	sid, ownerSocket, ownerClosed := clusterConversionHandshake(t, owner, fixture.urls[0])

	forwarder := newForwardingTransport(owner, ownerSocket, ownerSocket.Transport(), edge.NodeID(), false)
	if !ownerSocket.installClusterTransport(forwarder) {
		t.Fatal("could not install owner forwarding transport")
	}

	remoteEdge := newClusterConversionTransport()
	remote := edge.hookRemoteTransport(sid, owner.NodeID(), remoteEdge, false)
	if !edge.storeRemoteTransport(remote) {
		t.Fatal("could not store remote edge transport")
	}

	conversionFailure := errors.New("edge packet conversion failure")
	clusterErrors := make(chan error, 1)
	_ = edge.On("cluster_error", func(values ...any) {
		if err, ok := values[0].(error); ok {
			clusterErrors <- err
		}
	})
	// A polling POST may already be committed as 200/ok by its HTTP transport.
	// The cluster guarantee tested here is immediate teardown of both halves, so
	// the session cannot remain silently alive after conversion failed.
	remoteEdge.Emit("packet", &enginepacket.Packet{
		Type: enginepacket.MESSAGE,
		Data: &clusterConversionErrorReader{err: conversionFailure},
	})

	select {
	case got := <-clusterErrors:
		if !errors.Is(got, conversionFailure) {
			t.Fatalf("cluster error = %v, want wrapped conversion failure", got)
		}
	case <-time.After(time.Second):
		t.Fatal("edge did not report the packet conversion failure")
	}
	select {
	case <-remoteEdge.closed:
	case <-time.After(time.Second):
		t.Fatal("remote edge transport did not close")
	}
	if !remote.closed.Load() || !remoteEdge.Discarded() {
		t.Fatalf("remote edge state = closed %t discarded %t, want true/true", remote.closed.Load(), remoteEdge.Discarded())
	}
	select {
	case reason := <-ownerClosed:
		if reason != "transport error" {
			t.Fatalf("owner close reason = %q, want transport error", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("owner socket stayed open after edge conversion failure")
	}

	waitClusterCondition(t, func() bool {
		_, ownerHasClient := owner.Clients().Load(sid)
		return !ownerHasClient && edge.RemoteTransportCount() == 0
	})
}

func TestClusterEdgePacketPublishFailureClosesBothHalves(t *testing.T) {
	publishFailure := errors.New("cluster packet publish failure")
	bus := &clusterPacketFailureBus{
		MemoryClusterBus: NewMemoryClusterBus(),
		failure:          publishFailure,
	}
	clusterOptions := &ClusterOptions{
		ResponseTimeout:          250 * time.Millisecond,
		DelayedConnectionTimeout: 10 * time.Millisecond,
	}
	ownerServer, err := NewClusterServer(bus, config.DefaultServerOptions(), clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	edgeServer, err := NewClusterServer(bus, config.DefaultServerOptions(), clusterOptions)
	if err != nil {
		ownerServer.Close()
		t.Fatal(err)
	}
	owner := ownerServer.(*clusterServer)
	edge := edgeServer.(*clusterServer)
	ownerHTTP := httptest.NewServer(owner)
	t.Cleanup(func() {
		owner.Close()
		edge.Close()
		ownerHTTP.Close()
		_ = bus.Close()
	})

	sid, ownerSocket, ownerClosed := clusterConversionHandshake(t, owner, ownerHTTP.URL)
	forwarder := newForwardingTransport(owner, ownerSocket, ownerSocket.Transport(), edge.NodeID(), false)
	if !ownerSocket.installClusterTransport(forwarder) {
		t.Fatal("could not install owner forwarding transport")
	}

	remoteEdge := newClusterConversionTransport()
	remote := edge.hookRemoteTransport(sid, owner.NodeID(), remoteEdge, false)
	if !edge.storeRemoteTransport(remote) {
		t.Fatal("could not store remote edge transport")
	}
	clusterErrors := make(chan error, 1)
	_ = edge.On("cluster_error", func(values ...any) {
		if clusterErr, ok := values[0].(error); ok {
			clusterErrors <- clusterErr
		}
	})
	ownerMessages := make(chan string, 1)
	_ = ownerSocket.On("message", func(values ...any) {
		ownerMessages <- packetReaderString(values[0])
	})

	remoteEdge.Emit("packet", &enginepacket.Packet{
		Type: enginepacket.MESSAGE,
		Data: strings.NewReader("must-not-be-delivered"),
	})

	select {
	case got := <-clusterErrors:
		if !errors.Is(got, publishFailure) {
			t.Fatalf("cluster error = %v, want packet publish failure", got)
		}
	case <-time.After(time.Second):
		t.Fatal("edge did not report the packet publish failure")
	}
	select {
	case <-remoteEdge.closed:
	case <-time.After(time.Second):
		t.Fatal("remote edge transport did not close after packet publish failure")
	}
	select {
	case reason := <-ownerClosed:
		if reason != "transport error" {
			t.Fatalf("owner close reason = %q, want transport error", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("owner socket stayed open after packet publish failure")
	}
	select {
	case message := <-ownerMessages:
		t.Fatalf("failed cluster publication delivered owner message %q", message)
	default:
	}
	if !remote.closed.Load() || !remoteEdge.Discarded() {
		t.Fatalf("remote edge state = closed %t discarded %t, want true/true", remote.closed.Load(), remoteEdge.Discarded())
	}
	waitClusterCondition(t, func() bool {
		_, ownerHasClient := owner.Clients().Load(sid)
		return !ownerHasClient && edge.RemoteTransportCount() == 0
	})
}

func TestClusterPacketConversionBinaryParity(t *testing.T) {
	payload := []byte{0x00, 0x01, 0x7f, 0x80, 0xff}
	for _, test := range []struct {
		name       string
		data       func() io.Reader
		wantBinary bool
		wantType   any
	}{
		{
			name:       "StringBuffer is text",
			data:       func() io.Reader { return types.NewStringBuffer(append([]byte(nil), payload...)) },
			wantBinary: false,
			wantType:   (*types.StringBuffer)(nil),
		},
		{
			name:       "strings.Reader is text",
			data:       func() io.Reader { return strings.NewReader(string(payload)) },
			wantBinary: false,
			wantType:   (*types.StringBuffer)(nil),
		},
		{
			name:       "BytesBuffer is binary",
			data:       func() io.Reader { return types.NewBytesBuffer(append([]byte(nil), payload...)) },
			wantBinary: true,
			wantType:   (*types.BytesBuffer)(nil),
		},
		{
			name:       "bytes.Reader is binary",
			data:       func() io.Reader { return bytes.NewReader(payload) },
			wantBinary: true,
			wantType:   (*types.BytesBuffer)(nil),
		},
		{
			name:       "other readers are binary",
			data:       func() io.Reader { return bytes.NewBuffer(append([]byte(nil), payload...)) },
			wantBinary: true,
			wantType:   (*types.BytesBuffer)(nil),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			compress := false
			sourceData := test.data()
			wirePacket, err := packetToClusterPacket(&enginepacket.Packet{
				Type:    enginepacket.MESSAGE,
				Data:    sourceData,
				Options: &enginepacket.Options{Compress: &compress},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !wirePacket.HasData || wirePacket.Binary != test.wantBinary || !bytes.Equal(wirePacket.Data, payload) {
				t.Fatalf("wire packet = hasData %t binary %t data %v", wirePacket.HasData, wirePacket.Binary, wirePacket.Data)
			}
			if !wirePacket.HasCompress || wirePacket.Compress {
				t.Fatalf("wire compression = present %t value %t, want true/false", wirePacket.HasCompress, wirePacket.Compress)
			}

			decoded := clusterPacketToPacket(wirePacket)
			switch test.wantType.(type) {
			case *types.StringBuffer:
				if _, ok := decoded.Data.(*types.StringBuffer); !ok {
					t.Fatalf("decoded data type = %T, want *types.StringBuffer", decoded.Data)
				}
			case *types.BytesBuffer:
				if _, ok := decoded.Data.(*types.BytesBuffer); !ok {
					t.Fatalf("decoded data type = %T, want *types.BytesBuffer", decoded.Data)
				}
			}

			roundTrip, err := packetToClusterPacket(decoded)
			if err != nil {
				t.Fatal(err)
			}
			if roundTrip.Type != wirePacket.Type ||
				roundTrip.HasData != wirePacket.HasData ||
				roundTrip.Binary != wirePacket.Binary ||
				roundTrip.HasCompress != wirePacket.HasCompress ||
				roundTrip.Compress != wirePacket.Compress ||
				!bytes.Equal(roundTrip.Data, wirePacket.Data) {
				t.Fatalf("round trip = %#v, want %#v", roundTrip, wirePacket)
			}
			decodedPayload, err := io.ReadAll(decoded.Data)
			if err != nil || !bytes.Equal(decodedPayload, payload) {
				t.Fatalf("decoded payload = %v error %v, want %v", decodedPayload, err, payload)
			}

			// clonePacketForEvent has a seek-preserving bytes.Reader branch. Verify
			// conversion did not consume that caller-owned reader while classifying it
			// as binary.
			if original, ok := sourceData.(*bytes.Reader); ok {
				remaining, readErr := io.ReadAll(original)
				if readErr != nil || !bytes.Equal(remaining, payload) {
					t.Fatalf("original bytes.Reader = %v error %v, want unconsumed %v", remaining, readErr, payload)
				}
			}
		})
	}
}
