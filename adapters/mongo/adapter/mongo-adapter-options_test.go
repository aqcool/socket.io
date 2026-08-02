package adapter

import (
	"fmt"
	"testing"
	"time"

	cluster "github.com/aqcool/socket.io/adapters/adapter/v3"
	"github.com/aqcool/socket.io/servers/socket/v3"
)

type mongoTimeoutHarness struct {
	cluster.ClusterAdapterWithHeartbeat
}

func (h *mongoTimeoutHarness) DoPublish(message *cluster.ClusterMessage) (cluster.Offset, error) {
	if message.Type != cluster.SERVER_SIDE_EMIT {
		return "", nil
	}
	request, ok := message.Data.(*cluster.ServerSideEmitMessage)
	if !ok || request.RequestId == nil {
		return "", nil
	}
	h.OnMessage(&cluster.ClusterMessage{
		Uid:  "responding-peer",
		Nsp:  "/",
		Type: cluster.SERVER_SIDE_EMIT_RESPONSE,
		Data: &cluster.ServerSideEmitResponse{RequestId: *request.RequestId, Packet: 2},
	}, "")
	return "", nil
}

func (*mongoTimeoutHarness) DoPublishResponse(cluster.ServerId, *cluster.ClusterResponse) error {
	return nil
}

func TestMongoAdapterOptionsRequestsTimeout(t *testing.T) {
	t.Run("unset options preserve the official default", func(t *testing.T) {
		opts := DefaultMongoAdapterOptions()
		if opts.GetRawRequestsTimeout() != nil {
			t.Fatal("raw requestsTimeout must be nil before a caller or builder sets it")
		}
		if got := opts.RequestsTimeout(); got != 0 {
			t.Fatalf("unset requestsTimeout = %v, want 0", got)
		}

		instance := MakeMongoAdapter()
		if got := instance.RequestsTimeout(); got != DefaultRequestsTimeout {
			t.Fatalf("adapter requestsTimeout = %v, want %v", got, DefaultRequestsTimeout)
		}
	})

	t.Run("custom options are copied and applied", func(t *testing.T) {
		const timeout = 275 * time.Millisecond
		source := DefaultMongoAdapterOptions()
		source.SetRequestsTimeout(timeout)

		copy := DefaultMongoAdapterOptions()
		copy.Assign(source)
		if copy.GetRawRequestsTimeout() == nil || copy.RequestsTimeout() != timeout {
			t.Fatalf("copied requestsTimeout = %v", copy.RequestsTimeout())
		}

		instance := MakeMongoAdapter()
		instance.SetOpts(copy)
		if got := instance.RequestsTimeout(); got != timeout {
			t.Fatalf("adapter requestsTimeout = %v, want %v", got, timeout)
		}
	})

	t.Run("non-positive values restore the official default", func(t *testing.T) {
		opts := DefaultMongoAdapterOptions()
		opts.SetRequestsTimeout(0)
		instance := MakeMongoAdapter()
		instance.SetOpts(opts)
		if got := instance.RequestsTimeout(); got != DefaultRequestsTimeout {
			t.Fatalf("adapter requestsTimeout = %v, want %v", got, DefaultRequestsTimeout)
		}
	})
}

func TestOfficialMongoAdapter040ServerSideEmitTimeoutText(t *testing.T) {
	const timeout = 25 * time.Millisecond
	instance := MakeMongoAdapter()
	opts := DefaultMongoAdapterOptions()
	opts.SetRequestsTimeout(timeout)
	instance.SetOpts(opts)

	concrete := instance.(*mongoAdapter)
	harness := &mongoTimeoutHarness{ClusterAdapterWithHeartbeat: concrete.ClusterAdapterWithHeartbeat}
	harness.Prototype(harness)
	harness.Construct(socket.NewNamespace(socket.NewServer(nil, nil), "/"))
	t.Cleanup(harness.CloseLocal)
	for _, uid := range []cluster.ServerId{"responding-peer", "silent-peer"} {
		harness.OnMessage(&cluster.ClusterMessage{Uid: uid, Nsp: "/", Type: cluster.HEARTBEAT}, "")
	}

	type result struct {
		responses []any
		err       error
	}
	resultChannel := make(chan result, 1)
	ack := func(responses []any, err error) {
		resultChannel <- result{responses: responses, err: err}
	}
	if err := harness.ServerSideEmit([]any{"hello", ack}); err != nil {
		t.Fatal(err)
	}

	select {
	case actual := <-resultChannel:
		if actual.err == nil || actual.err.Error() != "timeout reached: only 1 responses received out of 2" {
			t.Fatalf("timeout error = %v", actual.err)
		}
		if len(actual.responses) != 1 || fmt.Sprint(actual.responses[0]) != "2" {
			t.Fatalf("partial responses = %#v, want [2]", actual.responses)
		}
	case <-time.After(time.Second):
		t.Fatal("server-side acknowledgement timeout callback was not called")
	}
}
