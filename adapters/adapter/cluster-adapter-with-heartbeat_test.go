package adapter

import (
	"sync/atomic"
	"testing"

	"github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/utils"
)

func TestClusterAdapterWithHeartbeatBuilder(t *testing.T) {
	builder := &ClusterAdapterWithHeartbeatBuilder{
		Opts: nil,
	}

	builder.New(socket.NewNamespace(socket.NewServer(nil, nil), "/test"))
}

type memoryClusterAdapter struct {
	ClusterAdapterWithHeartbeat
	peers map[ServerId]*memoryClusterAdapter
}

func newMemoryClusterAdapter(t *testing.T, namespace string) *memoryClusterAdapter {
	t.Helper()
	nsp := socket.NewNamespace(socket.NewServer(nil, nil), namespace)
	adapter := &memoryClusterAdapter{
		ClusterAdapterWithHeartbeat: NewClusterAdapterWithHeartbeat(nsp, nil),
		peers:                       make(map[ServerId]*memoryClusterAdapter),
	}
	adapter.Prototype(adapter)
	t.Cleanup(adapter.Close)
	return adapter
}

func (m *memoryClusterAdapter) DoPublish(message *ClusterMessage) (Offset, error) {
	for uid, peer := range m.peers {
		if uid != m.Uid() {
			peer.OnMessage(message, "")
		}
	}
	return "", nil
}

func (m *memoryClusterAdapter) DoPublishResponse(requesterUID ServerId, response *ClusterResponse) error {
	if peer := m.peers[requesterUID]; peer != nil {
		peer.OnResponse(response)
	}
	return nil
}

func TestClusterCountSocketsAndListRooms(t *testing.T) {
	first := newMemoryClusterAdapter(t, "/presence")
	second := newMemoryClusterAdapter(t, "/presence")
	peers := map[ServerId]*memoryClusterAdapter{
		first.Uid():  first,
		second.Uid(): second,
	}
	first.peers = peers
	second.peers = peers
	first.Publish(&ClusterMessage{Type: HEARTBEAT})
	second.Publish(&ClusterMessage{Type: HEARTBEAT})

	first.AddAll("first-1", types.NewSet(socket.Room("room-a")))
	second.AddAll("second-1", types.NewSet(socket.Room("room-a"), socket.Room("room-b")))
	second.AddAll("second-2", types.NewSet(socket.Room("room-b")))

	first.CountSockets(nil)(func(count uint64, err error) {
		if err != nil {
			t.Fatal(err)
		}
		if count != 3 {
			t.Fatalf("count = %d, want 3", count)
		}
	})
	first.ListRooms(nil)(func(rooms map[socket.Room]uint64, err error) {
		if err != nil {
			t.Fatal(err)
		}
		if rooms["room-a"] != 2 || rooms["room-b"] != 2 {
			t.Fatalf("unexpected room counts: %v", rooms)
		}
	})
}

func TestHeartbeatOnMessageRoutesResponsesToCustomRequests(t *testing.T) {
	adapter := newMemoryClusterAdapter(t, "/responses")
	heartbeat := adapter.ClusterAdapterWithHeartbeat.(*clusterAdapterWithHeartbeat)
	requestID := "request-1"
	peerID := ServerId("peer-1")
	resolved := make(chan struct{}, 1)

	heartbeat.customRequests.Store(requestID, &CustomClusterRequest{
		Type:        COUNT_SOCKETS,
		Resolve:     func(*types.Slice[any]) { resolved <- struct{}{} },
		Timeout:     &atomic.Pointer[utils.Timer]{},
		MissingUids: types.NewSet(peerID),
		Responses:   types.NewSlice[any](),
	})

	adapter.OnMessage(&ClusterMessage{
		Uid:  peerID,
		Nsp:  "/responses",
		Type: COUNT_SOCKETS_RESPONSE,
		Data: &CountSocketsResponse{RequestId: requestID, Count: 1},
	}, "")

	select {
	case <-resolved:
	default:
		t.Fatal("heartbeat-aware response did not resolve the custom request")
	}
}
