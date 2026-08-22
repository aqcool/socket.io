package adapter

import (
	"github.com/aqcool/socket.io/servers/socket/v4"
)

// AdapterBuilder is a builder for creating Adapter instances.
type (
	AdapterBuilder struct {
	}

	// inMemoryAdapter fixes the Set.Delete return-value mismatch between the
	// generic Go collection and the JavaScript Set used by the official
	// adapter. In particular, deleting an absent sid must not emit another
	// leave-room event.
	inMemoryAdapter struct {
		Adapter
	}
)

func (*AdapterBuilder) SupportsConnectionStateRecovery() bool { return false }

func (*AdapterBuilder) Capabilities() socket.AdapterCapabilities {
	return socket.AdapterCapabilities{
		Broadcast: true, RoomBroadcast: true, BroadcastAck: true,
		FetchSockets: true, SocketManagement: true, ServerSideEmit: true,
		OrderedDelivery: true, DuplicateSuppression: true,
	}
}

// New creates a new Adapter for the given Namespace.
func (*AdapterBuilder) New(nsp socket.Namespace) Adapter {
	return NewAdapter(nsp)
}

// MakeAdapter returns a new default Adapter instance.
func MakeAdapter() Adapter {
	base := socket.MakeAdapter()
	result := &inMemoryAdapter{Adapter: base}
	base.Prototype(result)
	return result
}

// NewAdapter creates a new Adapter for the given Namespace.
func NewAdapter(nsp socket.Namespace) Adapter {
	result := MakeAdapter()
	result.Construct(nsp)
	return result
}

func (a *inMemoryAdapter) Del(id socket.SocketId, room socket.Room) {
	if rooms, ok := a.Sids().Load(id); ok && rooms.Has(room) {
		rooms.Delete(room)
	}
	a.delRoom(room, id)
}

func (a *inMemoryAdapter) DelAll(id socket.SocketId) {
	rooms, ok := a.Sids().Load(id)
	if !ok {
		return
	}
	for _, room := range rooms.Keys() {
		rooms.Delete(room)
		a.delRoom(room, id)
	}
	a.Sids().Delete(id)
}

func (a *inMemoryAdapter) delRoom(room socket.Room, id socket.SocketId) {
	ids, ok := a.Rooms().Load(room)
	if !ok || !ids.Has(id) {
		return
	}
	ids.Delete(id)
	a.Emit("leave-room", room, id)
	if ids.Len() != 0 {
		return
	}
	if _, deleted := a.Rooms().LoadAndDelete(room); deleted {
		a.Emit("delete-room", room)
	}
}
