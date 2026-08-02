package socket

import "testing"

func TestAdapterCapabilities(t *testing.T) {
	local := CapabilitiesOf(&AdapterBuilder{})
	if !local.Broadcast || !local.RoomBroadcast || !local.BroadcastAck ||
		!local.FetchSockets || !local.SocketManagement || !local.ServerSideEmit ||
		!local.OrderedDelivery || !local.DuplicateSuppression ||
		local.ConnectionStateRecovery {
		t.Fatalf("unexpected local capabilities: %#v", local)
	}
	recovery := CapabilitiesOf(&SessionAwareAdapterBuilder{})
	if !recovery.ConnectionStateRecovery {
		t.Fatal("session-aware adapter did not declare recovery")
	}
}
