package socket

import (
	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
)

type (
	ParentBroadcastAdapterBuilder struct {
	}

	// A dummy adapter that only supports broadcasting to child (concrete) namespaces.
	parentBroadcastAdapter struct {
		Adapter
	}
)

func (b *ParentBroadcastAdapterBuilder) New(nsp Namespace) Adapter {
	return NewParentBroadcastAdapter(nsp)
}

func (b *ParentBroadcastAdapterBuilder) SupportsConnectionStateRecovery() bool {
	return false
}

func (b *ParentBroadcastAdapterBuilder) Capabilities() AdapterCapabilities {
	return (&AdapterBuilder{}).Capabilities()
}

func MakeParentBroadcastAdapter() ParentBroadcastAdapter {
	s := &parentBroadcastAdapter{
		Adapter: MakeAdapter(),
	}

	s.Prototype(s)

	return s
}

func NewParentBroadcastAdapter(nsp Namespace) ParentBroadcastAdapter {
	s := MakeParentBroadcastAdapter()

	s.Construct(nsp)

	return s
}

func (s *parentBroadcastAdapter) Broadcast(packet *parser.Packet, opts *BroadcastOptions) {
	for _, nsp := range s.Nsp().(ParentNamespace).Children().Keys() {
		nsp.Adapter().Broadcast(packet, opts)
	}
}
