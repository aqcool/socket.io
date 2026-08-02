package adapter

import (
	"github.com/aqcool/socket.io/servers/socket/v3"
)

// SessionAwareAdapterBuilder is a builder for creating SessionAwareAdapter instances.
type (
	SessionAwareAdapterBuilder struct {
	}
)

func (*SessionAwareAdapterBuilder) SupportsConnectionStateRecovery() bool { return true }

func (*SessionAwareAdapterBuilder) Capabilities() socket.AdapterCapabilities {
	capabilities := (&AdapterBuilder{}).Capabilities()
	capabilities.ConnectionStateRecovery = true
	return capabilities
}

// New creates a new SessionAwareAdapter for the given Namespace.
func (*SessionAwareAdapterBuilder) New(nsp socket.Namespace) Adapter {
	return NewSessionAwareAdapter(nsp)
}

// MakeSessionAwareAdapter returns a new default SessionAwareAdapter instance.
func MakeSessionAwareAdapter() SessionAwareAdapter {
	return socket.MakeSessionAwareAdapter()
}

// NewSessionAwareAdapter creates a new SessionAwareAdapter for the given Namespace.
func NewSessionAwareAdapter(nsp socket.Namespace) SessionAwareAdapter {
	return socket.NewSessionAwareAdapter(nsp)
}
