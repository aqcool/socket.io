// Package adapter defines types and interfaces for the Valkey Streams-based Socket.IO adapter.
package adapter

import (
	"github.com/aqcool/socket.io/adapters/adapter/v4"
	valkey "github.com/aqcool/socket.io/adapters/valkey/v4"
)

type (
	// RawClusterMessage represents a raw message from the Valkey stream.
	RawClusterMessage map[string]any

	// ValkeyStreamsAdapter defines the interface for a Valkey Streams-based Socket.IO adapter.
	ValkeyStreamsAdapter interface {
		adapter.ClusterAdapter

		SetValkey(*valkey.ValkeyClient)
		SetOpts(any)
		Cleanup(func())
		OnRawMessage(RawClusterMessage, string) error
	}
)

// Uid returns the UID from the raw cluster message.
func (r RawClusterMessage) Uid() string {
	if value, ok := r["uid"].(string); ok {
		return value
	}
	return ""
}

// Nsp returns the namespace from the raw cluster message.
func (r RawClusterMessage) Nsp() string {
	if value, ok := r["nsp"].(string); ok {
		return value
	}
	return ""
}

// Type returns the message type from the raw cluster message.
func (r RawClusterMessage) Type() string {
	if value, ok := r["type"].(string); ok {
		return value
	}
	return ""
}

// Data returns the data field from the raw cluster message.
func (r RawClusterMessage) Data() string {
	if value, ok := r["data"].(string); ok {
		return value
	}
	return ""
}
