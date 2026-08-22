// Package adapter defines the interface for the Valkey sharded Pub/Sub adapter for Socket.IO.
package adapter

import (
	"github.com/aqcool/socket.io/adapters/adapter/v4"
	valkey "github.com/aqcool/socket.io/adapters/valkey/v4"
)

// ShardedValkeyAdapter defines the interface for a sharded Valkey-based Socket.IO adapter.
type ShardedValkeyAdapter interface {
	adapter.ClusterAdapter

	SetValkey(*valkey.ValkeyClient)
	SetOpts(any)
}
