package adapter

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/aqcool/socket.io/v4/pkg/utils"
)

// CustomClusterRequest represents a custom request in the cluster with tracking for missing responses.
type (
	CustomClusterRequest struct {
		Type        MessageType
		Resolve     func(*types.Slice[any])
		Timeout     *atomic.Pointer[utils.Timer]
		Expected    int
		Current     *atomic.Int64
		MissingUids *types.Set[ServerId]
		Responses   *types.Slice[any]
		Once        sync.Once // guards against double callback invocation (timeout vs response race)
	}

	// ResponseTimeoutErrorFormatter creates the observable error returned when
	// an inter-node request does not receive every expected response.
	ResponseTimeoutErrorFormatter func(received, expected int) error

	// ClusterAdapterWithHeartbeat extends ClusterAdapter with heartbeat and custom options support.
	ClusterAdapterWithHeartbeat interface {
		ClusterAdapter

		SetOpts(any)
		// SetRequestsTimeout configures the default timeout for inter-node
		// requests. A non-positive value restores DEFAULT_TIMEOUT.
		SetRequestsTimeout(time.Duration)
		RequestsTimeout() time.Duration
		// SetResponseTimeoutErrorFormatter allows adapters whose official wire
		// versions expose a legacy error string to retain that observable API.
		// Passing nil restores the current shared-adapter format.
		SetResponseTimeoutErrorFormatter(ResponseTimeoutErrorFormatter)
		// CloseLocal stops timers without publishing ADAPTER_CLOSE. This is
		// required by transports whose official protocol reserves message type
		// 13 for another purpose (notably @socket.io/mongo-adapter@0.4.0).
		CloseLocal()
	}
)
