package socket

import (
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v4/packet"
)

// Flags represents emission flags for a socket event, such as volatile, timeout, and queue status.
type (
	Flags struct {
		packet.Options

		Volatile  bool           `json:"volatile" msgpack:"volatile"`
		Timeout   *time.Duration `json:"timeout,omitempty" msgpack:"timeout,omitempty"`
		FromQueue bool           `json:"fromQueue" msgpack:"fromQueue"`
	}

	// QueuedPacket represents a packet that is queued for guaranteed delivery with retry support.
	// Id is for debugging; deduplication should use a unique offset.
	QueuedPacket struct {
		Id       uint64
		Args     []any
		Flags    *Flags
		Size     int64
		Ack      func([]any, error)
		Pending  atomic.Bool
		TryCount atomic.Int64
	}

	// BufferStats is a snapshot of the current per-namespace client backlog.
	BufferStats struct {
		SendPackets    int   `json:"sendPackets"`
		SendBytes      int64 `json:"sendBytes"`
		ReceivePackets int   `json:"receivePackets"`
		ReceiveBytes   int64 `json:"receiveBytes"`
		RetryPackets   int   `json:"retryPackets"`
		RetryBytes     int64 `json:"retryBytes"`
	}

	// OverflowDetails describes a packet rejected or dropped by a bounded
	// client buffer.
	OverflowDetails struct {
		Buffer        string           `json:"buffer"`
		Strategy      OverflowStrategy `json:"strategy"`
		IncomingBytes int64            `json:"incomingBytes"`
		Stats         BufferStats      `json:"stats"`
	}
)
