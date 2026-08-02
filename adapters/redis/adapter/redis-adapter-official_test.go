package adapter

import (
	"errors"
	"sync/atomic"
	"testing"
)

type official830RejectingParser struct {
	decodeCalls atomic.Int64
}

func (*official830RejectingParser) Encode(any) ([]byte, error) {
	return nil, errors.New("unexpected encode")
}

func (p *official830RejectingParser) Decode([]byte, any) error {
	p.decodeCalls.Add(1)
	return errors.New("unexpected decode")
}

// TestOfficialRedisAdapter830IgnoresUnknownChannels maps the two executable
// "ignores messages from unknown channels" assertions in the official
// @socket.io/redis-adapter@8.3.0 test/specifics.ts suite. Those assertions
// deliberately attach extra subscriptions to the adapter's subscriber; the Go
// equivalent invokes the same dispatch boundary and verifies that an unknown
// pattern or direct channel never reaches the wire parser.
func TestOfficialRedisAdapter830IgnoresUnknownChannels(t *testing.T) {
	parser := &official830RejectingParser{}
	instance := &redisAdapter{
		channel:                 "socket.io#/#",
		requestChannel:          "socket.io-request#/#",
		responseChannel:         "socket.io-response#/#",
		specificResponseChannel: "socket.io-response#/#self#",
		parser:                  parser,
	}

	t.Run("redis4 pattern subscription", func(t *testing.T) {
		instance.onMessage("f?o", "foo", []byte("bar"))
	})
	t.Run("redis4 direct subscription", func(t *testing.T) {
		instance.onRequest("woot", []byte("toow"))
	})
	t.Run("redis3 and ioredis pattern subscription", func(t *testing.T) {
		instance.onMessage("f?o", "foo", []byte("bar"))
	})
	t.Run("redis3 and ioredis direct subscription", func(t *testing.T) {
		instance.onRequest("woot", []byte("toow"))
	})

	if calls := parser.decodeCalls.Load(); calls != 0 {
		t.Fatalf("unknown-channel payloads reached the parser %d times, want 0", calls)
	}
}
