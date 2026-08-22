package adapter

import (
	"testing"

	"github.com/aqcool/socket.io/adapters/redis/v4"
	"github.com/aqcool/socket.io/servers/socket/v4"
)

func TestShardedAdapterDecodesLegacyMessageWithoutNamespace(t *testing.T) {
	server := socket.NewServer(nil, nil)
	nsp := socket.NewNamespace(server, "/legacy")
	sharded := MakeShardedRedisAdapter().(*shardedRedisAdapter)
	sharded.ClusterAdapter.Construct(nsp)

	message, err := sharded.decodeClusterMessage([]byte(`{"uid":"old-node","type":1}`))
	if err != nil {
		t.Fatalf("decode legacy sharded message: %v", err)
	}
	if message.Nsp != "/legacy" {
		t.Fatalf("legacy message namespace = %q, want %q", message.Nsp, "/legacy")
	}
}

func TestShardedAdapterUsesSameDynamicRoomRuleForPublishAndSubscribe(t *testing.T) {
	instance := MakeShardedRedisAdapter().(*shardedRedisAdapter)

	instance.opts.SetSubscriptionMode(redis.DynamicSubscriptionMode)
	if !instance.shouldUseASeparateNamespace("abcdefghijklmnopqrstuvwx") {
		t.Fatal("24-character Go socket ID must use the dynamic channel selected by publishers")
	}
	if instance.shouldUseASeparateNamespace("abcdefghijklmnopqrst") {
		t.Fatal("20-character official Socket.IO ID must stay on the namespace channel in dynamic mode")
	}

	instance.opts.SetSubscriptionMode(redis.DynamicPrivateSubscriptionMode)
	if !instance.shouldUseASeparateNamespace("abcdefghijklmnopqrst") {
		t.Fatal("dynamic-private mode must subscribe private rooms")
	}

	instance.opts.SetSubscriptionMode(redis.StaticSubscriptionMode)
	if instance.shouldUseASeparateNamespace("public-room") {
		t.Fatal("static mode must not subscribe dynamic room channels")
	}
}
