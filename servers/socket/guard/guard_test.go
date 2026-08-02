package guard

import (
	"testing"
	"time"
)

func TestFixedWindowRateLimit(t *testing.T) {
	guard := &Guard{windows: make(map[string]window)}
	limit := RateLimit{Limit: 2, Window: 25 * time.Millisecond}
	if allowed, _ := guard.allow("socket:1", limit); !allowed {
		t.Fatal("first event was rejected")
	}
	if allowed, _ := guard.allow("socket:1", limit); !allowed {
		t.Fatal("second event was rejected")
	}
	if allowed, retry := guard.allow("socket:1", limit); allowed || retry <= 0 {
		t.Fatalf("third event allowed=%v retry=%v", allowed, retry)
	}
	time.Sleep(30 * time.Millisecond)
	if allowed, _ := guard.allow("socket:1", limit); !allowed {
		t.Fatal("window did not reset")
	}
}

func TestDefaultOptions(t *testing.T) {
	options := normalize(nil)
	if options.Action != Reject || options.UserID == nil ||
		options.MaxThrottleDelay <= 0 || options.ClusterQueryTimeout <= 0 {
		t.Fatalf("unexpected defaults: %#v", options)
	}
}
