package sql

import (
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/util/hlc"
)

func TestResolveMaxStaleness(t *testing.T) {
	now := hlc.Timestamp{WallTime: 100 * int64(time.Second)}
	bound := func(d time.Duration) hlc.Timestamp { return now.AddNanos(-d.Nanoseconds()) }

	// Local closed timestamp fresher than the bound: take it (freshest
	// locally-servable read).
	fresh := bound(1 * time.Second)
	ts, err := resolveMaxStaleness("10s", now, fresh)
	if err != nil || ts != fresh {
		t.Fatalf("fresh local: %v, %v", ts, err)
	}
	// Local closed timestamp older than the bound: clamp to now-bound
	// (those ranges fall back to leaders).
	stale := bound(30 * time.Second)
	ts, err = resolveMaxStaleness("10s", now, stale)
	if err != nil || ts != bound(10*time.Second) {
		t.Fatalf("stale local: %v, %v", ts, err)
	}
	// No local closed timestamp (pure gateway): now-bound.
	ts, err = resolveMaxStaleness("10s", now, hlc.Timestamp{})
	if err != nil || ts != bound(10*time.Second) {
		t.Fatalf("no local: %v, %v", ts, err)
	}
	// Invalid bounds.
	for _, bad := range []string{"0s", "-5s", "yesterday", ""} {
		if _, err := resolveMaxStaleness(bad, now, fresh); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}
