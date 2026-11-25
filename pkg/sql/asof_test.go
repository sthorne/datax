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

// TestResolveMinTimestamp (CR-AOST-001): with_min_timestamp reads at the
// freshest locally servable timestamp, never older than its bound, and
// refuses a bound in the future; follower_read_timestamp() is a fixed
// lag behind now.
func TestResolveMinTimestamp(t *testing.T) {
	now := hlc.Timestamp{WallTime: time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC).UnixNano()}
	min := time.Date(2026, 11, 18, 11, 59, 50, 0, time.UTC)
	fresh := now.AddNanos(-int64(time.Second))
	if ts, err := resolveMinTimestamp(min.Format(time.RFC3339Nano), now, fresh); err != nil || ts != fresh {
		t.Fatalf("fresh local: %v %v", ts, err)
	}
	if ts, err := resolveMinTimestamp("2026-11-18 11:59:50+00", now, hlc.Timestamp{}); err != nil || ts.WallTime != min.UnixNano() {
		t.Fatalf("PostgreSQL timestamp text, no local: %v %v", ts, err)
	}
	for _, bad := range []string{"2026-11-18T13:00:00Z", "yesterday", ""} {
		if _, err := resolveMinTimestamp(bad, now, fresh); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
	if ts, err := resolveAsOf("follower_read_timestamp()", now); err != nil || ts != now.AddNanos(-FollowerReadLag.Nanoseconds()) {
		t.Fatalf("follower_read_timestamp(): %v %v", ts, err)
	}
}

// TestAsOfOutOfRange (PG-T-005): an RFC 3339 timestamp outside the
// int64-nanosecond range is refused by AS OF SYSTEM TIME and
// with_min_timestamp, not read at the instant its nanoseconds wrap to
// (2400-01-01 used to read as of 1815).
func TestAsOfOutOfRange(t *testing.T) {
	now := hlc.Timestamp{WallTime: time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC).UnixNano()}
	for _, bad := range []string{"2400-01-01T00:00:00Z", "1500-01-01T00:00:00Z"} {
		if ts, err := resolveAsOf(bad, now); err == nil {
			t.Errorf("AS OF SYSTEM TIME %q: read at %v", bad, time.Unix(0, ts.WallTime).UTC())
		}
		if ts, err := resolveMinTimestamp(bad, now, hlc.Timestamp{}); err == nil {
			t.Errorf("with_min_timestamp(%q): read at %v", bad, time.Unix(0, ts.WallTime).UTC())
		}
	}
}
