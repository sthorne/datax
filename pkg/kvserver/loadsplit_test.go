package kvserver

import (
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

func testClock(start int64) (*int64, func() int64) {
	now := start
	return &now, func() int64 { return now }
}

// TestReplicaLoadRateWindow: the two-bucket window blends previous and
// current counts, matures after one full rotation, decays across idle
// gaps, and resets on split.
func TestReplicaLoadRateWindow(t *testing.T) {
	now, clock := testClock(time.Now().UnixNano())
	var l replicaLoad
	l.init(clock)

	k := keys.Key("k")
	if _, mature := l.qps(); mature {
		t.Fatal("fresh tracker reported mature")
	}
	// 100 requests in the first window: not yet mature.
	for i := 0; i < 100; i++ {
		l.record(k)
	}
	if _, mature := l.qps(); mature {
		t.Fatal("tracker mature before a full window")
	}

	// One window later, those 100 are the previous bucket: at the start
	// of the new window the blended rate is ~100/10s = 10 qps.
	*now += loadRateWindow.Nanoseconds()
	q, mature := l.qps()
	if !mature {
		t.Fatal("tracker not mature after a full window")
	}
	if q < 8 || q > 12 {
		t.Fatalf("blended rate %f, want ~10", q)
	}

	// A long idle gap decays to zero.
	*now += 5 * loadRateWindow.Nanoseconds()
	if q, _ := l.qps(); q != 0 {
		t.Fatalf("rate after idle gap: %f", q)
	}

	// Reset clears maturity and stamps the load-split guard.
	for i := 0; i < 10; i++ {
		l.record(k)
	}
	l.resetForSplit(true)
	if q, mature := l.qps(); q != 0 || mature {
		t.Fatalf("post-reset: q=%f mature=%v", q, mature)
	}
	if !l.recentLoadSplit(time.Minute) {
		t.Fatal("load split not stamped")
	}
	*now += 2 * time.Minute.Nanoseconds()
	if l.recentLoadSplit(time.Minute) {
		t.Fatal("load-split stamp did not expire")
	}
}

// TestChooseSplitKeyBalances: with traffic spread over many keys the
// chosen split key balances observed left/right load, sits strictly
// inside the range, and degenerate single-key traffic yields nil.
func TestChooseSplitKeyBalances(t *testing.T) {
	now, clock := testClock(time.Now().UnixNano())
	_ = now
	var l replicaLoad
	l.init(clock)

	desc := kvpb.RangeDescriptor{StartKey: keys.Key("a"), EndKey: keys.Key("z")}
	// Skewed traffic: 80% on keys d00..d09, 20% on t00..t09.
	for i := 0; i < 2000; i++ {
		if i%5 == 0 {
			l.record(keys.Key(fmt.Sprintf("t%02d", i%10)))
		} else {
			l.record(keys.Key(fmt.Sprintf("d%02d", i%10)))
		}
	}
	split := l.chooseSplitKey(desc)
	if split == nil {
		t.Fatal("no split key chosen for spread traffic")
	}
	if split.Compare(desc.StartKey) <= 0 || split.Compare(desc.EndKey) >= 0 {
		t.Fatalf("split key %s outside (%s, %s)", split, desc.StartKey, desc.EndKey)
	}
	// The traffic median lies inside the hot d-prefix: a balanced key
	// must be one of the d-keys past d00 (splitting at t* would put 80%+
	// of traffic on one side).
	if split[0] != 'd' || split.Equal(keys.Key("d00")) {
		t.Fatalf("split key %s does not balance the skewed load", split)
	}

	// Degenerate: every request on one key → nil (nothing to balance).
	var one replicaLoad
	one.init(clock)
	for i := 0; i < 500; i++ {
		one.record(keys.Key("hot"))
	}
	if k := one.chooseSplitKey(desc); k != nil {
		t.Fatalf("single-key traffic produced split key %s", k)
	}

	// Clamping: traffic entirely at/before StartKey yields nil.
	var edge replicaLoad
	edge.init(clock)
	for i := 0; i < 500; i++ {
		edge.record(keys.Key("a"))
		edge.record(keys.Key("A")) // sorts before StartKey
	}
	if k := edge.chooseSplitKey(desc); k != nil {
		t.Fatalf("out-of-range traffic produced split key %s", k)
	}
}
