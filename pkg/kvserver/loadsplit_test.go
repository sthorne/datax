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
	// Skewed traffic: 80% on the hot d-prefix, 20% on the cold t-prefix.
	// Keep the true distribution so the assertion below can score the
	// chosen key against it rather than against a prefix letter.
	sent := map[string]int{}
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("d%02d", i%10)
		if i%5 == 0 {
			k = fmt.Sprintf("t%02d", i%10)
		}
		l.record(keys.Key(k))
		sent[k]++
	}
	split := l.chooseSplitKey(desc)
	if split == nil {
		t.Fatal("no split key chosen for spread traffic")
	}
	if split.Compare(desc.StartKey) <= 0 || split.Compare(desc.EndKey) >= 0 {
		t.Fatalf("split key %s outside (%s, %s)", split, desc.StartKey, desc.EndKey)
	}
	// The property under test, stated as the property: the chosen key
	// must actually divide the traffic. Scoring against the distribution
	// that was sent — rather than asserting a prefix letter — says what a
	// good split key is, and holds whatever the reservoir happened to
	// keep. The best available key here puts 50% either side; anything
	// on the cold prefix puts 90% on one.
	var left, total int
	for k, n := range sent {
		total += n
		if keys.Key(k).Compare(split) < 0 {
			left += n
		}
	}
	if share := float64(left) / float64(total); share < 0.35 || share > 0.65 {
		t.Fatalf("split key %s puts %.0f%% of traffic on its left; a balancing key is within 35-65%%",
			split, share*100)
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

// TestChooseSplitKeyIgnoresUnderObservedSamples (issue #186): a sample
// that has seen five requests must not outrank one that has seen two
// thousand merely because five is a small number.
//
// Samples enter the reservoir at different times and each counts only the
// traffic that arrives after it, so their raw left/right differences are
// not comparable. Ranking by the difference made the least-observed
// candidate win — which is uncorrelated with where the traffic median
// is, and on a real range biased every load split toward whatever was
// sampled last.
func TestChooseSplitKeyIgnoresUnderObservedSamples(t *testing.T) {
	desc := kvpb.RangeDescriptor{StartKey: keys.Key("a"), EndKey: keys.Key("z")}
	var l replicaLoad
	l.init(func() int64 { return 0 })
	l.samples = []loadSample{
		// Well observed, and close to even: 45/55 of two thousand.
		{key: keys.Key("d07"), left: 900, right: 1100},
		// A late arrival on the cold prefix. Its raw imbalance is 1, so
		// it beat everything; its share is 60/40 and it has seen five
		// requests.
		{key: keys.Key("t00"), left: 3, right: 2},
	}
	got := l.chooseSplitKey(desc)
	if got == nil || !got.Equal(keys.Key("d07")) {
		t.Fatalf("chose %v, want d07: a sample with 5 observations outranked one with 2000", got)
	}
}

// TestChooseSplitKeyRefusesUninformativeSamples: when nothing has been
// observed enough to believe, there is no load-balancing key to return.
// The caller falls back to the byte midpoint, which is honest about
// knowing nothing rather than dressing up a five-request sample.
func TestChooseSplitKeyRefusesUninformativeSamples(t *testing.T) {
	desc := kvpb.RangeDescriptor{StartKey: keys.Key("a"), EndKey: keys.Key("z")}
	var l replicaLoad
	l.init(func() int64 { return 0 })
	l.samples = []loadSample{
		{key: keys.Key("d07"), left: 3, right: 2},
		{key: keys.Key("t00"), left: 1, right: 1},
	}
	if got := l.chooseSplitKey(desc); got != nil {
		t.Fatalf("chose %v from samples of 5 and 2 observations; want nil so the caller uses the midpoint", got)
	}
}
