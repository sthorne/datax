package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage"
)

func TestWindowSplitDue(t *testing.T) {
	older := func() bool { return true }
	younger := func() bool { return false }
	for _, c := range []struct {
		name          string
		ttl, storeTTL time.Duration
		holdsOlder    func() bool
		want          bool
	}{
		{"short window, young table", time.Hour, 25 * time.Hour, younger, false},
		{"short window, table older than it", time.Hour, 25 * time.Hour, older, true},
		{"window longer than the store's", 48 * time.Hour, 25 * time.Hour, younger, true},
		{"window equal to the store's", 25 * time.Hour, 25 * time.Hour, younger, false},
		{"store collects nothing at its own window", 48 * time.Hour, 0, younger, false},
		{"store collects nothing, table older than its window", 48 * time.Hour, -1, older, true},
	} {
		if got := windowSplitDue(c.ttl, c.storeTTL, c.holdsOlder); got != c.want {
			t.Errorf("%s: due = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestHoldsVersionBelow(t *testing.T) {
	env := startRouterStore(t, TestingKnobs{})
	lo, hi := keys.TableDataSpan(60)
	before := env.clock.Now()
	time.Sleep(time.Millisecond)
	env.put(t, append(lo.Clone(), 'a'), "v")
	after := env.clock.Now()
	eng := env.store.cfg.Engine
	if holdsVersionBelow(eng, lo, hi, before) {
		t.Fatal("found a version below a timestamp taken before the only write")
	}
	if !holdsVersionBelow(eng, lo, hi, after) {
		t.Fatal("missed the version written before the timestamp")
	}
	if l2, h2 := keys.TableDataSpan(61); holdsVersionBelow(eng, l2, h2, after) {
		t.Fatal("found a version in an empty span")
	}
}

// TestGCPassSplitsForAWindowOnlyWhenDue is issue #395's rule: the GC pass
// gives a table with a GC window ranges of its own once the window has
// something to act on, and not before — a table younger than a window
// shorter than the store's stays on the shared range.
func TestGCPassSplitsForAWindowOnlyWhenDue(t *testing.T) {
	ctx := context.Background()
	const table = 70
	lo, hi := keys.TableDataSpan(table)
	var ttl time.Duration
	env := startRouterStoreCfg(t, func(cfg *StoreConfig) {
		cfg.WindowSpans = func(start, end keys.Key) []WindowSpan {
			if start.Compare(hi) >= 0 || lo.Compare(end) >= 0 || (lo.Compare(start) >= 0 && end.Compare(hi) <= 0) {
				return nil
			}
			return []WindowSpan{{Start: lo, End: hi, TTL: ttl}}
		}
	})
	env.put(t, append(lo.Clone(), 'a'), "v")
	ranges := func() int {
		n := 0
		env.store.VisitReplicas(func(*Replica) bool { n++; return true })
		return n
	}
	ownRange := func() bool {
		own := false
		env.store.VisitReplicas(func(r *Replica) bool {
			d := r.Desc()
			own = own || d.StartKey.Equal(lo) && d.EndKey.Equal(hi)
			return true
		})
		return own
	}

	// An hour's window on a table written a moment ago, at the default
	// store window: nothing for the window to do, no split.
	ttl = time.Hour
	env.store.RunGCOnce(ctx, 25*time.Hour)
	if n := ranges(); n != 1 {
		t.Fatalf("a young table with a short window was split: %d ranges", n)
	}
	// The same window at a store window shorter than it: the shared range
	// holds the store's collection back, so the split is due at once.
	env.store.RunGCOnce(ctx, 30*time.Minute)
	if !ownRange() {
		t.Fatal("a window longer than the store's did not get the table ranges of its own")
	}
	checkStoreTiling(t, env.store)
}

// TestGCPassSplitsForAWindowOnceTheTableIsOlderThanIt: a short window
// becomes due once the table holds a version older than it, and the pass
// that splits also collects at the table's window.
func TestGCPassSplitsForAWindowOnceTheTableIsOlderThanIt(t *testing.T) {
	ctx := context.Background()
	const table = 71
	lo, hi := keys.TableDataSpan(table)
	ttl := 300 * time.Millisecond
	env := startRouterStoreCfg(t, func(cfg *StoreConfig) {
		cfg.WindowSpans = func(start, end keys.Key) []WindowSpan {
			if start.Compare(hi) >= 0 || lo.Compare(end) >= 0 || (lo.Compare(start) >= 0 && end.Compare(hi) <= 0) {
				return nil
			}
			return []WindowSpan{{Start: lo, End: hi, TTL: ttl}}
		}
		cfg.RetentionOverride = func(start, end keys.Key) (time.Duration, bool, bool) {
			if lo.Compare(start) <= 0 && end.Compare(hi) <= 0 {
				return ttl, false, true
			}
			return 0, false, false
		}
	})
	k := append(lo.Clone(), 'a')
	env.put(t, k, "v1")
	env.put(t, k, "v2")
	env.store.RunGCOnce(ctx, 25*time.Hour)
	if n := len(storeDescs(env.store)); n != 1 {
		t.Fatalf("split before the table was older than its window: %d ranges", n)
	}
	time.Sleep(2 * ttl)
	env.store.RunGCOnce(ctx, 25*time.Hour)
	var own *Replica
	env.store.VisitReplicas(func(r *Replica) bool {
		if d := r.Desc(); d.StartKey.Equal(lo) && d.EndKey.Equal(hi) {
			own = r
		}
		return true
	})
	if own == nil {
		t.Fatal("the table older than its window did not get ranges of its own")
	}
	c, err := storage.MVCCSpanCensus(env.store.cfg.Engine, lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	if c.Garbage != 0 || c.LiveKeys != 1 {
		t.Fatalf("the pass that split did not collect at the table's window: %+v", c)
	}
	checkStoreTiling(t, env.store)
}

func storeDescs(s *Store) []string {
	var out []string
	s.VisitReplicas(func(r *Replica) bool {
		d := r.Desc()
		out = append(out, d.StartKey.String()+"-"+d.EndKey.String())
		return true
	})
	return out
}
