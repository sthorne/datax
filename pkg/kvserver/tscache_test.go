package kvserver

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func ts(wall int64) hlc.Timestamp { return hlc.Timestamp{WallTime: wall} }

func sp(start, end string) latchSpan {
	s := latchSpan{Start: keys.Key(start)}
	if end != "" {
		s.End = keys.Key(end)
	}
	return s
}

func spans(ss ...latchSpan) []latchSpan { return ss }

// TestTSCacheDisjointWritesUnaffected: the point of the interval cache — a
// read pushes only writers that overlap it.
func TestTSCacheDisjointWritesUnaffected(t *testing.T) {
	var c tsCache
	c.Bump(spans(sp("b", "")), ts(100), uuid.Nil)

	if ok, _ := c.AllowsWrite(spans(sp("a", "")), ts(50), uuid.Nil); !ok {
		t.Fatal("disjoint point write pushed")
	}
	if ok, _ := c.AllowsWrite(spans(sp("c", "z")), ts(50), uuid.Nil); !ok {
		t.Fatal("disjoint span write pushed")
	}
	if ok, low := c.AllowsWrite(spans(sp("b", "")), ts(50), uuid.Nil); ok || !low.Equal(ts(100)) {
		t.Fatalf("overlapping write allowed (ok=%v low=%v)", ok, low)
	}
	if ok, low := c.AllowsWrite(spans(sp("a", "c")), ts(50), uuid.Nil); ok || !low.Equal(ts(100)) {
		t.Fatalf("covering span write allowed (ok=%v low=%v)", ok, low)
	}
	if ok, _ := c.AllowsWrite(spans(sp("b", "")), ts(150), uuid.Nil); !ok {
		t.Fatal("write above the read pushed")
	}
}

// TestTSCacheLowIsMaxOverlapping: the push target is the maximum over the
// floor and every overlapping entry, not the first hit.
func TestTSCacheLowIsMaxOverlapping(t *testing.T) {
	var c tsCache
	c.Bump(spans(sp("a", "m")), ts(100), uuid.Nil)
	c.Bump(spans(sp("k", "z")), ts(200), uuid.Nil)

	if _, low := c.AllowsWrite(spans(sp("l", "")), ts(50), uuid.Nil); !low.Equal(ts(200)) {
		t.Fatalf("low = %v, want 200 (max of both overlapping reads)", low)
	}
	if _, low := c.AllowsWrite(spans(sp("b", "")), ts(50), uuid.Nil); !low.Equal(ts(100)) {
		t.Fatalf("low = %v, want 100 (only the first read overlaps)", low)
	}
}

// TestTSCacheOwnReadExemption: a transaction writing at exactly the
// timestamp of its OWN read is allowed; anyone else at that timestamp is
// not, and neither is anyone below it.
func TestTSCacheOwnReadExemption(t *testing.T) {
	me, other := uuid.New(), uuid.New()
	var c tsCache
	c.Bump(spans(sp("k", "")), ts(100), me)

	if ok, _ := c.AllowsWrite(spans(sp("k", "")), ts(100), me); !ok {
		t.Fatal("own equal-timestamp write pushed")
	}
	if ok, _ := c.AllowsWrite(spans(sp("k", "")), ts(100), other); ok {
		t.Fatal("foreign equal-timestamp write allowed")
	}
	if ok, _ := c.AllowsWrite(spans(sp("k", "")), ts(100), uuid.Nil); ok {
		t.Fatal("non-transactional equal-timestamp write allowed")
	}
	if ok, _ := c.AllowsWrite(spans(sp("k", "")), ts(99), me); ok {
		t.Fatal("own BELOW-timestamp write allowed")
	}

	// A second reader at the same timestamp on the same key removes the
	// exemption for both (mirrors v1's attribution collapse, per entry).
	c.Bump(spans(sp("k", "")), ts(100), other)
	if ok, _ := c.AllowsWrite(spans(sp("k", "")), ts(100), me); ok {
		t.Fatal("equal-timestamp write allowed with a second reader present")
	}
}

// TestTSCacheFloor: whole-range bumps (leadership changes) block everything
// at or below the floor regardless of span or transaction.
func TestTSCacheFloor(t *testing.T) {
	me := uuid.New()
	var c tsCache
	c.Bump(spans(wholeRangeSpan), ts(100), uuid.Nil)
	if ok, low := c.AllowsWrite(spans(sp("anything", "")), ts(100), me); ok || !low.Equal(ts(100)) {
		t.Fatalf("write at whole-range mark allowed (ok=%v low=%v)", ok, low)
	}
	if ok, _ := c.AllowsWrite(spans(sp("anything", "")), ts(101), me); !ok {
		t.Fatal("write above whole-range mark pushed")
	}
}

// TestTSCacheRotation: overflowing generations fold the older one into the
// floor — conservative (old reads stay covered range-wide), bounded (memory
// caps at two generations), and precise for recent reads.
func TestTSCacheRotation(t *testing.T) {
	var c tsCache
	c.Bump(spans(sp("old", "")), ts(500), uuid.Nil)
	// Two full generations of unrelated reads at a lower timestamp: the
	// "old" entry ages out of span storage and into the floor.
	for i := 0; i < 2*tsCacheGenerationSize+1; i++ {
		c.Bump(spans(sp(fmt.Sprintf("f%06d", i), "")), ts(10), uuid.Nil)
	}
	if ok, low := c.AllowsWrite(spans(sp("unrelated", "")), ts(400), uuid.Nil); ok || !low.Equal(ts(500)) {
		t.Fatalf("aged-out read no longer covered (ok=%v low=%v)", ok, low)
	}
	if ok, _ := c.AllowsWrite(spans(sp("unrelated", "")), ts(600), uuid.Nil); !ok {
		t.Fatal("write above the folded floor pushed")
	}
	// Memory stays bounded.
	c.mu.Lock()
	total := len(c.cur) + len(c.prev)
	c.mu.Unlock()
	if total > 2*tsCacheGenerationSize {
		t.Fatalf("cache grew unbounded: %d entries", total)
	}
}

// TestTSCacheFloorSwallowsCoveredBumps: reads at or below the floor need no
// entries — they are already covered range-wide.
func TestTSCacheFloorSwallowsCoveredBumps(t *testing.T) {
	var c tsCache
	c.Bump(spans(wholeRangeSpan), ts(100), uuid.Nil)
	c.Bump(spans(sp("k", "")), ts(50), uuid.Nil)
	c.mu.Lock()
	n := len(c.cur)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("covered bump stored %d entries", n)
	}
}

func BenchmarkTSCacheBump(b *testing.B) {
	var c tsCache
	span := sp("some/reasonably/long/key", "")
	for i := 0; i < b.N; i++ {
		c.Bump(spans(span), ts(int64(i+1)), uuid.Nil)
	}
}

func BenchmarkTSCacheAllowsWriteFull(b *testing.B) {
	// Worst case: both generations full, the write overlaps nothing — the
	// scan visits every entry.
	var c tsCache
	for i := 0; i < 2*tsCacheGenerationSize; i++ {
		c.Bump(spans(sp(fmt.Sprintf("k%06d", i), "")), ts(int64(i+1)), uuid.Nil)
	}
	w := spans(sp("zzz-unread", ""))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.AllowsWrite(w, ts(1<<40), uuid.Nil)
	}
}
