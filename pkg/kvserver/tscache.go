package kvserver

import (
	"sync"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// tsCache is the range's read timestamp cache: which key spans were read at
// which timestamps, so no write may land at or below a timestamp already
// served to a reader of an overlapping span. v1 kept a single high-water
// mark — any read pushed every writer on the range; this is the interval
// edition: only writers that actually overlap a read get pushed.
//
// Structure (matching the latch manager's stance: linear scans over a small
// bounded set beat an interval tree at prototype rates, and the narrow API
// allows swapping the implementation later):
//
//   - floor: a timestamp covering the WHOLE range with no transaction
//     attribution. Bumped on leadership acquisition (a new leader cannot
//     know what the old one served) and by generation rotation below.
//   - two generations of span entries, cur and prev. Bumps append to cur;
//     when cur fills, prev COLLAPSES into the floor (the max of its
//     timestamps — conservative, never incorrect: every prev read is still
//     covered, just range-wide) and cur becomes prev. Memory stays bounded
//     at 2×tsCacheGenerationSize entries per range, recent reads keep full
//     span precision, and old ones age into the floor.
//
// A writer AT an entry's exact timestamp is allowed only if it is the same
// transaction that performed the read (read-your-key-then-write is the
// normal transactional pattern); the floor carries no attribution, so
// writes at the floor are conservatively rejected.
//
// It is only consulted on the leader (writes and current reads are
// leader-only; follower reads at or below the closed timestamp need no
// bump — the closed timestamp already keeps every future write above
// them, see closedts.go).
type tsCache struct {
	mu    sync.Mutex
	floor hlc.Timestamp
	cur   []tsCacheEntry
	prev  []tsCacheEntry
}

type tsCacheEntry struct {
	span  latchSpan
	ts    hlc.Timestamp
	txnID uuid.UUID // reader; Nil = non-transactional / unknown
}

// tsCacheGenerationSize bounds each generation. On rotation the older
// generation folds into the floor, briefly pushing all writers on the range
// once (they forward above the floor and their narrow-span refreshes
// succeed) — the bounded-degradation trade, same shape as the transaction
// coordinator's read-span cap.
const tsCacheGenerationSize = 1024

// Bump records a read of the given spans at ts by txnID (uuid.Nil for
// non-transactional reads).
func (c *tsCache) Bump(spans []latchSpan, ts hlc.Timestamp, txnID uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.floor.Less(ts) {
		return // already covered range-wide
	}
	for _, sp := range spans {
		if sp.Start.Compare(wholeRangeSpan.Start) <= 0 && sp.End != nil && wholeRangeSpan.End.Compare(sp.End) <= 0 {
			// A whole-range bump (leadership acquisition) IS a floor: no
			// span precision to keep, no attribution to honor.
			c.floor = ts
			continue
		}
		c.cur = append(c.cur, tsCacheEntry{span: sp, ts: ts, txnID: txnID})
	}
	if len(c.cur) >= tsCacheGenerationSize {
		for _, e := range c.prev {
			if c.floor.Less(e.ts) {
				c.floor = e.ts
			}
		}
		c.prev = c.cur
		c.cur = nil
	}
}

// AllowsWrite reports whether a write to the given spans at ts by txnID
// (uuid.Nil for non-transactional) may proceed, and the timestamp it must
// exceed if not: the maximum of the floor and every overlapping entry.
func (c *tsCache) AllowsWrite(spans []latchSpan, ts hlc.Timestamp, txnID uuid.UUID) (bool, hlc.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	low := c.floor
	ok := c.floor.Less(ts)
	for _, gen := range [][]tsCacheEntry{c.prev, c.cur} {
		for _, e := range gen {
			if !overlapsAny(e.span, spans) {
				continue
			}
			if low.Less(e.ts) {
				low = e.ts
			}
			if ts.Less(e.ts) || (ts.Equal(e.ts) && (txnID == uuid.Nil || txnID != e.txnID)) {
				ok = false
			}
		}
	}
	return ok, low
}

func overlapsAny(s latchSpan, spans []latchSpan) bool {
	for _, o := range spans {
		if spansOverlap(s, o) {
			return true
		}
	}
	return false
}

// spansOverlap matches latchSpan.overlaps but never materializes Key.Next()
// — the cache scans every entry per write, and per-comparison allocations
// dominate at generation scale.
func spansOverlap(a, b latchSpan) bool {
	switch {
	case a.End == nil && b.End == nil:
		return a.Start.Equal(b.Start)
	case a.End == nil:
		return b.Start.Compare(a.Start) <= 0 && a.Start.Compare(b.End) < 0
	case b.End == nil:
		return a.Start.Compare(b.Start) <= 0 && b.Start.Compare(a.End) < 0
	default:
		return a.Start.Compare(b.End) < 0 && b.Start.Compare(a.End) < 0
	}
}
