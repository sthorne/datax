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
// Structure (issue #108: the write path consults the cache once per
// batch, and an INSERT's uniqueness probes put one point entry per row
// in it, so the scan of every entry against every write span that v2
// did was a quarter of a leader's CPU under batched ingest):
//
//   - floor: a timestamp covering the WHOLE range with no transaction
//     attribution. Bumped on leadership acquisition (a new leader cannot
//     know what the old one served) and by generation rotation below.
//   - two generations, cur and prev, each holding its point reads in a
//     map by key — one entry per key, the newest read of it — and its
//     ranged reads (scans) in a slice. A point write looks its key up in
//     both maps and scans only the ranged entries; a ranged write scans
//     everything. Bumps go to cur; when cur fills, prev COLLAPSES into
//     the floor (the max of its timestamps — conservative, never
//     incorrect: every prev read is still covered, just range-wide) and
//     cur becomes prev. Memory stays bounded at 2×tsCacheGenerationSize
//     entries per range, recent reads keep full span precision, and old
//     ones age into the floor.
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
	cur   tsCacheGen
	prev  tsCacheGen
}

// tsCacheGen is one generation of entries.
type tsCacheGen struct {
	points map[string]tsCacheRead // key → its newest read
	ranged []tsCacheEntry
	n      int // entries held (keys plus ranged spans)
}

// tsCacheRead is a point read: its timestamp and reader.
type tsCacheRead struct {
	ts    hlc.Timestamp
	txnID uuid.UUID // reader; Nil = non-transactional / unknown / several
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
// coordinator's read-span cap. A generation of point reads is one map
// entry per key however often the key is read, so a hot key set never
// rotates on its own.
const tsCacheGenerationSize = 4096

// len is the generation's entry count (tests).
func (g *tsCacheGen) len() int { return g.n }

// record adds a point read: the newest read of a key wins, and two
// readers at the same timestamp leave it unattributed (neither may write
// at exactly that timestamp, since the other read it).
func (g *tsCacheGen) record(key string, ts hlc.Timestamp, txnID uuid.UUID) {
	if g.points == nil {
		g.points = make(map[string]tsCacheRead)
	}
	old, ok := g.points[key]
	switch {
	case !ok:
		g.points[key] = tsCacheRead{ts: ts, txnID: txnID}
		g.n++
	case old.ts.Less(ts):
		g.points[key] = tsCacheRead{ts: ts, txnID: txnID}
	case old.ts.Equal(ts) && old.txnID != txnID:
		g.points[key] = tsCacheRead{ts: ts, txnID: uuid.Nil}
	}
}

// Bump records a read of the given spans at ts by txnID (uuid.Nil for
// non-transactional reads).
func (c *tsCache) Bump(spans []latchSpan, ts hlc.Timestamp, txnID uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.floor.Less(ts) {
		return // already covered range-wide
	}
	for _, sp := range spans {
		if sp.End == nil {
			c.cur.record(string(sp.Start), ts, txnID)
			continue
		}
		if sp.Start.Compare(wholeRangeSpan.Start) <= 0 && wholeRangeSpan.End.Compare(sp.End) <= 0 {
			// A whole-range bump (leadership acquisition) IS a floor: no
			// span precision to keep, no attribution to honor.
			c.floor = ts
			continue
		}
		c.cur.ranged = append(c.cur.ranged, tsCacheEntry{span: sp, ts: ts, txnID: txnID})
		c.cur.n++
	}
	if c.cur.n >= tsCacheGenerationSize {
		for _, e := range c.prev.points {
			if c.floor.Less(e.ts) {
				c.floor = e.ts
			}
		}
		for _, e := range c.prev.ranged {
			if c.floor.Less(e.ts) {
				c.floor = e.ts
			}
		}
		c.prev = c.cur
		c.cur = tsCacheGen{}
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
	note := func(rts hlc.Timestamp, rtxn uuid.UUID) {
		if low.Less(rts) {
			low = rts
		}
		if ts.Less(rts) || (ts.Equal(rts) && (txnID == uuid.Nil || txnID != rtxn)) {
			ok = false
		}
	}
	for _, gen := range [2]*tsCacheGen{&c.prev, &c.cur} {
		for _, e := range gen.ranged {
			if overlapsAny(e.span, spans) {
				note(e.ts, e.txnID)
			}
		}
		if len(gen.points) == 0 {
			continue
		}
		for _, w := range spans {
			if w.End == nil {
				if r, hit := gen.points[string(w.Start)]; hit {
					note(r.ts, r.txnID)
				}
				continue
			}
			for k, r := range gen.points {
				if spansOverlap(latchSpan{Start: []byte(k)}, w) {
					note(r.ts, r.txnID)
				}
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
