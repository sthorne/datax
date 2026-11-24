package kvserver

import (
	"context"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// Span latches serialize overlapping requests on a range while letting
// disjoint ones run in parallel (replacing v1's range-coarse RWMutex).
//
// Invariants (documented in docs/transactions.md):
//
//	L1: any two operations with overlapping spans, at least one of which
//	    writes, are fully serialized from timestamp-cache check to apply
//	    visibility.
//	L2: a read bumps the timestamp cache BEFORE evaluating, while holding
//	    its shared latch.
//
// Together: for an overlapping write W and read R, either W serialized
// before R (R's evaluation sees W's applied effect, because W holds its
// latch until apply) or R serialized before W (W's tsCache check observes
// R's bump and is pushed above it). Non-overlapping pairs need no ordering.
//
// The implementation is a mutex-protected set of held latches. Point spans
// (the overwhelmingly common case: every Get/Put/CPut in a batch) are
// indexed by key so a conflict check for a point is a map lookup plus a
// scan of the (rare) held latches that cover key ranges. Only ranged
// requests — scans, splits, merges — pay for a linear scan of every holder.
// Fairness is best-effort for requests (a waiter re-scans after any
// conflicting release); starvation of writers behind a stream of readers is
// possible and accepted. Range-wide operations (splits, merges, range
// clears, closed-timestamp promises) are the exception: such a latch, once
// it waits, is honored by every later conflicting acquirer as if it were
// held (see latchOpts.fair), so a busy range cannot starve it.
//
// Two latches conflict when their spans overlap and at least one is
// exclusive — except that a closed-timestamp barrier (a shared latch with
// latchOpts.closedBelow = T) conflicts only with exclusive latches that
// may write at or below T (latchOpts.ts empty or <= T). See latch.conflicts.

type latchMode int

const (
	latchShared latchMode = iota
	latchExclusive
)

// latchSpan is a [Start, End) interval over addressed (global) keys.
// End == nil means the point span [Start, Start.Next()).
type latchSpan struct {
	Start, End keys.Key
}

// overlaps reports whether the two intervals intersect. It does not
// allocate: a point span [p, p.Next()) intersects [s, e) iff s <= p < e.
func (s latchSpan) overlaps(o latchSpan) bool {
	switch {
	case s.End == nil && o.End == nil:
		return s.Start.Equal(o.Start)
	case s.End == nil:
		return o.Start.Compare(s.Start) <= 0 && s.Start.Compare(o.End) < 0
	case o.End == nil:
		return s.Start.Compare(o.Start) <= 0 && o.Start.Compare(s.End) < 0
	default:
		return s.Start.Compare(o.End) < 0 && o.Start.Compare(s.End) < 0
	}
}

type latch struct {
	spans []latchSpan
	mode  latchMode
	// done is closed when the latch is released. It is made (under the
	// manager's mu) by the first acquirer that has to wait on it: most
	// latches are never waited on.
	done chan struct{}
	// ranged is set when at least one span covers a key range; such latches
	// are kept in latchManager.ranged so point lookups can consult them.
	ranged bool
	// seq orders acquirers by arrival; a fair waiter is honored only by
	// acquirers that arrived after it.
	seq uint64
	latchOpts
}

// latchOpts refines how a latch conflicts. The zero value is a plain
// request latch.
type latchOpts struct {
	// ts, on an exclusive latch, is the lowest timestamp at which its
	// batch can write an MVCC version (batchLatchOpts). Empty means
	// unknown — the latch conflicts with every barrier.
	ts hlc.Timestamp
	// noMVCC, on an exclusive latch, says its batch writes no MVCC
	// version at all (transaction records only): it never conflicts with
	// a barrier.
	noMVCC bool
	// closedBelow, on a shared latch, makes it a closed-timestamp barrier
	// at that timestamp: it drains only the in-flight writes that could
	// still land at or below it (closedts.go), and writes above it pass.
	closedBelow hlc.Timestamp
	// fair makes a waiting latch block every later acquirer it conflicts
	// with, so it waits for the holders it found and no others.
	fair bool
	// op names a range-wide operation for the range-latch wait metric
	// (empty for requests).
	op string
}

type latchManager struct {
	mu sync.Mutex
	// held is every latch currently held.
	held map[*latch]struct{}
	// points indexes holders by the exact key of each point span. A latch
	// with n point spans appears under n keys (twice under a repeated key).
	points map[string][]*latch
	// ranged is the subset of held latches with at least one ranged span.
	ranged map[*latch]struct{}
	// waiting is every fair latch that has found a conflict and is waiting
	// for it; later acquirers treat them as held. Usually empty.
	waiting map[*latch]struct{}
	seq     uint64
}

func newLatchManager() *latchManager {
	return &latchManager{
		held:    make(map[*latch]struct{}),
		points:  make(map[string][]*latch),
		ranged:  make(map[*latch]struct{}),
		waiting: make(map[*latch]struct{}),
	}
}

// latchGuard releases an acquired latch. Release is idempotent.
type latchGuard struct {
	m    *latchManager
	l    *latch
	once sync.Once
	// own is the latch l points to, allocated with its guard.
	own latch
}

func (g *latchGuard) Release() {
	g.once.Do(func() {
		g.m.mu.Lock()
		g.m.remove(g.l)
		// Removed, the latch is found by no acquirer: no waiter makes
		// done after this.
		done := g.l.done
		g.m.mu.Unlock()
		if done != nil {
			close(done)
		}
	})
}

// Acquire blocks until the given spans can be held in the given mode (all
// spans atomically — no incremental holds, hence no latch-latch deadlock),
// or ctx is done.
func (m *latchManager) Acquire(ctx context.Context, spans []latchSpan, mode latchMode) (*latchGuard, error) {
	return m.AcquireOpts(ctx, spans, mode, latchOpts{})
}

// AcquireOpts is Acquire with the conflict refinements of opts.
func (m *latchManager) AcquireOpts(ctx context.Context, spans []latchSpan, mode latchMode, opts latchOpts) (*latchGuard, error) {
	g := &latchGuard{m: m, own: latch{spans: spans, mode: mode, latchOpts: opts}}
	l := &g.own
	g.l = l
	for _, sp := range spans {
		if sp.End != nil {
			l.ranged = true
			break
		}
	}
	var waitedSince time.Time
	m.mu.Lock()
	m.seq++
	l.seq = m.seq
	for {
		conflict := m.findConflict(l)
		if conflict == nil {
			if l.fair {
				delete(m.waiting, l)
			}
			m.insert(l)
			m.mu.Unlock()
			if !waitedSince.IsZero() {
				d := time.Since(waitedSince).Seconds()
				metrics.LatchWait.Observe(d)
				if l.op != "" {
					metrics.RangeLatchWait.WithLabelValues(l.op, "acquired").Observe(d)
				}
			}
			return g, nil
		}
		if l.fair {
			m.waiting[l] = struct{}{}
		}
		if conflict.done == nil {
			conflict.done = make(chan struct{})
		}
		wait := conflict.done
		m.mu.Unlock()
		if waitedSince.IsZero() {
			waitedSince = time.Now()
		}
		select {
		case <-ctx.Done():
			m.mu.Lock()
			var done chan struct{}
			if l.fair {
				// Never held, so never released: wake whoever queued
				// behind this waiter here.
				delete(m.waiting, l)
				done = l.done
			}
			m.mu.Unlock()
			if done != nil {
				close(done)
			}
			if l.op != "" {
				metrics.RangeLatchWait.WithLabelValues(l.op, "abandoned").Observe(time.Since(waitedSince).Seconds())
			}
			return nil, ctx.Err()
		case <-wait:
			// A conflicting latch released; re-scan.
		}
		m.mu.Lock()
	}
}

func (m *latchManager) insert(l *latch) {
	m.held[l] = struct{}{}
	if l.ranged {
		m.ranged[l] = struct{}{}
	}
	for _, sp := range l.spans {
		if sp.End == nil {
			k := string(sp.Start)
			m.points[k] = append(m.points[k], l)
		}
	}
}

func (m *latchManager) remove(l *latch) {
	delete(m.held, l)
	delete(m.ranged, l)
	for _, sp := range l.spans {
		if sp.End != nil {
			continue
		}
		// The key converted in place (no allocation) to look up and
		// delete; only a key other holders keep is stored again.
		hs := m.points[string(sp.Start)]
		n := hs[:0]
		for _, h := range hs {
			if h != l {
				n = append(n, h)
			}
		}
		if len(n) == 0 {
			delete(m.points, string(sp.Start))
			continue
		}
		clear(hs[len(n):])
		m.points[string(sp.Start)] = n
	}
}

// conflicts reports whether a held latch conflicts with l: at least one of
// the two must write, and a closed-timestamp barrier conflicts only with
// a write that may land at or below its timestamp. Span overlap is checked
// by the caller.
func (l *latch) conflicts(held *latch) bool {
	switch {
	case l.mode == latchExclusive && held.mode == latchExclusive:
		return true
	case l.mode == latchExclusive:
		return writesAtOrBelow(l, held.closedBelow)
	case held.mode == latchExclusive:
		return writesAtOrBelow(held, l.closedBelow)
	}
	return false
}

// writesAtOrBelow reports whether the exclusive latch w may write at or
// below the barrier timestamp closed (always, when closed is empty: a
// plain shared latch).
func writesAtOrBelow(w *latch, closed hlc.Timestamp) bool {
	if closed.IsEmpty() {
		return true
	}
	return !w.noMVCC && (w.ts.IsEmpty() || w.ts.LessEq(closed))
}

// overlapsAnySpan reports whether any span of a overlaps any span of b.
func overlapsAnySpan(a, b []latchSpan) bool {
	for _, x := range a {
		for _, y := range b {
			if x.overlaps(y) {
				return true
			}
		}
	}
	return false
}

func (m *latchManager) findConflict(l *latch) *latch {
	// A fair waiter that arrived first is honored as if held.
	for w := range m.waiting {
		if w != l && w.seq < l.seq && l.conflicts(w) && overlapsAnySpan(l.spans, w.spans) {
			return w
		}
	}
	for _, ls := range l.spans {
		if ls.End != nil {
			// A ranged span may overlap any holder: scan them all.
			for held := range m.held {
				if !l.conflicts(held) {
					continue
				}
				for _, hs := range held.spans {
					if hs.overlaps(ls) {
						return held
					}
				}
			}
			continue
		}
		// A point span conflicts with holders of exactly that key and with
		// ranged holders whose ranged spans cover it.
		for _, held := range m.points[string(ls.Start)] {
			if l.conflicts(held) {
				return held
			}
		}
		for held := range m.ranged {
			if !l.conflicts(held) {
				continue
			}
			for _, hs := range held.spans {
				if hs.End != nil && hs.overlaps(ls) {
					return held
				}
			}
		}
	}
	return nil
}

// wholeRangeSpan covers every addressable key — used by merges and range
// clears, which must serialize against everything on the range, and by
// closed-timestamp barriers.
var wholeRangeSpan = latchSpan{Start: keys.MinKey, End: keys.MaxKey}

// latchSpans derives the latch spans and mode for a batch. Keys are
// addressed via keys.Addr so transaction-record operations latch under
// their anchor key.
func latchSpans(ba *kvpb.BatchRequest) ([]latchSpan, latchMode, error) {
	mode := latchExclusive
	if ba.IsReadOnly() {
		mode = latchShared
	}
	spans := make([]latchSpan, 0, len(ba.Requests))
	for _, u := range ba.Requests {
		req := u.GetInner()
		if req == nil {
			continue
		}
		h := req.Header()
		start, err := keys.Addr(h.Key)
		if err != nil {
			return nil, 0, err
		}
		sp := latchSpan{Start: start}
		if len(h.EndKey) > 0 {
			end, err := keys.Addr(h.EndKey)
			if err != nil {
				return nil, 0, err
			}
			sp.End = end
		}
		spans = append(spans, sp)
	}
	return spans, mode, nil
}

// batchLatchOpts gives a batch's exclusive latch the lowest timestamp at
// which it can write an MVCC version (latchOpts.ts), so a closed-timestamp
// barrier below that neither waits for it nor holds it up:
//
//   - a write, a locking read or a read in the batch: the batch's read
//     timestamp — at or below its write timestamp, and evaluation only
//     forwards both (the timestamp cache pushes a write up; a transaction
//     commits at or above every intent it wrote, and a lock is taken at
//     the read timestamp);
//   - a committing intent resolution: its commit timestamp, where the
//     version lands;
//   - a transaction-record operation (EndTxn, HeartbeatTxn, PushTxn,
//     RecoverTxn): nothing — it writes no MVCC version (the writes of a
//     one-phase commit are the batch's other requests);
//   - anything else (an aborting resolution, a savepoint rollback, GC, an
//     unknown request): unknown, so the batch conflicts with every
//     barrier, as every write did before.
//
// A batch of transaction-record operations alone is marked noMVCC.
func batchLatchOpts(ba *kvpb.BatchRequest, mode latchMode) latchOpts {
	if mode != latchExclusive {
		return latchOpts{}
	}
	var lo hlc.Timestamp
	contribute := func(ts hlc.Timestamp) bool {
		if ts.IsEmpty() {
			return false
		}
		if lo.IsEmpty() || ts.Less(lo) {
			lo = ts
		}
		return true
	}
	for _, u := range ba.Requests {
		switch q := u.GetInner().(type) {
		case *kvpb.EndTxnRequest, *kvpb.HeartbeatTxnRequest, *kvpb.PushTxnRequest, *kvpb.RecoverTxnRequest:
		case *kvpb.PutRequest, *kvpb.CPutRequest, *kvpb.DeleteRequest, *kvpb.IncrementRequest,
			*kvpb.UpdateMetaRequest, *kvpb.GetRequest, *kvpb.ScanRequest:
			if !contribute(readTimestamp(ba)) {
				return latchOpts{}
			}
		case *kvpb.ResolveIntentRequest:
			if q.Status != enginepb.COMMITTED || !contribute(q.CommitTS) {
				return latchOpts{}
			}
		default:
			return latchOpts{}
		}
	}
	if lo.IsEmpty() {
		return latchOpts{noMVCC: true}
	}
	return latchOpts{ts: lo}
}

// closedTSBarrier is the latch a closed-timestamp promise at target
// drains under (closedts.go): shared over the whole range, so readers are
// undisturbed, and conflicting only with writes that may land at or below
// target. Writes at current timestamps — target trails now by the
// closed-timestamp lag — and transaction-record operations neither delay
// the promise nor wait for it. Fair, so the writes it does wait for
// cannot starve it.
func closedTSBarrier(target hlc.Timestamp) latchOpts {
	return latchOpts{closedBelow: target, fair: true, op: "closed_ts"}
}

// splitLatchSpan is the latch a split at splitKey holds until it applies:
// exclusive over the right-hand side, [splitKey, Max). Requests there must
// not be in flight through the parent while the split lands — one that
// applied after it would write a key the new range owns through the
// parent's Raft group, and a read served after it would miss the new
// range's writes; the ones that waited find their keys moved
// (checkKeyBounds after the latch). The left-hand side needs no latch:
// everything the split changes there — the descriptor, the recomputed
// size, the abort span copied to the right — is computed at apply from
// the state in log order, and a left-hand request applies on the same
// range whether it lands before or after the split, just as it would if
// it had waited.
func splitLatchSpan(splitKey keys.Key) latchSpan {
	return latchSpan{Start: splitKey, End: keys.MaxKey}
}
