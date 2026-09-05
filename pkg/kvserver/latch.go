package kvserver

import (
	"context"
	"sync"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
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
// Fairness is best-effort (a waiter re-scans after any conflicting release);
// starvation of writers behind a stream of readers is possible and accepted.

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
	done  chan struct{}
	// ranged is set when at least one span covers a key range; such latches
	// are kept in latchManager.ranged so point lookups can consult them.
	ranged bool
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
}

func newLatchManager() *latchManager {
	return &latchManager{
		held:   make(map[*latch]struct{}),
		points: make(map[string][]*latch),
		ranged: make(map[*latch]struct{}),
	}
}

// latchGuard releases an acquired latch. Release is idempotent.
type latchGuard struct {
	m    *latchManager
	l    *latch
	once sync.Once
}

func (g *latchGuard) Release() {
	g.once.Do(func() {
		g.m.mu.Lock()
		g.m.remove(g.l)
		g.m.mu.Unlock()
		close(g.l.done)
	})
}

// Acquire blocks until the given spans can be held in the given mode (all
// spans atomically — no incremental holds, hence no latch-latch deadlock),
// or ctx is done.
func (m *latchManager) Acquire(ctx context.Context, spans []latchSpan, mode latchMode) (*latchGuard, error) {
	l := &latch{spans: spans, mode: mode, done: make(chan struct{})}
	for _, sp := range spans {
		if sp.End != nil {
			l.ranged = true
			break
		}
	}
	for {
		m.mu.Lock()
		conflict := m.findConflict(l)
		if conflict == nil {
			m.insert(l)
			m.mu.Unlock()
			return &latchGuard{m: m, l: l}, nil
		}
		wait := conflict.done
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait:
			// A conflicting latch released; re-scan.
		}
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
		k := string(sp.Start)
		hs := m.points[k]
		n := hs[:0]
		for _, h := range hs {
			if h != l {
				n = append(n, h)
			}
		}
		if len(n) == 0 {
			delete(m.points, k)
			continue
		}
		clear(hs[len(n):])
		m.points[k] = n
	}
}

// conflicts reports whether a held latch conflicts with l: at least one of
// the two must write. Span overlap is checked by the caller.
func (l *latch) conflicts(held *latch) bool {
	return l.mode == latchExclusive || held.mode == latchExclusive
}

func (m *latchManager) findConflict(l *latch) *latch {
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

// wholeRangeSpan covers every addressable key — used by splits, which must
// serialize against everything on the range.
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
