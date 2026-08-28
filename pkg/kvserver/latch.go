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
// The implementation is a mutex-protected set of held latches with a linear
// overlap scan — at prototype request rates an interval tree is unearned
// complexity, and the API allows swapping the implementation later.
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

func (s latchSpan) overlaps(o latchSpan) bool {
	sEnd, oEnd := s.End, o.End
	if sEnd == nil {
		sEnd = s.Start.Next()
	}
	if oEnd == nil {
		oEnd = o.Start.Next()
	}
	return s.Start.Compare(oEnd) < 0 && o.Start.Compare(sEnd) < 0
}

type latch struct {
	spans []latchSpan
	mode  latchMode
	done  chan struct{}
}

type latchManager struct {
	mu   sync.Mutex
	held map[*latch]struct{}
}

func newLatchManager() *latchManager {
	return &latchManager{held: make(map[*latch]struct{})}
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
		delete(g.m.held, g.l)
		g.m.mu.Unlock()
		close(g.l.done)
	})
}

// Acquire blocks until the given spans can be held in the given mode (all
// spans atomically — no incremental holds, hence no latch-latch deadlock),
// or ctx is done.
func (m *latchManager) Acquire(ctx context.Context, spans []latchSpan, mode latchMode) (*latchGuard, error) {
	l := &latch{spans: spans, mode: mode, done: make(chan struct{})}
	for {
		m.mu.Lock()
		conflict := m.findConflict(l)
		if conflict == nil {
			m.held[l] = struct{}{}
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

func (m *latchManager) findConflict(l *latch) *latch {
	for held := range m.held {
		if l.mode == latchShared && held.mode == latchShared {
			continue
		}
		for _, hs := range held.spans {
			for _, ls := range l.spans {
				if hs.overlaps(ls) {
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
