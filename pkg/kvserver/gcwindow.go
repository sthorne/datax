package kvserver

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Ranges of their own for tables with a GC window (issue #395).
//
// A table's gc_ttl governs only the ranges wholly inside the table: a
// range it shares with other data keeps the longest window of its
// tenants (RetentionOverride). Until issue #395 the statement that set a
// window split the table's two edges at once, and the merge queue folded
// them back after a DROP. A workload that creates and drops databases of
// such tables in a loop drove a split and a merge per edge, dozens a
// second, for tables that never lived long enough for their window to
// collect anything.
//
// The edges are split by the GC pass instead, the first time it finds a
// range straddling one at a moment when the split changes what the pass
// may do (windowSplitDue):
//
//   - the table's window is longer than the store's: the shared range
//     keeps the table's window for every tenant, holding the
//     neighbours' collection back, so the split is due at once;
//   - otherwise the shared range keeps the store's window for the
//     table, longer than its own, which only keeps history longer than
//     asked for. That matters once the table holds a version older than
//     its window, and not before: a table younger than its window has
//     nothing for it to collect, and the threshold its own range would
//     take lies before anything the table wrote.
//
// The split lands before the pass collects, so the pass that finds it
// due also applies the table's window. Deferring only ever keeps history
// longer than the window says, the direction GC policy is allowed to err
// in (see RetentionOverride).

// WindowSpan is a table's data span with a GC window of its own.
type WindowSpan struct {
	Start, End keys.Key
	TTL        time.Duration
}

// windowLeaderWait bounds how long one GC pass waits, in all, for the
// new ranges its window splits made to elect their leader here, so that
// the pass that split also collects them. A range still leaderless after
// it is split further or collected by the next pass.
const windowLeaderWait = 2 * time.Second

// windowSplitDue reports whether a range shared by a table with GC
// window ttl must be split at the table's edges now. storeTTL is the
// store's own window (<= 0: the store collects nothing at its own
// window, so the neighbours lose nothing to a longer one), and
// holdsOlder whether the table's part of the range holds a version
// older than ttl. Pure, for tests.
func windowSplitDue(ttl, storeTTL time.Duration, holdsOlder func() bool) bool {
	if storeTTL > 0 && ttl > storeTTL {
		return true
	}
	return holdsOlder()
}

// splitForWindows splits, at the edges of the tables with a GC window,
// every range this store leads that straddles one and for which the
// split is due, then waits briefly for the new ranges' leadership.
func (s *Store) splitForWindows(ctx context.Context, storeTTL time.Duration) {
	if s.cfg.WindowSpans == nil {
		return
	}
	now := s.cfg.Clock.Now()
	type due struct {
		span WindowSpan
		keys []keys.Key
	}
	var todo []due
	s.VisitReplicas(func(r *Replica) bool {
		if ctx.Err() != nil {
			return false
		}
		if !r.IsLeader() || r.isFrozen() {
			return true
		}
		desc := r.Desc()
		for _, ws := range s.cfg.WindowSpans(desc.StartKey, desc.EndKey) {
			var at []keys.Key
			for _, k := range []keys.Key{ws.Start, ws.End} {
				if desc.StartKey.Compare(k) < 0 && k.Compare(desc.EndKey) < 0 {
					at = append(at, k)
				}
			}
			if len(at) == 0 {
				continue
			}
			lo, hi := ws.Start, ws.End
			if lo.Compare(desc.StartKey) < 0 {
				lo = desc.StartKey
			}
			if hi.Compare(desc.EndKey) > 0 {
				hi = desc.EndKey
			}
			cutoff := now.AddNanos(-ws.TTL.Nanoseconds())
			if windowSplitDue(ws.TTL, storeTTL, func() bool { return holdsVersionBelow(s.cfg.Engine, lo, hi, cutoff) }) {
				todo = append(todo, due{span: ws, keys: at})
			}
		}
		return true
	})
	deadline := time.Now().Add(windowLeaderWait)
	for _, d := range todo {
		for _, k := range d.keys {
			if ctx.Err() != nil {
				return
			}
			r, ok := s.replicaForKey(k)
			if !ok || !r.IsLeader() || r.isFrozen() || r.Desc().StartKey.Equal(k) {
				continue
			}
			resp, kerr := r.adminSplit(ctx, &kvpb.AdminSplitRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
			if kerr != nil {
				log.Warnf("%s: splitting at %s for a table's GC window: %v (will retry)", r.rangeID, k, kerr)
				continue
			}
			metrics.GCWindowSplits.Inc()
			log.Infof("%s: split at %s so a table's GC window of %s governs ranges of its own", r.rangeID, k, d.span.TTL)
			// The new range's leader is elected in the background; wait
			// for it, so that the table's other edge, which the new range
			// now holds, can be split too and the pass collects both.
			s.waitForLeader(ctx, resp.Right.RangeID, deadline)
		}
	}
}

// waitForLeader waits, until deadline, for this store's replica of id to
// lead its range.
func (s *Store) waitForLeader(ctx context.Context, id base.RangeID, deadline time.Time) {
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if r, ok := s.GetReplica(id); !ok || r.IsLeader() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// holdsVersionBelow reports whether [start, end) holds a version written
// below ts. It stops at the first one, so the scan is short for any
// table old enough to need its own ranges; a younger table's part of a
// range is read whole, which is the size of the range at most and less
// than what the GC pass reads to collect the range anyway.
func holdsVersionBelow(r storage.Reader, start, end keys.Key, ts hlc.Timestamp) bool {
	lower := storage.EncodeMVCCKey(start, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(end, hlc.Timestamp{})
	it := r.NewIter(lower, upper)
	defer func() { _ = it.Close() }()
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		_, vts, err := storage.DecodeMVCCKey(it.Key())
		if err != nil {
			return true // unreadable: split, as before issue #395
		}
		if !vts.IsEmpty() && vts.Less(ts) {
			return true
		}
	}
	return false
}
