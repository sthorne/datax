package kvserver

import (
	"context"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Size-based auto-splitting. Each range's approximate data size is
// replicated state (see commandSizeDelta / stageSplit); when a led range
// exceeds SplitSizeThreshold the housekeeping loop splits it at the
// byte-midpoint, clamped to a user-key boundary. A range mid-membership-
// change fails the admin op safely and is retried next tick.

// RunAutoSplitOnce splits every led range whose size exceeds the store's
// threshold, or whose sustained request rate exceeds the load threshold.
// Exported for tests and debug tooling.
func (s *Store) RunAutoSplitOnce(ctx context.Context) {
	sizeThreshold := s.cfg.SplitSizeThreshold
	loadThreshold := s.loadSplitThreshold()
	if sizeThreshold <= 0 && loadThreshold <= 0 {
		return
	}
	s.VisitReplicas(func(r *Replica) bool {
		if ctx.Err() != nil {
			return false
		}
		if !r.IsLeader() || r.isFrozen() {
			return true
		}
		if size := r.SizeBytes(); sizeThreshold > 0 && size > sizeThreshold {
			desc := r.Desc()
			splitKey, err := findSplitMidpoint(s.cfg.Engine, desc.StartKey, desc.EndKey)
			if err != nil {
				log.Warnf("%s: finding split point: %v", r.rangeID, err)
				return true
			}
			if splitKey == nil {
				return true // all bytes under one user key: nothing to split
			}
			log.Infof("%s: size %d bytes exceeds %d; auto-splitting at %s", r.rangeID, size, sizeThreshold, splitKey)
			s.cfg.Events.Record("auto-split", "%s at %d bytes (threshold %d): splitting at %s", r.rangeID, size, sizeThreshold, splitKey)
			if _, kerr := r.adminSplit(ctx, splitKey); kerr != nil {
				log.Warnf("%s: auto-split at %s: %v (will retry)", r.rangeID, splitKey, kerr)
			} else {
				metrics.AutoSplits.Inc()
				r.load.resetForSplit(false)
			}
			return true
		}
		if loadThreshold > 0 {
			s.maybeLoadSplit(ctx, r, loadThreshold)
		}
		return true
	})
}

// maybeLoadSplit splits r when its request rate has been sustained above
// threshold: the tracker must be mature (a full window observed, or a
// testing override) and not freshly reset by a recent load split.
func (s *Store) maybeLoadSplit(ctx context.Context, r *Replica, threshold float64) {
	qps, trusted := s.effectiveQPS(r)
	if !trusted || qps <= threshold || r.load.recentLoadSplit(s.loadSettleWindow()) {
		return
	}
	desc := r.Desc()
	splitKey := r.load.chooseSplitKey(desc)
	if splitKey == nil {
		// No sample balances the traffic (or none survived clamping):
		// fall back to the byte midpoint. A single hot KEY stays unsplit —
		// findSplitMidpoint returns nil for a one-key range, and splitting
		// elsewhere would not move any of its load.
		var err error
		splitKey, err = findSplitMidpoint(s.cfg.Engine, desc.StartKey, desc.EndKey)
		if err != nil || splitKey == nil {
			return
		}
	}
	log.Infof("%s: %.0f qps exceeds %.0f; load-splitting at %s", r.rangeID, qps, threshold, splitKey)
	s.cfg.Events.Record("load-split", "%s at %.0f qps (threshold %.0f): splitting at %s", r.rangeID, qps, threshold, splitKey)
	if _, kerr := r.adminSplit(ctx, splitKey); kerr != nil {
		log.Warnf("%s: load-split at %s: %v (will retry)", r.rangeID, splitKey, kerr)
		return
	}
	metrics.LoadSplits.Inc()
	// Both halves start over: the LHS's window and samples described a
	// span that no longer exists, and the RHS is brand new. The stamp is
	// what keeps the merge pass from immediately undoing this split while
	// the fresh trackers still read ~0 QPS.
	r.load.resetForSplit(true)
	if rhs := s.replicaStartingAt(splitKey); rhs != nil {
		rhs.load.resetForSplit(true)
	}
}

// findSplitMidpoint returns the first user-key boundary at or past half the
// range's stored bytes — every version of a user key stays on one side.
// Returns nil when no interior boundary exists (a single huge key).
func findSplitMidpoint(eng *storage.Engine, start, end keys.Key) (keys.Key, error) {
	total, err := spanSizeBytes(eng, start, end)
	if err != nil || total == 0 {
		return nil, err
	}
	lower := storage.EncodeMVCCKey(start, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(end, hlc.Timestamp{})
	it := eng.NewIter(lower, upper)
	defer func() { _ = it.Close() }()

	var acc int64
	var prevUser keys.Key
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		user, _, err := storage.DecodeMVCCKey(it.Key())
		if err != nil {
			return nil, err
		}
		if acc >= total/2 && prevUser != nil && !prevUser.Equal(user) {
			return keys.Key(user).Clone(), nil
		}
		prevUser = keys.Key(user).Clone()
		acc += int64(len(it.Key()) + len(it.Value()))
	}
	return nil, nil
}
