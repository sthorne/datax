package kvserver

import (
	"context"

	"github.com/sthorne/datax/pkg/keys"
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
// threshold. Exported for tests and debug tooling.
func (s *Store) RunAutoSplitOnce(ctx context.Context) {
	threshold := s.cfg.SplitSizeThreshold
	if threshold <= 0 {
		return
	}
	s.VisitReplicas(func(r *Replica) bool {
		if ctx.Err() != nil {
			return false
		}
		if !r.IsLeader() {
			return true
		}
		size := r.SizeBytes()
		if size <= threshold {
			return true
		}
		desc := r.Desc()
		splitKey, err := findSplitMidpoint(s.cfg.Engine, desc.StartKey, desc.EndKey)
		if err != nil {
			log.Warnf("%s: finding split point: %v", r.rangeID, err)
			return true
		}
		if splitKey == nil {
			return true // all bytes under one user key: nothing to split
		}
		log.Infof("%s: size %d bytes exceeds %d; auto-splitting at %s", r.rangeID, size, threshold, splitKey)
		if _, kerr := r.adminSplit(ctx, splitKey); kerr != nil {
			log.Warnf("%s: auto-split at %s: %v (will retry)", r.rangeID, splitKey, kerr)
		}
		return true
	})
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
