package kvserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// A purge (issue #295) is garbage collection that does not wait: over the
// ranges of a key span it collects at a threshold of now instead of now
// minus the GC window, and then makes the collected bytes leave the files
// on every replica. It is driven from the server (server.Node.PurgeSpan),
// one range at a time, in two steps that this file provides:
//
//   - PurgeRange, on the range's leader: resolve what intents can be
//     resolved, collect garbage at the leader's current time through the
//     ordinary replicated GC command, and truncate the raft log up to the
//     applied index. The log is the other place the deleted values live:
//     every write was an entry in it before it was a version.
//   - PurgeReplica, on each replica: wait for the leader's applied index,
//     flush, drop the truncated log entries, compact the range's span on
//     the state engine and its log span on the raft engine, and count what
//     the span holds, below the MVCC layer.
//
// The collection itself is the same replicated command as any other GC,
// so what makes it safe is what makes that safe: once it applies, the
// range's threshold is the purge time, and every batch at or below it is
// refused. A transaction that started before the purge and comes to the
// range afterwards gets that refusal, as an error; it is never served a
// result with rows missing. Keys under an unresolved intent are skipped,
// as always, and show up in the replica's count as intents.

// ErrPurgeNotLeader is PurgeRange's answer on a store that does not lead
// the range; the caller tries the next replica.
var ErrPurgeNotLeader = errors.New("not the range's leader")

// purgeIntentWait bounds how long a purge waits for transactions with
// intents in the range to finish before collecting around them.
const purgeIntentWait = 5 * time.Second

// purgeFollowerWait bounds how long the leader waits for its followers to
// hold the collection before it truncates the log beneath them. A
// follower that has not caught up by then is caught up by a snapshot.
const purgeFollowerWait = 5 * time.Second

// purgeTruncateWait bounds how long a replica goes on flushing to get
// its truncated log entries dropped before it reports that it could not.
const purgeTruncateWait = 10 * time.Second

// PurgeRangeResult is what the leader's step reports.
type PurgeRangeResult struct {
	// Desc is the range as the collection left it: narrower than the
	// range the purge listed when a split applied before the collection.
	Desc kvpb.RangeDescriptor
	// Threshold is the time the range collected at.
	Threshold hlc.Timestamp
	// Removed is the number of versions the collection deleted.
	Removed int
	// AppliedIndex is the leader's applied index once the collection and
	// the truncation have applied; a replica at or past it holds both.
	AppliedIndex uint64
}

// intentResolver is the part of kvclient.DB a purge uses to clear the
// intents of finished transactions out of its way.
type intentResolver interface {
	PushAndResolveIntents(ctx context.Context, intents []storage.Intent) (bool, error)
}

// PurgeRange runs the leader's step for rangeID, which must lie wholly
// inside [start, end): the threshold a purge raises covers a whole range,
// and a range holding other data would lose that data's history too.
func (s *Store) PurgeRange(ctx context.Context, rangeID base.RangeID, start, end keys.Key) (PurgeRangeResult, error) {
	return s.PurgeRangeBelow(ctx, rangeID, start, end, hlc.Timestamp{})
}

// PurgeRangeBelow is PurgeRange with the collection's threshold held at
// or below limit when limit is set (issue #376): the purging node reads
// the backups in progress and passes the highest threshold none of them
// is cut off by. The threshold is the leader's now or limit, whichever
// is lower.
func (s *Store) PurgeRangeBelow(ctx context.Context, rangeID base.RangeID, start, end keys.Key, limit hlc.Timestamp) (PurgeRangeResult, error) {
	var res PurgeRangeResult
	r, ok := s.GetReplica(rangeID)
	if !ok || !r.IsLeader() {
		return res, ErrPurgeNotLeader
	}
	if r.isFrozen() {
		return res, fmt.Errorf("%s is frozen for a merge; purge again once it has finished", rangeID)
	}
	desc := r.Desc()
	if desc.StartKey.Compare(start) < 0 || end.Compare(desc.EndKey) < 0 {
		return res, fmt.Errorf("%s spans %s to %s, beyond the purged span: a purge acts on whole ranges", rangeID, keys.Pretty(desc.StartKey), keys.Pretty(desc.EndKey))
	}
	r.resolveIntentsForPurge(ctx, desc)

	res.Threshold = s.cfg.Clock.Now()
	if !limit.IsEmpty() && limit.Less(res.Threshold) {
		res.Threshold = limit
	}
	if k := s.cfg.TestingKnobs.BeforePurgeCollect; k != nil {
		k(rangeID)
	}
	removed, err := r.collectGarbage(ctx, res.Threshold, false, nil)
	if err != nil {
		return res, err
	}
	res.Removed = removed
	// The range as the collection left it, not as it was checked: a split
	// that applied in between (a load-based split, the purge's own of
	// another span) took the right-hand side away, with the threshold
	// that side had then, and the purge must reach that side itself. A
	// split that applies after the collection copies the new threshold.
	res.Desc = r.Desc()
	gcIndex := r.AppliedIndex()

	// Followers first, then the log: truncating beneath a follower that
	// is a few entries behind costs it a snapshot it did not need.
	deadline := time.Now().Add(purgeFollowerWait)
	for !r.followersHold(gcIndex) && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if err := r.truncateLog(ctx, 0, 1); err != nil {
		return res, fmt.Errorf("%s: truncating the raft log: %w", rangeID, err)
	}
	res.AppliedIndex = r.AppliedIndex()
	return res, nil
}

// followersHold reports whether every replica's log holds index.
func (r *Replica) followersHold(index uint64) bool {
	st, ok := r.raftStatus()
	if !ok {
		return false
	}
	for _, pr := range st.Progress {
		if pr.Match < index {
			return false
		}
	}
	return true
}

// resolveIntentsForPurge resolves the intents in the range whose
// transactions have finished, and waits a bounded time for those that
// have not. Collection skips a key with an intent whole, so an intent
// left by a DELETE that committed a moment ago would otherwise keep that
// row's versions through the purge.
func (r *Replica) resolveIntentsForPurge(ctx context.Context, desc kvpb.RangeDescriptor) {
	resolver, ok := r.store.getSender().(intentResolver)
	if !ok {
		return
	}
	deadline := time.Now().Add(purgeIntentWait)
	for {
		intents, err := r.rangeIntents(desc)
		if err != nil || len(intents) == 0 {
			return
		}
		done, err := resolver.PushAndResolveIntents(ctx, intents)
		if err != nil {
			log.Debugf("%s: resolving intents before a purge: %v", r.rangeID, err)
		}
		if done && err == nil {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// rangeIntents lists the intents in the range's MVCC span.
func (r *Replica) rangeIntents(desc kvpb.RangeDescriptor) ([]storage.Intent, error) {
	lower := storage.EncodeMVCCKey(desc.StartKey, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(desc.EndKey, hlc.Timestamp{})
	snap := r.store.cfg.Engine.NewSnapshot()
	defer func() { _ = snap.Close() }()
	it := snap.NewIter(lower, upper)
	defer func() { _ = it.Close() }()
	var out []storage.Intent
	ok := it.SeekGE(lower)
	for ok {
		userKey, ts, err := storage.DecodeMVCCKey(it.Key())
		if err != nil {
			return nil, err
		}
		cur := keys.Key(userKey).Clone()
		if ts.IsEmpty() {
			meta, err := storage.DecodeMVCCMetadata(it.Value())
			if err != nil {
				return nil, err
			}
			out = append(out, storage.Intent{Key: cur, Txn: meta.Txn})
		}
		// One look per user key: the metadata record sorts first.
		ok = it.SeekGE(storage.EncodeMVCCKey(cur.Next(), hlc.Timestamp{}))
	}
	return out, nil
}

// PurgeReplica runs a replica's step for rangeID once the replica has
// applied index, and reports what the range's span then holds.
func (s *Store) PurgeReplica(ctx context.Context, rangeID base.RangeID, index uint64) (kvpb.PurgeReplicaReport, error) {
	rep := kvpb.PurgeReplicaReport{RangeID: rangeID, NodeID: s.cfg.NodeID}
	if _, err := s.WaitForApplied(ctx, rangeID, index); err != nil {
		return rep, err
	}
	r, ok := s.GetReplica(rangeID)
	if !ok {
		return rep, fmt.Errorf("%s: no replica on n%d", rangeID, s.cfg.NodeID)
	}
	desc := r.Desc()
	s.purgeMu.Lock()
	defer s.purgeMu.Unlock()

	// The flush makes the collection durable on a store without a
	// write-ahead log, which is what lets the truncated log entries go:
	// until then a crash would replay them.
	lower := storage.EncodeMVCCKey(desc.StartKey, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(desc.EndKey, hlc.Timestamp{})
	if err := s.cfg.Engine.CompactSpan(ctx, lower, upper); err != nil {
		return rep, fmt.Errorf("compacting the span: %w", err)
	}
	// The truncation the leader proposed has applied here, and its log
	// entries are deleted once the flush above is known to cover it.
	// What a replica knows is sampled (noteAppliedSeq keeps the newest
	// applied index and its batch): an entry that applies between the
	// flush and this point replaces the sample the flush covered with
	// one it did not, and the truncation waits for the next flush. So
	// flush again until it has gone through; compacting the log before
	// the entries are deleted would only rewrite them into a new file.
	deadline := time.Now().Add(purgeTruncateWait)
	for {
		if err := r.maybeTruncateDeferred(); err != nil {
			return rep, fmt.Errorf("dropping truncated log entries: %w", err)
		}
		if !r.truncatePending() {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return rep, errors.New("the truncated raft log entries are still waiting on a flush")
		}
		if err := s.flushState(); err != nil {
			return rep, fmt.Errorf("flushing for the log truncation: %w", err)
		}
	}
	logLo := keys.RaftLogPrefix(rangeID)
	if err := s.raftEngine().CompactSpan(ctx, logLo, logLo.PrefixEnd()); err != nil {
		return rep, fmt.Errorf("compacting the raft log: %w", err)
	}
	rep.Compacted = true

	census, err := storage.MVCCSpanCensus(s.cfg.Engine, desc.StartKey, desc.EndKey)
	if err != nil {
		return rep, err
	}
	rep.LiveKeys, rep.Garbage, rep.Tombstones, rep.Intents = census.LiveKeys, census.Garbage, census.Tombstones, census.Intents
	return rep, nil
}

// ScrubLogs retires the write-ahead logs of the store's engines that
// predate the call (storage.Engine.ScrubLogs): the last place on this
// node a purged value written before it could still be read from a file.
func (s *Store) ScrubLogs(ctx context.Context) error {
	s.purgeMu.Lock()
	defer s.purgeMu.Unlock()
	if err := s.raftEngine().ScrubLogs(ctx); err != nil {
		return err
	}
	if s.splitEngines() {
		return s.cfg.Engine.ScrubLogs(ctx)
	}
	return nil
}
