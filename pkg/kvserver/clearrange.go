package kvserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/raft/v3"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// A replicated range clear (issue #316): the dropped-table reclaim's way
// to empty one range of a dropped table's span with ONE raft command,
// instead of a tombstone per row through the log and a collection of
// each afterwards. It is driven from the server (server.Node.ClearSpan),
// one range at a time, after splitting at the span's ends, exactly as a
// purge is; the purge still follows it (see below).
//
// What the command means: a ClearRange at timestamp T is a deletion of
// every key of the range at T followed at once by a collection at a
// threshold of T. Every read at or below T is refused from then on (the
// range's GC threshold, raised in the same command), and every read above
// T sees nothing, so it is correct exactly when no read above T was
// answered before it and no version above T exists. The leader
// establishes both under an exclusive latch on the whole range, which
// drains every write in flight (writes hold their latches until they
// apply, invariant L1) and keeps every new request out until the command
// has applied:
//
//   - T is at least the leader's clock, at least the newest version in
//     the span, at least the highest read the timestamp cache holds for
//     the range (which includes every closed-timestamp promise made on
//     the log or off it: each bumps the cache first, under a shared
//     latch that this one excludes) and the replicated closed timestamp,
//     and above the current threshold. So no read above T has been
//     served, by the leader or by a follower, and nothing above T is
//     deleted.
//   - An intent anywhere in the span refuses the clear: its transaction
//     may still commit, and its record (elsewhere) says what the intent
//     means. The driver resolves the intents of finished transactions
//     first, as a purge does, and the reclaim job runs again on its next
//     pass if one is left. The leader looks only after an entry of its
//     own term has applied (waitOwnTermApplied): an entry of an earlier
//     term was written under another leader's latches, and could still
//     be on its way to the state machine with an intent in it; an entry
//     of this term that touches the span holds a latch this one waits
//     for.
//   - After it, a write at or below T is refused like any batch below the
//     threshold, and a follower refuses a stale read at or below T as
//     soon as it has applied the command; before that, its closed
//     timestamp is below T (a promise at or above T can only be made
//     once the latch is released, at an index after the command), so it
//     sends such a read to the leader.
//
// Statistics. The range's size (replicaState.SizeBytes) moves by deltas
// that are a pure function of each command, so that every replica agrees.
// The design note before this command had the leader count the span and
// the command carry the negated count. A clear acts on a whole range,
// though, so after it the range's MVCC span is empty and its size is
// exactly zero: the command sets it to that, which needs no count to
// agree with what the replicas hold, and also corrects whatever the
// approximate deltas before it had drifted. (The leader still counts, for
// its report.) What has to hold is that the command covers the whole
// range on every replica that applies it: a replica applies it only while
// its range still has the bounds and the generation the leader evaluated
// at and is not frozen for a merge (a split, merge or replica change in
// between: the command then applies nothing, and the driver tries
// again), and only while its threshold is below T — the command raises it
// to T, so a second application of the same command, a replayed or
// re-proposed entry, finds it there and applies nothing either. All of
// this is replicated state read at the command's log position, so every
// replica decides alike, and the consistency checker, which compares the
// size and the threshold as well as the data, agrees.
//
// Apply is a range deletion of the span's MVCC keys
// (storage.Batch.DeleteRange — the same tool snapshots and replica
// removal use) in the same engine batch as the threshold, the size and
// the applied index. The range-local transaction records are left alone:
// a committed record is proof for intents on other ranges, and GC
// reclaims records by its own rules. A snapshot taken after the clear
// streams no key of the span and carries the threshold and the size; one
// taken before carries the keys and an applied index below the command,
// which then applies on top of it. There are no rangefeeds or
// changefeeds to tell.
//
// What a range deletion does not do is take the bytes out of the files:
// the keys stay in their sstables under the range tombstone until a
// compaction, and the values stay in the raft log until it is truncated.
// So the reclaim still purges the span afterwards (purge.go): the
// collection there finds nothing to collect, and the compaction, the log
// truncation and the log scrub are what remove the bytes.

// ClearRangeResult is what the leader's step reports.
type ClearRangeResult struct {
	Desc kvpb.RangeDescriptor
	// Timestamp is the clear's time: the range's GC threshold after it.
	Timestamp hlc.Timestamp
	// Keys and Bytes count the stored MVCC keys (every version and
	// tombstone) the command removed, and their key and value bytes.
	Keys  int64
	Bytes int64
	// AppliedIndex is the leader's applied index once the command applied.
	AppliedIndex uint64
}

// clearOwnTermWait bounds how long a new leader waits for the first entry
// of its own term to apply before it evaluates a clear.
const clearOwnTermWait = 5 * time.Second

// ClearRange runs the leader's step for rangeID, which must lie wholly
// inside [start, end) — and [start, end) must be one dropped table's data
// span: keys.TableDataSpan of an id that has a dropped-table record and no
// descriptor of any kind. It returns ErrPurgeNotLeader on a store that
// does not lead the range.
func (s *Store) ClearRange(ctx context.Context, rangeID base.RangeID, start, end keys.Key) (ClearRangeResult, error) {
	var res ClearRangeResult
	if s.cfg.ClusterVersion != nil && s.cfg.ClusterVersion() < version.V22 {
		return res, fmt.Errorf("a range clear needs cluster version %s (finalize the upgrade first)", version.V22)
	}
	r, ok := s.GetReplica(rangeID)
	if !ok || !r.IsLeader() {
		return res, ErrPurgeNotLeader
	}
	if r.isFrozen() {
		return res, fmt.Errorf("%s is frozen for a merge; clear again once it has finished", rangeID)
	}
	id, err := droppedTableOfSpan(start, end)
	if err != nil {
		return res, err
	}
	desc := r.Desc()
	if desc.StartKey.Compare(start) < 0 || end.Compare(desc.EndKey) < 0 {
		return res, fmt.Errorf("%s spans %s to %s, beyond the cleared span: a clear acts on whole ranges", rangeID, keys.Pretty(desc.StartKey), keys.Pretty(desc.EndKey))
	}
	// The intents of finished transactions out of the way first, without
	// the latch (resolving them needs it).
	r.resolveIntentsForPurge(ctx, desc)

	guard, gerr := r.latches.AcquireOpts(ctx, []latchSpan{wholeRangeSpan}, latchExclusive, latchOpts{fair: true, op: "clear_range"})
	if gerr != nil {
		return res, gerr
	}
	defer guard.Release()
	if err := r.waitOwnTermApplied(ctx); err != nil {
		return res, err
	}
	if !r.IsLeader() {
		return res, ErrPurgeNotLeader
	}
	if r.isFrozen() {
		return res, fmt.Errorf("%s is frozen for a merge; clear again once it has finished", rangeID)
	}
	desc = r.Desc()
	if desc.StartKey.Compare(start) < 0 || end.Compare(desc.EndKey) < 0 {
		return res, fmt.Errorf("%s changed to %s-%s under the clear", rangeID, keys.Pretty(desc.StartKey), keys.Pretty(desc.EndKey))
	}
	// The catalog, read under the latch: a restore that brings a table back
	// under this id removes the record before it writes a row, and its
	// rows wait on this latch; so either the record is gone here and the
	// clear is refused, or the rows land after the clear, above it.
	if err := s.checkDroppedTable(ctx, desc, id); err != nil {
		return res, err
	}

	count, err := countSpanForClear(s.cfg.Engine, desc.StartKey, desc.EndKey)
	if err != nil {
		return res, err
	}
	if count.intents > 0 {
		return res, fmt.Errorf("%s holds %d intent(s), the first at %s: the clear waits for their transactions", rangeID, count.intents, keys.Pretty(count.firstIntent))
	}
	ts := r.clearTimestamp(count.newest)
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: rangeID, Timestamp: ts}}
	ba.Add(&kvpb.ClearRangeRequest{
		RequestHeader: kvpb.RequestHeader{Key: desc.StartKey, EndKey: desc.EndKey},
		Timestamp:     ts,
		Generation:    desc.Generation,
	})
	br, kerr := r.propose(ctx, ba)
	if kerr != nil {
		return res, kerr
	}
	if len(br.Responses) != 1 || br.Responses[0].ClearRange == nil || !br.Responses[0].ClearRange.Cleared {
		return res, fmt.Errorf("%s changed before the clear applied; clear again", rangeID)
	}
	metrics.RangeClears.Inc()
	res = ClearRangeResult{Desc: desc, Timestamp: ts, Keys: count.keys, Bytes: count.bytes, AppliedIndex: r.AppliedIndex()}
	return res, nil
}

// clearTimestamp picks the clear's time on the leader, under the
// exclusive latch (see the top of this file): no read above it can have
// been served and no version above it exists.
func (r *Replica) clearTimestamp(newest hlc.Timestamp) hlc.Timestamp {
	ts := r.store.cfg.Clock.Now().Forward(newest)
	if _, low := r.tsCache.AllowsWrite([]latchSpan{wholeRangeSpan}, ts, uuid.Nil); ts.Less(low) {
		ts = low
	}
	ts = ts.Forward(r.ClosedTimestamp())
	if thr := r.GCThreshold(); !thr.Less(ts) {
		ts = thr.Next()
	}
	return ts
}

// waitOwnTermApplied returns once this leader has applied an entry of its
// own term, so every entry of an earlier term — written by a leader whose
// latches this one never saw — has applied too.
func (r *Replica) waitOwnTermApplied(ctx context.Context) error {
	deadline := time.Now().Add(clearOwnTermWait)
	for {
		var term uint64
		leader := false
		_ = r.withRaftGroup(func(rn *raft.RawNode) error {
			st := rn.BasicStatus()
			term, leader = st.Term, st.RaftState == raft.StateLeader
			return nil
		})
		if !leader {
			return ErrPurgeNotLeader
		}
		r.mu.Lock()
		applied := r.mu.appliedTerm
		r.mu.Unlock()
		if applied >= term {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: no entry of term %d applied yet", r.rangeID, term)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// droppedTableOfSpan returns the table whose whole data span [start, end)
// is: a clear names one dropped table's span, never part of one or more.
func droppedTableOfSpan(start, end keys.Key) (uint64, error) {
	id, ok := keys.TableIDOf(start)
	if ok {
		if s, e := keys.TableDataSpan(id); s.Equal(start) && e.Equal(end) {
			return id, nil
		}
	}
	return 0, fmt.Errorf("a clear acts on one table's whole data span, not %s to %s", keys.Pretty(start), keys.Pretty(end))
}

// checkDroppedTable refuses the clear unless table id has a dropped-table
// record and no descriptor under its id: the span is a dropped table's,
// not a live one's (whatever the caller thought). The keys are read
// through the routed sender; none of them can lie in the latched range.
func (s *Store) checkDroppedTable(ctx context.Context, desc kvpb.RangeDescriptor, id uint64) error {
	sender := s.getSender()
	if sender == nil {
		return errors.New("a clear needs the routed sender to read the catalog")
	}
	recordKey := keys.DroppedTableKey(id)
	descKeys := []keys.Key{keys.TableDescKey(id), keys.SequenceDescKey(id), keys.TypeDescKey(id), keys.DatabaseDescKey(id)}
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: s.cfg.Clock.Now()}}
	for _, k := range append([]keys.Key{recordKey}, descKeys...) {
		if desc.ContainsKey(k) {
			return fmt.Errorf("%s holds the catalog key %s; it cannot be cleared", desc.RangeID, keys.Pretty(k))
		}
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
	}
	br, kerr := sender.Send(ctx, ba)
	if kerr != nil {
		return fmt.Errorf("reading the catalog for table %d: %w", id, kerr)
	}
	if len(br.Responses) != 1+len(descKeys) {
		return fmt.Errorf("reading the catalog for table %d: %d answers to %d reads", id, len(br.Responses), 1+len(descKeys))
	}
	if g := br.Responses[0].Get; g == nil || g.Value == nil {
		return fmt.Errorf("table %d has no dropped-table record: only a dropped table's span is cleared", id)
	}
	for i, k := range descKeys {
		if g := br.Responses[1+i].Get; g != nil && g.Value != nil {
			return fmt.Errorf("id %d names a live descriptor (%s): its span is not cleared", id, keys.Pretty(k))
		}
	}
	return nil
}

// clearCount is what a clear's leader finds in the span.
type clearCount struct {
	keys, bytes int64
	newest      hlc.Timestamp
	intents     int
	firstIntent keys.Key
}

// countSpanForClear counts the stored MVCC keys of [start, end) — exactly
// what DeleteRange over the same encoded bounds removes, and what
// spanSizeBytes measures — with the newest version's timestamp and the
// intents.
func countSpanForClear(r storage.Reader, start, end keys.Key) (clearCount, error) {
	var c clearCount
	lower := storage.EncodeMVCCKey(start, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(end, hlc.Timestamp{})
	it := r.NewIter(lower, upper)
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		c.keys++
		c.bytes += int64(len(it.Key()) + len(it.Value()))
		userKey, ts, err := storage.DecodeMVCCKey(it.Key())
		if err != nil {
			_ = it.Close()
			return c, err
		}
		if ts.IsEmpty() {
			if c.intents == 0 {
				c.firstIntent = keys.Key(userKey).Clone()
			}
			c.intents++
			continue
		}
		c.newest = c.newest.Forward(ts)
	}
	return c, it.Close()
}

// evalClearRange is the command's apply on every replica (see the top of
// this file): it reports false, and writes nothing, when the range has
// changed since the leader evaluated it or the command has already
// applied.
func (r *Replica) evalClearRange(b *storage.Batch, req *kvpb.ClearRangeRequest) (bool, error) {
	r.mu.Lock()
	desc, thr, frozen := r.mu.desc, r.mu.gcThreshold, r.mu.frozen
	r.mu.Unlock()
	if frozen || desc.Generation != req.Generation || !desc.StartKey.Equal(req.Key) || !desc.EndKey.Equal(req.EndKey) {
		return false, nil
	}
	if !thr.Less(req.Timestamp) {
		return false, nil
	}
	if err := b.DeleteRange(storage.EncodeMVCCKey(req.Key, hlc.Timestamp{}), storage.EncodeMVCCKey(req.EndKey, hlc.Timestamp{})); err != nil {
		return false, err
	}
	return true, nil
}
