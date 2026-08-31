package kvserver

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Closed timestamps enable follower reads (issue #5): the leader
// periodically promises "no write at or below T will ever commit on this
// range", and any replica may then serve reads at or below T locally.
//
// The promise rides the raft log itself as a tiny replicated command
// rather than a side channel, which buys two properties for free:
//
//   - The "applied index has caught up to the publication" condition is
//     log order: by the time a replica applies the closed-ts command,
//     every write below T has applied too. No index bookkeeping.
//   - The closed timestamp is replicated state (persisted in
//     replicaState), so it survives restarts and leader failure — a
//     follower keeps serving reads below T with the leader gone, which is
//     the acceptance bar for the issue.
//
// Publication correctness on the leader (nothing may sneak beneath T):
//
//  1. Acquire a whole-range SHARED latch. Writes hold exclusive latches
//     from their timestamp-cache check until they apply (invariant L1),
//     so acquisition drains every in-flight write — their log entries
//     precede the closed-ts command — while readers are undisturbed.
//  2. Bump the timestamp-cache floor to T under the latch. Every write
//     checked afterwards is forwarded above T (transactional writes get
//     pushed, non-transactional ones bounced — the standard machinery).
//  3. Release and propose. Post-bump writes are above T, so their log
//     position relative to the command is irrelevant.
//
// EndTxn is exempt from the timestamp-cache check (it writes no MVCC
// versions), so a transaction that wrote intents BEFORE the bump could
// still commit at ≤ T and resolution would move its versions to a commit
// timestamp ≤ T. That is safe for followers because those intents applied
// before the publication (step 1 drained them): a follower read at ≤ T
// either sees the intent (and bails to the leader) or sees the resolved
// committed value — never a miss that later materializes.
const (
	defaultClosedTSLag      = 3 * time.Second
	defaultClosedTSInterval = time.Second
)

// StartClosedTimestamps starts the store's closed-timestamp publisher. A
// negative lag disables publication (and with it follower reads).
func (s *Store) StartClosedTimestamps() error {
	lag := s.cfg.ClosedTimestampLag
	if lag < 0 {
		return nil
	}
	if lag == 0 {
		lag = defaultClosedTSLag
	}
	interval := s.cfg.ClosedTimestampInterval
	if interval <= 0 {
		interval = defaultClosedTSInterval
	}
	return s.cfg.Stopper.RunWorker(func(ctx context.Context) {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.publishClosedTimestamps(ctx, lag)
			}
		}
	})
}

func (s *Store) publishClosedTimestamps(ctx context.Context, lag time.Duration) {
	s.VisitReplicas(func(r *Replica) bool {
		if err := ctx.Err(); err != nil {
			return false
		}
		if !r.isLeader() || r.isFrozen() {
			return true
		}
		target := s.cfg.Clock.Now().AddNanos(-lag.Nanoseconds())
		if !r.ClosedTimestamp().Less(target) {
			return true
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := r.publishClosedTimestamp(pctx, target); err != nil {
			log.Debugf("%s: closed timestamp publication failed: %v", r.rangeID, err)
		}
		cancel()
		return true
	})
}

// publishClosedTimestamp closes the range at target: after it returns
// successfully, no write at or below target will ever commit here.
func (r *Replica) publishClosedTimestamp(ctx context.Context, target hlc.Timestamp) error {
	// Step 1: drain in-flight writes (they hold exclusive latches until
	// applied); readers share fine.
	guard, gerr := r.latches.Acquire(ctx, []latchSpan{wholeRangeSpan}, latchShared)
	if gerr != nil {
		return gerr
	}
	// Step 2: from here on no write may pass the cache at or below target.
	r.tsCache.Bump([]latchSpan{wholeRangeSpan}, target, uuid.Nil)
	guard.Release()
	// Step 3: replicate. Log order does the rest.
	_, kerr := r.proposeCmd(ctx, &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: r.rangeID}}, cmdTriggers{closedTS: target})
	if kerr != nil {
		return kerr
	}
	return nil
}

// executeStaleRead serves a read-only batch pinned at a fixed timestamp on
// a NON-leader replica, legal exactly when the timestamp is at or below
// the range's closed timestamp. No latches: entries apply here without
// latching anyway, and everything at or below the closed timestamp is
// immutable (intent resolution only moves versions to commit timestamps
// above it). No timestamp-cache bump either — the closed timestamp already
// keeps every future write above this read.
func (r *Replica) executeStaleRead(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	if err := r.checkKeyBounds(ba); err != nil {
		return nil, err
	}
	if err := r.checkFrozen(ba); err != nil {
		return nil, err
	}
	ts := readTimestamp(ba)
	if thr := r.GCThreshold(); !thr.IsEmpty() && ts.LessEq(thr) {
		return nil, kvpb.NewErrorf("%s: batch timestamp %s is below the GC threshold %s", r.rangeID, ts, thr)
	}
	if closed := r.ClosedTimestamp(); closed.Less(ts) {
		// Not provably closed here (yet): the leader serves it.
		return nil, r.notLeaderError()
	}
	br, rerr := r.evalReadOnly(ba)
	if rerr != nil {
		if rerr.WriteIntent != nil {
			// Conflict machinery (pushes, resolution) is the leader's;
			// redirect rather than looping on an intent only the leader's
			// path can clear.
			return nil, r.notLeaderError()
		}
		return nil, rerr
	}
	metrics.FollowerReads.Inc()
	return br, nil
}
