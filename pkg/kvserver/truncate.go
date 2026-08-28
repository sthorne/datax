package kvserver

import (
	"context"
	"math"

	"go.etcd.io/raft/v3"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/log"
)

// Raft log truncation. The log is the range's durability record between the
// applied state and the newest proposals; once every voter has durably
// appended a prefix AND the leader has applied it, that prefix can never be
// needed again:
//
//   - truncation index = min(every voter's Match, the leader's applied
//     index) − a safety floor. Match is what a voter has durably appended
//     (acknowledged), so no live voter ever needs a truncated entry — across
//     elections too: any electable peer's log already covers the prefix.
//   - A dead voter's Match freezes, pinning truncation until dead-node
//     repair (v2.5) removes it. This is deliberate: datax has no
//     raft-internal snapshot delivery, so the log must stay sufficient to
//     catch up any configured voter.
//   - The command is replicated: each replica deletes its OWN unreplicated
//     log prefix at apply time, at which point it has durably applied
//     everything at or below the index. TruncatedIndex/Term persist in
//     replicaState atomically with the applied index.
//
// The floor keeps a little history for stragglers mid-append; the
// min-pending threshold avoids proposing tiny truncations every tick.

const (
	// raftLogTruncateFloor is how many acknowledged entries are retained
	// below the truncation point.
	raftLogTruncateFloor = 64
	// raftLogTruncateMinPending is the minimum number of entries a
	// truncation must reclaim to be worth proposing.
	raftLogTruncateMinPending = 256
)

// RunLogTruncationOnce proposes a log truncation for every range this store
// leads whose log has accumulated enough reclaimable entries. Exported for
// tests and debug tooling; the housekeeping loop calls it each tick.
func (s *Store) RunLogTruncationOnce(ctx context.Context) {
	s.VisitReplicas(func(r *Replica) bool {
		if ctx.Err() != nil {
			return false
		}
		if !r.IsLeader() {
			return true
		}
		if err := r.maybeTruncateLog(ctx); err != nil {
			log.Warnf("%s: log truncation: %v", r.rangeID, err)
		}
		return true
	})
}

// TruncatedIndex returns the replica's current log truncation point (0 =
// never truncated).
func (r *Replica) TruncatedIndex() uint64 { return r.rs.truncatedState().Index }

func (r *Replica) maybeTruncateLog(ctx context.Context) error {
	st := r.node.Status()
	if st.RaftState != raft.StateLeader || len(st.Progress) == 0 {
		return nil
	}
	minMatch := uint64(math.MaxUint64)
	for _, pr := range st.Progress {
		if pr.Match < minMatch {
			minMatch = pr.Match
		}
	}
	r.mu.Lock()
	applied := r.mu.appliedIndex
	r.mu.Unlock()
	target := minMatch
	if applied < target {
		target = applied // never truncate above what the leader has applied
	}
	if target <= raftLogTruncateFloor {
		return nil
	}
	target -= raftLogTruncateFloor
	if cur := r.rs.truncatedState().Index; target < cur+raftLogTruncateMinPending {
		return nil
	}
	term, err := r.rs.Term(target)
	if err != nil {
		return err
	}
	desc := r.Desc()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: r.rangeID, Timestamp: r.store.cfg.Clock.Now()}}
	ba.Add(&kvpb.TruncateLogRequest{RequestHeader: kvpb.RequestHeader{Key: desc.StartKey}, Index: target, Term: term})
	if _, kerr := r.Execute(ctx, ba); kerr != nil {
		return kerr
	}
	metrics.LogTruncations.Inc()
	log.Debugf("%s: log truncated through index %d", r.rangeID, target)
	return nil
}
