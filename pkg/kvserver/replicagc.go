package kvserver

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/log"
	"go.etcd.io/raft/v3"
)

// Replica GC (issue #245). A replica addition preseeds the target with a
// snapshot before proposing the membership change, so a change that
// never lands — its proposal times out, the leader loses leadership, or
// the apply refuses it because the range changed under it (issue #200)
// — leaves the target holding a replica that is a member of nothing: a
// raft group of one, seeded with a descriptor the range has moved past,
// never contacted by the real leader, campaigning on its own election
// timer with nobody voting for it. #200's fix lets the next preseed
// replace it; this pass is for the leftover nothing ever tries to
// replace, which otherwise sits on the store for good: its data on
// disk, its raft group ticking, its votes reaching the members.
//
// The pass is conservative by construction. A replica qualifies only
// when it leads nothing, knows no leader, and has applied nothing for
// ReplicaGCGrace — long enough that a freshly preseeded replica whose
// change is about to apply never qualifies, and that a member of a
// range whose leader is briefly gone does not either. Then /meta, the
// cluster's own account of the range, decides: a record naming another
// range for the replica's span, or naming this range without this
// store, or with a newer replica on this store, says the replica is a
// leftover, and it is destroyed with its data. No record, or one that
// does not cover the span, is an addressing repair in flight and means
// nothing; a record that lists this store is a partitioned member, left
// alone however long it has been quiet. A frozen replica (a merge's
// right-hand side, whose record the merge has already folded into its
// neighbour's) is the merge's to remove, never this pass's.

// DefaultReplicaGCGrace is how long a quiet, leaderless replica is left
// alone before /meta is asked about it.
const DefaultReplicaGCGrace = 60 * time.Second

func (s *Store) replicaGCGrace() time.Duration {
	if s.cfg.ReplicaGCGrace > 0 {
		return s.cfg.ReplicaGCGrace
	}
	return DefaultReplicaGCGrace
}

// RunReplicaGCOnce destroys the store's leftover replicas: those /meta
// says are not members of their range, after the grace. Called from the
// housekeeping loop.
func (s *Store) RunReplicaGCOnce(ctx context.Context) {
	lookup, ok := s.getSender().(metaLookup)
	if !ok {
		return
	}
	grace := s.replicaGCGrace()
	s.VisitReplicas(func(r *Replica) bool {
		if ctx.Err() != nil {
			return false
		}
		if !r.replicaGCCandidate(grace) {
			return true
		}
		desc := r.Desc()
		lctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rec, found, err := lookup.LookupMetaRecord(lctx, desc.StartKey)
		cancel()
		if err != nil {
			log.Debugf("%s/%d: replica GC: reading the meta record: %v", r.rangeID, r.replicaID, err)
			return true
		}
		if !found {
			return true
		}
		why := s.leftoverReason(r, desc, rec)
		if why == "" {
			return true
		}
		if !r.replicaGCCandidate(grace) {
			// Something changed while /meta was read.
			return true
		}
		log.Infof("%s/%d: replica GC: %s; removing the replica and its data (issue #245)", r.rangeID, r.replicaID, why)
		s.cfg.Events.Record("replica-gc", "%s: replica %d removed from this store: %s", r.rangeID, r.replicaID, why)
		metrics.ReplicaGCRemoved.Inc()
		s.destroyStaleReplica(r)
		return true
	})
}

// replicaGCCandidate reports whether the replica has been quiet and
// leaderless for longer than the grace: not the leader, no leader known
// to raft, nothing applied, not frozen, not destroyed, its group still
// running.
func (r *Replica) replicaGCCandidate(grace time.Duration) bool {
	r.mu.Lock()
	destroyed, frozen, leader, appliedAt := r.mu.destroyed, r.mu.frozen, r.mu.leader, r.mu.appliedAt
	r.mu.Unlock()
	if destroyed || frozen || leader == uint64(r.replicaID) || time.Since(appliedAt) < grace {
		return false
	}
	st, ok := r.raftStatus()
	if !ok || st.RaftState == raft.StateLeader || st.Lead != 0 {
		return false
	}
	return true
}

// leftoverReason says why the meta record rec shows the replica to be a
// leftover, or "" when it does not: the record covers the replica's
// start key by construction.
func (s *Store) leftoverReason(r *Replica, desc, rec kvpb.RangeDescriptor) string {
	if rec.RangeID != desc.RangeID {
		return "/meta names " + rec.String() + " for its span, not " + desc.RangeID.String()
	}
	for _, rd := range rec.Replicas {
		if rd.StoreID != s.cfg.StoreID && rd.NodeID != s.cfg.NodeID {
			continue
		}
		if rd.ReplicaID > r.replicaID {
			return "/meta lists this store with the newer replica " + rd.ReplicaID.String() + " of " + rec.String()
		}
		// Listed, at this replica's id or an older record's: a member,
		// however quiet.
		return ""
	}
	return "/meta lists " + rec.String() + " without this store"
}
