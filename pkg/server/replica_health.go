package server

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/log"
)

// A node that keeps heartbeating counts as live, and before this every
// replica the descriptors placed on a live node counted as live too. Two
// kinds do not serve although the node is up:
//   - failed: the replica's group was stopped by a failure (a raft
//     engine write error such as ENOSPC or EIO, a failed apply) and stays
//     stopped until the node restarts (kvserver.Store.FailedReplicas);
//   - missing: a descriptor names this node, but no replica of the range
//     is loaded here at all (one that never reached disk before a crash,
//     say), so the leader's messages to it are dropped.
//
// The node finds both, exports them as datax_replicas_failed and
// datax_replicas_missing, and reports them in its heartbeat
// (kvpb.NodeDescriptor.FailedReplicas, MissingReplicas), so that every
// node's health check counts those replicas as unavailable when it decides
// whether a range is under-replicated or has lost quorum.

// replicaMissingGrace is how long a descriptor must have named this node,
// with no replica of the range loaded, before the replica is reported
// missing. A replica is created (preseeded) before a descriptor names it,
// but a follower that has yet to apply a split holds no right-hand
// replica for as long as it lags, and that is not yet a fault.
const replicaMissingGrace = time.Minute

// replicaHealthInterval paces the range-descriptor scan that finds
// missing replicas.
const replicaHealthInterval = 15 * time.Second

// replicaHealth is what the node last found.
type replicaHealth struct {
	mu        sync.Mutex
	checkedAt time.Time
	// since is when each range was first seen naming this node with no
	// replica loaded, until it is seen otherwise.
	since   map[base.RangeID]time.Time
	missing []base.RangeID
}

// refreshMissingReplicas rescans the range descriptors, at most once per
// replicaHealthInterval; the heartbeat calls it. A failed scan keeps
// what was last found.
func (n *Node) refreshMissingReplicas(ctx context.Context) {
	if n.store == nil {
		return
	}
	now := time.Now()
	n.replicaHealth.mu.Lock()
	due := now.Sub(n.replicaHealth.checkedAt) >= replicaHealthInterval
	n.replicaHealth.mu.Unlock()
	if !due {
		return
	}
	descs, err := n.listRanges(ctx)
	if err != nil {
		log.Debugf("replica health: listing ranges: %v", err)
		return
	}
	has := func(id base.RangeID) bool {
		_, ok := n.store.GetReplica(id)
		return ok
	}
	n.replicaHealth.mu.Lock()
	defer n.replicaHealth.mu.Unlock()
	n.replicaHealth.checkedAt = now
	n.replicaHealth.since, n.replicaHealth.missing = missingReplicas(descs, n.ident.NodeID, has, n.replicaHealth.since, now, replicaMissingGrace)
}

// missingReplicas returns, for the ranges whose descriptors name node and
// for which has reports no local replica, when each was first seen so
// (carried over from since) and, in order, those seen so for at least
// grace.
func missingReplicas(descs []kvpb.RangeDescriptor, node base.NodeID, has func(base.RangeID) bool,
	since map[base.RangeID]time.Time, now time.Time, grace time.Duration) (map[base.RangeID]time.Time, []base.RangeID) {
	next := map[base.RangeID]time.Time{}
	var missing []base.RangeID
	for _, d := range descs {
		named := false
		for _, rep := range d.Replicas {
			if rep.NodeID == node {
				named = true
				break
			}
		}
		if !named || has(d.RangeID) {
			continue
		}
		first, ok := since[d.RangeID]
		if !ok {
			first = now
		}
		next[d.RangeID] = first
		if now.Sub(first) >= grace {
			missing = append(missing, d.RangeID)
		}
	}
	slices.Sort(missing)
	return next, missing
}

// unavailableReplicas is what this node reports: its failed replicas,
// read now, and its missing ones as last found.
func (n *Node) unavailableReplicas() (failed, missing []base.RangeID) {
	if n.store != nil {
		failed = n.store.FailedReplicas()
	}
	n.replicaHealth.mu.Lock()
	missing = slices.Clone(n.replicaHealth.missing)
	n.replicaHealth.mu.Unlock()
	if len(missing) > 0 {
		// A missing replica loaded since the scan (a late split apply,
		// a catch-up snapshot) is not missing any more.
		missing = slices.DeleteFunc(missing, func(id base.RangeID) bool {
			_, ok := n.store.GetReplica(id)
			return ok
		})
	}
	return failed, missing
}

// markUnavailable records that node holds no serving replica of the
// given ranges.
func markUnavailable(m map[base.NodeID]map[base.RangeID]bool, node base.NodeID, ranges ...[]base.RangeID) {
	for _, rs := range ranges {
		for _, r := range rs {
			if m[node] == nil {
				m[node] = map[base.RangeID]bool{}
			}
			m[node][r] = true
		}
	}
}
