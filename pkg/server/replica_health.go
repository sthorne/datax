package server

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
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
//
// A heartbeat's lists are capped (maxReportedReplicas, the lowest range
// IDs) with exact counts beside them, so that a node with thousands of
// dead replicas does not rewrite a huge row every few seconds. When a
// peer's count exceeds its list, the reader asks that peer for its full
// lists (the unavailable-replicas admin op, replicaFaults); a peer that
// does not answer leaves its replicas past its lists unknown, and the
// health check and the replication view say so instead of counting them
// as serving.

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

// maxReportedReplicas caps each replica list a heartbeat carries.
const maxReportedReplicas = 64

// reportCap is the cap on this node's heartbeat lists.
func (n *Node) reportCap() int {
	if c := n.cfg.TestingMaxReportedReplicas; c > 0 {
		return c
	}
	return maxReportedReplicas
}

// capReplicaReport returns the first limit of ids (which are sorted) and
// the exact count of ids, for the heartbeat row.
func capReplicaReport(ids []base.RangeID, limit int) ([]base.RangeID, int) {
	n := len(ids)
	if n > limit {
		ids = ids[:limit:limit]
	}
	return ids, n
}

// reportedCount is the number of replicas a heartbeat reports in list,
// whose exact total is count (0 from binaries that predate the cap).
func reportedCount(list []base.RangeID, count int) int {
	return max(count, len(list))
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

// unavailableReport answers the unavailable-replicas admin op: the
// serving node's failed and missing replicas, uncapped.
type unavailableReport struct {
	Failed  []base.RangeID `json:"failed,omitempty"`
	Missing []base.RangeID `json:"missing,omitempty"`
}

// replicaFaults is what the live nodes hold no serving replica of, for
// classifying ranges.
type replicaFaults struct {
	down map[base.NodeID]map[base.RangeID]bool
	// unknownAbove holds, for a node whose heartbeat lists were capped
	// and that did not answer for the rest, the range ID its lists are
	// exact up to: whether its replicas of higher ranges serve is not
	// known.
	unknownAbove map[base.NodeID]base.RangeID
	// unreached says why each such node could not be asked.
	unreached map[base.NodeID]error
}

// state reports whether node's replica of r is known not to serve, or
// whether that is not known.
func (f *replicaFaults) state(node base.NodeID, r base.RangeID) (down, unknown bool) {
	if f.down[node][r] {
		return true, false
	}
	if above, ok := f.unknownAbove[node]; ok && r > above {
		return false, true
	}
	return false, false
}

// cappedReport reports whether a heartbeat list fell short of its count
// and, if so, the range ID it is exact up to (its last; the list holds
// the lowest IDs).
func cappedReport(list []base.RangeID, count int) (exactTo base.RangeID, capped bool) {
	if count <= len(list) {
		return 0, false
	}
	if len(list) == 0 {
		return 0, true
	}
	return list[len(list)-1], true
}

// replicaFaults gathers what each of the live nodes holds no serving
// replica of: this node's read now; a peer's from its last heartbeat or,
// where the heartbeat's lists fell short of its counts, asked of the peer
// itself (in parallel, each within peerAnswerTimeout). Only a binary that
// caps its lists sends counts, and it answers the op, so an older peer is
// never asked.
func (n *Node) replicaFaults(ctx context.Context, live []base.NodeID) *replicaFaults {
	f := &replicaFaults{
		down:         map[base.NodeID]map[base.RangeID]bool{},
		unknownAbove: map[base.NodeID]base.RangeID{},
		unreached:    map[base.NodeID]error{},
	}
	type ask struct {
		node    base.NodeID
		nd      kvpb.NodeDescriptor
		exactTo base.RangeID
		rep     unavailableReport
		err     error
	}
	var asks []*ask
	for _, id := range live {
		if id == n.ident.NodeID {
			failed, missing := n.unavailableReplicas()
			markUnavailable(f.down, id, failed, missing)
			continue
		}
		nd, ok := n.registry.Get(id)
		if !ok {
			continue
		}
		fTo, fCapped := cappedReport(nd.FailedReplicas, nd.FailedReplicaCount)
		mTo, mCapped := cappedReport(nd.MissingReplicas, nd.MissingReplicaCount)
		if !fCapped && !mCapped {
			markUnavailable(f.down, id, nd.FailedReplicas, nd.MissingReplicas)
			continue
		}
		exactTo := fTo
		if !fCapped || (mCapped && mTo < fTo) {
			exactTo = mTo
		}
		asks = append(asks, &ask{node: id, nd: nd, exactTo: exactTo})
	}
	var wg sync.WaitGroup
	for _, a := range asks {
		wg.Go(func() { a.rep, a.err = n.peerUnavailableReplicas(ctx, a.node) })
	}
	wg.Wait()
	for _, a := range asks {
		if a.err != nil {
			// The capped lists still name replicas known not to serve.
			markUnavailable(f.down, a.node, a.nd.FailedReplicas, a.nd.MissingReplicas)
			f.unknownAbove[a.node] = a.exactTo
			f.unreached[a.node] = a.err
			continue
		}
		markUnavailable(f.down, a.node, a.rep.Failed, a.rep.Missing)
	}
	return f
}

// peerUnavailableReplicas asks one node for its full failed and missing
// replica lists over the internode admin RPC, within peerAnswerTimeout.
func (n *Node) peerUnavailableReplicas(ctx context.Context, id base.NodeID) (unavailableReport, error) {
	addr, err := n.resolvePeer(id)
	if err != nil {
		return unavailableReport{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, peerAnswerTimeout)
	defer cancel()
	var resp cluster.AdminResponse
	if err := n.trans.Call(ctx, addr, "admin", cluster.AdminRequest{Op: "unavailable-replicas"}, &resp); err != nil {
		return unavailableReport{}, err
	}
	if resp.Error != "" {
		return unavailableReport{}, fmt.Errorf("%s", resp.Error)
	}
	var rep unavailableReport
	if err := json.Unmarshal(resp.Status, &rep); err != nil {
		return unavailableReport{}, fmt.Errorf("undecodable answer: %w", err)
	}
	return rep, nil
}

// Replication states, worst last; a range is in exactly one.
type replState int

const (
	replHealthy replState = iota
	replOver
	replUnder
	replNoQuorum
)

// classifyReplication is a range's state with live of its total
// replicas serving, against a target of want; the health check and the
// replication view share it.
func classifyReplication(live, total, want int) replState {
	switch {
	case live*2 <= total:
		return replNoQuorum
	case live < want || total < want:
		return replUnder
	case total > want:
		return replOver
	default:
		return replHealthy
	}
}

// classifyUncertain classifies a range of whose live replicas unknown
// may or may not serve: known reports whether the state is the same
// either way.
func classifyUncertain(live, unknown, total, want int) (state replState, known bool) {
	state = classifyReplication(live, total, want)
	return state, classifyReplication(live-unknown, total, want) == state
}
