package server

import (
	"context"
	"sort"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/placement"
	"github.com/sthorne/datax/pkg/util/log"
)

// Load- and byte-weighted balancing. Count-based rebalancing (replicate.go)
// treats every replica as equal; a node holding all the hot leaseholders or
// all the large ranges still reads as balanced. Two lower-priority passes
// act on the load aggregates every node advertises through its registry
// heartbeat (kvpb.NodeDescriptor's load fields, from Store.LoadSummary):
//
//   - lease shedding: an overloaded node's hottest lease moves to the
//     coolest node already holding a replica — membership unchanged, so
//     no data moves and no diversity question arises;
//   - byte rebalancing: when counts are level but bytes are not, the
//     biggest range moves off the fullest node, diversity-gated.
//
// The signals are best-effort: QPS is leader-local, resets to zero on
// every transfer (a 10s window to maturity), and reaches the allocator up
// to a heartbeat late. The defenses are thresholds with absolute floors,
// a per-range cooldown much longer than the maturity window, and one op
// per tick across all balancing passes.

const (
	// loadAdvertiseTopK bounds the hot/big range lists in the heartbeat.
	loadAdvertiseTopK = 8

	defaultLeaseShedFactor     = 1.5
	defaultLeaseShedMinQPS     = 100
	defaultBytesThresholdBytes = 64 << 20
	defaultLoadCooldown        = 60 * time.Second
)

func (n *Node) leaseShedFactor() float64 {
	if n.cfg.LeaseShedFactor > 0 {
		return n.cfg.LeaseShedFactor
	}
	return defaultLeaseShedFactor
}

func (n *Node) leaseShedMinQPS() float64 {
	if n.cfg.LeaseShedMinQPS > 0 {
		return n.cfg.LeaseShedMinQPS
	}
	return defaultLeaseShedMinQPS
}

func (n *Node) rebalanceBytesThreshold() int64 {
	if n.cfg.RebalanceBytesThreshold != 0 {
		return n.cfg.RebalanceBytesThreshold
	}
	return defaultBytesThresholdBytes
}

func (n *Node) loadCooldownWindow() time.Duration {
	if n.cfg.LoadCooldown > 0 {
		return n.cfg.LoadCooldown
	}
	return defaultLoadCooldown
}

// balanceLiveSet builds the live, non-draining node set every balancing
// pass works over. ok=false when a dead node is present (repair owns the
// field) — balancing against a shrinking live set only causes churn.
func (n *Node) balanceLiveSet() (map[base.NodeID]kvpb.NodeDescriptor, bool) {
	now := n.clock.Now().WallTime
	live := map[base.NodeID]kvpb.NodeDescriptor{}
	for _, nd := range n.registry.All() {
		switch {
		case nd.Draining:
			continue
		case nd.NodeID == n.ident.NodeID:
			live[nd.NodeID] = nd
		case now-nd.LivenessTime > int64(n.deadNodeThreshold()):
			return nil, false
		case now-nd.LivenessTime < int64(n.livenessGrace()):
			live[nd.NodeID] = nd
		}
	}
	return live, true
}

// loadOpAllowed checks and (when allowed) stamps the per-range cooldown.
// The cooldown outlives the QPS maturity window by design: after a
// transfer the range's rate reads zero until a full window has passed, and
// acting on that blind spot ping-pongs the lease.
func (n *Node) loadOpAllowed(id base.RangeID) bool {
	now := time.Now()
	n.loadCooldownMu.Lock()
	defer n.loadCooldownMu.Unlock()
	if n.loadCooldown == nil {
		n.loadCooldown = map[base.RangeID]time.Time{}
	}
	if last, ok := n.loadCooldown[id]; ok && now.Sub(last) < n.loadCooldownWindow() {
		return false
	}
	n.loadCooldown[id] = now
	return true
}

// shedLeaseOnce moves ONE lease off the most QPS-loaded node when its
// aggregate leader rate exceeds the live-set mean by the shed factor (and
// the spread clears an absolute floor, so an idle cluster never acts). The
// target is the coolest live node already holding a replica of the chosen
// hot range; the projected post-transfer loads must actually shrink the
// imbalance. Reports whether an op was performed or attempted.
func (n *Node) shedLeaseOnce(ctx context.Context) bool {
	live, ok := n.balanceLiveSet()
	if !ok || len(live) < 2 {
		return false
	}
	var mean, minQPS float64
	var src base.NodeID
	first := true
	for id, nd := range live {
		mean += nd.LeaderQPS
		if first || nd.LeaderQPS < minQPS {
			minQPS = nd.LeaderQPS
		}
		if src == 0 || nd.LeaderQPS > live[src].LeaderQPS || (nd.LeaderQPS == live[src].LeaderQPS && id < src) {
			src = id
		}
		first = false
	}
	mean /= float64(len(live))
	srcQPS := live[src].LeaderQPS
	if srcQPS <= mean*n.leaseShedFactor() || srcQPS-minQPS <= n.leaseShedMinQPS() {
		return false
	}

	descs, err := n.listRanges(ctx)
	if err != nil {
		log.Debugf("lease shed: listing ranges: %v", err)
		return false
	}
	byID := map[base.RangeID]kvpb.RangeDescriptor{}
	for _, d := range descs {
		byID[d.RangeID] = d
	}

	// Hottest advertised range first (the list is QPS-sorted at the
	// source, but sort again — the row is external input).
	hot := append([]kvpb.HotRange(nil), live[src].HotRanges...)
	sort.Slice(hot, func(i, j int) bool { return hot[i].QPS > hot[j].QPS })
	for _, hr := range hot {
		desc, ok := byID[hr.RangeID]
		if !ok || !allReplicasLive(desc, live) {
			continue
		}
		// Coolest live follower of this range.
		var target base.NodeID
		for _, rep := range desc.Replicas {
			if rep.NodeID == src {
				continue
			}
			if _, ok := live[rep.NodeID]; !ok {
				continue
			}
			if target == 0 || live[rep.NodeID].LeaderQPS < live[target].LeaderQPS ||
				(live[rep.NodeID].LeaderQPS == live[target].LeaderQPS && rep.NodeID < target) {
				target = rep.NodeID
			}
		}
		if target == 0 {
			continue
		}
		// The transfer must shrink the imbalance, not relocate it.
		if live[target].LeaderQPS+hr.QPS >= srcQPS-hr.QPS {
			continue
		}
		if !n.loadOpAllowed(hr.RangeID) {
			continue
		}
		log.Infof("shedding lease of %s (%.0f qps) from n%d (%.0f qps) to n%d (%.0f qps)",
			hr.RangeID, hr.QPS, src, srcQPS, target, live[target].LeaderQPS)
		if err := n.db.AdminTransferLease(ctx, desc.StartKey, target); err != nil {
			log.Warnf("shedding lease of %s: %v", hr.RangeID, err)
			return true // attempted: don't stack another op this tick
		}
		metrics.LeaseSheds.Inc()
		return true
	}
	return false
}

// rebalanceBytesOnce moves ONE replica of the biggest advertised range off
// the byte-fullest node to the byte-emptiest when the byte spread is large
// while range counts are level (count imbalance is rebalanceOnce's job and
// runs at higher priority). Diversity-gated like every replica move.
func (n *Node) rebalanceBytesOnce(ctx context.Context) bool {
	live, ok := n.balanceLiveSet()
	if !ok || len(live) < 2 {
		return false
	}
	var src, dst base.NodeID
	var total int64
	for id, nd := range live {
		total += nd.ReplicaBytes
		if src == 0 || nd.ReplicaBytes > live[src].ReplicaBytes || (nd.ReplicaBytes == live[src].ReplicaBytes && id < src) {
			src = id
		}
		if dst == 0 || nd.ReplicaBytes < live[dst].ReplicaBytes || (nd.ReplicaBytes == live[dst].ReplicaBytes && id < dst) {
			dst = id
		}
	}
	mean := total / int64(len(live))
	spread := live[src].ReplicaBytes - live[dst].ReplicaBytes
	threshold := n.rebalanceBytesThreshold()
	if threshold < 0 {
		return false // disabled
	}
	if t := mean / 5; t > threshold {
		threshold = t
	}
	if spread <= threshold {
		return false
	}

	descs, err := n.listRanges(ctx)
	if err != nil {
		log.Debugf("byte rebalance: listing ranges: %v", err)
		return false
	}
	byID := map[base.RangeID]kvpb.RangeDescriptor{}
	for _, d := range descs {
		byID[d.RangeID] = d
	}
	big := append([]kvpb.HotRange(nil), live[src].BigRanges...)
	sort.Slice(big, func(i, j int) bool { return big[i].Bytes > big[j].Bytes })
	for _, br := range big {
		desc, ok := byID[br.RangeID]
		if !ok || len(desc.Replicas) < base.DefaultReplicationFactor || !allReplicasLive(desc, live) {
			continue
		}
		if _, holds := desc.GetReplica(src); !holds {
			continue // stale advertisement
		}
		if _, onDst := desc.GetReplica(dst); onDst {
			continue
		}
		var existing []kvpb.NodeDescriptor
		for _, r := range desc.Replicas {
			existing = append(existing, live[r.NodeID])
		}
		if !placement.RebalanceKeepsDiversity(existing, src, live[dst]) {
			continue
		}
		if !n.loadOpAllowed(br.RangeID) {
			continue
		}
		log.Infof("byte-rebalancing %s (%d MiB): n%d (%d MiB total) -> n%d (%d MiB total)",
			br.RangeID, br.Bytes>>20, src, live[src].ReplicaBytes>>20, dst, live[dst].ReplicaBytes>>20)
		if _, err := n.moveReplica(ctx, desc, dst, src); err != nil {
			log.Warnf("byte-rebalancing %s: %v", br.RangeID, err)
			return true // attempted
		}
		metrics.ByteRebalances.Inc()
		return true
	}
	return false
}

// RunRebalanceOnce drives one balancing decision — count move, lease
// shed, or byte move, in that priority — and reports which (or "" for
// none). Deterministic test hook; production runs the same chain from the
// allocator tick. The caller is responsible for invoking it on the
// current range-1 leader.
func (n *Node) RunRebalanceOnce(ctx context.Context) string {
	if n.rebalanceOnce(ctx) {
		return "rebalance"
	}
	if n.shedLeaseOnce(ctx) {
		return "lease-shed"
	}
	if n.rebalanceBytesOnce(ctx) {
		return "bytes"
	}
	return ""
}
