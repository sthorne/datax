package server

import (
	"sort"
	"strconv"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
)

// Replication status and failure domains (issue #152). Rack-aware
// placement is the product's headline claim — losing a rack costs at
// most one replica of any range — and until now the console could not
// show whether it holds. The health checks compute the hard part and
// report one example range each; an operator planning a maintenance
// window needs the distribution, and the answer to "what breaks if I
// lose this rack".
//
// All of it comes from data the serving node already has: the range
// descriptors from the /meta scan behind /api/cluster, and the node
// registry's localities and liveness. No fan-out, so it costs a
// partitioned node's console nothing.
//
// It is computed here rather than in the page because it is a JSON
// contract every node must agree on: two nodes reading the same
// descriptors must bucket them identically, and that is a claim a test
// can hold.

// exampleRanges bounds the range ids carried per bucket. The buckets
// carry a full count; the ids are there to start an investigation, and
// a cluster with ten thousand unhealthy ranges does not need all of
// them in a status document.
const exampleRanges = 32

// ReplicationBucket is one replication state and the ranges in it.
type ReplicationBucket struct {
	Count int `json:"count"`
	// Ranges are up to exampleRanges ids, ascending; Truncated says the
	// count exceeded what is listed.
	Ranges    []int64 `json:"ranges,omitempty"`
	Truncated bool    `json:"truncated,omitempty"`
}

func (b *ReplicationBucket) add(id int64) {
	b.Count++
	if len(b.Ranges) < exampleRanges {
		b.Ranges = append(b.Ranges, id)
		return
	}
	b.Truncated = true
}

// ReplicationDomain is one failure domain — one value of one locality
// tier, e.g. rack=c — with what it holds and what its loss would cost.
type ReplicationDomain struct {
	Tier  string `json:"tier"`
	Value string `json:"value"`
	Nodes int    `json:"nodes"`
	Live  int    `json:"live_nodes"`
	// Replicas and Leases are what the domain currently holds. Leases
	// are the nodes' own leader counts (leader = leaseholder here).
	Replicas int `json:"replicas"`
	Leases   int `json:"leases"`
	// LosesQuorum is how many ranges would drop below a majority of
	// live replicas if every node in this domain were lost at once;
	// BareMajority how many would survive with no margin left — one
	// further failure would finish them. This is the question asked
	// before every maintenance window.
	LosesQuorum   int   `json:"loses_quorum"`
	BareMajority  int   `json:"bare_majority"`
	ExampleAtRisk int64 `json:"example_at_risk_range,omitempty"`
}

// ClusterReplication is the replication section of /api/cluster.
type ClusterReplication struct {
	// Healthy: at the range's target replica count, every replica live.
	// Under: fewer live replicas than the target. Over: more replicas
	// than the target (an interrupted rebalance; the allocator trims
	// it). NoQuorum: fewer than a majority of replicas live — the range
	// cannot serve.
	//
	// A range is counted once, worst state first: a range with no
	// quorum is not also counted as under-replicated.
	Healthy   ReplicationBucket `json:"healthy"`
	Under     ReplicationBucket `json:"under_replicated"`
	Over      ReplicationBucket `json:"over_replicated"`
	NoQuorum  ReplicationBucket `json:"no_quorum"`
	Undiverse ReplicationBucket `json:"undiverse"`
	// Domains are the failure domains in use, ordered by tier as the
	// localities declare them (region before rack) and then by value.
	Domains []ReplicationDomain `json:"domains,omitempty"`
	// Tiers names the locality tier keys in use, outermost first.
	Tiers []string `json:"tiers,omitempty"`
	// DefaultFactor is the cluster's replication factor; a range whose
	// database names a placement policy is measured against that
	// instead (issue #176), so the buckets are right for a database
	// pinned to a region with a count of its own.
	DefaultFactor int `json:"default_replication_factor"`
	// Unlocalized counts nodes declaring no locality: they belong to no
	// failure domain, so the projections below say nothing about them.
	Unlocalized int `json:"unlocalized_nodes,omitempty"`
}

// replicationStatus computes the section from the cluster's range
// descriptors and the node registry.
func (n *Node) replicationStatus(descs []kvpb.RangeDescriptor, nodes []ClusterNode) ClusterReplication {
	out := ClusterReplication{DefaultFactor: base.DefaultReplicationFactor}
	live := map[int]bool{}
	byNode := map[int]ClusterNode{}
	locOf := map[int]base.Locality{}
	for _, cn := range nodes {
		byNode[cn.NodeID] = cn
		live[cn.NodeID] = cn.Live
		if nd, ok := n.registry.Get(base.NodeID(cn.NodeID)); ok {
			locOf[cn.NodeID] = nd.Locality
			if len(nd.Locality.Tiers) == 0 {
				out.Unlocalized++
			}
		}
	}

	// The tiers in use, outermost first, in the order the localities
	// declare them — a cluster using region/zone/rack answers "lose a
	// region" and "lose a rack" separately.
	var tiers []string
	seenTier := map[string]bool{}
	for _, cn := range nodes {
		for _, t := range locOf[cn.NodeID].Tiers {
			if !seenTier[t.Key] {
				seenTier[t.Key] = true
				tiers = append(tiers, t.Key)
			}
		}
	}
	out.Tiers = tiers

	type domainKey struct{ tier, value string }
	domains := map[domainKey]*ReplicationDomain{}
	domainNodes := map[domainKey]map[int]bool{}
	for _, cn := range nodes {
		for _, t := range locOf[cn.NodeID].Tiers {
			k := domainKey{t.Key, t.Value}
			d := domains[k]
			if d == nil {
				d = &ReplicationDomain{Tier: t.Key, Value: t.Value}
				domains[k] = d
				domainNodes[k] = map[int]bool{}
			}
			d.Nodes++
			if cn.Live {
				d.Live++
			}
			d.Leases += cn.LeaderCount
			domainNodes[k][cn.NodeID] = true
		}
	}

	for _, d := range descs {
		id := int64(d.RangeID)
		want := n.placementOf(d).ReplicasOr(base.DefaultReplicationFactor)
		total, liveReps := len(d.Replicas), 0
		seenLoc := map[string]bool{}
		dup := false
		for _, rep := range d.Replicas {
			nid := int(rep.NodeID)
			if live[nid] {
				liveReps++
			}
			l := locOf[nid].String()
			if l != "" {
				if seenLoc[l] {
					dup = true
				}
				seenLoc[l] = true
			}
			for _, t := range locOf[nid].Tiers {
				if dm := domains[domainKey{t.Key, t.Value}]; dm != nil {
					dm.Replicas++
				}
			}
		}
		switch {
		case liveReps*2 <= total:
			out.NoQuorum.add(id)
		case liveReps < want || total < want:
			out.Under.add(id)
		case total > want:
			out.Over.add(id)
		default:
			out.Healthy.add(id)
		}
		if dup {
			out.Undiverse.add(id)
		}
		// What the loss of each domain would cost this range: the
		// replicas outside it must still be a majority of the whole.
		for k, members := range domainNodes {
			lost := 0
			for _, rep := range d.Replicas {
				if members[int(rep.NodeID)] {
					lost++
				}
			}
			if lost == 0 {
				continue
			}
			survivors := total - lost
			dm := domains[k]
			if survivors*2 <= total {
				dm.LosesQuorum++
				if dm.ExampleAtRisk == 0 {
					dm.ExampleAtRisk = id
				}
			} else if (survivors-1)*2 <= total {
				dm.BareMajority++
			}
		}
	}

	tierRank := map[string]int{}
	for i, t := range tiers {
		tierRank[t] = i
	}
	for _, d := range domains {
		out.Domains = append(out.Domains, *d)
	}
	sort.Slice(out.Domains, func(i, j int) bool {
		if a, b := tierRank[out.Domains[i].Tier], tierRank[out.Domains[j].Tier]; a != b {
			return a < b
		}
		if out.Domains[i].Tier != out.Domains[j].Tier {
			return out.Domains[i].Tier < out.Domains[j].Tier
		}
		return out.Domains[i].Value < out.Domains[j].Value
	})
	return out
}

// newOpID mints an id pairing the two ends of a long-running operation
// in the event ring (issue #153). Node-local and short: the pairing only
// has to be unique within one node's ring.
func newOpID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}
