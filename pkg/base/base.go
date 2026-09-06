// Package base holds identifier types and configuration shared across all
// layers of datax.
package base

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// NodeID uniquely identifies a node (process) within a cluster.
type NodeID int32

// StoreID uniquely identifies a Pebble store. v1 runs exactly one store per
// node, so StoreID == NodeID, but the types stay distinct to keep the door
// open for multi-store nodes.
type StoreID int32

// RangeID uniquely identifies a range (one Raft group) within a cluster.
type RangeID int64

// ReplicaID identifies a replica within one range's history. IDs are never
// reused within a range, even after a replica is removed.
type ReplicaID int32

func (n NodeID) String() string    { return fmt.Sprintf("n%d", int32(n)) }
func (s StoreID) String() string   { return fmt.Sprintf("s%d", int32(s)) }
func (r RangeID) String() string   { return fmt.Sprintf("r%d", int64(r)) }
func (r ReplicaID) String() string { return fmt.Sprintf("replica%d", int32(r)) }

// DefaultReplicationFactor is the target number of replicas per range once
// the cluster has enough nodes.
const DefaultReplicationFactor = 3

// MaxReplicationFactor bounds a per-database placement policy's replica
// count (issue #176). Beyond this a range costs more in raft traffic and
// commit latency than the extra copies are worth, and a typo — REPLICAS
// = 30 on a three-node cluster — should be refused rather than leaving
// every range permanently under-replicated.
const MaxReplicationFactor = 9

// DefaultMaxClockOffset bounds tolerated physical clock skew between nodes.
const DefaultMaxClockOffset = 500 * time.Millisecond

// DefaultGCTTL is how long MVCC history is retained before garbage
// collection reclaims it.
const DefaultGCTTL = 25 * time.Hour

// DefaultGCInterval is how often each store's GC loop scans its led ranges.
const DefaultGCInterval = 60 * time.Second

// Tier is one element of a node's ordered locality, e.g. region=us-east.
type Tier struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Locality is an ordered list of failure-domain tiers, from broadest
// (region) to narrowest (rack). Order matters for diversity scoring.
type Locality struct {
	Tiers []Tier `json:"tiers,omitempty"`
}

// ParseLocality parses "region=us-east,zone=b,rack=12".
func ParseLocality(s string) (Locality, error) {
	var l Locality
	if s == "" {
		return l, nil
	}
	seen := map[string]bool{}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			return Locality{}, fmt.Errorf("invalid locality %q: expected key=value[,key=value...]", s)
		}
		if seen[kv[0]] {
			return Locality{}, fmt.Errorf("invalid locality %q: duplicate tier %q", s, kv[0])
		}
		seen[kv[0]] = true
		l.Tiers = append(l.Tiers, Tier{Key: kv[0], Value: kv[1]})
	}
	return l, nil
}

func (l Locality) String() string {
	parts := make([]string, len(l.Tiers))
	for i, t := range l.Tiers {
		parts[i] = t.Key + "=" + t.Value
	}
	return strings.Join(parts, ",")
}

// SharedPrefix returns how many leading tiers l and other have in common.
func (l Locality) SharedPrefix(other Locality) int {
	n := 0
	for n < len(l.Tiers) && n < len(other.Tiers) && l.Tiers[n] == other.Tiers[n] {
		n++
	}
	return n
}

// Diversity returns a score in [0, 1]: 0 for identical localities, 1 for
// completely disjoint ones. Higher is more diverse.
func (l Locality) Diversity(other Locality) float64 {
	maxTiers := len(l.Tiers)
	if len(other.Tiers) > maxTiers {
		maxTiers = len(other.Tiers)
	}
	if maxTiers == 0 {
		return 0
	}
	return float64(maxTiers-l.SharedPrefix(other)) / float64(maxTiers)
}

// SortTierKeys returns the tier keys in order. Exposed for debugging output.
func (l Locality) SortTierKeys() []string {
	keys := make([]string, len(l.Tiers))
	for i, t := range l.Tiers {
		keys[i] = t.Key
	}
	sort.Strings(keys)
	return keys
}
