// Package cluster handles cluster identity, bootstrap, membership (join),
// and the in-memory node registry. See docs/replication-and-placement.md.
package cluster

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// StoreIdent identifies a store: which cluster it belongs to and which
// node/store it is. Written once at bootstrap/join, never changed.
type StoreIdent struct {
	ClusterID uuid.UUID    `json:"cluster_id"`
	NodeID    base.NodeID  `json:"node_id"`
	StoreID   base.StoreID `json:"store_id"`
}

// ReadStoreIdent loads the ident, reporting whether the store is initialized.
func ReadStoreIdent(eng *storage.Engine) (StoreIdent, bool, error) {
	var id StoreIdent
	raw, err := eng.Get(keys.StoreIdentKey())
	if err != nil || raw == nil {
		return id, false, err
	}
	if err := json.Unmarshal(raw, &id); err != nil {
		return id, false, fmt.Errorf("corrupt store ident: %w", err)
	}
	return id, true, nil
}

// WriteStoreIdent persists the ident (synced).
func WriteStoreIdent(eng *storage.Engine, id StoreIdent) error {
	raw, err := json.Marshal(id)
	if err != nil {
		return err
	}
	b := eng.NewBatch()
	if err := b.Put(keys.StoreIdentKey(), raw); err != nil {
		_ = b.Close()
		return err
	}
	return b.Commit(true)
}

// bootstrapTimestamp is the MVCC timestamp of the cluster's pre-Raft seed
// writes. It must be identical on every seed node so their state machines
// start byte-identical.
var bootstrapTimestamp = hlc.Timestamp{WallTime: 1}

// BootstrapEngine seeds a fresh engine with the cluster's initial state:
// the store ident and — identically on every seed node — the pre-applied
// state machine of range 1: its /meta addressing record and the ID
// generation counters. These writes happen BEFORE Raft starts, forming the
// state at applied index 0; determinism across seed nodes is what makes
// this sound. The caller then creates the range-1 replica with bootstrap
// semantics.
func BootstrapEngine(eng *storage.Engine, ident StoreIdent, range1 kvpb.RangeDescriptor, seedNodes int) error {
	if _, ok, err := ReadStoreIdent(eng); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("store already initialized")
	}
	if err := WriteStoreIdent(eng, ident); err != nil {
		return err
	}
	b := eng.NewBatch()
	// /meta record for range 1.
	descJSON, err := json.Marshal(range1)
	if err != nil {
		return err
	}
	if err := storage.MVCCPut(b, keys.RangeMetaKey(range1.EndKey), bootstrapTimestamp, descJSON, nil); err != nil {
		return err
	}
	// ID generators: node IDs 1..seedNodes are taken; range ID 1 is taken.
	if err := storage.MVCCPut(b, keys.NodeIDGenKey(), bootstrapTimestamp, []byte(fmt.Sprintf("%d", seedNodes)), nil); err != nil {
		return err
	}
	if err := storage.MVCCPut(b, keys.RangeIDGenKey(), bootstrapTimestamp, []byte("1"), nil); err != nil {
		return err
	}
	return b.Commit(true)
}

// Range1Descriptor builds the initial whole-keyspace descriptor across the
// given seed nodes (replica i+1 on node i+1; StoreID == NodeID in v1).
func Range1Descriptor(nodeIDs []base.NodeID) kvpb.RangeDescriptor {
	desc := kvpb.RangeDescriptor{
		RangeID:  1,
		StartKey: keys.MinKey.Clone(),
		EndKey:   keys.MaxKey.Clone(),
	}
	for i, id := range nodeIDs {
		desc.Replicas = append(desc.Replicas, kvpb.ReplicaDescriptor{
			NodeID:    id,
			StoreID:   base.StoreID(id),
			ReplicaID: base.ReplicaID(i + 1),
		})
	}
	desc.NextReplicaID = base.ReplicaID(len(nodeIDs) + 1)
	return desc
}

// PersistRegistry saves the node registry to a local store key, letting a
// restarted node reach its peers before any range has a leader (the
// bootstrap of the bootstrap).
func PersistRegistry(eng *storage.Engine, nodes []kvpb.NodeDescriptor) error {
	raw, err := json.Marshal(nodes)
	if err != nil {
		return err
	}
	return eng.Put(keys.StoreRegistryKey(), raw)
}

// LoadPersistedRegistry reads the registry saved by PersistRegistry.
func LoadPersistedRegistry(eng *storage.Engine) ([]kvpb.NodeDescriptor, error) {
	raw, err := eng.Get(keys.StoreRegistryKey())
	if err != nil || raw == nil {
		return nil, err
	}
	var nodes []kvpb.NodeDescriptor
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, fmt.Errorf("corrupt persisted registry: %w", err)
	}
	return nodes, nil
}

// JoinRequest is sent by a new node to any existing node.
type JoinRequest struct {
	Address  string        `json:"address"`
	Locality base.Locality `json:"locality"`
}

// JoinResponse tells the joiner who it is and how to route.
type JoinResponse struct {
	ClusterID uuid.UUID             `json:"cluster_id"`
	NodeID    base.NodeID           `json:"node_id"`
	Nodes     []kvpb.NodeDescriptor `json:"nodes"`
	Range1    kvpb.RangeDescriptor  `json:"range1"`
	Error     string                `json:"error,omitempty"`
}

// Registry is the in-memory view of cluster membership, fed by joins and
// periodic scans of the node registry keys in range 1.
type Registry struct {
	mu    sync.Mutex
	nodes map[base.NodeID]kvpb.NodeDescriptor
}

func NewRegistry() *Registry {
	return &Registry{nodes: make(map[base.NodeID]kvpb.NodeDescriptor)}
}

func (r *Registry) Upsert(nd kvpb.NodeDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.nodes[nd.NodeID]; !ok || cur.LivenessTime <= nd.LivenessTime {
		r.nodes[nd.NodeID] = nd
	}
}

// UpsertAddress records a peer's address learned from Raft traffic without
// clobbering locality/liveness from real registry rows.
func (r *Registry) UpsertAddress(id base.NodeID, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.nodes[id]; ok {
		if cur.Address != addr {
			cur.Address = addr
			r.nodes[id] = cur
		}
		return
	}
	r.nodes[id] = kvpb.NodeDescriptor{NodeID: id, Address: addr}
}

func (r *Registry) Get(id base.NodeID) (kvpb.NodeDescriptor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	nd, ok := r.nodes[id]
	return nd, ok
}

// All returns descriptors sorted by node ID.
func (r *Registry) All() []kvpb.NodeDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]kvpb.NodeDescriptor, 0, len(r.nodes))
	for _, nd := range r.nodes {
		out = append(out, nd)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// Resolve implements rpc.Resolver.
func (r *Registry) Resolve(id base.NodeID) (string, error) {
	nd, ok := r.Get(id)
	if !ok {
		return "", fmt.Errorf("unknown node %s", id)
	}
	return nd.Address, nil
}
