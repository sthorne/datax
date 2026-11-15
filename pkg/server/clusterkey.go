package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// The cluster key (issue #310; docs/design-key-service.md and
// docs/encryption.md, "Cluster key"). One 32-byte key per cluster,
// versioned, that wraps the named keys of datax_keys (pkg/keystore). Each
// node keeps it wrapped by its own root in its data directory
// (enc.ClusterKeyFileName) and unwrapped only in held memory.
//
// Three pieces of replicated state, none of them secret:
//
//   - the record (keys.ClusterKeyMarkerKey): the current id and version,
//     the versions a rotation is still leaving, imported keys, and for
//     each a check value (enc.ClusterKeyCheck) that lets a node verify
//     the key it holds. Named keys may be created only once it exists.
//   - an operation in progress (keys.ClusterKeyPendingKey): the keys an
//     enabling, rotation or import is handing to every node, so that a
//     retry after a partial failure finishes it instead of starting
//     another.
//   - each node's holder record (keys.ClusterKeyHolderKey): the root its
//     store key is under and the versions it holds.
//
// Keys move between nodes two ways, both over mutual TLS with the node
// certificate, both sealed inside it for the one exchange (the exchange
// of peer unseal, enc.SealClusterKeys):
//
//   - pushed, by the node an operator asked (init, rotate, import, sync):
//     it asks each member at its registered address for an exchange key
//     (cluster-key-offer) and sends the keys sealed for it
//     (cluster-key-install). The member takes only keys the replicated
//     state names, by their check values: a peer cannot give it a key of
//     its own choosing.
//   - handed over, at the asking of a node that lacks a key the record
//     names — one that joined after the key was made, or whose copy is
//     gone (cluster-key-handover). The answering node applies peer
//     unseal's checks (node certificate, this cluster, a member, not
//     being decommissioned) and peer unseal's reason for its liveness
//     rule: a second copy of a running node is handed nothing. A node
//     asks while it runs, so it is live; the rule therefore recognises
//     the running node itself by where it is — a live asker is answered
//     only from the host the cluster has it registered at, and anything
//     else waits until the node it claims to be is no longer live.
//     Hand-over is not governed by the peer-unseal setting, which is
//     about a root key that opens a disk; see docs/encryption.md.
//
// The files: this one has the state, its replicated record, the status
// and the key store's view of the key; clusterkey_held.go a node's own
// copies and the background loop; clusterkey_handover.go the hand-over;
// clusterkey_push.go pushing to members and the preflight;
// clusterkey_enable.go the operations' shared steps and init;
// clusterkey_rotate.go rotation and sync; clusterkey_export.go export
// and import; clusterkey_restore.go what a restore does with named keys.

// clusterKeyInterval paces the background loop: holder reports,
// reconciling what is held with the record, and asking for a missing key.
const clusterKeyInterval = 2 * time.Second

// clusterKeyMaxBackoff caps the wait between rounds of asking that got
// nothing.
const clusterKeyMaxBackoff = time.Minute

// clusterKeyAskMinGap is the least time between two hand-over requests
// one node id may make of this node.
const clusterKeyAskMinGap = 500 * time.Millisecond

// clusterKeyCall bounds one call to a peer.
const clusterKeyCall = 15 * time.Second

// ckCheck is one cluster key as the replicated state names it.
type ckCheck struct {
	enc.ClusterKeyRef
	Check       string `json:"check"`
	Imported    bool   `json:"imported,omitempty"`
	FromCluster string `json:"from_cluster,omitempty"`
}

// ckMarker is the cluster key's record.
type ckMarker struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
	Check   string `json:"check"`
	// Previous are versions a rotation has not finished leaving: rows of
	// datax_keys may still be sealed under them.
	Previous []ckCheck `json:"previous,omitempty"`
	// Imported are other clusters' keys every node holds, so that their
	// backups' datax_keys rows can be opened and rewrapped.
	Imported  []ckCheck `json:"imported,omitempty"`
	EnabledAt int64     `json:"enabled_at"`
	RotatedAt int64     `json:"rotated_at,omitempty"`
}

func (m *ckMarker) current() ckCheck {
	return ckCheck{ClusterKeyRef: enc.ClusterKeyRef{ID: m.ID, Version: m.Version}, Check: m.Check}
}

// accepted lists every key the record names: current, previous, imported.
func (m *ckMarker) accepted() []ckCheck {
	if m == nil {
		return nil
	}
	out := []ckCheck{m.current()}
	out = append(out, m.Previous...)
	return append(out, m.Imported...)
}

func (m *ckMarker) find(ref enc.ClusterKeyRef) (ckCheck, bool) {
	for _, c := range m.accepted() {
		if c.ClusterKeyRef == ref {
			return c, true
		}
	}
	return ckCheck{}, false
}

// ckPending is an operation that is handing keys to every node.
type ckPending struct {
	Op          string      `json:"op"`
	Keys        []ckCheck   `json:"keys"`
	Coordinator base.NodeID `json:"coordinator"`
	StartedAt   int64       `json:"started_at"`
}

func (p *ckPending) find(ref enc.ClusterKeyRef) (ckCheck, bool) {
	if p == nil {
		return ckCheck{}, false
	}
	for _, c := range p.Keys {
		if c.ClusterKeyRef == ref {
			return c, true
		}
	}
	return ckCheck{}, false
}

// ckHolder is one node's report of itself.
type ckHolder struct {
	Root      string   `json:"root"`
	Encrypted bool     `json:"encrypted"`
	Holds     []string `json:"holds,omitempty"`
}

// heldClusterKey is one cluster key in this node's memory.
type heldClusterKey struct {
	ckCheck
	key []byte // enc.HoldKey
}

type ckOffer struct {
	x  *enc.PeerExchange
	at time.Time
}

// clusterKeyState is a node's part in the cluster key.
type clusterKeyState struct {
	mu   sync.Mutex
	held map[enc.ClusterKeyRef]*heldClusterKey
	// offers are this node's open exchanges, by nonce, awaiting an
	// install.
	offers map[string]ckOffer
	// lastAsk is when each node id last asked this node for a hand-over.
	lastAsk map[base.NodeID]time.Time
	// reported is the holder record last written.
	reported string
	// said is the last answer logged per peer when asking, so a wait is
	// not a flood; nextAsk paces the asking.
	said    map[string]string
	nextAsk time.Time
	backoff time.Duration
	noRoot  bool

	// pushFilter, set by tests, fails a push to a node before it is sent.
	pushFilter atomic.Pointer[func(to base.NodeID) error]
	// stopBeforeRewrap, set by tests, ends a rotation once the new
	// version is recorded, before any named key is rewrapped: the state
	// a rotation interrupted there leaves.
	stopBeforeRewrap atomic.Bool

	// opMu serializes init, rotate, import and sync on this node.
	opMu    sync.Mutex
	granted atomic.Int64
	refused atomic.Int64
}

// ---- replicated state ----

type kvGetter interface {
	Get(ctx context.Context, key keys.Key) ([]byte, error)
}

func readJSON[T any](ctx context.Context, g kvGetter, key keys.Key) (*T, error) {
	raw, err := g.Get(ctx, key)
	if err != nil || raw == nil {
		return nil, err
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("corrupt %s: %w", keys.Key(key), err)
	}
	return &v, nil
}

func (n *Node) readCKMarker(ctx context.Context) (*ckMarker, error) {
	return readJSON[ckMarker](ctx, n.db, keys.ClusterKeyMarkerKey())
}

func (n *Node) readCKPending(ctx context.Context) (*ckPending, error) {
	return readJSON[ckPending](ctx, n.db, keys.ClusterKeyPendingKey())
}

func (n *Node) readCKHolders(ctx context.Context) (map[base.NodeID]ckHolder, error) {
	lo, hi := keys.ClusterKeyHolderSpan()
	kvs, err := n.db.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	out := map[base.NodeID]ckHolder{}
	for _, kv := range kvs {
		var h ckHolder
		if json.Unmarshal(kv.Value, &h) != nil {
			continue
		}
		rest := keys.Key(kv.Key)[len(lo):]
		var id int64
		for i := 0; i < 8 && i < len(rest); i++ {
			id = id<<8 | int64(rest[i])
		}
		id ^= -1 << 63 // EncodeInt64 flips the sign bit
		out[base.NodeID(id)] = h
	}
	return out, nil
}

// ---- status ----

// ClusterKeyStatus is the cluster key as the cluster records it.
func (n *Node) ClusterKeyStatus(ctx context.Context) (*cluster.ClusterKeyStatus, error) {
	out := &cluster.ClusterKeyStatus{ClusterID: n.ident.ClusterID.String()}
	if n.db == nil || n.ClusterVersion() < version.V22 {
		return out, nil
	}
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return out, err
	}
	if p, err := n.readCKPending(ctx); err == nil && p != nil {
		refs := make([]string, len(p.Keys))
		for i, c := range p.Keys {
			refs[i] = c.ClusterKeyRef.String()
		}
		out.Pending = fmt.Sprintf("%s of %s, begun by n%d", p.Op, strings.Join(refs, ", "), p.Coordinator)
	}
	var current string
	if m != nil {
		out.Enabled, out.ID, out.Version = true, m.ID, m.Version
		for _, c := range m.Previous {
			out.Previous = append(out.Previous, c.Version)
		}
		for _, c := range m.Imported {
			out.Imported = append(out.Imported, c.ClusterKeyRef.String())
		}
		current = m.current().ClusterKeyRef.String()
	}
	holders, err := n.readCKHolders(ctx)
	if err != nil {
		return out, err
	}
	now, grace := n.clock.Now().WallTime, n.livenessGrace()
	all := n.registry.All()
	sort.Slice(all, func(i, j int) bool { return all[i].NodeID < all[j].NodeID })
	for _, nd := range all {
		h, ok := holders[nd.NodeID]
		cn := cluster.ClusterKeyNode{NodeID: nd.NodeID, Live: time.Duration(now-nd.LivenessTime) < grace || nd.NodeID == n.ident.NodeID,
			Decommissioning: nd.Draining, Reported: ok, Root: h.Root, Holds: h.Holds}
		for _, r := range h.Holds {
			cn.HoldsCurrent = cn.HoldsCurrent || (current != "" && r == current)
		}
		out.Nodes = append(out.Nodes, cn)
	}
	return out, nil
}

func (n *Node) clusterKeyStatusResponse(ctx context.Context, notes ...string) cluster.AdminResponse {
	st, err := n.ClusterKeyStatus(ctx)
	ans := &cluster.ClusterKeyAnswer{Status: st, Notes: notes}
	if err != nil {
		ans.Notes = append(ans.Notes, "status incomplete: "+err.Error())
	}
	return cluster.AdminResponse{ClusterKey: ans}
}

// ---- the key store's view of the cluster key ----

// KeyStore returns the named-key store (pkg/keystore) over this node's
// cluster key.
func (n *Node) KeyStore() (*keystore.Store, error) {
	if n.db == nil {
		return nil, errors.New("the store is not open yet")
	}
	cat, err := n.catalogAccessor()
	if err != nil {
		return nil, err
	}
	return keystore.New(keystore.Config{
		DB: n.db, Catalog: cat, ClusterKeys: nodeClusterKeys{n},
		Ready:  n.clusterKeyReady,
		Accept: n.clusterKeyAccept,
	}), nil
}

// KeyService returns the node's key service (pkg/keystore.Service): the
// SQL statements and functions of the key service, and the cache of
// named keys' material that this gateway shares with anything else that
// uses named keys. nil before the store is open.
func (n *Node) KeyService() *keystore.Service {
	n.keyServiceOnce.Do(func() {
		ks, err := n.KeyStore()
		if err != nil {
			return
		}
		n.keyService = keystore.NewService(keystore.ServiceConfig{
			Store:   ks,
			Lease:   n.cfg.KeyCacheLease,
			Rate:    keystore.ClusterSettingRate(n.db),
			MaxKeys: n.cfg.KeyCacheMaxKeys,
		})
		if err := n.stopper.RunWorker(n.keyService.Run); err != nil {
			log.Debugf("key cache sweeper: %v", err)
		}
	})
	return n.keyService
}

// clusterKeyReady refuses named keys until the cluster key is enabled.
func (n *Node) clusterKeyReady(ctx context.Context) error {
	if n.ClusterVersion() < version.V22 {
		return fmt.Errorf("named keys need cluster version %s: finalize the upgrade first", version.V22)
	}
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return err
	}
	if m == nil {
		return errors.New("the cluster key is not enabled: run `datax debug cluster-key init` first")
	}
	return nil
}

// clusterKeyAccept is checked inside every transaction that seals
// material under ref: ref must still be a version of this cluster's own
// key that the record names. Read in the transaction, it orders the
// write against a rotation's retirement of a version.
func (n *Node) clusterKeyAccept(ctx context.Context, txn *kvclient.Txn, ref enc.ClusterKeyRef) error {
	m, err := readJSON[ckMarker](ctx, txn, keys.ClusterKeyMarkerKey())
	if err != nil {
		return err
	}
	if m == nil {
		return errors.New("the cluster key is not enabled")
	}
	if c, ok := m.find(ref); !ok || c.Imported {
		return fmt.Errorf("cluster key %s is no longer current (now %s); retry", ref, m.current().ClusterKeyRef)
	}
	return nil
}

type nodeClusterKeys struct{ n *Node }

func (k nodeClusterKeys) Current(ctx context.Context) (enc.ClusterKeyRef, []byte, error) {
	m, err := k.n.readCKMarker(ctx)
	if err != nil {
		return enc.ClusterKeyRef{}, nil, err
	}
	if m == nil {
		return enc.ClusterKeyRef{}, nil, errors.New("the cluster key is not enabled")
	}
	c := m.current()
	key, ok := k.n.heldClusterKeyCopy(c.ClusterKeyRef)
	if !ok || !enc.CheckClusterKey(key, c.ClusterKeyRef, c.Check) {
		enc.Zero(key)
		return enc.ClusterKeyRef{}, nil, fmt.Errorf("n%d does not hold the current cluster key %s yet (it asks its peers for it; `datax debug cluster-key status`)", k.n.ident.NodeID, c.ClusterKeyRef)
	}
	return c.ClusterKeyRef, key, nil
}

func (k nodeClusterKeys) Key(ctx context.Context, ref enc.ClusterKeyRef) ([]byte, error) {
	m, err := k.n.readCKMarker(ctx)
	if err != nil {
		return nil, err
	}
	c, ok := m.find(ref)
	if !ok {
		return nil, fmt.Errorf("cluster key %s is not one this cluster holds (import it with `datax debug cluster-key import`)", ref)
	}
	key, held := k.n.heldClusterKeyCopy(ref)
	if !held || !enc.CheckClusterKey(key, ref, c.Check) {
		enc.Zero(key)
		return nil, fmt.Errorf("n%d does not hold cluster key %s yet", k.n.ident.NodeID, ref)
	}
	return key, nil
}
