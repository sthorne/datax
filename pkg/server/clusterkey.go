package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
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

// ---- held keys ----

// holdClusterKeys puts keys into held memory, replacing any of the same
// ref.
func (n *Node) holdClusterKeys(ks []enc.TransportKey) {
	st := &n.clusterKey
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.held == nil {
		st.held = map[enc.ClusterKeyRef]*heldClusterKey{}
	}
	for _, tk := range ks {
		if old, ok := st.held[tk.ClusterKeyRef]; ok {
			enc.ReleaseKey(old.key)
		}
		st.held[tk.ClusterKeyRef] = &heldClusterKey{
			ckCheck: ckCheck{ClusterKeyRef: tk.ClusterKeyRef, Check: enc.ClusterKeyCheck(tk.Key, tk.ClusterKeyRef), Imported: tk.Imported, FromCluster: tk.FromCluster},
			key:     enc.HoldKey(tk.Key),
		}
	}
}

// heldClusterKey returns a copy of a held key, which the caller zeroes.
func (n *Node) heldClusterKeyCopy(ref enc.ClusterKeyRef) ([]byte, bool) {
	st := &n.clusterKey
	st.mu.Lock()
	defer st.mu.Unlock()
	h, ok := st.held[ref]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), h.key...), true
}

// heldClusterKeys lists what this node holds.
func (n *Node) heldClusterKeys() []ckCheck {
	st := &n.clusterKey
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make([]ckCheck, 0, len(st.held))
	for _, h := range st.held {
		out = append(out, h.ckCheck)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].Version < out[j].Version
	})
	return out
}

// transportKeys copies the held keys named by refs, for sealing.
func (n *Node) transportKeys(refs []ckCheck) ([]enc.TransportKey, error) {
	var out []enc.TransportKey
	for _, c := range refs {
		k, ok := n.heldClusterKeyCopy(c.ClusterKeyRef)
		if !ok || !enc.CheckClusterKey(k, c.ClusterKeyRef, c.Check) {
			enc.Zero(k)
			enc.ZeroTransportKeys(out)
			return nil, fmt.Errorf("this node does not hold cluster key %s", c.ClusterKeyRef)
		}
		out = append(out, enc.TransportKey{ClusterKeyRef: c.ClusterKeyRef, Imported: c.Imported, FromCluster: c.FromCluster, Key: k})
	}
	return out, nil
}

// clusterKeyRoot is the root this node wraps its copies under.
func (n *Node) clusterKeyRoot() (enc.Root, []enc.Root) {
	n.encKeyMu.Lock()
	defer n.encKeyMu.Unlock()
	return n.encRoot, append([]enc.Root(nil), n.encRoots...)
}

// persistClusterKeys wraps ks under this node's root, writes them into
// its cluster key file (replacing entries of the same ref) and holds
// them. The file is written before the keys are held or acknowledged,
// so an acknowledged key survives a restart.
func (n *Node) persistClusterKeys(ctx context.Context, ks []enc.TransportKey) error {
	root, _ := n.clusterKeyRoot()
	if root == nil {
		return errors.New("this node's store key is not under a root (--enc-root), so it has nothing to keep the cluster key under")
	}
	if n.cfg.Dir == "" {
		return errors.New("this node has no data directory to keep the cluster key in")
	}
	n.clusterKey.mu.Lock()
	defer n.clusterKey.mu.Unlock()
	entries, err := enc.ReadClusterKeyFile(vfs.Default, n.cfg.Dir)
	if err != nil && !isNotExist(err) {
		return fmt.Errorf("reading %s: %w", enc.ClusterKeyFileName, err)
	}
	wctx := enc.WithBinding(ctx, n.storeKeyBinding())
	for _, tk := range ks {
		e, err := enc.WrapClusterKey(wctx, root, tk.ClusterKeyRef, tk.Key)
		if err != nil {
			return err
		}
		if tk.Imported {
			if err := e.SetImported(tk.Key, tk.FromCluster); err != nil {
				return err
			}
		}
		kept := entries[:0]
		for _, old := range entries {
			if old.ClusterKeyRef != tk.ClusterKeyRef {
				kept = append(kept, old)
			}
		}
		entries = append(kept, e)
	}
	if err := enc.WriteClusterKeyFile(vfs.Default, n.cfg.Dir, entries); err != nil {
		return fmt.Errorf("writing %s: %w", enc.ClusterKeyFileName, err)
	}
	st := &n.clusterKey
	if st.held == nil {
		st.held = map[enc.ClusterKeyRef]*heldClusterKey{}
	}
	for _, tk := range ks {
		if old, ok := st.held[tk.ClusterKeyRef]; ok {
			enc.ReleaseKey(old.key)
		}
		st.held[tk.ClusterKeyRef] = &heldClusterKey{
			ckCheck: ckCheck{ClusterKeyRef: tk.ClusterKeyRef, Check: enc.ClusterKeyCheck(tk.Key, tk.ClusterKeyRef), Imported: tk.Imported, FromCluster: tk.FromCluster},
			key:     enc.HoldKey(tk.Key),
		}
	}
	return nil
}

func isNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }

// dropClusterKeys removes refs from this node's file and memory: the
// version a finished rotation left, or a key an abandoned enabling made.
func (n *Node) dropClusterKeys(refs []enc.ClusterKeyRef, why string) {
	if len(refs) == 0 {
		return
	}
	drop := map[enc.ClusterKeyRef]bool{}
	for _, r := range refs {
		drop[r] = true
	}
	st := &n.clusterKey
	st.mu.Lock()
	defer st.mu.Unlock()
	if n.cfg.Dir != "" {
		if entries, err := enc.ReadClusterKeyFile(vfs.Default, n.cfg.Dir); err == nil {
			kept := entries[:0]
			for _, e := range entries {
				if !drop[e.ClusterKeyRef] {
					kept = append(kept, e)
				}
			}
			if len(kept) != len(entries) {
				if err := enc.WriteClusterKeyFile(vfs.Default, n.cfg.Dir, kept); err != nil {
					log.Warnf("cluster key: removing %v from %s: %v", refs, enc.ClusterKeyFileName, err)
					return
				}
			}
		}
	}
	for r := range drop {
		if h, ok := st.held[r]; ok {
			enc.ReleaseKey(h.key)
			delete(st.held, r)
		}
	}
	log.Infof("cluster key: dropped %v (%s)", refs, why)
	log.Audit("cluster-key-dropped", "keys", refList(refs), "reason", why)
}

func refList(refs []enc.ClusterKeyRef) string {
	s := make([]string, len(refs))
	for i, r := range refs {
		s[i] = r.String()
	}
	return strings.Join(s, ",")
}

// releaseClusterKeys zeroes every held copy: the node is stopping.
func (n *Node) releaseClusterKeys() {
	st := &n.clusterKey
	st.mu.Lock()
	defer st.mu.Unlock()
	for r, h := range st.held {
		enc.ReleaseKey(h.key)
		delete(st.held, r)
	}
}

// loadClusterKeys unwraps this node's copies at start, through its root.
// A copy that does not unwrap is left for a peer to hand over again.
func (n *Node) loadClusterKeys(ctx context.Context) {
	if n.cfg.Dir == "" || !enc.ClusterKeyFileExists(vfs.Default, n.cfg.Dir) {
		return
	}
	entries, err := enc.ReadClusterKeyFile(vfs.Default, n.cfg.Dir)
	if err != nil {
		log.Warnf("cluster key: %v; the node will ask its peers for the key", err)
		return
	}
	_, roots := n.clusterKeyRoot()
	if len(roots) == 0 {
		log.Warnf("cluster key: %s is present but this node's store key is not under a root (--enc-root); the copies in it cannot be opened", enc.ClusterKeyFileName)
		return
	}
	uctx := enc.WithBinding(ctx, n.storeKeyBinding())
	var got []enc.TransportKey
	for i := range entries {
		e := &entries[i]
		k, err := enc.UnwrapClusterKey(uctx, roots, e)
		if err != nil {
			log.Warnf("cluster key %s: %v; the node will ask its peers for it", e.ClusterKeyRef, err)
			continue
		}
		got = append(got, enc.TransportKey{ClusterKeyRef: e.ClusterKeyRef, Imported: e.Imported, FromCluster: e.FromCluster, Key: k})
	}
	n.holdClusterKeys(got)
	refs := make([]string, len(got))
	for i, tk := range got {
		refs[i] = tk.ClusterKeyRef.String()
	}
	enc.ZeroTransportKeys(got)
	if len(got) > 0 {
		log.Infof("cluster key: %d cop(ies) unwrapped by the root (%s)", len(got), strings.Join(refs, ", "))
		log.Audit("cluster-key-unwrap", "keys", strings.Join(refs, ","))
	}
}

// ---- the background loop ----

func (n *Node) clusterKeyLoop(ctx context.Context) {
	t := time.NewTicker(clusterKeyInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n.clusterKeyTick(ctx)
	}
}

// clusterKeyTick reports this node, settles what it holds against the
// record, and asks for what it lacks. Exported through
// RunClusterKeyTickForTesting.
func (n *Node) clusterKeyTick(ctx context.Context) {
	if n.db == nil || n.ClusterVersion() < version.V22 {
		return
	}
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return
	}
	p, err := n.readCKPending(ctx)
	if err != nil {
		return
	}
	n.reconcileClusterKeys(m, p)
	n.writeCKHolder(ctx)
	if m == nil {
		return
	}
	var missing []ckCheck
	for _, c := range m.accepted() {
		if k, ok := n.heldClusterKeyCopy(c.ClusterKeyRef); !ok || !enc.CheckClusterKey(k, c.ClusterKeyRef, c.Check) {
			missing = append(missing, c)
		} else {
			enc.Zero(k)
		}
	}
	if len(missing) > 0 {
		n.askPeersForClusterKey(ctx, m, missing)
	}
}

// RunClusterKeyTickForTesting runs one pass of the background loop.
func (n *Node) RunClusterKeyTickForTesting(ctx context.Context) { n.clusterKeyTick(ctx) }

// reconcileClusterKeys drops what the record says is gone: versions of
// the current key older than the current and not still being left, keys
// whose check no longer matches what the record or the pending
// operation names, and keys of an enabling that was abandoned. Nothing
// is dropped while there is no record: a key is never discarded on the
// strength of an absence.
func (n *Node) reconcileClusterKeys(m *ckMarker, p *ckPending) {
	if m == nil {
		return
	}
	var drop []enc.ClusterKeyRef
	for _, h := range n.heldClusterKeys() {
		want, inMarker := m.find(h.ClusterKeyRef)
		if !inMarker {
			want, inMarker = p.find(h.ClusterKeyRef)
		}
		switch {
		case !inMarker:
			drop = append(drop, h.ClusterKeyRef)
		case want.Check != h.Check:
			drop = append(drop, h.ClusterKeyRef)
		}
	}
	n.dropClusterKeys(drop, "no longer named by the cluster key's record")
}

// writeCKHolder writes this node's holder record when it changed.
func (n *Node) writeCKHolder(ctx context.Context) {
	h := ckHolder{}
	if root, _ := n.clusterKeyRoot(); root != nil {
		h.Root = root.Kind()
	}
	h.Encrypted = n.engine != nil && n.engine.Encrypted()
	for _, c := range n.heldClusterKeys() {
		h.Holds = append(h.Holds, c.ClusterKeyRef.String())
	}
	raw, err := json.Marshal(h)
	if err != nil {
		return
	}
	n.clusterKey.mu.Lock()
	same := n.clusterKey.reported == string(raw)
	n.clusterKey.mu.Unlock()
	if same {
		return
	}
	if err := n.db.Put(ctx, keys.ClusterKeyHolderKey(int64(n.ident.NodeID)), raw); err != nil {
		log.Debugf("cluster key: writing the holder record: %v", err)
		return
	}
	n.clusterKey.mu.Lock()
	n.clusterKey.reported = string(raw)
	n.clusterKey.mu.Unlock()
}

// ---- hand-over: the asking side ----

func handoverBinding(clusterID string, node base.NodeID) []byte {
	return []byte(fmt.Sprintf("datax cluster key handover|%s|%d", clusterID, node))
}

func pushBinding(clusterID string, node base.NodeID, nonce string) []byte {
	return []byte(fmt.Sprintf("datax cluster key push|%s|%d|%s", clusterID, node, nonce))
}

func exportBinding(clusterID string) []byte {
	return []byte("datax cluster key export|" + clusterID)
}

// askPeersForClusterKey asks running peers, one at a time, for the keys
// the record names and this node lacks; it keeps what checks out.
func (n *Node) askPeersForClusterKey(ctx context.Context, m *ckMarker, missing []ckCheck) {
	st := &n.clusterKey
	if self, ok := n.registry.Get(n.ident.NodeID); ok && self.Draining {
		return // a node being decommissioned is handed nothing
	}
	st.mu.Lock()
	if time.Now().Before(st.nextAsk) {
		st.mu.Unlock()
		return
	}
	// A round that gets nothing doubles the wait before the next, up to
	// a minute, so that a node every peer refuses (one whose connections
	// do not come from its registered host) is not a stream of refusals
	// in every peer's audit log. A key received resets it.
	if st.backoff < clusterKeyInterval {
		st.backoff = clusterKeyInterval
	}
	st.nextAsk = time.Now().Add(st.backoff)
	if st.backoff *= 2; st.backoff > clusterKeyMaxBackoff {
		st.backoff = clusterKeyMaxBackoff
	}
	if st.said == nil {
		st.said = map[string]string{}
	}
	st.mu.Unlock()
	if root, _ := n.clusterKeyRoot(); root == nil {
		st.mu.Lock()
		warned := st.noRoot
		st.noRoot = true
		st.mu.Unlock()
		if !warned {
			log.Warnf("cluster key: the cluster has one, but this node's store key is not under a root (--enc-root), so it cannot keep a copy; move it with `datax debug enc-root adopt`")
		}
		return
	}
	if n.tlsCfgs == nil {
		return
	}
	clusterID := n.ident.ClusterID.String()
	peers := n.registry.All()
	sort.Slice(peers, func(i, j int) bool { return peers[i].NodeID < peers[j].NodeID })
	for _, nd := range peers {
		if nd.NodeID == n.ident.NodeID || nd.Address == "" || nd.Draining {
			continue
		}
		x, err := enc.NewPeerExchange()
		if err != nil {
			return
		}
		cctx, cancel := context.WithTimeout(ctx, clusterKeyCall)
		var resp cluster.AdminResponse
		err = n.trans.Call(cctx, nd.Address, "admin", cluster.AdminRequest{Op: "cluster-key-handover", ClusterKey: &cluster.ClusterKeyRequest{
			ClusterID: clusterID, NodeID: n.ident.NodeID, Exchange: x.Public(),
		}}, &resp)
		cancel()
		var answer string
		switch {
		case err != nil:
			answer = "unreachable"
		case resp.Error != "":
			answer = resp.Error
		case resp.ClusterKey == nil:
			answer = "no answer (the peer predates the cluster key)"
		default:
			got, err := x.OpenClusterKeys(resp.ClusterKey.Exchange, resp.ClusterKey.Sealed, handoverBinding(clusterID, n.ident.NodeID))
			if err != nil {
				answer = err.Error()
				break
			}
			keep, refused := checkAgainst(got, func(ref enc.ClusterKeyRef) (ckCheck, bool) { return m.find(ref) })
			if len(refused) > 0 {
				log.Warnf("cluster key: n%d handed over %s, which the cluster's record does not name; not kept", nd.NodeID, strings.Join(refused, ", "))
			}
			var perr error
			if len(keep) > 0 {
				perr = n.persistClusterKeys(ctx, keep)
			}
			refs := transportRefs(keep)
			enc.ZeroTransportKeys(got)
			if perr != nil {
				log.Warnf("cluster key: keeping what n%d handed over: %v", nd.NodeID, perr)
				return
			}
			if len(refs) > 0 {
				metrics.ClusterKeyInstalls.WithLabelValues("handover").Add(float64(len(refs)))
				log.Infof("cluster key: %s handed over by n%d", strings.Join(refs, ", "), nd.NodeID)
				log.Audit("cluster-key-received", "from", int64(nd.NodeID), "keys", strings.Join(refs, ","), "via", "handover")
				n.events.Record("cluster-key", "cluster key %s handed over by n%d", strings.Join(refs, ", "), nd.NodeID)
				st.mu.Lock()
				st.backoff, st.nextAsk = 0, time.Time{}
				st.mu.Unlock()
				if n.holdsAll(missing) {
					return
				}
			}
			answer = "handed over nothing this node lacks"
		}
		st.mu.Lock()
		if st.said[nd.Address] != answer {
			st.said[nd.Address] = answer
			log.Infof("cluster key: asking n%d for the key: %s", nd.NodeID, answer)
		}
		st.mu.Unlock()
	}
}

func (n *Node) holdsAll(want []ckCheck) bool {
	for _, c := range want {
		k, ok := n.heldClusterKeyCopy(c.ClusterKeyRef)
		good := ok && enc.CheckClusterKey(k, c.ClusterKeyRef, c.Check)
		enc.Zero(k)
		if !good {
			return false
		}
	}
	return true
}

// checkAgainst keeps the keys whose check matches what find names.
func checkAgainst(got []enc.TransportKey, find func(enc.ClusterKeyRef) (ckCheck, bool)) (keep []enc.TransportKey, refused []string) {
	for _, tk := range got {
		want, ok := find(tk.ClusterKeyRef)
		if !ok || !enc.CheckClusterKey(tk.Key, tk.ClusterKeyRef, want.Check) {
			refused = append(refused, tk.ClusterKeyRef.String())
			continue
		}
		tk.Imported, tk.FromCluster = want.Imported, want.FromCluster
		keep = append(keep, tk)
	}
	return keep, refused
}

func transportRefs(ks []enc.TransportKey) []string {
	out := make([]string, len(ks))
	for i, tk := range ks {
		out[i] = tk.ClusterKeyRef.String()
	}
	return out
}

// ---- hand-over: the answering side ----

// clusterKeyAskerAllowed is the liveness rule of a hand-over. A node that
// is not live (its last heartbeat older than the grace) is answered, as
// under peer unseal. A live node is answered only when its connection
// comes from the host it is registered at: that is the running node
// itself asking (it joined after the key was made, or lost its copy),
// while a second copy of it elsewhere — a stolen disk and node
// certificate — is handed nothing until the node it claims to be has
// stopped. resolve maps a registered host name to addresses.
func clusterKeyAskerAllowed(age, grace time.Duration, registered, from string, resolve func(string) []string) (bool, string) {
	if age >= grace {
		return true, ""
	}
	host, _, err := net.SplitHostPort(registered)
	if err != nil {
		host = registered
	}
	if from != "" && host != "" {
		if net.ParseIP(host) != nil {
			if net.ParseIP(host).Equal(net.ParseIP(from)) {
				return true, ""
			}
		} else if resolve != nil {
			for _, a := range resolve(host) {
				if net.ParseIP(a).Equal(net.ParseIP(from)) {
					return true, ""
				}
			}
		}
	}
	return false, fmt.Sprintf("it is live (its last heartbeat was %s ago) at %s and this request comes from %s: a second copy of a running node is not handed the key; it is answered from anywhere once %s have passed since that node's last heartbeat",
		age.Round(100*time.Millisecond), registered, from, grace)
}

func resolveHost(host string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil
	}
	return addrs
}

// serveClusterKeyHandover decides whether to hand the cluster key to a
// peer that lacks it.
func (n *Node) serveClusterKeyHandover(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	p := req.ClusterKey
	cn := rpc.PeerCN(ctx)
	var node base.NodeID
	if p != nil {
		node = p.NodeID
	}
	refuse := func(reason string) cluster.AdminResponse {
		n.clusterKey.refused.Add(1)
		metrics.ClusterKeyHandovers.WithLabelValues("refused").Inc()
		log.Audit("cluster-key-handover", "outcome", "refused", "node", int64(node), "principal", cn, "reason", reason)
		n.events.Record("cluster-key", "refused to hand the cluster key to n%d: %s", node, reason)
		return cluster.AdminResponse{Error: "cluster key hand-over refused: " + reason}
	}
	if p == nil {
		return refuse("the request names no node")
	}
	if n.tlsCfgs == nil {
		return refuse("this cluster runs without TLS, so there is no node identity to check")
	}
	if cn != security.NodePrincipal {
		return refuse(fmt.Sprintf("only the cluster's node certificate may ask (connected as %q)", cn))
	}
	if n.ClusterVersion() < version.V22 {
		return refuse(fmt.Sprintf("the cluster key needs cluster version %s", version.V22))
	}
	if p.ClusterID != n.ident.ClusterID.String() {
		return refuse("the request is for another cluster")
	}
	if p.NodeID == n.ident.NodeID {
		return refuse("the request claims this node's own id")
	}
	st := &n.clusterKey
	st.mu.Lock()
	if st.lastAsk == nil {
		st.lastAsk = map[base.NodeID]time.Time{}
	}
	prev, asked := st.lastAsk[p.NodeID]
	st.lastAsk[p.NodeID] = time.Now()
	st.mu.Unlock()
	if asked && time.Since(prev) < clusterKeyAskMinGap {
		return refuse("asked again too soon")
	}
	nd, ok := n.registry.Get(p.NodeID)
	if !ok {
		return refuse(fmt.Sprintf("n%d is not a member of this cluster", p.NodeID))
	}
	if nd.Draining {
		return refuse(fmt.Sprintf("n%d is being decommissioned", p.NodeID))
	}
	age := time.Duration(n.clock.Now().WallTime - nd.LivenessTime)
	if ok, why := clusterKeyAskerAllowed(age, n.livenessGrace(), nd.Address, rpc.PeerHost(ctx), resolveHost); !ok {
		return refuse(fmt.Sprintf("n%d: %s", p.NodeID, why))
	}
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return refuse("reading the cluster key's record: " + err.Error())
	}
	if m == nil {
		return refuse("this cluster has no cluster key")
	}
	if holders, err := n.readCKHolders(ctx); err == nil {
		if h, ok := holders[p.NodeID]; ok && h.Root == "" {
			return refuse(fmt.Sprintf("n%d reports that its store key is not under a root (--enc-root)", p.NodeID))
		}
	}
	var have []ckCheck
	for _, c := range m.accepted() {
		if k, ok := n.heldClusterKeyCopy(c.ClusterKeyRef); ok {
			if enc.CheckClusterKey(k, c.ClusterKeyRef, c.Check) {
				have = append(have, c)
			}
			enc.Zero(k)
		}
	}
	if _, ok := m.find(m.current().ClusterKeyRef); !ok || len(have) == 0 || have[0].ClusterKeyRef != m.current().ClusterKeyRef {
		return refuse("this node does not hold the current cluster key itself")
	}
	ks, err := n.transportKeys(have)
	if err != nil {
		return refuse(err.Error())
	}
	pub, sealed, err := enc.SealClusterKeys(p.Exchange, handoverBinding(p.ClusterID, p.NodeID), ks)
	refs := transportRefs(ks)
	enc.ZeroTransportKeys(ks)
	if err != nil {
		return refuse(err.Error())
	}
	st.granted.Add(1)
	metrics.ClusterKeyHandovers.WithLabelValues("granted").Inc()
	log.Audit("cluster-key-handover", "outcome", "granted", "node", int64(p.NodeID), "principal", cn, "keys", strings.Join(refs, ","))
	n.events.Record("cluster-key", "handed cluster key %s to n%d", strings.Join(refs, ", "), p.NodeID)
	return cluster.AdminResponse{ClusterKey: &cluster.ClusterKeyAnswer{Exchange: pub, Sealed: sealed}}
}

// ---- push: the receiving side ----

const maxCKOffers = 16

// serveClusterKeyOffer opens an exchange for a peer that is about to
// push keys to this node.
func (n *Node) serveClusterKeyOffer(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	p := req.ClusterKey
	cn := rpc.PeerCN(ctx)
	if n.tlsCfgs == nil || cn != security.NodePrincipal {
		log.Audit("cluster-key-offer", "outcome", "refused", "principal", cn)
		return cluster.AdminResponse{Error: fmt.Sprintf("cluster key offer refused: only the cluster's node certificate may push the cluster key (connected as %q)", cn)}
	}
	if p == nil || p.ClusterID != n.ident.ClusterID.String() || p.NodeID != n.ident.NodeID {
		return cluster.AdminResponse{Error: "cluster key offer refused: the offer is not for this node of this cluster"}
	}
	if root, _ := n.clusterKeyRoot(); root == nil {
		return cluster.AdminResponse{Error: fmt.Sprintf("cluster key offer refused: n%d's store key is not under a root (--enc-root), so it cannot keep the cluster key", n.ident.NodeID)}
	}
	x, err := enc.NewPeerExchange()
	if err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	nonce := hex.EncodeToString(b[:])
	st := &n.clusterKey
	st.mu.Lock()
	if st.offers == nil {
		st.offers = map[string]ckOffer{}
	}
	for k, o := range st.offers {
		if time.Since(o.at) > time.Minute {
			delete(st.offers, k)
		}
	}
	if len(st.offers) >= maxCKOffers {
		st.mu.Unlock()
		return cluster.AdminResponse{Error: "cluster key offer refused: too many offers open"}
	}
	st.offers[nonce] = ckOffer{x: x, at: time.Now()}
	st.mu.Unlock()
	return cluster.AdminResponse{ClusterKey: &cluster.ClusterKeyAnswer{Exchange: x.Public(), Nonce: nonce}}
}

// serveClusterKeyInstall takes keys a peer pushed for an offer, keeps
// those the replicated state names, and acknowledges.
func (n *Node) serveClusterKeyInstall(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	p := req.ClusterKey
	cn := rpc.PeerCN(ctx)
	refuse := func(reason string) cluster.AdminResponse {
		log.Audit("cluster-key-install", "outcome", "refused", "principal", cn, "reason", reason)
		return cluster.AdminResponse{Error: "cluster key install refused: " + reason}
	}
	if n.tlsCfgs == nil || cn != security.NodePrincipal {
		return refuse(fmt.Sprintf("only the cluster's node certificate may push the cluster key (connected as %q)", cn))
	}
	if p == nil {
		return refuse("the request carries nothing")
	}
	st := &n.clusterKey
	st.mu.Lock()
	o, ok := st.offers[p.Nonce]
	delete(st.offers, p.Nonce)
	st.mu.Unlock()
	if !ok {
		return refuse("no open offer has that nonce")
	}
	got, err := o.x.OpenClusterKeys(p.Giver, p.Sealed, pushBinding(n.ident.ClusterID.String(), n.ident.NodeID, p.Nonce))
	if err != nil {
		return refuse(err.Error())
	}
	defer enc.ZeroTransportKeys(got)
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return refuse(err.Error())
	}
	pend, err := n.readCKPending(ctx)
	if err != nil {
		return refuse(err.Error())
	}
	keep, refused := checkAgainst(got, func(ref enc.ClusterKeyRef) (ckCheck, bool) {
		if c, ok := pend.find(ref); ok {
			return c, true
		}
		if m != nil {
			return m.find(ref)
		}
		return ckCheck{}, false
	})
	if len(refused) > 0 {
		return refuse(fmt.Sprintf("%s: not named by the cluster key's record or the operation in progress", strings.Join(refused, ", ")))
	}
	if err := n.persistClusterKeys(ctx, keep); err != nil {
		return refuse(err.Error())
	}
	refs := transportRefs(keep)
	metrics.ClusterKeyInstalls.WithLabelValues("push").Add(float64(len(refs)))
	log.Audit("cluster-key-received", "principal", cn, "keys", strings.Join(refs, ","), "via", "push")
	n.events.Record("cluster-key", "received cluster key %s", strings.Join(refs, ", "))
	n.writeCKHolder(ctx)
	return cluster.AdminResponse{ClusterKey: &cluster.ClusterKeyAnswer{}}
}

// ---- push: the giving side ----

// pushClusterKeys hands ks to one member: itself directly, any other at
// its registered address.
func (n *Node) pushClusterKeys(ctx context.Context, nd kvpb.NodeDescriptor, ks []enc.TransportKey) error {
	if f := n.clusterKey.pushFilter.Load(); f != nil {
		if err := (*f)(nd.NodeID); err != nil {
			return fmt.Errorf("n%d: %w", nd.NodeID, err)
		}
	}
	if nd.NodeID == n.ident.NodeID {
		return n.persistClusterKeys(ctx, ks)
	}
	clusterID := n.ident.ClusterID.String()
	cctx, cancel := context.WithTimeout(ctx, clusterKeyCall)
	defer cancel()
	var offer cluster.AdminResponse
	if err := n.trans.Call(cctx, nd.Address, "admin", cluster.AdminRequest{Op: "cluster-key-offer", ClusterKey: &cluster.ClusterKeyRequest{ClusterID: clusterID, NodeID: nd.NodeID}}, &offer); err != nil {
		return fmt.Errorf("n%d at %s: %w", nd.NodeID, nd.Address, err)
	}
	if offer.Error != "" {
		return fmt.Errorf("n%d: %s", nd.NodeID, offer.Error)
	}
	if offer.ClusterKey == nil || offer.ClusterKey.Nonce == "" {
		return fmt.Errorf("n%d: no offer (it predates the cluster key)", nd.NodeID)
	}
	pub, sealed, err := enc.SealClusterKeys(offer.ClusterKey.Exchange, pushBinding(clusterID, nd.NodeID, offer.ClusterKey.Nonce), ks)
	if err != nil {
		return err
	}
	var ack cluster.AdminResponse
	if err := n.trans.Call(cctx, nd.Address, "admin", cluster.AdminRequest{Op: "cluster-key-install", ClusterKey: &cluster.ClusterKeyRequest{
		Nonce: offer.ClusterKey.Nonce, Giver: pub, Sealed: sealed,
	}}, &ack); err != nil {
		return fmt.Errorf("n%d at %s: %w", nd.NodeID, nd.Address, err)
	}
	if ack.Error != "" {
		return fmt.Errorf("n%d: %s", nd.NodeID, ack.Error)
	}
	return nil
}

// StopClusterKeyRotationBeforeRewrapForTesting makes a rotation on this
// node stop once its new version is recorded, before the rewrap.
func (n *Node) StopClusterKeyRotationBeforeRewrapForTesting(stop bool) {
	n.clusterKey.stopBeforeRewrap.Store(stop)
}

// SetClusterKeyPushFilterForTesting makes a push from this node to a
// node fail when fn returns an error for it; nil clears it.
func (n *Node) SetClusterKeyPushFilterForTesting(fn func(to base.NodeID) error) {
	if fn == nil {
		n.clusterKey.pushFilter.Store(nil)
		return
	}
	n.clusterKey.pushFilter.Store(&fn)
}

// pushToMembers pushes ks to every member given; it returns the
// failures, by node.
func (n *Node) pushToMembers(ctx context.Context, members []kvpb.NodeDescriptor, ks []enc.TransportKey) []string {
	var failed []string
	for _, nd := range members {
		if err := n.pushClusterKeys(ctx, nd, ks); err != nil {
			failed = append(failed, err.Error())
		}
	}
	return failed
}

// clusterKeyPreflight is what every distributing operation requires:
// every member not being decommissioned is live and keeps its store key
// under a root. It returns those members.
func (n *Node) clusterKeyPreflight(ctx context.Context) ([]kvpb.NodeDescriptor, error) {
	if n.tlsCfgs == nil {
		return nil, errors.New("the cluster key needs a secure cluster: it is handed between nodes over mutual TLS with the node certificate, and an insecure cluster has neither")
	}
	if n.ClusterVersion() < version.V22 {
		return nil, fmt.Errorf("the cluster key needs cluster version %s (this cluster is at %s): finalize the upgrade with `datax debug upgrade` first", version.V22, n.ClusterVersion())
	}
	holders, err := n.readCKHolders(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading the nodes' reports: %w", err)
	}
	now, grace := n.clock.Now().WallTime, n.livenessGrace()
	all := n.registry.All()
	sort.Slice(all, func(i, j int) bool { return all[i].NodeID < all[j].NodeID })
	var members []kvpb.NodeDescriptor
	var problems []string
	for _, nd := range all {
		if nd.Draining {
			continue
		}
		if age := time.Duration(now - nd.LivenessTime); nd.NodeID != n.ident.NodeID && age >= grace {
			problems = append(problems, fmt.Sprintf("n%d is not live (last heartbeat %s ago): every member must be running to be given the key; start it, or decommission it if it is gone", nd.NodeID, age.Round(time.Second)))
			continue
		}
		h, ok := holders[nd.NodeID]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("n%d has not reported its encryption root yet (every node does within seconds of the cluster reaching %s); retry shortly", nd.NodeID, version.V22))
		case h.Root == "" && h.Encrypted:
			problems = append(problems, fmt.Sprintf("n%d keeps its store key in a key file (--enc-key): the cluster key needs --enc-root on every node; put its key under a root with `datax debug enc-root adopt` (node stopped; docs/encryption.md)", nd.NodeID))
		case h.Root == "":
			problems = append(problems, fmt.Sprintf("n%d is not encrypted: the cluster key needs --enc-root on every node, and a plaintext store cannot be moved under a root in place", nd.NodeID))
		default:
			members = append(members, nd)
		}
	}
	if len(problems) > 0 {
		return nil, errors.New(strings.Join(problems, "; "))
	}
	return members, nil
}

// ---- operations ----

func clusterKeyError(format string, args ...any) cluster.AdminResponse {
	return cluster.AdminResponse{Error: fmt.Sprintf(format, args...)}
}

// takePending makes p the operation in progress unless one is already
// recorded (which it returns instead), and refuses if the record says
// the work is done or contradicts it (check returns an error).
func (n *Node) takePending(ctx context.Context, p *ckPending, replace bool, check func(m *ckMarker, cur *ckPending) error) (*ckPending, error) {
	var out *ckPending
	err := n.db.RunTxn(ctx, "cluster-key-pending", func(ctx context.Context, txn *kvclient.Txn) error {
		m, err := readJSON[ckMarker](ctx, txn, keys.ClusterKeyMarkerKey())
		if err != nil {
			return err
		}
		cur, err := readJSON[ckPending](ctx, txn, keys.ClusterKeyPendingKey())
		if err != nil {
			return err
		}
		if err := check(m, cur); err != nil {
			return err
		}
		if p == nil || (cur != nil && !replace) {
			// Only reading, or one is recorded already: that one stands.
			out = cur
			return nil
		}
		raw, err := json.Marshal(p)
		if err != nil {
			return err
		}
		out = p
		return txn.Put(ctx, keys.ClusterKeyPendingKey(), raw)
	})
	return out, err
}

// resumeOrMint returns the keys of the operation in progress when this
// node holds them, and otherwise mints fresh ones (mint) and records
// them as the operation, replacing an operation whose keys nobody here
// holds. Minting happens only for keys no record names, so no data is
// ever under them.
func (n *Node) resumeOrMint(ctx context.Context, op string, check func(m *ckMarker, cur *ckPending) error, mint func(m *ckMarker) ([]enc.TransportKey, error)) ([]enc.TransportKey, *ckPending, error) {
	cur, err := n.takePending(ctx, nil, false, check)
	if err != nil {
		return nil, nil, err
	}
	if cur != nil {
		if cur.Op != op {
			return nil, nil, fmt.Errorf("a %s of the cluster key is in progress; run `datax debug cluster-key %s` to finish it first", cur.Op, cur.Op)
		}
		if ks, err := n.transportKeys(cur.Keys); err == nil {
			return ks, cur, nil
		}
		log.Infof("cluster key: an unfinished %s's keys are not held here; making them again", op)
	}
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return nil, nil, err
	}
	ks, err := mint(m)
	if err != nil {
		return nil, nil, err
	}
	p := &ckPending{Op: op, Coordinator: n.ident.NodeID, StartedAt: n.clock.Now().WallTime}
	for _, tk := range ks {
		p.Keys = append(p.Keys, ckCheck{ClusterKeyRef: tk.ClusterKeyRef, Check: enc.ClusterKeyCheck(tk.Key, tk.ClusterKeyRef), Imported: tk.Imported, FromCluster: tk.FromCluster})
	}
	// This node's own copy first: a pending operation is recorded only
	// once at least one node can finish it.
	if err := n.persistClusterKeys(ctx, ks); err != nil {
		enc.ZeroTransportKeys(ks)
		return nil, nil, err
	}
	err = n.db.RunTxn(ctx, "cluster-key-pending", func(ctx context.Context, txn *kvclient.Txn) error {
		m, err := readJSON[ckMarker](ctx, txn, keys.ClusterKeyMarkerKey())
		if err != nil {
			return err
		}
		now, err := readJSON[ckPending](ctx, txn, keys.ClusterKeyPendingKey())
		if err != nil {
			return err
		}
		if err := check(m, now); err != nil {
			return err
		}
		if now != nil && (cur == nil || now.StartedAt != cur.StartedAt || now.Coordinator != cur.Coordinator) {
			return fmt.Errorf("another node started a %s of the cluster key at the same moment; run the command again to finish it", now.Op)
		}
		raw, err := json.Marshal(p)
		if err != nil {
			return err
		}
		return txn.Put(ctx, keys.ClusterKeyPendingKey(), raw)
	})
	if err != nil {
		enc.ZeroTransportKeys(ks)
		return nil, nil, err
	}
	return ks, p, nil
}

// finishPending writes the record change the operation was for and
// clears it, in one transaction.
func (n *Node) finishPending(ctx context.Context, p *ckPending, change func(m *ckMarker) (*ckMarker, error)) (*ckMarker, error) {
	var out *ckMarker
	err := n.db.RunTxn(ctx, "cluster-key-finish", func(ctx context.Context, txn *kvclient.Txn) error {
		m, err := readJSON[ckMarker](ctx, txn, keys.ClusterKeyMarkerKey())
		if err != nil {
			return err
		}
		cur, err := readJSON[ckPending](ctx, txn, keys.ClusterKeyPendingKey())
		if err != nil {
			return err
		}
		if cur == nil || cur.StartedAt != p.StartedAt || cur.Coordinator != p.Coordinator {
			return errors.New("the operation was taken over or finished by another node; run the command again")
		}
		next, err := change(m)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(next)
		if err != nil {
			return err
		}
		if err := txn.Put(ctx, keys.ClusterKeyMarkerKey(), raw); err != nil {
			return err
		}
		out = next
		return txn.Delete(ctx, keys.ClusterKeyPendingKey())
	})
	return out, err
}

// serveClusterKeyInit enables the cluster key: one is made here, handed
// to every member, and recorded once every member has acknowledged.
func (n *Node) serveClusterKeyInit(ctx context.Context) cluster.AdminResponse {
	st := &n.clusterKey
	st.opMu.Lock()
	defer st.opMu.Unlock()
	if m, err := n.readCKMarker(ctx); err != nil {
		return clusterKeyError("reading the cluster key's record: %v", err)
	} else if m != nil {
		return n.clusterKeyStatusResponse(ctx, fmt.Sprintf("the cluster key is already enabled (%s)", m.current().ClusterKeyRef))
	}
	members, err := n.clusterKeyPreflight(ctx)
	if err != nil {
		return clusterKeyError("cluster key init refused: %v", err)
	}
	check := func(m *ckMarker, _ *ckPending) error {
		if m != nil {
			return errors.New("the cluster key was enabled meanwhile")
		}
		return nil
	}
	ks, p, err := n.resumeOrMint(ctx, "init", check, func(*ckMarker) ([]enc.TransportKey, error) {
		id, err := enc.NewClusterKeyID()
		if err != nil {
			return nil, err
		}
		k, err := enc.NewClusterKey()
		if err != nil {
			return nil, err
		}
		return []enc.TransportKey{{ClusterKeyRef: enc.ClusterKeyRef{ID: id, Version: 1}, Key: k}}, nil
	})
	if err != nil {
		return clusterKeyError("cluster key init: %v", err)
	}
	defer enc.ZeroTransportKeys(ks)
	ref := ks[0].ClusterKeyRef
	if failed := n.pushToMembers(ctx, members, ks); len(failed) > 0 {
		log.Audit("cluster-key-init", "outcome", "incomplete", "key", ref.String(), "failed", strings.Join(failed, "; "))
		return clusterKeyError("cluster key init is incomplete: %s. Nothing uses the key yet; run `datax debug cluster-key init` again to finish handing out the same key", strings.Join(failed, "; "))
	}
	m, err := n.finishPending(ctx, p, func(m *ckMarker) (*ckMarker, error) {
		if m != nil {
			return nil, errors.New("the cluster key was enabled meanwhile")
		}
		return &ckMarker{ID: ref.ID, Version: ref.Version, Check: p.Keys[0].Check, EnabledAt: n.clock.Now().WallTime}, nil
	})
	if err != nil {
		return clusterKeyError("cluster key init: every node holds the key, but recording it failed: %v; run the command again", err)
	}
	notes := []string{fmt.Sprintf("cluster key %s enabled; every member (%d) holds it", m.current().ClusterKeyRef, len(members))}
	if cat, err := n.catalogAccessor(); err == nil {
		if _, err := sql.EnsureKeysTable(ctx, n.db, cat); err != nil {
			notes = append(notes, fmt.Sprintf("the %s table will be created on first use: %v", "datax_keys", err))
		}
	}
	log.Audit("cluster-key-init", "outcome", "ok", "key", ref.String(), "members", int64(len(members)))
	n.events.Record("cluster-key", "cluster key %s enabled on %d node(s)", ref, len(members))
	log.Infof("cluster key %s enabled on %d node(s)", ref, len(members))
	return n.clusterKeyStatusResponse(ctx, notes...)
}

// serveClusterKeyRotate makes a new version of the cluster key, hands it
// to every member, rewraps every datax_keys row under it, and retires
// the old version. A run that stops anywhere is finished by running it
// again.
func (n *Node) serveClusterKeyRotate(ctx context.Context) cluster.AdminResponse {
	st := &n.clusterKey
	st.opMu.Lock()
	defer st.opMu.Unlock()
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return clusterKeyError("reading the cluster key's record: %v", err)
	}
	if m == nil {
		return clusterKeyError("cluster key rotate refused: the cluster key is not enabled (`datax debug cluster-key init`)")
	}
	var notes []string
	pend, err := n.readCKPending(ctx)
	if err != nil {
		return clusterKeyError("%v", err)
	}
	if len(m.Previous) == 0 || pend != nil {
		members, err := n.clusterKeyPreflight(ctx)
		if err != nil {
			return clusterKeyError("cluster key rotate refused: %v", err)
		}
		check := func(cm *ckMarker, _ *ckPending) error {
			if cm == nil {
				return errors.New("the cluster key's record is gone")
			}
			return nil
		}
		ks, p, err := n.resumeOrMint(ctx, "rotate", check, func(cm *ckMarker) ([]enc.TransportKey, error) {
			k, err := enc.NewClusterKey()
			if err != nil {
				return nil, err
			}
			return []enc.TransportKey{{ClusterKeyRef: enc.ClusterKeyRef{ID: cm.ID, Version: cm.Version + 1}, Key: k}}, nil
		})
		if err != nil {
			return clusterKeyError("cluster key rotate: %v", err)
		}
		next := ks[0].ClusterKeyRef
		failed := n.pushToMembers(ctx, members, ks)
		enc.ZeroTransportKeys(ks)
		if len(failed) > 0 {
			log.Audit("cluster-key-rotate", "outcome", "incomplete", "key", next.String(), "failed", strings.Join(failed, "; "))
			return clusterKeyError("cluster key rotate is incomplete: %s. Nothing uses the new version yet; run `datax debug cluster-key rotate` again to finish", strings.Join(failed, "; "))
		}
		if _, err = n.finishPending(ctx, p, func(cm *ckMarker) (*ckMarker, error) {
			if cm == nil || cm.ID != next.ID || cm.Version+1 != next.Version {
				return nil, errors.New("the cluster key's record changed meanwhile")
			}
			out := *cm
			out.Previous = append(append([]ckCheck(nil), cm.Previous...), cm.current())
			out.Version, out.Check, out.RotatedAt = next.Version, p.Keys[0].Check, n.clock.Now().WallTime
			return &out, nil
		}); err != nil {
			return clusterKeyError("cluster key rotate: every node holds the new version, but recording it failed: %v; run the command again", err)
		}
		notes = append(notes, fmt.Sprintf("cluster key rotated to %s; every member (%d) holds it", next, len(members)))
		log.Audit("cluster-key-rotate", "outcome", "ok", "key", next.String())
		n.events.Record("cluster-key", "cluster key rotated to %s", next)
	} else {
		notes = append(notes, fmt.Sprintf("finishing an unfinished rotation to %s", m.current().ClusterKeyRef))
	}
	if st.stopBeforeRewrap.Load() {
		return clusterKeyError("cluster key rotate: stopped before the rewrap (testing); run it again to finish")
	}
	// Rewrap and retire.
	done, rewrapped, err := n.retirePreviousClusterKeys(ctx)
	if err != nil {
		return clusterKeyError("cluster key rotate: the new version is current, but rewrapping the named keys stopped: %v; run `datax debug cluster-key rotate` again to finish (the old version is kept until then)", err)
	}
	notes = append(notes, fmt.Sprintf("%d named key(s) rewrapped", rewrapped))
	if done {
		notes = append(notes, "the old version is destroyed: every node drops its copy")
	}
	return n.clusterKeyStatusResponse(ctx, notes...)
}

// retirePreviousClusterKeys rewraps every named key not under the current
// cluster key, and once none is under a previous version, drops the
// previous versions from the record (and so from every node). The check
// and the record change are one transaction, so a key created under a
// previous version meanwhile is caught rather than stranded.
func (n *Node) retirePreviousClusterKeys(ctx context.Context) (bool, int, error) {
	ks, err := n.KeyStore()
	if err != nil {
		return false, 0, err
	}
	total := 0
	for attempt := 0; attempt < 5; attempt++ {
		res, err := ks.RewrapAll(ctx, false)
		total += res.Rewrapped
		if err != nil {
			return false, total, err
		}
		retired := false
		var dropped []enc.ClusterKeyRef
		err = n.db.RunTxn(ctx, "cluster-key-retire", func(ctx context.Context, txn *kvclient.Txn) error {
			retired, dropped = false, nil
			m, err := readJSON[ckMarker](ctx, txn, keys.ClusterKeyMarkerKey())
			if err != nil || m == nil || len(m.Previous) == 0 {
				retired = err == nil
				return err
			}
			refs, err := keystore.RefsIn(ctx, txn, n.keysDesc(ctx))
			if err != nil {
				return err
			}
			for _, c := range m.Previous {
				if refs[c.ClusterKeyRef] > 0 {
					return nil // not yet: rewrap again
				}
			}
			for _, c := range m.Previous {
				dropped = append(dropped, c.ClusterKeyRef)
			}
			m.Previous = nil
			raw, err := json.Marshal(m)
			if err != nil {
				return err
			}
			retired = true
			return txn.Put(ctx, keys.ClusterKeyMarkerKey(), raw)
		})
		if err != nil {
			return false, total, err
		}
		if retired {
			if len(dropped) > 0 {
				n.dropClusterKeys(dropped, "a rotation finished")
				log.Audit("cluster-key-retired", "keys", refList(dropped))
				n.events.Record("cluster-key", "cluster key %s destroyed after rotation", refList(dropped))
			}
			return true, total, nil
		}
		if len(res.Unopened) > 0 {
			return false, total, fmt.Errorf("named keys under cluster keys this node does not hold: %v", res.Unopened)
		}
	}
	return false, total, errors.New("named keys kept appearing under the old version")
}

// keysDesc is datax_keys' descriptor, or nil.
func (n *Node) keysDesc(ctx context.Context) *catalog.TableDescriptor {
	cat, err := n.catalogAccessor()
	if err != nil {
		return nil
	}
	d, err := sql.LookupKeysTable(ctx, n.db, cat)
	if err != nil {
		return nil
	}
	return d
}

// serveClusterKeySync pushes the keys the record names to every live
// member that does not report holding them: the operator's remedy for a
// node a hand-over refused.
func (n *Node) serveClusterKeySync(ctx context.Context) cluster.AdminResponse {
	st := &n.clusterKey
	st.opMu.Lock()
	defer st.opMu.Unlock()
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return clusterKeyError("%v", err)
	}
	if m == nil {
		return clusterKeyError("cluster key sync refused: the cluster key is not enabled")
	}
	if n.tlsCfgs == nil {
		return clusterKeyError("cluster key sync refused: the cluster key needs a secure cluster")
	}
	holders, err := n.readCKHolders(ctx)
	if err != nil {
		return clusterKeyError("%v", err)
	}
	ks, err := n.transportKeys(m.accepted())
	if err != nil {
		return clusterKeyError("cluster key sync: %v (sync from a node that holds every key; `datax debug cluster-key status` lists who does)", err)
	}
	defer enc.ZeroTransportKeys(ks)
	want := map[string]bool{}
	for _, c := range m.accepted() {
		want[c.ClusterKeyRef.String()] = true
	}
	now, grace := n.clock.Now().WallTime, n.livenessGrace()
	var notes, failed []string
	for _, nd := range n.registry.All() {
		if nd.Draining || nd.NodeID == n.ident.NodeID || time.Duration(now-nd.LivenessTime) >= grace {
			continue
		}
		h := holders[nd.NodeID]
		have := map[string]bool{}
		for _, r := range h.Holds {
			have[r] = true
		}
		complete := true
		for r := range want {
			complete = complete && have[r]
		}
		if complete {
			continue
		}
		if err := n.pushClusterKeys(ctx, nd, ks); err != nil {
			failed = append(failed, err.Error())
			continue
		}
		notes = append(notes, fmt.Sprintf("n%d given the cluster key", nd.NodeID))
	}
	log.Audit("cluster-key-sync", "given", strings.Join(notes, "; "), "failed", strings.Join(failed, "; "))
	if len(failed) > 0 {
		return clusterKeyError("cluster key sync: %s", strings.Join(failed, "; "))
	}
	if len(notes) == 0 {
		notes = append(notes, "every live member already holds the cluster key")
	}
	return n.clusterKeyStatusResponse(ctx, notes...)
}

// serveClusterKeyExport hands the cluster's keys to the CLI, sealed for
// the exchange it opened, to be wrapped there under the operator's root.
// Only over mutual TLS, to an admin; the request names no root.
func (n *Node) serveClusterKeyExport(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	if n.tlsCfgs == nil {
		return clusterKeyError("cluster key export refused: the cluster key leaves the cluster only over mutual TLS, and this cluster runs without TLS")
	}
	if req.ClusterKey == nil || len(req.ClusterKey.Exchange) == 0 {
		return clusterKeyError("cluster key export refused: the request carries no exchange key")
	}
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return clusterKeyError("%v", err)
	}
	if m == nil {
		return clusterKeyError("cluster key export refused: the cluster key is not enabled")
	}
	ks, err := n.transportKeys(m.accepted())
	if err != nil {
		return clusterKeyError("cluster key export: %v", err)
	}
	refs := transportRefs(ks)
	pub, sealed, err := enc.SealClusterKeys(req.ClusterKey.Exchange, exportBinding(n.ident.ClusterID.String()), ks)
	enc.ZeroTransportKeys(ks)
	if err != nil {
		return clusterKeyError("cluster key export: %v", err)
	}
	log.Audit("cluster-key-export", "principal", rpc.PeerCN(ctx), "keys", strings.Join(refs, ","))
	n.events.Record("cluster-key", "cluster key %s exported by %s", strings.Join(refs, ", "), rpc.PeerCN(ctx))
	resp := n.clusterKeyStatusResponse(ctx)
	resp.ClusterKey.Exchange, resp.ClusterKey.Sealed = pub, sealed
	return resp
}

// serveClusterKeyImport adds another cluster's keys to every node, so that
// the datax_keys rows of its backups can be opened here (and are rewrapped
// under this cluster's key by the restore).
func (n *Node) serveClusterKeyImport(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	if n.tlsCfgs == nil {
		return clusterKeyError("cluster key import refused: key material is accepted only over mutual TLS, and this cluster runs without TLS")
	}
	if req.ClusterKey == nil || len(req.ClusterKey.Keys) == 0 {
		return clusterKeyError("cluster key import refused: the request carries no key")
	}
	in := req.ClusterKey.Keys
	defer enc.ZeroTransportKeys(in)
	st := &n.clusterKey
	st.opMu.Lock()
	defer st.opMu.Unlock()
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return clusterKeyError("%v", err)
	}
	if m == nil {
		return clusterKeyError("cluster key import refused: enable this cluster's own key first (`datax debug cluster-key init`); imported keys' rows are rewrapped under it")
	}
	var add []enc.TransportKey
	var notes []string
	for _, tk := range in {
		if len(tk.Key) != enc.KeyLen || tk.ID == "" || tk.Version < 1 {
			return clusterKeyError("cluster key import refused: a malformed key in the request")
		}
		if tk.ID == m.ID {
			return clusterKeyError("cluster key import refused: %s is this cluster's own key", tk.ClusterKeyRef)
		}
		if c, ok := m.find(tk.ClusterKeyRef); ok {
			if c.Check != enc.ClusterKeyCheck(tk.Key, tk.ClusterKeyRef) {
				return clusterKeyError("cluster key import refused: %s is already imported, and the key given is a different one", tk.ClusterKeyRef)
			}
			notes = append(notes, fmt.Sprintf("%s already imported", tk.ClusterKeyRef))
			continue
		}
		tk.Imported = true
		add = append(add, tk)
	}
	if len(add) == 0 {
		return n.clusterKeyStatusResponse(ctx, notes...)
	}
	members, err := n.clusterKeyPreflight(ctx)
	if err != nil {
		return clusterKeyError("cluster key import refused: %v", err)
	}
	p := &ckPending{Op: "import", Coordinator: n.ident.NodeID, StartedAt: n.clock.Now().WallTime}
	for _, tk := range add {
		p.Keys = append(p.Keys, ckCheck{ClusterKeyRef: tk.ClusterKeyRef, Check: enc.ClusterKeyCheck(tk.Key, tk.ClusterKeyRef), Imported: true, FromCluster: tk.FromCluster})
	}
	// An unfinished import is replaced: its keys came in its request,
	// and this one carries its own.
	if _, err := n.takePending(ctx, p, true, func(cm *ckMarker, cur *ckPending) error {
		if cur != nil && cur.Op != "import" {
			return fmt.Errorf("a %s of the cluster key is in progress; run `datax debug cluster-key %s` to finish it first", cur.Op, cur.Op)
		}
		return nil
	}); err != nil {
		return clusterKeyError("cluster key import: %v", err)
	}
	if failed := n.pushToMembers(ctx, members, add); len(failed) > 0 {
		return clusterKeyError("cluster key import is incomplete: %s; run the import again", strings.Join(failed, "; "))
	}
	if _, err := n.finishPending(ctx, p, func(cm *ckMarker) (*ckMarker, error) {
		if cm == nil {
			return nil, errors.New("the cluster key's record is gone")
		}
		out := *cm
		out.Imported = append([]ckCheck(nil), cm.Imported...)
		for _, c := range p.Keys {
			if _, ok := cm.find(c.ClusterKeyRef); !ok {
				out.Imported = append(out.Imported, c)
			}
		}
		return &out, nil
	}); err != nil {
		return clusterKeyError("cluster key import: %v", err)
	}
	refs := transportRefs(add)
	log.Audit("cluster-key-import", "principal", rpc.PeerCN(ctx), "keys", strings.Join(refs, ","), "from_cluster", add[0].FromCluster)
	n.events.Record("cluster-key", "imported cluster key %s from cluster %s", strings.Join(refs, ", "), add[0].FromCluster)
	notes = append(notes, fmt.Sprintf("imported %s; every member (%d) holds it", strings.Join(refs, ", "), len(members)))
	return n.clusterKeyStatusResponse(ctx, notes...)
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
