package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// This node's copies of the cluster key (issue #310; the model is in
// clusterkey.go): held in memory, kept wrapped in CLUSTER-KEY, and
// settled against the record by the background loop.

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
