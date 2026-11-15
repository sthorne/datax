package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Exporting the cluster key to the CLI and importing another cluster's
// (issue #310; the model is in clusterkey.go).

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
