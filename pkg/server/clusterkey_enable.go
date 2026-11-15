package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
)

// The distributing operations' shared steps, and enabling the cluster
// key (issue #310; the model is in clusterkey.go).

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
