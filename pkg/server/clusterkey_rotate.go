package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Rotating the cluster key, retiring its old version, and handing it to
// nodes that lack it (issue #310; the model is in clusterkey.go).

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
