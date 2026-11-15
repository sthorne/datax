package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Pushing the cluster key to every member (issue #310; the model is in
// clusterkey.go): the receiving side's offer and install, the giving
// side, and the preflight every distributing operation runs.

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
