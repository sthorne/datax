package server

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Hand-over of the cluster key to a node that lacks it (issue #310; the
// model is in clusterkey.go): the asking side and the answering side.

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
