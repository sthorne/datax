package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Peer unseal (issues #292, #307). With a root key held as shares, every
// start of every node needs a quorum of people. That is the right price
// for a cold start and the wrong one for a rolling restart: the cluster
// is up, two of its three nodes hold the root key in memory, and the
// third is a member they have known all along.
//
// So a sealed node, before it settles down to wait for shares, asks the
// peers it knew for the root key; and a running node hands it over when
// — and only when — the asker
//
//   - presents the cluster's NODE certificate over mutual TLS (a client
//     certificate, root's included, is not a node),
//   - names this cluster and the root key this node itself runs under,
//   - is a member of the cluster by its node id, and not one being
//     decommissioned,
//   - is not running: its last heartbeat is older than the liveness
//     grace, so a second copy of a live node is handed nothing,
//
// and the cluster's peer-unseal setting is on. Every request is audited
// on the node that answers it, granted or refused, and counted.
//
// What this keeps: a disk that leaves the building cannot open itself,
// because it cannot reach the cluster. What it does not keep: whoever
// holds a node's certificate and key AND can reach the cluster is a
// node, as far as the cluster can tell — so with peer unseal on, the
// node key sitting on the same disk as the data is what stands between
// a stolen disk and the store, for an attacker who is also on the
// network. Operators for whom that is in scope turn the setting off, or
// keep the node key off the data's disk.

// SealIdentityFile is, in the data directory of a node under a share
// root, the little a sealed node must know before its store is open:
// which cluster and node it is, and where its peers were. None of it is
// secret (it is in every node's registry and every certificate's
// subject), and it is not trusted for anything but where to ask: a peer
// decides by its own registry, not by what the asker claims to be.
const SealIdentityFile = "SEAL-IDENTITY"

type sealIdentity struct {
	ClusterID string      `json:"cluster_id"`
	NodeID    base.NodeID `json:"node_id"`
	Peers     []string    `json:"peers"`
}

// writeSealIdentity refreshes the file; called from the heartbeat, so it
// follows the registry.
func (n *Node) writeSealIdentity() {
	if n.cfg.Dir == "" {
		return
	}
	n.encKeyMu.Lock()
	_, shared := n.encRoot.(*enc.ShareRoot)
	n.encKeyMu.Unlock()
	if !shared {
		return
	}
	id := sealIdentity{ClusterID: n.ident.ClusterID.String(), NodeID: n.ident.NodeID}
	for _, nd := range n.registry.All() {
		if nd.NodeID != n.ident.NodeID && nd.Address != "" {
			id.Peers = append(id.Peers, nd.Address)
			// Its locality address too, where the two share a tier: a
			// sealed node asks each peer address in turn, so the public
			// one still answers if the private network does not.
			if loc, ok := base.SelectLocalityAddress(nd.LocalityAddresses, n.cfg.Locality); ok && loc != nd.Address {
				id.Peers = append(id.Peers, loc)
			}
		}
	}
	sort.Strings(id.Peers)
	raw, err := json.Marshal(id)
	if err != nil {
		return
	}
	path := filepath.Join(n.cfg.Dir, SealIdentityFile)
	if prev, err := os.ReadFile(path); err == nil && string(prev) == string(raw) {
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		log.Debugf("writing %s: %v", SealIdentityFile, err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Debugf("writing %s: %v", SealIdentityFile, err)
	}
}

func readSealIdentity(dir string) (sealIdentity, bool) {
	var id sealIdentity
	raw, err := os.ReadFile(filepath.Join(dir, SealIdentityFile))
	if err != nil || json.Unmarshal(raw, &id) != nil || id.NodeID == 0 || id.ClusterID == "" {
		return id, false
	}
	return id, true
}

func peerUnsealBinding(clusterID string, nodeID base.NodeID, keyID string) []byte {
	return []byte(fmt.Sprintf("%s|%d|%s", clusterID, nodeID, keyID))
}

// peerUnsealInterval is how often a sealed node asks round its peers.
const peerUnsealInterval = 2 * time.Second

// askPeersToUnseal runs while the node is sealed: it asks each peer in
// turn for the root key until one hands it over, the root is opened some
// other way (shares), or ctx ends. It needs TLS — without it there are
// no node identities for a peer to check — and a store that has been
// open before: a node that has never been a member has no id to be
// known by, and waits for shares.
func (n *Node) askPeersToUnseal(ctx context.Context, root *enc.ShareRoot) {
	if n.tlsCfgs == nil || n.cfg.Dir == "" {
		return
	}
	ident, ok := readSealIdentity(n.cfg.Dir)
	keyID := root.KeyID()
	if !ok || keyID == "" {
		return
	}
	peers := append([]string(nil), ident.Peers...)
	if j := strings.TrimSpace(n.cfg.Join); j != "" {
		peers = append(peers, j)
	}
	if len(peers) == 0 {
		return
	}
	trans := rpc.NewTransport(n.clock, nil, nil)
	trans.SetTLS(n.tlsCfgs.Client)
	defer trans.Close()
	binding := peerUnsealBinding(ident.ClusterID, ident.NodeID, keyID)
	said := map[string]string{} // the last answer logged per peer, so a wait is not a flood
	log.Infof("sealed: asking %d peer(s) for root key %s (peer unseal); shares are accepted meanwhile", len(peers), keyID)
	for {
		for _, addr := range peers {
			if !root.Sealed() || ctx.Err() != nil {
				return
			}
			x, err := enc.NewPeerExchange()
			if err != nil {
				return
			}
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			var resp cluster.AdminResponse
			err = trans.Call(cctx, addr, "admin", cluster.AdminRequest{Op: "peer-unseal", Peer: &cluster.PeerUnsealRequest{
				ClusterID: ident.ClusterID, NodeID: ident.NodeID, KeyID: keyID, Exchange: x.Public(),
			}}, &resp)
			cancel()
			var answer string
			switch {
			case err != nil:
				answer = "unreachable"
			case resp.Error != "":
				answer = resp.Error
			case resp.Peer == nil:
				answer = "no answer (the peer predates peer unseal)"
			default:
				if err := root.OpenFromPeer(x, resp.Peer.Exchange, resp.Peer.Sealed, binding); err != nil {
					answer = err.Error()
					break
				}
				log.Infof("root key %s handed over by the peer at %s", keyID, addr)
				log.Audit("peer-unseal-received", "from", addr, "key_id", keyID, "node", int64(ident.NodeID))
				return
			}
			if said[addr] != answer {
				said[addr] = answer
				log.Infof("peer unseal: %s: %s", addr, answer)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-root.Opened():
			return
		case <-time.After(peerUnsealInterval):
		}
	}
}

// peerUnsealState is the answering side's memory.
type peerUnsealState struct {
	off     atomic.Bool
	granted atomic.Int64
	refused atomic.Int64
	mu      sync.Mutex
	last    map[base.NodeID]time.Time
}

// peerUnsealMinGap is the least time between two requests one node id
// may make of this node. An honest sealed node asks every couple of
// seconds; this is against a loop.
const peerUnsealMinGap = 500 * time.Millisecond

// servePeerUnseal decides whether to hand the root key to a sealed peer.
func (n *Node) servePeerUnseal(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	p := req.Peer
	cn := rpc.PeerCN(ctx)
	var node base.NodeID
	if p != nil {
		node = p.NodeID
	}
	refuse := func(reason string) cluster.AdminResponse {
		n.peerUnseal.refused.Add(1)
		log.Audit("peer-unseal", "outcome", "refused", "node", int64(node), "principal", cn, "reason", reason)
		n.events.Record("peer-unseal", "refused to hand the root key to n%d: %s", node, reason)
		return cluster.AdminResponse{Error: "peer unseal refused: " + reason}
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
	n.encKeyMu.Lock()
	root, _ := n.encRoot.(*enc.ShareRoot)
	n.encKeyMu.Unlock()
	if root == nil || root.Sealed() {
		return refuse("this node's store key is not under a root key held as shares")
	}
	if n.peerUnseal.off.Load() {
		return refuse("peer unseal is turned off for this cluster; the node must be unsealed with shares")
	}
	if p.ClusterID != n.ident.ClusterID.String() {
		return refuse("the request is for another cluster")
	}
	if p.KeyID != root.KeyID() {
		return refuse(fmt.Sprintf("the node waits for root key %s; this node runs under %s", p.KeyID, root.KeyID()))
	}
	if p.NodeID == n.ident.NodeID {
		return refuse("the request claims this node's own id")
	}
	n.peerUnseal.mu.Lock()
	if n.peerUnseal.last == nil {
		n.peerUnseal.last = map[base.NodeID]time.Time{}
	}
	prev, asked := n.peerUnseal.last[p.NodeID]
	n.peerUnseal.last[p.NodeID] = time.Now()
	n.peerUnseal.mu.Unlock()
	if asked && time.Since(prev) < peerUnsealMinGap {
		return refuse("asked again too soon")
	}
	nd, ok := n.registry.Get(p.NodeID)
	if !ok {
		return refuse(fmt.Sprintf("n%d is not a member of this cluster", p.NodeID))
	}
	if nd.Draining {
		return refuse(fmt.Sprintf("n%d is being decommissioned", p.NodeID))
	}
	if age := time.Duration(n.clock.Now().WallTime - nd.LivenessTime); age < n.livenessGrace() {
		return refuse(fmt.Sprintf("n%d is live (its last heartbeat was %s ago): a second copy of a running node is not handed the key; a node that has just stopped is answered once %s have passed", p.NodeID, age.Round(100*time.Millisecond), n.livenessGrace()))
	}
	pub, sealed, err := root.HandTo(p.Exchange, peerUnsealBinding(p.ClusterID, p.NodeID, p.KeyID))
	if err != nil {
		return refuse(err.Error())
	}
	n.peerUnseal.granted.Add(1)
	log.Audit("peer-unseal", "outcome", "granted", "node", int64(p.NodeID), "principal", cn, "key_id", p.KeyID)
	n.events.Record("peer-unseal", "handed root key %s to restarting node n%d", p.KeyID, p.NodeID)
	return cluster.AdminResponse{Peer: &cluster.PeerUnsealAnswer{Exchange: pub, Sealed: sealed}}
}

// loadPeerUnsealSetting reads the store's copy of the setting at start.
func (n *Node) loadPeerUnsealSetting() {
	if n.engine == nil {
		return
	}
	if raw, err := n.engine.Get(keys.StorePeerUnsealKey()); err == nil && string(raw) == "off" {
		n.peerUnseal.off.Store(true)
	}
}

// setPeerUnsealLocal adopts a value of the setting and keeps the store's
// copy in step.
func (n *Node) setPeerUnsealLocal(off bool) {
	if n.peerUnseal.off.Swap(off) == off || n.engine == nil {
		return
	}
	val := "on"
	if off {
		val = "off"
	}
	if err := n.engine.Put(keys.StorePeerUnsealKey(), []byte(val)); err != nil {
		log.Debugf("recording the peer-unseal setting: %v", err)
	}
	log.Infof("peer unseal is %s for this cluster", val)
}

// mirrorPeerUnsealSetting follows the cluster's setting; called from the
// heartbeat.
func (n *Node) mirrorPeerUnsealSetting(ctx context.Context) {
	raw, err := n.db.Get(ctx, keys.PeerUnsealSettingKey())
	if err != nil {
		return
	}
	n.setPeerUnsealLocal(string(raw) == "off")
}

// servePeerUnsealSetting reports the setting and, given Enable, changes
// it for the cluster.
func (n *Node) servePeerUnsealSetting(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	if req.Enable != nil {
		val := "on"
		if !*req.Enable {
			val = "off"
		}
		if err := n.db.Put(ctx, keys.PeerUnsealSettingKey(), []byte(val)); err != nil {
			return cluster.AdminResponse{Error: fmt.Sprintf("recording the setting: %v", err)}
		}
		n.setPeerUnsealLocal(!*req.Enable)
		n.events.Record("peer-unseal", "peer unseal turned %s for the cluster", val)
	}
	return n.serveSealStatus()
}

func (n *Node) peerUnsealSetting() string {
	if n.peerUnseal.off.Load() {
		return "off"
	}
	return "on"
}
