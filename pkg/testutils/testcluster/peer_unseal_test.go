package testcluster

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestPeerUnseal (issue #307): under a root key held as shares, a
// running cluster vouches for a member that restarts. A rolling restart
// needs no operator; a node that is live elsewhere, an unknown node, a
// node being decommissioned, a request for another cluster or another
// key, and anyone without the node certificate are refused; with the
// setting off a restarted node waits for shares; and a cold start needs
// people for one node only — the rest unseal from it.
func TestPeerUnseal(t *testing.T) {
	const grace = 8 * time.Second
	rootKey, err := enc.NewStoreKey()
	if err != nil {
		t.Fatal(err)
	}
	shares, err := enc.SplitRootKey(nil, rootKey, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	keyID := enc.RootKeyID(rootKey)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	var (
		transMu             sync.Mutex
		asRoot, asNodeTrans *rpc.Transport
	)
	call := func(addr string, asNode bool, req cluster.AdminRequest) cluster.AdminResponse {
		transMu.Lock()
		tr := asRoot
		if asNode {
			tr = asNodeTrans
		}
		transMu.Unlock()
		cctx, ccancel := context.WithTimeout(ctx, 20*time.Second)
		defer ccancel()
		var resp cluster.AdminResponse
		if err := tr.Call(cctx, addr, "admin", req, &resp); err != nil {
			resp.Error = "transport: " + err.Error()
		}
		return resp
	}
	// withShares unseals a node by hand; sealedAt only notes that it
	// stopped to wait.
	withShares := func(addr string) {
		go func() {
			for _, s := range shares[:2] {
				call(addr, false, cluster.AdminRequest{Op: "unseal", Share: s.String()})
			}
		}()
	}
	dirs := make([]string, 3)
	for i := range dirs {
		dirs[i] = t.TempDir()
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			if err := security.CreateClientCert(cfg.CertsDir, "root"); err != nil {
				t.Fatal(err)
			}
			rootTLS, err := security.LoadClientTLS(cfg.CertsDir, "root")
			if err != nil {
				t.Fatal(err)
			}
			nodeTLS, err := security.LoadNodeTLS(cfg.CertsDir)
			if err != nil {
				t.Fatal(err)
			}
			transMu.Lock()
			asRoot = rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
			asRoot.SetTLS(rootTLS)
			asNodeTrans = rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
			asNodeTrans.SetTLS(nodeTLS.Client)
			transMu.Unlock()
		}
		cfg.Dir = dirs[i]
		cfg.EncRoot = "shares"
		cfg.GCInterval = -1
		cfg.LivenessGrace = grace
		cfg.OnSealed = withShares
	})
	for _, n := range tc.Nodes {
		tc.addrs = append(tc.addrs, n.Addr())
	}
	clusterID := tc.Nodes[0].ClusterID().String()

	restart := func(i int, onSealed func(string)) (*server.Node, error) {
		return tc.RestartNodeErr(i, nil, func(cfg *server.Config) {
			cfg.Dir = dirs[i]
			cfg.CertsDir = certsDir
			cfg.EncRoot = "shares"
			cfg.LivenessGrace = grace
			cfg.OnSealed = onSealed
		})
	}
	// waitRegistry waits until every running node's seal-status answers,
	// which it does only once its heartbeat has run.
	exchange := func() []byte {
		x, err := enc.NewPeerExchange()
		if err != nil {
			t.Fatal(err)
		}
		return x.Public()
	}
	ask := func(addr string, asNode bool, p cluster.PeerUnsealRequest) string {
		if p.Exchange == nil {
			p.Exchange = exchange()
		}
		resp := call(addr, asNode, cluster.AdminRequest{Op: "peer-unseal", Peer: &p})
		if resp.Error == "" {
			return "granted"
		}
		return resp.Error
	}
	wantRefusal := func(what, got, want string) {
		t.Helper()
		if !strings.Contains(got, want) {
			t.Fatalf("%s: %q, want a refusal containing %q", what, got, want)
		}
	}

	// The refusals, asked of n1 while everything is running.
	good := cluster.PeerUnsealRequest{ClusterID: clusterID, NodeID: 2, KeyID: keyID}
	wantRefusal("the root user's certificate", ask(tc.addrs[0], false, good), "only the cluster's node certificate")
	wantRefusal("a node that is running", ask(tc.addrs[0], true, good), "is live")
	time.Sleep(600 * time.Millisecond) // past the per-node gap between requests
	unknown := good
	unknown.NodeID = 99
	wantRefusal("a node the cluster has never had", ask(tc.addrs[0], true, unknown), "not a member")
	elsewhere := good
	elsewhere.ClusterID = "00000000-0000-0000-0000-000000000000"
	wantRefusal("another cluster", ask(tc.addrs[0], true, elsewhere), "another cluster")
	otherKey := good
	otherKey.KeyID = "0011223344556677"
	wantRefusal("another root key", ask(tc.addrs[0], true, otherKey), "this node runs under")
	self := good
	self.NodeID = 1
	wantRefusal("the answering node's own id", ask(tc.addrs[0], true, self), "own id")
	again := good
	wantRefusal("asking twice at once", func() string { ask(tc.addrs[0], true, again); return ask(tc.addrs[0], true, again) }(), "too soon")

	// A rolling restart, with nobody at a keyboard: each node stops to
	// wait, asks its peers, is refused while its last heartbeat is fresh,
	// and is handed the key once it is not.
	for i := range dirs {
		tc.StopNode(i)
		stopped := time.Now()
		sealed := make(chan struct{}, 1)
		if _, err := restart(i, func(string) { sealed <- struct{}{} }); err != nil {
			t.Fatalf("rolling restart of n%d: %v", i+1, err)
		}
		select {
		case <-sealed:
		default:
			t.Fatalf("n%d restarted without ever being sealed", i+1)
		}
		// A peer judges "not live" by the last heartbeat it knows of, and
		// what it knows is as old as the node's last heartbeat (every 3s)
		// plus its own last read of the registry (every 3s). So the wait
		// seen from here, measured from the stop, can be up to two such
		// periods short of the grace; it cannot be nothing.
		if waited := time.Since(stopped); waited < grace-7*time.Second {
			t.Fatalf("n%d was handed the key %s after it stopped, while a node with its id could still have been running (grace %s)", i+1, waited, grace)
		}
		if kind, id := tc.Nodes[i].EncryptionRoot(); kind != "shares" || id != keyID {
			t.Fatalf("n%d after peer unseal: root %q %q", i+1, kind, id)
		}
	}

	// A node being decommissioned is not vouched for, stopped or not.
	// n1 learns of it at its next heartbeat.
	if resp := call(tc.addrs[0], false, cluster.AdminRequest{Op: "decommission", NodeID: 3}); resp.Error != "" {
		t.Fatalf("decommission n3: %s", resp.Error)
	}
	draining := good
	draining.NodeID = 3
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(700 * time.Millisecond) {
		got := ask(tc.addrs[0], true, draining)
		if strings.Contains(got, "being decommissioned") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a node being decommissioned: %q", got)
		}
	}
	if resp := call(tc.addrs[0], false, cluster.AdminRequest{Op: "decommission", NodeID: 3, Cancel: true}); resp.Error != "" {
		t.Fatalf("cancelling the decommission: %s", resp.Error)
	}

	// With the setting off, a restarted node waits for people however
	// long it has been down.
	setting := func(on bool) {
		t.Helper()
		if resp := call(tc.addrs[0], false, cluster.AdminRequest{Op: "peer-unseal-setting", Enable: &on}); resp.Error != "" {
			t.Fatalf("setting peer unseal: %s", resp.Error)
		}
		want := map[bool]string{true: "on", false: "off"}[on]
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
			n := 0
			for i, addr := range tc.addrs {
				if tc.Nodes[i] == nil {
					n++
					continue
				}
				if st := call(addr, false, cluster.AdminRequest{Op: "seal-status"}); st.Seal != nil && st.Seal.PeerUnseal == want {
					n++
				}
			}
			if n == len(tc.addrs) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("peer unseal reads %q on %d of %d nodes", want, n, len(tc.addrs))
			}
		}
	}
	setting(false)
	tc.StopNode(1)
	started := make(chan error, 1)
	sealedAt := make(chan string, 1)
	go func() {
		_, err := restart(1, func(addr string) { sealedAt <- addr })
		started <- err
	}()
	addr := <-sealedAt
	select {
	case err := <-started:
		t.Fatalf("with peer unseal off, n2 started without shares: %v", err)
	case <-time.After(grace + 4*time.Second):
	}
	wantRefusal("asking with the setting off", ask(tc.addrs[0], true, good), "turned off")
	withShares(addr)
	if err := <-started; err != nil {
		t.Fatalf("n2 with shares: %v", err)
	}
	setting(true)

	// A cold start. Everything is down; people unseal n1 alone, and the
	// others take the key from it — before the cluster has a quorum to
	// read anything from, which is why each node keeps the setting
	// itself.
	tc.StopAll()
	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range dirs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			onSealed := func(string) {}
			if i == 0 {
				onSealed = withShares
			}
			_, errs[i] = restart(i, onSealed)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("cold start, n%d: %v", i+1, err)
		}
	}
	// What was done is on record: n1 handed the key over at least once,
	// and two hand-overs happened in all — the third node may have asked
	// the second, which by then held the key too.
	granted := int64(0)
	for i, n := range tc.Nodes {
		doc := n.SecurityStores()
		if len(doc) != 1 || doc[0].PeerUnseal == nil || !doc[0].PeerUnseal.Enabled {
			t.Fatalf("n%d's record of the cold start: %+v", i+1, doc)
		}
		if i == 0 && doc[0].PeerUnseal.Granted < 1 {
			t.Fatalf("n1, the only node unsealed by people, handed the key to nobody: %+v", *doc[0].PeerUnseal)
		}
		granted += doc[0].PeerUnseal.Granted
	}
	if granted < 2 {
		t.Fatalf("%d hand-overs recorded for two nodes that started without shares", granted)
	}
}
