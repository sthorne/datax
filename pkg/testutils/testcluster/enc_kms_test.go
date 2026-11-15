package testcluster

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/storage/enc/kmstest"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestStoreKeyUnderAKMS (issue #308): a node whose store key is kept
// under a key management service — here a fake of Vault's Transit
// engine. It starts, and binds its wrapped key to itself once it knows
// who it is. With the service down it keeps serving, says so in
// /api/security, the health document and a metric, and recovers when
// the service does; stopped, it cannot start until the service is back,
// and says which service and why. After the service rotates its key a
// rewrap moves the node to the new version. A node that joins binds its
// key to its own id, and one node's wrapped key is refused as another's.
func TestStoreKeyUnderAKMS(t *testing.T) {
	const token = "hvs.test-token-for-the-fake-vault"
	vault := kmstest.NewVault(token)
	defer vault.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	spec := "vault:" + vault.URL + "/transit/datax?token_file=" + tokenFile + "&timeout=1s&retries=1"
	dir := t.TempDir()
	var httpAddr string
	opts := func(cfg *server.Config) {
		cfg.EncRoot = spec
		cfg.EncRootHealthInterval = 50 * time.Millisecond
		cfg.GCInterval = -1
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		cfg.HTTPListener = lis
		httpAddr = lis.Addr().String()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	n := startDiskNode(t, dir, true, "", opts)
	stopped := false
	defer func() {
		if !stopped {
			n.Stop()
		}
	}()
	root := func(n *server.Node) *server.SecurityRoot {
		t.Helper()
		st := n.SecurityStores()
		if len(st) != 1 || st[0].Root == nil {
			t.Fatalf("the node reports no root: %+v", st)
		}
		return st[0].Root
	}
	// waitRoot waits for the node's account of its root to satisfy ok.
	waitRoot := func(n *server.Node, what string, ok func(*server.SecurityRoot) bool) *server.SecurityRoot {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for {
			r := root(n)
			if ok(r) {
				return r
			}
			if time.Now().After(deadline) {
				t.Fatalf("waiting for the root to be %s: %+v", what, r)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	healthy := func(r *server.SecurityRoot) bool { return r.Healthy != nil && *r.Healthy }
	unhealthy := func(r *server.SecurityRoot) bool { return r.Healthy != nil && !*r.Healthy }
	entry := func(dir string) enc.WrappedEntry {
		t.Helper()
		e, err := enc.ReadWrappedKeys(vfs.Default, dir)
		if err != nil || len(e) != 1 {
			t.Fatalf("reading the wrapped key in %s: %v (%d entries)", dir, err, len(e))
		}
		return e[0]
	}
	prefix := keys.TableDataPrefix(958)
	put := func(n *server.Node, k byte) {
		t.Helper()
		if err := n.DB().Put(ctx, append(prefix.Clone(), k), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	rows := func(n *server.Node, want int, when string) {
		t.Helper()
		got, err := n.DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0)
		if err != nil || len(got) != want {
			t.Fatalf("%s: %d rows, %v; want %d", when, len(got), err, want)
		}
	}

	// Started: under the service's key at its first version, bound to
	// this node, healthy.
	r := root(n)
	if r.Kind != "vault" || r.KeyID != "transit/datax@v1" || !r.Bound || !healthy(r) {
		t.Fatalf("the node's root after its first start: %+v", r)
	}
	self := enc.Binding{ClusterID: n.ClusterID().String(), NodeID: uint64(n.NodeID())}
	if e := entry(dir); e.Root != "vault" || e.KeyID != "transit/datax@v1" || e.Binding == nil || *e.Binding != self {
		t.Fatalf("the wrapped-key file after the first start: root %s, key id %s, binding %v; want bound to %s", e.Root, e.KeyID, e.Binding, self)
	}
	put(n, '1')

	// The service goes away under a running node.
	vault.Down(true)
	r = waitRoot(n, "unhealthy", unhealthy)
	if !strings.Contains(r.HealthError, "Vault Transit key transit/datax") || !strings.Contains(r.HealthError, "could not reach") || strings.Contains(r.HealthError, token) {
		t.Fatalf("the reason given for an unreachable root: %q", r.HealthError)
	}
	put(n, '2')
	rows(n, 2, "with the KMS down")
	_, _, metrics := httpGet(t, "http://"+httpAddr+"/metrics")
	if !strings.Contains(metrics, "\ndatax_enc_root_healthy 0\n") {
		t.Fatal("/metrics does not report datax_enc_root_healthy 0 with the KMS down")
	}
	_, _, sec := httpGet(t, "http://"+httpAddr+"/api/security")
	if !strings.Contains(sec, `"healthy": false`) && !strings.Contains(sec, `"healthy":false`) {
		t.Fatalf("/api/security does not say the root is unhealthy: %s", sec)
	}
	if strings.Contains(sec, token) || strings.Contains(metrics, token) {
		t.Fatal("the Vault token is in a document the node serves")
	}
	_, _, health := httpGet(t, "http://"+httpAddr+"/api/health")
	if !strings.Contains(health, "enc-root-unreachable") {
		t.Fatalf("the health document has no problem for an unreachable root: %s", health)
	}

	// And comes back.
	vault.Down(false)
	waitRoot(n, "healthy again", healthy)
	_, _, metrics = httpGet(t, "http://"+httpAddr+"/metrics")
	if !strings.Contains(metrics, "\ndatax_enc_root_healthy 1\n") {
		t.Fatal("/metrics does not report datax_enc_root_healthy 1 after the KMS returned")
	}
	var sawDown, sawUp bool
	for _, ev := range n.Events().Recent(0, 1000, true) {
		if ev.Kind == "enc-root" {
			sawDown = sawDown || strings.Contains(ev.Summary, "not usable")
			sawUp = sawUp || strings.Contains(ev.Summary, "usable again")
		}
	}
	if !sawDown || !sawUp {
		t.Fatalf("the event timeline does not record the root going (%t) and returning (%t)", sawDown, sawUp)
	}

	// Stopped, the node cannot start without its KMS, and says why.
	n.Stop()
	stopped = true
	vault.Down(true)
	began := time.Now()
	if _, err := tryStartDiskNode(t, dir, false, "", opts); err == nil ||
		!strings.Contains(err.Error(), "unwrapping the store key") || !strings.Contains(err.Error(), "Vault Transit key transit/datax") ||
		!strings.Contains(err.Error(), "could not reach") || !strings.Contains(err.Error(), "after 2 attempts") || strings.Contains(err.Error(), token) {
		t.Fatalf("a start with the KMS down: %v", err)
	}
	if d := time.Since(began); d > 15*time.Second {
		t.Fatalf("a start with the KMS down took %s to give up", d)
	}
	// Refusing is as clear as being away.
	vault.Down(false)
	vault.Fail(403, -1)
	if _, err := tryStartDiskNode(t, dir, false, "", opts); err == nil || !strings.Contains(err.Error(), "HTTP 403") || !strings.Contains(err.Error(), "after 1 attempt") {
		t.Fatalf("a start with the KMS refusing: %v", err)
	}
	vault.Fail(0, 0)
	n = startDiskNode(t, dir, false, "", opts)
	stopped = false
	rows(n, 2, "after a restart through the KMS")
	if r := root(n); r.KeyID != "transit/datax@v1" || !r.Bound || !healthy(r) {
		t.Fatalf("the node's root after a restart: %+v", r)
	}

	// The service rotates its key. The node notices, and a rewrap moves
	// its store key to the new version.
	vault.Rotate()
	r = waitRoot(n, "aware of the new key version", func(r *server.SecurityRoot) bool { return r.LatestKeyID != "" })
	if r.KeyID != "transit/datax@v1" || r.LatestKeyID != "transit/datax@v2" {
		t.Fatalf("after the service rotated its key: %+v", r)
	}
	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	defer trans.Close()
	var resp cluster.AdminResponse
	cctx, ccancel := context.WithTimeout(ctx, 20*time.Second)
	err := trans.Call(cctx, n.Addr(), "admin", cluster.AdminRequest{Op: "rewrap-store-key"}, &resp)
	ccancel()
	if err != nil || resp.Error != "" {
		t.Fatalf("rewrap: %v %s", err, resp.Error)
	}
	if r := root(n); r.KeyID != "transit/datax@v2" || r.LatestKeyID != "" || !r.Bound {
		t.Fatalf("the node's root after the rewrap: %+v", r)
	}
	if e := entry(dir); e.KeyID != "transit/datax@v2" || e.Binding == nil || *e.Binding != self {
		t.Fatalf("the wrapped-key file after the rewrap: key id %s, binding %v", e.KeyID, e.Binding)
	}
	raw, _ := json.Marshal(n.Events().Recent(0, 1000, true))
	if strings.Contains(string(raw), token) {
		t.Fatal("the Vault token is in the event timeline")
	}

	// A second node joins: its key is wrapped before it has an id and
	// bound to it once it does.
	dir2 := t.TempDir()
	n2 := startDiskNode(t, dir2, false, n.Addr(), opts)
	other := enc.Binding{ClusterID: n2.ClusterID().String(), NodeID: uint64(n2.NodeID())}
	if other == self || other.ClusterID != self.ClusterID {
		t.Fatalf("the joined node is %s; the first is %s", other, self)
	}
	if e := entry(dir2); e.Binding == nil || *e.Binding != other {
		t.Fatalf("the joined node's wrapped key is bound to %v, want %s", e.Binding, other)
	}
	if !root(n2).Bound {
		t.Fatal("the joined node does not report its key as bound")
	}
	// Both stop before the files are looked at from outside.
	n2.Stop()
	n.Stop()
	stopped = true
	// One node's wrapped key is not another's: refused by the entry's
	// binding for a caller who says who it is, and by the service for
	// one who presents it under the other node's context.
	roots, err := enc.ParseRoots(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.UnwrapStoreKeys(enc.WithBinding(ctx, other), vfs.Default, dir, roots); err == nil || !strings.Contains(err.Error(), "another node's") {
		t.Fatalf("node 1's wrapped key opened as node 2: %v", err)
	}
	e := entry(dir)
	if _, err := roots[0].(enc.ContextRoot).UnwrapContext(ctx, e.Wrapped, other); err == nil {
		t.Fatal("the service unwrapped node 1's key under node 2's context")
	}
	if _, err := roots[0].(enc.ContextRoot).UnwrapContext(ctx, e.Wrapped, self); err != nil {
		t.Fatalf("the service refused node 1's key under its own context: %v", err)
	}
	// A wrapped-key file that claims an identity its store does not have
	// — made here the only way it can be, with the store key in hand —
	// is refused at the start.
	got, err := enc.UnwrapStoreKeys(ctx, vfs.Default, dir, roots)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.RewrapStoreKey(enc.WithBinding(ctx, other), vfs.Default, dir, roots[0], got[0].Key); err != nil {
		t.Fatal(err)
	}
	if _, err := tryStartDiskNode(t, dir, false, "", opts); err == nil || !strings.Contains(err.Error(), "is bound to") || !strings.Contains(err.Error(), "not this store's own") {
		t.Fatalf("a start with a wrapped key bound to another node: %v", err)
	}
}
