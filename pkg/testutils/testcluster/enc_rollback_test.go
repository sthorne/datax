package testcluster

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/version"
)

// encKeyFile writes a fresh store key and returns the config option
// that uses it.
func encKeyFile(t *testing.T) func(*server.Config) {
	t.Helper()
	key := make([]byte, enc.KeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "store.key")
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		t.Fatal(err)
	}
	return func(cfg *server.Config) { cfg.EncKeyPath = keyPath }
}

// registryPaths are a split store's two key registries.
func registryPaths(dir string) []string {
	return []string{filepath.Join(dir, enc.RegistryName), filepath.Join(dir, enc.RaftSubdir, enc.RegistryName)}
}

// upgradeClusterTo drives the upgrade admin op until the cluster has
// finalized v (every node has to be heard from first).
func upgradeClusterTo(t *testing.T, ctx context.Context, addr string, v version.Version) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp := adminCall(t, ctx, addr, cluster.AdminRequest{Op: "upgrade-cluster", Version: int(v)})
		if resp.Error == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("upgrading the cluster to %s: %s", v, resp.Error)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitAuthenticatedWrites(t *testing.T, n *server.Node, what string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if au := n.AuthenticatedEncryptionStatus(); au != nil && au.Writes {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the store never switched to authenticated writes: %+v", what, n.AuthenticatedEncryptionStatus())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestEncryptionRegistryRollbackIsRefused (issue #248): the key registry
// alone restored from before the switch to authenticated encryption —
// an older, validly sealed copy beside a store that made the switch —
// is caught at open by the store's own record of the switch, and the
// node refuses to start. --accept-encryption-rollback starts it once,
// lowering the record; the store then re-completes the switch, and a
// plain start works again.
func TestEncryptionRegistryRollbackIsRefused(t *testing.T) {
	dir := t.TempDir()
	withKey := encKeyFile(t)
	before := func(cfg *server.Config) { cfg.BinaryVersionOverride = version.V17 }
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	prefix := keys.TableDataPrefix(911)

	// Born before v18: encrypted, unauthenticated.
	tc := &TestCluster{T: t}
	n := startDiskNode(t, dir, true, "", withKey, before)
	tc.Nodes = append(tc.Nodes, n)
	defer tc.StopAll()
	for i := 0; i < 5; i++ {
		if err := n.DB().Put(ctx, append(prefix.Clone(), fmt.Sprintf("k%02d", i)...), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if au := n.AuthenticatedEncryptionStatus(); au == nil || au.Writes {
		t.Fatalf("before v18: authenticated status %+v, want unauthenticated", au)
	}
	tc.StopNode(0)

	// The upgrade: the store switches, and records that it did. The
	// registries are copied after this open and before the switch, so
	// the copies hold the data keys the store's files are sealed under
	// (an open can add one) and lack only the switch — the backup an
	// operator would reach for.
	n = startDiskNode(t, dir, false, "", withKey)
	tc.Nodes[0] = n
	old := map[string][]byte{}
	for _, p := range registryPaths(dir) {
		if b, err := os.ReadFile(p); err == nil {
			old[p] = b
		}
	}
	if len(old) == 0 {
		t.Fatal("no key registry to snapshot")
	}
	if au := n.AuthenticatedEncryptionStatus(); au == nil || au.Writes {
		t.Fatalf("before the upgrade: authenticated status %+v, want unauthenticated", au)
	}
	upgradeClusterTo(t, ctx, n.Addr(), version.Current)
	waitAuthenticatedWrites(t, n, "after the upgrade")
	tc.StopNode(0)

	// The registries from before the switch, put back.
	for p, b := range old {
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := tryStartDiskNode(t, dir, false, "", withKey); err == nil {
		n.Stop()
		t.Fatal("a store whose key registry was rolled back to before the switch started")
	} else if !strings.Contains(err.Error(), "key registry was rolled back") || !strings.Contains(err.Error(), "accept-encryption-rollback") {
		t.Fatalf("refused for another reason: %v", err)
	}

	// Accepted once: the record is lowered, the store re-completes the
	// switch as the cluster version asks, and the data is intact.
	n = startDiskNode(t, dir, false, "", withKey, func(cfg *server.Config) { cfg.AcceptEncryptionRollback = true })
	tc.Nodes[0] = n
	waitAuthenticatedWrites(t, n, "after accepting the rollback")
	if rows, err := n.DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0); err != nil || len(rows) != 5 {
		t.Fatalf("after accepting the rollback: %d rows, %v; want 5", len(rows), err)
	}
	tc.StopNode(0)
	// And a plain start again, the records and the registry agreeing.
	n = startDiskNode(t, dir, false, "", withKey)
	tc.Nodes[0] = n
	waitAuthenticatedWrites(t, n, "after the accepted start")
}

// TestEncryptionStoreRollbackStopsTheNode (issue #248): a whole store
// restored from before the switch — the registry and the store's own
// record with it — opens, and is caught once the node reads the
// cluster's record of its store: the record, replicated, says the
// store had switched, so the node stops serving. Accepted once, the
// record is lowered and the node stays up.
func TestEncryptionStoreRollbackStopsTheNode(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	withKey := encKeyFile(t)
	before := func(cfg *server.Config) { cfg.BinaryVersionOverride = version.V17 }
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	tc := &TestCluster{T: t}
	n1 := startDiskNode(t, dirs[0], true, "", withKey, before)
	tc.Nodes = append(tc.Nodes, n1)
	tc.Nodes = append(tc.Nodes, startDiskNode(t, dirs[1], false, n1.Addr(), withKey, before))
	tc.Nodes = append(tc.Nodes, startDiskNode(t, dirs[2], false, n1.Addr(), withKey, before))
	defer tc.StopAll()
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}
	storeID := base.StoreID(tc.Nodes[2].NodeID())
	tc.StopAll()
	snapshot := t.TempDir()
	copyTree(t, dirs[2], snapshot)

	// The upgrade on every node: each store switches and records it in
	// the cluster.
	for i := range dirs {
		tc.Nodes[i] = startDiskNode(t, dirs[i], false, "", withKey)
	}
	upgradeClusterTo(t, ctx, tc.Nodes[0].Addr(), version.Current)
	for i, n := range tc.Nodes {
		waitAuthenticatedWrites(t, n, fmt.Sprintf("node %d after the upgrade", i+1))
	}
	recKey := keys.StoreEncryptionCutoverKey(storeID)
	deadline := time.Now().Add(60 * time.Second)
	for {
		raw, err := tc.Nodes[0].DB().Get(ctx, recKey)
		if err == nil && raw != nil {
			var rec struct {
				Writes bool `json:"writes"`
			}
			if err := json.Unmarshal(raw, &rec); err != nil {
				t.Fatalf("cutover record %q: %v", raw, err)
			}
			if rec.Writes {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cluster never recorded store %s's switch: %q, %v", storeID, raw, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The third store rolled back wholesale. The other two restart
	// before it comes back: a leader that still holds the progress it
	// had for the store — entries it acknowledged and now lacks — would
	// send a commit index past its log, which raft refuses with a panic
	// of its own (the pre-existing refusal docs/encryption.md notes);
	// fresh leaders probe the store's log and catch it up, so what
	// stops the node is the cross-check, which is what this test is
	// about.
	tc.StopAll()
	if err := os.RemoveAll(dirs[2]); err != nil {
		t.Fatal(err)
	}
	copyTree(t, snapshot, dirs[2])
	for i := 0; i < 2; i++ {
		tc.Nodes[i] = startDiskNode(t, dirs[i], false, "", withKey)
	}
	n3, err := tryStartDiskNode(t, dirs[2], false, "", withKey)
	if err != nil {
		t.Fatalf("a store rolled back wholesale, its own record with it, did not open: %v", err)
	}
	tc.Nodes[2] = n3
	select {
	case <-n3.Stopper().ShouldQuiesce():
	case <-time.After(90 * time.Second):
		t.Fatal("the node kept serving a store the cluster's record says was rolled back")
	}
	found := false
	for _, ev := range n3.Events().Recent(0, 50, true) {
		if ev.Kind == "security" && strings.Contains(ev.Summary, "encryption rollback refused") {
			found = true
		}
	}
	if !found {
		t.Error("the refusal was not recorded as a security event")
	}
	tc.StopNode(2)

	// Accepted once: the node stays up and re-completes the switch.
	n3 = startDiskNode(t, dirs[2], false, "", withKey, func(cfg *server.Config) { cfg.AcceptEncryptionRollback = true })
	tc.Nodes[2] = n3
	waitAuthenticatedWrites(t, n3, "node 3 after accepting the rollback")
	select {
	case <-n3.Stopper().ShouldQuiesce():
		t.Fatal("the node stopped though the rollback was accepted")
	case <-time.After(10 * time.Second):
	}
}
