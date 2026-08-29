package testcluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestUnsafeRecoverQuorumLoss: a 3-node disk cluster loses two nodes
// permanently — including range 1's quorum, which normally bricks all
// cluster metadata. UnsafeRecover on the survivor rewrites its descriptors
// to solo membership; the node restarts, serves the surviving data, and
// fresh nodes joining restore RF=3 via upreplication. Regression test for
// issue #10.
func TestUnsafeRecoverQuorumLoss(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	tc := &TestCluster{T: t}
	n1 := startDiskNode(t, dirs[0], true, "")
	tc.Nodes = append(tc.Nodes, n1)
	tc.Nodes = append(tc.Nodes, startDiskNode(t, dirs[1], false, n1.Addr()))
	tc.Nodes = append(tc.Nodes, startDiskNode(t, dirs[2], false, n1.Addr()))
	defer tc.StopAll()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}

	// Multi-range data, so recovery must fix more than range 1.
	prefix := keys.TableDataPrefix(790)
	for i := 0; i < 10; i++ {
		if err := n1.DB().Put(ctx, append(prefix.Clone(), fmt.Sprintf("k%02d", i)...), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := n1.DB().AdminSplit(ctx, append(prefix.Clone(), "k05"...)); err != nil {
		t.Fatal(err)
	}
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}

	// Catastrophe: nodes 2 and 3 die and their disks are gone. Range 1 (and
	// every other range) has lost quorum.
	for i := range tc.Nodes {
		tc.StopNode(i)
	}
	if err := os.RemoveAll(dirs[1]); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dirs[2]); err != nil {
		t.Fatal(err)
	}

	// Recover every range on the survivor to solo membership.
	recovered, err := server.UnsafeRecover(dirs[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) < 2 {
		t.Fatalf("recovered only %d ranges", len(recovered))
	}
	for _, d := range recovered {
		if len(d.Replicas) != 1 {
			t.Fatalf("recovered descriptor not solo: %+v", d)
		}
	}

	// The survivor restarts alone and serves everything it had.
	n1b := startDiskNode(t, dirs[0], false, "")
	tc.Nodes = append(tc.Nodes, n1b)
	for i := 0; i < 10; i++ {
		k := append(prefix.Clone(), fmt.Sprintf("k%02d", i)...)
		v, err := n1b.DB().Get(ctx, k)
		if err != nil || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("post-recovery read %s: %q, %v", k, v, err)
		}
	}
	if err := n1b.DB().Put(ctx, append(prefix.Clone(), "post"...), []byte("recovered")); err != nil {
		t.Fatalf("post-recovery write: %v", err)
	}

	// Fresh nodes join and upreplication restores RF=3 everywhere.
	tc.Nodes = append(tc.Nodes, startDiskNode(t, t.TempDir(), false, n1b.Addr()))
	tc.Nodes = append(tc.Nodes, startDiskNode(t, t.TempDir(), false, n1b.Addr()))
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatalf("replication never recovered: %v", err)
	}
	if v, err := tc.Nodes[len(tc.Nodes)-1].DB().Get(ctx, append(prefix.Clone(), "post"...)); err != nil || string(v) != "recovered" {
		t.Fatalf("read via rejoined node: %q, %v", v, err)
	}
}

// TestMetadataBackupExport: disk nodes periodically export cluster metadata
// to their data directory — the recovery artifact for quorum loss.
func TestMetadataBackupExport(t *testing.T) {
	dir := t.TempDir()
	n := startDiskNode(t, dir, true, "")
	defer n.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE backup_me (id INT PRIMARY KEY, v TEXT)`)

	path := filepath.Join(dir, server.MetadataBackupFile)
	deadline := time.Now().Add(30 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			var bak server.MetadataBackup
			if jsonUnmarshal(raw, &bak) == nil && len(bak.Ranges) >= 1 && len(bak.Tables) >= 1 && len(bak.Nodes) >= 1 {
				if _, ok := bak.Namespace["backup_me"]; ok {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("metadata backup never became complete at %s", path)
		}
		time.Sleep(300 * time.Millisecond)
	}
}
