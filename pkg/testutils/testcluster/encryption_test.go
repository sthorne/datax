package testcluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// TestEncryptedDiskNode is the end-to-end encryption tripwire: a real-dir
// node with a key must leave NO plaintext canary anywhere in its data
// directory (sstables, WAL, MANIFEST, and the sealed metadata backup),
// survive a restart with the key, and keep working after an offline
// store-key rotation.
func TestEncryptedDiskNode(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, enc.KeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "store.key")
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		t.Fatal(err)
	}
	withKey := func(cfg *server.Config) { cfg.EncKeyPath = keyPath }

	canary := []byte("plaintext-canary-7f3a")
	prefix := keys.TableDataPrefix(910)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	tc := &TestCluster{T: t}
	n := startDiskNode(t, dir, true, "", withKey)
	tc.Nodes = append(tc.Nodes, n)
	defer tc.StopAll()

	for i := 0; i < 20; i++ {
		if err := n.DB().Put(ctx, append(prefix.Clone(), fmt.Sprintf("k%02d", i)...), canary); err != nil {
			t.Fatal(err)
		}
	}

	// The metadata backup (written each heartbeat) must come out sealed.
	bakPath := filepath.Join(dir, server.MetadataBackupFile)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if raw, err := os.ReadFile(bakPath); err == nil {
			if !bytes.HasPrefix(raw, []byte(server.MetadataBackupMagic)) {
				t.Fatalf("metadata backup on an encrypted store is not sealed: %q...", raw[:16])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("metadata backup never written")
		}
		time.Sleep(250 * time.Millisecond)
	}

	tc.StopNode(0)

	// Tripwire: grep every file in the store directory for the canary.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(b, canary) {
			t.Errorf("canary found in %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	// Restart with the key: everything is still there.
	n = startDiskNode(t, dir, false, "", withKey)
	tc.Nodes[0] = n
	rows, err := n.DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 20 {
		t.Fatalf("after restart: %d rows, want 20", len(rows))
	}
	tc.StopNode(0)

	// Offline store-key rotation, then restart under the new key.
	newKey := make([]byte, enc.KeyLen)
	if _, err := rand.Read(newKey); err != nil {
		t.Fatal(err)
	}
	if err := enc.RotateStoreKey(vfs.Default, dir, key, newKey); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, newKey, 0o600); err != nil {
		t.Fatal(err)
	}
	n = startDiskNode(t, dir, false, "", withKey)
	tc.Nodes[0] = n
	rows, err = n.DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 20 {
		t.Fatalf("after rotation: %d rows, want 20", len(rows))
	}
}

// TestEncryptedCluster: a 3-node cluster over encrypted in-memory engines
// replicates normally — raft snapshots and appends are logical KV streams,
// so encryption at rest is invisible to replication.
func TestEncryptedCluster(t *testing.T) {
	key := make([]byte, enc.KeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	tc, _ := StartWithEngineOptions(t, 3, storage.Options{EncryptionKey: key})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}

	prefix := keys.TableDataPrefix(911)
	for i := 0; i < 10; i++ {
		if err := tc.Nodes[0].DB().Put(ctx, append(prefix.Clone(), fmt.Sprintf("k%02d", i)...), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tc.Nodes[0].DB().AdminSplit(ctx, append(prefix.Clone(), "k05"...)); err != nil {
		t.Fatal(err)
	}
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}
	// Every gateway serves the data.
	for i, n := range tc.Nodes {
		rows, err := n.DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0)
		if err != nil {
			t.Fatalf("node %d scan: %v", i+1, err)
		}
		if len(rows) != 10 {
			t.Fatalf("node %d: %d rows, want 10", i+1, len(rows))
		}
	}
}
