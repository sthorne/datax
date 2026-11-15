package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// The offline tools for a store's root (issue #305), end to end on a
// real store directory: a key-file store is adopted by a root, rotated
// with no key file in sight, moved to a second root, and released back
// to a key file — and at each step it opens with what the step says it
// should and with nothing else, its data intact.
func TestDebugEncRootOffline(t *testing.T) {
	keyDir, dir := t.TempDir(), t.TempDir()
	write := func(name string, fill byte) string {
		p := filepath.Join(keyDir, name)
		if err := os.WriteFile(p, bytes.Repeat([]byte{fill}, enc.KeyLen), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	storeKeyPath, aPath, bPath := write("store.key", 1), write("a.key", 2), write("b.key", 3)
	rootA, rootB := "file:"+aPath, "file:"+bPath
	storeKey, _ := enc.LoadKeyFile(storeKeyPath)

	eng, err := storage.Open(dir, storage.Options{EncryptionKey: storeKey})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	// opens reports whether the store opens through spec, data intact.
	opens := func(spec string) error {
		keys, err := storeKeyCandidates(dir, "", spec)
		if err != nil {
			return err
		}
		idx, err := enc.MatchStoreKey(vfs.Default, dir, keys)
		if err != nil {
			return err
		}
		e, err := storage.Open(dir, storage.Options{EncryptionKey: keys[idx]})
		if err != nil {
			return err
		}
		defer func() { _ = e.Close() }()
		if v, err := e.Get([]byte("k")); err != nil || string(v) != "v" {
			t.Fatalf("the store opened through %s but its data did not survive: %q, %v", spec, v, err)
		}
		return nil
	}
	run := func(args ...string) error { return runDebugEncRoot(args) }

	if err := run("adopt", "--dir", dir, "--enc-key", storeKeyPath, "--enc-root", rootA); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if err := opens(rootA); err != nil {
		t.Fatalf("after adopt: %v", err)
	}
	if err := run("adopt", "--dir", dir, "--enc-key", storeKeyPath, "--enc-root", rootA); err == nil {
		t.Fatal("a second adopt was accepted")
	}
	// Until the key is rotated the old key file still opens the store;
	// the rotation is what retires it. No key file is named.
	if err := runDebugRotateEncKey([]string{"--dir", dir, "--enc-root", rootA}); err != nil {
		t.Fatalf("rooted offline rotation: %v", err)
	}
	if _, err := enc.MatchStoreKey(vfs.Default, dir, [][]byte{storeKey}); err == nil {
		t.Fatal("the key that was in the file still opens the store after a rotation")
	}
	if err := opens(rootA); err != nil {
		t.Fatalf("after rotation: %v", err)
	}
	if err := runDebugRotateEncKey([]string{"--dir", dir, "--enc-root", rootA, "--old-key", storeKeyPath, "--new-key", aPath}); err == nil {
		t.Fatal("a rooted rotation accepted key files")
	}
	// Move to the second root.
	if err := run("rewrap", "--dir", dir, "--enc-root", rootA+","+rootB, "--to-root", "2"); err != nil {
		t.Fatalf("rewrap to the second root: %v", err)
	}
	if err := opens(rootA); err == nil {
		t.Fatal("the root the store moved from still opens it")
	}
	if err := opens(rootB); err != nil {
		t.Fatalf("after moving roots: %v", err)
	}
	if err := run("rewrap", "--dir", dir, "--enc-root", rootB, "--to-root", "2"); err == nil || !strings.Contains(err.Error(), "no root 2") {
		t.Fatalf("rewrap to a root that was not given: %v", err)
	}
	if err := run("status", "--dir", dir); err != nil {
		t.Fatalf("status: %v", err)
	}
	// And back to a key file, which is written once and never over
	// another file.
	out := filepath.Join(keyDir, "released.key")
	if err := run("release", "--dir", dir, "--enc-root", rootB, "--out", aPath); err == nil {
		t.Fatal("release overwrote an existing file")
	}
	if !enc.WrappedKeyExists(vfs.Default, dir) {
		t.Fatal("a refused release removed the wrapped key")
	}
	if err := run("release", "--dir", dir, "--enc-root", rootB, "--out", out); err != nil {
		t.Fatalf("release: %v", err)
	}
	if enc.WrappedKeyExists(vfs.Default, dir) {
		t.Fatal("release left the wrapped key behind")
	}
	released, err := enc.LoadKeyFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(out); st.Mode().Perm() != 0o600 {
		t.Fatalf("the released key file has mode %v", st.Mode().Perm())
	}
	e, err := storage.Open(dir, storage.Options{EncryptionKey: released})
	if err != nil {
		t.Fatalf("the released key does not open the store: %v", err)
	}
	_ = e.Close()
	if _, err := enc.UnwrapStoreKeys(context.Background(), vfs.Default, dir, nil); err == nil {
		t.Fatal("unwrapping with no root was accepted")
	}
}

// The offline tools cannot open a store to learn whose it is, so they
// keep the binding its wrapped key has (issue #308): a key bound to its
// node is still bound to it after an offline rotation and an offline
// move to another root.
func TestDebugEncRootOfflineKeepsTheBinding(t *testing.T) {
	keyDir, dir := t.TempDir(), t.TempDir()
	spec := func(name string, fill byte) string {
		p := filepath.Join(keyDir, name)
		if err := os.WriteFile(p, bytes.Repeat([]byte{fill}, enc.KeyLen), 0o600); err != nil {
			t.Fatal(err)
		}
		return "file:" + p
	}
	rootA, rootB := spec("a.key", 2), spec("b.key", 3)
	node := enc.Binding{ClusterID: "6f1c2a0e-8a53-4c60-9d0c-0c8f8f1d3b11", NodeID: 4}
	roots, err := enc.ParseRoots(rootA)
	if err != nil {
		t.Fatal(err)
	}
	ctx := enc.WithBinding(context.Background(), node)
	key, err := enc.InitWrappedStoreKey(ctx, vfs.Default, dir, roots[0])
	if err != nil {
		t.Fatal(err)
	}
	eng, err := storage.Open(dir, storage.Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	bound := func(when string) {
		t.Helper()
		entries, err := enc.ReadWrappedKeys(vfs.Default, dir)
		if err != nil || len(entries) != 1 || entries[0].Binding == nil || *entries[0].Binding != node {
			t.Fatalf("%s: the wrapped key is not bound to %s: %v %+v", when, node, err, entries)
		}
	}
	bound("as made")
	if err := runDebugRotateEncKey([]string{"--dir", dir, "--enc-root", rootA}); err != nil {
		t.Fatalf("offline rotation: %v", err)
	}
	bound("after an offline rotation")
	if err := runDebugEncRoot([]string{"rewrap", "--dir", dir, "--enc-root", rootA + "," + rootB, "--to-root", "2"}); err != nil {
		t.Fatalf("offline move to another root: %v", err)
	}
	bound("after an offline move to another root")
	// And the key is refused to a caller that says it is another node.
	other := node
	other.NodeID = 5
	rootsB, err := enc.ParseRoots(rootB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.UnwrapStoreKeys(enc.WithBinding(context.Background(), other), vfs.Default, dir, rootsB); err == nil || !strings.Contains(err.Error(), "another node's") {
		t.Fatalf("a bound key opened as another node: %v", err)
	}
}
