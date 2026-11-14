package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// typeShares makes the given lines what the share prompt reads next.
func typeShares(t *testing.T, lines ...string) {
	t.Helper()
	prevIn, prevReader := shareInput, shareReader
	shareInput, shareReader = strings.NewReader(strings.Join(lines, "\n")+"\n"), nil
	t.Cleanup(func() { shareInput, shareReader = prevIn, prevReader })
}

func readShareFiles(t *testing.T, dir string, total int) []string {
	t.Helper()
	var out []string
	for i := 1; i <= total; i++ {
		matches, err := filepath.Glob(filepath.Join(dir, "share-"+string(rune('0'+i))+"-of-*.txt"))
		if err != nil || len(matches) != 1 {
			t.Fatalf("share %d: %v %v", i, matches, err)
		}
		info, err := os.Stat(matches[0])
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("share file %s: %v, mode %v", matches[0], err, info.Mode())
		}
		raw, err := os.ReadFile(matches[0])
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, strings.TrimSpace(string(raw)))
	}
	return out
}

// TestShareRootAtTheCommandLine (issue #306): the offline half. A root
// key is split into files and kept nowhere; a store is moved under it by
// people typing shares; the offline tools open that store the same way,
// through refusals; a new split of the same key opens it too; and
// nothing that was typed reaches the data directory.
func TestShareRootAtTheCommandLine(t *testing.T) {
	work, dir := t.TempDir(), t.TempDir()
	storeKeyPath := filepath.Join(work, "store.key")
	if err := os.WriteFile(storeKeyPath, bytes.Repeat([]byte{7}, enc.KeyLen), 0o600); err != nil {
		t.Fatal(err)
	}
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

	for _, bad := range []string{"", "3", "1/3", "4/3", "2/300", "two/three"} {
		if err := runDebugEncRoot([]string{"split", "--shares", bad, "--out-dir", filepath.Join(work, "bad")}); err == nil {
			t.Errorf("split --shares %q was accepted", bad)
		}
	}
	sharesDir := filepath.Join(work, "shares")
	if err := runDebugEncRoot([]string{"split", "--shares", "2/3", "--out-dir", sharesDir}); err != nil {
		t.Fatal(err)
	}
	shares := readShareFiles(t, sharesDir, 3)
	// The files are never written over: a second split into the same
	// directory would destroy the only copies of the first.
	if err := runDebugEncRoot([]string{"split", "--shares", "2/3", "--out-dir", sharesDir}); err == nil {
		t.Fatal("a second split wrote over the first's share files")
	}
	otherDir := filepath.Join(work, "other")
	if err := runDebugEncRoot([]string{"split", "--shares", "2/2", "--out-dir", otherDir}); err != nil {
		t.Fatal(err)
	}
	other := readShareFiles(t, otherDir, 2)

	// The store comes under the root key: two people type their shares.
	typeShares(t, shares[0], shares[2])
	if err := runDebugEncRoot([]string{"adopt", "--dir", dir, "--enc-key", storeKeyPath, "--enc-root", "shares"}); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	entries, err := enc.ReadWrappedKeys(vfs.Default, dir)
	if err != nil || len(entries) != 1 || entries[0].Root != "shares" || entries[0].KeyID == "" {
		t.Fatalf("wrapped key after adopt: %+v, %v", entries, err)
	}

	opens := func(lines ...string) error {
		typeShares(t, lines...)
		keys, err := storeKeyCandidates(dir, "", "shares")
		if err != nil {
			return err
		}
		if len(keys) != 1 || !bytes.Equal(keys[0], storeKey) {
			t.Fatalf("the shares unwrapped something other than the store key")
		}
		return nil
	}
	// Every pair opens it; refused lines in between are passed over: a
	// mistyped share, the same share twice, a share of another key.
	for _, pair := range [][2]int{{0, 1}, {0, 2}, {1, 2}} {
		if err := opens(shares[pair[1]], "datax-share-nonsense", shares[pair[1]], other[0], shares[pair[0]]); err != nil {
			t.Fatalf("shares %v: %v", pair, err)
		}
	}
	// One share and then nothing more to read is not enough, and neither
	// is a quorum of another key's shares.
	if err := opens(shares[0]); err == nil || !strings.Contains(err.Error(), "no share on standard input") {
		t.Fatalf("one share: %v", err)
	}
	if err := opens(other[0], other[1]); err == nil {
		t.Fatal("another key's shares opened the store")
	}

	// A new split of the same key: three of five now, from a quorum of
	// the old shares, and the new shares open the store.
	resplitDir := filepath.Join(work, "resplit")
	typeShares(t, shares[1], shares[2])
	if err := runDebugEncRoot([]string{"resplit", "--shares", "3/5", "--out-dir", resplitDir}); err != nil {
		t.Fatalf("resplit: %v", err)
	}
	again := readShareFiles(t, resplitDir, 5)
	if err := opens(again[4], again[0], again[2]); err != nil {
		t.Fatalf("the new split: %v", err)
	}
	if err := opens(again[4], again[0]); err == nil {
		t.Fatal("two shares of a three-of-five split opened the store")
	}
	// The two splits do not mix.
	if err := opens(again[4], shares[0], again[0]); err == nil {
		t.Fatal("shares of two splits were combined")
	}

	// Nothing typed is in the data directory.
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		raw, _ := os.ReadFile(path)
		for _, s := range append(append(append([]string{}, shares...), again...), other...) {
			if bytes.Contains(raw, []byte(strings.TrimPrefix(s, enc.SharePrefix))) {
				t.Fatalf("%s contains a share", path)
			}
		}
		if bytes.Contains(raw, storeKey) {
			t.Fatalf("%s contains the store key", path)
		}
		return nil
	})
}

// TestUnsealCommand: `datax unseal` and `datax debug seal-status`
// against a node that is waiting, on a cluster without TLS — where a
// share is accepted because it comes from the node's own machine.
func TestUnsealCommand(t *testing.T) {
	work := t.TempDir()
	if err := runDebugEncRoot([]string{"split", "--shares", "2/3", "--out-dir", work}); err != nil {
		t.Fatal(err)
	}
	shares := readShareFiles(t, work, 3)
	sealedAt := make(chan string, 1)
	started := make(chan error, 1)
	var node *server.Node
	go func() {
		n, err := server.Start(server.Config{
			Listen: "127.0.0.1:0", Dir: t.TempDir(), EncRoot: "shares", BootstrapSelf: true,
			GCInterval: -1, MetricsRecordInterval: -1,
			OnSealed: func(addr string) { sealedAt <- addr },
		})
		node = n
		started <- err
	}()
	var addr string
	select {
	case addr = <-sealedAt:
	case err := <-started:
		t.Fatalf("the node did not wait to be unsealed: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the node never reported it was sealed")
	}
	if err := runDebugSealStatus([]string{"--addr", addr}); err != nil {
		t.Fatalf("seal-status of a sealed node: %v", err)
	}
	if err := runUnseal([]string{"--addr", addr, shares[0]}); err == nil || !strings.Contains(err.Error(), "never given on the command line") {
		t.Fatalf("a share as an argument: %v", err)
	}
	typeShares(t, "not-a-share")
	if err := runUnseal([]string{"--addr", addr}); err == nil || !strings.Contains(err.Error(), "not a share") {
		t.Fatalf("nonsense: %v", err)
	}
	typeShares(t, shares[2])
	if err := runUnseal([]string{"--addr", addr}); err != nil {
		t.Fatalf("first share: %v", err)
	}
	select {
	case err := <-started:
		t.Fatalf("the node started on one share: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	typeShares(t, shares[0])
	if err := runUnseal([]string{"--addr", addr}); err != nil {
		t.Fatalf("second share: %v", err)
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("start after unsealing: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the node did not start after a quorum of shares")
	}
	defer node.Stop()
	if node.Addr() != addr {
		t.Fatalf("the node serves on %s after being sealed on %s", node.Addr(), addr)
	}
	if err := runDebugSealStatus([]string{"--addr", addr}); err != nil {
		t.Fatalf("seal-status of a running node: %v", err)
	}
}
