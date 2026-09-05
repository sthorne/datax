package enc

import (
	"crypto/rand"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func randKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, KeyLen)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

// TestMatchStoreKey: among several candidate store keys the one sealing
// the registry is chosen, in whichever position; none matching is an
// error naming the count; an absent registry picks the first (the store
// is about to be initialized). Issue #67.
func TestMatchStoreKey(t *testing.T) {
	fs := vfs.NewMem()
	k1, k2, k3 := randKey(t), randKey(t), randKey(t)
	if idx, err := MatchStoreKey(fs, "fresh", [][]byte{k2, k1}); err != nil || idx != 0 {
		t.Fatalf("no registry: idx=%d err=%v, want the first candidate", idx, err)
	}
	if _, err := LoadOrInitRegistry(fs, "store", k1); err != nil {
		t.Fatal(err)
	}
	for _, cands := range [][][]byte{{k1}, {k1, k2}, {k2, k1}, {k2, k3, k1}} {
		idx, err := MatchStoreKey(fs, "store", cands)
		if err != nil {
			t.Fatalf("%d candidates: %v", len(cands), err)
		}
		if string(cands[idx]) != string(k1) {
			t.Fatalf("%d candidates: picked index %d, not the sealing key", len(cands), idx)
		}
	}
	if _, err := MatchStoreKey(fs, "store", [][]byte{k2, k3}); err == nil || !strings.Contains(err.Error(), "none of the 2 store keys") {
		t.Fatalf("no candidate matches: %v", err)
	}
	if _, err := MatchStoreKey(fs, "store", nil); err == nil {
		t.Fatal("no candidates accepted")
	}
	if _, err := MatchStoreKey(fs, "store", [][]byte{k1[:5]}); err == nil {
		t.Fatal("short candidate accepted")
	}
	// After a rotation the registry answers to the new key only.
	if err := RotateStoreKey(fs, "store", k1, k2); err != nil {
		t.Fatal(err)
	}
	if idx, err := MatchStoreKey(fs, "store", [][]byte{k1, k2}); err != nil || idx != 1 {
		t.Fatalf("after rotation: idx=%d err=%v", idx, err)
	}
}

func TestSplitKeyPaths(t *testing.T) {
	for in, want := range map[string]string{
		"":                       "",
		"a.key":                  "a.key",
		"a.key,b.key":            "a.key|b.key",
		" a.key , b.key ,,c.key": "a.key|b.key|c.key",
		",":                      "",
	} {
		if got := strings.Join(SplitKeyPaths(in), "|"); got != want {
			t.Errorf("SplitKeyPaths(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestRotateStoreKeyCrashSafety: the registry reseal is tmp + rename +
// directory fsync, so a crash at any step leaves the store openable with
// exactly one of the two keys — the old one until the rename is durable,
// the new one after — and never neither. Simulated on the strict
// in-memory FS, whose CrashClone keeps only what was fsynced,
// exactly as a power loss would. Issue #68.
func TestRotateStoreKeyCrashSafety(t *testing.T) {
	k1, k2 := randKey(t), randKey(t)
	opensWith := func(fs vfs.FS, key []byte) bool {
		t.Helper()
		sealed, err := readAll(fs, fs.PathJoin("store", RegistryName))
		if err != nil {
			return false
		}
		_, err = Unseal(registryMagic, key, sealed)
		return err == nil
	}
	// The store directory itself must be durable for the crash simulation
	// to mean anything (in production, init created it long before).
	initStore := func(fs *vfs.MemFS) {
		t.Helper()
		if _, err := LoadOrInitRegistry(fs, "store", k1); err != nil {
			t.Fatal(err)
		}
		root, err := fs.OpenDir("/")
		if err != nil {
			t.Fatal(err)
		}
		if err := root.Sync(); err != nil {
			t.Fatal(err)
		}
		_ = root.Close()
	}

	// Full rotation, then crash: the new key opens the store.
	fs := vfs.NewCrashableMem()
	initStore(fs)
	fs = fs.CrashClone(vfs.CrashCloneCfg{}) // what survived the crash: exactly what was synced
	if !opensWith(fs, k1) {
		t.Fatal("initialized registry not durable after a crash")
	}
	if err := RotateStoreKey(fs, "store", k1, k2); err != nil {
		t.Fatal(err)
	}
	fs = fs.CrashClone(vfs.CrashCloneCfg{}) // what survived the crash: exactly what was synced
	if opensWith(fs, k1) || !opensWith(fs, k2) {
		t.Fatalf("after a completed rotation and a crash: old opens=%v new opens=%v", opensWith(fs, k1), opensWith(fs, k2))
	}

	// Crash before the directory fsync (the rename itself is not yet
	// durable): the rotation reports failure, the crash undoes the rename,
	// and the OLD key still opens the store — the operator was told not to
	// retire it.
	fs = vfs.NewCrashableMem()
	initStore(fs)
	fs = fs.CrashClone(vfs.CrashCloneCfg{}) // what survived the crash: exactly what was synced
	crashFS := &noDirSyncFS{FS: fs}
	if err := RotateStoreKey(crashFS, "store", k1, k2); err == nil {
		t.Fatal("rotation reported success without a durable directory entry")
	}
	fs = fs.CrashClone(vfs.CrashCloneCfg{}) // what survived the crash: exactly what was synced
	if !opensWith(fs, k1) || opensWith(fs, k2) {
		t.Fatalf("after a crash before the directory fsync: old opens=%v new opens=%v", opensWith(fs, k1), opensWith(fs, k2))
	}
	// And the leftover tmp file does not confuse a later successful
	// rotation.
	if err := RotateStoreKey(fs, "store", k1, k2); err != nil {
		t.Fatal(err)
	}
	fs = fs.CrashClone(vfs.CrashCloneCfg{}) // what survived the crash: exactly what was synced
	if !opensWith(fs, k2) {
		t.Fatal("retry after the aborted rotation did not land")
	}
}

// noDirSyncFS fails directory opens, so a registry write's directory fsync
// can never happen — the crash window after the rename.
type noDirSyncFS struct{ vfs.FS }

func (noDirSyncFS) OpenDir(string) (vfs.File, error) {
	return nil, errors.New("simulated crash before the directory fsync")
}
