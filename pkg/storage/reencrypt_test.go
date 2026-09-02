package storage

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func testKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, enc.KeyLen)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

// TestRotateStoreKeyLive: the store key rotates on a LIVE engine under
// concurrent writes; the wrong old key is refused; after a clean close
// only the new key opens the store, and every row survives. Issue #48.
func TestRotateStoreKeyLive(t *testing.T) {
	dir := t.TempDir()
	key1, key2 := testKey(t), testKey(t)
	e, err := Open(dir, Options{EncryptionKey: key1})
	if err != nil {
		t.Fatal(err)
	}
	if !e.Encrypted() {
		t.Fatal("engine does not report encrypted")
	}

	// Writes continue right through the rotation.
	stop := make(chan struct{})
	writeErr := make(chan error, 1)
	go func() {
		defer close(writeErr)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := e.Put([]byte(fmt.Sprintf("live-%06d", i)), []byte("v")); err != nil {
				writeErr <- err
				return
			}
		}
	}()

	if err := e.RotateStoreKeyLive(key2, key2); err == nil ||
		!strings.Contains(err.Error(), "does not match") {
		t.Fatalf("rotation with the wrong old key: %v", err)
	}
	if err := e.RotateStoreKeyLive(key1, key2); err != nil {
		t.Fatalf("live rotation: %v", err)
	}
	// A second rotation must need the NEW key — proof the reseal landed.
	if err := e.RotateStoreKeyLive(key1, testKey(t)); err == nil {
		t.Fatal("stale old key accepted after rotation")
	}
	close(stop)
	if err := <-writeErr; err != nil {
		t.Fatalf("write failed during rotation: %v", err)
	}
	if err := e.Put([]byte("post-rotate"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(dir, Options{EncryptionKey: key1}); err == nil {
		t.Fatal("old store key still opens the store")
	}
	e2, err := Open(dir, Options{EncryptionKey: key2})
	if err != nil {
		t.Fatalf("new store key rejected: %v", err)
	}
	defer func() { _ = e2.Close() }()
	if v, err := e2.Get([]byte("post-rotate")); err != nil || string(v) != "v" {
		t.Fatalf("data lost across rotation: %q %v", v, err)
	}
}

// TestReencryptPass: after a restart mints a fresh active data key, the
// previous session's sstables read as stale; re-encryption passes rewrite
// them until none remain, data intact, and every live sstable's header
// then carries the active key. Issue #48.
func TestReencryptPass(t *testing.T) {
	dir := t.TempDir()
	key := testKey(t)
	e, err := Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	// MVCC-encoded keys — the shape real data has, and what the pass's
	// overlap seeding keys off.
	mvccKey := func(i int) []byte {
		return EncodeMVCCKey(keys.Key(fmt.Sprintf("k-%06d", i)), hlc.Timestamp{WallTime: int64(i + 1)})
	}
	val := make([]byte, 100)
	for i := 0; i < 1000; i++ {
		if err := e.Put(mvccKey(i), val); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: a fresh data key becomes active; the old ssts are now stale.
	e2, err := Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e2.Close() }()
	remaining, files, _ := e2.ReencryptionStatus()
	if remaining == 0 || files == 0 {
		t.Fatalf("no stale files after restart: %d bytes / %d files", remaining, files)
	}

	ctx := context.Background()
	var rewritten int64
	for pass := 0; ; pass++ {
		if pass > 20 {
			t.Fatalf("re-encryption did not converge; %d bytes remain", remaining)
		}
		targeted, rem, _, err := e2.ReencryptPass(ctx, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		rewritten += targeted
		remaining = rem
		if remaining == 0 {
			break
		}
	}
	if rewritten == 0 {
		t.Fatal("no bytes were rewritten")
	}
	if rb, rf, serr := e2.ReencryptionStatus(); rb != 0 || rf != 0 || serr != nil {
		t.Fatalf("status after convergence: %d bytes / %d files", rb, rf)
	}

	// Data intact, and every remaining encrypted file (registry aside)
	// carries the active data key.
	for _, i := range []int{0, 500, 999} {
		if v, err := e2.Get(mvccKey(i)); err != nil || len(v) != len(val) {
			t.Fatalf("row %d after re-encryption: %d bytes, %v", i, len(v), err)
		}
	}
	activeID, _ := e2.encKeys.Active()
	stale, err := e2.staleTables()
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 0 {
		t.Fatalf("%d live sstables still stale (active key %d)", len(stale), activeID)
	}
}
