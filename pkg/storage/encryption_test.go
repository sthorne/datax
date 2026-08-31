package storage

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testStoreKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

// TestEncryptedEngineRoundtrip: an encrypted store works across restarts,
// and the open-validation matrix holds on a real directory.
func TestEncryptedEngineRoundtrip(t *testing.T) {
	dir := t.TempDir()
	key := testStoreKey(t)

	e, err := Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Correct key: reopen and read back.
	e, err = Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if v, err := e.Get([]byte("k1")); err != nil || string(v) != "v1" {
		t.Fatalf("reopen read: %q %v", v, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Wrong key: refused with a key-mismatch error.
	if _, err := Open(dir, Options{EncryptionKey: testStoreKey(t)}); err == nil ||
		!strings.Contains(err.Error(), "does not match") {
		t.Fatalf("wrong key: %v", err)
	}
	// No key on an encrypted store: refused.
	if _, err := Open(dir, Options{}); err == nil ||
		!strings.Contains(err.Error(), "encrypted") {
		t.Fatalf("missing key: %v", err)
	}

	// Existing plaintext store + key: refused (no silent conversion).
	plainDir := t.TempDir()
	pe, err := Open(plainDir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := pe.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(plainDir, Options{EncryptionKey: key}); err == nil ||
		!strings.Contains(err.Error(), "unencrypted") {
		t.Fatalf("plaintext store with key: %v", err)
	}

	// In-memory engines encrypt too (fresh MemFS per Open; just prove it opens).
	me, err := Open("", Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := me.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, err := me.Get([]byte("k")); err != nil || string(v) != "v" {
		t.Fatalf("in-mem encrypted read: %q %v", v, err)
	}
	if err := me.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestEncryptionTripwire: with a key, a canary value written and flushed
// must not appear in ANY file in the store directory; the identical run
// without a key must find it (validating that the tripwire can detect
// plaintext at all).
func TestEncryptionTripwire(t *testing.T) {
	canary := []byte("plaintext-canary-7f3a")

	run := func(o Options) bool {
		t.Helper()
		dir := t.TempDir()
		e, err := Open(dir, o)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 50; i++ {
			if err := e.Put(append([]byte("key"), byte(i)), canary); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}

		found := false
		err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(b, canary) {
				found = true
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return found
	}

	if run(Options{EncryptionKey: testStoreKey(t)}) {
		t.Fatal("canary found in raw files of an encrypted store")
	}
	if !run(Options{}) {
		t.Fatal("canary not found in a plaintext store — tripwire cannot detect plaintext")
	}
}
