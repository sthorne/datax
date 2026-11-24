package server

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/storage/enc"
)

// TestMetadataBackupRotationWritesLast: an export that read the old store
// key before a rotation's swap must not land after the rotation's reseal.
// The first export parks after sealing under key1; the rotation swaps to
// key2 and writes; then the parked export is let go. The file must open
// under key2. Without exportMu the rotation's write lands first and the
// parked one overwrites it under the retired key.
func TestMetadataBackupRotationWritesLast(t *testing.T) {
	key1 := bytes.Repeat([]byte{1}, 32)
	key2 := bytes.Repeat([]byte{2}, 32)
	dir := t.TempDir()
	n := &Node{cfg: Config{Dir: dir}, encKey: key1}

	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	testingMetadataBackupSealed = func() {
		once.Do(func() {
			close(parked)
			<-release
		})
	}
	defer func() { testingMetadataBackupSealed = nil }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the heartbeat's export, under key1
		defer wg.Done()
		n.writeMetadataBackup([]byte(`{"from":"heartbeat"}`), []byte("heartbeat"))
	}()
	<-parked

	// The rotation: swap the key, then export (as adoptStoreKey does).
	n.encKeyMu.Lock()
	n.encKey = key2
	n.encKeyMu.Unlock()
	rotated := make(chan struct{})
	go func() {
		defer close(rotated)
		n.writeMetadataBackup([]byte(`{"from":"rotation"}`), []byte("rotation"))
	}()
	// Give the rotation's export the chance to finish ahead of the parked
	// one, which is what it did before the fix.
	select {
	case <-rotated:
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	<-rotated

	raw, err := os.ReadFile(filepath.Join(dir, MetadataBackupFile))
	if err != nil {
		t.Fatal(err)
	}
	got, err := enc.Unseal(MetadataBackupMagic, key2, raw)
	if err != nil {
		t.Fatalf("after the rotation the metadata backup does not open under the new key: %v", err)
	}
	if string(got) != `{"from":"rotation"}` {
		t.Fatalf("the backup holds %s, want the rotation's export", got)
	}
}

// TestMetadataBackupResealedWithoutExport: a rotation reseals the backup
// already on disk before it exports, so the file never stays under the
// retired key when the export's scans fail (they are best-effort).
func TestMetadataBackupResealedWithoutExport(t *testing.T) {
	key1 := bytes.Repeat([]byte{1}, 32)
	key2 := bytes.Repeat([]byte{2}, 32)
	dir := t.TempDir()
	n := &Node{cfg: Config{Dir: dir}, encKey: key1}
	n.writeMetadataBackup([]byte(`{"from":"heartbeat"}`), []byte("heartbeat"))

	n.encKeyMu.Lock()
	n.encKey = key2
	n.encKeyMu.Unlock()
	n.resealMetadataBackup(key1, key2)

	raw, err := os.ReadFile(filepath.Join(dir, MetadataBackupFile))
	if err != nil {
		t.Fatal(err)
	}
	got, err := enc.Unseal(MetadataBackupMagic, key2, raw)
	if err != nil {
		t.Fatalf("the metadata backup was not resealed under the new key: %v", err)
	}
	if string(got) != `{"from":"heartbeat"}` {
		t.Fatalf("the backup holds %s", got)
	}
	// A second reseal (already under the new key) leaves it alone.
	n.resealMetadataBackup(key1, key2)
	if again, _ := os.ReadFile(filepath.Join(dir, MetadataBackupFile)); !bytes.Equal(again, raw) {
		t.Fatal("a reseal of a backup already under the new key rewrote it")
	}
}
