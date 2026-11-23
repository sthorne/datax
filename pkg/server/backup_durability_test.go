package server

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// recordSyncs replaces syncFile and syncDir with versions that note what
// they sync (a file by its base name, a directory as "dir:" plus its base
// name) and then sync it for real.
func recordSyncs(t *testing.T) *[]string {
	t.Helper()
	var got []string
	prevFile, prevDir := syncFile, syncDir
	syncFile = func(f *os.File) error {
		got = append(got, filepath.Base(f.Name()))
		return prevFile(f)
	}
	syncDir = func(dir string) error {
		// The order matters: what the directory holds when it is synced
		// is what the sync makes durable.
		names, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		entry := "dir:" + filepath.Base(dir)
		for _, e := range names {
			if !e.IsDir() {
				entry += " " + e.Name()
			}
		}
		got = append(got, entry)
		return prevDir(dir)
	}
	t.Cleanup(func() { syncFile, syncDir = prevFile, prevDir })
	return &got
}

// TestBackupCommitIsDurable: a backup's manifest is synced before it is
// renamed into place, and the destination directory is synced once the
// data files are in it (before the manifest appears) and again once the
// manifest is. Without that a power loss after "backup written" could
// leave a manifest naming data files that never reached the disk.
func TestBackupCommitIsDurable(t *testing.T) {
	got := recordSyncs(t)
	dest := filepath.Join(t.TempDir(), "bak")
	if err := os.Mkdir(dest, 0o700); err != nil {
		t.Fatal(err)
	}
	staging, err := os.MkdirTemp(dest, ".backup-in-progress-")
	if err != nil {
		t.Fatal(err)
	}
	tables := []backupTable{{File: backupDataFile(52)}, {File: backupDataFile(53)}}
	for _, bt := range tables {
		if err := os.WriteFile(filepath.Join(staging, bt.File), []byte("rows"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	moved, err := commitBackup(staging, dest, tables, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(moved) != 2 {
		t.Fatalf("moved %v, want both data files", moved)
	}
	f52, f53 := backupDataFile(52), backupDataFile(53)
	want := []string{
		backupManifestName,
		"dir:bak " + f52 + " " + f53,
		"dir:bak " + backupManifestName + " " + f52 + " " + f53,
	}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("syncs:\n got %q\nwant %q", *got, want)
	}
}

// TestBackupCommitFailedSyncLeavesNoManifest: if the destination cannot
// be synced after the manifest's rename, the backup is not reported
// written and no manifest is left to name the data files the caller then
// removes.
func TestBackupCommitFailedSyncLeavesNoManifest(t *testing.T) {
	dest := t.TempDir()
	staging, err := os.MkdirTemp(dest, ".backup-in-progress-")
	if err != nil {
		t.Fatal(err)
	}
	tables := []backupTable{{File: backupDataFile(52)}}
	if err := os.WriteFile(filepath.Join(staging, tables[0].File), []byte("rows"), 0o600); err != nil {
		t.Fatal(err)
	}
	prev := syncDir
	calls := 0
	syncDir = func(string) error {
		if calls++; calls == 2 {
			return errors.New("EIO")
		}
		return nil
	}
	defer func() { syncDir = prev }()
	if _, err := commitBackup(staging, dest, tables, []byte(`{}`)); err == nil {
		t.Fatal("commit succeeded though the directory sync failed")
	}
	if _, err := os.Stat(filepath.Join(dest, backupManifestName)); !os.IsNotExist(err) {
		t.Fatalf("manifest left behind after a failed sync: %v", err)
	}
}

// TestMetadataBackupIsDurable: metadata-backup.json is synced before its
// rename and the data directory after it.
func TestMetadataBackupIsDurable(t *testing.T) {
	got := recordSyncs(t)
	dir := t.TempDir()
	n := &Node{cfg: Config{Dir: dir}}
	n.writeMetadataBackup([]byte(`{}`), []byte(`{}`))
	want := []string{
		MetadataBackupFile + ".tmp",
		"dir:" + filepath.Base(dir) + " " + MetadataBackupFile,
	}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("syncs:\n got %q\nwant %q", *got, want)
	}
}

// TestMetadataBackupWrittenOnlyOnChange: the heartbeat exports every
// beat, and each write is fsynced, so an export whose content (less the
// capture time and per-beat registry fields) and key match the last one
// written leaves the file alone; changed content, a new key or a missing
// file write it again.
func TestMetadataBackupWrittenOnlyOnChange(t *testing.T) {
	got := recordSyncs(t)
	dir := t.TempDir()
	path := filepath.Join(dir, MetadataBackupFile)
	n := &Node{cfg: Config{Dir: dir}}
	read := func() string {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	n.writeMetadataBackup([]byte(`{"beat":1}`), []byte("v1"))
	writes := len(*got)
	if writes == 0 {
		t.Fatal("the first export did not write")
	}
	n.writeMetadataBackup([]byte(`{"beat":2}`), []byte("v1"))
	if len(*got) != writes || read() != `{"beat":1}` {
		t.Fatalf("an unchanged export was written again (%d syncs, was %d; file %s)", len(*got), writes, read())
	}
	n.writeMetadataBackup([]byte(`{"beat":3}`), []byte("v2"))
	if read() != `{"beat":3}` {
		t.Fatalf("a changed export was not written: %s", read())
	}
	n.encKey = bytes.Repeat([]byte{7}, 32)
	n.writeMetadataBackup([]byte(`{"beat":4}`), []byte("v2"))
	if _, err := enc.Unseal(MetadataBackupMagic, n.encKey, []byte(read())); err != nil {
		t.Fatalf("a new key did not reseal the backup: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	n.writeMetadataBackup([]byte(`{"beat":5}`), []byte("v2"))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a removed backup was not written again: %v", err)
	}
}

// TestRootFilesAreDurable: the files a sealed node needs before its
// store opens (SEAL-IDENTITY) and the metadata backup resealed under a
// rotated key are synced before they are renamed into place, and the
// directory after, so a power loss cannot leave either truncated.
func TestRootFilesAreDurable(t *testing.T) {
	t.Run("seal identity", func(t *testing.T) {
		got := recordSyncs(t)
		dir := t.TempDir()
		n := &Node{cfg: Config{Dir: dir}, encRoot: &enc.ShareRoot{}, registry: cluster.NewRegistry()}
		n.registry.Upsert(kvpb.NodeDescriptor{NodeID: 2, Address: "10.0.0.2:26257"})
		n.writeSealIdentity()
		want := []string{
			SealIdentityFile + ".tmp",
			"dir:" + filepath.Base(dir) + " " + SealIdentityFile,
		}
		if !reflect.DeepEqual(*got, want) {
			t.Fatalf("syncs:\n got %q\nwant %q", *got, want)
		}
	})
	t.Run("offline metadata backup reseal", func(t *testing.T) {
		dir := t.TempDir()
		oldKey, newKey := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
		sealed, err := enc.Seal(MetadataBackupMagic, oldKey, []byte(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, MetadataBackupFile), sealed, 0o600); err != nil {
			t.Fatal(err)
		}
		got := recordSyncs(t)
		if err := ResealMetadataBackup(dir, oldKey, newKey); err != nil {
			t.Fatal(err)
		}
		want := []string{
			MetadataBackupFile + ".tmp",
			"dir:" + filepath.Base(dir) + " " + MetadataBackupFile,
		}
		if !reflect.DeepEqual(*got, want) {
			t.Fatalf("syncs:\n got %q\nwant %q", *got, want)
		}
	})
}
