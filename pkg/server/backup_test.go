package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/kvpb"
)

// A manifest is read from wherever an operator points restore, and it
// used to name the data file restore opened; a crafted one read any path
// on the node (issue #214). Every name but the one backup itself writes
// is refused when the manifest is read, before any file is opened.
func TestBackupManifestNamesOnlyItsOwnFiles(t *testing.T) {
	write := func(file string) string {
		dir := t.TempDir()
		man := backupManifest{Magic: backupManifestMagic, Tables: []backupTable{{ID: 7, Name: "t", File: file}}}
		raw, err := json.Marshal(man)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, backupManifestName), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}

	if _, err := readBackupManifest(write("table_7.dxbk")); err != nil {
		t.Fatalf("a backup's own name refused: %v", err)
	}
	for _, file := range []string{
		"../../../etc/shadow",
		"/etc/shadow",
		"sub/table_7.dxbk",
		"table_8.dxbk", // another table's file, inside the directory
		"table_7.dxbk/",
		"..",
		".",
		"",
	} {
		_, err := readBackupManifest(write(file))
		if err == nil {
			t.Errorf("manifest naming %q accepted", file)
			continue
		}
		if !strings.Contains(err.Error(), "which a datax backup keeps in") || !strings.Contains(err.Error(), "table_7.dxbk") {
			t.Errorf("manifest naming %q refused for the wrong reason: %v", file, err)
		}
	}
}

// A backup's files are read at derived names, and only a regular file is
// read at one: a symlink at the manifest's or a data file's name reaches
// outside the directory as surely as a manifest naming a path did, so it
// is refused without being followed (issue #214, the PR's review).
func TestBackupFilesMustBeRegular(t *testing.T) {
	elsewhere := t.TempDir()
	man := backupManifest{Magic: backupManifestMagic, Tables: []backupTable{{ID: 7, Name: "t", File: "table_7.dxbk"}}}
	raw, err := json.Marshal(man)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(elsewhere, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(elsewhere, "data.dxbk"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	// A symlink at the manifest's name.
	dir := t.TempDir()
	if err := os.Symlink(filepath.Join(elsewhere, "manifest.json"), filepath.Join(dir, backupManifestName)); err != nil {
		t.Fatal(err)
	}
	if _, err := readBackupManifest(dir); err == nil || !strings.Contains(err.Error(), "is not a regular file (L---------)") {
		t.Fatalf("a symlink at the manifest's name: %v", err)
	}
	// A symlink at a data file's name: a valid, empty file elsewhere.
	// Refused by the read, and — since the data files are read only
	// after restore has applied the schema — by the manifest read
	// before it, so a refused restore leaves the target as it was.
	if err := os.Symlink(filepath.Join(elsewhere, "data.dxbk"), filepath.Join(dir, backupDataFile(7))); err != nil {
		t.Fatal(err)
	}
	if err := readBackupRecords(filepath.Join(dir, backupDataFile(7)), func(kvpb.ExportRecord) error { return nil }); err == nil || !strings.Contains(err.Error(), "is not a regular file") {
		t.Fatalf("a symlink at a data file's name: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, backupManifestName)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, backupManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	man2, err := readBackupManifest(dir)
	if err != nil {
		t.Fatalf("the manifest reader looks at data files, which the --base door does not need: %v", err)
	}
	if err := checkBackupFiles(dir, man2); err == nil || !strings.Contains(err.Error(), "table_7.dxbk is not a regular file") {
		t.Fatalf("a manifest whose data file is a symlink: %v", err)
	}
	// And a data file that is not there at all.
	if err := os.Remove(filepath.Join(dir, backupDataFile(7))); err != nil {
		t.Fatal(err)
	}
	if err := checkBackupFiles(dir, man2); err == nil || !os.IsNotExist(err) {
		t.Fatalf("a manifest whose data file is missing: %v", err)
	}
	// A directory at the name.
	if err := os.Mkdir(filepath.Join(dir, backupDataFile(8)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := readBackupRecords(filepath.Join(dir, backupDataFile(8)), func(kvpb.ExportRecord) error { return nil }); err == nil || !strings.Contains(err.Error(), "is not a regular file (d---------)") {
		t.Fatalf("a directory at a data file's name: %v", err)
	}
	// The plain files themselves are read.
	plain := t.TempDir()
	if err := os.WriteFile(filepath.Join(plain, backupManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(plain, backupDataFile(7)), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readBackupManifest(plain); err != nil {
		t.Fatalf("a plain manifest: %v", err)
	}
	if err := readBackupRecords(filepath.Join(plain, backupDataFile(7)), func(kvpb.ExportRecord) error { return nil }); err != nil {
		t.Fatalf("a plain data file: %v", err)
	}
}
