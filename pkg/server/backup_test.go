package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
