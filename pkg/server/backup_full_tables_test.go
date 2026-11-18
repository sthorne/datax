package server

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestFullTablesManifestMagic (issue #376): an incremental holding a
// table in full carries a magic of its own, so a binary from before it
// refuses the backup rather than laying the table over its base — which
// would bring back the rows a purge removed — and this binary refuses a
// manifest whose magic and tables disagree.
func TestFullTablesManifestMagic(t *testing.T) {
	for _, m := range []string{backupManifestMagic, backupHistoryMagic, backupTimedMagic} {
		if m == backupFullTablesMagic {
			t.Fatalf("the full-tables magic %q is already another format's", m)
		}
	}
	incr := backupManifest{ClusterID: "c", BaseTS: hts(1), EndTS: hts(2)}
	for _, c := range []struct {
		what                    string
		magic                   string
		base                    bool
		history, full, commitTS bool
		want                    string
	}{
		{"full table", backupFullTablesMagic, true, false, true, false, ""},
		{"full table, with history", backupFullTablesMagic, true, true, true, false, ""},
		{"full table, with commit times", backupFullTablesMagic, true, false, true, true, ""},
		{"no full table", backupFullTablesMagic, true, false, false, false, "disagree"},
		{"a full backup", backupFullTablesMagic, false, false, true, false, "disagree"},
		{"history and commit times", backupFullTablesMagic, true, true, true, true, "disagree"},
		{"under the plain magic", backupManifestMagic, true, false, true, false, "disagree"},
		{"under the history magic", backupHistoryMagic, true, true, true, false, "disagree"},
		{"under the timed magic", backupTimedMagic, true, false, true, true, "disagree"},
	} {
		man := incr
		if !c.base {
			man.BaseTS = hts(0)
		}
		man.Magic, man.RevisionHistory = c.magic, c.history
		man.Tables = []backupTable{
			{ID: 100, Name: "purged", File: backupDataFile(100), Full: c.full, CommitTimes: c.commitTS},
			{ID: 101, Name: "other", File: backupDataFile(101)},
		}
		raw, _ := json.Marshal(man)
		got, err := parseBackupManifest("dir", raw)
		if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%s: got %v, want %q", c.what, err, c.want)
			continue
		}
		if err != nil {
			continue
		}
		// The full table's versions are as old as its rows; the other
		// table's lie in the interval.
		if !got.Tables[0].window[0].IsEmpty() || !got.Tables[1].window[0].Equal(hts(1)) {
			t.Errorf("%s: windows %v and %v", c.what, got.Tables[0].window, got.Tables[1].window)
		}
	}
	// What a binary that knows only the earlier magics sees.
	raw, _ := json.Marshal(backupManifest{Magic: backupFullTablesMagic, BaseTS: hts(1), EndTS: hts(2)})
	var old struct {
		Magic string `json:"magic"`
	}
	if err := json.Unmarshal(raw, &old); err != nil || old.Magic == backupManifestMagic || old.Magic == backupHistoryMagic || old.Magic == backupTimedMagic {
		t.Fatalf("a full-tables manifest reads as %q to an older binary", old.Magic)
	}
}

// TestSupersedeFullTables: a restore reads a table held in full from the
// last backup of the chain that holds it so, and from none before it;
// the other tables, and the backups after it, are read as before.
func TestSupersedeFullTables(t *testing.T) {
	tbl := func(id uint64, full bool) backupTable { return backupTable{ID: id, Full: full} }
	full := &backupManifest{Tables: []backupTable{tbl(1, false), tbl(2, false)}}
	inc1 := &backupManifest{BaseTS: hts(1), Tables: []backupTable{tbl(1, true), tbl(2, false)}}
	inc2 := &backupManifest{BaseTS: hts(2), Tables: []backupTable{tbl(1, false), tbl(2, true)}}
	inc3 := &backupManifest{BaseTS: hts(3), Tables: []backupTable{tbl(1, false), tbl(2, false)}}
	mans := []*backupManifest{full, inc1, inc2, inc3}
	supersedeFullTables(mans)
	ids := func(m *backupManifest) []uint64 {
		var out []uint64
		for _, t := range m.Tables {
			out = append(out, t.ID)
		}
		return out
	}
	for i, m := range mans {
		got := ids(m)
		w := [][]uint64{{}, {1}, {1, 2}, {1, 2}}[i]
		if len(got) != len(w) {
			t.Fatalf("backup %d keeps tables %v, want %v", i, got, w)
		}
		for j := range w {
			if got[j] != w[j] {
				t.Fatalf("backup %d keeps tables %v, want %v", i, got, w)
			}
		}
	}
	// The manifests the chain was opened with are not changed.
	if len(full.Tables) != 2 || len(inc1.Tables) != 2 {
		t.Fatal("supersedeFullTables changed the manifests it was given")
	}
}
