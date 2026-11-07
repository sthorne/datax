package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
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
	if err := readBackupRecordsUnchecked(filepath.Join(dir, backupDataFile(7)), func(kvpb.ExportRecord) error { return nil }); err == nil || !strings.Contains(err.Error(), "is not a regular file") {
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
	if err := readBackupRecordsUnchecked(filepath.Join(dir, backupDataFile(8)), func(kvpb.ExportRecord) error { return nil }); err == nil || !strings.Contains(err.Error(), "is not a regular file (d---------)") {
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
	if err := readBackupRecordsUnchecked(filepath.Join(plain, backupDataFile(7)), func(kvpb.ExportRecord) error { return nil }); err != nil {
		t.Fatalf("a plain data file: %v", err)
	}
}

// The raw key/value groups of a manifest are written back verbatim by
// restore, so each is held to the spans backup collects it from when the
// manifest is read (issue #238): a manifest carrying the cluster's
// authentication secret, a /meta record, or a database entry outside both
// database spans is refused, and what a real backup produces — database
// descriptors mixed with name entries, sequences from three spans — is
// still accepted.
func TestBackupManifestKeysStayInTheirSpans(t *testing.T) {
	write := func(man backupManifest) string {
		dir := t.TempDir()
		man.Magic = backupManifestMagic
		raw, err := json.Marshal(man)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, backupManifestName), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	start := func(span func() (keys.Key, keys.Key)) []byte { s, _ := span(); return s }
	metaStart, _ := keys.MetaSpan()

	// What backup writes is accepted, spans mixed as backup mixes them.
	real := backupManifest{
		Users:     []backupKV{{Key: start(keys.UserSpan), Value: []byte("u")}},
		Admins:    []backupKV{{Key: start(keys.AdminUserSpan), Value: []byte("a")}},
		Roles:     []backupKV{{Key: keys.RoleKey("root"), Value: []byte("r")}},
		Databases: []backupKV{{Key: keys.DatabaseDescKey(1), Value: []byte("d")}, {Key: keys.DatabaseNamespaceKey("datax"), Value: []byte("1")}},
		Sequences: []backupKV{{Key: start(keys.SequenceDescSpan), Value: []byte("s")}, {Key: start(keys.AllSequenceNamespaceSpan), Value: []byte("n")}, {Key: start(keys.SequenceValueSpan), Value: []byte("v")}},
	}
	if _, err := readBackupManifest(write(real)); err != nil {
		t.Fatalf("what backup writes was refused: %v", err)
	}

	for _, c := range []struct {
		name string
		man  backupManifest
		key  keys.Key
	}{
		{"users", backupManifest{Users: []backupKV{{Key: keys.AuthSecretKey(), Value: make([]byte, 32)}}}, keys.AuthSecretKey()},
		{"admins", backupManifest{Admins: []backupKV{{Key: keys.AuthSecretKey()}}}, keys.AuthSecretKey()},
		{"roles", backupManifest{Roles: []backupKV{{Key: keys.ClusterVersionKey()}}}, keys.ClusterVersionKey()},
		{"users", backupManifest{Users: []backupKV{{Key: metaStart}}}, metaStart},
		{"databases", backupManifest{Databases: []backupKV{{Key: keys.DatabaseDescKey(1)}, {Key: keys.TableDescKey(5)}}}, keys.TableDescKey(5)},
		{"sequences", backupManifest{Sequences: []backupKV{{Key: start(keys.UserSpan)}}}, start(keys.UserSpan)},
		// The span's end is outside it.
		{"roles", backupManifest{Roles: []backupKV{{Key: func() []byte { _, e := keys.RoleSpan(); return e }()}}}, nil},
	} {
		_, err := readBackupManifest(write(c.man))
		if err == nil {
			t.Fatalf("a manifest whose %s carries %s was accepted", c.name, c.key)
		}
		if !strings.Contains(err.Error(), "manifest's "+c.name+" carries the key") || !strings.Contains(err.Error(), "never collects there") {
			t.Fatalf("a manifest whose %s carries %s was refused for another reason: %v", c.name, c.key, err)
		}
	}

	// The same, derived from the table rather than listed: every group
	// refuses every key a backup never collects anywhere — the
	// authentication secret above all. A span added to one group's
	// entry that reaches one of these is caught here whichever group it
	// is added to (issue #256: the hand-written cases above asserted the
	// secret refused for users and admins only, so widening sequences
	// to accept it survived the suite).
	outside := []struct {
		what string
		key  keys.Key
	}{
		{"the authentication secret", keys.AuthSecretKey()},
		{"the cluster version", keys.ClusterVersionKey()},
		{"the meta span's start", metaStart},
		{"a table descriptor", keys.TableDescKey(5)},
		{"a table's data", keys.TableDataPrefix(5)},
	}
	for _, g := range manifestKeyGroups {
		for _, o := range outside {
			if inAnySpan(o.key, g.spans()) {
				t.Fatalf("manifestKeyGroups[%q] accepts %s (%s), which no backup collects", g.name, o.what, o.key)
			}
			var man backupManifest
			*g.kvs(&man) = []backupKV{{Key: o.key, Value: make([]byte, 32)}}
			_, err := readBackupManifest(write(man))
			if err == nil {
				t.Fatalf("a manifest whose %s carries %s (%s) was accepted", g.name, o.what, o.key)
			}
			if !strings.Contains(err.Error(), "manifest's "+g.name+" carries the key") {
				t.Fatalf("a manifest whose %s carries %s was refused for another reason: %v", g.name, o.what, err)
			}
		}
	}
}

// TestBackupDataFilesStayInTheirTableSpan (issue #255): a table's data
// file is held to that table's data span before anything is applied,
// and tombstones are held to it too — a record appended to the file
// with the manifest untouched is refused, as a manifest key outside its
// group's span is.
func TestBackupDataFilesStayInTheirTableSpan(t *testing.T) {
	const tableID = 5
	table := backupTable{ID: tableID, Name: "t", File: backupDataFile(tableID)}
	man := &backupManifest{Tables: []backupTable{table}}
	write := func(recs ...kvpb.ExportRecord) string {
		dir := t.TempDir()
		f, err := os.Create(filepath.Join(dir, table.File))
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range recs {
			if err := writeBackupRecord(f, rec); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	own := kvpb.ExportRecord{Key: keys.TableIndexPrefix(tableID, 1), Value: []byte("row")}
	ownGone := kvpb.ExportRecord{Key: keys.TableIndexPrefix(tableID, 2), Deleted: true}
	if err := checkBackupRecords(write(own, ownGone), man); err != nil {
		t.Fatalf("a file carrying the table's own keys, a tombstone among them, was refused: %v", err)
	}
	if err := checkBackupRecords(write(), man); err != nil {
		t.Fatalf("an empty data file was refused: %v", err)
	}
	metaStart, _ := keys.MetaSpan()
	_, end := keys.TableDataSpan(tableID)
	for _, c := range []struct {
		what string
		rec  kvpb.ExportRecord
	}{
		{"the authentication secret", kvpb.ExportRecord{Key: keys.AuthSecretKey(), Value: make([]byte, 32)}},
		{"a tombstone for the authentication secret", kvpb.ExportRecord{Key: keys.AuthSecretKey(), Deleted: true}},
		{"a role's verifier", kvpb.ExportRecord{Key: keys.RoleKey("root"), Value: []byte("v")}},
		{"the cluster version", kvpb.ExportRecord{Key: keys.ClusterVersionKey(), Value: []byte("1")}},
		{"a table descriptor", kvpb.ExportRecord{Key: keys.TableDescKey(tableID), Value: []byte("{}")}},
		{"the meta span", kvpb.ExportRecord{Key: metaStart, Value: []byte("m")}},
		{"another table's row", kvpb.ExportRecord{Key: keys.TableIndexPrefix(tableID+1, 1), Value: []byte("row")}},
		{"the span's end", kvpb.ExportRecord{Key: end, Value: []byte("row")}},
	} {
		// After the table's own records, so a refusal has to come from
		// the record and not from a position.
		dir := write(own, c.rec)
		err := checkBackupRecords(dir, man)
		if err == nil {
			t.Fatalf("a data file carrying %s (%s) was accepted", c.what, keys.Key(c.rec.Key))
		}
		if !strings.Contains(err.Error(), "table t's data file carries the key") || !strings.Contains(err.Error(), "outside the table's own span") {
			t.Fatalf("a data file carrying %s was refused for another reason: %v", c.what, err)
		}
		// The apply loop's read is the same read: the bad record is
		// refused before the caller sees it, and the records before it
		// were delivered (a reader that saw nothing would have refused
		// at the wrong place).
		var seen []string
		err = readCheckedBackupRecords(filepath.Join(dir, table.File), table, func(rec kvpb.ExportRecord) error {
			seen = append(seen, keys.Key(rec.Key).String())
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "outside the table's own span") {
			t.Fatalf("readCheckedBackupRecords delivered %s to its caller: %v", c.what, err)
		}
		if len(seen) != 1 || seen[0] != keys.Key(own.Key).String() {
			t.Fatalf("readCheckedBackupRecords delivered %v before refusing %s", seen, c.what)
		}
	}
}
