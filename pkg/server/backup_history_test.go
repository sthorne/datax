package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func hts(wall int64) hlc.Timestamp { return hlc.Timestamp{WallTime: wall} }

// testBackupDir lays down one backup of a chain by hand: man as given
// (its magic set from RevisionHistory) and each table's records, framed
// with timestamps for a history backup and sealed under backupKey when
// one is given.
func testBackupDir(t *testing.T, backupKey []byte, man backupManifest, records map[uint64][]kvpb.ExportRecord) string {
	t.Helper()
	dir := t.TempDir()
	man.Magic = backupManifestMagic
	if man.RevisionHistory {
		man.Magic = backupHistoryMagic
	}
	var dataKey []byte
	if backupKey != nil {
		dataKey = bytes.Repeat([]byte{0x5a}, enc.KeyLen)
	}
	// The caller may hand the same tables to several backups.
	man.Tables = append([]backupTable(nil), man.Tables...)
	for i := range man.Tables {
		man.Tables[i].File = backupDataFile(man.Tables[i].ID)
		var buf bytes.Buffer
		// Counted and hashed as RunBackupWith does: once per key, at its
		// newest version.
		h := sha256.New()
		var last keys.Key
		bt := &man.Tables[i]
		for _, rec := range records[bt.ID] {
			write := writeBackupRecord
			if man.RevisionHistory {
				write = writeBackupRevision
				bt.Versions++
			}
			if err := write(&buf, rec); err != nil {
				t.Fatal(err)
			}
			if man.RevisionHistory && last != nil && last.Equal(rec.Key) {
				continue
			}
			last = keys.Key(rec.Key).Clone()
			bt.Records++
			if !rec.Deleted {
				hashLiveRecord(h, rec)
			}
		}
		bt.SHA256 = hex.EncodeToString(h.Sum(nil))
		out := buf.Bytes()
		if dataKey != nil {
			var sealed bytes.Buffer
			w, err := enc.NewStreamWriter(&sealed, dataKey, man.Tables[i].File)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(out); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			out = sealed.Bytes()
		}
		if err := os.WriteFile(filepath.Join(dir, man.Tables[i].File), out, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(man)
	if err != nil {
		t.Fatal(err)
	}
	if dataKey != nil {
		if raw, err = sealBackupManifest(backupKey, dataKey, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, backupManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func tableKey(id uint64, suffix string) keys.Key {
	return append(keys.TableDataPrefix(id), suffix...)
}

func testTableDesc(t *testing.T, d catalog.TableDescriptor) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(&d)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// testChain is a full backup at 100, a plain incremental to 200 and a
// history incremental to 300, of one table (id 60):
//
//	a: a1 in the full; a2@250, a3@280
//	b: b1 in the full; deleted by 200
//	c: created by 200 (c1); deleted@260; back as c2@290
//	d: d1 in the full, untouched
//	e: created@270 (e1)
func testChain(t *testing.T, backupKey []byte) (srcs []string, desc json.RawMessage) {
	t.Helper()
	const id = 60
	desc = testTableDesc(t, catalog.TableDescriptor{ID: id, Name: "t", DatabaseID: 1,
		Columns: []catalog.Column{{ID: 1, Name: "k"}}, PrimaryKey: []catalog.ColumnID{1}})
	tbl := []backupTable{{ID: id, Name: "t", Descriptor: desc}}
	rec := func(k, v string, wall int64) kvpb.ExportRecord {
		r := kvpb.ExportRecord{Key: tableKey(id, k), Timestamp: hlc.Timestamp{WallTime: wall}}
		if v == "" {
			r.Deleted = true
		} else {
			r.Value = []byte(v)
		}
		return r
	}
	full := testBackupDir(t, backupKey, backupManifest{ClusterID: "c", EndTS: hts(100), Tables: tbl},
		map[uint64][]kvpb.ExportRecord{id: {rec("a", "a1", 0), rec("b", "b1", 0), rec("d", "d1", 0)}})
	plain := testBackupDir(t, backupKey, backupManifest{ClusterID: "c", BaseTS: hts(100), EndTS: hts(200), Tables: tbl},
		map[uint64][]kvpb.ExportRecord{id: {rec("b", "", 0), rec("c", "c1", 0)}})
	hist := testBackupDir(t, backupKey, backupManifest{ClusterID: "c", BaseTS: hts(200), EndTS: hts(300), RevisionHistory: true, Tables: tbl},
		map[uint64][]kvpb.ExportRecord{id: {
			rec("a", "a3", 280), rec("a", "a2", 250),
			rec("c", "c2", 290), rec("c", "", 260),
			rec("e", "e1", 270),
		}})
	return []string{full, plain, hist}, desc
}

// TestMergeTableAsOf: a table walked through a chain to a moment comes
// out as it stood then — for the end of each backup and for moments
// inside the interval kept with history, sealed and plaintext alike.
func TestMergeTableAsOf(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		var key []byte
		var keysArg [][]byte
		if sealed {
			key = bytes.Repeat([]byte{7}, enc.KeyLen)
			keysArg = [][]byte{key}
		}
		srcs, _ := testChain(t, key)
		for _, c := range []struct {
			asOf int64
			want string
		}{
			{0, "a=a3 c=c2 d=d1 e=e1"},   // the chain's end
			{300, "a=a3 c=c2 d=d1 e=e1"}, // exactly the last backup
			{100, "a=a1 b=b1 d=d1"},      // exactly the full backup
			{200, "a=a1 c=c1 d=d1"},      // exactly the plain incremental
			{201, "a=a1 c=c1 d=d1"},      // just inside the history interval
			{250, "a=a2 c=c1 d=d1"},      // at a version's own timestamp
			{259, "a=a2 c=c1 d=d1"},
			{260, "a=a2 d=d1"}, // c's deletion
			{275, "a=a2 d=d1 e=e1"},
			{285, "a=a3 d=d1 e=e1"},
			{299, "a=a3 c=c2 d=d1 e=e1"},
		} {
			var asOf hlc.Timestamp
			if c.asOf != 0 {
				asOf = hts(c.asOf)
			}
			chain, err := openRestoreChain(srcs, keysArg, asOf)
			if err != nil {
				t.Fatalf("sealed=%v as of %d: %v", sealed, c.asOf, err)
			}
			var got []string
			if err := mergeTable(chain, 60, func(rec kvpb.ExportRecord) error {
				got = append(got, string(rec.Key[len(keys.TableDataPrefix(60)):])+"="+string(rec.Value))
				return nil
			}); err != nil {
				t.Fatalf("sealed=%v as of %d: %v", sealed, c.asOf, err)
			}
			if s := strings.Join(got, " "); s != c.want {
				t.Errorf("sealed=%v as of %d: table is %q, want %q", sealed, c.asOf, s, c.want)
			}
		}
	}
}

// TestChainCutRefusals: a moment the chain cannot restore to is refused,
// and the refusal says what the chain does restore to.
func TestChainCutRefusals(t *testing.T) {
	srcs, _ := testChain(t, nil)
	for _, c := range []struct {
		asOf int64
		want string
	}{
		{99, "is earlier than the full backup"},
		{301, "is later than the chain's last backup"},
		{150, "was taken without --revision-history"},
	} {
		_, err := openRestoreChain(srcs, nil, hts(c.asOf))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("as of %d: got %v, want a refusal saying %q", c.asOf, err, c.want)
		}
		// The window: the full backup's end exactly, then the history
		// interval, which starts at the plain incremental's end.
		for _, part := range []string{"exactly 100 ", "any time from 200 ", " to 300 "} {
			if !strings.Contains(err.Error(), part) {
				t.Fatalf("as of %d: the refusal does not say what is restorable (%q missing): %v", c.asOf, part, err)
			}
		}
	}
	// A logical tick past a backup's end is past it.
	if _, err := openRestoreChain(srcs, nil, hlc.Timestamp{WallTime: 300, Logical: 1}); err == nil {
		t.Fatal("a moment one logical tick after the last backup was accepted")
	}
	// The chain is cut where the moment falls: a restore to the full
	// backup's end reads nothing of the backups after it.
	chain, err := openRestoreChain(srcs, nil, hts(100))
	if err != nil || len(chain.srcs) != 1 || !chain.cutoff.IsEmpty() {
		t.Fatalf("as of the full backup: chain of %d, cutoff %s, err %v", len(chain.srcs), chain.cutoff, err)
	}
}

// TestHistoryManifestMagic: a backup with revision history carries a
// magic of its own, so a binary that knows only the first format refuses
// it by the check it has always made, and a manifest whose magic and
// marker disagree is refused by this one.
func TestHistoryManifestMagic(t *testing.T) {
	if backupHistoryMagic == backupManifestMagic {
		t.Fatal("a history backup must not carry the plain backup's magic: an older binary would read its files in the old framing")
	}
	base := backupManifest{ClusterID: "c", BaseTS: hts(1), EndTS: hts(2)}
	for _, c := range []struct {
		magic   string
		history bool
		want    string
	}{
		{backupManifestMagic, false, ""},
		{backupHistoryMagic, true, ""},
		{backupManifestMagic, true, "disagree"},
		{backupHistoryMagic, false, "disagree"},
		{"DXBK3", false, "not a datax backup"},
	} {
		man := base
		man.Magic, man.RevisionHistory = c.magic, c.history
		raw, _ := json.Marshal(man)
		_, err := parseBackupManifest("dir", raw)
		if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
			t.Errorf("magic %q, history %v: got %v, want %q", c.magic, c.history, err, c.want)
		}
	}
	// History with no base is not a thing a backup writes.
	man := backupManifest{Magic: backupHistoryMagic, RevisionHistory: true, EndTS: hts(2)}
	raw, _ := json.Marshal(man)
	if _, err := parseBackupManifest("dir", raw); err == nil {
		t.Error("a history manifest without a base timestamp was accepted")
	}
	// What a binary from before the format does with one: it compares the
	// magic with the only one it knows.
	raw, _ = json.Marshal(backupManifest{Magic: backupHistoryMagic, RevisionHistory: true, BaseTS: hts(1), EndTS: hts(2)})
	var old struct {
		Magic string `json:"magic"`
	}
	if err := json.Unmarshal(raw, &old); err != nil || old.Magic == "DXBK1" {
		t.Fatalf("a history manifest reads as %q to a binary that knows only DXBK1", old.Magic)
	}
}

// TestHistoryFileShape: a history file that is not in the shape backup
// writes — a key's versions out of order, keys out of order, a version
// outside the backup's interval, a key outside the table — is refused
// before any of it is handed on.
func TestHistoryFileShape(t *testing.T) {
	const id = 60
	rec := func(k, v string, wall int64) kvpb.ExportRecord {
		return kvpb.ExportRecord{Key: tableKey(id, k), Value: []byte(v), Timestamp: hts(wall)}
	}
	for _, c := range []struct {
		what    string
		records []kvpb.ExportRecord
		want    string
	}{
		{"versions ascending", []kvpb.ExportRecord{rec("a", "1", 250), rec("a", "2", 260)}, "versions of"},
		{"a version twice", []kvpb.ExportRecord{rec("a", "1", 250), rec("a", "2", 250)}, "versions of"},
		{"keys descending", []kvpb.ExportRecord{rec("b", "1", 250), rec("a", "2", 260)}, "out of key order"},
		{"a key split in two", []kvpb.ExportRecord{rec("a", "1", 260), rec("b", "1", 250), rec("a", "2", 250)}, "out of key order"},
		{"a version at the base", []kvpb.ExportRecord{rec("a", "1", 200)}, "outside the backup's own interval"},
		{"a version past the end", []kvpb.ExportRecord{rec("a", "1", 301)}, "outside the backup's own interval"},
		{"another table's key", []kvpb.ExportRecord{{Key: tableKey(61, "a"), Value: []byte("1"), Timestamp: hts(250)}}, "outside the table's own span"},
	} {
		dir := testBackupDir(t, nil, backupManifest{ClusterID: "c", BaseTS: hts(200), EndTS: hts(300), RevisionHistory: true,
			Tables: []backupTable{{ID: id, Name: "t"}}}, map[uint64][]kvpb.ExportRecord{id: c.records})
		man, dataKey, err := openBackupManifest(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		// The pass restore makes before applying anything.
		err = checkBackupRecords(dir, dataKey, man)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want a refusal saying %q", c.what, err, c.want)
		}
	}
	// A plain file is not held to key order by a restore of everything
	// (it never was), and is by the merge a table restore makes.
	dir := testBackupDir(t, nil, backupManifest{ClusterID: "c", EndTS: hts(100), Tables: []backupTable{{ID: id, Name: "t"}}},
		map[uint64][]kvpb.ExportRecord{id: {rec("b", "1", 0), rec("a", "2", 0)}})
	man, _, err := openBackupManifest(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBackupRecords(dir, nil, man); err != nil {
		t.Fatalf("a restore of everything refused an unordered plain file: %v", err)
	}
	chain, err := openRestoreChain([]string{dir}, nil, hlc.Timestamp{})
	if err != nil {
		t.Fatal(err)
	}
	err = mergeTable(chain, id, func(kvpb.ExportRecord) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "out of key order") {
		t.Fatalf("the merge accepted an unordered file: %v", err)
	}
}

// TestParseBackupTS: --as-of reads what a summary prints, and an RFC 3339
// time.
func TestParseBackupTS(t *testing.T) {
	for _, ts := range []hlc.Timestamp{{WallTime: 1731628800123456789}, {WallTime: 1731628800123456789, Logical: 4}} {
		got, err := ParseBackupTS(formatBackupTS(ts))
		if err != nil || got != ts {
			t.Fatalf("%s: read back as %s (%v)", formatBackupTS(ts), got, err)
		}
	}
	got, err := ParseBackupTS("2026-11-15T10:00:00.5Z")
	if err != nil || got.WallTime != 1794736800500000000 || got.Logical != 0 {
		t.Fatalf("RFC 3339: %v %v", got, err)
	}
	for _, bad := range []string{"", "yesterday", "-5", "0", "12,x", "12,-1", "-10s"} {
		if _, err := ParseBackupTS(bad); err == nil {
			t.Errorf("%q was accepted as a timestamp", bad)
		}
	}
}

// TestTableStableAcross: a restore to a moment inside an interval takes
// a table only if its layout is the same at both ends of the interval.
func TestTableStableAcross(t *testing.T) {
	base := catalog.TableDescriptor{ID: 60, Name: "t", DatabaseID: 1, Version: 3,
		Columns: []catalog.Column{{ID: 1, Name: "k"}}, PrimaryKey: []catalog.ColumnID{1}}
	prev := &backupManifest{EndTS: hts(200), Tables: []backupTable{{ID: 60, Name: "t", Descriptor: testTableDesc(t, base)}}}
	cur := func(d catalog.TableDescriptor) (*backupManifest, backupTable) {
		bt := backupTable{ID: 60, Name: "t", Descriptor: testTableDesc(t, d)}
		return &backupManifest{BaseTS: hts(200), EndTS: hts(300), Tables: []backupTable{bt}}, bt
	}
	// A grant and a comment bump the version and change no row's layout.
	granted := base
	granted.Version, granted.Comment = 5, "c"
	granted.Privileges = map[string][]string{"ann": {"SELECT"}}
	man, bt := cur(granted)
	if err := tableStableAcross(prev, man, bt, hts(250)); err != nil {
		t.Fatalf("a grant made the table unrestorable inside the interval: %v", err)
	}
	// A new column does.
	altered := base
	altered.Columns = append([]catalog.Column{}, base.Columns...)
	altered.Columns = append(altered.Columns, catalog.Column{ID: 2, Name: "v"})
	man, bt = cur(altered)
	if err := tableStableAcross(prev, man, bt, hts(250)); err == nil || !strings.Contains(err.Error(), "definition changed") {
		t.Fatalf("an altered table: %v", err)
	}
	// A table the previous backup does not have was created in between.
	created := base
	created.ID = 61
	man = &backupManifest{BaseTS: hts(200), EndTS: hts(300)}
	if err := tableStableAcross(prev, man, backupTable{ID: 61, Name: "u", Descriptor: testTableDesc(t, created)}, hts(250)); err == nil || !strings.Contains(err.Error(), "was created between") {
		t.Fatalf("a table created in the interval: %v", err)
	}
}
