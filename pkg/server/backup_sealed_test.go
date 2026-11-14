package server

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// writeSealedBackup lays down a sealed backup directory by hand, the way
// RunBackupWith does: a wrapped data key and sealed manifest in
// BACKUP.json, and each table's records as a sealed stream.
func writeSealedBackup(t *testing.T, backupKey []byte, man backupManifest, records map[uint64][]kvpb.ExportRecord) (dir string, dataKey []byte) {
	t.Helper()
	dir = t.TempDir()
	dataKey = bytes.Repeat([]byte{0x5a}, enc.KeyLen)
	man.Magic = backupManifestMagic
	for i := range man.Tables {
		man.Tables[i].File = backupDataFile(man.Tables[i].ID)
		f, err := os.Create(filepath.Join(dir, man.Tables[i].File))
		if err != nil {
			t.Fatal(err)
		}
		w, err := enc.NewStreamWriter(f, dataKey, man.Tables[i].File)
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range records[man.Tables[i].ID] {
			if err := writeBackupRecord(w, rec); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(man)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealBackupManifest(backupKey, dataKey, raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, backupManifestName), sealed, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, dataKey
}

// TestSealedBackupManifest (issue #291): a sealed manifest opens with its
// key and only as a sealed one, keeps nothing of itself in the clear, and
// is held to every check a plaintext manifest is — holding the key says
// who wrote a backup, not that it may write any key in the cluster.
func TestSealedBackupManifest(t *testing.T) {
	key := bytes.Repeat([]byte{1}, enc.KeyLen)
	const tableID = 107
	man := backupManifest{
		ClusterID: "cluster-id-in-the-manifest",
		Tables:    []backupTable{{ID: tableID, Name: "name_in_the_manifest", Descriptor: json.RawMessage(`{}`)}},
		Roles:     []backupKV{{Key: keys.RoleKey("root"), Value: []byte("r")}},
	}
	own := kvpb.ExportRecord{Key: keys.TableIndexPrefix(tableID, 1), Value: []byte("value-in-the-data-file")}
	dir, _ := writeSealedBackup(t, key, man, map[uint64][]kvpb.ExportRecord{tableID: {own, {Key: keys.TableIndexPrefix(tableID, 2), Deleted: true}}})

	got, dataKey, err := openBackupManifest(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClusterID != man.ClusterID || len(got.Tables) != 1 || got.Tables[0].Name != "name_in_the_manifest" || len(dataKey) != enc.KeyLen {
		t.Fatalf("opened manifest: %+v", got)
	}
	var seen []kvpb.ExportRecord
	if err := readCheckedBackupRecords(filepath.Join(dir, backupDataFile(tableID)), dataKey, got.Tables[0], func(rec kvpb.ExportRecord) error {
		seen = append(seen, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || string(seen[0].Value) != "value-in-the-data-file" || !seen[1].Deleted {
		t.Fatalf("records read back: %+v", seen)
	}
	for _, name := range []string{backupManifestName, backupDataFile(tableID)} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		for _, clear := range []string{"cluster-id-in-the-manifest", "name_in_the_manifest", "value-in-the-data-file", string(dataKey), string(key)} {
			if bytes.Contains(raw, []byte(clear)) {
				t.Fatalf("%s holds %q in the clear", name, clear)
			}
		}
	}
	if sealed, err := backupIsSealed(dir); err != nil || !sealed {
		t.Fatalf("backupIsSealed: %v %v", sealed, err)
	}

	if _, err := readBackupManifest(dir); err == nil || !strings.Contains(err.Error(), "is a sealed backup and no key was given") {
		t.Fatalf("sealed manifest read without a key: %v", err)
	}
	if _, _, err := openBackupManifest(dir, bytes.Repeat([]byte{2}, enc.KeyLen)); err == nil || !strings.Contains(err.Error(), "does not open this backup") {
		t.Fatalf("sealed manifest opened with another key: %v", err)
	}
	// The sealed file read as a plaintext one.
	if err := readBackupRecordsUnchecked(filepath.Join(dir, backupDataFile(tableID)), nil, func(kvpb.ExportRecord) error { return nil }); err == nil {
		t.Fatal("a sealed data file was read as plaintext records")
	}

	// The checks a plaintext manifest gets: a key outside the spans a
	// backup collects, a file name of the manifest's choosing, a record
	// outside its table's span.
	planted, _ := writeSealedBackup(t, key, backupManifest{Roles: []backupKV{{Key: keys.AuthSecretKey(), Value: []byte("x")}}}, nil)
	if _, _, err := openBackupManifest(planted, key); err == nil || !strings.Contains(err.Error(), "which a datax backup never collects there") {
		t.Fatalf("sealed manifest carrying the authentication secret: %v", err)
	}
	raw, err := json.Marshal(backupManifest{Magic: backupManifestMagic, Tables: []backupTable{{ID: 7, File: "../elsewhere"}}})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealBackupManifest(key, bytes.Repeat([]byte{9}, enc.KeyLen), raw)
	if err != nil {
		t.Fatal(err)
	}
	named := t.TempDir()
	if err := os.WriteFile(filepath.Join(named, backupManifestName), sealed, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openBackupManifest(named, key); err == nil || !strings.Contains(err.Error(), "which a datax backup keeps in") {
		t.Fatalf("sealed manifest naming a file of its own: %v", err)
	}
	stray := kvpb.ExportRecord{Key: keys.TableIndexPrefix(tableID+1, 1), Value: []byte("v")}
	outside, dk := writeSealedBackup(t, key, man, map[uint64][]kvpb.ExportRecord{tableID: {own, stray}})
	if err := checkBackupRecords(outside, dk, &man); err == nil || !strings.Contains(err.Error(), "outside the table's own span") {
		t.Fatalf("sealed data file carrying another table's key: %v", err)
	}
}

// TestBackupKeyTransport (issue #291): a backup key is taken over mutual
// TLS, or without TLS from the node's own machine, and from nowhere else;
// a request without one is not this gate's business.
func TestBackupKeyTransport(t *testing.T) {
	for _, c := range []struct {
		carries, secure, loopback, ok bool
	}{
		{false, false, false, true},
		{true, true, false, true},
		{true, true, true, true},
		{true, false, true, true},
		{true, false, false, false},
	} {
		err := backupKeyTransportOK(c.carries, c.secure, c.loopback)
		if (err == nil) != c.ok {
			t.Fatalf("%+v: %v", c, err)
		}
		if err != nil && !strings.Contains(err.Error(), "accepted only from the node's own machine") {
			t.Fatalf("%+v refused without saying what to do: %v", c, err)
		}
	}
}

// TestRestoreKeysPairByPosition (issue #291).
func TestRestoreKeysPairByPosition(t *testing.T) {
	a, b := bytes.Repeat([]byte{1}, enc.KeyLen), bytes.Repeat([]byte{2}, enc.KeyLen)
	srcs := []string{"full", "inc1", "inc2"}
	for _, c := range []struct {
		what string
		keys [][]byte
		want [][]byte
		err  string
	}{
		{"no keys", nil, [][]byte{nil, nil, nil}, ""},
		{"one key for the chain", [][]byte{a}, [][]byte{a, a, a}, ""},
		{"a key each", [][]byte{a, b, a}, [][]byte{a, b, a}, ""},
		{"an empty entry", [][]byte{nil, b, {}}, [][]byte{nil, b, nil}, ""},
		{"two keys for three", [][]byte{a, b}, nil, "2 backup keys for 3 backup directories"},
		{"a short key", [][]byte{a[:5]}, nil, "must be 32 bytes"},
	} {
		got, err := restoreKeys(srcs, c.keys)
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Fatalf("%s: %v", c.what, err)
			}
			continue
		}
		if err != nil || len(got) != len(c.want) {
			t.Fatalf("%s: %v %v", c.what, got, err)
		}
		for i := range got {
			if !bytes.Equal(got[i], c.want[i]) || (got[i] == nil) != (c.want[i] == nil) {
				t.Fatalf("%s: directory %d paired with %x", c.what, i, got[i])
			}
		}
	}
}
