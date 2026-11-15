package enc

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// TestClusterKeyCheck (issue #310): the recorded check value says
// whether a key is the cluster key a ref names, and nothing less: another
// key, another version or another id does not pass.
func TestClusterKeyCheck(t *testing.T) {
	k, _ := NewClusterKey()
	other, _ := NewClusterKey()
	ref := ClusterKeyRef{ID: "00112233aabbccdd", Version: 3}
	check := ClusterKeyCheck(k, ref)
	if !CheckClusterKey(k, ref, check) {
		t.Fatal("a key does not pass its own check")
	}
	if CheckClusterKey(other, ref, check) {
		t.Fatal("another key passes the check")
	}
	if CheckClusterKey(k, ClusterKeyRef{ID: ref.ID, Version: 4}, check) {
		t.Fatal("the key passes as another version")
	}
	if CheckClusterKey(k, ClusterKeyRef{ID: "ffffffffffffffff", Version: 3}, check) {
		t.Fatal("the key passes as another id")
	}
	if CheckClusterKey(k, ref, "") || CheckClusterKey(k[:16], ref, check) {
		t.Fatal("an empty check or a short key passes")
	}
	if bytes.Contains([]byte(check), k) {
		t.Fatal("the check contains the key")
	}
	if got, err := ParseClusterKeyRef(ref.String()); err != nil || got != ref {
		t.Fatalf("ParseClusterKeyRef(%q) = %v, %v", ref.String(), got, err)
	}
	for _, bad := range []string{"", "abc", "abc/v", "abc/v0", "/v1", "abc/vx"} {
		if _, err := ParseClusterKeyRef(bad); err == nil {
			t.Fatalf("ParseClusterKeyRef(%q) accepted", bad)
		}
	}
}

// TestClusterKeyFile: a node's copies round-trip through its root, the
// file never holds a key in the clear, an edited entry or one bound to
// another node is refused, and another root opens nothing.
func TestClusterKeyFile(t *testing.T) {
	root, _ := fileRootIn(t, 0x11)
	stranger, _ := fileRootIn(t, 0x22)
	dir := t.TempDir()
	b := Binding{ClusterID: "c1", NodeID: 2}
	ctx := WithBinding(bg, b)
	k1, _ := NewClusterKey()
	k2, _ := NewClusterKey()
	r1, r2 := ClusterKeyRef{ID: "aa", Version: 1}, ClusterKeyRef{ID: "aa", Version: 2}
	e1, err := WrapClusterKey(ctx, root, r1, k1)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := WrapClusterKey(ctx, root, r2, k2)
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.SetImported(k2, "other-cluster"); err != nil {
		t.Fatal(err)
	}
	if err := WriteClusterKeyFile(vfs.Default, dir, []ClusterKeyEntry{e2, e1}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, ClusterKeyFileName))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, k1) || bytes.Contains(raw, k2) {
		t.Fatal("the cluster key file holds a key in the clear")
	}
	got, err := ReadClusterKeyFile(vfs.Default, dir)
	if err != nil || len(got) != 2 || got[0].ClusterKeyRef != r1 {
		t.Fatalf("read back %v, %v (want sorted by version)", got, err)
	}
	for i, want := range [][]byte{k1, k2} {
		k, err := UnwrapClusterKey(ctx, []Root{root}, &got[i])
		if err != nil || !bytes.Equal(k, want) {
			t.Fatalf("entry %d: %v", i, err)
		}
	}
	if !got[1].Imported || got[1].FromCluster != "other-cluster" {
		t.Fatalf("imported entry read back as %+v", got[1])
	}
	if _, err := UnwrapClusterKey(ctx, []Root{stranger}, &got[0]); err == nil {
		t.Fatal("another root opened the entry")
	}
	if _, err := UnwrapClusterKey(WithBinding(bg, Binding{ClusterID: "c1", NodeID: 3}), []Root{root}, &got[0]); err == nil {
		t.Fatal("an entry bound to n2 opened as n3's")
	}
	for name, edit := range map[string]func(e *ClusterKeyEntry){
		"version":  func(e *ClusterKeyEntry) { e.Version = 7 },
		"id":       func(e *ClusterKeyEntry) { e.ID = "bb" },
		"imported": func(e *ClusterKeyEntry) { e.Imported = true },
		"from":     func(e *ClusterKeyEntry) { e.FromCluster = "x" },
		"binding":  func(e *ClusterKeyEntry) { e.Binding = nil },
	} {
		e := got[0]
		edit(&e)
		if _, err := UnwrapClusterKey(ctx, []Root{root}, &e); err == nil {
			t.Fatalf("an entry with its %s edited opened", name)
		}
	}
	if !ClusterKeyFileExists(vfs.Default, dir) {
		t.Fatal("ClusterKeyFileExists")
	}
	if err := RemoveClusterKeyFile(vfs.Default, dir); err != nil || ClusterKeyFileExists(vfs.Default, dir) {
		t.Fatalf("RemoveClusterKeyFile: %v", err)
	}
	if err := RemoveClusterKeyFile(vfs.Default, dir); err != nil {
		t.Fatalf("removing an absent file: %v", err)
	}
	if _, err := ReadClusterKeyFile(vfs.Default, dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reading an absent file: %v", err)
	}
}

// TestClusterKeyTransport: keys sealed for one exchange open only for it,
// and only under the binding they were sealed with.
func TestClusterKeyTransport(t *testing.T) {
	x, _ := NewPeerExchange()
	k, _ := NewClusterKey()
	ks := []TransportKey{{ClusterKeyRef: ClusterKeyRef{ID: "aa", Version: 1}, Key: append([]byte(nil), k...)}}
	pub, sealed, err := SealClusterKeys(x.Public(), []byte("b1"), ks)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, k) {
		t.Fatal("the sealed bundle holds the key in the clear")
	}
	got, err := x.OpenClusterKeys(pub, sealed, []byte("b1"))
	if err != nil || len(got) != 1 || !bytes.Equal(got[0].Key, k) || got[0].ClusterKeyRef != ks[0].ClusterKeyRef {
		t.Fatalf("open: %v %v", got, err)
	}
	if _, err := x.OpenClusterKeys(pub, sealed, []byte("b2")); err == nil {
		t.Fatal("opened under another binding")
	}
	y, _ := NewPeerExchange()
	if _, err := y.OpenClusterKeys(pub, sealed, []byte("b1")); err == nil {
		t.Fatal("another exchange opened the bundle")
	}
	// A root key handed over for peer unseal is sealed under another
	// label: the two never open as each other.
	if _, err := Unseal(peerUnsealMagic, make([]byte, KeyLen), sealed); err == nil {
		t.Fatal("unexpected")
	}
}

// TestClusterKeyExport: an export opens under the root it was wrapped
// under, marks every key as the source cluster's, and holds no key in
// the clear.
func TestClusterKeyExport(t *testing.T) {
	root, _ := fileRootIn(t, 0x33)
	stranger, _ := fileRootIn(t, 0x44)
	k1, _ := NewClusterKey()
	k2, _ := NewClusterKey()
	ks := []TransportKey{
		{ClusterKeyRef: ClusterKeyRef{ID: "aa", Version: 2}, Key: k1},
		{ClusterKeyRef: ClusterKeyRef{ID: "bb", Version: 1}, Imported: true, FromCluster: "third", Key: k2},
	}
	x, err := ExportClusterKeys(bg, root, "src", ks[0].ClusterKeyRef, ks)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(x)
	if bytes.Contains(raw, k1) || bytes.Contains(raw, k2) {
		t.Fatal("the export holds a key in the clear")
	}
	back, err := ParseClusterKeyExport(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := back.Open(bg, []Root{root})
	if err != nil || len(got) != 2 {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(got[0].Key, k1) || !got[0].Imported || got[0].FromCluster != "src" {
		t.Fatalf("first key: %+v", got[0])
	}
	if !bytes.Equal(got[1].Key, k2) || got[1].FromCluster != "third" {
		t.Fatalf("second key: %+v", got[1])
	}
	if _, err := back.Open(bg, []Root{stranger}); err == nil {
		t.Fatal("another root opened the export")
	}
	if _, err := ParseClusterKeyExport([]byte(`{"magic":"nope"}`)); err == nil {
		t.Fatal("a file that is not an export parsed")
	}
}
