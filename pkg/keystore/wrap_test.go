package keystore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sthorne/datax/pkg/storage/enc"
)

// TestMaterialIsBoundToItsKey (issue #310): a version's sealed material
// opens only as that version of that key, under the cluster key it was
// sealed with. Presented under another id, name, version or kind, or
// opened with another cluster key, it fails — it is never a wrong key.
func TestMaterialIsBoundToItsKey(t *testing.T) {
	ck, _ := enc.NewClusterKey()
	other, _ := enc.NewClusterKey()
	material := bytes.Repeat([]byte{0x7e}, 32)
	sealed, err := WrapMaterial(ck, 42, "billing", 3, KindWrapping, material)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, material) {
		t.Fatal("sealed material holds the key in the clear")
	}
	got, err := UnwrapMaterial(ck, 42, "billing", 3, KindWrapping, sealed)
	if err != nil || !bytes.Equal(got, material) {
		t.Fatalf("round trip: %v", err)
	}
	for name, open := range map[string]func() ([]byte, error){
		"another id":          func() ([]byte, error) { return UnwrapMaterial(ck, 43, "billing", 3, KindWrapping, sealed) },
		"another name":        func() ([]byte, error) { return UnwrapMaterial(ck, 42, "billing2", 3, KindWrapping, sealed) },
		"another version":     func() ([]byte, error) { return UnwrapMaterial(ck, 42, "billing", 2, KindWrapping, sealed) },
		"another kind":        func() ([]byte, error) { return UnwrapMaterial(ck, 42, "billing", 3, KindTenant, sealed) },
		"another cluster key": func() ([]byte, error) { return UnwrapMaterial(other, 42, "billing", 3, KindWrapping, sealed) },
	} {
		if _, err := open(); !errors.Is(err, ErrMaterialMismatch) {
			t.Fatalf("%s: %v, want ErrMaterialMismatch", name, err)
		}
	}
	// The length prefixes keep fields apart: "a"+"bc" is not "ab"+"c".
	if !bytes.Equal(materialAD(1, "ab", 1, "c"), materialAD(1, "ab", 1, "c")) ||
		bytes.Equal(materialAD(1, "a", 1, "bc"), materialAD(1, "ab", 1, "c")) {
		t.Fatal("associated data is ambiguous")
	}
	flipped := append([]byte(nil), sealed...)
	flipped[len(flipped)-1] ^= 1
	if _, err := UnwrapMaterial(ck, 42, "billing", 3, KindWrapping, flipped); err == nil {
		t.Fatal("altered material opened")
	}
	if _, err := UnwrapMaterial(ck, 42, "billing", 3, KindWrapping, sealed[:10]); err == nil {
		t.Fatal("truncated material opened")
	}
	if _, err := WrapMaterial(ck[:16], 42, "billing", 3, KindWrapping, material); err == nil {
		t.Fatal("a short cluster key was taken")
	}
}

// TestRewrapVersions: a rotation of the cluster key reseals every version
// of a key under the new cluster key; the material is unchanged, the old
// cluster key no longer opens it, and a version that does not open under
// the old key stops the rewrap rather than being carried over.
func TestRewrapVersions(t *testing.T) {
	from, _ := enc.NewClusterKey()
	to, _ := enc.NewClusterKey()
	var vs []sealedVersion
	var plain [][]byte
	for v := 1; v <= 3; v++ {
		m, _ := enc.NewStoreKey()
		s, err := WrapMaterial(from, 7, "t", v, KindTable, m)
		if err != nil {
			t.Fatal(err)
		}
		vs = append(vs, sealedVersion{Version: v, Sealed: s, Created: int64(v)})
		plain = append(plain, m)
	}
	out, err := rewrapVersions(from, to, 7, "t", KindTable, vs)
	if err != nil {
		t.Fatal(err)
	}
	for i, sv := range out {
		if sv.Version != i+1 || sv.Created != int64(i+1) {
			t.Fatalf("version %d came back as %+v", i+1, sv)
		}
		got, err := UnwrapMaterial(to, 7, "t", sv.Version, KindTable, sv.Sealed)
		if err != nil || !bytes.Equal(got, plain[i]) {
			t.Fatalf("version %d under the new cluster key: %v", sv.Version, err)
		}
		if _, err := UnwrapMaterial(from, 7, "t", sv.Version, KindTable, sv.Sealed); err == nil {
			t.Fatalf("version %d still opens under the old cluster key", sv.Version)
		}
	}
	// One version sealed under something else: the rewrap stops.
	stray, _ := enc.NewClusterKey()
	bad, _ := WrapMaterial(stray, 7, "t", 4, KindTable, plain[0])
	if _, err := rewrapVersions(from, to, 7, "t", KindTable, append(vs, sealedVersion{Version: 4, Sealed: bad})); err == nil {
		t.Fatal("a version the old cluster key does not open was rewrapped")
	}
	raw, err := encodeVersions(out)
	if err != nil {
		t.Fatal(err)
	}
	back, err := decodeVersions(raw)
	if err != nil || len(back) != 3 || !bytes.Equal(back[2].Sealed, out[2].Sealed) {
		t.Fatalf("versions round trip: %v", err)
	}
	if got, err := decodeVersions(nil); err != nil || got != nil {
		t.Fatalf("no material: %v %v", got, err)
	}
}
