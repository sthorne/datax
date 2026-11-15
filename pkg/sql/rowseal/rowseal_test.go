package rowseal

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore/keycache"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/tsearch"
	"github.com/sthorne/datax/pkg/sql/types"
)

func newAEAD(t *testing.T) cipher.AEAD {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	b, err := aes.NewCipher(k)
	if err != nil {
		t.Fatal(err)
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// fakeKeys is a key store of named versions.
type fakeKeys struct {
	keys map[int64]map[int]cipher.AEAD
	cur  map[int64]int
	err  error
}

func (f *fakeKeys) Current(_ context.Context, id int64) (keycache.Key, error) {
	return f.Version(context.Background(), id, f.cur[id])
}

func (f *fakeKeys) Version(_ context.Context, id int64, v int) (keycache.Key, error) {
	if f.err != nil {
		return keycache.Key{}, f.err
	}
	a, ok := f.keys[id][v]
	if !ok {
		return keycache.Key{}, keycache.ErrNoVersion
	}
	return keycache.Key{ID: id, Version: v, AEAD: a}, nil
}

func (f *fakeKeys) Invalidate(int64) {}

// everyType is a table with a column of every type the row encoding
// knows, and a row with a value in each.
func everyType() (*catalog.TableDescriptor, map[catalog.ColumnID]types.Datum) {
	fams := []types.Family{types.Int, types.Float, types.String, types.Bool, types.Timestamp, types.Date,
		types.Bytes, types.Uuid, types.Decimal, types.Jsonb, types.IntervalFam, types.Time, types.Enum,
		types.Tsvector, types.Tsquery, types.Vector, types.ArrayOf(types.Int), types.ArrayOf(types.String)}
	d := &catalog.TableDescriptor{ID: 60, Name: "every", Columns: []catalog.Column{{ID: 1, Name: "id", Type: types.Int, NotNull: true}}, PrimaryKey: []catalog.ColumnID{1}}
	row := map[catalog.ColumnID]types.Datum{1: types.NewInt(1)}
	tv, _ := tsearch.ParseVector("a:1 b:2")
	tq, _ := tsearch.ParseQuery("a & b")
	vals := map[types.Family]types.Datum{
		types.Int: types.NewInt(-5), types.Float: types.NewFloat(2.5), types.String: types.NewString("secret text"),
		types.Bool: types.NewBool(true), types.Timestamp: types.NewTimestamp(1e18), types.Date: types.NewDate(20000),
		types.Bytes: types.NewBytes([]byte{0xD5, 0, 1}), types.Uuid: types.NewUUID([16]byte{0xD5, 1}),
		types.Decimal: types.NewDecimal("1.25"), types.Jsonb: types.NewJsonb(`{"a":1}`),
		types.IntervalFam: types.NewInterval(types.Interval{Months: 1, Days: 2, Nanos: 3}), types.Time: types.NewTime(5e9),
		types.Enum: types.NewEnum(2, "two"), types.Tsvector: types.NewTsvector(tv), types.Tsquery: types.NewTsquery(tq),
		types.Vector:                types.NewVector([]float32{1, 2}),
		types.ArrayOf(types.Int):    types.NewArray(types.Int, []types.Datum{types.NewInt(1), types.DNull}),
		types.ArrayOf(types.String): types.NewArray(types.String, []types.Datum{types.NewString("x")}),
	}
	for i, f := range fams {
		id := catalog.ColumnID(i + 2)
		col := catalog.Column{ID: id, Name: f.String(), Type: f}
		if f == types.Vector {
			col.MaxLen = 2
		}
		d.Columns = append(d.Columns, col)
		row[id] = vals[f]
	}
	return d, row
}

// TestMarkerNeverStartsAPlainValue checks the claim the sealed form rests
// on: no output of rowenc.EncodeValue begins with the marker, whatever
// the columns — every one begins with the encoding's version byte, which
// is not the marker — so a stored value says by its first byte which form
// it is in.
func TestMarkerNeverStartsAPlainValue(t *testing.T) {
	if rowenc.ValueVersion == Marker {
		t.Fatal("the row encoding's version byte is the sealed marker")
	}
	d, full := everyType()
	// Each column alone, every column, and no column at all.
	rows := []map[catalog.ColumnID]types.Datum{{1: full[1]}, full}
	for id, v := range full {
		if id != 1 {
			rows = append(rows, map[catalog.ColumnID]types.Datum{1: full[1], id: v})
		}
	}
	for _, r := range rows {
		raw, err := rowenc.EncodeValue(d, r)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) == 0 || raw[0] == Marker || IsSealed(raw) {
			t.Fatalf("EncodeValue output begins with the marker: % x", raw)
		}
	}
	// And DecodeValue refuses a sealed value rather than misreading it.
	sealed, err := Seal(newAEAD(t), 1, nil, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rowenc.DecodeValue(d, sealed); err == nil {
		t.Fatal("DecodeValue decoded a sealed value")
	}
}

// TestSealFormat pins the layout: marker, uvarint version, 12-byte nonce,
// ciphertext and 16-byte tag; 30 bytes of overhead at version < 128.
func TestSealFormat(t *testing.T) {
	a := newAEAD(t)
	plain := []byte("row value")
	for _, v := range []int{1, 127, 128, 300} {
		s, err := Seal(a, v, []byte("ad"), plain)
		if err != nil {
			t.Fatal(err)
		}
		if s[0] != Marker || len(s) != len(plain)+Overhead(v) {
			t.Fatalf("v%d: % x", v, s)
		}
		if got, err := Version(s); err != nil || got != v {
			t.Fatalf("version of v%d: %d %v", v, got, err)
		}
		p, err := Open(a, []byte("ad"), s)
		if err != nil || !bytes.Equal(p, plain) {
			t.Fatalf("open v%d: %q %v", v, p, err)
		}
		if bytes.Contains(s, plain) {
			t.Fatal("plaintext visible in the sealed value")
		}
	}
	if Overhead(1) != 30 {
		t.Fatalf("overhead %d", Overhead(1))
	}
	s1, _ := Seal(a, 1, nil, plain)
	s2, _ := Seal(a, 1, nil, plain)
	if bytes.Equal(s1, s2) {
		t.Fatal("two seals of one value are equal: the nonce is not fresh")
	}
	for _, bad := range [][]byte{nil, {Marker}, {Marker, 0}, append([]byte{Marker, 1}, make([]byte, 27)...), {1, 2, 3}} {
		if _, err := Open(a, nil, bad); err == nil {
			t.Fatalf("malformed % x opened", bad)
		}
	}
	tampered := append([]byte(nil), s1...)
	tampered[len(tampered)-1] ^= 1
	if _, err := Open(a, nil, tampered); err == nil {
		t.Fatal("a tampered value opened")
	}
}

// TestBoundaryBinding: values open where they were sealed and nowhere
// else — not at another row, in another table, under another key or
// another version — and a sealed table refuses plain values; the errors
// say which table and why.
func TestBoundaryBinding(t *testing.T) {
	ctx := context.Background()
	fk := &fakeKeys{keys: map[int64]map[int]cipher.AEAD{
		11: {1: newAEAD(t), 2: newAEAD(t)},
		12: {1: newAEAD(t)},
	}, cur: map[int64]int{11: 1, 12: 1}}
	b := New(fk, nil, nil)
	mk := func(id uint64, key int64, seal uint64) *catalog.TableDescriptor {
		return &catalog.TableDescriptor{ID: id, Name: "t" + string(rune('0'+id%10)), Columns: []catalog.Column{{ID: 1, Name: "id", Type: types.Int}},
			PrimaryKey: []catalog.ColumnID{1}, EncryptionKey: &catalog.TableEncryptionKey{KeyID: key, SealID: seal}}
	}
	t1, t2 := mk(51, 11, 51), mk(52, 12, 52)
	k1, _ := rowenc.EncodePK(t1, []types.Datum{types.NewInt(1)})
	k2, _ := rowenc.EncodePK(t1, []types.Datum{types.NewInt(2)})
	plain := []byte{rowenc.ValueVersion, 2, 3}

	s, err := b.SealValue(ctx, t1, k1, plain)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := b.OpenValue(ctx, t1, k1, s); err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("round trip: %v %v", got, err)
	}
	corrupt := func(what string, err error) {
		t.Helper()
		var ce *CorruptError
		if !errors.As(err, &ce) {
			t.Fatalf("%s: %v, want a corrupt value", what, err)
		}
	}
	// Another row of the same table.
	_, err = b.OpenValue(ctx, t1, k2, s)
	corrupt("moved to another row", err)
	// Another table with the same key: its id differs.
	t1b := mk(53, 11, 53)
	k1b, _ := rowenc.EncodePK(t1b, []types.Datum{types.NewInt(1)})
	_, err = b.OpenValue(ctx, t1b, k1b, s)
	corrupt("moved to another table", err)
	// Under another key.
	k1t2, _ := rowenc.EncodePK(t2, []types.Datum{types.NewInt(1)})
	_, err = b.OpenValue(ctx, t2, k1t2, s)
	corrupt("moved to a table under another key", err)
	// A restored copy: a new table id, the same seal id and key, opens.
	restored := mk(99, 11, 51)
	kr, _ := rowenc.EncodePK(restored, []types.Datum{types.NewInt(1)})
	if _, err := b.OpenValue(ctx, restored, kr, s); err != nil {
		t.Fatalf("a restored copy's row: %v", err)
	}
	// A plain value in a sealed table.
	_, err = b.OpenValue(ctx, t1, k1, plain)
	corrupt("plain value", err)

	// Rotation: new writes under v2, v1 still opens.
	fk.cur[11] = 2
	s2, err := b.SealValue(ctx, t1, k2, plain)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := Version(s2); v != 2 {
		t.Fatalf("after rotation sealed under v%d", v)
	}
	if _, err := b.OpenValue(ctx, t1, k1, s); err != nil {
		t.Fatalf("v1 after rotation: %v", err)
	}
	// Presented as another version: the header says v1, swap in v2.
	forged := append([]byte{Marker, 2}, s[2:]...)
	_, err = b.OpenValue(ctx, t1, k1, forged)
	corrupt("presented as v2", err)

	// A destroyed key: the statement fails, naming the table.
	fk.err = keycache.ErrDestroyed
	_, err = b.OpenValue(ctx, t1, k1, s)
	var ku *KeyUnavailableError
	if !errors.As(err, &ku) || ku.Table != t1.Name || !errors.Is(err, keycache.ErrDestroyed) {
		t.Fatalf("destroyed key: %v", err)
	}
	if _, err := b.SealValue(ctx, t1, k1, plain); !errors.As(err, &ku) {
		t.Fatalf("writing under a destroyed key: %v", err)
	}
	// A table that is not sealed passes through; a nil boundary refuses
	// a sealed one.
	plainDesc := &catalog.TableDescriptor{ID: 54, Name: "p"}
	pk := keys.Key(append(keys.TableDataPrefix(54), 1))
	if got, err := (*Boundary)(nil).OpenValue(ctx, plainDesc, pk, plain); err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("unsealed through a nil boundary: %v", err)
	}
	if _, err := (*Boundary)(nil).OpenValue(ctx, t1, k1, s); !errors.As(err, &ku) {
		t.Fatalf("sealed through a nil boundary: %v", err)
	}
}

// TestPlaintextColumns: primary-key and indexed columns, nothing else.
func TestPlaintextColumns(t *testing.T) {
	d := &catalog.TableDescriptor{
		Columns:    []catalog.Column{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}},
		PrimaryKey: []catalog.ColumnID{1},
		Indexes:    []catalog.IndexDescriptor{{ID: 2, ColumnIDs: []catalog.ColumnID{3}}},
	}
	got := PlaintextColumns(d)
	if !got[1] || got[2] || !got[3] || got[4] || len(got) != 2 {
		t.Fatalf("%v", got)
	}
}
