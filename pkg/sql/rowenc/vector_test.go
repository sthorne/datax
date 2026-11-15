package rowenc

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vector"
)

func vectorDesc(build string) *catalog.TableDescriptor {
	return &catalog.TableDescriptor{ID: 11, Name: "items", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "embedding", Type: types.Vector, MaxLen: 2},
		{ID: 3, Name: "note", Type: types.String},
	}, PrimaryKey: []catalog.ColumnID{1}, Indexes: []catalog.IndexDescriptor{
		{ID: 2, Name: "items_ivf", ColumnIDs: []catalog.ColumnID{2},
			Vector: &catalog.VectorIndex{Method: "ivfflat", Metric: "l2", Lists: 2, Dims: 2, Build: build}},
	}}
}

func vecRow(t *testing.T, id int64, v string) map[catalog.ColumnID]types.Datum {
	t.Helper()
	row := map[catalog.ColumnID]types.Datum{1: types.NewInt(id), 2: types.DNull, 3: types.NewString("n")}
	if v != "" {
		d, err := types.ParseVector(v)
		if err != nil {
			t.Fatal(err)
		}
		row[2] = d
	}
	return row
}

// TestVectorValueRoundTrip: a vector column's value survives the row
// encoding bit for bit, a NULL is absent, and a reader whose descriptor
// no longer has the column skips the payload.
func TestVectorValueRoundTrip(t *testing.T) {
	desc := vectorDesc("unused")
	row := vecRow(t, 1, "[0.1,-2.5]")
	raw, err := EncodeValue(desc, row)
	if err != nil {
		t.Fatal(err)
	}
	// The payload's tag is on-disk format: version, column 2, tag 18
	// (after the text-search types' 16 and 17), then the length.
	if len(raw) < 4 || raw[1] != 2 || raw[2] != 18 || tagVector != 18 || tagVector == tagTsvector || tagVector == tagTsquery {
		t.Fatalf("the vector payload is tagged 0x%02x, want 18: % x", raw[2], raw[:4])
	}
	got, err := DecodeValue(desc, raw)
	if err != nil {
		t.Fatal(err)
	}
	if d := got[2]; d.Fam != types.Vector || d.S != row[2].S || d.Text() != "[0.1,-2.5]" {
		t.Fatalf("decoded %+v", d)
	}
	rawNull, err := EncodeValue(desc, vecRow(t, 2, ""))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeValue(desc, rawNull); err != nil || !got[2].Null && got[2].Fam != types.Unknown {
		t.Fatalf("a NULL vector decoded as %+v (%v)", got[2], err)
	}
	dropped := *desc
	dropped.Columns = []catalog.Column{desc.Columns[0], desc.Columns[2]}
	dropped.Indexes = nil
	got, err = DecodeValue(&dropped, raw)
	if err != nil {
		t.Fatalf("a reader without the column: %v", err)
	}
	if _, has := got[2]; has || got[3].S != "n" {
		t.Fatalf("a reader without the column decoded %+v", got)
	}
	// A text value is coerced on the way in, as for every type.
	row[2] = types.NewString("[3,4]")
	if raw, err = EncodeValue(desc, row); err != nil {
		t.Fatal(err)
	}
	if got, _ := DecodeValue(desc, raw); got[2].Text() != "[3,4]" {
		t.Fatalf("text coerced to %s", got[2].Text())
	}
}

// TestVectorIndexEntries: a row has one entry, keyed by the list its
// vector is nearest and then its primary key, carrying the vector; the
// primary key reads back from it; a NULL vector has none; and without
// the centres there is an error, not a guess.
func TestVectorIndexEntries(t *testing.T) {
	const build = "rowenc-test-build"
	desc := vectorDesc(build)
	idx := &desc.Indexes[0]
	if _, err := IndexEntries(desc, idx, vecRow(t, 1, "[0,0]")); err == nil {
		t.Fatal("entries were computed without the centres")
	} else {
		var nl *ErrCentroidsNotLoaded
		if !errors.As(err, &nl) || nl.Index != "items_ivf" {
			t.Fatalf("error without the centres: %v", err)
		}
	}
	vector.Register(build, &vector.Centroids{Metric: vector.L2, Centers: [][]float32{{0, 0}, {10, 10}}})
	defer vector.Forget(build)

	for _, tc := range []struct {
		id   int64
		v    string
		list int
	}{{1, "[1,1]", 0}, {2, "[9,8]", 1}, {3, "[5,5]", 0}, {4, "[5.1,5]", 1}} {
		row := vecRow(t, tc.id, tc.v)
		entries, err := IndexEntries(desc, idx, row)
		if err != nil || len(entries) != 1 {
			t.Fatalf("row %d: %d entries, %v", tc.id, len(entries), err)
		}
		e := entries[0]
		if !bytes.HasPrefix(e.Key, VectorListPrefix(desc.ID, idx.ID, tc.list)) {
			t.Errorf("row %d (%s) is not under list %d: %s", tc.id, tc.v, tc.list, PrettyKey(desc, e.Key))
		}
		if string(e.Value) != row[2].S {
			t.Errorf("row %d: the entry does not carry the vector", tc.id)
		}
		pk, err := IndexEntryPrimaryKey(desc, idx, e.Key, e.Value)
		want, _ := EncodePK(desc, []types.Datum{types.NewInt(tc.id)})
		if err != nil || !pk.Equal(want) {
			t.Errorf("row %d: primary key from the entry = %s (%v), want %s", tc.id, pk, err, want)
		}
		// The same row encodes the same key: what a delete relies on.
		again, _ := IndexEntries(desc, idx, row)
		if !again[0].Key.Equal(e.Key) {
			t.Errorf("row %d: the entry key is not stable", tc.id)
		}
		// At a shadow index ID (TRUNCATE and re-shard move indexes) the
		// list is the same.
		shadow, err := IndexEntriesAt(desc, idx, 7, row)
		if err != nil || !bytes.HasPrefix(shadow[0].Key, VectorListPrefix(desc.ID, 7, tc.list)) {
			t.Errorf("row %d at a shadow ID: %v", tc.id, err)
		}
	}
	if entries, err := IndexEntries(desc, idx, vecRow(t, 9, "")); err != nil || len(entries) != 0 {
		t.Fatalf("a NULL vector has %d entries (%v)", len(entries), err)
	}
	wrong := vecRow(t, 9, "[1,2]")
	wrong[2], _ = types.ParseVector("[1,2,3]")
	if _, err := IndexEntries(desc, idx, wrong); err == nil {
		t.Fatal("a vector of the wrong dimension was filed")
	}
	// Lists are contiguous, in list order, and disjoint from the centres'
	// keys, which sit under the table but under no index.
	l0, l1 := VectorListPrefix(desc.ID, idx.ID, 0), VectorListPrefix(desc.ID, idx.ID, 1)
	if l0.Compare(l1) >= 0 || !l0.PrefixEnd().Equal(l1) {
		t.Error("consecutive lists are not adjacent key spans")
	}
	lo, hi := keys.TableIndexSpan(desc.ID, idx.ID)
	ck := keys.VectorCentroidKey(desc.ID, build, 1)
	if ck.Compare(lo) >= 0 && ck.Compare(hi) < 0 {
		t.Error("a centre's key lies inside the index's span")
	}
	tlo, thi := keys.TableDataSpan(desc.ID)
	if ck.Compare(tlo) < 0 || ck.Compare(thi) >= 0 {
		t.Error("a centre's key lies outside the table's span")
	}
	if got := PrettyKey(desc, VectorListPrefix(desc.ID, idx.ID, 1)); got != "/table/items/items_ivf/list 1" {
		t.Errorf("pretty key: %s", got)
	}
}
