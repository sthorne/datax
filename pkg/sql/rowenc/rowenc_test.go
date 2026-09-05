package rowenc

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

func testDesc() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID:   42,
		Name: "t",
		Columns: []catalog.Column{
			{ID: 1, Name: "a", Type: types.Int},
			{ID: 2, Name: "b", Type: types.String},
			{ID: 3, Name: "i", Type: types.Int},
			{ID: 4, Name: "f", Type: types.Float},
			{ID: 5, Name: "s", Type: types.String},
			{ID: 6, Name: "bo", Type: types.Bool},
		},
		PrimaryKey: []catalog.ColumnID{1, 2},
	}
}

func randDatum(rng *rand.Rand, fam types.Family, allowNull bool) types.Datum {
	if allowNull && rng.Intn(4) == 0 {
		return types.DNull
	}
	switch fam {
	case types.Int:
		return types.NewInt(rng.Int63() - rng.Int63())
	case types.Float:
		return types.NewFloat(rng.NormFloat64() * 1e6)
	case types.String:
		b := make([]byte, rng.Intn(40))
		rng.Read(b)
		return types.NewString(string(b))
	case types.Bool:
		return types.NewBool(rng.Intn(2) == 0)
	}
	panic("unreachable")
}

// TestValueRoundTrip: random rows across every type (with NULLs) survive
// encode/decode exactly.
func TestValueRoundTrip(t *testing.T) {
	desc := testDesc()
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 500; iter++ {
		row := map[catalog.ColumnID]types.Datum{
			1: randDatum(rng, types.Int, false),
			2: randDatum(rng, types.String, false),
			3: randDatum(rng, types.Int, true),
			4: randDatum(rng, types.Float, true),
			5: randDatum(rng, types.String, true),
			6: randDatum(rng, types.Bool, true),
		}
		raw, err := EncodeValue(desc, row)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeValue(desc, raw)
		if err != nil {
			t.Fatal(err)
		}
		for _, col := range desc.Columns {
			if desc.IsPKCol(col.ID) {
				continue
			}
			want := row[col.ID]
			gotD, present := got[col.ID]
			if want.Null {
				if present {
					t.Fatalf("iter %d col %d: NULL came back as %+v", iter, col.ID, gotD)
				}
				continue
			}
			if !present {
				t.Fatalf("iter %d col %d: value lost", iter, col.ID)
			}
			if !reflect.DeepEqual(gotD, want) {
				t.Fatalf("iter %d col %d: %+v != %+v", iter, col.ID, gotD, want)
			}
		}
	}
}

// TestUnknownColumnSkipped: bytes for a column the descriptor no longer
// knows decode cleanly to nothing (the lazy-DROP-COLUMN contract).
func TestUnknownColumnSkipped(t *testing.T) {
	desc := testDesc()
	row := map[catalog.ColumnID]types.Datum{
		3: types.NewInt(7),
		5: types.NewString("keep"),
		6: types.NewBool(true),
	}
	raw, err := EncodeValue(desc, row)
	if err != nil {
		t.Fatal(err)
	}
	// A narrower descriptor: columns 5 and 6 dropped.
	narrow := &catalog.TableDescriptor{
		ID: 42, Name: "t",
		Columns: []catalog.Column{
			{ID: 1, Name: "a", Type: types.Int},
			{ID: 2, Name: "b", Type: types.String},
			{ID: 3, Name: "i", Type: types.Int},
		},
		PrimaryKey: []catalog.ColumnID{1, 2},
	}
	got, err := DecodeValue(narrow, raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[3], types.NewInt(7)) {
		t.Fatalf("unexpected decode with dropped columns: %+v", got)
	}
}

// TestCompositePKOrdering: byte order of encoded keys matches tuple order
// of the primary key values.
func TestCompositePKOrdering(t *testing.T) {
	desc := testDesc()
	rng := rand.New(rand.NewSource(2))
	type pair struct {
		vals []types.Datum
		key  []byte
	}
	var pairs []pair
	for i := 0; i < 300; i++ {
		vals := []types.Datum{randDatum(rng, types.Int, false), randDatum(rng, types.String, false)}
		k, err := EncodePK(desc, vals)
		if err != nil {
			t.Fatal(err)
		}
		pairs = append(pairs, pair{vals, k})

		// Round-trip too.
		dec, err := DecodePK(desc, k)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(dec[0], vals[0]) || !reflect.DeepEqual(dec[1], vals[1]) {
			t.Fatalf("pk round trip: %+v != %+v", dec, vals)
		}
	}
	byTuple := func(a, b pair) bool {
		if a.vals[0].I != b.vals[0].I {
			return a.vals[0].I < b.vals[0].I
		}
		return a.vals[1].S < b.vals[1].S
	}
	sorted := append([]pair(nil), pairs...)
	sort.Slice(sorted, func(i, j int) bool { return byTuple(sorted[i], sorted[j]) })
	byKey := append([]pair(nil), pairs...)
	sort.Slice(byKey, func(i, j int) bool { return bytes.Compare(byKey[i].key, byKey[j].key) < 0 })
	for i := range sorted {
		if !bytes.Equal(sorted[i].key, byKey[i].key) {
			t.Fatalf("ordering diverges at %d: tuple order %v vs key order %v", i, sorted[i].vals, byKey[i].vals)
		}
	}
}

// jsonEncodeValue is the v1 encoding, kept here only as the benchmark
// baseline.
func jsonEncodeValue(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) ([]byte, error) {
	obj := make(map[string]any, len(row))
	for colID, d := range row {
		if desc.IsPKCol(colID) || d.Null {
			continue
		}
		col, _ := desc.ColByID(colID)
		key := strconv.Itoa(int(colID))
		switch col.Type {
		case types.Int:
			obj[key] = json.Number(strconv.FormatInt(d.I, 10))
		case types.Float:
			obj[key] = d.F
		case types.String:
			obj[key] = d.S
		case types.Bool:
			obj[key] = d.B
		}
	}
	return json.Marshal(obj)
}

func benchRow() (*catalog.TableDescriptor, map[catalog.ColumnID]types.Datum) {
	return testDesc(), map[catalog.ColumnID]types.Datum{
		1: types.NewInt(12345),
		2: types.NewString("pk-part"),
		3: types.NewInt(987654321),
		4: types.NewFloat(3.14159),
		5: types.NewString("some medium length string value here"),
		6: types.NewBool(true),
	}
}

func BenchmarkEncodeValueBinary(b *testing.B) {
	desc, row := benchRow()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeValue(desc, row); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeValueJSON(b *testing.B) {
	desc, row := benchRow()
	for i := 0; i < b.N; i++ {
		if _, err := jsonEncodeValue(desc, row); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeValueBinary(b *testing.B) {
	desc, row := benchRow()
	raw, err := EncodeValue(desc, row)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeValue(desc, raw); err != nil {
			b.Fatal(err)
		}
	}
}
