package rowenc

import (
	"bytes"
	"sort"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

func newTypeDesc() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: 7, Name: "t",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", Type: types.Int, NotNull: true},
			{ID: 2, Name: "ts", Type: types.Timestamp},
			{ID: 3, Name: "d", Type: types.Date},
			{ID: 4, Name: "b", Type: types.Bytes},
			{ID: 5, Name: "u", Type: types.Uuid},
		},
		PrimaryKey: []catalog.ColumnID{1},
	}
}

func TestNewTypeValueRoundTrip(t *testing.T) {
	desc := newTypeDesc()
	u, _ := types.ParseUUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	row := map[catalog.ColumnID]types.Datum{
		1: types.NewInt(1),
		2: types.NewTimestamp(1_756_512_123_456_789_000),
		3: types.NewDate(20_700),
		4: types.NewBytes([]byte{0, 1, 0xff, 0xde}),
		5: types.NewUUID(u),
	}
	raw, err := EncodeValue(desc, row)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeValue(desc, raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []catalog.ColumnID{2, 3, 4, 5} {
		want := row[id]
		g, ok := got[id]
		if !ok || g.Fam != want.Fam || g.I != want.I || g.S != want.S {
			t.Fatalf("column %d: got %+v want %+v", id, g, want)
		}
	}
}

// Timestamp and UUID primary keys round-trip and preserve order under the
// key encoding.
func TestNewTypePKOrdering(t *testing.T) {
	tsDesc := &catalog.TableDescriptor{
		ID: 8, Columns: []catalog.Column{{ID: 1, Name: "ts", Type: types.Timestamp, NotNull: true}},
		PrimaryKey: []catalog.ColumnID{1},
	}
	nanos := []int64{-5e18, -1, 0, 1, 999, 1e15, 5e18}
	var keys [][]byte
	for _, n := range nanos {
		k, err := EncodePK(tsDesc, []types.Datum{types.NewTimestamp(n)})
		if err != nil {
			t.Fatal(err)
		}
		back, err := DecodePK(tsDesc, k)
		if err != nil || back[0].I != n || back[0].Fam != types.Timestamp {
			t.Fatalf("round trip %d: %+v, %v", n, back, err)
		}
		keys = append(keys, k)
	}
	if !sort.SliceIsSorted(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 }) {
		t.Fatal("timestamp key encoding is not order-preserving")
	}

	uDesc := &catalog.TableDescriptor{
		ID: 9, Columns: []catalog.Column{{ID: 1, Name: "u", Type: types.Uuid, NotNull: true}},
		PrimaryKey: []catalog.ColumnID{1},
	}
	us := []string{
		"00000000-0000-0000-0000-000000000000",
		"00000000-0000-0000-0000-0000000000ff",
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
	}
	keys = nil
	for _, s := range us {
		v, _ := types.ParseUUID(s)
		k, err := EncodePK(uDesc, []types.Datum{types.NewUUID(v)})
		if err != nil {
			t.Fatal(err)
		}
		back, err := DecodePK(uDesc, k)
		if err != nil || back[0].S != string(v[:]) {
			t.Fatalf("round trip %s: %v", s, err)
		}
		keys = append(keys, k)
	}
	if !sort.SliceIsSorted(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 }) {
		t.Fatal("uuid key encoding is not order-preserving")
	}
}

// Fill-on-read: a FillDefault column absent from the stored value decodes
// as the default, while an explicitly-stored NULL stays NULL.
func TestFillDefaultDecoding(t *testing.T) {
	seven := types.NewInt(7)
	desc := &catalog.TableDescriptor{
		ID: 10, Name: "t",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", Type: types.Int, NotNull: true},
			{ID: 2, Name: "v", Type: types.Int},
		},
		PrimaryKey: []catalog.ColumnID{1},
	}

	// A row written before the column existed (encoded without column 3).
	oldRaw, err := EncodeValue(desc, map[catalog.ColumnID]types.Datum{
		1: types.NewInt(1), 2: types.NewInt(10),
	})
	if err != nil {
		t.Fatal(err)
	}

	// The column is added with a fill-on-read default.
	desc.Columns = append(desc.Columns, catalog.Column{
		ID: 3, Name: "c", Type: types.Int, Default: &seven, FillDefault: true,
	})

	got, err := DecodeValue(desc, oldRaw)
	if err != nil {
		t.Fatal(err)
	}
	if d, ok := got[3]; !ok || d.Null || d.I != 7 {
		t.Fatalf("old row column 3 = %+v, want default 7", got[3])
	}

	// A new row storing an explicit NULL for the column keeps it NULL.
	nullRaw, err := EncodeValue(desc, map[catalog.ColumnID]types.Datum{
		1: types.NewInt(2), 2: types.NewInt(20), 3: types.DNull,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err = DecodeValue(desc, nullRaw)
	if err != nil {
		t.Fatal(err)
	}
	if d, ok := got[3]; !ok || !d.Null {
		t.Fatalf("explicit NULL decoded as %+v", got[3])
	}

	// And a real value round-trips normally.
	valRaw, err := EncodeValue(desc, map[catalog.ColumnID]types.Datum{
		1: types.NewInt(3), 3: types.NewInt(42),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err = DecodeValue(desc, valRaw)
	if err != nil {
		t.Fatal(err)
	}
	if d := got[3]; d.Null || d.I != 42 {
		t.Fatalf("value decoded as %+v", got[3])
	}
}
