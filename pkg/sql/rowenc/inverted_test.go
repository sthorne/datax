package rowenc

import (
	"bytes"
	"sort"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/types"
)

func invertedDesc() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{ID: 9, Name: "docs", Columns: []catalog.Column{
		{ID: 1, Name: "tenant", Type: types.String},
		{ID: 2, Name: "id", Type: types.Int},
		{ID: 3, Name: "doc", Type: types.Jsonb},
	}, PrimaryKey: []catalog.ColumnID{1, 2}, Indexes: []catalog.IndexDescriptor{
		{ID: 2, Name: "docs_gin", ColumnIDs: []catalog.ColumnID{3}, Inverted: inverted.JSONBOps},
	}}
}

func docRow(t *testing.T, tenant string, id int64, doc string) map[catalog.ColumnID]types.Datum {
	t.Helper()
	row := map[catalog.ColumnID]types.Datum{1: types.NewString(tenant), 2: types.NewInt(id), 3: types.DNull}
	if doc != "" {
		d, err := types.ParseJSONB(doc)
		if err != nil {
			t.Fatal(err)
		}
		row[3] = d
	}
	return row
}

// TestInvertedIndexEntries: one entry per distinct element, each key the
// element then the primary key, sorted; the primary key reads back from
// every entry; a NULL or empty document has none.
func TestInvertedIndexEntries(t *testing.T) {
	desc := invertedDesc()
	idx := &desc.Indexes[0]
	row := docRow(t, "acme\x00x", 7, `{"a": [1, 1, 2], "b": {"c": "v\u0000w"}}`)
	entries, err := IndexEntries(desc, idx, row)
	if err != nil {
		t.Fatal(err)
	}
	// Keys a and b, values a.[]=1, a.[]=2, b.c="v\0w".
	if len(entries) != 5 {
		t.Fatalf("%d entries, want 5", len(entries))
	}
	if !sort.SliceIsSorted(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 }) {
		t.Fatal("entries are not sorted by key")
	}
	wantPK, err := EncodePK(desc, []types.Datum{row[1], row[2]})
	if err != nil {
		t.Fatal(err)
	}
	kind, _ := inverted.Lookup(inverted.JSONBOps)
	elements, _ := kind.Elements(row[3])
	for i, e := range entries {
		pk, err := IndexEntryPrimaryKey(desc, idx, e.Key, e.Value)
		if err != nil {
			t.Fatal(err)
		}
		if !pk.Equal(wantPK) {
			t.Fatalf("entry %d: primary key %s, want %s", i, pk, wantPK)
		}
		el, _, err := InvertedEntryElement(desc, idx, e.Key)
		if err != nil || !bytes.Equal(el, elements[i]) {
			t.Fatalf("entry %d: element %x (%v), want %x", i, el, err, elements[i])
		}
		// The element's span holds the entry and no other element's.
		start, end := InvertedElementSpan(desc, idx, el)
		for j, o := range entries {
			in := bytes.Compare(o.Key, start) >= 0 && bytes.Compare(o.Key, end) < 0
			if in != (i == j) {
				t.Fatalf("span of element %d contains entry %d: %v", i, j, in)
			}
		}
		if len(e.Value) == 0 {
			t.Fatal("an entry's value must not be empty")
		}
	}
	for _, doc := range []string{"", "{}", "[]"} {
		if got, err := IndexEntries(desc, idx, docRow(t, "acme", 1, doc)); err != nil || len(got) != 0 {
			t.Fatalf("document %q: %d entries, %v", doc, len(got), err)
		}
	}
	// A second row with the same element sorts by primary key within it.
	other, _ := IndexEntries(desc, idx, docRow(t, "acme\x00x", 8, `{"a": [1]}`))
	start, end := InvertedElementSpan(desc, idx, elements[2])
	var inSpan []keys.Key
	for _, e := range append(entries, other...) {
		if bytes.Compare(e.Key, start) >= 0 && bytes.Compare(e.Key, end) < 0 {
			inSpan = append(inSpan, e.Key)
		}
	}
	if len(inSpan) != 2 || bytes.Compare(inSpan[0], inSpan[1]) >= 0 {
		t.Fatalf("two rows with one element: %d keys in its span", len(inSpan))
	}
	// At a shadow index ID the entries move as a whole.
	shadow, err := IndexEntriesAt(desc, idx, 77, row)
	if err != nil || len(shadow) != len(entries) || !shadow[0].Key.HasPrefix(keys.TableIndexPrefix(desc.ID, 77)) {
		t.Fatalf("shadow entries: %d, %v", len(shadow), err)
	}
	// An ordinary index still has its one entry through the same call.
	desc.Indexes = append(desc.Indexes, catalog.IndexDescriptor{ID: 3, Name: "by_id", ColumnIDs: []catalog.ColumnID{2}})
	if got, err := IndexEntries(desc, &desc.Indexes[1], row); err != nil || len(got) != 1 {
		t.Fatalf("ordinary index: %d entries, %v", len(got), err)
	}
}

func TestInvertedIndexUnknownKind(t *testing.T) {
	desc := invertedDesc()
	desc.Indexes[0].Inverted = "from_the_future"
	if _, err := IndexEntries(desc, &desc.Indexes[0], docRow(t, "a", 1, `{"a": 1}`)); err == nil {
		t.Fatal("an index of an unknown kind was written as if it were known")
	}
}

func TestPrettyInvertedKey(t *testing.T) {
	desc := invertedDesc()
	entries, err := IndexEntries(desc, &desc.Indexes[0], docRow(t, "acme", 7, `{"a": {"b": 1}}`))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, PrettyKey(desc, e.Key))
	}
	want := []string{`/table/docs/docs_gin/? "a"/"acme"/7`, `/table/docs/docs_gin/"a"."b" = 1/"acme"/7`}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("pretty keys:\n got %q\nwant %q", got, want)
	}
}
