package sql

import (
	"context"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// borrowTestRows encodes articles rows (id, body, title) as a scan
// receives them.
func borrowTestRows(t testing.TB, desc *catalog.TableDescriptor, bodies []types.Datum) (ks []keys.Key, vals [][]byte) {
	t.Helper()
	for i, body := range bodies {
		key, err := rowenc.EncodePK(desc, []types.Datum{types.NewInt(int64(i))})
		if err != nil {
			t.Fatal(err)
		}
		val, err := rowenc.EncodeValue(desc, map[catalog.ColumnID]types.Datum{3: body, 4: types.NewString("title")})
		if err != nil {
			t.Fatal(err)
		}
		ks, vals = append(ks, key), append(vals, val)
	}
	return ks, vals
}

// TestScanTextSearchBorrowsOnlyWhatItDrops: a scan filtering on
// `to_tsvector(config, body) @@ query` decodes body as a view of the row
// value, not a copy, when nothing else reads it. A row it keeps gets a
// copy of its own: once the scan has moved on, overwriting every row
// value it was handed leaves the kept rows as they were, and they equal
// what the scan keeps without borrowing. A column the filter reads
// anywhere else is not borrowed.
func TestScanTextSearchBorrowsOnlyWhatItDrops(t *testing.T) {
	desc := textSearchTestDesc()
	bodies := []types.Datum{
		types.NewString("the quick brown fox"), types.NewString("a lazy dog"), types.DNull, types.NewString(""),
		types.NewString("Foxes everywhere, fox"), types.NewString(strings.Repeat("fox and hound ", 2000)), // past the fused matcher's length
		types.NewString("héllo wörld fox"),
	}
	ctx := context.Background()
	for _, q := range []struct {
		cond     string
		borrowed bool
	}{
		{"to_tsvector('english', body) @@ 'fox'", true},
		{"NOT (to_tsvector('english', body) @@ 'fox')", true},
		{"to_tsvector('english', body) @@ 'fox' AND id >= 0", true},
		{"to_tsvector('english', body) @@ 'fox' AND length(body) > 0", false},
		{"to_tsvector('english', body) @@ plainto_tsquery('english', body)", false},
	} {
		where := whereOf(t, q.cond)
		run := func(borrow bool) [][]types.Datum {
			proj := []projCol{{name: "id", col: desc.Columns[0]}, {name: "body", col: desc.Columns[2]}, {name: "title", col: desc.Columns[3]}}
			it := &scanIter{s: &Session{}, desc: desc, where: where, proj: proj}
			it.prepare()
			if (it.borrowing != nil) != q.borrowed {
				t.Fatalf("%s: borrowing %v, want %v", q.cond, it.borrowing != nil, q.borrowed)
			}
			if !borrow {
				it.borrowing, it.clone = nil, nil
			}
			ks, vals := borrowTestRows(t, desc, bodies)
			for i := range ks {
				if err := it.accept(ctx, ks[i], vals[i], len(ks)-i); err != nil {
					t.Fatalf("%s: %v", q.cond, err)
				}
			}
			for _, v := range vals {
				for j := range v {
					v[j] = 'X'
				}
			}
			return it.out
		}
		got, want := run(true), run(false)
		if len(got) == 0 || len(got) != len(want) {
			t.Fatalf("%s: %d rows kept, %d without borrowing", q.cond, len(got), len(want))
		}
		for i := range got {
			for c := range got[i] {
				g, w := got[i][c], want[i][c]
				if g.Null != w.Null || g.S != w.S || g.I != w.I || strings.Contains(g.S, "XXX") {
					t.Errorf("%s: row %d column %d = %.40q (null %v), want %.40q (null %v)", q.cond, i, c, g.S, g.Null, w.S, w.Null)
				}
			}
		}
	}
}

// TestScanTextSearchFilterAllocs: rejecting a row on a text-search
// filter allocates nothing; the text was copied out of every row
// before.
func TestScanTextSearchFilterAllocs(t *testing.T) {
	desc := textSearchTestDesc()
	where := whereOf(t, "to_tsvector('english', body) @@ 'fox'")
	it := &scanIter{s: &Session{}, desc: desc, where: where, proj: []projCol{{name: "id", col: desc.Columns[0]}}}
	it.prepare()
	ks, vals := borrowTestRows(t, desc, []types.Datum{types.NewString("a lazy dog sleeps in the afternoon sun")})
	ctx := context.Background()
	allocs := testing.AllocsPerRun(100, func() {
		if err := it.accept(ctx, ks[0], vals[0], 1); err != nil {
			t.Fatal(err)
		}
	})
	if len(it.out) != 0 {
		t.Fatalf("%d rows kept, want none", len(it.out))
	}
	if allocs > 0 {
		t.Errorf("%.1f allocations per rejected row, want none", allocs)
	}
}
