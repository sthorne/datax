package sql

import (
	"fmt"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// shardedTestDesc: timeseries-shaped, hidden _shard (id 3) leading the
// PK, logical PK (series, ts).
func shardedTestDesc() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: 7, Name: "m", Timeseries: true, ShardBuckets: 4,
		Columns: []catalog.Column{
			{ID: 1, Name: "series", Type: types.Int, NotNull: true},
			{ID: 2, Name: "ts", Type: types.Timestamp, NotNull: true},
			{ID: 3, Name: "_shard", Type: types.Int, Hidden: true},
			{ID: 4, Name: "v", Type: types.Int},
		},
		PrimaryKey: []catalog.ColumnID{3, 1, 2},
	}
}

func ob(cols ...string) []parser.OrderCol {
	var out []parser.OrderCol
	for _, c := range cols {
		oc := parser.OrderCol{Column: c}
		if c[0] == '-' {
			oc = parser.OrderCol{Column: c[1:], Desc: true}
		}
		out = append(out, oc)
	}
	return out
}

func TestOrderPlan(t *testing.T) {
	sharded := shardedTestDesc()
	plain := testDesc() // PK (a, b), index by_cd

	fan := accessPlan{kind: planPKScan, fanBuckets: 4}
	// The dashboard shape: WHERE series = 'x' pins the logical prefix.
	one := types.NewInt(1)
	fanPinned := accessPlan{kind: planPKScan, fanBuckets: 4, idxVals: []types.Datum{one}, eqCols: []string{"series"}}
	flat := accessPlan{kind: planPKScan}
	full := accessPlan{kind: planFullScan}
	idxScan := accessPlan{kind: planIndexScan, idx: &plain.Indexes[0]}

	cases := []struct {
		name      string
		desc      *catalog.TableDescriptor
		plan      accessPlan
		order     []parser.OrderCol
		reverseOK bool
		want      orderDecision
	}{
		{"fan-asc-prefix", sharded, fan, ob("series", "ts"), false,
			orderDecision{satisfied: true, mergeFan: true}},
		{"fan-asc-first-col", sharded, fan, ob("series"), false,
			orderDecision{satisfied: true, mergeFan: true}},
		{"fan-desc-gated", sharded, fan, ob("-series", "-ts"), true,
			orderDecision{satisfied: true, mergeFan: true, reverse: true}},
		{"fan-desc-no-gate", sharded, fan, ob("-series", "-ts"), false, orderDecision{}},
		{"fan-mixed-dirs", sharded, fan, ob("series", "-ts"), true, orderDecision{}},
		{"fan-non-prefix", sharded, fan, ob("ts"), false, orderDecision{}},
		{"fan-non-key", sharded, fan, ob("v"), false, orderDecision{}},
		// Equality-pinned prefix: WHERE series = 'x' ORDER BY ts [DESC].
		{"fan-pinned-ts", sharded, fanPinned, ob("ts"), false,
			orderDecision{satisfied: true, mergeFan: true}},
		{"fan-pinned-ts-desc", sharded, fanPinned, ob("-ts"), true,
			orderDecision{satisfied: true, mergeFan: true, reverse: true}},
		{"fan-pinned-const-any-dir", sharded, fanPinned, ob("-series", "ts"), false,
			orderDecision{satisfied: true, mergeFan: true}},
		{"fan-pinned-all-const", sharded, fanPinned, ob("series"), false,
			orderDecision{satisfied: true}},
		{"fan-pinned-desc-no-gate", sharded, fanPinned, ob("-ts"), false, orderDecision{}},
		// A full scan of a sharded table is in PHYSICAL (_shard-first)
		// order: never satisfied by a user ORDER BY.
		{"sharded-full-scan", sharded, full, ob("series", "ts"), true, orderDecision{}},
		// Plain tables: the pre-existing ascending behavior, plus
		// descending via reverse scans when gated on.
		{"plain-asc", plain, flat, ob("a", "b"), false, orderDecision{satisfied: true}},
		{"plain-desc-gated", plain, flat, ob("-a", "-b"), true,
			orderDecision{satisfied: true, reverse: true}},
		{"plain-desc-no-gate", plain, flat, ob("-a"), false, orderDecision{}},
		{"plain-full-desc", plain, full, ob("-a"), true,
			orderDecision{satisfied: true, reverse: true}},
		{"index-asc", plain, idxScan, ob("c", "d"), false, orderDecision{satisfied: true}},
		{"index-desc-gated", plain, idxScan, ob("-c", "-d"), true,
			orderDecision{satisfied: true, reverse: true}},
		{"index-non-prefix", plain, idxScan, ob("d"), false, orderDecision{}},
	}
	for _, tc := range cases {
		if got := orderPlan(tc.desc, tc.plan, tc.order, tc.reverseOK); got != tc.want {
			t.Errorf("%s: got %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

func TestMergeFannedRows(t *testing.T) {
	prefix := "PP" // 2-byte stand-in for the constant prefix
	mk := func(suffixes ...string) []fetchedRow {
		var out []fetchedRow
		for _, s := range suffixes {
			out = append(out, fetchedRow{key: keys.Key(prefix + s)})
		}
		return out
	}
	suffixesOf := func(rows []fetchedRow) []string {
		var out []string
		for _, r := range rows {
			out = append(out, string(r.key[len(prefix):]))
		}
		return out
	}

	runs := [][]fetchedRow{
		mk("a", "d", "g"),
		mk("b", "e"),
		{},
		mk("c", "f", "h", "i"),
	}
	got := suffixesOf(mergeFannedRows(runs, len(prefix), false, 0))
	if fmt.Sprint(got) != "[a b c d e f g h i]" {
		t.Fatalf("merged: %v", got)
	}
	// Global limit stops early.
	got = suffixesOf(mergeFannedRows(runs, len(prefix), false, 4))
	if fmt.Sprint(got) != "[a b c d]" {
		t.Fatalf("limited merge: %v", got)
	}
	// Reverse merge over descending runs.
	rev := [][]fetchedRow{
		mk("g", "d", "a"),
		mk("e", "b"),
		mk("i", "h", "f", "c"),
	}
	got = suffixesOf(mergeFannedRows(rev, len(prefix), true, 0))
	if fmt.Sprint(got) != "[i h g f e d c b a]" {
		t.Fatalf("reverse merged: %v", got)
	}
	got = suffixesOf(mergeFannedRows(rev, len(prefix), true, 3))
	if fmt.Sprint(got) != "[i h g]" {
		t.Fatalf("reverse limited: %v", got)
	}
	if out := mergeFannedRows(nil, 0, false, 0); len(out) != 0 {
		t.Fatalf("empty merge: %v", out)
	}
}
