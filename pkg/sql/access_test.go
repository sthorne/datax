package sql

import (
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// testDesc: PRIMARY KEY (a, b); non-unique index by_cd on (c, d); c and d
// nullable.
func testDesc() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: 42, Name: "t",
		Columns: []catalog.Column{
			{ID: 1, Name: "a", Type: types.Int, NotNull: true},
			{ID: 2, Name: "b", Type: types.Int, NotNull: true},
			{ID: 3, Name: "c", Type: types.Int},
			{ID: 4, Name: "d", Type: types.Int},
		},
		PrimaryKey: []catalog.ColumnID{1, 2},
		Indexes: []catalog.IndexDescriptor{
			{ID: 2, Name: "by_cd", ColumnIDs: []catalog.ColumnID{3, 4}},
		},
	}
}

func cmpInt(col, op string, v int64) parser.Comparison {
	d := types.NewInt(v)
	return parser.Comparison{Column: col, Op: op, Value: parser.Expr{Lit: &d}}
}

func cmpStr(col, op, v string) parser.Comparison {
	d := types.NewString(v)
	return parser.Comparison{Column: col, Op: op, Value: parser.Expr{Lit: &d}}
}

func mustPlan(t *testing.T, desc *catalog.TableDescriptor, where []parser.Comparison) accessPlan {
	t.Helper()
	plan, err := pickPlan(desc, where, nil)
	if err != nil {
		t.Fatalf("pickPlan: %v", err)
	}
	return plan
}

func TestPickPlanPKRange(t *testing.T) {
	desc := testDesc()
	plan := mustPlan(t, desc, []parser.Comparison{
		cmpInt("a", "=", 1), cmpInt("b", ">=", 5), cmpInt("b", "<", 9),
	})
	if plan.kind != planPKScan {
		t.Fatalf("kind = %v, want planPKScan", plan.kind)
	}
	if len(plan.idxVals) != 1 || plan.idxVals[0].I != 1 {
		t.Fatalf("eq prefix = %+v", plan.idxVals)
	}
	if plan.lo == nil || !plan.lo.inclusive || plan.lo.val.I != 5 {
		t.Fatalf("lo = %+v", plan.lo)
	}
	if plan.hi == nil || plan.hi.inclusive || plan.hi.val.I != 9 {
		t.Fatalf("hi = %+v", plan.hi)
	}
	if len(plan.residual) != 0 {
		t.Fatalf("residual = %+v", plan.residual)
	}
	want := "range scan of primary key (a = 1, b >= 5, b < 9)"
	if got := plan.String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestPickPlanRangeOnFirstPKColumn(t *testing.T) {
	plan := mustPlan(t, testDesc(), []parser.Comparison{cmpInt("a", ">", 5)})
	if plan.kind != planPKScan || len(plan.idxVals) != 0 {
		t.Fatalf("plan = %+v", plan)
	}
	if plan.lo == nil || plan.lo.inclusive || plan.lo.val.I != 5 || plan.hi != nil {
		t.Fatalf("bounds = %+v / %+v", plan.lo, plan.hi)
	}
	if len(plan.residual) != 0 {
		t.Fatalf("residual = %+v", plan.residual)
	}
}

// A range on a trailing PK column without the leading prefix pinned cannot
// constrain the scan.
func TestPickPlanUnanchoredTrailingRange(t *testing.T) {
	plan := mustPlan(t, testDesc(), []parser.Comparison{cmpInt("b", ">", 3)})
	if plan.kind != planFullScan {
		t.Fatalf("kind = %v, want planFullScan", plan.kind)
	}
	if len(plan.residual) != 1 {
		t.Fatalf("residual = %+v", plan.residual)
	}
}

func TestPickPlanBoundTightening(t *testing.T) {
	plan := mustPlan(t, testDesc(), []parser.Comparison{
		cmpInt("a", ">", 5), cmpInt("a", ">", 7), cmpInt("a", "<=", 10), cmpInt("a", "<", 9),
		cmpInt("a", ">=", 7), // looser than the exclusive > 7: must not win
	})
	if plan.kind != planPKScan {
		t.Fatalf("kind = %v", plan.kind)
	}
	if plan.lo == nil || plan.lo.inclusive || plan.lo.val.I != 7 {
		t.Fatalf("lo = %+v", plan.lo)
	}
	if plan.hi == nil || plan.hi.inclusive || plan.hi.val.I != 9 {
		t.Fatalf("hi = %+v", plan.hi)
	}
	if len(plan.residual) != 0 {
		t.Fatalf("residual = %+v", plan.residual)
	}
}

func TestPickPlanResidualKept(t *testing.T) {
	plan := mustPlan(t, testDesc(), []parser.Comparison{
		cmpInt("a", "=", 1), cmpInt("b", ">", 5), cmpInt("c", "=", 3),
	})
	if plan.kind != planPKScan {
		t.Fatalf("kind = %v", plan.kind)
	}
	if len(plan.residual) != 1 || plan.residual[0].Column != "c" {
		t.Fatalf("residual = %+v", plan.residual)
	}
}

// != never becomes a bound.
func TestPickPlanNotEqualStaysResidual(t *testing.T) {
	plan := mustPlan(t, testDesc(), []parser.Comparison{cmpInt("a", "!=", 5)})
	if plan.kind != planFullScan || len(plan.residual) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
}

// An un-coercible comparison value cannot bound the scan.
func TestPickPlanUncoercibleValue(t *testing.T) {
	plan := mustPlan(t, testDesc(), []parser.Comparison{cmpStr("a", ">", "zzz")})
	if plan.kind != planFullScan || len(plan.residual) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
}

// Equal scores prefer the primary key (no index join).
func TestPickPlanTiePrefersPK(t *testing.T) {
	plan := mustPlan(t, testDesc(), []parser.Comparison{
		cmpInt("a", "=", 1),
		cmpInt("c", "=", 2),
		{Column: "d", Op: "IS NOT NULL"}, // makes by_cd complete, adds no bound
	})
	if plan.kind != planPKScan {
		t.Fatalf("kind = %v, want planPKScan (tie must prefer PK)", plan.kind)
	}
	if len(plan.residual) != 2 {
		t.Fatalf("residual = %+v", plan.residual)
	}
}

// A non-unique index misses rows with NULL in ANY indexed column, so it is
// only usable when the WHERE clause (or schema) rules those rows out.
func TestPickPlanNullSafety(t *testing.T) {
	desc := testDesc()
	// d unconstrained and nullable: by_cd is not a complete row source.
	plan := mustPlan(t, desc, []parser.Comparison{cmpInt("c", "=", 1)})
	if plan.kind != planFullScan {
		t.Fatalf("kind = %v, want planFullScan (by_cd incomplete)", plan.kind)
	}
	// d IS NOT NULL restores completeness.
	plan = mustPlan(t, desc, []parser.Comparison{
		cmpInt("c", "=", 1), {Column: "d", Op: "IS NOT NULL"},
	})
	if plan.kind != planIndexScan || plan.idx.Name != "by_cd" {
		t.Fatalf("plan = %+v", plan)
	}
	// So does declaring d NOT NULL.
	desc.Columns[3].NotNull = true
	plan = mustPlan(t, desc, []parser.Comparison{cmpInt("c", "=", 1)})
	if plan.kind != planIndexScan || plan.idx.Name != "by_cd" {
		t.Fatalf("plan = %+v", plan)
	}
	if got, want := plan.String(), `scan of index "by_cd" (1 column prefix) + primary key join`; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
	// A range on d shows up in the plan and its EXPLAIN string.
	plan = mustPlan(t, desc, []parser.Comparison{cmpInt("c", "=", 1), cmpInt("d", ">", 7)})
	if plan.kind != planIndexScan || plan.lo == nil || plan.lo.val.I != 7 {
		t.Fatalf("plan = %+v", plan)
	}
	if got, want := plan.String(), `scan of index "by_cd" (c = 1, d > 7) + primary key join`; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

// WHERE col IS NULL must never pick an index containing col: those rows
// have no index entry.
func TestPickPlanIsNullForcesFullScan(t *testing.T) {
	plan := mustPlan(t, testDesc(), []parser.Comparison{{Column: "c", Op: "IS NULL"}})
	if plan.kind != planFullScan || len(plan.residual) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
}

// testStats builds statistics for testDesc: distinct counts per column ID.
func testStats(rowCount int64, distinct map[catalog.ColumnID]int64) *catalog.TableStatistics {
	st := &catalog.TableStatistics{TableID: 42, RowCount: rowCount}
	for id, d := range distinct {
		st.Columns = append(st.Columns, catalog.ColumnStatistics{ID: id, Distinct: d})
	}
	return st
}

func mustPlanStats(t *testing.T, desc *catalog.TableDescriptor, st *catalog.TableStatistics, where []parser.Comparison) accessPlan {
	t.Helper()
	plan, err := pickPlanWithStats(desc, st, where, nil)
	if err != nil {
		t.Fatalf("pickPlanWithStats: %v", err)
	}
	return plan
}

// Without statistics no plan ever carries a row estimate — the structural
// path is byte-identical to the pre-statistics planner (every other test
// in this file exercises it through pickPlan, which passes nil stats).
func TestPickPlanNoStatsNoEstimate(t *testing.T) {
	for _, where := range [][]parser.Comparison{
		nil,
		{cmpInt("a", "=", 1)},
		{cmpInt("a", "=", 1), cmpInt("b", "=", 2)},
		{cmpInt("c", "=", 1), {Column: "d", Op: "IS NOT NULL"}},
	} {
		if plan := mustPlan(t, testDesc(), where); plan.estRows != 0 {
			t.Fatalf("where %+v: estRows = %v, want 0", where, plan.estRows)
		}
	}
}

// A selective equality on a non-unique index beats the full scan and gets
// a row estimate: 10000 rows / 1000 distinct = ~10 rows.
func TestPickPlanStatsSelectiveIndex(t *testing.T) {
	desc := testDesc()
	desc.Columns[3].NotNull = true // by_cd complete without extra conjuncts
	st := testStats(10000, map[catalog.ColumnID]int64{3: 1000})
	plan := mustPlanStats(t, desc, st, []parser.Comparison{cmpInt("c", "=", 1)})
	if plan.kind != planIndexScan || plan.idx.Name != "by_cd" {
		t.Fatalf("plan = %+v", plan)
	}
	if plan.estRows != 10 {
		t.Fatalf("estRows = %v, want 10", plan.estRows)
	}
}

// A low-selectivity index scan (2 distinct values over 10000 rows: 5000
// estimated rows × the index-join multiplier ≥ table rows) loses to the
// full scan — the structural planner would have picked the index.
func TestPickPlanStatsLowSelectivityFlipsToFullScan(t *testing.T) {
	desc := testDesc()
	desc.Columns[3].NotNull = true
	where := []parser.Comparison{cmpInt("c", "=", 1)}
	// Structural choice, for contrast: index scan.
	if plan := mustPlan(t, desc, where); plan.kind != planIndexScan {
		t.Fatalf("structural plan = %+v", plan)
	}
	st := testStats(10000, map[catalog.ColumnID]int64{3: 2})
	plan := mustPlanStats(t, desc, st, where)
	if plan.kind != planFullScan {
		t.Fatalf("plan = %+v, want full scan", plan)
	}
	if plan.estRows != 10000 {
		t.Fatalf("estRows = %v, want 10000", plan.estRows)
	}
	if len(plan.residual) != 1 {
		t.Fatalf("residual = %+v", plan.residual)
	}
}

// When the index is far more selective than the PK prefix, cost overrides
// the structural tie-goes-to-PK rule.
func TestPickPlanStatsIndexBeatsPKPrefix(t *testing.T) {
	desc := testDesc()
	desc.Columns[3].NotNull = true
	where := []parser.Comparison{cmpInt("a", "=", 1), cmpInt("c", "=", 2)}
	// Structural: equal scores, PK wins.
	if plan := mustPlan(t, desc, where); plan.kind != planPKScan {
		t.Fatalf("structural plan = %+v", plan)
	}
	// a: 2 distinct (5000 rows, cost 5000); c: 1000 distinct (10 rows,
	// cost 40 with the join multiplier) — the index wins.
	st := testStats(10000, map[catalog.ColumnID]int64{1: 2, 3: 1000})
	plan := mustPlanStats(t, desc, st, where)
	if plan.kind != planIndexScan || plan.idx.Name != "by_cd" {
		t.Fatalf("plan = %+v, want by_cd index scan", plan)
	}
	if plan.estRows != 10 {
		t.Fatalf("estRows = %v, want 10", plan.estRows)
	}
}

// An exact cost tie keeps the primary key: PK cost 4 (1000/250) equals
// index cost 4 (1000/1000 × join multiplier), and ties never pay a join.
func TestPickPlanStatsTiePrefersPK(t *testing.T) {
	desc := testDesc()
	desc.Columns[3].NotNull = true
	st := testStats(1000, map[catalog.ColumnID]int64{1: 250, 3: 1000})
	plan := mustPlanStats(t, desc, st, []parser.Comparison{
		cmpInt("a", "=", 1), cmpInt("c", "=", 2),
	})
	if plan.kind != planPKScan {
		t.Fatalf("plan = %+v, want planPKScan on cost tie", plan)
	}
	if plan.estRows != 4 {
		t.Fatalf("estRows = %v, want 4", plan.estRows)
	}
}

// Selectivity defaults: a column the statistics never saw uses the
// conservative equality guess, ranges use the fixed fraction, and the
// estimate never drops below one row.
func TestPickPlanStatsDefaultSelectivities(t *testing.T) {
	desc := testDesc()
	// Column a absent from stats: eq selectivity 1/10 → 100 rows.
	st := testStats(1000, nil)
	plan := mustPlanStats(t, desc, st, []parser.Comparison{cmpInt("a", "=", 1)})
	if plan.kind != planPKScan || plan.estRows != 100 {
		t.Fatalf("plan = %+v, want PK scan with estRows 100", plan)
	}
	// Range bound: 1000 × 1/10 (a=1, unknown) × 1/3 (range on b) ≈ 33.
	plan = mustPlanStats(t, desc, st, []parser.Comparison{
		cmpInt("a", "=", 1), cmpInt("b", ">", 5),
	})
	if plan.kind != planPKScan {
		t.Fatalf("plan = %+v", plan)
	}
	if plan.estRows < 33 || plan.estRows > 34 {
		t.Fatalf("estRows = %v, want ~33.3", plan.estRows)
	}
	// Floor at one row: 10 rows, a distinct 10 → exactly 1, never below.
	st = testStats(10, map[catalog.ColumnID]int64{1: 100})
	plan = mustPlanStats(t, desc, st, []parser.Comparison{cmpInt("a", "=", 1)})
	if plan.estRows != 1 {
		t.Fatalf("estRows = %v, want floor of 1", plan.estRows)
	}
}

// Point lookups short-circuit before any costing and carry no estimate;
// an unconstrained full scan with statistics reports the table row count.
func TestPickPlanStatsShortCircuitsAndFullScan(t *testing.T) {
	desc := testDesc()
	st := testStats(5000, map[catalog.ColumnID]int64{1: 5000, 2: 5000})
	plan := mustPlanStats(t, desc, st, []parser.Comparison{
		cmpInt("a", "=", 1), cmpInt("b", "=", 2),
	})
	if plan.kind != planPKPoint || plan.estRows != 0 {
		t.Fatalf("plan = %+v, want PK point without estimate", plan)
	}
	plan = mustPlanStats(t, desc, st, nil)
	if plan.kind != planFullScan || plan.estRows != 5000 {
		t.Fatalf("plan = %+v, want full scan with estRows 5000", plan)
	}
}

func TestSpanBounds(t *testing.T) {
	prefix := keys.Key("pfx-")
	enc := func(v int64) keys.Key {
		k, err := rowenc.AppendKeyDatum(prefix.Clone(), types.Int, types.NewInt(v))
		if err != nil {
			t.Fatal(err)
		}
		return k
	}

	// [5, 9): inclusive lo encodes directly; exclusive hi encodes directly.
	p := accessPlan{lo: &colBound{val: types.NewInt(5), inclusive: true}, hi: &colBound{val: types.NewInt(9)}}
	start, end, err := p.spanBounds(prefix, types.Int)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(enc(5)) || !end.Equal(enc(9)) {
		t.Fatalf("span = [%s, %s)", start, end)
	}

	// (5, 9]: exclusive lo steps past the bound value; inclusive hi includes
	// every key carrying it.
	p = accessPlan{lo: &colBound{val: types.NewInt(5)}, hi: &colBound{val: types.NewInt(9), inclusive: true}}
	start, end, err = p.spanBounds(prefix, types.Int)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(enc(5).PrefixEnd()) || !end.Equal(enc(9).PrefixEnd()) {
		t.Fatalf("span = [%s, %s)", start, end)
	}

	// No bounds: the whole prefix.
	start, end, err = accessPlan{}.spanBounds(prefix, types.Int)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(prefix) || !end.Equal(prefix.PrefixEnd()) {
		t.Fatalf("span = [%s, %s)", start, end)
	}
}
