package sql

import (
	"reflect"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Three-table fixture: a(id), b(id, aid), c(id, bid, aid).
func reorderFixture() ([]joinSide, map[string]*catalog.TableDescriptor) {
	mk := func(id uint64, name string, cols ...string) *catalog.TableDescriptor {
		d := &catalog.TableDescriptor{ID: id, Name: name, PrimaryKey: []catalog.ColumnID{1}}
		for i, c := range cols {
			d.Columns = append(d.Columns, catalog.Column{
				ID: catalog.ColumnID(i + 1), Name: c, Type: types.Int, NotNull: true,
			})
		}
		return d
	}
	descs := map[string]*catalog.TableDescriptor{
		"a": mk(1, "a", "id"),
		"b": mk(2, "b", "id", "aid"),
		"c": mk(3, "c", "id", "bid", "aid"),
	}
	sides := []joinSide{
		{desc: descs["a"], alias: "a"},
		{desc: descs["b"], alias: "b"},
		{desc: descs["c"], alias: "c"},
	}
	return sides, descs
}

func onCond(lt, lc, rt, rc string) parser.JoinCond {
	return parser.JoinCond{
		L: parser.ColumnRef{Table: lt, Column: lc},
		R: parser.ColumnRef{Table: rt, Column: rc},
	}
}

// FROM a JOIN b ON b.aid = a.id JOIN c ON c.bid = b.id (worst-first when
// a is the big table).
func chainSelect() *parser.Select {
	return &parser.Select{
		Exprs: []parser.SelectExpr{{Star: true}},
		Table: "a",
		Joins: []parser.JoinClause{
			{Table: "b", On: []parser.JoinCond{onCond("b", "aid", "a", "id")}},
			{Table: "c", On: []parser.JoinCond{onCond("c", "bid", "b", "id")}},
		},
		Limit: -1,
	}
}

func rowStats(sides []joinSide, rows ...int64) []*catalog.TableStatistics {
	out := make([]*catalog.TableStatistics, len(sides))
	for i, r := range rows {
		out[i] = &catalog.TableStatistics{TableID: sides[i].desc.ID, RowCount: r}
	}
	return out
}

func TestReorderJoinsGreedy(t *testing.T) {
	sides, _ := reorderFixture()
	sel := chainSelect()
	before := *sel

	// a=1000, b=10, c=100: cheapest-first connected order is b, c, a.
	clone, order, changed, ok := reorderJoins(sel, sides, rowStats(sides, 1000, 10, 100))
	if !ok || !changed {
		t.Fatalf("ok=%v changed=%v", ok, changed)
	}
	if want := []int{1, 2, 0}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	if clone.Table != "b" || clone.Joins[0].Table != "c" || clone.Joins[1].Table != "a" {
		t.Fatalf("clone order: %s, %s, %s", clone.Table, clone.Joins[0].Table, clone.Joins[1].Table)
	}
	// The c level binds to b; the a level binds via the pooled b.aid = a.id.
	if len(clone.Joins[0].On) != 1 || clone.Joins[0].On[0].L.String() != "c.bid" || clone.Joins[0].On[0].R.String() != "b.id" {
		t.Fatalf("level 1 ON: %+v", clone.Joins[0].On)
	}
	if len(clone.Joins[1].On) != 1 || clone.Joins[1].On[0].L.String() != "b.aid" || clone.Joins[1].On[0].R.String() != "a.id" {
		t.Fatalf("level 2 ON: %+v", clone.Joins[1].On)
	}
	// The star was pre-expanded in the ORIGINAL side order, qualified.
	wantCols := []string{"a.id", "b.id", "b.aid", "c.id", "c.bid", "c.aid"}
	if len(clone.Exprs) != len(wantCols) {
		t.Fatalf("exprs: %+v", clone.Exprs)
	}
	for i, w := range wantCols {
		if clone.Exprs[i].Star || clone.Exprs[i].Expr.Column != w {
			t.Fatalf("expr[%d] = %+v, want column %s", i, clone.Exprs[i], w)
		}
	}
	// The input AST was not mutated.
	if !reflect.DeepEqual(before, *sel) {
		t.Fatalf("input Select mutated:\nbefore %+v\nafter  %+v", before, *sel)
	}
}

// A side-local WHERE equality with a high distinct count makes the big
// table the cheapest driving side.
func TestReorderJoinsWhereSelectivity(t *testing.T) {
	sides, _ := reorderFixture()
	sel := chainSelect()
	one := types.NewInt(1)
	sel.Where = []parser.Comparison{{Column: "a.id", Op: "=", Value: parser.Expr{Lit: &one}}}

	stats := rowStats(sides, 1000, 10, 100)
	stats[0].Columns = []catalog.ColumnStatistics{{ID: 1, Distinct: 1000}}
	_, order, changed, ok := reorderJoins(sel, sides, stats)
	if !ok {
		t.Fatal("reorder declined")
	}
	// est(a) = 1, est(b) = 10, est(c) = 100 → a, b, c: already the
	// syntactic order, so nothing changes.
	if changed {
		t.Fatalf("changed with order %v, want identity", order)
	}
}

// An ON conjunct whose endpoints both land early re-attaches at the
// level where its LATER side is placed.
func TestReorderJoinsPooling(t *testing.T) {
	sides, _ := reorderFixture()
	// FROM a JOIN b ON b.aid = a.id JOIN c ON c.aid = a.id: edges a—b, a—c.
	sel := chainSelect()
	sel.Joins[1].On = []parser.JoinCond{onCond("c", "aid", "a", "id")}

	// b(10) first; c is not connected to b, so a(1000) joins next, then c.
	clone, order, changed, ok := reorderJoins(sel, sides, rowStats(sides, 1000, 10, 100))
	if !ok || !changed {
		t.Fatalf("ok=%v changed=%v", ok, changed)
	}
	if want := []int{1, 0, 2}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	if len(clone.Joins[0].On) != 1 || clone.Joins[0].On[0].L.String() != "b.aid" {
		t.Fatalf("level 1 ON: %+v", clone.Joins[0].On)
	}
	if len(clone.Joins[1].On) != 1 || clone.Joins[1].On[0].L.String() != "c.aid" {
		t.Fatalf("level 2 ON: %+v", clone.Joins[1].On)
	}
}

func TestReorderJoinsSkips(t *testing.T) {
	sides, descs := reorderFixture()
	stats := rowStats(sides, 1000, 10, 100)

	// LEFT join anywhere: order is semantics.
	leftSides := append([]joinSide(nil), sides...)
	leftSides[2].left = true
	if _, _, _, ok := reorderJoins(chainSelect(), leftSides, stats); ok {
		t.Fatal("reordered across a LEFT join")
	}

	// Self-join (duplicate table name): qualified references bind by
	// position, so order must stay put.
	dupSides := []joinSide{
		{desc: descs["a"], alias: "x"},
		{desc: descs["a"], alias: "y"},
		{desc: descs["c"], alias: "c"},
	}
	dup := chainSelect()
	if _, _, _, ok := reorderJoins(dup, dupSides, stats); ok {
		t.Fatal("reordered a self-join")
	}

	// Single side: nothing to do.
	if _, _, _, ok := reorderJoins(&parser.Select{Table: "a"}, sides[:1], stats[:1]); ok {
		t.Fatal("reordered a single table")
	}

	// Unresolvable ON reference: decline rather than guess.
	bad := chainSelect()
	bad.Joins[0].On = []parser.JoinCond{onCond("b", "nope", "a", "id")}
	if _, _, _, ok := reorderJoins(bad, sides, stats); ok {
		t.Fatal("reordered with an unresolvable ON condition")
	}
}
