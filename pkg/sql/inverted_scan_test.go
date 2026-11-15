package sql

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// invertedTestDesc: PRIMARY KEY (id); doc JSONB with an inverted index;
// tenant with an ordinary index; note unindexed.
func invertedTestDesc() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: 43, Name: "docs",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", Type: types.Int, NotNull: true},
			{ID: 2, Name: "doc", Type: types.Jsonb},
			{ID: 3, Name: "tenant", Type: types.Int, NotNull: true},
			{ID: 4, Name: "note", Type: types.String},
			{ID: 5, Name: "other", Type: types.Jsonb},
		},
		PrimaryKey: []catalog.ColumnID{1},
		Indexes: []catalog.IndexDescriptor{
			{ID: 2, Name: "by_tenant", ColumnIDs: []catalog.ColumnID{3}},
			{ID: 3, Name: "docs_gin", ColumnIDs: []catalog.ColumnID{2}, Inverted: inverted.JSONBOps},
		},
	}
}

func TestPickPlanInverted(t *testing.T) {
	desc := invertedTestDesc()
	contains := cmpStr("doc", "@>", `{"a": 1}`)
	hasKey := cmpStr("doc", "?", "a")
	param := parser.Comparison{Column: "doc", Op: "@>", Value: parser.Expr{Param: 1}}
	colRHS := parser.Comparison{Column: "doc", Op: "@>", Value: parser.Expr{Column: "other"}}
	path := cmpStr("doc", "@>", `{"a": 1}`)
	path.Path = []parser.PathStep{{Key: "x"}}
	or := func(branches ...[]parser.Comparison) parser.Comparison {
		return parser.Comparison{Op: "OR", Or: branches}
	}

	for _, c := range []struct {
		name   string
		where  []parser.Comparison
		params []types.Datum
		want   planKind
		expr   string
	}{
		{"containment", []parser.Comparison{contains}, nil, planInvertedScan, `"a" = 1`},
		{"key", []parser.Comparison{hasKey}, nil, planInvertedScan, `? "a"`},
		{"both", []parser.Comparison{contains, hasKey}, nil, planInvertedScan, `"a" = 1 AND ? "a"`},
		{"parameter", []parser.Comparison{param}, []types.Datum{types.NewString(`{"b": true}`)}, planInvertedScan, `"b" = true`},
		{"with an unindexed filter", []parser.Comparison{contains, cmpStr("note", "=", "x")}, nil, planInvertedScan, `"a" = 1`},
		// A range alone does not outrank it; an equality on a key does.
		{"with a pk range", []parser.Comparison{contains, cmpInt("id", ">", 5)}, nil, planInvertedScan, `"a" = 1`},
		{"with an index equality", []parser.Comparison{contains, cmpInt("tenant", "=", 5)}, nil, planIndexScan, ""},
		{"with a pk point", []parser.Comparison{contains, cmpInt("id", "=", 5)}, nil, planPKPoint, ""},
		// OR: usable only when every branch constrains the index.
		{"or of two", []parser.Comparison{or([]parser.Comparison{contains}, []parser.Comparison{hasKey})}, nil, planInvertedScan, `"a" = 1 OR ? "a"`},
		{"or with a branch of two", []parser.Comparison{or([]parser.Comparison{contains, cmpStr("note", "=", "x")}, []parser.Comparison{hasKey})}, nil, planInvertedScan, `"a" = 1 OR ? "a"`},
		{"or with an unindexed branch", []parser.Comparison{or([]parser.Comparison{contains}, []parser.Comparison{cmpStr("note", "=", "x")})}, nil, planFullScan, ""},
		// What says nothing to the index.
		{"negated", []parser.Comparison{cmpStr("doc", "NOT @>", `{"a": 1}`)}, nil, planFullScan, ""},
		{"contained by", []parser.Comparison{cmpStr("doc", "<@", `{"a": 1}`)}, nil, planFullScan, ""},
		{"empty object", []parser.Comparison{cmpStr("doc", "@>", `{}`)}, nil, planFullScan, ""},
		{"row-dependent value", []parser.Comparison{colRHS}, nil, planFullScan, ""},
		{"under a path", []parser.Comparison{path}, nil, planFullScan, ""},
		{"another column", []parser.Comparison{cmpStr("other", "@>", `{"a": 1}`)}, nil, planFullScan, ""},
	} {
		plan, err := pickPlan(desc, c.where, c.params)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if plan.kind != c.want {
			t.Errorf("%s: plan %q, want kind %d", c.name, plan.String(), c.want)
			continue
		}
		if c.want != planInvertedScan {
			continue
		}
		if plan.idx == nil || plan.idx.Name != "docs_gin" {
			t.Errorf("%s: index %+v", c.name, plan.idx)
		}
		// Nothing is guaranteed by the lookups: the whole clause is
		// residual, which keeps a LIMIT above the recheck.
		if len(plan.residual) != len(c.where) {
			t.Errorf("%s: %d residual conjuncts of %d", c.name, len(plan.residual), len(c.where))
		}
		if want := `inverted scan of index "docs_gin" (` + c.expr + `) + primary key join, rows rechecked`; plan.String() != want {
			t.Errorf("%s:\n got %s\nwant %s", c.name, plan.String(), want)
		}
	}
}

func TestPickPlanInvertedModes(t *testing.T) {
	desc := invertedTestDesc()
	contains := cmpStr("doc", "@>", `{"a": 1}`)
	withEq := []parser.Comparison{contains, cmpInt("tenant", "=", 5)}

	// off: never; on: over any scan, but not over a point lookup.
	if p, _ := pickPlanMode(desc, nil, []parser.Comparison{contains}, nil, invertedOff); p.kind != planFullScan {
		t.Errorf("off: %s", p)
	}
	if p, _ := pickPlanMode(desc, nil, withEq, nil, invertedOn); p.kind != planInvertedScan {
		t.Errorf("on: %s", p)
	}
	if p, _ := pickPlanMode(desc, nil, []parser.Comparison{contains, cmpInt("id", "=", 5)}, nil, invertedOn); p.kind != planPKPoint {
		t.Errorf("on, with a point lookup: %s", p)
	}

	// A write-only index is maintained but not read.
	building := invertedTestDesc()
	building.Indexes[1].State = catalog.IndexStateWriteOnly
	if p, _ := pickPlanMode(building, nil, []parser.Comparison{contains}, nil, invertedOn); p.kind != planFullScan {
		t.Errorf("write-only index read: %s", p)
	}

	// With statistics the inverted scan is costed at a fixed selectivity:
	// it beats an equality on a column of few distinct values and loses
	// to one on a column of many.
	stats := func(distinct int64) *catalog.TableStatistics {
		return &catalog.TableStatistics{TableID: desc.ID, RowCount: 100000, Columns: []catalog.ColumnStatistics{{ID: 3, Distinct: distinct}}}
	}
	if p, _ := pickPlanMode(desc, stats(10), withEq, nil, invertedAuto); p.kind != planInvertedScan || p.estRows != 1000 {
		t.Errorf("few distinct tenants: %s (~%.0f rows)", p, p.estRows)
	}
	if p, _ := pickPlanMode(desc, stats(50000), withEq, nil, invertedAuto); p.kind != planIndexScan {
		t.Errorf("many distinct tenants: %s", p)
	}
	if p, _ := pickPlanMode(desc, stats(10), []parser.Comparison{contains}, nil, invertedAuto); p.kind != planInvertedScan {
		t.Errorf("alone, with statistics: %s", p)
	}
}

// TestInvertedPlanStringIsBounded: EXPLAIN does not print one lookup per
// value of a large contained document.
func TestInvertedPlanStringIsBounded(t *testing.T) {
	desc := invertedTestDesc()
	var b strings.Builder
	b.WriteString(`{"k0": 0`)
	for i := 1; i < 200; i++ {
		b.WriteString(`, "key` + strings.Repeat("x", i%7) + string(rune('a'+i%26)) + `": ` + string(rune('0'+i%10)))
	}
	b.WriteString("}")
	plan, err := pickPlan(desc, []parser.Comparison{cmpStr("doc", "@>", b.String())}, nil)
	if err != nil || plan.kind != planInvertedScan {
		t.Fatalf("plan: %v %v", plan.kind, err)
	}
	if s := plan.String(); len(s) > 300 || !strings.Contains(s, "…") {
		t.Fatalf("plan text of %d bytes: %s", len(s), s)
	}
}

// TestEntryDiff: an update writes and deletes only the entries that
// differ.
func TestEntryDiff(t *testing.T) {
	desc := invertedTestDesc()
	idx := &desc.Indexes[1]
	row := func(doc string) map[catalog.ColumnID]types.Datum {
		d, err := types.ParseJSONB(doc)
		if err != nil {
			t.Fatal(err)
		}
		return map[catalog.ColumnID]types.Datum{1: types.NewInt(1), 2: d, 3: types.NewInt(1)}
	}
	entries := func(doc string) []rowenc.IndexEntry {
		e, err := rowenc.IndexEntries(desc, idx, row(doc))
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	removed, added := entryDiff(entries(`{"a": 1, "b": [1, 2], "c": "x"}`), entries(`{"a": 1, "b": [2, 3], "d": "x"}`))
	// Out: b.[]=1, key c, c="x". In: b.[]=3, key d, d="x".
	if len(removed) != 3 || len(added) != 3 {
		t.Fatalf("removed %d, added %d", len(removed), len(added))
	}
	if removed, added = entryDiff(entries(`{"a": 1}`), entries(`{"a": 1.0}`)); len(removed)+len(added) != 0 {
		t.Fatalf("an equal document moved entries: -%d +%d", len(removed), len(added))
	}
}
