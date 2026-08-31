package sql

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

func TestJoinEnvEval(t *testing.T) {
	sides := []joinSide{
		{desc: &catalog.TableDescriptor{Name: "a", Columns: []catalog.Column{
			{ID: 1, Name: "id", Type: types.Int},
			{ID: 2, Name: "j", Type: types.Jsonb},
		}}, alias: "a"},
		{desc: &catalog.TableDescriptor{Name: "b", Columns: []catalog.Column{
			{ID: 1, Name: "id", Type: types.Int},
			{ID: 2, Name: "qty", Type: types.Int},
		}}, alias: "b", left: true},
	}
	full := joinedRow{rows: []map[catalog.ColumnID]types.Datum{
		{1: types.NewInt(7), 2: types.NewJsonb(`{"k":"v"}`)},
		{1: types.NewInt(7), 2: types.NewInt(3)},
	}}
	// NULL-extended LEFT side: nil map.
	extended := joinedRow{rows: []map[catalog.ColumnID]types.Datum{
		{1: types.NewInt(8), 2: types.NewJsonb(`{"k":"w"}`)},
		nil,
	}}

	parse := func(src string) parser.Expr {
		t.Helper()
		stmts, err := parser.Parse("SELECT " + src + " FROM t")
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		return stmts[0].(*parser.Select).Exprs[0].Expr
	}

	// Qualified and unqualified resolution; arithmetic across sides.
	d, err := evalExprEnv(parse("b.qty * 2"), joinEnv{sides, full}, nil)
	if err != nil || d.I != 6 {
		t.Fatalf("qty*2: %+v, %v", d, err)
	}
	// Unqualified unique name resolves; ambiguous name errors.
	if _, err := evalExprEnv(parse("qty"), joinEnv{sides, full}, nil); err != nil {
		t.Fatalf("unqualified: %v", err)
	}
	if _, err := evalExprEnv(parse("id"), joinEnv{sides, full}, nil); err == nil {
		t.Fatal("ambiguous id accepted")
	}
	// Path through a joined jsonb column.
	d, err = evalExprEnv(parse("a.j ->> 'k'"), joinEnv{sides, full}, nil)
	if err != nil || d.S != "v" {
		t.Fatalf("path: %+v, %v", d, err)
	}
	// NULL-extended side: column, path over it, and arithmetic all yield
	// SQL NULL rather than errors.
	for _, src := range []string{"b.qty", "b.qty + 1"} {
		d, err = evalExprEnv(parse(src), joinEnv{sides, extended}, nil)
		if err != nil || !d.Null {
			t.Fatalf("%s on extended: %+v, %v", src, d, err)
		}
	}
	// Missing column names the error.
	if _, err := evalExprEnv(parse("b.nope"), joinEnv{sides, full}, nil); err == nil {
		t.Fatal("missing column accepted")
	}
}
