package sql

import (
	"context"
	"testing"

	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestScalarSubqueryOperandColumns: a FROM-less select item that is an
// expression over scalar subqueries is described by PostgreSQL's
// operator rules over the subqueries' own column types (#402), where it
// was text: int4 with int4 is int4, anything with an int8 is int8, a
// numeric or float operand lifts it, a comparison is a boolean, and
// nested expressions, coalesce and CASE type through.
func TestScalarSubqueryOperandColumns(t *testing.T) {
	s := &Session{user: "root"}
	s.vars.searchPath = defaultSearchPath
	for _, tc := range []struct {
		expr  string
		fam   types.Family
		width int32 // an integer's width (0 is int8)
		notz  bool
	}{
		{"(SELECT 1::INT8)", types.Int, 0, false},
		{"(SELECT 1::INT8) + (SELECT 2::INT8)", types.Int, 0, false},
		{"(SELECT count(*) FROM (VALUES (1)) v) + (SELECT count(*) FROM (VALUES (1)) w)", types.Int, 0, false},
		{"(SELECT 1) + (SELECT 2)", types.Int, 4, false},
		{"(SELECT 1) + 1", types.Int, 4, false},
		{"1 + (SELECT 1::INT8)", types.Int, 0, false},
		{"(SELECT 1::INT2) + (SELECT 2::INT2)", types.Int, 2, false},
		{"(SELECT 1::INT2) * (SELECT 2)", types.Int, 4, false},
		{"(SELECT 7) - (SELECT 2)", types.Int, 4, false},
		{"(SELECT 7) / (SELECT 2::INT8)", types.Int, 0, false},
		{"(SELECT 7) % (SELECT 2)", types.Int, 4, false},
		{"((SELECT 1) + (SELECT 2)) * 3", types.Int, 4, false},
		{"((SELECT 1) + (SELECT count(*) FROM (VALUES (1)) v)) * 3", types.Int, 0, false},
		{"coalesce((SELECT 1::INT8), 0)", types.Int, 0, false},
		{"coalesce((SELECT 1), 0)", types.Int, 4, false},
		{"(SELECT 1.5) + (SELECT 2)", types.Decimal, 0, false},
		{"(SELECT 1.5::FLOAT8) * (SELECT 2)", types.Float, 0, false},
		{"(SELECT 'a'::TEXT) || (SELECT 'b'::TEXT)", types.String, 0, false},
		{"(SELECT '2026-01-01'::TIMESTAMP) + INTERVAL '1 day'", types.Timestamp, 0, true},
		{"(SELECT '2026-01-01'::TIMESTAMPTZ) + INTERVAL '1 day'", types.Timestamp, 0, false},
		{"(SELECT 1) < (SELECT 2)", types.Bool, 0, false},
		{"(SELECT 1) = (SELECT 2::INT8)", types.Bool, 0, false},
		{"(SELECT 1) IS NULL", types.Bool, 0, false},
		{"CASE WHEN true THEN (SELECT 1) END", types.Int, 4, false},
		{"CASE WHEN true THEN (SELECT 1) ELSE (SELECT 2::INT8) END + 1", types.Int, 0, false},
		{"CASE WHEN (SELECT 1) > 0 THEN 'a' ELSE 'b' END", types.String, 0, false},
		{"CASE WHEN EXISTS (SELECT 1) THEN (SELECT 1.5) END", types.Decimal, 0, false},
		{"CASE (SELECT 1) WHEN 1 THEN (SELECT count(*) FROM (VALUES (1)) v) END", types.Int, 0, false},
	} {
		stmts, err := parser.Parse("SELECT " + tc.expr)
		if err != nil {
			t.Fatalf("%s: %v", tc.expr, err)
		}
		cols, serr := s.PlanColumns(context.Background(), stmts[0])
		if serr != nil || len(cols) != 1 {
			t.Fatalf("%s: %v %v", tc.expr, cols, serr)
		}
		if c := cols[0]; c.Type != tc.fam || c.Width != tc.width || c.NoTZ != tc.notz {
			t.Errorf("%s: described %v width %d notz %v, want %v width %d notz %v",
				tc.expr, c.Type, c.Width, c.NoTZ, tc.fam, tc.width, tc.notz)
		}
	}
}

// TestScalarSubNull: the NULL a scalar subquery stands as while the
// expression around it is typed has its column's family, integer width
// and time-zone flavour.
func TestScalarSubNull(t *testing.T) {
	for _, tc := range []struct {
		col   ResultColumn
		width int32
		notz  bool
	}{
		{ResultColumn{Type: types.Int, Width: 4}, 4, false},
		{ResultColumn{Type: types.Int, Width: 2}, 2, false},
		{ResultColumn{Type: types.Int}, 8, false},
		{ResultColumn{Type: types.Timestamp, NoTZ: true}, 8, true},
		{ResultColumn{Type: types.Float}, 8, false},
	} {
		d := scalarSubNull(tc.col)
		if !d.Null || d.Fam != tc.col.Type || d.IntWidth() != tc.width || d.NoTZ != tc.notz {
			t.Errorf("%+v: got %+v", tc.col, d)
		}
		e := parser.Expr{Lit: &d}
		if got := exprNoTZ(e, nil, nil); got != tc.notz {
			t.Errorf("%+v: exprNoTZ %v, want %v", tc.col, got, tc.notz)
		}
	}
}
