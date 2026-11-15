package parser

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// TestParseVectorType: vector and vector(n) are column types, the
// dimension is bounded, and arrays of vectors are refused.
func TestParseVectorType(t *testing.T) {
	ct := parseOne(t, `CREATE TABLE items (id INT8 PRIMARY KEY, a vector(3), b VECTOR, c vector(16000))`).(*CreateTable)
	want := map[string]int32{"a": 3, "b": 0, "c": 16000}
	for _, col := range ct.Columns[1:] {
		if col.Type != types.Vector || col.MaxLen != want[col.Name] {
			t.Errorf("column %s: type %s, dimension %d, want VECTOR(%d)", col.Name, col.Type, col.MaxLen, want[col.Name])
		}
	}
	for _, bad := range []string{
		`CREATE TABLE t (a vector(0))`,
		`CREATE TABLE t (a vector(16001))`,
		`CREATE TABLE t (a vector(3, 4))`,
		`CREATE TABLE t (a vector())`,
		`CREATE TABLE t (a vector(3)[])`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s parsed", bad)
		}
	}
}

// TestParseDistanceOperators: the three operators lex as one token
// each, bind looser than + and tighter than a comparison, and leave the
// neighbouring operators what they were.
func TestParseDistanceOperators(t *testing.T) {
	for _, op := range []string{"<->", "<=>", "<#>"} {
		sel := parseOne(t, `SELECT id FROM items ORDER BY embedding `+op+` $1 LIMIT 5`).(*Select)
		oc := sel.OrderBy[0]
		if oc.Expr == nil || oc.Expr.Column != "embedding" || oc.Expr.BinOp != op || oc.Expr.Right == nil || oc.Expr.Right.Param != 1 {
			t.Fatalf("%s: ORDER BY parsed as %+v", op, oc.Expr)
		}
		if sel.Limit != 5 {
			t.Fatalf("%s: limit %d", op, sel.Limit)
		}
	}
	// a <-> b + c is a <-> (b + c).
	sel := parseOne(t, `SELECT a <-> b + c FROM t`).(*Select)
	e := sel.Exprs[0].Expr
	if e.BinOp != "<->" || e.Right == nil || e.Right.BinOp != "+" {
		t.Fatalf("a <-> b + c parsed as %s", FormatExpr(e))
	}
	// a + b <-> c is (a + b) <-> c.
	sel = parseOne(t, `SELECT a + b <-> c FROM t`).(*Select)
	e = sel.Exprs[0].Expr
	if e.BinOp != "<->" || e.Left == nil || e.Left.BinOp != "+" {
		t.Fatalf("a + b <-> c parsed as %s", FormatExpr(e))
	}
	// The distance is the comparison's left-hand side.
	sel = parseOne(t, `SELECT id FROM items WHERE embedding <-> '[1,2]' < 0.5`).(*Select)
	if len(sel.Where) != 1 || sel.Where[0].Op != "<" || sel.Where[0].Expr == nil || sel.Where[0].Expr.BinOp != "<->" {
		t.Fatalf("distance in WHERE parsed as %+v", sel.Where)
	}
	// Their neighbours: <= and <>, a < -b, and the jsonb #>.
	sel = parseOne(t, `SELECT id FROM t WHERE a <= 1 AND b <> 2 AND c < -3 AND d #> '{x}' = '1'`).(*Select)
	ops := []string{sel.Where[0].Op, sel.Where[1].Op, sel.Where[2].Op}
	if strings.Join(ops, " ") != "<= != <" {
		t.Fatalf("neighbouring operators parsed as %v", ops)
	}
}

// TestParseVectorDDL: CREATE INDEX ... USING ivfflat with an operator
// class and WITH parameters, CREATE EXTENSION, and the qualified
// setting name.
func TestParseVectorDDL(t *testing.T) {
	ci := parseOne(t, `CREATE INDEX items_ivf ON items USING ivfflat (embedding vector_cosine_ops) WITH (lists = 200)`).(*CreateIndex)
	if ci.Using != "ivfflat" || len(ci.Columns) != 1 || ci.OpClasses[0] != "vector_cosine_ops" || ci.With["lists"] != "200" {
		t.Fatalf("CREATE INDEX parsed as %+v", ci)
	}
	ci = parseOne(t, `CREATE INDEX i ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64)`).(*CreateIndex)
	if ci.Using != "hnsw" || ci.With["m"] != "16" || ci.With["ef_construction"] != "64" {
		t.Fatalf("hnsw index parsed as %+v", ci)
	}
	// The full-text expression index parses beside it, with no
	// parameters, and both forms combine.
	if ci = parseOne(t, `CREATE INDEX d ON docs USING GIN (to_tsvector('english', body))`).(*CreateIndex); ci.ExprFunc != "to_tsvector" || ci.With != nil {
		t.Fatalf("expression index parsed as %+v", ci)
	}
	if ci = parseOne(t, `CREATE INDEX d ON docs USING GIN (to_tsvector('english', body)) WITH (fastupdate = off)`).(*CreateIndex); ci.ExprFunc != "to_tsvector" || ci.With["fastupdate"] != "off" {
		t.Fatalf("expression index with parameters parsed as %+v", ci)
	}
	if ci = parseOne(t, `CREATE INDEX i ON items (a, b)`).(*CreateIndex); ci.With != nil || ci.Using != "" {
		t.Fatalf("a plain index gained parameters: %+v", ci)
	}
	for _, bad := range []string{
		`CREATE INDEX i ON items USING ivfflat (e) WITH ()`,
		`CREATE INDEX i ON items USING ivfflat (e) WITH (lists)`,
		`CREATE INDEX i ON items USING ivfflat (e) WITH (lists = 1, lists = 2)`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s parsed", bad)
		}
	}
	for src, name := range map[string]string{
		`CREATE EXTENSION vector`:                                    "vector",
		`CREATE EXTENSION IF NOT EXISTS vector`:                      "vector",
		`CREATE EXTENSION IF NOT EXISTS "vector" WITH SCHEMA public`: "vector",
		`CREATE EXTENSION pg_trgm`:                                   "pg_trgm",
	} {
		if ce := parseOne(t, src).(*CreateExtension); ce.Name != name {
			t.Errorf("%s: extension %q", src, ce.Name)
		}
	}
	if sv := parseOne(t, `SET ivfflat.probes = 10`).(*SetVar); sv.Name != "ivfflat.probes" || sv.Value != "10" {
		t.Fatalf("SET parsed as %+v", sv)
	}
	if sv := parseOne(t, `SET LOCAL ivfflat.probes TO 3`).(*SetVar); sv.Name != "ivfflat.probes" || sv.Value != "3" || !sv.Local {
		t.Fatalf("SET LOCAL parsed as %+v", sv)
	}
	if sv := parseOne(t, `SHOW ivfflat.probes`).(*SetVar); sv.Name != "show:ivfflat.probes" {
		t.Fatalf("SHOW parsed as %+v", sv)
	}
	if sv := parseOne(t, `RESET ivfflat.probes`).(*SetVar); sv.Name != "ivfflat.probes" || !sv.Reset {
		t.Fatalf("RESET parsed as %+v", sv)
	}
}
