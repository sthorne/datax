package parser

import (
	"errors"
	"testing"

	"github.com/sthorne/datax/pkg/sql/builtins"
)

// Parser coverage of gaps PG-Q-013, PG-Q-015, PG-Q-017, PG-Q-023 and
// PG-Q-024 (docs/compatibility-gaps.md).

func parseCode(t *testing.T, src, code, msg string) {
	t.Helper()
	_, err := Parse(src)
	var se *SyntaxError
	if !errors.As(err, &se) {
		t.Fatalf("%s: got %v, want a syntax error", src, err)
	}
	if se.Code != code || (msg != "" && se.Msg != msg) {
		t.Fatalf("%s: got [%s] %s, want [%s] %s", src, se.Code, se.Msg, code, msg)
	}
}

func TestQuantifiedParse(t *testing.T) {
	sel := parseOne(t, `SELECT * FROM t WHERE a > ALL (SELECT b FROM u)`).(*Select)
	if c := sel.Where[0]; c.Op != "> ALL" || c.Value.Func != "array" || c.Value.Args[0].Sub == nil {
		t.Fatalf("> ALL (SELECT ...): %+v", c)
	}
	sel = parseOne(t, `SELECT * FROM t WHERE a <= SOME (SELECT b FROM u)`).(*Select)
	if c := sel.Where[0]; c.Op != "<= ANY" {
		t.Fatalf("<= SOME: %+v", c)
	}
	// = ANY and <> ALL stay IN and NOT IN.
	sel = parseOne(t, `SELECT * FROM t WHERE a = ANY (SELECT b FROM u) AND a <> ALL (SELECT b FROM u)`).(*Select)
	if sel.Where[0].Op != "IN" || sel.Where[1].Op != "NOT IN" {
		t.Fatalf("IN forms: %+v", sel.Where)
	}
	// NOT over a quantified comparison: the base negated, the
	// quantifier flipped.
	for src, want := range map[string]string{
		`SELECT * FROM t WHERE NOT (a = ANY (ARRAY[1]))`:          "!= ALL",
		`SELECT * FROM t WHERE NOT (a < ALL (SELECT b FROM u))`:   ">= ANY",
		`SELECT * FROM t WHERE NOT (a LIKE ANY (ARRAY['x%']))`:    "NOT LIKE ALL",
		`SELECT * FROM t WHERE NOT (a NOT LIKE ALL (ARRAY['x']))`: "LIKE ANY",
	} {
		if got := parseOne(t, src).(*Select).Where[0].Op; got != want {
			t.Errorf("%s: op %q, want %q", src, got, want)
		}
	}
	parseCode(t, `SELECT 1 > ALL (SELECT 1, 2)`, "42601", "subquery has too many columns")
}

func TestTableAndValuesParse(t *testing.T) {
	sel := parseOne(t, `TABLE ONLY public.t ORDER BY 1 LIMIT 2`).(*Select)
	if sel.Table != "public.t" || len(sel.Exprs) != 1 || !sel.Exprs[0].Star || sel.Limit != 2 || len(sel.OrderBy) != 1 {
		t.Fatalf("TABLE: %+v", sel)
	}
	sel = parseOne(t, `TABLE a UNION TABLE b`).(*Select)
	if sel.Table != "a" || sel.Union == nil || sel.Union.Table != "b" {
		t.Fatalf("TABLE UNION TABLE: %+v", sel)
	}
	sel = parseOne(t, `SELECT * FROM (TABLE t) s WHERE x IN (TABLE u)`).(*Select)
	if sel.Derived == nil || sel.Derived.Table != "t" || sel.Where[0].Sub == nil || sel.Where[0].Sub.Table != "u" {
		t.Fatalf("(TABLE t): %+v", sel)
	}
	ins := parseOne(t, `INSERT INTO t TABLE u`).(*Insert)
	if ins.Select == nil || ins.Select.Table != "u" {
		t.Fatalf("INSERT ... TABLE: %+v", ins)
	}
	parseCode(t, `VALUES (1, 2), (3)`, "42601", "VALUES lists must all be the same length")
	parseCode(t, `SELECT * FROM (VALUES (1, 2)) v(a, b, c)`, "42P10", `table "v" has 2 columns available but 3 columns specified`)
	sel = parseOne(t, `VALUES (1), (2)`).(*Select)
	if !sel.Values || !sel.Union.Values {
		t.Fatalf("VALUES rows not marked: %+v", sel)
	}
	// A LATERAL VALUES list of several rows reads as lockstep unnests.
	sel = parseOne(t, `SELECT * FROM t, LATERAL (VALUES ('a', t.x), ('b', t.y)) v(k, val)`).(*Select)
	d := sel.Joins[0].Derived
	if d.Union != nil || len(d.Exprs) != 2 || d.Exprs[1].Alias != "val" || d.Exprs[1].Expr.Func != "unnest" ||
		len(d.Exprs[1].Expr.Args[0].Args) != 2 || d.Exprs[1].Expr.Args[0].Args[1].Column != "t.y" {
		t.Fatalf("LATERAL VALUES: %+v", d)
	}
}

func TestMultiColumnSetParse(t *testing.T) {
	up := parseOne(t, `UPDATE t SET x = 1, (a, b) = (2, DEFAULT), (c) = ROW(3), (d, e) = (SELECT 4, 5)`).(*Update)
	if len(up.Set) != 6 {
		t.Fatalf("SET: %+v", up.Set)
	}
	if up.Set[1].Column != "a" || up.Set[1].Value.Lit == nil || up.Set[2].Column != "b" || !up.Set[2].Value.IsDefault ||
		up.Set[3].Column != "c" || up.Set[3].Value.Lit == nil {
		t.Fatalf("row forms: %+v", up.Set)
	}
	if s4, s5 := up.Set[4].Value, up.Set[5].Value; s4.Sub == nil || s4.Sub != s5.Sub || s4.SubCol != 1 || s5.SubCol != 2 || s5.SubCols != 2 {
		t.Fatalf("subquery form: %+v %+v", s4, s5)
	}
	ins := parseOne(t, `INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET (a, b) = (excluded.a, 'x')`).(*Insert)
	if len(ins.OnConflict.Set) != 2 || ins.OnConflict.Set[0].Value.Column != "excluded.a" {
		t.Fatalf("ON CONFLICT: %+v", ins.OnConflict.Set)
	}
	parseCode(t, `UPDATE t SET (a, b) = (1, 2, 3)`, "42601", "number of columns does not match number of values")
	parseCode(t, `UPDATE t SET (a, b) = ROW(1)`, "42601", "number of columns does not match number of values")
	parseCode(t, `UPDATE t SET (a, b) = (1)`, "42601", "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression")
	parseCode(t, `UPDATE t SET (a) = 1`, "42601", "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression")
}

func TestCollateParse(t *testing.T) {
	for _, src := range []string{
		`SELECT b FROM t ORDER BY b COLLATE "C" DESC`,
		`SELECT b COLLATE "POSIX" FROM t WHERE b COLLATE "default" < 'x' COLLATE ucs_basic`,
		`SELECT min(b COLLATE pg_catalog."C") FROM t GROUP BY b COLLATE "C"`,
		`SELECT (a || b) COLLATE "C"::text FROM t`,
	} {
		parseOne(t, src)
	}
	parseCode(t, `SELECT b COLLATE "en_US" FROM t`, "42704", `collation "en_US" for encoding "UTF8" does not exist`)
	parseCode(t, `SELECT b COLLATE C FROM t`, "42704", `collation "c" for encoding "UTF8" does not exist`)
	parseCode(t, `SELECT b COLLATE public."C" FROM t`, "42704", `collation "public.C" for encoding "UTF8" does not exist`)
	parseCode(t, `SELECT 1 COLLATE "C"`, "42804", "collations are not supported by type integer")
}

func TestDiscardParse(t *testing.T) {
	for src, want := range map[string]string{
		"DISCARD ALL": "all", "discard plans": "plans", "DISCARD SEQUENCES": "sequences",
		"DISCARD TEMP": "temp", "DISCARD TEMPORARY;": "temp",
	} {
		if d, ok := parseOne(t, src).(*Discard); !ok || d.Kind != want {
			t.Errorf("%s: %+v", src, d)
		}
	}
	parseCode(t, "DISCARD everything", "42601", `syntax error at or near "everything"`)
}

// TestMultiColumnSetWithSubscripts mixes the multiple-column form
// (PG-Q-017) with jsonb subscripted assignments (PG-F-005) in one SET
// list: both parse, and the subscripted ones still fold into one
// assignment per column.
func TestMultiColumnSetWithSubscripts(t *testing.T) {
	up := parseOne(t, `UPDATE t SET (a, b) = (1, 2), j['x'] = '1', j['y'][0] = '2', c = 3`).(*Update)
	if len(up.Set) != 4 {
		t.Fatalf("SET: %+v", up.Set)
	}
	if up.Set[0].Column != "a" || up.Set[1].Column != "b" || up.Set[3].Column != "c" {
		t.Fatalf("columns: %+v", up.Set)
	}
	if j := up.Set[2]; j.Column != "j" || j.Value.Func != builtins.SubscriptAssignFunc || len(j.Value.Args) == 0 || j.Value.Args[0].Func != builtins.SubscriptAssignFunc {
		t.Fatalf("subscripted j: %+v", j)
	}
	if _, err := Parse(`UPDATE t SET (a, j['x']) = (1, 2)`); err == nil {
		t.Fatal("a subscript inside a multiple-column target list must be refused")
	}
}
