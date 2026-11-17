package parser

import "testing"

// TestAtTimeZone: x AT TIME ZONE z parses as timezone(z, x), binding
// tighter than the arithmetic operators and looser than ::, in either
// direction and chained; AT LOCAL names the session's setting.
func TestAtTimeZone(t *testing.T) {
	sel := parseOne(t, `SELECT now() AT TIME ZONE 'UTC', ts AT TIME ZONE 'America/New_York' + interval '1 day', '2024-01-01 12:00'::timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo', ts at time zone tz AS local_ts, ts AT LOCAL FROM t WHERE ts AT TIME ZONE 'UTC' > '2024-01-01'`).(*Select)
	if len(sel.Exprs) != 5 {
		t.Fatalf("%d select items", len(sel.Exprs))
	}
	e := sel.Exprs[0].Expr
	if e.Func != "timezone" || len(e.Args) != 2 || e.Args[0].Lit == nil || e.Args[0].Lit.S != "UTC" || e.Args[1].Func != "now" {
		t.Fatalf("now() AT TIME ZONE 'UTC': %+v", e)
	}
	e = sel.Exprs[1].Expr
	if e.Func != "timezone" || e.BinOp != "+" || e.Args[1].Column != "ts" {
		t.Fatalf("AT TIME ZONE binds tighter than +: %+v", e)
	}
	e = sel.Exprs[2].Expr
	if e.Func != "timezone" || e.Args[0].Lit.S != "Asia/Tokyo" || e.Args[1].Func != "timezone" || e.Args[1].Args[1].Cast != "timestamp" {
		t.Fatalf("chained, over a cast: %+v", e)
	}
	if e := sel.Exprs[3]; e.Expr.Func != "timezone" || e.Expr.Args[0].Column != "tz" || e.Alias != "local_ts" {
		t.Fatalf("lower case, a column zone, an alias: %+v", e)
	}
	if e := sel.Exprs[4].Expr; e.Func != "timezone" || e.Args[0].Func != "current_setting" {
		t.Fatalf("AT LOCAL: %+v", e)
	}
	if len(sel.Where) != 1 || sel.Where[0].Expr == nil || sel.Where[0].Expr.Func != "timezone" {
		t.Fatalf("AT TIME ZONE in WHERE: %+v", sel.Where)
	}
	// A column named "at" still reads as one.
	sel = parseOne(t, `SELECT at, at + interval '1 day' FROM r`).(*Select)
	if sel.Exprs[0].Expr.Column != "at" || sel.Exprs[1].Expr.Column != "at" {
		t.Fatalf("a column named at: %+v", sel.Exprs)
	}
	if _, err := Parse(`SELECT now() AT TIME 'UTC'`); err == nil {
		t.Fatal("AT TIME without ZONE accepted")
	}
}

// TestSetReturningFunctionsInFrom: the registry's set-returning
// functions are table functions in FROM, with an alias and column
// names, and call syntax in a select list.
func TestSetReturningFunctionsInFrom(t *testing.T) {
	for _, c := range []struct {
		sql, fn, alias string
		cols           int
	}{
		{`SELECT * FROM generate_series(1, 10, 2) AS g(n)`, "generate_series", "g", 1},
		{`SELECT * FROM pg_catalog.generate_series(now(), now() + interval '1 day', interval '1 hour')`, "generate_series", "generate_series", 0},
		{`SELECT key, value FROM jsonb_each('{"a":1}') e`, "jsonb_each", "e", 0},
		{`SELECT * FROM jsonb_array_elements_text('[1,2]') AS x(v)`, "jsonb_array_elements_text", "x", 1},
		{`SELECT * FROM regexp_split_to_table('a,b', ',')`, "regexp_split_to_table", "regexp_split_to_table", 0},
		{`SELECT * FROM json_object_keys('{"a":1}') k`, "json_object_keys", "k", 0},
	} {
		sel := parseOne(t, c.sql).(*Select)
		if sel.FuncTable == nil || sel.FuncTable.Func != c.fn || sel.Alias != c.alias || len(sel.FuncCols) != c.cols {
			t.Errorf("%s: %+v alias %q cols %v", c.sql, sel.FuncTable, sel.Alias, sel.FuncCols)
		}
	}
	// key (a keyword in key definitions) is jsonb_each's column.
	sel := parseOne(t, `SELECT key, value FROM jsonb_each('{"a":1}') WHERE key <> 'b' ORDER BY key`).(*Select)
	if sel.Exprs[0].Expr.Column != "key" || len(sel.Where) != 1 || len(sel.OrderBy) != 1 {
		t.Fatalf("key as a column: %+v", sel)
	}
	sel = parseOne(t, `SELECT generate_series(1, 3), regexp_replace(name, '\s+', ' ', 'g') FROM t`).(*Select)
	if sel.Exprs[0].Expr.Func != "generate_series" || len(sel.Exprs[1].Expr.Args) != 4 {
		t.Fatalf("select-list calls: %+v", sel.Exprs)
	}
	for _, bad := range []string{`SELECT generate_series(1)`, `SELECT regexp_replace('a', 'b')`, `SELECT jsonb_each()`} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s: arity accepted", bad)
		}
	}
}
