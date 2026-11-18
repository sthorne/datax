package testcluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Issue #363: JOIN ... ON takes any boolean condition — ON true, non-equi
// conditions, OR, conditions on one side only, expressions — for INNER,
// LEFT, RIGHT and FULL joins. The expected rows come from a small
// reference evaluator of PostgreSQL's join semantics (an outer join's ON
// decides only what matches, never which preserved rows survive; SQL
// comparisons with NULL are never true), cross-checked by hand-written
// cases.

// jrec is one fixture row (nil = SQL NULL); jtup is a joined row by
// alias, a nil jrec being a NULL extension.
type (
	jrec map[string]any
	jtup map[string]jrec
)

func (t jtup) get(alias, col string) any {
	if r := t[alias]; r != nil {
		return r[col]
	}
	return nil
}

// jcmp compares two non-NULL fixture values; ok is false when either is
// NULL (the comparison is then not true, whatever the operator).
func jcmp(a, b any) (c int, ok bool) {
	if a == nil || b == nil {
		return 0, false
	}
	switch x := a.(type) {
	case int64:
		y := b.(int64)
		switch {
		case x < y:
			return -1, true
		case x > y:
			return 1, true
		}
		return 0, true
	case string:
		return strings.Compare(x, b.(string)), true
	}
	panic(fmt.Sprintf("jcmp %T", a))
}

func jlt(a, b any) bool { c, ok := jcmp(a, b); return ok && c < 0 }
func jle(a, b any) bool { c, ok := jcmp(a, b); return ok && c <= 0 }
func jgt(a, b any) bool { c, ok := jcmp(a, b); return ok && c > 0 }
func jge(a, b any) bool { c, ok := jcmp(a, b); return ok && c >= 0 }
func jeq(a, b any) bool { c, ok := jcmp(a, b); return ok && c == 0 }
func jne(a, b any) bool { c, ok := jcmp(a, b); return ok && c != 0 }

func jlower(a any) any {
	if a == nil {
		return nil
	}
	return strings.ToLower(a.(string))
}

// refJoin joins rows of a new member (alias) onto left, PostgreSQL
// style: kind is inner, left, right or full; earlier lists the aliases
// already joined (NULL-extended for the new member's unmatched rows).
func refJoin(left []jtup, earlier []string, right []jrec, alias, kind string, on func(jtup) bool) []jtup {
	var out []jtup
	matchedR := make([]bool, len(right))
	for _, l := range left {
		matched := false
		for j, r := range right {
			t := jtup{alias: r}
			for k, v := range l {
				t[k] = v
			}
			if on(t) {
				out = append(out, t)
				matched = true
				matchedR[j] = true
			}
		}
		if !matched && (kind == "left" || kind == "full") {
			t := jtup{alias: nil}
			for k, v := range l {
				t[k] = v
			}
			out = append(out, t)
		}
	}
	if kind == "right" || kind == "full" {
		for j, r := range right {
			if matchedR[j] {
				continue
			}
			t := jtup{alias: r}
			for _, a := range earlier {
				t[a] = nil
			}
			out = append(out, t)
		}
	}
	return out
}

// refRender renders each joined row's alias.id values, ordered like
// ORDER BY 1, 2, ... (NULLS LAST).
func refRender(rows []jtup, aliases ...string) string {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, a := range aliases {
			x, y := rows[i].get(a, "id"), rows[j].get(a, "id")
			switch {
			case x == nil && y == nil:
				continue
			case x == nil:
				return false
			case y == nil:
				return true
			}
			if c, _ := jcmp(x, y); c != 0 {
				return c < 0
			}
		}
		return false
	})
	var out []string
	for _, r := range rows {
		var cells []string
		for _, a := range aliases {
			cells = append(cells, jcell(r.get(a, "id")))
		}
		out = append(out, strings.Join(cells, ":"))
	}
	return strings.Join(out, " ")
}

func jcell(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(v)
}

func toTups(alias string, rows []jrec) []jtup {
	out := make([]jtup, len(rows))
	for i, r := range rows {
		out[i] = jtup{alias: r}
	}
	return out
}

func TestJoinOnAnyCondition(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// rows renders a result: cells joined by ":", rows by " ".
	rows := func(q string, args ...any) string {
		t.Helper()
		rs, err := conn.Query(ctx, q, args...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		defer rs.Close()
		var out []string
		for rs.Next() {
			vals, err := rs.Values()
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			var cells []string
			for _, v := range vals {
				cells = append(cells, jcell(v))
			}
			out = append(out, strings.Join(cells, ":"))
		}
		if err := rs.Err(); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return strings.Join(out, " ")
	}
	eq := func(what, got, want string) {
		t.Helper()
		if got != want {
			t.Fatalf("%s:\n got  %q\n want %q", what, got, want)
		}
	}
	wantErr := func(q, code, msg string) {
		t.Helper()
		_, err := conn.Exec(ctx, q)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != code || pgErr.Message != msg {
			t.Fatalf("%s: got %v, want %s %s", q, err, code, msg)
		}
	}

	// NULLs on both sides, in every column a condition reads; ea and eb
	// are empty copies of a and b.
	exec(`CREATE TABLE a (id INT8 PRIMARY KEY, x INT8, s TEXT)`)
	exec(`CREATE TABLE b (id INT8 PRIMARY KEY, y INT8, lo INT8, hi INT8, ok BOOL, s TEXT)`)
	exec(`CREATE TABLE c (id INT8 PRIMARY KEY, k INT8)`)
	exec(`CREATE TABLE ea (id INT8 PRIMARY KEY, x INT8, s TEXT)`)
	exec(`CREATE TABLE eb (id INT8 PRIMARY KEY, y INT8, lo INT8, hi INT8, ok BOOL, s TEXT)`)
	exec(`INSERT INTO a VALUES (1, 1, 'Ab'), (2, 2, 'cd'), (3, NULL, NULL), (4, 5, 'zz')`)
	exec(`INSERT INTO b VALUES (1, 1, 0, 1, true, 'ab'), (2, 3, 2, 4, false, 'CD'), (3, NULL, NULL, NULL, NULL, NULL), (4, 2, 1, 3, true, 'x')`)
	exec(`INSERT INTO c VALUES (1, 1), (2, NULL), (3, 4)`)
	i := func(v int64) any { return v }
	aRows := []jrec{
		{"id": i(1), "x": i(1), "s": "Ab"},
		{"id": i(2), "x": i(2), "s": "cd"},
		{"id": i(3), "x": nil, "s": nil},
		{"id": i(4), "x": i(5), "s": "zz"},
	}
	bRows := []jrec{
		{"id": i(1), "y": i(1), "lo": i(0), "hi": i(1), "ok": true, "s": "ab"},
		{"id": i(2), "y": i(3), "lo": i(2), "hi": i(4), "ok": false, "s": "CD"},
		{"id": i(3), "y": nil, "lo": nil, "hi": nil, "ok": nil, "s": nil},
		{"id": i(4), "y": i(2), "lo": i(1), "hi": i(3), "ok": true, "s": "x"},
	}
	cRows := []jrec{{"id": i(1), "k": i(1)}, {"id": i(2), "k": nil}, {"id": i(3), "k": i(4)}}
	isTrue := func(v any) bool { b, ok := v.(bool); return ok && b }

	conds := []struct {
		sql string
		on  func(jtup) bool
	}{
		{"true", func(jtup) bool { return true }},
		{"false", func(jtup) bool { return false }},
		{"b.y = a.x", func(t jtup) bool { return jeq(t.get("b", "y"), t.get("a", "x")) }},
		{"b.y = a.x AND b.ok", func(t jtup) bool { return jeq(t.get("b", "y"), t.get("a", "x")) && isTrue(t.get("b", "ok")) }},
		{"b.id = a.x AND b.lo < a.id", func(t jtup) bool {
			return jeq(t.get("b", "id"), t.get("a", "x")) && jlt(t.get("b", "lo"), t.get("a", "id"))
		}},
		{"b.y < a.x", func(t jtup) bool { return jlt(t.get("b", "y"), t.get("a", "x")) }},
		{"a.x <> b.y", func(t jtup) bool { return jne(t.get("a", "x"), t.get("b", "y")) }},
		{"a.x BETWEEN b.lo AND b.hi", func(t jtup) bool {
			return jge(t.get("a", "x"), t.get("b", "lo")) && jle(t.get("a", "x"), t.get("b", "hi"))
		}},
		{"b.y = a.x OR b.lo = a.x", func(t jtup) bool {
			return jeq(t.get("b", "y"), t.get("a", "x")) || jeq(t.get("b", "lo"), t.get("a", "x"))
		}},
		{"b.y > a.x OR b.ok", func(t jtup) bool { return jgt(t.get("b", "y"), t.get("a", "x")) || isTrue(t.get("b", "ok")) }},
		{"b.ok", func(t jtup) bool { return isTrue(t.get("b", "ok")) }},
		{"a.x > 1", func(t jtup) bool { return jgt(t.get("a", "x"), i(1)) }},
		{"b.lo = b.hi - 1", func(t jtup) bool {
			lo, hi := t.get("b", "lo"), t.get("b", "hi")
			return hi != nil && jeq(lo, hi.(int64)-1)
		}},
		{"b.y = b.id", func(t jtup) bool { return jeq(t.get("b", "y"), t.get("b", "id")) }},
		{"a.x = a.id AND b.y IS NULL", func(t jtup) bool {
			return jeq(t.get("a", "x"), t.get("a", "id")) && t.get("b", "y") == nil
		}},
		{"lower(a.s) = lower(b.s)", func(t jtup) bool { return jeq(jlower(t.get("a", "s")), jlower(t.get("b", "s"))) }},
		{"NOT (b.y = a.x)", func(t jtup) bool { return jne(t.get("b", "y"), t.get("a", "x")) }},
	}
	sides := []struct {
		at, bt string
		ar, br []jrec
	}{
		{"a", "b", aRows, bRows},
		{"a", "eb", aRows, nil},
		{"ea", "b", nil, bRows},
		{"ea", "eb", nil, nil},
	}
	for _, sd := range sides {
		for _, kind := range []string{"inner", "left", "right", "full"} {
			for _, c := range conds {
				q := fmt.Sprintf(`SELECT a.id, b.id FROM %s AS a %s JOIN %s AS b ON %s ORDER BY 1, 2`, sd.at, strings.ToUpper(kind), sd.bt, c.sql)
				want := refRender(refJoin(toTups("a", sd.ar), []string{"a"}, sd.br, "b", kind, c.on), "a", "b")
				eq(q, rows(q), want)
			}
		}
	}

	// Hand-checked anchors for the reference evaluator.
	eq("ON true is a cross join", rows(`SELECT a.id, b.id FROM a JOIN b ON true WHERE a.id < 3 ORDER BY 1, 2`),
		"1:1 1:2 1:3 1:4 2:1 2:2 2:3 2:4")
	eq("FULL non-equi", rows(`SELECT a.id, b.id FROM a FULL JOIN b ON b.y < a.x ORDER BY 1, 2`),
		"1:NULL 2:1 3:NULL 4:1 4:2 4:4 NULL:3")
	eq("LEFT ON filters the match only", rows(`SELECT a.id, b.id FROM a LEFT JOIN b ON a.x > 1 AND b.ok ORDER BY 1, 2`),
		"1:NULL 2:1 2:4 3:NULL 4:1 4:4")
	eq("RIGHT BETWEEN", rows(`SELECT a.id, b.id FROM a RIGHT JOIN b ON a.x BETWEEN b.lo AND b.hi ORDER BY 2, 1`),
		"1:1 2:2 NULL:3 1:4 2:4")
	eq("WHERE after a LEFT non-equi join (anti-join)", rows(`SELECT a.id FROM a LEFT JOIN b ON b.y < a.x WHERE b.id IS NULL ORDER BY 1`),
		"1 3")
	eq("aggregate over an OR join", rows(`SELECT a.id, count(b.id) FROM a LEFT JOIN b ON b.y > a.x OR b.ok GROUP BY a.id ORDER BY 1`),
		"1:3 2:3 3:2 4:2")

	// Three-way chains, each level any condition, outer joins mixed.
	chains := []struct {
		sql      string
		k1, k2   string
		on1, on2 func(jtup) bool
	}{
		{`a JOIN b ON b.y < a.x JOIN c ON c.k > b.y`, "inner", "inner",
			func(t jtup) bool { return jlt(t.get("b", "y"), t.get("a", "x")) },
			func(t jtup) bool { return jgt(t.get("c", "k"), t.get("b", "y")) }},
		{`a FULL JOIN b ON b.y > a.x LEFT JOIN c ON c.k < b.id`, "full", "left",
			func(t jtup) bool { return jgt(t.get("b", "y"), t.get("a", "x")) },
			func(t jtup) bool { return jlt(t.get("c", "k"), t.get("b", "id")) }},
		{`a LEFT JOIN b ON b.y < a.x RIGHT JOIN c ON c.k >= a.x OR c.k IS NULL`, "left", "right",
			func(t jtup) bool { return jlt(t.get("b", "y"), t.get("a", "x")) },
			func(t jtup) bool { return jge(t.get("c", "k"), t.get("a", "x")) || t.get("c", "k") == nil }},
		{`a FULL JOIN b ON b.y = a.x FULL JOIN c ON c.k <> b.id`, "full", "full",
			func(t jtup) bool { return jeq(t.get("b", "y"), t.get("a", "x")) },
			func(t jtup) bool { return jne(t.get("c", "k"), t.get("b", "id")) }},
		{`a JOIN b ON true JOIN c ON c.k = b.y AND c.id < a.id`, "inner", "inner",
			func(jtup) bool { return true },
			func(t jtup) bool {
				return jeq(t.get("c", "k"), t.get("b", "y")) && jlt(t.get("c", "id"), t.get("a", "id"))
			}},
		{`a RIGHT JOIN b ON a.x = b.id FULL JOIN c ON c.k = a.x OR c.k = b.y`, "right", "full",
			func(t jtup) bool { return jeq(t.get("a", "x"), t.get("b", "id")) },
			func(t jtup) bool {
				return jeq(t.get("c", "k"), t.get("a", "x")) || jeq(t.get("c", "k"), t.get("b", "y"))
			}},
	}
	for _, ch := range chains {
		q := `SELECT a.id, b.id, c.id FROM ` + ch.sql + ` ORDER BY 1, 2, 3`
		lvl := refJoin(toTups("a", aRows), []string{"a"}, bRows, "b", ch.k1, ch.on1)
		lvl = refJoin(lvl, []string{"a", "b"}, cRows, "c", ch.k2, ch.on2)
		eq(q, rows(q), refRender(lvl, "a", "b", "c"))
	}

	// An OR of equalities on one column parses to IN: its members may
	// read the joined row, in ON and in WHERE.
	eq("ON IN over joined columns", rows(`SELECT a.id, b.id FROM a JOIN b ON b.y IN (a.x, a.id + 1) ORDER BY 1, 2`), "1:1 1:4 2:2 2:4")
	eq("WHERE OR over joined columns", rows(`SELECT a.id, b.id FROM a, b WHERE a.x = b.y OR a.x = b.lo ORDER BY 1, 2`), "1:1 1:4 2:2 2:4")

	// Mixed with LATERAL: a lateral member's columns in a later ON, and
	// a lateral member after a non-equi join.
	eq("LEFT JOIN ON a lateral column", rows(`SELECT a.id, l.m, b.id FROM a CROSS JOIN LATERAL (SELECT a.id * 2 AS m) l LEFT JOIN b ON b.hi >= l.m ORDER BY 1, 3`),
		"1:2:2 1:2:4 2:4:2 3:6:NULL 4:8:NULL")
	eq("lateral after a non-equi join", rows(`SELECT a.id, b.id, l.n FROM a JOIN b ON b.y <= a.x JOIN LATERAL (SELECT count(*) AS n FROM c WHERE c.k = b.id) l ON l.n > 0 ORDER BY 1, 2`),
		"1:1:1 2:1:1 2:4:1 4:1:1 4:4:1")

	// Correlated references inside ON: an enclosing row's column in a
	// filter conjunct, and as the only ON equality (once refused).
	eq("correlated ON filter", rows(`SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON c.k > a.x AND c.id = b.id) ORDER BY 1`), "1 2")
	eq("correlated ON equality", rows(`SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON c.k = a.x) ORDER BY 1`), "1")
	eq("correlated ON in the select list", rows(`SELECT a.id, (SELECT count(*) FROM b JOIN c ON c.k > b.y AND b.lo < a.x) FROM a ORDER BY 1`),
		"1:1 2:2 3:0 4:3")

	// Parameters in ON, over the extended protocol, prepared and rerun.
	eq("param in a non-equi ON", rows(`SELECT a.id, b.id FROM a JOIN b ON b.y < a.x AND b.lo >= $1 ORDER BY 1, 2`, int64(1)), "4:2 4:4")
	eq("param in a single-side LEFT ON", rows(`SELECT a.id, b.id FROM a LEFT JOIN b ON b.y = $1 ORDER BY 1, 2`, int64(3)), "1:2 2:2 3:2 4:2")
	if _, err := conn.Prepare(ctx, "fullp", `SELECT a.id, b.id FROM a FULL JOIN b ON a.x > $1 AND b.id <= $2 ORDER BY 1, 2`); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	eq("prepared FULL ON params", rows("fullp", int64(1), int64(2)), "1:NULL 2:1 2:2 3:NULL 4:1 4:2 NULL:3 NULL:4")
	eq("prepared FULL ON params, rerun", rows("fullp", int64(4), int64(9)), "1:NULL 2:NULL 3:NULL 4:1 4:2 4:3 4:4")

	// EXPLAIN names the method: a key lookup per row with the rest as a
	// join filter, or a nested loop over a side fetched once.
	plan := rows(`EXPLAIN SELECT a.id, b.id FROM a JOIN b ON b.id = a.x AND b.lo < a.id`)
	if !strings.Contains(plan, "inner (b) per outer row: point lookup on primary key + join filter") {
		t.Fatalf("equi + residual plan: %s", plan)
	}
	plan = rows(`EXPLAIN SELECT a.id, b.id FROM a FULL JOIN b ON b.y < a.x`)
	if !strings.Contains(plan, "nested loop full join") || !strings.Contains(plan, "inner (b) fetched once, every row tried: full table scan + join filter + unmatched b rows appended") {
		t.Fatalf("non-equi plan: %s", plan)
	}

	// An ON clause sees only the members joined so far.
	wantErr(`SELECT 1 FROM a JOIN b ON c.k = a.x JOIN c ON true`, "42P01", `invalid reference to FROM-clause entry for table "c"`)
	wantErr(`SELECT 1 FROM a JOIN b ON c.k < a.x JOIN c ON true`, "42P01", `invalid reference to FROM-clause entry for table "c"`)
	wantErr(`SELECT 1 FROM a JOIN b ON k = 1 JOIN c ON true`, "42703", `column "k" does not exist`)
	// ...and a name a later member would make ambiguous is not.
	eq("unqualified name resolved in the ON scope", rows(`SELECT count(*) FROM a JOIN b ON x = 2 JOIN a AS a2 ON true`), "16")

	// A RIGHT or FULL join from an always-empty catalog keeps the joined
	// rows.
	eq("RIGHT JOIN from an empty catalog", rows(`SELECT a.id, p.prrelid FROM pg_publication_rel p RIGHT JOIN a ON true ORDER BY 1`),
		"1:NULL 2:NULL 3:NULL 4:NULL")

	// UPDATE ... FROM and DELETE ... USING with a non-equi JOIN in the
	// list (SELECT's join code).
	exec(`CREATE TABLE d (id INT8 PRIMARY KEY, v INT8)`)
	exec(`INSERT INTO d VALUES (1, 0), (2, 0), (3, 0)`)
	eq("UPDATE ... FROM a non-equi JOIN", rows(`UPDATE d SET v = c.id * 10 + b.id FROM b JOIN c ON c.k > b.y WHERE d.id = b.id RETURNING d.id, d.v`),
		"1:31 2:32")
	eq("DELETE ... USING a LEFT non-equi JOIN", rows(`DELETE FROM d USING b LEFT JOIN c ON c.k < b.y WHERE d.id = b.id AND c.id IS NULL RETURNING d.id`),
		"1 3")
	eq("after DML", rows(`SELECT id, v FROM d ORDER BY id`), "2:32")
	exec(`INSERT INTO d VALUES (1, 0), (3, 0)`)
	eq("UPDATE ... FROM a JOIN ON true", rows(`UPDATE d SET v = 7 FROM b JOIN c ON true WHERE d.id = c.k RETURNING d.id`), "1")

	// The nested loop's output counts against statement_memory_limit.
	var vals []string
	for n := 1; n <= 400; n++ {
		vals = append(vals, fmt.Sprintf("(%d)", n))
	}
	exec(`CREATE TABLE g (id INT8 PRIMARY KEY)`)
	exec(`INSERT INTO g VALUES ` + strings.Join(vals, ", "))
	eq("non-equi self join", rows(`SELECT count(*) FROM g g1 JOIN g g2 ON g1.id < g2.id`), "79800")
	exec(`SET statement_memory_limit = '1MB'`)
	_, err = conn.Exec(ctx, `SELECT count(*) FROM g g1 JOIN g g2 ON g1.id < g2.id`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "53200" {
		t.Fatalf("non-equi join over the memory limit: %v (want 53200)", err)
	}
	exec(`RESET statement_memory_limit`)
}
