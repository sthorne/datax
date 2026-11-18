package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// Gaps PG-Q-008, PG-Q-013, PG-Q-015, PG-Q-017 and PG-Q-023 of
// docs/compatibility-gaps.md: every expected result is PostgreSQL's.

// TestFromlessAggregates: an aggregate in a SELECT without FROM groups
// the one row such a select reads (PG-Q-008), with WHERE, HAVING and
// over the extended protocol.
func TestFromlessAggregates(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	runGroupedCases(t, ctx, s, []groupedCase{
		{sql: `SELECT count(*)`, want: "1"},
		{sql: `SELECT max(1), sum(2), min('a'), count(NULL), bool_and(true)`, want: "1|2|a|0|t"},
		{sql: `SELECT count(*) + 1 AS c, string_agg('x', ',')`, want: "2|x"},
		{sql: `SELECT sum(2) HAVING sum(2) > 1`, want: "2"},
		{sql: `SELECT sum(2) HAVING sum(2) > 5`, want: ""},
		{sql: `SELECT count(*) WHERE false`, want: "0"},
		{sql: `SELECT max(1) WHERE false`, want: "NULL"},
		{sql: `SELECT 1 HAVING 1 > 0`, want: "1"},
		{sql: `SELECT 1 HAVING 1 > 2`, want: ""},
		{sql: `SELECT 1 AS one GROUP BY 1`, want: "1"},
		{sql: `SELECT (SELECT count(*)) + (SELECT max(5))`, want: "6"},
		{sql: `SELECT count(*) FROM (SELECT 1) s`, want: "1"},
	})

	conn := pgConn(t, ctx, tc, 0)
	var n int64
	var m *int64
	if err := conn.QueryRow(ctx, `SELECT count(*), max($1::int8) WHERE $2::bool`, 5, false).Scan(&n, &m); err != nil {
		t.Fatal(err)
	}
	if n != 0 || m != nil {
		t.Fatalf("extended protocol: count %d, max %v", n, m)
	}
}

// TestQuantifiedComparisons: <, <=, >, >=, = and <> with ANY / SOME /
// ALL over subqueries and arrays (PG-Q-013), with PostgreSQL's NULL
// semantics and its answer over an empty subquery.
func TestQuantifiedComparisons(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE q (id INT8 PRIMARY KEY, a INT8, b TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO q VALUES (1, 10, 'abc'), (2, 20, 'xyz'), (3, NULL, NULL)`)
	execSQL(t, ctx, s, `CREATE TABLE r (id INT8 PRIMARY KEY, x INT8)`)
	execSQL(t, ctx, s, `INSERT INTO r VALUES (1, 15), (2, 25)`)
	execSQL(t, ctx, s, `CREATE TABLE r2 (id INT8 PRIMARY KEY, x INT8)`)
	execSQL(t, ctx, s, `INSERT INTO r2 VALUES (1, 15), (2, NULL)`)
	runGroupedCases(t, ctx, s, []groupedCase{
		// Subqueries.
		{sql: `SELECT id FROM q WHERE a > ALL (SELECT x FROM r) ORDER BY id`, want: ""},
		{sql: `SELECT id FROM q WHERE a < ALL (SELECT x FROM r) ORDER BY id`, want: "1"},
		{sql: `SELECT id FROM q WHERE a < ANY (SELECT x FROM r) ORDER BY id`, want: "1; 2"},
		{sql: `SELECT id FROM q WHERE a >= SOME (SELECT x FROM r) ORDER BY id`, want: "2"},
		{sql: `SELECT id FROM q WHERE a <= ANY (SELECT x - 5 FROM r) ORDER BY id`, want: "1; 2"},
		{sql: `SELECT id FROM q WHERE a <> ANY (SELECT x FROM r) ORDER BY id`, want: "1; 2"},
		{sql: `SELECT id FROM q WHERE a <> ALL (SELECT x FROM r) ORDER BY id`, want: "1; 2"},
		{sql: `SELECT id FROM q WHERE a = ALL (SELECT 10) ORDER BY id`, want: "1"},
		{sql: `SELECT id FROM q WHERE a = ANY (SELECT x - 5 FROM r) ORDER BY id`, want: "1; 2"},
		// An empty subquery: ALL holds and ANY does not, whatever the
		// left side is (NULL included).
		{sql: `SELECT id FROM q WHERE a <= ALL (SELECT x FROM r WHERE false) ORDER BY id`, want: "1; 2; 3"},
		{sql: `SELECT id FROM q WHERE a > ANY (SELECT x FROM r WHERE false) ORDER BY id`, want: ""},
		// A NULL among the subquery's rows: UNKNOWN unless decided.
		{sql: `SELECT id FROM q WHERE a > ALL (SELECT x FROM r2) ORDER BY id`, want: ""},
		{sql: `SELECT id FROM q WHERE a < ANY (SELECT x FROM r2) ORDER BY id`, want: "1"},
		{sql: `SELECT id FROM q WHERE NOT (a > ALL (SELECT x FROM r2)) ORDER BY id`, want: "1"},
		{sql: `SELECT id FROM q WHERE NOT (a < ANY (SELECT x FROM r2)) ORDER BY id`, want: ""},
		{sql: `SELECT 20 > ALL (SELECT x FROM r2), 10 > ALL (SELECT x FROM r2), 10 < ANY (SELECT x FROM r2), 20 < ANY (SELECT x FROM r2)`,
			want: "NULL|f|t|NULL"},
		{sql: `SELECT NULL::int8 > ALL (SELECT x FROM r WHERE false), NULL::int8 > ANY (SELECT x FROM r WHERE false), NULL::int8 > ALL (SELECT x FROM r)`,
			want: "t|f|NULL"},
		{sql: `SELECT id, a < ANY (SELECT x FROM r2) FROM q ORDER BY id`, want: "1|t; 2|NULL; 3|NULL"},
		// Correlated, in a join, and over a set operation.
		{sql: `SELECT id FROM q WHERE a >= ALL (SELECT q2.a FROM q q2 WHERE q2.id <= q.id AND q2.a IS NOT NULL) ORDER BY id`, want: "1; 2"},
		{sql: `SELECT q.id, r.x FROM q JOIN r ON r.id = q.id WHERE q.a < ALL (SELECT x FROM r) ORDER BY 1`, want: "1|15"},
		{sql: `SELECT 'abc' LIKE ANY (SELECT 'a%' UNION ALL SELECT 'z%'), 'abc' NOT LIKE ALL (SELECT 'a%' UNION ALL SELECT 'z%')`, want: "t|f"},
		{sql: `SELECT id FROM q WHERE b > ANY (SELECT 'm') ORDER BY id`, want: "2"},
		// Arrays.
		{sql: `SELECT 5 > ALL (ARRAY[1, NULL]), 0 > ALL (ARRAY[1, NULL]), 5 > ANY (ARRAY[1, NULL]), 0 > ANY (ARRAY[1, NULL])`, want: "NULL|f|t|NULL"},
		{sql: `SELECT 5 <> ALL (ARRAY[]::int8[]), 5 = ANY (ARRAY[]::int8[]), 5 = ANY (NULL::int8[])`, want: "t|f|NULL"},
		{sql: `SELECT id FROM q WHERE a > ALL (ARRAY[]::int8[]) ORDER BY id`, want: "1; 2; 3"},
		{sql: `SELECT id FROM q WHERE NOT (a = ANY (ARRAY[10, 30])) ORDER BY id`, want: "2"},
		{sql: `SELECT id FROM q WHERE NOT (a <> ALL (ARRAY[10, NULL])) ORDER BY id`, want: "1"},
		{sql: `SELECT id FROM q WHERE b NOT LIKE ALL (ARRAY['a%', 'q%']) ORDER BY id`, want: "2"},
		// The subquery must have one column.
		{sql: `SELECT 1 > ALL (SELECT 1, 2)`, code: "42601"},
	})
}

// TestValuesFromItem: VALUES as a FROM item with column aliases, joined
// and LATERAL, its column types resolved across the rows; and TABLE t
// (PG-Q-015).
func TestValuesFromItem(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE q (id INT8 PRIMARY KEY, a INT8)`)
	execSQL(t, ctx, s, `INSERT INTO q VALUES (1, 10), (2, 20), (3, NULL)`)
	execSQL(t, ctx, s, `CREATE TABLE r (x INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s, `INSERT INTO r VALUES (15), (25)`)
	execSQL(t, ctx, s, `CREATE TABLE r3 (x INT8 PRIMARY KEY)`)
	runGroupedCases(t, ctx, s, []groupedCase{
		{sql: `SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(x, y) ORDER BY x DESC`, want: "2|b; 1|a"},
		{sql: `SELECT v.x, q.a FROM (VALUES (1), (3)) v(x) JOIN q ON q.id = v.x ORDER BY 1`, want: "1|10; 3|NULL"},
		{sql: `SELECT q.id, v.y FROM q LEFT JOIN (VALUES (1, 'one'), (2, 'two')) v(x, y) ON v.x = q.id ORDER BY 1`, want: "1|one; 2|two; 3|NULL"},
		{sql: `SELECT q.id, v.k, v.val FROM q CROSS JOIN LATERAL (VALUES ('a', q.a), ('id', q.id)) AS v(k, val) ORDER BY 1, 2`,
			want: "1|a|10; 1|id|1; 2|a|20; 2|id|2; 3|a|NULL; 3|id|3"},
		{sql: `SELECT q.id, v.d FROM q, LATERAL (VALUES (q.a * 2)) v(d) ORDER BY 1`, want: "1|20; 2|40; 3|NULL"},
		{sql: `SELECT x, count(*) FROM (VALUES (1), (1), (2)) v(x) GROUP BY x ORDER BY x`, want: "1|2; 2|1"},
		// Column types across the rows: an untyped literal takes the
		// other rows' type, numbers lift, mismatches are refused.
		{sql: `SELECT x + 1 FROM (VALUES ('1'), (2)) v(x) ORDER BY 1`, want: "2; 3"},
		{sql: `SELECT pg_typeof(x), pg_typeof(y) FROM (VALUES (1, NULL), (2.5, 'q')) v(x, y) LIMIT 1`, want: "numeric|text"},
		{sql: `SELECT pg_typeof(x) FROM (VALUES (NULL), (NULL)) v(x) LIMIT 1`, want: "text"},
		{sql: `SELECT * FROM (VALUES (1, 'a'), ('x', 2)) v(x, y)`, code: "22P02"},
		{sql: `VALUES (1), (true)`, code: "42804"},
		{sql: `SELECT * FROM (VALUES (1, 'a'), (2)) v(x, y)`, code: "42601"},
		{sql: `SELECT * FROM (VALUES (1, 'a')) v(x, y, z)`, code: "42P10"},
		// TABLE t.
		{sql: `TABLE q`, want: "1|10; 2|20; 3|NULL"},
		{sql: `TABLE q ORDER BY id DESC LIMIT 2`, want: "3|NULL; 2|20"},
		{sql: `TABLE r UNION ALL TABLE r ORDER BY 1`, want: "15; 15; 25; 25"},
		{sql: `SELECT count(*) FROM (TABLE q) s`, want: "3"},
		{sql: `WITH w AS (TABLE q) SELECT sum(a) FROM w`, want: "30"},
		{sql: `SELECT x FROM r WHERE x IN (TABLE r) ORDER BY 1`, want: "15; 25"},
		{sql: `INSERT INTO r3 TABLE r`, want: ""},
		{sql: `TABLE r3`, want: "15; 25"},
		{sql: `TABLE nope`, code: "42P01"},
	})
	res := execSQL(t, ctx, s, `SELECT * FROM (VALUES (1, 'a')) v(x)`)
	if len(res.Columns) != 2 || res.Columns[0].Name != "x" || res.Columns[1].Name != "column2" {
		t.Fatalf("columns %+v, want x, column2", res.Columns)
	}
	// Described as executed, over the extended protocol.
	conn := pgConn(t, ctx, tc, 0)
	var x int64
	var y string
	if err := conn.QueryRow(ctx, `SELECT * FROM (VALUES ('1', NULL), (2, 'b')) v(x, y) ORDER BY x DESC LIMIT 1`).Scan(&x, &y); err != nil {
		t.Fatal(err)
	}
	if x != 2 || y != "b" {
		t.Fatalf("got %d, %q", x, y)
	}
	var n int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM (TABLE q) s`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("TABLE over the extended protocol: %d, %v", n, err)
	}
}

// TestMultiColumnUpdate: UPDATE ... SET (a, b) = (...) | ROW(...) |
// (SELECT ...), in UPDATE, UPDATE ... FROM and ON CONFLICT DO UPDATE,
// with PostgreSQL's errors (PG-Q-017).
func TestMultiColumnUpdate(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE m (id INT8 PRIMARY KEY, a INT8, b TEXT, c INT8 DEFAULT 7)`)
	execSQL(t, ctx, s, `INSERT INTO m VALUES (1, 0, 'z', 0), (2, 0, 'z', 0), (3, 0, 'z', 0), (4, 0, 'z', 0)`)
	execSQL(t, ctx, s, `CREATE TABLE src (id INT8 PRIMARY KEY, x INT8, y TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO src VALUES (1, 100, 'p'), (2, 200, 'q')`)
	runGroupedCases(t, ctx, s, []groupedCase{
		{sql: `UPDATE m SET (a, b) = (1, 'one') WHERE id = 1 RETURNING a, b`, want: "1|one"},
		{sql: `UPDATE m SET (a, b) = ROW(2, 'two') WHERE id = 2 RETURNING a, b`, want: "2|two"},
		{sql: `UPDATE m SET (c) = ROW(DEFAULT), (a, b) = (a + 5, DEFAULT) WHERE id = 1 RETURNING a, b, c`, want: "6|NULL|7"},
		{sql: `UPDATE m SET c = 9, (a, b) = (SELECT x, y FROM src WHERE id = 1) WHERE id = 3 RETURNING a, b, c`, want: "100|p|9"},
		{sql: `UPDATE m SET (a, b) = (SELECT x, y FROM src WHERE false) WHERE id = 4 RETURNING a, b`, want: "NULL|NULL"},
		{sql: `UPDATE m SET (b, a) = (SELECT y, x FROM src WHERE src.id = m.id) WHERE id IN (1, 2, 3) RETURNING id, a, b`,
			want: "1|100|p; 2|200|q; 3|NULL|NULL"},
		{sql: `UPDATE m SET (a, b) = (src.x + 1, src.y || '!') FROM src WHERE src.id = m.id RETURNING m.id, a, b`,
			want: "1|101|p!; 2|201|q!"},
		{sql: `INSERT INTO m (id, a, b) VALUES (1, 50, 'w') ON CONFLICT (id) DO UPDATE SET (a, b) = (excluded.a, 'conf') RETURNING a, b`,
			want: "50|conf"},
		{sql: `UPDATE m SET (a, b) = (SELECT x FROM src WHERE id = 1)`, code: "42601"},
		{sql: `UPDATE m SET (a, b) = (SELECT x, y FROM src)`, code: "21000"},
		{sql: `UPDATE m SET (a, b) = (1, 'q', 3)`, code: "42601"},
		{sql: `UPDATE m SET (a, b) = (1)`, code: "42601"},
		{sql: `UPDATE m SET (a) = (1)`, code: "42601"},
		{sql: `UPDATE m SET (a, nope) = (1, 2)`, code: "42703"},
	})
}

// TestCollateExpressions: COLLATE after an expression and in ORDER BY
// (PG-Q-023). "C", "POSIX", "default" and ucs_basic all order bytewise,
// which is how every string compares; an unknown collation is 42704.
func TestCollateExpressions(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE w (id INT8 PRIMARY KEY, b TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO w VALUES (1, 'b'), (2, 'B'), (3, 'a'), (4, 'é'), (5, NULL)`)
	runGroupedCases(t, ctx, s, []groupedCase{
		{sql: `SELECT b FROM w ORDER BY b COLLATE "C"`, want: "B; a; b; é; NULL"},
		{sql: `SELECT b FROM w ORDER BY b COLLATE "POSIX" DESC NULLS LAST`, want: "é; b; a; B; NULL"},
		{sql: `SELECT b COLLATE "default" AS b FROM w WHERE b COLLATE ucs_basic < 'b' ORDER BY 1`, want: "B; a"},
		{sql: `SELECT id FROM w WHERE b = 'a' COLLATE pg_catalog."C"`, want: "3"},
		{sql: `SELECT max(b COLLATE "C"), min(b) FROM w`, want: "é|B"},
		{sql: `SELECT (b || 'z') COLLATE "C" AS z FROM w WHERE id < 3 ORDER BY z`, want: "Bz; bz"},
		{sql: `SELECT b FROM w GROUP BY b COLLATE "C" ORDER BY 1 LIMIT 2`, want: "B; a"},
		{sql: `SELECT 'a' COLLATE "C" < 'b' COLLATE "C"`, want: "t"},
		{sql: `SELECT b FROM w ORDER BY b COLLATE "en_US"`, code: "42704"},
		{sql: `SELECT b FROM w ORDER BY b COLLATE C`, code: "42704"},
		{sql: `SELECT b COLLATE public."C" FROM w`, code: "42704"},
		{sql: `SELECT 1 COLLATE "C"`, code: "42804"},
	})
	_, serr := trySQL(ctx, s, `SELECT b FROM w ORDER BY b COLLATE "en_US"`)
	if serr == nil || serr.Msg != `collation "en_US" for encoding "UTF8" does not exist` {
		t.Fatalf("unknown collation: %v", serr)
	}
}
