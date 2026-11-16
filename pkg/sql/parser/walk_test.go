package parser

import "testing"

// TestCountParamsEveryPosition: a parameter counts wherever it stands —
// in a subquery (IN, EXISTS, scalar, derived, joined, lateral-style),
// a WITH member, a set-operation member, HAVING, ORDER BY, LIMIT /
// OFFSET, JOIN ON, a window, an aggregate's FILTER, RETURNING, ON
// CONFLICT, INSERT ... SELECT, and under EXPLAIN.
func TestCountParamsEveryPosition(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want int
	}{
		{`SELECT count(*) FROM sr WHERE sid IN (SELECT sid FROM s WHERE g = $1)`, 1},
		{`DELETE FROM sr WHERE sid IN (SELECT sid FROM s WHERE g = $1)`, 1},
		{`UPDATE sr SET n = 1 WHERE sid IN (SELECT sid FROM s WHERE g = $2) AND id = $1`, 2},
		{`SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.x = $3)`, 3},
		{`SELECT id, (SELECT max(x) FROM u WHERE y = $1) FROM t`, 1},
		{`SELECT * FROM (SELECT id FROM t WHERE a = $2) AS d`, 2},
		{`SELECT t.id FROM t JOIN (SELECT id FROM u WHERE b = $1) AS d ON d.id = t.id`, 1},
		{`SELECT t.id FROM t JOIN u ON u.id = t.id AND u.k = $2`, 2},
		{`WITH w AS (SELECT id FROM t WHERE a = $1) SELECT id FROM w`, 1},
		{`SELECT id FROM t WHERE a = 1 UNION SELECT id FROM u WHERE b = $2`, 2},
		{`SELECT g, count(*) FROM t GROUP BY g HAVING count(*) > $1`, 1},
		{`SELECT id FROM t ORDER BY abs(a - $1)`, 1},
		{`SELECT id FROM t LIMIT $1 OFFSET $2`, 2},
		{`SELECT count(*) FILTER (WHERE a > $1) FROM t`, 1},
		{`SELECT sum(a) OVER (PARTITION BY a + $1) FROM t`, 1},
		{`SELECT CASE WHEN a = $2 THEN 1 ELSE 0 END FROM t`, 2},
		{`UPDATE r SET n = n + 1 WHERE id = 1 RETURNING n + $1`, 1},
		{`INSERT INTO t (a, b) SELECT $1, $2`, 2},
		{`INSERT INTO t (a) SELECT x FROM u WHERE y IN (SELECT z FROM v WHERE w = $3)`, 3},
		{`INSERT INTO t VALUES ($1) ON CONFLICT (id) DO UPDATE SET v = $2 WHERE t.n > $3 RETURNING v || $4`, 4},
		{`EXPLAIN SELECT $1::text`, 1},
		{`EXPLAIN ANALYZE SELECT count(*) FROM t WHERE a = $1`, 1},
		{`CREATE TABLE c AS SELECT id FROM t WHERE a = $1`, 1},
	} {
		stmts, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if got := CountParams(stmts[0]); got != tc.want {
			t.Errorf("%s: %d parameters, want %d", tc.sql, got, tc.want)
		}
	}
}
