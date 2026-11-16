package testcluster

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// TestOrderByPosition (issue #322): ORDER BY n and GROUP BY n name
// select item n, never whatever its output name resolves to. ORDER BY 1
// over an unnamed expression failed with 42703 (column "?column?" does
// not exist), and over a column that another item's alias named sorted
// by the aliased column instead.
func TestOrderByPosition(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c := rlsConn(t, ctx, tc, 0, "root")
	rlsExec(t, ctx, c, `CREATE TABLE o (id INT8 PRIMARY KEY, a TEXT, b INT8)`)
	rlsExec(t, ctx, c, `INSERT INTO o VALUES (1, 'x', 3), (2, 'y', 1), (3, 'x', 2), (4, 'z', 5)`)
	for _, tc := range []struct {
		q    string
		want []string
	}{
		// The report's query.
		{`SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY 1`, []string{"public.o"}},
		{`SELECT a || 'q' FROM o ORDER BY 1`, []string{"xq", "xq", "yq", "zq"}},
		{`SELECT a || 'q' FROM o ORDER BY 1 DESC`, []string{"zq", "yq", "xq", "xq"}},
		{`SELECT id, b * -1 FROM o ORDER BY 2`, []string{"4|-5", "1|-3", "3|-2", "2|-1"}},
		// Two unnamed expressions share the name "?column?".
		{`SELECT b * -1, b * 2 FROM o ORDER BY 2 DESC`, []string{"-5|10", "-3|6", "-2|4", "-1|2"}},
		{`SELECT b * -1 AS n, id FROM o ORDER BY 1`, []string{"-5|4", "-3|1", "-2|3", "-1|2"}},
		// An alias naming another column: position 1 is id, not b.
		{`SELECT id AS b, b AS id FROM o ORDER BY 1`, []string{"1|3", "2|1", "3|2", "4|5"}},
		{`SELECT b, id AS b FROM o ORDER BY 1`, []string{"1|2", "2|3", "3|1", "5|4"}},
		{`SELECT DISTINCT a || 'q' FROM o ORDER BY 1`, []string{"xq", "yq", "zq"}},
		{`SELECT 1 + 1, 'b' ORDER BY 1`, []string{"2|b"}},
		{`SELECT p.b * 10, o.id FROM o JOIN o AS p ON o.id = p.id ORDER BY 1 DESC`, []string{"50|4", "30|1", "20|3", "10|2"}},
		{`SELECT upper(a) FROM (SELECT a FROM o) s ORDER BY 1`, []string{"X", "X", "Y", "Z"}},
		{`SELECT * FROM o ORDER BY 3`, []string{"2|y|1", "3|x|2", "1|x|3", "4|z|5"}},
		{`SELECT b, * FROM o ORDER BY 1 DESC`, []string{"5|4|z|5", "3|1|x|3", "2|3|x|2", "1|2|y|1"}},
		// Aggregates: by the aggregate at that position.
		{`SELECT a, count(*), max(b) FROM o GROUP BY a ORDER BY 3 DESC`, []string{"z|1|5", "x|2|3", "y|1|1"}},
		{`SELECT a, count(*) FROM o GROUP BY a ORDER BY 2 DESC, 1`, []string{"x|2", "y|1", "z|1"}},
		// A set operation's ORDER BY n is the result's column n,
		// whatever the last member calls it.
		{`SELECT a AS x FROM o UNION ALL SELECT 'w' AS y ORDER BY 1`, []string{"w", "x", "x", "y", "z"}},
		{`SELECT a || 'q' FROM o UNION ALL SELECT 'a' ORDER BY 1`, []string{"a", "xq", "xq", "yq", "zq"}},
		{`VALUES (1, 'z'), (2, 'y'), (3, 'x') ORDER BY 2`, []string{"3|x", "2|y", "1|z"}},
		// GROUP BY n: the column item n reads.
		{`SELECT a, count(*) FROM o GROUP BY 1 ORDER BY 1`, []string{"x|2", "y|1", "z|1"}},
		{`SELECT a AS k, count(*) FROM o GROUP BY 1 ORDER BY 1`, []string{"x|2", "y|1", "z|1"}},
		{`SELECT id AS a, count(*) FROM o GROUP BY 1 ORDER BY 1`, []string{"1|1", "2|1", "3|1", "4|1"}},
	} {
		got, err := rlsTryRows(ctx, c, tc.q)
		if err != nil {
			t.Errorf("%s: %v", tc.q, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s:\n got %v\nwant %v", tc.q, got, tc.want)
		}
	}
	// What a position cannot name is refused, not resolved by a name.
	for q, code := range map[string]string{
		`SELECT a, b FROM o ORDER BY 3`:               "42601",
		`SELECT count(*) FROM o GROUP BY 1`:           "42601",
		`SELECT lower(a), count(*) FROM o GROUP BY 1`: "42601",
		`SELECT *, b * 2 FROM o ORDER BY 4`:           "42601",
		`SELECT a FROM o ORDER BY 0`:                  "42601",
	} {
		if _, err := rlsTryRows(ctx, c, q); rlsCode(err) != code {
			t.Errorf("%s: %v, want %s", q, err, code)
		}
	}
}
