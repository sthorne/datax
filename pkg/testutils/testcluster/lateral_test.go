package testcluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// lateralFixture makes the users / orders / items tables the LATERAL
// tests read: dan has no orders, item 3 has no tags (an empty array),
// item 4 a NULL array.
func lateralFixture(t *testing.T, exec func(string)) {
	t.Helper()
	exec(`CREATE TABLE users (id INT8 PRIMARY KEY, name TEXT)`)
	exec(`CREATE TABLE orders (id INT8 PRIMARY KEY, user_id INT8, created INT8, amount INT8)`)
	exec(`CREATE INDEX orders_by_user ON orders (user_id)`)
	exec(`CREATE TABLE items (id INT8 PRIMARY KEY, tags TEXT[], qty INT8[])`)
	exec(`INSERT INTO users VALUES (1, 'ann'), (2, 'bob'), (3, 'cat'), (4, 'dan')`)
	exec(`INSERT INTO orders VALUES
		(10, 1, 1, 5), (11, 1, 2, 7), (12, 1, 3, 9), (13, 1, 4, 11),
		(20, 2, 1, 3), (21, 2, 5, 4),
		(30, 3, 9, 100)`)
	exec(`INSERT INTO items VALUES (1, ARRAY['a', 'b'], ARRAY[1, 2]), (2, ARRAY['c'], ARRAY[3]), (3, ARRAY[]::TEXT[], ARRAY[]::INT8[]), (4, NULL, NULL)`)
}

// TestLateral: LATERAL subqueries and table functions in FROM read the
// columns of the members to their left, in every join form (comma,
// CROSS JOIN, JOIN ... ON, LEFT JOIN ... ON true, a miss
// NULL-extending), nested, with WHERE, ORDER BY, LIMIT, aggregates over
// the result, parameters and EXPLAIN; a reference to a member to the
// right is 42P01, as in PostgreSQL.
func TestLateral(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	lateralFixture(t, func(q string) { execSQL(t, ctx, s, q) })

	rows := func(q string, params ...types.Datum) string {
		t.Helper()
		res, serr := trySQL(ctx, s, q, params...)
		if serr != nil {
			t.Fatalf("%s: %v", q, serr)
		}
		var out []string
		for _, r := range res.Rows {
			var cells []string
			for _, d := range r {
				if d.Null {
					cells = append(cells, "NULL")
				} else {
					cells = append(cells, d.Text())
				}
			}
			out = append(out, strings.Join(cells, ","))
		}
		return strings.Join(out, " ")
	}
	eq := func(what, got, want string) {
		t.Helper()
		if got != want {
			t.Fatalf("%s:\n got  %q\n want %q", what, got, want)
		}
	}

	// The issue's own example: a FROM-less lateral subquery.
	eq("comma LATERAL, FROM-less", rows(`SELECT * FROM users u, LATERAL (SELECT u.id + 1) l ORDER BY u.id`),
		"1,ann,2 2,bob,3 3,cat,4 4,dan,5")

	// Top-N per group: each user's two latest orders.
	eq("top-N per group", rows(`SELECT u.name, o.id FROM users u, LATERAL (SELECT * FROM orders o WHERE o.user_id = u.id ORDER BY created DESC LIMIT 2) o ORDER BY u.id, o.created DESC`),
		"ann,13 ann,12 bob,21 bob,20 cat,30")
	eq("CROSS JOIN LATERAL", rows(`SELECT u.name, o.id FROM users u CROSS JOIN LATERAL (SELECT id FROM orders WHERE user_id = u.id ORDER BY created LIMIT 1) AS o ORDER BY 1`),
		"ann,10 bob,20 cat,30")
	eq("JOIN LATERAL ON", rows(`SELECT u.name, c.n FROM users u JOIN LATERAL (SELECT user_id, count(*) AS n FROM orders WHERE user_id = u.id GROUP BY user_id) c ON c.user_id = u.id ORDER BY u.id`),
		"ann,4 bob,2 cat,1")
	eq("JOIN LATERAL ON a filter", rows(`SELECT u.name, o.amount FROM users u JOIN LATERAL (SELECT amount FROM orders WHERE user_id = u.id) o ON o.amount > 8 ORDER BY 2`),
		"ann,9 ann,11 cat,100")

	// LEFT JOIN LATERAL ... ON true keeps the user without orders, its
	// lateral columns NULL; an ON that rejects every candidate is a miss
	// too.
	eq("LEFT JOIN LATERAL ON true", rows(`SELECT u.name, o.id FROM users u LEFT JOIN LATERAL (SELECT id FROM orders WHERE user_id = u.id ORDER BY created DESC LIMIT 1) o ON true ORDER BY u.id`),
		"ann,13 bob,21 cat,30 dan,NULL")
	eq("LEFT JOIN LATERAL, ON rejects", rows(`SELECT u.name, o.id FROM users u LEFT JOIN LATERAL (SELECT id, amount FROM orders WHERE user_id = u.id) o ON o.amount > 50 ORDER BY u.id`),
		"ann,NULL bob,NULL cat,30 dan,NULL")
	// An aggregate without GROUP BY yields a row even for no orders.
	eq("lateral aggregate", rows(`SELECT u.name, s.n, s.total FROM users u, LATERAL (SELECT count(*) AS n, sum(amount) AS total FROM orders WHERE user_id = u.id) s ORDER BY u.id`),
		"ann,4,32 bob,2,7 cat,1,100 dan,0,NULL")

	// WHERE, ORDER BY, LIMIT over the joined rows; aggregates over the
	// result.
	eq("WHERE on lateral columns", rows(`SELECT u.name, o.id FROM users u, LATERAL (SELECT * FROM orders WHERE user_id = u.id) o WHERE o.amount BETWEEN 4 AND 9 AND u.id < 3 ORDER BY o.id LIMIT 3`),
		"ann,10 ann,11 ann,12")
	eq("aggregate over lateral rows", rows(`SELECT u.name, count(*), sum(o.amount) FROM users u, LATERAL (SELECT * FROM orders WHERE user_id = u.id ORDER BY created DESC LIMIT 2) o GROUP BY u.name ORDER BY u.name`),
		"ann,2,20 bob,2,7 cat,1,100")
	eq("count over lateral rows", rows(`SELECT count(*) FROM users u, LATERAL (SELECT 1 FROM orders WHERE user_id = u.id LIMIT 3) o`), "6")

	// Parameters reach into the lateral subquery.
	eq("parameter", rows(`SELECT u.name, o.id FROM users u, LATERAL (SELECT id FROM orders WHERE user_id = u.id AND amount > $1) o ORDER BY o.id`, types.NewInt(8)),
		"ann,12 ann,13 cat,30")

	// A table function in FROM is implicitly lateral.
	eq("unnest of a column", rows(`SELECT i.id, tag FROM items i, unnest(i.tags) AS tag ORDER BY 1, 2`),
		"1,a 1,b 2,c")
	eq("unnest of a column, typed", rows(`SELECT i.id, q.n * 10 FROM items i CROSS JOIN unnest(i.qty) AS q(n) ORDER BY 1, 2`),
		"1,10 1,20 2,30")
	eq("LEFT JOIN LATERAL unnest", rows(`SELECT i.id, x.tag FROM items i LEFT JOIN LATERAL unnest(i.tags) AS x(tag) ON true ORDER BY 1, 2`),
		"1,a 1,b 2,c 3,NULL 4,NULL")
	eq("unnest after a join", rows(`SELECT u.name, tag FROM users u JOIN items i ON i.id = u.id, unnest(i.tags) tag WHERE tag <> 'b' ORDER BY 1, 2`),
		"ann,a bob,c")

	// Nested: the inner lateral reads both its sibling (o) and the outer
	// statement's member (u).
	eq("nested lateral", rows(`SELECT u.name, x.id, x.v FROM users u, LATERAL (SELECT o.id, y.v FROM orders o, LATERAL (SELECT o.amount * 10 + u.id AS v) y WHERE o.user_id = u.id) x WHERE u.id >= 2 ORDER BY x.id`),
		"bob,20,32 bob,21,42 cat,30,1003")
	// Chained laterals: the second reads the first.
	eq("chained laterals", rows(`SELECT u.name, a.m, b.c FROM users u, LATERAL (SELECT max(created) AS m FROM orders WHERE user_id = u.id) a, LATERAL (SELECT count(*) AS c FROM orders WHERE user_id = u.id AND created < a.m) b WHERE u.id < 4 ORDER BY u.id`),
		"ann,4,3 bob,5,1 cat,9,0")
	// A lateral subquery reading nothing to its left runs once, like a
	// plain derived table.
	eq("uncorrelated LATERAL", rows(`SELECT u.name, k.x FROM users u, LATERAL (SELECT 7 AS x) k WHERE u.id = 1`), "ann,7")

	// In a WITH member, and in a view (as a joined subquery is).
	eq("LATERAL in WITH", rows(`WITH w AS (SELECT u.name, o.id FROM users u, LATERAL (SELECT id FROM orders WHERE user_id = u.id ORDER BY id LIMIT 1) o) SELECT count(*), max(id) FROM w`), "3,30")
	execSQL(t, ctx, s, `CREATE VIEW first_orders AS SELECT u.name, o.id FROM users u LEFT JOIN LATERAL (SELECT id FROM orders WHERE user_id = u.id ORDER BY id LIMIT 1) o ON true`)
	eq("LATERAL in a view", rows(`SELECT * FROM first_orders ORDER BY name`), "ann,10 bob,20 cat,30 dan,NULL")
	execSQL(t, ctx, s, `CREATE VIEW joined_sub AS SELECT u.name FROM users u JOIN (SELECT 2 AS k) d ON d.k = u.id`)
	eq("joined subquery in a view", rows(`SELECT * FROM joined_sub`), "bob")

	// The columns are described before execution.
	res := execSQL(t, ctx, s, `SELECT * FROM users u, LATERAL (SELECT o.id AS oid, o.amount FROM orders o WHERE o.user_id = u.id LIMIT 1) o WHERE false`)
	if len(res.Columns) != 4 || res.Columns[2].Name != "oid" || res.Columns[3].Type != types.Int {
		t.Fatalf("lateral columns: %+v", res.Columns)
	}
	cols, serr := s.PlanColumns(ctx, mustParse(t, `SELECT u.name, l.tag FROM items i, unnest(i.tags) AS l(tag), users u WHERE u.id = i.id`))
	if serr != nil || len(cols) != 2 || cols[1].Name != "tag" || cols[1].Type != types.String {
		t.Fatalf("PlanColumns: %+v %v", cols, serr)
	}
	cols, serr = s.PlanColumns(ctx, mustParse(t, `SELECT q.n FROM items i, unnest(i.qty) AS q(n)`))
	if serr != nil || len(cols) != 1 || cols[0].Type != types.Int {
		t.Fatalf("PlanColumns, typed unnest: %+v %v", cols, serr)
	}

	// EXPLAIN names the lateral level.
	plan := rows(`EXPLAIN SELECT u.name, o.id FROM users u, LATERAL (SELECT id FROM orders WHERE user_id = u.id LIMIT 2) o`)
	if !strings.Contains(plan, "lateral (o) per outer row") {
		t.Fatalf("EXPLAIN: %s", plan)
	}

	// A reference to a member further right is an error (PostgreSQL:
	// invalid reference to FROM-clause entry, 42P01).
	for _, q := range []string{
		`SELECT * FROM LATERAL (SELECT u.id) x, users u`,
		`SELECT * FROM users u, LATERAL (SELECT z.id) x, users z`,
		`SELECT * FROM items i, unnest(j.tags) t, items j`,
		`SELECT * FROM unnest(i.tags) t, items i`,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != sql.CodeUndefinedTable {
			t.Fatalf("%s: got %v, want 42P01", q, serr)
		}
	}
	// A RIGHT or FULL join cannot feed a lateral reference (42P10).
	if _, serr := trySQL(ctx, s, `SELECT * FROM users u RIGHT JOIN LATERAL (SELECT id FROM orders WHERE user_id = u.id) o ON true`); serr == nil || serr.Code != sql.CodeInvalidColumnReference {
		t.Fatalf("RIGHT JOIN LATERAL: %v", serr)
	}
}

// TestLateralOverPgwire: lateral queries through a stock pgx client —
// extended protocol (Describe before Execute), prepared statements run
// again with new parameters, and the plan cache reused across runs.
func TestLateralOverPgwire(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	lateralFixture(t, func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	})

	type row struct {
		name string
		id   *int64
	}
	query := func(q string, args ...any) []row {
		t.Helper()
		rs, err := conn.Query(ctx, q, args...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		defer rs.Close()
		var out []row
		for rs.Next() {
			var r row
			if err := rs.Scan(&r.name, &r.id); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, r)
		}
		if rs.Err() != nil {
			t.Fatalf("%s: %v", q, rs.Err())
		}
		return out
	}
	render := func(rs []row) string {
		var parts []string
		for _, r := range rs {
			v := "NULL"
			if r.id != nil {
				v = types.NewInt(*r.id).Text()
			}
			parts = append(parts, r.name+":"+v)
		}
		return strings.Join(parts, " ")
	}

	const topN = `SELECT u.name, o.id FROM users u LEFT JOIN LATERAL (SELECT id FROM orders WHERE user_id = u.id ORDER BY created DESC LIMIT $1) o ON true WHERE u.id <= $2 ORDER BY u.id, o.id`
	if _, err := conn.Prepare(ctx, "topn", topN); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	for i, tc := range []struct {
		limit, maxID int64
		want         string
	}{
		{1, 4, "ann:13 bob:21 cat:30 dan:NULL"},
		{2, 2, "ann:12 ann:13 bob:20 bob:21"},
		{1, 4, "ann:13 bob:21 cat:30 dan:NULL"},
	} {
		if got := render(query("topn", tc.limit, tc.maxID)); got != tc.want {
			t.Fatalf("run %d: got %q, want %q", i, got, tc.want)
		}
	}
	// The same text unprepared (pgx's statement cache: Describe, then
	// Execute), twice.
	for i := 0; i < 2; i++ {
		if got := render(query(`SELECT i.tags[1], q.n FROM items i, unnest(i.qty) AS q(n) WHERE q.n > $1 ORDER BY q.n`, int64(1))); got != "a:2 c:3" {
			t.Fatalf("unnest run %d: %q", i, got)
		}
	}

	var pgErr *pgconn.PgError
	_, err = conn.Exec(ctx, `SELECT * FROM users u, LATERAL (SELECT z.id) x, users z`)
	if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
		t.Fatalf("forward reference: %v", err)
	}
}

func mustParse(t *testing.T, src string) parser.Statement {
	t.Helper()
	stmts, err := parser.Parse(src)
	if err != nil || len(stmts) != 1 {
		t.Fatalf("parse %s: %v", src, err)
	}
	return stmts[0]
}
