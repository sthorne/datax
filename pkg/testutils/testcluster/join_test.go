package testcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestJoinsOverPgwire: two-table inner and LEFT OUTER joins through a stock
// pgx client, matching PostgreSQL semantics for the same data (NULL join
// keys match nothing; LEFT JOIN NULL-extends; WHERE filters after
// extension, enabling the IS NULL anti-join).
func TestJoinsOverPgwire(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	mustExec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	mustExec(`CREATE TABLE customers (id INT8 PRIMARY KEY, name TEXT, city TEXT)`)
	mustExec(`CREATE TABLE orders (id INT8 PRIMARY KEY, customer_id INT8, total INT8)`)
	mustExec(`CREATE INDEX by_customer ON orders (customer_id)`)
	mustExec(`INSERT INTO customers VALUES (1, 'ann', 'oslo'), (2, 'bob', 'bergen'), (3, 'cat', 'oslo')`)
	mustExec(`INSERT INTO orders VALUES (100, 1, 50), (101, 1, 70), (102, 2, 30), (103, NULL, 99)`)

	type pair struct {
		name  string
		total *int64
	}
	collect := func(q string, args ...any) []pair {
		t.Helper()
		rows, err := conn.Query(ctx, q, args...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		defer rows.Close()
		var out []pair
		for rows.Next() {
			var p pair
			if err := rows.Scan(&p.name, &p.total); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, p)
		}
		if rows.Err() != nil {
			t.Fatalf("%s: %v", q, rows.Err())
		}
		return out
	}
	iv := func(v int64) *int64 { return &v }
	expect := func(got []pair, want []pair) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
		for i := range want {
			g, w := got[i], want[i]
			if g.name != w.name || (g.total == nil) != (w.total == nil) || (w.total != nil && *g.total != *w.total) {
				t.Fatalf("row %d: got %+v, want %+v", i, got, want)
			}
		}
	}

	// Inner join: the NULL customer_id order matches nothing.
	expect(collect(`SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id
		ORDER BY c.name, o.total`),
		[]pair{{"ann", iv(50)}, {"ann", iv(70)}, {"bob", iv(30)}})

	// LEFT JOIN: cat has no orders → NULL-extended row.
	expect(collect(`SELECT c.name, o.total FROM customers c LEFT JOIN orders o ON c.id = o.customer_id
		ORDER BY c.name, o.total`),
		[]pair{{"ann", iv(50)}, {"ann", iv(70)}, {"bob", iv(30)}, {"cat", nil}})

	// Anti-join: WHERE on the inner side filters AFTER NULL extension.
	expect(collect(`SELECT c.name, o.total FROM customers c LEFT JOIN orders o ON c.id = o.customer_id
		WHERE o.id IS NULL`),
		[]pair{{"cat", nil}})

	// WHERE mixing both sides on an inner join.
	expect(collect(`SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id
		WHERE c.city = 'oslo' AND o.total > 60`),
		[]pair{{"ann", iv(70)}})

	// Orders as the outer side: inner lookup is a PK point per row; the
	// NULL join key row disappears from the inner join…
	rows, err := conn.Query(ctx, `SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id ORDER BY o.id`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		var nm string
		if err := rows.Scan(&id, &nm); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 3 || ids[0] != 100 || ids[2] != 102 {
		t.Fatalf("order-outer inner join ids: %v", ids)
	}
	// …and NULL-extends on a LEFT JOIN.
	rows, err = conn.Query(ctx, `SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON o.customer_id = c.id ORDER BY o.id`)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	var lastName *string
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id, &lastName); err != nil {
			t.Fatal(err)
		}
		n++
	}
	rows.Close()
	if n != 4 || lastName != nil {
		t.Fatalf("order-outer left join: %d rows, last name %v", n, lastName)
	}

	// SELECT * = outer columns then inner columns (PostgreSQL order).
	r := conn.QueryRow(ctx, `SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.id = 100`)
	var cid, oid, ocust, ototal int64
	var cname, ccity string
	if err := r.Scan(&cid, &cname, &ccity, &oid, &ocust, &ototal); err != nil {
		t.Fatalf("star scan: %v", err)
	}
	if cid != 1 || cname != "ann" || oid != 100 || ototal != 50 {
		t.Fatalf("star row: %d %s %s %d %d %d", cid, cname, ccity, oid, ocust, ototal)
	}

	// Parameters + DISTINCT over a join (extended protocol).
	var name string
	if err := conn.QueryRow(ctx, `SELECT DISTINCT c.name FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.total > $1`, 40).Scan(&name); err != nil || name != "ann" {
		t.Fatalf("param join: %v (%s)", err, name)
	}

	// Self-join through aliases.
	var cnt int
	rows, err = conn.Query(ctx, `SELECT a.id, b.id FROM customers a JOIN customers b ON a.id = b.id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		cnt++
	}
	rows.Close()
	if cnt != 3 {
		t.Fatalf("self-join rows: %d", cnt)
	}

	// Errors: ambiguous unqualified column; unknown qualifier.
	var pgErr *pgconn.PgError
	if _, err := conn.Exec(ctx, `SELECT id FROM customers c JOIN orders o ON c.id = o.customer_id`); err == nil {
		t.Fatal("ambiguous column accepted")
	} else if !errors.As(err, &pgErr) || pgErr.Code != "42702" {
		t.Fatalf("ambiguous column error: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT x.name FROM customers c JOIN orders o ON c.id = o.customer_id`); err == nil {
		t.Fatal("unknown qualifier accepted")
	} else if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
		t.Fatalf("unknown qualifier error: %v", err)
	}
}

// TestJoinSession: EXPLAIN shows the inner access path, and the session
// rejects unsupported join combinations.
func TestJoinSession(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total INT)`)
	execSQL(t, ctx, s, `CREATE INDEX by_customer ON orders (customer_id)`)
	execSQL(t, ctx, s, `INSERT INTO customers VALUES (1, 'ann'), (2, 'bob')`)
	execSQL(t, ctx, s, `INSERT INTO orders VALUES (100, 1, 50), (101, 2, 30), (102, 1, 70)`)

	// Join key hits the inner PK → point lookup per outer row.
	want := "nested loop inner join; outer (o): full table scan; inner (customers) per outer row: point lookup on primary key"
	if p := explainPlan(t, ctx, s, `SELECT o.id, customers.name FROM orders o JOIN customers ON o.customer_id = customers.id`); p != want {
		t.Fatalf("plan: %s", p)
	}
	// Join key hits an inner index; outer WHERE becomes a bounded PK scan.
	want = `nested loop left join; outer (c): range scan of primary key (id > 1); inner (o) per outer row: scan of index "by_customer" (1 column prefix) + primary key join`
	if p := explainPlan(t, ctx, s, `SELECT c.name, o.total FROM customers c LEFT JOIN orders o ON c.id = o.customer_id WHERE c.id > 1`); p != want {
		t.Fatalf("plan: %s", p)
	}

	// ORDER BY a qualified column with LIMIT.
	res := execSQL(t, ctx, s, `SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id ORDER BY o.total DESC LIMIT 2`)
	if len(res.Rows) != 2 || res.Rows[0][1].I != 70 || res.Rows[1][1].I != 50 {
		t.Fatalf("rows: %+v", res.Rows)
	}

	// Unsupported combinations.
	if _, serr := trySQL(ctx, s, `SELECT c.name FROM customers c JOIN orders o ON c.id = o.customer_id FOR UPDATE`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("join FOR UPDATE: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT COUNT(*) FROM customers c JOIN orders o ON c.id = o.customer_id`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("join aggregate: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT c.name FROM customers c JOIN orders o ON c.id = o.id AND o.total = o.total`); serr == nil {
		t.Fatal("same-side ON condition accepted")
	}
}
