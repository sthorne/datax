package testcluster

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestJoinExpressionsAndPaths: expressions and ->/->> path operators in
// join queries — select lists and WHERE conjuncts, on base and non-base
// sides, with LEFT-join NULL extension flowing through paths. Issue #41.
func TestJoinExpressionsAndPaths(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	for _, q := range []string{
		`CREATE TABLE orders (id INT8 PRIMARY KEY, cust INT8, qty INT8, meta JSONB)`,
		`CREATE TABLE custs (id INT8 PRIMARY KEY, name TEXT, prefs JSONB)`,
		`INSERT INTO custs VALUES
			(1, 'ada', '{"tier":"gold","opts":{"ship":"fast"}}'),
			(2, 'bob', '{"tier":"basic"}'),
			(3, 'cyd', NULL)`,
		`INSERT INTO orders VALUES
			(10, 1, 5, '{"gift":true}'),
			(11, 2, 2, '{"gift":false}'),
			(12, 1, 7, NULL)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	// Expressions and paths in the join SELECT list; computed items render
	// as TEXT (single-table precedent).
	rows, err := conn.Query(ctx, `SELECT o.id, o.qty * 2 AS dbl, c.prefs ->> 'tier' AS tier
		FROM orders AS o JOIN custs AS c ON c.id = o.cust ORDER BY o.id`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	type outRow struct {
		id   int64
		dbl  string
		tier *string
	}
	var got []outRow
	for rows.Next() {
		var r outRow
		if err := rows.Scan(&r.id, &r.dbl, &r.tier); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if rows.Err() != nil {
		t.Fatalf("select rows: %v", rows.Err())
	}
	rows.Close()
	if len(got) != 3 || got[0].dbl != "10" || *got[0].tier != "gold" ||
		got[1].dbl != "4" || *got[1].tier != "basic" || got[2].dbl != "14" {
		t.Fatalf("rows: %+v", got)
	}
	// Nested path on a join side.
	var ship string
	if err := conn.QueryRow(ctx, `SELECT c.prefs -> 'opts' ->> 'ship'
		FROM orders AS o JOIN custs AS c ON c.id = o.cust WHERE o.id = 10`).Scan(&ship); err != nil || ship != "fast" {
		t.Fatalf("nested path: %q, %v", ship, err)
	}

	// Path conjuncts in join WHERE — on the non-base side (post-join
	// filtering) and on the base side (never pushed as bounds, still
	// correct).
	var n int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM orders AS o JOIN custs AS c ON c.id = o.cust
		WHERE c.prefs ->> 'tier' = 'gold'`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("non-base path conjunct: %d, %v", n, err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM orders AS o JOIN custs AS c ON c.id = o.cust
		WHERE o.meta ->> 'gift' = 'true'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("base path conjunct: %d, %v", n, err)
	}
	// Computed-LHS conjunct across the join.
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM orders AS o JOIN custs AS c ON c.id = o.cust
		WHERE o.qty * 2 > 8`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("computed lhs: %d, %v", n, err)
	}

	// LEFT JOIN: NULL extension flows through paths; IS NULL keeps the
	// extended row.
	if _, err := conn.Exec(ctx, `INSERT INTO orders VALUES (13, 99, 1, NULL)`); err != nil {
		t.Fatalf("orphan insert: %v", err)
	}
	var tier *string
	if err := conn.QueryRow(ctx, `SELECT c.prefs ->> 'tier' FROM orders AS o LEFT JOIN custs AS c ON c.id = o.cust
		WHERE o.id = 13`).Scan(&tier); err != nil || tier != nil {
		t.Fatalf("left extension path: %v, %v", tier, err)
	}
	// Path IS NULL keeps exactly the NULL-extended row (order 13; every
	// matched cust has a tier, and cyd's NULL prefs have no orders).
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM orders AS o LEFT JOIN custs AS c ON c.id = o.cust
		WHERE c.prefs ->> 'tier' IS NULL`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("path IS NULL over extension: %d, %v", n, err)
	}

	// DISTINCT over computed join output. (ORDER BY on a computed alias
	// stays out of scope — join ORDER BY resolves side columns only —
	// so order-independence is asserted by sorting client-side.)
	rows, err = conn.Query(ctx, `SELECT DISTINCT c.prefs ->> 'tier' AS tier
		FROM orders AS o JOIN custs AS c ON c.id = o.cust`)
	if err != nil {
		t.Fatalf("distinct: %v", err)
	}
	var tiers []string
	for rows.Next() {
		var s *string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		if s != nil {
			tiers = append(tiers, *s)
		}
	}
	if rows.Err() != nil {
		t.Fatalf("distinct rows: %v", rows.Err())
	}
	rows.Close()
	sort.Strings(tiers)
	if strings.Join(tiers, ",") != "basic,gold" {
		t.Fatalf("distinct tiers: %v", tiers)
	}

	// EXPLAIN succeeds for pathed/expressive joins.
	var plan string
	if err := conn.QueryRow(ctx, `EXPLAIN SELECT o.qty + 1, c.prefs ->> 'tier'
		FROM orders AS o JOIN custs AS c ON c.id = o.cust WHERE c.prefs ->> 'tier' = 'gold'`).Scan(&plan); err != nil {
		t.Fatalf("explain: %v", err)
	}
	if plan == "" {
		t.Fatal("empty plan")
	}

	// Grouped joins: paths in WHERE work (pre-grouping); pathed SELECT
	// items under GROUP BY stay 0A000; @> in join WHERE stays 0A000.
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM orders AS o JOIN custs AS c ON c.id = o.cust
		WHERE c.prefs ->> 'tier' = 'gold' GROUP BY o.cust`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("grouped where path: %d, %v", n, err)
	}
	expectCode(t, ctx, conn, `SELECT c.prefs ->> 'tier', count(*) FROM orders AS o JOIN custs AS c ON c.id = o.cust GROUP BY c.prefs`, "0A000")
	expectCode(t, ctx, conn, `SELECT o.id FROM orders AS o JOIN custs AS c ON c.id = o.cust WHERE c.prefs @> '{"tier":"gold"}'`, "0A000")

	// Bad references error at resolve time with real codes.
	expectCode(t, ctx, conn, `SELECT o.qty + z.x FROM orders AS o JOIN custs AS c ON c.id = o.cust`, "42P01")
	expectCode(t, ctx, conn, `SELECT o.nope + 1 FROM orders AS o JOIN custs AS c ON c.id = o.cust`, "42703")
	// Path on a non-jsonb join column.
	expectCode(t, ctx, conn, `SELECT o.qty ->> 'k' FROM orders AS o JOIN custs AS c ON c.id = o.cust`, "0A000")
}
