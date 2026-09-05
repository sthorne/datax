package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/metrics"
)

// planCacheCounts reads the plan cache's hit and miss counters.
func planCacheCounts() (hits, misses float64) {
	return testutil.ToFloat64(metrics.SQLPlanCacheHits), testutil.ToFloat64(metrics.SQLPlanCacheMisses)
}

// TestPlanCache (issue #107): a prepared statement plans once and its
// executions reuse the plan; a schema change, ANALYZE and a dropped
// table each invalidate it; EXPLAIN reports a cached plan; a distinct
// statement is a distinct entry and the cache is bounded.
func TestPlanCache(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// pgx's default mode: every statement text prepared once per
	// connection, then Bind/Execute.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE pc (k INT8 PRIMARY KEY, v TEXT, n INT8)`); err != nil {
		t.Fatal(err)
	}
	vals := make([]string, 0, 50)
	for g := 1; g <= 50; g++ {
		vals = append(vals, fmt.Sprintf("(%d, 'v%d', %d)", g, g, g*2))
	}
	if _, err := conn.Exec(ctx, `INSERT INTO pc VALUES `+strings.Join(vals, ", ")); err != nil {
		t.Fatal(err)
	}

	// Point select: the first execution plans and caches, the next 20 hit.
	h0, m0 := planCacheCounts()
	for i := 1; i <= 21; i++ {
		var v string
		if err := conn.QueryRow(ctx, `SELECT v FROM pc WHERE k = $1`, int64(i)).Scan(&v); err != nil || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("k=%d: %q, %v", i, v, err)
		}
	}
	h1, m1 := planCacheCounts()
	if hits, misses := h1-h0, m1-m0; hits != 20 || misses != 1 {
		t.Fatalf("point select: %0.f hits, %0.f misses (want 20, 1)", hits, misses)
	}

	// Update and delete cache too; the shape re-binds per execution.
	h0, m0 = planCacheCounts()
	for i := 1; i <= 5; i++ {
		if _, err := conn.Exec(ctx, `UPDATE pc SET n = $1 WHERE k = $2`, int64(100+i), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 41; i <= 45; i++ {
		if _, err := conn.Exec(ctx, `DELETE FROM pc WHERE k = $1`, int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	h1, m1 = planCacheCounts()
	if hits, misses := h1-h0, m1-m0; hits != 8 || misses != 2 {
		t.Fatalf("update+delete: %0.f hits, %0.f misses (want 8, 2)", hits, misses)
	}
	var n int64
	if err := conn.QueryRow(ctx, `SELECT n FROM pc WHERE k = $1`, int64(3)).Scan(&n); err != nil || n != 103 {
		t.Fatalf("updated row: %d, %v", n, err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pc`).Scan(&n); err != nil || n != 45 {
		t.Fatalf("rows after deletes: %d, %v", n, err)
	}

	// A schema change: the prepared select misses once (a new descriptor)
	// and sees the new column's default through a fresh statement.
	if _, err := conn.Exec(ctx, `ALTER TABLE pc ADD COLUMN w TEXT DEFAULT 'dw'`); err != nil {
		t.Fatal(err)
	}
	h0, m0 = planCacheCounts()
	for i := 1; i <= 3; i++ {
		var v string
		if err := conn.QueryRow(ctx, `SELECT v FROM pc WHERE k = $1`, int64(i)).Scan(&v); err != nil {
			t.Fatal(err)
		}
	}
	h1, m1 = planCacheCounts()
	if hits, misses := h1-h0, m1-m0; hits != 2 || misses != 1 {
		t.Fatalf("after ADD COLUMN: %0.f hits, %0.f misses (want 2, 1)", hits, misses)
	}
	var w string
	if err := conn.QueryRow(ctx, `SELECT w FROM pc WHERE k = $1`, int64(1)).Scan(&w); err != nil || w != "dw" {
		t.Fatalf("new column: %q, %v", w, err)
	}

	// ANALYZE replaces the statistics the plan was built on: one miss.
	if _, err := conn.Exec(ctx, `ANALYZE pc`); err != nil {
		t.Fatal(err)
	}
	h0, m0 = planCacheCounts()
	for i := 1; i <= 3; i++ {
		var v string
		if err := conn.QueryRow(ctx, `SELECT v FROM pc WHERE k = $1`, int64(i)).Scan(&v); err != nil {
			t.Fatal(err)
		}
	}
	h1, m1 = planCacheCounts()
	if hits, misses := h1-h0, m1-m0; hits != 2 || misses != 1 {
		t.Fatalf("after ANALYZE: %0.f hits, %0.f misses (want 2, 1)", hits, misses)
	}

	// EXPLAIN of a statement whose plan is cached says so; the first
	// EXPLAIN of a fresh statement does not, the second does.
	var plan string
	if err := conn.QueryRow(ctx, `EXPLAIN SELECT v FROM pc WHERE k = 7`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plan, "(cached plan)") {
		t.Fatalf("first EXPLAIN reports a cached plan: %s", plan)
	}
	if err := conn.QueryRow(ctx, `EXPLAIN SELECT v FROM pc WHERE k = 7`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "point lookup") || !strings.Contains(plan, "(cached plan)") {
		t.Fatalf("second EXPLAIN: %s", plan)
	}

	// A full-scan shape caches its projection and re-plans the path;
	// the result is identical.
	for i := 0; i < 3; i++ {
		rows, err := conn.Query(ctx, `SELECT k FROM pc WHERE n > $1 ORDER BY k`, int64(90))
		if err != nil {
			t.Fatal(err)
		}
		var ks []int64
		for rows.Next() {
			var k int64
			if err := rows.Scan(&k); err != nil {
				t.Fatal(err)
			}
			ks = append(ks, k)
		}
		rows.Close()
		// k 1–5 (n set to 101–105 above) and k 46–50 (n 92–100).
		if len(ks) != 10 || ks[0] != 1 || ks[4] != 5 || ks[5] != 46 || ks[9] != 50 {
			t.Fatalf("scan shape, execution %d: %v", i, ks)
		}
	}

	// Distinct statement texts are distinct entries; the cache holds at
	// most 128 and evicts the oldest.
	ev0 := testutil.ToFloat64(metrics.SQLPlanCacheEvictions)
	for i := 0; i < 140; i++ {
		var v string
		if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT v FROM pc WHERE k = $1 AND n <> %d`, -1-i), int64(1)).Scan(&v); err != nil {
			t.Fatal(err)
		}
	}
	if ev := testutil.ToFloat64(metrics.SQLPlanCacheEvictions) - ev0; ev < 12 {
		t.Fatalf("140 statements evicted %0.f plans (want at least 12)", ev)
	}

	// A dropped table: the prepared statement fails as it should.
	if _, err := conn.Exec(ctx, `DROP TABLE pc`); err != nil {
		t.Fatal(err)
	}
	var v string
	err = conn.QueryRow(ctx, `SELECT v FROM pc WHERE k = $1`, int64(1)).Scan(&v)
	if code := pgErrCode(err); code != "42P01" {
		t.Fatalf("select on a dropped table: %v", err)
	}
}

// TestParseCache (issue #107): a connection repeating a simple-protocol
// query text does not parse it again, and the repeated statement is the
// same one to the plan cache.
func TestParseCache(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE pq (k INT8 PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO pq VALUES (1, 'a'), (2, 'b')`); err != nil {
		t.Fatal(err)
	}
	p0 := testutil.ToFloat64(metrics.SQLParseCacheHits)
	h0, m0 := planCacheCounts()
	for i := 0; i < 10; i++ {
		var v string
		if err := conn.QueryRow(ctx, `SELECT v FROM pq WHERE k = 2`).Scan(&v); err != nil || v != "b" {
			t.Fatalf("%q, %v", v, err)
		}
	}
	if p := testutil.ToFloat64(metrics.SQLParseCacheHits) - p0; p != 9 {
		t.Fatalf("parse cache hits: %0.f (want 9)", p)
	}
	h1, m1 := planCacheCounts()
	if hits, misses := h1-h0, m1-m0; hits != 9 || misses != 1 {
		t.Fatalf("plan cache through the parse cache: %0.f hits, %0.f misses (want 9, 1)", hits, misses)
	}
}
