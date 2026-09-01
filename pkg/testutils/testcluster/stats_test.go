package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/keys"
)

// TestAnalyzeAndShowStats: ANALYZE sweeps a table at a frozen timestamp
// and stores row/distinct statistics; SHOW STATS renders them; the gates
// (admin-only, no transaction blocks) hold; DROP TABLE removes the blob.
// Issue #56 (SA1).
func TestAnalyzeAndShowStats(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE cities (
		id INT8 PRIMARY KEY, name TEXT, region TEXT, pop INT8
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	// 500 rows, 5 regions, some NULL pops.
	batch := &pgx.Batch{}
	for i := 0; i < 500; i++ {
		if i%50 == 0 {
			batch.Queue(`INSERT INTO cities VALUES ($1, $2, $3, NULL)`,
				int64(i), "city", "region-0")
			continue
		}
		batch.Queue(`INSERT INTO cities (id, name, region, pop) VALUES ($1, $2, $3, $4)`,
			int64(i), "city", "region-"+string(rune('0'+i%5)), int64(1000+i))
	}
	if err := conn.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// No stats yet.
	rows, err := conn.Query(ctx, `SHOW STATS FOR cities`)
	if err != nil {
		t.Fatalf("show empty: %v", err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if n != 0 {
		t.Fatalf("stats rows before ANALYZE: %d", n)
	}

	if _, err := conn.Exec(ctx, `ANALYZE cities`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	type statRow struct {
		table                      string
		rowCount, distinct, nullCt int64
		col                        string
		collected                  time.Time
	}
	read := func() map[string]statRow {
		t.Helper()
		rows, err := conn.Query(ctx, `SHOW STATS FOR cities`)
		if err != nil {
			t.Fatalf("show: %v", err)
		}
		defer rows.Close()
		out := map[string]statRow{}
		for rows.Next() {
			var r statRow
			if err := rows.Scan(&r.table, &r.rowCount, &r.collected, &r.col, &r.distinct, &r.nullCt); err != nil {
				t.Fatal(err)
			}
			out[r.col] = r
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		return out
	}
	st := read()
	if len(st) != 4 {
		t.Fatalf("stat rows: %+v", st)
	}
	within := func(got, want int64) bool { // KMV beyond capacity is an estimate
		diff := got - want
		if diff < 0 {
			diff = -diff
		}
		return diff*100 <= want*20 // ±20%
	}
	if r := st["id"]; r.rowCount != 500 || !within(r.distinct, 500) || r.nullCt != 0 {
		t.Fatalf("id stats: %+v", r)
	}
	if r := st["region"]; r.distinct != 5 {
		t.Fatalf("region stats: %+v", r)
	}
	if r := st["name"]; r.distinct != 1 {
		t.Fatalf("name stats: %+v", r)
	}
	if r := st["pop"]; r.nullCt != 10 || !within(r.distinct, 490) {
		t.Fatalf("pop stats: %+v", r)
	}
	if time.Since(st["id"].collected) > time.Minute {
		t.Fatalf("collected_at stale: %v", st["id"].collected)
	}

	// Re-ANALYZE reflects new data (bare form covers all tables).
	if _, err := conn.Exec(ctx, `INSERT INTO cities VALUES (1000, 'x', 'region-new', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `ANALYZE`); err != nil {
		t.Fatalf("bare analyze: %v", err)
	}
	if r := read()["region"]; r.rowCount != 501 || r.distinct != 6 {
		t.Fatalf("post-reanalyze region: %+v", r)
	}

	// Gates: inside BEGIN → 25001; non-admin → 42501.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ANALYZE cities`); err == nil {
		t.Fatal("ANALYZE inside BEGIN accepted")
	}
	_ = tx.Rollback(ctx)

	if _, err := conn.Exec(ctx, `CREATE USER alice PASSWORD 'pw'`); err != nil {
		t.Fatalf("create user: %v", err)
	}
	acfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	acfg.User, acfg.Password = "alice", "pw"
	aconn, err := pgx.ConnectConfig(ctx, acfg)
	if err != nil {
		t.Fatalf("alice connect: %v", err)
	}
	defer func() { _ = aconn.Close(ctx) }()
	expectCode(t, ctx, aconn, `ANALYZE cities`, "42501")

	// DROP TABLE removes the stats blob.
	var tableID uint64
	{
		n0 := tc.Nodes[0]
		lo, hi := keys.TableStatsSpan()
		kvs, err := n0.DB().Scan(ctx, lo, hi, 0)
		if err != nil {
			t.Fatalf("stats span scan: %v", err)
		}
		if len(kvs) == 0 {
			t.Fatal("no stats keys stored")
		}
		tableID, _ = keys.TableStatsID(kvs[0].Key)
	}
	if _, err := conn.Exec(ctx, `DROP TABLE cities`); err != nil {
		t.Fatalf("drop: %v", err)
	}
	raw, err := tc.Nodes[0].DB().Get(ctx, keys.TableStatsKey(tableID))
	if err != nil {
		t.Fatalf("get after drop: %v", err)
	}
	if raw != nil {
		t.Fatal("stats blob survived DROP TABLE")
	}
}
