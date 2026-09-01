package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestStatsDriveAccessPaths: before ANALYZE the planner's EXPLAIN output
// is byte-identical to the structural planner; after ANALYZE a
// low-selectivity indexed predicate flips to a full table scan while a
// selective one keeps its index, and both carry row estimates. Issue #56
// (SA3).
func TestStatsDriveAccessPaths(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	explain := func(q string) string {
		t.Helper()
		var plan string
		if err := conn.QueryRow(ctx, "EXPLAIN "+q).Scan(&plan); err != nil {
			t.Fatalf("explain %q: %v", q, err)
		}
		return plan
	}

	// cat: 2 distinct values over 200 rows (low selectivity); tag: unique
	// per row (high selectivity). Both indexed, both NOT NULL so the
	// structural planner is willing to use the indexes.
	for _, q := range []string{
		`CREATE TABLE ev (id INT8 PRIMARY KEY, cat TEXT NOT NULL, tag TEXT NOT NULL)`,
		`CREATE INDEX by_cat ON ev (cat)`,
		`CREATE INDEX by_tag ON ev (tag)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	batch := &pgx.Batch{}
	for i := 0; i < 200; i++ {
		batch.Queue(`INSERT INTO ev VALUES ($1, $2, $3)`,
			int64(i), fmt.Sprintf("cat-%d", i%2), fmt.Sprintf("tag-%d", i))
	}
	if err := conn.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	catQ := `SELECT id FROM ev WHERE cat = 'cat-0'`
	tagQ := `SELECT id FROM ev WHERE tag = 'tag-7'`

	// Without statistics: structural planner, no row estimates — these
	// strings are the pre-statistics planner's, byte for byte.
	if got, want := explain(catQ), `scan of index "by_cat" (1 column prefix) + primary key join`; got != want {
		t.Fatalf("pre-ANALYZE cat plan = %q, want %q", got, want)
	}
	if got, want := explain(tagQ), `scan of index "by_tag" (1 column prefix) + primary key join`; got != want {
		t.Fatalf("pre-ANALYZE tag plan = %q, want %q", got, want)
	}

	if _, err := conn.Exec(ctx, `ANALYZE ev`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	// cat = 'cat-0' selects ~100 rows; the index scan would pay an extra
	// primary-key join per row (100 × 4 ≥ 200), so the full scan wins.
	if got, want := explain(catQ), `full table scan [~200 rows]`; got != want {
		t.Fatalf("post-ANALYZE cat plan = %q, want %q", got, want)
	}
	// tag is nearly unique: the index stays and the estimate shows it.
	if got, want := explain(tagQ), `scan of index "by_tag" (1 column prefix) + primary key join [~1 rows]`; got != want {
		t.Fatalf("post-ANALYZE tag plan = %q, want %q", got, want)
	}

	// The flipped plan still returns the right rows.
	rows, err := conn.Query(ctx, catQ)
	if err != nil {
		t.Fatalf("cat query: %v", err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if n != 100 {
		t.Fatalf("cat rows = %d, want 100", n)
	}
}
