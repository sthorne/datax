package testcluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestIndexJoinBatches (issue #103): an index join fetches its primary
// rows in pages of 256 — exactly 256, 257 and 0 matches, a residual
// filter, a limit, ordering — a range split landing between the index
// scan and the fetch still yields the complete result, and FOR UPDATE
// over an index join locks every selected row in one batch.
func TestIndexJoinBatches(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	execSQL(t, ctx, s, `CREATE TABLE ij (k INT8 PRIMARY KEY, g INT8, pad TEXT)`)
	execSQL(t, ctx, s, `CREATE INDEX ij_g ON ij (g)`)
	insert := func(g, from, n int) {
		t.Helper()
		var vals []string
		for i := 0; i < n; i++ {
			vals = append(vals, fmt.Sprintf("(%d, %d, 'p%d')", from+i, g, i%3))
		}
		for i := 0; i < len(vals); i += 100 {
			end := i + 100
			if end > len(vals) {
				end = len(vals)
			}
			execSQL(t, ctx, s, "INSERT INTO ij VALUES "+strings.Join(vals[i:end], ", "))
		}
	}
	insert(1, 1000, 256)
	insert(2, 2000, 257)
	insert(4, 4000, 5)

	count := func(q string) int {
		t.Helper()
		return len(execSQL(t, ctx, s, q).Rows)
	}
	if n := count(`SELECT k FROM ij WHERE g = 1`); n != 256 {
		t.Fatalf("g = 1: %d rows, want 256", n)
	}
	if n := count(`SELECT k FROM ij WHERE g = 2`); n != 257 {
		t.Fatalf("g = 2: %d rows, want 257", n)
	}
	if n := count(`SELECT k FROM ij WHERE g = 3`); n != 0 {
		t.Fatalf("g = 3: %d rows, want 0", n)
	}
	if n := count(`SELECT k FROM ij WHERE g = 2 LIMIT 300`); n != 257 {
		t.Fatalf("g = 2 limit 300: %d rows, want 257", n)
	}
	r := execSQL(t, ctx, s, `SELECT k FROM ij WHERE g = 2 ORDER BY k`)
	for i, row := range r.Rows {
		if row[0].I != int64(2000+i) {
			t.Fatalf("g = 2 ordered: row %d is %d", i, row[0].I)
		}
	}
	r = execSQL(t, ctx, s, `SELECT k FROM ij WHERE g = 2 ORDER BY k LIMIT 10`)
	if len(r.Rows) != 10 || r.Rows[9][0].I != 2009 {
		t.Fatalf("g = 2 ordered limit 10: %+v", r.Rows)
	}
	if n := count(`SELECT k FROM ij WHERE g = 4 AND pad = 'p0'`); n != 2 {
		t.Fatalf("residual filter: %d rows, want 2", n)
	}
	if p := explainPlan(t, ctx, s, `SELECT k FROM ij WHERE g = 2`); p != `scan of index "ij_g" (1 column prefix) + primary key join` {
		t.Fatalf("plan: %s", p)
	}
	var lines []string
	for _, row := range execSQL(t, ctx, s, `EXPLAIN ANALYZE SELECT k FROM ij WHERE g = 2`).Rows {
		lines = append(lines, row[0].Text())
	}
	if joined := strings.Join(lines, "\n"); !strings.Contains(joined, "index join: 257 primary rows fetched in 2 batches of up to 256") {
		t.Fatalf("explain analyze lacks the batch note:\n%s", joined)
	}

	// A split of the table's primary key span between the index scan and
	// the fetch: the routed batch re-addresses its keys and the result is
	// complete.
	var once sync.Once
	sql.TestingBeforePrimaryFetch = func() {
		once.Do(func() {
			mid := append(keys.TableDataPrefix(tableID(t, ctx, tc.Nodes[0], "ij")), 0x80)
			if _, err := tc.Nodes[0].DB().AdminSplit(ctx, mid); err != nil {
				t.Errorf("split mid-fetch: %v", err)
			}
		})
	}
	defer func() { sql.TestingBeforePrimaryFetch = nil }()
	if n := count(`SELECT k FROM ij WHERE g = 2`); n != 257 {
		t.Fatalf("g = 2 across a split mid-fetch: %d rows, want 257", n)
	}
	sql.TestingBeforePrimaryFetch = nil

	// FOR UPDATE through the index join locks every selected row: a
	// writer with a lock timeout is refused on the first and the last of
	// them until the locking transaction ends.
	execSQL(t, ctx, s, `BEGIN`)
	if n := len(execSQL(t, ctx, s, `SELECT k FROM ij WHERE g = 1 FOR UPDATE`).Rows); n != 256 {
		t.Fatalf("for update: %d rows, want 256", n)
	}
	w := sql.NewSession(tc.Nodes[1].DB(), catalog.NewAccessor())
	execSQL(t, ctx, w, `SET lock_timeout = '300ms'`)
	for _, k := range []int{1000, 1255} {
		if _, serr := trySQL(ctx, w, fmt.Sprintf(`UPDATE ij SET pad = 'w' WHERE k = %d`, k)); serr == nil || serr.Code != sql.CodeLockNotAvailable {
			t.Fatalf("update of locked row %d: want %s, got %v", k, sql.CodeLockNotAvailable, serr)
		}
	}
	if _, serr := trySQL(ctx, w, `UPDATE ij SET pad = 'w' WHERE k = 2000`); serr != nil {
		t.Fatalf("update of an unlocked row: %v", serr)
	}
	execSQL(t, ctx, s, `ROLLBACK`)
	execSQL(t, ctx, w, `UPDATE ij SET pad = 'w' WHERE k = 1000`)
}
