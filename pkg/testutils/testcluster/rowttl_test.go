package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// noRowTTLLoop turns the background job off, for tests that drive passes
// themselves and assert on exactly what one pass did.
func noRowTTLLoop(cfg *server.Config) { cfg.RowTTLInterval = -1 }

func tsD(tm time.Time) types.Datum { return types.NewTimestamp(tm.UnixNano()) }

func countSQL(t *testing.T, ctx context.Context, s *sql.Session, src string, params ...types.Datum) int64 {
	t.Helper()
	return execSQL(t, ctx, s, src, params...).Rows[0][0].I
}

// liveKeys counts the keys a current read sees in one index of a table.
func liveKeys(t *testing.T, ctx context.Context, n *server.Node, tableID, indexID uint64) int {
	t.Helper()
	lo, hi := keys.TableIndexSpan(tableID, indexID)
	rows, err := n.DB().Scan(ctx, lo, hi, 0)
	if err != nil {
		t.Fatal(err)
	}
	return len(rows)
}

// TestRowTTLIndexesAndForeignKeys is issue #298's first acceptance item:
// a table with two secondary indexes, referenced by one foreign key that
// cascades and one that restricts, expires its rows. The indexes hold no
// entry for an expired row, the cascading children go with their parent,
// and a row a restricting child still references stays, is counted, and
// goes once the child does. A row whose expiry is NULL or in the future
// is untouched.
func TestRowTTLIndexesAndForeignKeys(t *testing.T) {
	tc := StartWithOptions(t, 1, noRowTTLLoop)
	n := tc.Nodes[0]
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE quarantine (
		id INT8 PRIMARY KEY, owner INT8, token TEXT, expires_at TIMESTAMPTZ
	) WITH (ttl_expiration = expires_at)`)
	execSQL(t, ctx, s, `CREATE INDEX quarantine_owner ON quarantine (owner)`)
	execSQL(t, ctx, s, `CREATE UNIQUE INDEX quarantine_token ON quarantine (token)`)
	execSQL(t, ctx, s, `CREATE TABLE note (id INT8 PRIMARY KEY, item INT8 REFERENCES quarantine (id) ON DELETE CASCADE)`)
	execSQL(t, ctx, s, `CREATE TABLE hold (id INT8 PRIMARY KEY, item INT8 REFERENCES quarantine (id))`)

	past, future := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	for i := 0; i < 30; i++ {
		execSQL(t, ctx, s, `INSERT INTO quarantine VALUES ($1, $2, $3, $4)`,
			intD(i), intD(i%3), strD("tok-"+string(rune('a'+i))), tsD(past.Add(time.Duration(i)*time.Second)))
	}
	for i := 100; i < 110; i++ {
		execSQL(t, ctx, s, `INSERT INTO quarantine VALUES ($1, 7, $2, $3)`, intD(i), strD("live-"+string(rune('a'+i-100))), tsD(future))
	}
	execSQL(t, ctx, s, `INSERT INTO quarantine VALUES (200, 7, 'never', NULL)`)
	for i := 0; i < 5; i++ {
		execSQL(t, ctx, s, `INSERT INTO note VALUES ($1, $2)`, intD(i), intD(i))
	}
	execSQL(t, ctx, s, `INSERT INTO note VALUES (50, 100)`) // child of a live row
	execSQL(t, ctx, s, `INSERT INTO hold VALUES (1, 3)`)    // restricts row 3
	execSQL(t, ctx, s, `INSERT INTO hold VALUES (2, 17)`)   // restricts row 17

	// The option made the index its scan needs.
	show := execSQL(t, ctx, s, `SHOW CREATE TABLE quarantine`)
	ddl := show.Rows[0][len(show.Rows[0])-1].S
	if !strings.Contains(ddl, "INDEX quarantine_ttl_idx (expires_at)") || !strings.Contains(ddl, "WITH (ttl_expiration = expires_at)") {
		t.Fatalf("SHOW CREATE TABLE lacks the expiry or its index:\n%s", ddl)
	}

	// An expired row is visible until the job gets to it.
	if got := countSQL(t, ctx, s, `SELECT count(*) FROM quarantine`); got != 41 {
		t.Fatalf("rows before the pass: %d", got)
	}

	passes := n.RunRowTTLOnce(ctx)
	if len(passes) != 1 || passes[0].Err != nil {
		t.Fatalf("pass: %+v", passes)
	}
	if p := passes[0]; p.Deleted != 28 || p.Blocked != 2 || p.Lag < 59*time.Minute || p.Lag > 61*time.Minute {
		t.Fatalf("pass deleted %d, blocked %d, lag %s; want 28, 2 and about an hour", p.Deleted, p.Blocked, p.Lag)
	}
	// Rows 3 and 17 (held), the ten live ones and the NULL one remain.
	if got := countSQL(t, ctx, s, `SELECT count(*) FROM quarantine`); got != 13 {
		t.Fatalf("rows after the pass: %d", got)
	}
	// Every index agrees, read as keys rather than through the planner.
	id := tableID(t, ctx, n, "quarantine")
	// Index 2 is the expiry column's, where the row with a NULL expiry
	// has no entry (a non-unique index holds none for a NULL).
	for idx, want := range map[uint64]int{1: 13, 2: 12, 3: 13, 4: 13} {
		if got := liveKeys(t, ctx, n, id, idx); got != want {
			t.Errorf("index %d holds %d entries, want %d", idx, got, want)
		}
	}
	// And through the planner, by each index.
	if got := countSQL(t, ctx, s, `SELECT count(*) FROM quarantine WHERE owner = 0`); got != 1 { // row 3
		t.Errorf("by owner: %d", got)
	}
	if got := countSQL(t, ctx, s, `SELECT count(*) FROM quarantine WHERE token = 'tok-b'`); got != 0 {
		t.Errorf("by token: %d", got)
	}
	// The cascade took the children of expired rows, except row 3's: it
	// was not deleted, so neither was its child.
	if got := countSQL(t, ctx, s, `SELECT count(*) FROM note`); got != 2 {
		t.Errorf("cascaded children left: %d, want 2 (of rows 3 and 100)", got)
	}
	if got := countSQL(t, ctx, s, `SELECT count(*) FROM hold`); got != 2 {
		t.Errorf("restricting children: %d", got)
	}

	// Release one hold: its row goes on the next pass, the other stays.
	execSQL(t, ctx, s, `DELETE FROM hold WHERE id = 1`)
	passes = n.RunRowTTLOnce(ctx)
	if len(passes) != 1 || passes[0].Deleted != 1 || passes[0].Blocked != 1 {
		t.Fatalf("second pass: %+v", passes)
	}
	if got := countSQL(t, ctx, s, `SELECT count(*) FROM quarantine WHERE id IN (3, 17)`); got != 1 {
		t.Fatalf("held rows left: %d", got)
	}

	// The column the expiry reads cannot be dropped from under it.
	if _, serr := trySQL(ctx, s, `ALTER TABLE quarantine DROP COLUMN expires_at`); serr == nil {
		t.Fatal("dropped the expiry column")
	}
}

// TestRowTTLAlterAndReset: the options go on and come off a table that
// already holds rows, in both spellings, and a table without a TTL is
// never touched.
func TestRowTTLAlterAndReset(t *testing.T) {
	tc := StartWithOptions(t, 1, noRowTTLLoop)
	n := tc.Nodes[0]
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE review (id INT8 PRIMARY KEY, created_at TIMESTAMPTZ, body TEXT)`)
	now := time.Now()
	for i := 0; i < 10; i++ {
		// Rows 0-4 are ten days old, rows 5-9 one day old.
		age := 24 * time.Hour
		if i < 5 {
			age = 240 * time.Hour
		}
		execSQL(t, ctx, s, `INSERT INTO review VALUES ($1, $2, 'b')`, intD(i), tsD(now.Add(-age)))
	}
	if passes := n.RunRowTTLOnce(ctx); len(passes) != 0 {
		t.Fatalf("a table without a TTL was expired: %+v", passes)
	}

	// A lifetime needs the column it is counted from.
	if _, serr := trySQL(ctx, s, `ALTER TABLE review SET (ttl = '7 days')`); serr == nil || !strings.Contains(serr.Msg, "ttl_column") {
		t.Fatalf("ttl without a column: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `ALTER TABLE review SET (ttl_column = body, ttl = '7 days')`); serr == nil {
		t.Fatal("a TEXT column was accepted as the expiry column")
	}
	execSQL(t, ctx, s, `ALTER TABLE review SET (ttl_column = created_at, ttl = '7 days')`)
	if passes := n.RunRowTTLOnce(ctx); len(passes) != 1 || passes[0].Deleted != 5 {
		t.Fatalf("7-day pass: %+v", passes)
	}
	// Shortening the lifetime alone keeps the column.
	execSQL(t, ctx, s, `ALTER TABLE review SET (ttl = '12 hours')`)
	show := execSQL(t, ctx, s, `SHOW CREATE TABLE review`)
	if ddl := show.Rows[0][len(show.Rows[0])-1].S; !strings.Contains(ddl, "ttl_column = created_at, ttl = '43200s'") {
		t.Fatalf("SHOW CREATE TABLE after SET (ttl):\n%s", ddl)
	}
	execSQL(t, ctx, s, `ALTER TABLE review RESET (ttl)`)
	if passes := n.RunRowTTLOnce(ctx); len(passes) != 0 {
		t.Fatalf("expired after RESET: %+v", passes)
	}
	if got := countSQL(t, ctx, s, `SELECT count(*) FROM review`); got != 5 {
		t.Fatalf("rows after RESET: %d", got)
	}
	execSQL(t, ctx, s, `ALTER TABLE review SET (ttl_column = created_at, ttl = '12 hours')`)
	if passes := n.RunRowTTLOnce(ctx); len(passes) != 1 || passes[0].Deleted != 5 {
		t.Fatalf("12-hour pass: %+v", passes)
	}
	if got := countSQL(t, ctx, s, `SELECT count(*) FROM review`); got != 0 {
		t.Fatalf("rows left: %d", got)
	}
}

// TestRowTTLLagUnderSteadyWrites is the second acceptance item, with the
// job running on its own on three nodes: rows arrive at a steady rate
// with a one-second lifetime, and throughout the run no row is found
// more than a bound past its expiry; the lag and the deletions are
// reported as metrics and the passes in the operations view. Small
// batches make every pass take several.
func TestRowTTLLagUnderSteadyWrites(t *testing.T) {
	tc := StartWithOptions(t, 3, func(cfg *server.Config) {
		cfg.RowTTLInterval = 200 * time.Millisecond
		cfg.RowTTLBatchSize = 7
		cfg.RowTTLBatchPause = time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE grace (id INT8 PRIMARY KEY, created_at TIMESTAMPTZ, who INT8)
		WITH (ttl_column = created_at, ttl = '1s')`)
	execSQL(t, ctx, s, `CREATE INDEX grace_who ON grace (who)`)

	deletedBefore := testutil.ToFloat64(metrics.RowTTLRowsDeleted)
	const bound = 3 * time.Second // the lifetime is 1s; a pass starts every 200ms
	const rows = 240
	var worst time.Duration
	for i := 0; i < rows; i++ {
		execSQL(t, ctx, s, `INSERT INTO grace VALUES ($1, $2, $3)`, intD(i), tsD(time.Now()), intD(i%5))
		time.Sleep(15 * time.Millisecond)
		if i%20 == 19 {
			// The oldest row still present says how far behind the job is.
			res := execSQL(t, ctx, s, `SELECT min(created_at) FROM grace`)
			if d := res.Rows[0][0]; !d.Null {
				if over := time.Since(time.Unix(0, d.I)) - time.Second; over > worst {
					worst = over
				}
			}
		}
	}
	if worst > bound {
		t.Fatalf("a row was still present %s after it expired; the bound is %s", worst, bound)
	}
	// Once the writes stop the table drains.
	deadline := time.Now().Add(15 * time.Second)
	for countSQL(t, ctx, s, `SELECT count(*) FROM grace`) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d row(s) never expired", countSQL(t, ctx, s, `SELECT count(*) FROM grace`))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := testutil.ToFloat64(metrics.RowTTLRowsDeleted) - deletedBefore; got != rows {
		t.Errorf("datax_row_ttl_rows_deleted_total rose by %v, want %d", got, rows)
	}
	if lag := testutil.ToFloat64(metrics.RowTTLLagSeconds); lag < 0 || lag > bound.Seconds() {
		t.Errorf("datax_row_ttl_lag_seconds = %v, outside [0, %v]", lag, bound.Seconds())
	}
	// The passes are operations on whichever node led range 1.
	found := false
	for _, n := range tc.Nodes {
		for _, ev := range n.Events().Recent(0, 0, true) {
			if ev.Kind == "row-ttl" && ev.Op != "" && strings.Contains(ev.Summary, "grace") {
				found = true
			}
		}
	}
	if !found {
		t.Error("no row-ttl operation in any node's operations view")
	}
}

// TestRowTTLSurvivesRestartAndBackup: the options are in the descriptor,
// so they are there after a node restarts — the job picks the table up
// again without being told — and in a backup, which restores a table
// that still expires its rows and still has its GC window.
func TestRowTTLSurvivesRestartAndBackup(t *testing.T) {
	dir := t.TempDir()
	n := startDiskNode(t, dir, true, "", noRowTTLLoop)
	stopped := false
	defer func() {
		if !stopped {
			n.Stop()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE token (id INT8 PRIMARY KEY, expires_at TIMESTAMPTZ, v TEXT)
		WITH (ttl_expiration = expires_at, gc_ttl = '10m')`)
	for i := 0; i < 25; i++ {
		execSQL(t, ctx, s, `INSERT INTO token VALUES ($1, $2, 'x')`, intD(i), tsD(time.Now().Add(-time.Minute)))
	}
	execSQL(t, ctx, s, `INSERT INTO token VALUES (100, $1, 'live')`, tsD(time.Now().Add(time.Hour)))

	backup := t.TempDir()
	if _, err := n.RunBackup(ctx, backup, "", false, false); err != nil {
		t.Fatal(err)
	}

	// Restart with the job on: nothing names the table to it again.
	n.Stop()
	n = startDiskNode(t, dir, false, "", func(cfg *server.Config) { cfg.RowTTLInterval = 100 * time.Millisecond })
	s = sql.NewSession(n.DB(), catalog.NewAccessor())
	deadline := time.Now().Add(60 * time.Second)
	for {
		res, serr := trySQL(ctx, s, `SELECT count(*) FROM token`)
		if serr == nil && res.Rows[0][0].I == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the restart the expired rows were not deleted: %v %v", res, serr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	n.Stop()
	stopped = true

	// The backup, into a fresh cluster.
	tc := StartWithOptions(t, 1, noRowTTLLoop)
	if _, err := tc.Nodes[0].RunRestore(ctx, []string{backup}); err != nil {
		t.Fatal(err)
	}
	s2 := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	show := execSQL(t, ctx, s2, `SHOW CREATE TABLE token`)
	if ddl := show.Rows[0][len(show.Rows[0])-1].S; !strings.Contains(ddl, "gc_ttl = '600s'") || !strings.Contains(ddl, "ttl_expiration = expires_at") {
		t.Fatalf("the restored table lost its options:\n%s", ddl)
	}
	if got := countSQL(t, ctx, s2, `SELECT count(*) FROM token`); got != 26 {
		t.Fatalf("restored rows: %d", got)
	}
	if passes := tc.Nodes[0].RunRowTTLOnce(ctx); len(passes) != 1 || passes[0].Deleted != 25 {
		t.Fatalf("pass on the restored table: %+v", passes)
	}
	// Nothing split the restored table's edges, and a GC window governs
	// only ranges of the table's own: the first GC pass that finds a
	// range straddling an edge asks for the splits.
	lo, hi := keys.TableDataSpan(tableID(t, ctx, tc.Nodes[0], "token"))
	deadline = time.Now().Add(30 * time.Second)
	for {
		tc.Nodes[0].Store().RunGCOnce(ctx, 24*time.Hour)
		own := false
		tc.Nodes[0].Store().VisitReplicas(func(r *kvserver.Replica) bool {
			d := r.Desc()
			own = own || d.StartKey.Equal(lo) && d.EndKey.Equal(hi)
			return true
		})
		if own {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the restored table with a GC window never got ranges of its own")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
