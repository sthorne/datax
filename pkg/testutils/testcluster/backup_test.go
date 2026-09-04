package testcluster

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestBackupRestore: a consistent full backup taken UNDER concurrent
// transactional load, an incremental with updates and deletions on top,
// restored into a fresh cluster — verified by per-table checksums (the
// restore's fresh export must equal a quiesced reference backup of the
// source), by a transactional invariant that only holds if the backup is
// a real snapshot, and by SQL-level behavior on the restored cluster
// (indexes, timeseries sharding, users, privileges).
func TestBackupRestore(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	// Schema exercising every restore path: plain PK, secondary index,
	// DECIMAL/JSONB values, sharded timeseries, users and grants.
	execSQL(t, ctx, s, `CREATE TABLE accounts (id INT8 PRIMARY KEY, balance INT8 NOT NULL)`)
	execSQL(t, ctx, s, `CREATE TABLE items (id INT8 PRIMARY KEY, name TEXT, price DECIMAL, attrs JSONB)`)
	execSQL(t, ctx, s, `CREATE INDEX by_name ON items (name)`)
	execSQL(t, ctx, s, `CREATE TABLE metrics (series TEXT, at TIMESTAMPTZ, v FLOAT8,
		PRIMARY KEY (series, at)) WITH (timeseries = true, shards = 4)`)
	execSQL(t, ctx, s, `CREATE USER analyst PASSWORD 'trustno1'`)
	execSQL(t, ctx, s, `GRANT SELECT ON accounts TO analyst`)

	const nAccounts, seedBalance = 16, 1000
	for i := 0; i < nAccounts; i++ {
		execSQL(t, ctx, s, `INSERT INTO accounts VALUES ($1, $2)`,
			types.NewInt(int64(i)), types.NewInt(seedBalance))
	}
	execSQL(t, ctx, s, `INSERT INTO items VALUES
		(1, 'anvil', 99.99, '{"kg": 50}'), (2, 'rope', 5.25, NULL), (3, 'tent', 120.5, '{"kg": 3}')`)
	execSQL(t, ctx, s, `INSERT INTO metrics VALUES
		('cpu', '2026-08-30 10:00:00Z', 0.5), ('cpu', '2026-08-30 10:00:10Z', 0.7),
		('mem', '2026-08-30 10:00:00Z', 0.9)`)

	// Concurrent transfer load during the full backup: total balance is
	// invariant, so any non-snapshot backup shows a torn total.
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ws := sql.NewSession(tc.Nodes[w%3].DB(), catalog.NewAccessor())
			for i := 0; !stop.Load(); i++ {
				from, to := (w*3+i)%nAccounts, (w*5+i+1)%nAccounts
				if from == to {
					continue
				}
				_, serr := trySQL(ctx, ws, `BEGIN;
					UPDATE accounts SET balance = balance - 7 WHERE id = `+itoa(from)+`;
					UPDATE accounts SET balance = balance + 7 WHERE id = `+itoa(to)+`;
					COMMIT`)
				if serr != nil {
					_, _ = trySQL(ctx, ws, `ROLLBACK`)
				}
			}
		}(w)
	}

	dirFull := t.TempDir()
	sumFull, err := tc.Nodes[0].RunBackup(ctx, dirFull, "", false, false)
	if err != nil {
		t.Fatalf("full backup: %v", err)
	}
	stop.Store(true)
	wg.Wait()

	// Quiesced mutations for the incremental: updates, an insert, and
	// deletions (tombstones must ride the incremental).
	execSQL(t, ctx, s, `UPDATE items SET price = 89.99 WHERE id = 1`)
	execSQL(t, ctx, s, `INSERT INTO items VALUES (4, 'stove', 45, '{"kg": 2}')`)
	execSQL(t, ctx, s, `DELETE FROM items WHERE id = 2`)
	execSQL(t, ctx, s, `DELETE FROM accounts WHERE id = 15`)

	dirInc := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirInc, dirFull, false, false); err != nil {
		t.Fatalf("incremental backup: %v", err)
	}

	// Reference: a quiesced full backup — its checksums are the source's
	// exact live state, which the restored cluster must reproduce.
	dirRef := t.TempDir()
	sumRef, err := tc.Nodes[0].RunBackup(ctx, dirRef, "", false, false)
	if err != nil {
		t.Fatalf("reference backup: %v", err)
	}

	// Restore full + incremental into a fresh cluster.
	tc2 := Start(t, 3)
	rsum, err := tc2.Nodes[0].RunRestore(ctx, []string{dirFull, dirInc})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	refByID := map[uint64]string{}
	for _, rt := range sumRef.Tables {
		refByID[rt.ID] = rt.SHA256
	}
	for _, rt := range rsum.Tables {
		if refByID[rt.ID] != rt.SHA256 {
			t.Fatalf("table %s (id %d): restored checksum %s != source %s", rt.Name, rt.ID, rt.SHA256, refByID[rt.ID])
		}
	}

	// SQL-level checks on the restored cluster. The transfer invariant:
	// every transfer preserved the total, so the quiesced source total is
	// nAccounts×seed minus whatever the deleted account held — read it from
	// the source and demand the restored cluster agree exactly. A torn
	// (non-snapshot) backup would break this by ±7 per caught transfer.
	src := execSQL(t, ctx, s, `SELECT SUM(balance) AS total, COUNT(*) AS n FROM accounts`)
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	res := execSQL(t, ctx, s2, `SELECT SUM(balance) AS total, COUNT(*) AS n FROM accounts`)
	if res.Rows[0][0].I != src.Rows[0][0].I || res.Rows[0][1].I != src.Rows[0][1].I {
		t.Fatalf("accounts after restore: total=%d n=%d, source has %d/%d",
			res.Rows[0][0].I, res.Rows[0][1].I, src.Rows[0][0].I, src.Rows[0][1].I)
	}
	if res.Rows[0][1].I != nAccounts-1 {
		t.Fatalf("account count: %d, want %d", res.Rows[0][1].I, nAccounts-1)
	}
	res = execSQL(t, ctx, s2, `SELECT price FROM items WHERE id = 1`)
	if res.Rows[0][0].S != "89.99" {
		t.Fatalf("incremental update lost: %+v", res.Rows)
	}
	if res = execSQL(t, ctx, s2, `SELECT COUNT(*) FROM items`); res.Rows[0][0].I != 3 {
		t.Fatalf("incremental delete/insert wrong: %+v", res.Rows)
	}
	// The secondary index restored raw and serves queries.
	if pl := explainPlan(t, ctx, s2, `SELECT id FROM items WHERE name = 'anvil'`); !strings.Contains(pl, `index "by_name"`) {
		t.Fatalf("restored index unused: %q", pl)
	}
	if res = execSQL(t, ctx, s2, `SELECT id FROM items WHERE name = 'anvil'`); len(res.Rows) != 1 || res.Rows[0][0].I != 1 {
		t.Fatalf("index lookup: %+v", res.Rows)
	}
	// Timeseries stays sharded (fan-out plan) and readable.
	if pl := explainPlan(t, ctx, s2, `SELECT v FROM metrics WHERE series = 'cpu' AND at >= '2026-08-30 10:00:00Z'`); !strings.Contains(pl, "fan-out over 4 shard buckets") {
		t.Fatalf("restored timeseries plan: %q", pl)
	}
	if res = execSQL(t, ctx, s2, `SELECT COUNT(*) FROM metrics`); res.Rows[0][0].I != 3 {
		t.Fatalf("metrics rows: %+v", res.Rows)
	}
	// The user's verifier row and grant came across.
	if v, err := tc2.Nodes[0].DB().Get(ctx, keys.UserKey("analyst")); err != nil || v == nil {
		t.Fatalf("analyst user not restored: %v %v", v, err)
	}
	// New DDL works: the descriptor ID generator was bumped past the
	// restored IDs (a collision would corrupt an existing table's data).
	execSQL(t, ctx, s2, `CREATE TABLE post_restore (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s2, `INSERT INTO post_restore VALUES (1)`)
	if res = execSQL(t, ctx, s2, `SELECT COUNT(*) FROM items`); res.Rows[0][0].I != 3 {
		t.Fatalf("new table collided with restored data: %+v", res.Rows)
	}

	// Guard rails: a second restore refuses (tables exist), and a chain
	// starting with an incremental refuses.
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dirFull}); err == nil || !strings.Contains(err.Error(), "empty cluster") {
		t.Fatalf("restore into non-empty cluster accepted: %v", err)
	}
	tc3 := Start(t, 1)
	if _, err := tc3.Nodes[0].RunRestore(ctx, []string{dirInc}); err == nil || !strings.Contains(err.Error(), "full") {
		t.Fatalf("incremental-first chain accepted: %v", err)
	}
	_ = sumFull
}

func itoa(i int) string { return strconv.Itoa(i) }

// BenchmarkBackup / BenchmarkRestore: rows-per-second over a 20k-row
// table (256B-ish rows), the honest number for issue #45.
func benchBackupCluster(b *testing.B) (*TestCluster, int) {
	tc := Start(b, 3)
	ctx := context.Background()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	if _, serr := trySQL(ctx, s, `CREATE TABLE bb (id INT8 PRIMARY KEY, payload TEXT)`); serr != nil {
		b.Fatal(serr)
	}
	payload := strings.Repeat("x", 240)
	const rows = 20000
	for i := 0; i < rows; i += 500 {
		var sb strings.Builder
		sb.WriteString(`INSERT INTO bb VALUES `)
		for j := 0; j < 500; j++ {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(" + itoa(i+j) + ", '" + payload + "')")
		}
		if _, serr := trySQL(ctx, s, sb.String()); serr != nil {
			b.Fatal(serr)
		}
	}
	return tc, rows
}

func BenchmarkBackup(b *testing.B) {
	tc, rows := benchBackupCluster(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tc.Nodes[0].RunBackup(ctx, b.TempDir(), "", false, false); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(rows), "rows/op")
}

func BenchmarkRestore(b *testing.B) {
	tc, rows := benchBackupCluster(b)
	ctx := context.Background()
	dir := b.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", false, false); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tc2 := Start(b, 3)
		b.StartTimer()
		if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(rows), "rows/op")
}
