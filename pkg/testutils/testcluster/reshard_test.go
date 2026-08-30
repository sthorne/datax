package testcluster

import (
	"context"
	"fmt"
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

// TestOnlineReshardUnderConcurrentWrites: ALTER TABLE ... SET (shards = 8)
// re-keys a 2-shard timeseries table while another gateway inserts and
// deletes continuously. Afterwards: descriptor swapped, every surviving
// row readable (count matches the ledger), point lookups recompute the
// new bucket, fan-out covers all 8, the old layout is empty, and the new
// layout's bucket pre-splits exist.
func TestOnlineReshardUnderConcurrentWrites(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const ttl = 2 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 1, ttl)

	execSQL(t, ctx, sA, `CREATE TABLE m (series INT8, ts TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (series, ts))
		WITH (timeseries = true, shards = 2)`)
	base := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC).UnixNano()
	at := func(i int) types.Datum { return types.NewTimestamp(base + int64(i)*int64(time.Second)) }
	for i := 0; i < 50; i++ {
		execSQL(t, ctx, sA, `INSERT INTO m VALUES ($1, $2, 1.0)`, types.NewInt(int64(i%5)), at(i))
	}
	// B leases the pre-reshard descriptor.
	execSQL(t, ctx, sB, `SELECT v FROM m WHERE series = 0 AND ts = $1`, at(0))

	// B keeps writing (and occasionally deleting) on its own gateway for
	// the whole re-shard; the ledger tracks the surviving row count.
	var net atomic.Int64
	var delMu sync.Mutex
	deleted := map[int]int64{} // i -> series, for phantom diagnosis
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			series, ts := int64(100+i%7), at(1000+i)
			if _, serr := trySQL(ctx, sB, `INSERT INTO m VALUES ($1, $2, 2.0)`, types.NewInt(series), ts); serr != nil {
				done <- fmt.Errorf("concurrent insert %d: [%s] %s", i, serr.Code, serr.Msg)
				return
			}
			net.Add(1)
			if i%10 == 9 {
				dres, serr := trySQL(ctx, sB, `DELETE FROM m WHERE series = $1 AND ts = $2`, types.NewInt(series), ts)
				if serr != nil {
					done <- fmt.Errorf("concurrent delete %d: [%s] %s", i, serr.Code, serr.Msg)
					return
				}
				if dres.Tag != "DELETE 1" {
					done <- fmt.Errorf("concurrent delete %d matched %q (row invisible to its own session?)", i, dres.Tag)
					return
				}
				delMu.Lock()
				deleted[1000+i] = series
				delMu.Unlock()
				net.Add(-1)
			}
		}
	}()
	for net.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}

	execSQL(t, ctx, sA, `ALTER TABLE m SET (shards = 8)`)

	close(stop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	want := 50 + int(net.Load())

	// Descriptor: swapped, no pending state, primary index moved.
	desc := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "m")
	if desc.ShardBuckets != 8 || desc.Reshard != nil || desc.LivePrimaryIndex() == 1 || desc.ReshardedAt == 0 {
		t.Fatalf("descriptor after swap: buckets=%d reshard=%v primary=%d reshardedAt=%d",
			desc.ShardBuckets, desc.Reshard, desc.LivePrimaryIndex(), desc.ReshardedAt)
	}

	// Every row is there — counted through BOTH gateways (B's lease must
	// have adopted the swap by the time the ALTER returned).
	for name, s := range map[string]*sql.Session{"A": sA, "B": sB} {
		res := execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM m`)
		if got := int(res.Rows[0][0].I); got != want {
			delMu.Lock()
			for i, series := range deleted {
				r := execSQL(t, ctx, s, `SELECT v FROM m WHERE series = $1 AND ts = $2`, types.NewInt(series), at(i))
				if len(r.Rows) != 0 {
					t.Logf("PHANTOM: deleted row (series=%d, i=%d) still present", series, i)
				}
			}
			delMu.Unlock()
			t.Fatalf("gateway %s: %d rows after re-shard, want %d", name, got, want)
		}
	}

	// Point lookup stays a point (bucket recomputed mod 8), and fan-out
	// says 8.
	if p := explainPlan(t, ctx, sA, `SELECT v FROM m WHERE series = 0 AND ts = '2026-08-30 00:00:00Z'`); p != "point lookup on primary key" {
		t.Fatalf("point plan: %q", p)
	}
	if p := explainPlan(t, ctx, sA, `SELECT v FROM m WHERE series = 0 AND ts >= '2026-08-30 00:00:00Z'`); !strings.Contains(p, "fan-out over 8 shard buckets") {
		t.Fatalf("fan plan: %q", p)
	}
	res := execSQL(t, ctx, sA, `SELECT v FROM m WHERE series = 0 AND ts = $1`, at(0))
	if len(res.Rows) != 1 {
		t.Fatalf("point read after re-shard: %+v", res.Rows)
	}

	// The old layout (index 1) is wiped.
	lo, hi := keys.TableIndexSpan(desc.ID, 1)
	rows, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("old layout still holds %d keys", len(rows))
	}

	// The new layout's bucket pre-splits landed: range boundaries exist
	// inside the new index span.
	descs, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	newLo, newHi := keys.TableIndexSpan(desc.ID, desc.LivePrimaryIndex())
	boundaries := 0
	for _, d := range descs {
		if d.StartKey.Compare(newLo) > 0 && d.StartKey.Compare(newHi) < 0 {
			boundaries++
		}
	}
	if boundaries < 7 {
		t.Fatalf("only %d range boundaries inside the new layout, want >= 7", boundaries)
	}
}

// TestReshardValidationAndGuards: the scope-guard matrix and the
// historical-read guard.
func TestReshardValidationAndGuards(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE plain (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s, `CREATE TABLE unsharded_ts (ts TIMESTAMPTZ PRIMARY KEY) WITH (timeseries = true)`)
	execSQL(t, ctx, s, `CREATE TABLE sharded (series INT8, ts TIMESTAMPTZ, v INT8, PRIMARY KEY (series, ts))
		WITH (timeseries = true, shards = 2)`)
	execSQL(t, ctx, s, `INSERT INTO sharded VALUES (1, '2026-08-30 00:00:00Z', 10)`)

	for q, wantCode := range map[string]string{
		`ALTER TABLE plain SET (shards = 4)`:        sql.CodeFeatureNotSupported,
		`ALTER TABLE unsharded_ts SET (shards = 4)`: sql.CodeFeatureNotSupported,
		`ALTER TABLE sharded SET (shards = 2)`:      sql.CodeSyntaxError, // same count
		`ALTER TABLE sharded SET (shards = 1)`:      sql.CodeSyntaxError,
		`ALTER TABLE sharded SET (shards = 500)`:    sql.CodeSyntaxError,
		`ALTER TABLE sharded SET (nope = 4)`:        sql.CodeFeatureNotSupported,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != wantCode {
			t.Fatalf("%s: %+v, want %s", q, serr, wantCode)
		}
	}

	// Secondary indexes block re-sharding.
	execSQL(t, ctx, s, `CREATE INDEX by_v ON sharded (v)`)
	if _, serr := trySQL(ctx, s, `ALTER TABLE sharded SET (shards = 4)`); serr == nil ||
		serr.Code != sql.CodeFeatureNotSupported || !strings.Contains(serr.Msg, "secondary indexes") {
		t.Fatalf("indexed table re-shard: %+v", serr)
	}

	// Explicit transaction blocks are rejected.
	execSQL(t, ctx, s, `BEGIN`)
	_, serr := trySQL(ctx, s, `ALTER TABLE sharded SET (shards = 4)`)
	execSQL(t, ctx, s, `ROLLBACK`)
	if serr == nil || serr.Code != sql.CodeActiveTransaction {
		t.Fatalf("in-txn re-shard: %+v", serr)
	}

	// Historical-read guard: reads below the swap are refused; at/after
	// (and current reads) work.
	execSQL(t, ctx, s, `CREATE TABLE g (series INT8, ts TIMESTAMPTZ, PRIMARY KEY (series, ts))
		WITH (timeseries = true, shards = 2)`)
	execSQL(t, ctx, s, `INSERT INTO g VALUES (1, '2026-08-30 00:00:00Z')`)
	preReshard := n.DB().Clock().Now().WallTime
	time.Sleep(10 * time.Millisecond)
	execSQL(t, ctx, s, `ALTER TABLE g SET (shards = 4)`)

	if _, serr := trySQL(ctx, s, fmt.Sprintf(`SELECT COUNT(*) AS n FROM g AS OF SYSTEM TIME '%d'`, preReshard)); serr == nil ||
		serr.Code != sql.CodeFeatureNotSupported || !strings.Contains(serr.Msg, "re-shard") {
		t.Fatalf("pre-reshard historical read: %+v", serr)
	}
	res := execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM g`)
	if res.Rows[0][0].I != 1 {
		t.Fatalf("current read after re-shard: %+v", res.Rows)
	}
}

// TestReshardRetentionContinues: retention expiry keeps working on the
// re-sharded layout (the retention span covers every index generation).
func TestReshardRetentionContinues(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE ev (id INT8, at TIMESTAMPTZ, PRIMARY KEY (id, at))
		WITH (timeseries = true, retention = '1s', shards = 2)`)
	for i := 0; i < 5; i++ {
		execSQL(t, ctx, s, `INSERT INTO ev VALUES ($1, $2)`,
			types.NewInt(int64(i)), mustTS(t, 2026, 8, 30, i))
	}
	execSQL(t, ctx, s, `ALTER TABLE ev SET (shards = 4)`)

	time.Sleep(1200 * time.Millisecond)
	n.Store().RunGCOnce(ctx, 24*time.Hour)

	res := execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM ev`)
	if res.Rows[0][0].I != 0 {
		t.Fatalf("rows survived retention after re-shard: %+v", res.Rows)
	}
}

// TestReshardFailureCleanup: a failed backfill abandons cleanly — the
// dual-write marker clears, the partial new layout is wiped, the table
// keeps serving at its old sharding, and a retry succeeds.
func TestReshardFailureCleanup(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE f (series INT8, ts TIMESTAMPTZ, PRIMARY KEY (series, ts))
		WITH (timeseries = true, shards = 2)`)
	for i := 0; i < 10; i++ {
		execSQL(t, ctx, s, `INSERT INTO f VALUES ($1, $2)`, types.NewInt(int64(i)), mustTS(t, 2026, 8, 30, i))
	}

	sql.TestingReshardFailBackfill = func() error { return fmt.Errorf("injected backfill failure") }
	defer func() { sql.TestingReshardFailBackfill = nil }()
	if _, serr := trySQL(ctx, s, `ALTER TABLE f SET (shards = 8)`); serr == nil ||
		!strings.Contains(serr.Msg, "injected backfill failure") {
		t.Fatalf("failed re-shard: %+v", serr)
	}

	// Abandoned: no pending marker, old sharding intact, table serves.
	desc := lookupDescriptor(t, ctx, n.DB(), "f")
	if desc.Reshard != nil || desc.ShardBuckets != 2 || desc.LivePrimaryIndex() != 1 {
		t.Fatalf("descriptor after abandon: %+v", desc)
	}
	execSQL(t, ctx, s, `INSERT INTO f VALUES (99, '2026-08-30 23:00:00Z')`)
	res := execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM f`)
	if res.Rows[0][0].I != 11 {
		t.Fatalf("rows after abandon: %+v", res.Rows)
	}

	// Retry with the hook cleared succeeds and lands all rows.
	sql.TestingReshardFailBackfill = nil
	execSQL(t, ctx, s, `ALTER TABLE f SET (shards = 8)`)
	desc = lookupDescriptor(t, ctx, n.DB(), "f")
	if desc.ShardBuckets != 8 || desc.Reshard != nil {
		t.Fatalf("descriptor after retry: %+v", desc)
	}
	res = execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM f`)
	if res.Rows[0][0].I != 11 {
		t.Fatalf("rows after retry: %+v", res.Rows)
	}
}
