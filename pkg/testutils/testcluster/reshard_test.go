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
	"github.com/sthorne/datax/pkg/server"
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

	// The old layout (index 1) is RETAINED for historical reads and
	// recorded as a retired layout for the janitor.
	lo, hi := keys.TableIndexSpan(desc.ID, 1)
	rows, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("old layout was wiped at the swap; it must stay for historical reads")
	}
	if len(desc.RetiredLayouts) != 1 || desc.RetiredLayouts[0].PrimaryIndexID != 1 || desc.RetiredLayouts[0].Buckets != 2 {
		t.Fatalf("retired layouts: %+v", desc.RetiredLayouts)
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

// TestReshardWithSecondaryIndexes: ALTER TABLE ... SET (shards = 8) on a
// table carrying a unique and a non-unique index, under live ingest from
// another gateway. Index entries embed the shard bucket in their
// primary-key suffix, so the re-shard rebuilds both indexes at shadow IDs
// and swaps them with the primary layout. Issue #53 (TS1).
func TestReshardWithSecondaryIndexes(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const ttl = 2 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 1, ttl)

	execSQL(t, ctx, sA, `CREATE TABLE mx (series INT8, ts TIMESTAMPTZ, tag INT8 NOT NULL, v INT8 NOT NULL,
		PRIMARY KEY (series, ts)) WITH (timeseries = true, shards = 2)`)
	execSQL(t, ctx, sA, `CREATE UNIQUE INDEX by_tag ON mx (tag)`)
	execSQL(t, ctx, sA, `CREATE INDEX by_v ON mx (v)`)
	base := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC).UnixNano()
	at := func(i int) types.Datum { return types.NewTimestamp(base + int64(i)*int64(time.Second)) }
	for i := 0; i < 50; i++ {
		execSQL(t, ctx, sA, `INSERT INTO mx VALUES ($1, $2, $3, $4)`,
			types.NewInt(int64(i%5)), at(i), types.NewInt(int64(i)), types.NewInt(int64(i%5)))
	}
	execSQL(t, ctx, sB, `SELECT v FROM mx WHERE series = 0 AND ts = $1`, at(0)) // B leases pre-reshard

	desc := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "mx")
	oldIndexIDs := map[string]uint64{}
	for _, idx := range desc.Indexes {
		oldIndexIDs[idx.Name] = idx.ID
	}

	// B ingests through the whole re-shard: inserts with fresh unique
	// tags, index-moving updates, deletes.
	var net, updates atomic.Int64
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
			// Paced, and capped: enough live overlap to exercise the
			// dual-write mirrors without starving the backfill on a
			// loaded machine (each insert pays two index mirrors and a
			// unique-check read on top of the primary dual-write).
			if i >= 400 {
				<-stop
				done <- nil
				return
			}
			time.Sleep(2 * time.Millisecond)
			series, ts, tag := int64(100+i%7), at(1000+i), int64(1000+i)
			if _, serr := trySQL(ctx, sB, `INSERT INTO mx VALUES ($1, $2, $3, 7)`,
				types.NewInt(series), ts, types.NewInt(tag)); serr != nil {
				done <- fmt.Errorf("concurrent insert %d: [%s] %s", i, serr.Code, serr.Msg)
				return
			}
			net.Add(1)
			switch i % 10 {
			case 3: // move the by_v entry
				if _, serr := trySQL(ctx, sB, `UPDATE mx SET v = 9000 WHERE series = $1 AND ts = $2`,
					types.NewInt(series), ts); serr != nil {
					done <- fmt.Errorf("concurrent update %d: [%s] %s", i, serr.Code, serr.Msg)
					return
				}
				updates.Add(1)
			case 9:
				dres, serr := trySQL(ctx, sB, `DELETE FROM mx WHERE series = $1 AND ts = $2`,
					types.NewInt(series), ts)
				if serr != nil || dres.Tag != "DELETE 1" {
					done <- fmt.Errorf("concurrent delete %d: %v %v", i, serr, dres)
					return
				}
				net.Add(-1)
			}
		}
	}()
	for net.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}

	execSQL(t, ctx, sA, `ALTER TABLE mx SET (shards = 8)`)

	close(stop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	want := 50 + int(net.Load())

	// Descriptor: primary swapped AND both indexes moved to shadow IDs.
	desc = lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "mx")
	if desc.ShardBuckets != 8 || desc.Reshard != nil || desc.ReshardedAt == 0 {
		t.Fatalf("descriptor after swap: %+v", desc)
	}
	for _, idx := range desc.Indexes {
		if idx.ID == oldIndexIDs[idx.Name] {
			t.Fatalf("index %q kept its old ID %d across the re-shard", idx.Name, idx.ID)
		}
	}

	// Counts agree between a primary scan and an index-driven scan, on
	// both gateways.
	for name, s := range map[string]*sql.Session{"A": sA, "B": sB} {
		res := execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM mx`)
		if got := int(res.Rows[0][0].I); got != want {
			t.Fatalf("gateway %s: %d rows, want %d", name, got, want)
		}
		res = execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM mx WHERE v = 9000`)
		if got := int(res.Rows[0][0].I); got != int(updates.Load()) {
			t.Fatalf("gateway %s: %d updated rows via by_v, want %d", name, got, updates.Load())
		}
	}

	// Unique-index point lookups route through the new layout to the
	// right primary rows.
	if p := explainPlan(t, ctx, sA, `SELECT series FROM mx WHERE tag = 7`); p != `point lookup via unique index "by_tag"` {
		t.Fatalf("unique point plan: %q", p)
	}
	for _, tag := range []int64{0, 7, 23, 49} {
		res := execSQL(t, ctx, sA, `SELECT series, v FROM mx WHERE tag = $1`, types.NewInt(tag))
		if len(res.Rows) != 1 || res.Rows[0][0].I != tag%5 {
			t.Fatalf("tag %d lookup: %+v", tag, res.Rows)
		}
	}

	// Uniqueness still enforced after the swap.
	if _, serr := trySQL(ctx, sA, `INSERT INTO mx VALUES (900, $1, 7, 1)`, at(5000)); serr == nil ||
		serr.Code != sql.CodeUniqueViolation {
		t.Fatalf("duplicate tag after re-shard: %+v", serr)
	}
	// And index maintenance works post-swap: update moves, delete drops.
	execSQL(t, ctx, sA, `UPDATE mx SET v = 8000 WHERE series = 0 AND ts = $1`, at(0))
	res := execSQL(t, ctx, sA, `SELECT COUNT(*) AS n FROM mx WHERE v = 8000`)
	if res.Rows[0][0].I != 1 {
		t.Fatalf("post-swap update via index: %+v", res.Rows)
	}
	execSQL(t, ctx, sA, `DELETE FROM mx WHERE series = 0 AND ts = $1`, at(0))
	res = execSQL(t, ctx, sA, `SELECT COUNT(*) AS n FROM mx WHERE v = 8000`)
	if res.Rows[0][0].I != 0 {
		t.Fatalf("post-swap delete via index: %+v", res.Rows)
	}

	// The old layouts — primary and both index generations — are
	// retained for historical reads and recorded for the janitor.
	if len(desc.RetiredLayouts) != 1 || desc.RetiredLayouts[0].PrimaryIndexID != 1 ||
		len(desc.RetiredLayouts[0].IndexIDs) != 2 {
		t.Fatalf("retired layouts: %+v", desc.RetiredLayouts)
	}
	for name, id := range oldIndexIDs {
		lo, hi := keys.TableIndexSpan(desc.ID, id)
		rows, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			t.Fatalf("old index %q generation was wiped at the swap; it must stay for historical reads", name)
		}
	}
	lo, hi := keys.TableIndexSpan(desc.ID, 1)
	rows, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("old primary layout was wiped at the swap; it must stay for historical reads")
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

	// Secondary indexes no longer block re-sharding — but CREATE INDEX
	// and a re-shard exclude each other. Hold a re-shard open at the
	// start of its backfill and try to add an index from another session.
	execSQL(t, ctx, s, `CREATE INDEX by_v ON sharded (v)`)
	{
		hold, release := make(chan struct{}), make(chan struct{})
		sql.TestingReshardFailBackfill = func() error {
			close(hold)
			<-release
			return nil
		}
		reshardDone := make(chan *sql.Error, 1)
		go func() {
			s2 := sql.NewSession(n.DB(), catalog.NewAccessor())
			_, serr := trySQL(ctx, s2, `ALTER TABLE sharded SET (shards = 4)`)
			reshardDone <- serr
		}()
		<-hold
		if _, serr := trySQL(ctx, s, `CREATE INDEX by_v2 ON sharded (v)`); serr == nil ||
			serr.Code != sql.CodeActiveTransaction || !strings.Contains(serr.Msg, "re-shard") {
			t.Fatalf("CREATE INDEX during re-shard: %+v", serr)
		}
		close(release)
		sql.TestingReshardFailBackfill = nil
		if serr := <-reshardDone; serr != nil {
			t.Fatalf("held re-shard failed: [%s] %s", serr.Code, serr.Msg)
		}
	}

	// Explicit transaction blocks are rejected.
	execSQL(t, ctx, s, `BEGIN`)
	_, serr := trySQL(ctx, s, `ALTER TABLE sharded SET (shards = 4)`)
	execSQL(t, ctx, s, `ROLLBACK`)
	if serr == nil || serr.Code != sql.CodeActiveTransaction {
		t.Fatalf("in-txn re-shard: %+v", serr)
	}

	// Historical reads below the swap route through the RETAINED pre-swap
	// layout: they see exactly the pre-reshard data while current reads
	// see everything.
	execSQL(t, ctx, s, `CREATE TABLE g (series INT8, ts TIMESTAMPTZ, PRIMARY KEY (series, ts))
		WITH (timeseries = true, shards = 2)`)
	execSQL(t, ctx, s, `INSERT INTO g VALUES (1, '2026-08-30 00:00:00Z')`)
	preReshard := n.DB().Clock().Now().WallTime
	time.Sleep(10 * time.Millisecond)
	execSQL(t, ctx, s, `ALTER TABLE g SET (shards = 4)`)
	execSQL(t, ctx, s, `INSERT INTO g VALUES (2, '2026-08-30 01:00:00Z')`)

	res := execSQL(t, ctx, s, fmt.Sprintf(`SELECT COUNT(*) AS n FROM g AS OF SYSTEM TIME '%d'`, preReshard))
	if res.Rows[0][0].I != 1 {
		t.Fatalf("pre-reshard historical read: %+v", res.Rows)
	}
	res = execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM g`)
	if res.Rows[0][0].I != 2 {
		t.Fatalf("current read after re-shard: %+v", res.Rows)
	}
}

// TestReshardHistoricalReads: AS OF SYSTEM TIME below a re-shard serves
// from the retained pre-swap layout (indexes included), and is refused
// with the same error style only once the janitor reclaims that layout
// past the keep window. Issue #53 (TS2).
func TestReshardHistoricalReads(t *testing.T) {
	n, _ := startGCNodeCfg(t, func(c *server.Config) {
		c.ReshardRetireFor = 50 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE h (series INT8, ts TIMESTAMPTZ, v INT8 NOT NULL, PRIMARY KEY (series, ts))
		WITH (timeseries = true, shards = 2)`)
	execSQL(t, ctx, s, `CREATE INDEX h_by_v ON h (v)`)
	for i := 0; i < 5; i++ {
		execSQL(t, ctx, s, `INSERT INTO h VALUES ($1, $2, $3)`,
			types.NewInt(int64(i)), mustTS(t, 2026, 8, 30, i), types.NewInt(int64(100+i)))
	}
	ts0 := n.DB().Clock().Now().WallTime
	time.Sleep(10 * time.Millisecond)
	execSQL(t, ctx, s, `ALTER TABLE h SET (shards = 4)`)
	execSQL(t, ctx, s, `INSERT INTO h VALUES (10, $1, 200)`, mustTS(t, 2026, 8, 30, 10))

	asof := func(q string) (*sql.Result, *sql.Error) {
		return trySQL(ctx, s, fmt.Sprintf(q, ts0))
	}
	// Primary-path historical read: exactly the pre-swap rows.
	res, serr := asof(`SELECT COUNT(*) AS n FROM h AS OF SYSTEM TIME '%d'`)
	if serr != nil || res.Rows[0][0].I != 5 {
		t.Fatalf("historical count: %+v %+v", res, serr)
	}
	// Index-path historical read: the retained old index generation.
	res, serr = asof(`SELECT series FROM h AS OF SYSTEM TIME '%d' WHERE v = 103`)
	if serr != nil || len(res.Rows) != 1 || res.Rows[0][0].I != 3 {
		t.Fatalf("historical index read: %+v %+v", res, serr)
	}
	// The retired layout is recorded and still on disk.
	desc := lookupDescriptor(t, ctx, n.DB(), "h")
	if len(desc.RetiredLayouts) != 1 || desc.RetiredLayouts[0].PrimaryIndexID != 1 ||
		len(desc.RetiredLayouts[0].IndexIDs) != 1 || desc.RetiredLayouts[0].Buckets != 2 {
		t.Fatalf("retired layouts: %+v", desc.RetiredLayouts)
	}
	lo, hi := keys.TableIndexSpan(desc.ID, 1)
	if kvs, err := n.DB().Scan(ctx, lo, hi, 0); err != nil || len(kvs) == 0 {
		t.Fatalf("retired primary layout: %d keys, err %v", len(kvs), err)
	}

	// Past the keep window the janitor reclaims it: descriptor entry
	// first, then the keyspaces — and the historical read is refused.
	time.Sleep(100 * time.Millisecond)
	n.RunReshardJanitorOnce(ctx)
	desc = lookupDescriptor(t, ctx, n.DB(), "h")
	if len(desc.RetiredLayouts) != 0 {
		t.Fatalf("retired layouts after janitor: %+v", desc.RetiredLayouts)
	}
	for _, id := range []uint64{1, 2} { // old primary, old index generation
		lo, hi := keys.TableIndexSpan(desc.ID, id)
		if kvs, err := n.DB().Scan(ctx, lo, hi, 0); err != nil || len(kvs) != 0 {
			t.Fatalf("old generation %d after janitor: %d keys, err %v", id, len(kvs), err)
		}
	}
	if _, serr := asof(`SELECT COUNT(*) AS n FROM h AS OF SYSTEM TIME '%d'`); serr == nil ||
		serr.Code != sql.CodeFeatureNotSupported || !strings.Contains(serr.Msg, "re-shard") {
		t.Fatalf("historical read after reclamation: %+v", serr)
	}
	// Current reads are untouched.
	res = execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM h`)
	if res.Rows[0][0].I != 6 {
		t.Fatalf("current count: %+v", res.Rows)
	}
}

// TestHistoricalLookupDoesNotPoisonLease: a cold-cache AS OF lookup must
// not cache the historical descriptor or write a backdated lease — the
// next current read has to see the post-reshard schema.
func TestHistoricalLookupDoesNotPoisonLease(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := leasedSession(t, tc, 0, 2*time.Second)

	execSQL(t, ctx, s, `CREATE TABLE lp (series INT8, ts TIMESTAMPTZ, PRIMARY KEY (series, ts))
		WITH (timeseries = true, shards = 2)`)
	execSQL(t, ctx, s, `INSERT INTO lp VALUES (1, '2026-08-30 00:00:00Z')`)
	ts0 := tc.Nodes[0].DB().Clock().Now().WallTime
	time.Sleep(10 * time.Millisecond)
	execSQL(t, ctx, s, `ALTER TABLE lp SET (shards = 4)`)
	execSQL(t, ctx, s, `INSERT INTO lp VALUES (2, '2026-08-30 01:00:00Z')`)

	// A brand-new leased session: its FIRST touch of the table is the
	// historical read (cold cache), which sees the pre-swap layout.
	s2 := leasedSession(t, tc, 0, 2*time.Second)
	res := execSQL(t, ctx, s2, fmt.Sprintf(`SELECT COUNT(*) AS n FROM lp AS OF SYSTEM TIME '%d'`, ts0))
	if res.Rows[0][0].I != 1 {
		t.Fatalf("historical count: %+v", res.Rows)
	}
	// The very next current read must plan against the CURRENT layout —
	// a poisoned cache would route to the retired one and lose a row.
	res = execSQL(t, ctx, s2, `SELECT COUNT(*) AS n FROM lp`)
	if res.Rows[0][0].I != 2 {
		t.Fatalf("current count after historical lookup: %+v", res.Rows)
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
