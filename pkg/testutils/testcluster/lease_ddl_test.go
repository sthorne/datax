package testcluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// leasedSession builds a session backed by its own leased accessor — its own
// gateway identity — on the given node. Two of these are two gateways as far
// as descriptor leasing is concerned.
func leasedSession(t *testing.T, tc *TestCluster, node int, ttl time.Duration) *sql.Session {
	t.Helper()
	n := tc.Nodes[node]
	cat := catalog.NewAccessor()
	if err := cat.StartLeasing(n.DB(), n.Clock(), n.Stopper(), ttl); err != nil {
		t.Fatal(err)
	}
	return sql.NewSession(n.DB(), cat)
}

// lookupDescriptor reads a table descriptor fresh (bare accessor, no cache).
func lookupDescriptor(t *testing.T, ctx context.Context, db *kvclient.DB, name string) *catalog.TableDescriptor {
	t.Helper()
	var desc *catalog.TableDescriptor
	err := db.RunTxn(ctx, "test-lookup", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.NewAccessor().Lookup(ctx, txn, name)
		desc = d
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return desc
}

// TestDescriptorLeaseDrainOnAlter: gateway B caches a table under lease;
// gateway A runs ALTER TABLE ADD COLUMN. The DDL drains until B's renewal
// adopts the new version, so the moment A's statement returns, B — still
// serving from its (renewed) cache — sees the new column. Regression test
// for issue #23's stale-cache hazard.
func TestDescriptorLeaseDrainOnAlter(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ttl = 3 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 1, ttl)

	execSQL(t, ctx, sA, `CREATE TABLE items (id INT PRIMARY KEY, v INT)`)
	execSQL(t, ctx, sA, `INSERT INTO items VALUES (1, 10)`)

	// B leases the descriptor at version 1 and caches it.
	if res := execSQL(t, ctx, sB, `SELECT v FROM items WHERE id = 1`); len(res.Rows) != 1 {
		t.Fatalf("B's initial read: %+v", res.Rows)
	}

	start := time.Now()
	execSQL(t, ctx, sA, `ALTER TABLE items ADD COLUMN note TEXT`)
	elapsed := time.Since(start)

	// Genuine adoption happens within a renewal period (ttl/3); only the
	// anomalous fallback path takes 2×ttl. Well under that = B truly adopted.
	if elapsed >= 2*ttl {
		t.Fatalf("ALTER took %v — drain hit its timeout instead of B adopting", elapsed)
	}

	// B answers from its cache (its lease is still live) and must already
	// know the new column. A stale cached version 1 would error 42703 here.
	res := execSQL(t, ctx, sB, `SELECT note FROM items WHERE id = 1`)
	if len(res.Rows) != 1 || !res.Rows[0][0].Null {
		t.Fatalf("B sees %+v for the new column", res.Rows)
	}
	execSQL(t, ctx, sB, `UPDATE items SET note = 'adopted' WHERE id = 1`)
	res = execSQL(t, ctx, sB, `SELECT note FROM items WHERE id = 1`)
	if len(res.Rows) != 1 || res.Rows[0][0].S != "adopted" {
		t.Fatalf("B round-trip through new column: %+v", res.Rows)
	}
}

// TestOnlineCreateIndexUnderConcurrentWrites: the flagship for issues #22 and
// #23. Gateway B inserts continuously while gateway A runs the three-step
// online CREATE INDEX (publish write-only → drain → backfill+publish →
// drain). Afterwards the index must contain exactly one entry per row —
// backfill covered everything before its snapshot, B's write-only
// maintenance covered everything after — and both gateways plan with it.
func TestOnlineCreateIndexUnderConcurrentWrites(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const ttl = 2 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 1, ttl)

	execSQL(t, ctx, sA, `CREATE TABLE kv (id INT PRIMARY KEY, v INT)`)
	for i := 0; i < 20; i++ {
		execSQL(t, ctx, sA, fmt.Sprintf(`INSERT INTO kv VALUES (%d, %d)`, i, i%5))
	}
	// B leases the descriptor before the index exists.
	execSQL(t, ctx, sB, `SELECT id FROM kv WHERE id = 0`)

	// B inserts continuously on its own gateway for the whole build.
	var inserted atomic.Int64
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
			if _, serr := trySQL(ctx, sB, fmt.Sprintf(`INSERT INTO kv VALUES (%d, %d)`, 1000+i, i%5)); serr != nil {
				done <- fmt.Errorf("concurrent insert %d: [%s] %s", i, serr.Code, serr.Msg)
				return
			}
			inserted.Add(1)
		}
	}()
	// Let the writer get going before the build starts.
	for inserted.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}

	execSQL(t, ctx, sA, `CREATE INDEX by_v ON kv (v)`)

	close(stop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	total := 20 + int(inserted.Load())

	// The committed descriptor carries the index in the public state.
	desc := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "kv")
	idx, ok := desc.Index("by_v")
	if !ok {
		t.Fatal("index by_v missing from descriptor")
	}
	if !idx.Public() || idx.State != catalog.IndexStatePublic {
		t.Fatalf("index state %q, want public", idx.State)
	}

	// Exactly one index entry per row: nothing missed during the build.
	lo, hi := keys.TableIndexSpan(desc.ID, idx.ID)
	entries, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rows := execSQL(t, ctx, sA, `SELECT id FROM kv`); len(rows.Rows) != total {
		t.Fatalf("full scan sees %d rows, expected %d", len(rows.Rows), total)
	}
	if len(entries) != total {
		t.Fatalf("index has %d entries for %d rows — writes were missed during the online build", len(entries), total)
	}

	// Both gateways plan with the now-public index, and index reads agree
	// with a full scan.
	want := `scan of index "by_v" (1 column prefix) + primary key join`
	if p := explainPlan(t, ctx, sA, `SELECT id FROM kv WHERE v = 3`); p != want {
		t.Fatalf("gateway A plan: %q", p)
	}
	if p := explainPlan(t, ctx, sB, `SELECT id FROM kv WHERE v = 3`); p != want {
		t.Fatalf("gateway B plan: %q", p)
	}
	full := execSQL(t, ctx, sA, `SELECT id FROM kv WHERE v = 3`)
	viaIdx := execSQL(t, ctx, sB, `SELECT id FROM kv WHERE v = 3`)
	if len(full.Rows) == 0 || len(full.Rows) != len(viaIdx.Rows) {
		t.Fatalf("index scan %d rows vs %d", len(viaIdx.Rows), len(full.Rows))
	}
}

// TestOnlineCreateIndexAbandonOnFailure: a failed backfill (unique violation)
// removes the write-only index again, so writers stop maintaining it and the
// planner never sees it.
func TestOnlineCreateIndexAbandonOnFailure(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := leasedSession(t, tc, 0, 2*time.Second)

	execSQL(t, ctx, s, `CREATE TABLE dup (id INT PRIMARY KEY, e INT)`)
	execSQL(t, ctx, s, `INSERT INTO dup VALUES (1, 7), (2, 7)`)

	_, serr := trySQL(ctx, s, `CREATE UNIQUE INDEX by_e ON dup (e)`)
	if serr == nil || serr.Code != sql.CodeUniqueViolation {
		t.Fatalf("expected unique violation, got %+v", serr)
	}

	desc := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "dup")
	if _, ok := desc.Index("by_e"); ok {
		t.Fatal("abandoned index still in descriptor")
	}
	if p := explainPlan(t, ctx, s, `SELECT id FROM dup WHERE e = 7`); p != "full table scan" {
		t.Fatalf("plan after abandon: %q", p)
	}
	// The table still works, and a de-duplicated retry succeeds.
	execSQL(t, ctx, s, `UPDATE dup SET e = 8 WHERE id = 2`)
	execSQL(t, ctx, s, `CREATE UNIQUE INDEX by_e ON dup (e)`)
	if p := explainPlan(t, ctx, s, `SELECT id FROM dup WHERE e = 7`); p != `point lookup via unique index "by_e"` {
		t.Fatalf("plan after retry: %q", p)
	}
}

// TestCreateIndexRefusedInTxnBlock: like CREATE INDEX CONCURRENTLY, the
// online build is multi-transaction and cannot run inside BEGIN.
func TestCreateIndexRefusedInTxnBlock(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE tb (id INT PRIMARY KEY, v INT)`)
	execSQL(t, ctx, s, `BEGIN`)
	if _, serr := trySQL(ctx, s, `CREATE INDEX by_v ON tb (v)`); serr == nil || serr.Code != sql.CodeActiveTransaction {
		t.Fatalf("expected 25001, got %+v", serr)
	}
	execSQL(t, ctx, s, `ROLLBACK`)
	execSQL(t, ctx, s, `CREATE INDEX by_v ON tb (v)`)
}
