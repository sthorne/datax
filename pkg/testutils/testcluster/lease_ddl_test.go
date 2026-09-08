package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
)

// unindexedRows names the primary keys that have no entry in idx, so a
// count mismatch reports which rows were missed rather than only how
// many.
func unindexedRows(t *testing.T, desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, rows, entries []kvpb.KeyValue) []string {
	t.Helper()
	indexed := map[string]bool{}
	for _, e := range entries {
		pk, err := rowenc.IndexEntryPrimaryKey(desc, idx, e.Key, e.Value)
		if err != nil {
			t.Fatalf("decoding index entry %s: %v", e.Key, err)
		}
		indexed[string(pk)] = true
	}
	var missing []string
	for _, kv := range rows {
		if indexed[string(kv.Key)] {
			continue
		}
		pkVals, err := rowenc.DecodePK(desc, kv.Key)
		if err != nil || len(pkVals) == 0 {
			missing = append(missing, kv.Key.String())
			continue
		}
		missing = append(missing, pkVals[0].Text())
	}
	return missing
}

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
	// Read through a transaction: the writer's last commit may still be
	// resolving its intents asynchronously (parallel commits finalize after
	// control returns), and only the transactional read path pushes them.
	lo, hi := keys.TableIndexSpan(desc.ID, idx.ID)
	plo, phi := rowenc.PrimarySpanFor(desc)
	reader := tc.Nodes[0].DB().NewTxn("index-count")
	entries, err := reader.Scan(ctx, lo, hi, 0)
	var rowKVs []kvpb.KeyValue
	if err == nil {
		rowKVs, err = reader.Scan(ctx, plo, phi, 0)
	}
	_ = reader.Rollback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rows := execSQL(t, ctx, sA, `SELECT id FROM kv`); len(rows.Rows) != total {
		t.Fatalf("full scan sees %d rows, expected %d", len(rows.Rows), total)
	}
	if len(entries) != total {
		// Name the rows that have no entry. Which they are is the whole
		// diagnosis: an id below 1000 was present before the build began
		// and the backfill missed it; one at or above 1000 was written by
		// the concurrent gateway, and that gateway failed to maintain the
		// index for it.
		t.Fatalf("index has %d entries for %d rows — no index entry for id %v; writes were missed during the online build",
			len(entries), total, unindexedRows(t, desc, &idx, rowKVs, entries))
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

// TestLeaseClaimsTheVersionItRead: a gateway whose lease on a table has
// lapsed takes a new one while a schema change commits. The lease record
// used to be written outside the transaction that read the descriptor, so
// the gateway could read version 1, the schema change commit version 2
// and drain (the lapsed lease was nothing to wait for), and the gateway
// then record a fresh lease at version 1 and serve it from its cache for
// a whole TTL. The lease is now taken in the transaction that read the
// descriptor: a write over a changed descriptor cannot commit, the
// transaction restarts, and the lease claims the version it read.
func TestLeaseClaimsTheVersionItRead(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ttl = 500 * time.Millisecond
	sA := leasedSession(t, tc, 0, ttl)
	sB, catB := leasedSessionWithAccessor(t, tc, 1, ttl)
	execSQL(t, ctx, sA, `CREATE TABLE items (id INT PRIMARY KEY, v INT)`)
	execSQL(t, ctx, sA, `INSERT INTO items VALUES (1, 10)`)
	execSQL(t, ctx, sB, `SELECT v FROM items WHERE id = 1`) // B leases version 1

	// B's renewal stops and its lease lapses.
	catB.TestingPauseRenewal(true)
	time.Sleep(ttl + 200*time.Millisecond)

	// B's next statement misses its lapsed cache entry, reads the
	// descriptor (version 1) and parks in its lease transaction before
	// the write.
	var once sync.Once
	entered, hold := make(chan struct{}), make(chan struct{})
	catalog.SetTestingBeforeLeaseWrite(func(a *catalog.Accessor, _ string) {
		if a == catB {
			once.Do(func() { close(entered) })
			<-hold
		}
	})
	defer catalog.SetTestingBeforeLeaseWrite(nil)
	done := make(chan *sql.Error, 1)
	go func() {
		_, serr := trySQL(ctx, sB, `SELECT v FROM items WHERE id = 1`)
		done <- serr
	}()
	<-entered

	// A's schema change commits version 2 and drains at once: B holds no
	// live lease to wait for.
	execSQL(t, ctx, sA, `ALTER TABLE items ADD COLUMN note TEXT`)
	close(hold)
	if serr := <-done; serr != nil {
		t.Fatalf("B's read: [%s] %s", serr.Code, serr.Msg)
	}

	// B's lease, and so its cache, carry version 2: the new column is
	// there. A lease at version 1 would answer 42703 for a whole TTL.
	if _, serr := trySQL(ctx, sB, `SELECT note FROM items WHERE id = 1`); serr != nil {
		t.Fatalf("B serves a superseded version after taking a fresh lease: [%s] %s", serr.Code, serr.Msg)
	}
}

// TestDrainWaitsOutADescriptorHandedToAStatement (issue #185): a gateway
// adopting a new descriptor version does not mean it has stopped using
// the old one.
//
// A statement is pinned to the descriptor it planned against, and when
// that came from the lease cache it was never read inside the statement's
// own transaction — so nothing but its commit deadline, the cache entry's
// expiration, stops it committing later. The renewal loop meanwhile
// publishes the new version within a third of a TTL, which used to end
// the drain. CREATE INDEX would then take its backfill boundary while a
// statement planned under the pre-index descriptor could still commit
// above it: a row in the table and no entry in the new index.
//
// The drain must therefore outlast the entry that was handed out, not
// merely the version that was published.
func TestDrainWaitsOutADescriptorHandedToAStatement(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ttl = 2 * time.Second
	sA, catA := leasedSessionWithAccessor(t, tc, 0, ttl)
	sB, catB := leasedSessionWithAccessor(t, tc, 0, ttl)

	execSQL(t, ctx, sA, `CREATE TABLE kv (id INT PRIMARY KEY, v INT)`)
	execSQL(t, ctx, sA, `INSERT INTO kv VALUES (1, 1), (2, 2)`)
	// Both gateways read the table twice: the first statement fills each
	// cache, the second is served from it — which is the case with no
	// protection but the deadline, and so the case the drain must wait
	// for.
	for i := 0; i < 2; i++ {
		execSQL(t, ctx, sA, `SELECT id FROM kv WHERE id = 1`)
		execSQL(t, ctx, sB, `SELECT id FROM kv WHERE id = 1`)
	}

	// The latest moment a statement holding the current descriptor could
	// still commit: no drain may finish before it. The lease records are
	// what the drain reads, so they are what is sampled — with renewal
	// held off around the sample, so the record each gateway published
	// is the entry its last statement was handed and not a fresh one a
	// renewal put in its place, which no statement holds and the drain
	// rightly does not wait for (issue #234).
	catA.TestingPauseRenewal(true)
	catB.TestingPauseRenewal(true)
	execSQL(t, ctx, sA, `SELECT id FROM kv WHERE id = 1`)
	execSQL(t, ctx, sB, `SELECT id FROM kv WHERE id = 1`)
	desc := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "kv")
	records := leaseRecords(t, ctx, tc, desc.ID)
	mustOutlast := records.maxExpiration()
	if mustOutlast == 0 {
		t.Fatal("no lease records for the table; the gateways are not leasing")
	}
	catA.TestingPauseRenewal(false)
	catB.TestingPauseRenewal(false)

	execSQL(t, ctx, sA, `CREATE INDEX by_v ON kv (v)`)

	if now := tc.Nodes[0].Clock().Now().WallTime; now < mustOutlast {
		t.Fatalf("CREATE INDEX drained %s before the descriptor it superseded stopped being usable: "+
			"a statement holding that descriptor can still commit, and its row would reach no index\n"+
			"records sampled before the build, relative to the drain's end: %s\nrecords now: %s",
			time.Duration(mustOutlast-now), records.describe(now), leaseRecords(t, ctx, tc, desc.ID).describe(now))
	}
}

// TestDrainOutlastsAnEntryARenewalReplaced (issue #234): the entry a
// statement was handed need not be the entry current when the schema
// change arrives. A renewal in between replaces it with a fresh one at
// the same version, which no statement has taken — and a drain that
// only looked at the entry being replaced at adoption had nothing to
// wait for, while the statement could still commit under the old
// descriptor for as long as its own entry lived. B's renewals are
// driven by hand so the renewal lands exactly between the statement and
// the schema change, and B adopts each version the moment it is
// published — the fastest B could be, and so the earliest the drain
// could end if it waited for nothing else.
func TestDrainOutlastsAnEntryARenewalReplaced(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ttl = 3 * time.Second
	sA, catA := leasedSessionWithAccessor(t, tc, 0, ttl)
	sB, catB := leasedSessionWithAccessor(t, tc, 0, ttl)
	catA.TestingPauseRenewal(true)
	catB.TestingPauseRenewal(true)

	execSQL(t, ctx, sA, `CREATE TABLE kv (id INT PRIMARY KEY, v INT)`)
	execSQL(t, ctx, sA, `INSERT INTO kv VALUES (1, 1), (2, 2)`)
	execSQL(t, ctx, sA, `SELECT id FROM kv WHERE id = 1`)
	execSQL(t, ctx, sB, `SELECT id FROM kv WHERE id = 1`)

	// B's transaction plans a statement against B's current entry and is
	// pinned to its expiration. A read, deliberately: a write would lay
	// an intent the index backfill has to wait out, and this transaction
	// stays open until the build is over — the property under test is
	// when the drain ends, and the intent path is #110's.
	execSQL(t, ctx, sB, `BEGIN`)
	execSQL(t, ctx, sB, `SELECT id FROM kv WHERE id = 2`)
	desc := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "kv")
	pinned := leaseRecords(t, ctx, tc, desc.ID).maxExpiration()
	if pinned == 0 {
		t.Fatal("no lease records for the table; the gateways are not leasing")
	}

	// A renewal on B, same version: the entry the statement holds is
	// replaced by one nobody has taken.
	if err := catB.TestingRenewNow(ctx); err != nil {
		t.Fatal(err)
	}
	if after := leaseRecords(t, ctx, tc, desc.ID).maxExpiration(); after <= pinned {
		t.Fatalf("the renewal did not move B's lease (%d, was %d)", after, pinned)
	}

	// The schema change, with B adopting every version as soon as it is
	// published.
	built := make(chan *sql.Error, 1)
	go func() {
		_, serr := trySQL(ctx, sA, `CREATE INDEX by_v ON kv (v)`)
		built <- serr
	}()
	var serr *sql.Error
adopting:
	for {
		select {
		case serr = <-built:
			break adopting
		case <-time.After(50 * time.Millisecond):
			_ = catB.TestingRenewNow(ctx)
		}
	}
	if serr != nil {
		t.Fatalf("CREATE INDEX: [%s] %s", serr.Code, serr.Msg)
	}
	if now := tc.Nodes[0].Clock().Now().WallTime; now < pinned {
		t.Fatalf("CREATE INDEX drained %s before the entry B's open transaction holds expires: "+
			"a renewal replaced that entry, and the drain waited for nothing on B",
			time.Duration(pinned-now))
	}

	// The entry B's transaction is pinned to has expired with the drain,
	// so the transaction cannot commit: that is the deadline of #110
	// doing its half of the work, and the drain has now done its half by
	// not ending before the deadline could.
	_, ierr := trySQL(ctx, sB, `INSERT INTO kv VALUES (99, 9)`)
	_, cerr := trySQL(ctx, sB, `COMMIT`)
	if ierr == nil && cerr == nil {
		t.Fatal("B committed a transaction pinned to a descriptor entry that expired before the build ended")
	}
}

// leaseRecord is a gateway's lease record on a descriptor, read as data:
// these tests are about the contract the drain honours, not the struct
// that carries it.
type leaseRecord struct {
	Version         uint64 `json:"version"`
	Expiration      int64  `json:"expiration"`
	PriorExpiration int64  `json:"prior_expiration"`
}

type leaseRecordList []leaseRecord

// leaseRecords reads the live lease records on descID.
func leaseRecords(t *testing.T, ctx context.Context, tc *TestCluster, descID uint64) leaseRecordList {
	t.Helper()
	lo, hi := keys.DescLeaseSpan(descID)
	rows, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out leaseRecordList
	for _, kv := range rows {
		var l leaseRecord
		if json.Unmarshal(kv.Value, &l) != nil {
			continue
		}
		out = append(out, l)
	}
	return out
}

// maxExpiration is the latest expiration among the records — the last
// moment any gateway's current descriptor could still back a committing
// statement.
func (rs leaseRecordList) maxExpiration() int64 {
	var max int64
	for _, l := range rs {
		if l.Expiration > max {
			max = l.Expiration
		}
	}
	return max
}

// describe renders the records with their times relative to now.
func (rs leaseRecordList) describe(now int64) string {
	var parts []string
	for _, l := range rs {
		parts = append(parts, fmt.Sprintf("{v%d expires %+v prior %+v}", l.Version,
			time.Duration(l.Expiration-now), time.Duration(l.PriorExpiration-now)))
	}
	return strings.Join(parts, " ")
}
