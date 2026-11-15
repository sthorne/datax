package testcluster

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// loEngineCluster is startLOCluster over injected engines, for the
// tests that restart nodes and look at raw storage.
func loEngineCluster(t *testing.T) (*TestCluster, []*storage.Engine) {
	t.Helper()
	sql.TestingLargeObjectHeartbeat = loTestGrace / 10
	t.Cleanup(func() { sql.TestingLargeObjectHeartbeat = 0 })
	return StartWithEngines(t, 3, withPG, func(c *server.Config) {
		c.LargeObjectSweepInterval = -1
		c.LargeObjectUploadGrace = loTestGrace
	})
}

// mvccVersions counts the stored versions (values and tombstones) under
// prefix on one engine.
func mvccVersions(t *testing.T, eng *storage.Engine, prefix keys.Key) int {
	t.Helper()
	lo := storage.EncodeMVCCKey(prefix, hlc.Timestamp{})
	hi := storage.EncodeMVCCKey(prefix.PrefixEnd(), hlc.Timestamp{})
	it := eng.NewIter(lo, hi)
	n := 0
	for ok := it.SeekGE(lo); ok; ok = it.Next() {
		n++
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestLargeObjectSplitRestartSnapshot: an object's pages are ordinary
// rows to the storage layer. A range boundary in the middle of an
// object changes nothing a reader or a writer can see; the replicas of
// every range holding pages agree; a follower that was down while
// objects were written and removed is brought back by a snapshot and
// serves them; and an unlinked object's pages leave the disk once the
// sweeper and then the garbage collector have run.
func TestLargeObjectSplitRestartSnapshot(t *testing.T) {
	tc, engines := loEngineCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	const page = sql.LargeObjectPageSize
	conn := diskSQL(t, ctx, tc.Nodes[0])
	defer func() { _ = conn.Close(ctx) }()
	pagesPrefix := keys.TableDataPrefix(catalog.LargeObjectTableID)

	data := randomBytes(t, 8*page+123)
	oid := loStore(t, ctx, conn, data, 300_000)

	// Split between the object's fourth and fifth pages.
	lo, hi := keys.TableDataSpan(catalog.LargeObjectTableID)
	rows, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
	if err != nil || len(rows) != 9 {
		t.Fatalf("page rows: %d, %v (want 9)", len(rows), err)
	}
	before, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tc.Nodes[0].DB().AdminSplit(ctx, rows[4].Key); err != nil {
		t.Fatalf("split inside the object: %v", err)
	}
	after, err := tc.ranges(ctx)
	if err != nil || len(after) != len(before)+1 {
		t.Fatalf("ranges after the split: %d, %v (want %d)", len(after), err, len(before)+1)
	}
	for i, n := range tc.Nodes {
		c := diskSQL(t, ctx, n)
		if got := loFetch(t, ctx, c, oid, 700_000); !bytes.Equal(got, data) {
			t.Fatalf("n%d: the object reads back differently across a range boundary", i+1)
		}
		_ = c.Close(ctx)
	}

	// A write across the boundary is one transaction over both ranges.
	patch := randomBytes(t, 2*page)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := loOf(tx).Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := obj.Seek(3*page+100, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := obj.Write(patch); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	copy(data[3*page+100:], patch)
	if got := loFetch(t, ctx, conn, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("a write across the range boundary did not land whole")
	}

	// A new upload streams across the boundary too (its pages sort after
	// the first object's, in the right-hand range, but the tables around
	// them are on both sides).
	second := randomBytes(t, 5*page+7)
	oid2 := loStore(t, ctx, conn, second, 128_000)

	checkConsistent := func(what string) {
		t.Helper()
		descs, err := tc.ranges(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range descs {
			leader := tc.LeaderIndex(d.RangeID)
			if leader < 0 {
				t.Fatalf("%s: r%d has no leader", what, d.RangeID)
			}
			mismatch, err := tc.Nodes[leader].CheckRangeConsistency(ctx, d.RangeID)
			if err != nil {
				t.Fatalf("%s: consistency check of r%d: %v", what, d.RangeID, err)
			}
			if mismatch {
				t.Fatalf("%s: the replicas of r%d disagree", what, d.RangeID)
			}
		}
	}
	checkConsistent("after the split")

	// A follower goes down; objects come and go; the logs are truncated
	// past it; it returns and is caught up by snapshot.
	snapshotsBefore := testutil.ToFloat64(metrics.CatchupSnapshots)
	down := (tc.LeaderIndex(1) + 1) % 3
	up := (down + 1) % 3
	stoppedAt := map[base.RangeID]uint64{}
	for _, d := range after {
		if rep, ok := tc.Nodes[down].Store().GetReplica(d.RangeID); ok {
			stoppedAt[d.RangeID] = rep.AppliedIndex()
		}
	}
	tc.StopNode(down)
	upConn := diskSQL(t, ctx, tc.Nodes[up])
	defer func() { _ = upConn.Close(ctx) }()
	third := randomBytes(t, 12*page+1)
	oid3 := loStore(t, ctx, upConn, third, 1<<20)
	if _, err := upConn.Exec(ctx, `SELECT lo_unlink($1)`, oid2); err != nil {
		t.Fatal(err)
	}
	if res, err := tc.Nodes[up].RunLargeObjectSweepOnce(ctx); err != nil || res.Contents != 1 || res.Pages != 6 {
		t.Fatalf("sweep with a node down: %+v, %v", res, err)
	}
	// Enough small writes into both halves of the first object that the
	// logs of the ranges holding pages run well past where the stopped
	// node left them (truncation keeps a tail, and waits for a few
	// hundred entries before it bothers).
	for i := 0; i < 250; i++ {
		off := i
		if i%2 == 1 {
			off = 6*page + i
		}
		b := []byte{byte(i), byte(i >> 8)}
		if _, err := upConn.Exec(ctx, `SELECT lo_put($1, $2, $3)`, oid, off, b); err != nil {
			t.Fatalf("lo_put %d: %v", i, err)
		}
		copy(data[off:], b)
	}
	// Truncate every log that moved while the node was away to beyond
	// where the node stopped, so the log can no longer bring it back.
	for _, d := range after {
		at, moved := stoppedAt[d.RangeID], false
		deadline := time.Now().Add(30 * time.Second)
		for {
			leader := tc.LeaderIndex(d.RangeID)
			if leader < 0 || leader == down {
				t.Fatalf("r%d has no leader among the running nodes", d.RangeID)
			}
			store := tc.Nodes[leader].Store()
			rep, ok := store.GetReplica(d.RangeID)
			if !ok {
				t.Fatalf("r%d: its leader has no replica", d.RangeID)
			}
			if moved = rep.AppliedIndex() > at+400; !moved || rep.TruncatedIndex() > at {
				break
			}
			store.RunLogTruncationOnce(ctx)
			if time.Now().After(deadline) {
				t.Fatalf("r%d: truncation stuck at %d, the stopped node was at %d", d.RangeID, rep.TruncatedIndex(), at)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	tc.RestartNode(down, engines[down], withPG, func(c *server.Config) {
		c.LargeObjectSweepInterval = -1
		c.LargeObjectUploadGrace = loTestGrace
	})
	deadline := time.Now().Add(90 * time.Second)
	for {
		sums, counts := dataChecksums(t, engines, pagesPrefix)
		if sums[0] == sums[1] && sums[1] == sums[2] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the returning follower never converged on the page table: %v versions per store", counts)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if testutil.ToFloat64(metrics.CatchupSnapshots) == snapshotsBefore {
		t.Fatal("the returning follower caught up without a snapshot: the test did not exercise one")
	}
	back := diskSQL(t, ctx, tc.Nodes[down])
	defer func() { _ = back.Close(ctx) }()
	if got := loFetch(t, ctx, back, oid3, 1<<20); !bytes.Equal(got, third) {
		t.Fatal("the object written while the node was down reads back differently through it")
	}
	if got := loFetch(t, ctx, back, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("the first object reads back differently through the returning node")
	}
	if _, err := back.Exec(ctx, `SELECT lo_open($1, 262144)`, oid2); sqlState(err) != "42704" {
		t.Fatalf("the unlinked object through the returning node: %v", err)
	}
	checkConsistent("after the follower returned")

	// Storage: the sweeper's deletes are tombstones over the pages, and
	// the garbage collector removes both once they are past the window.
	// What must remain is exactly the live objects' pages, one version
	// each (the overwritten pages' old versions go the same way).
	if _, err := upConn.Exec(ctx, `SELECT lo_unlink($1)`, oid3); err != nil {
		t.Fatal(err)
	}
	if res, err := tc.Nodes[up].RunLargeObjectSweepOnce(ctx); err != nil || res.Contents != 1 || res.Pages != 13 {
		t.Fatalf("sweep: %+v, %v", res, err)
	}
	livePages := 9
	deadline = time.Now().Add(60 * time.Second)
	for {
		time.Sleep(50 * time.Millisecond)
		for _, n := range tc.Nodes {
			n.Store().RunGCOnce(ctx, time.Millisecond)
		}
		done := true
		var counts []int
		for _, eng := range engines {
			c := mvccVersions(t, eng, pagesPrefix)
			counts = append(counts, c)
			done = done && c == livePages
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("page versions per store after sweep and GC: %v (want %d each)", counts, livePages)
		}
	}
	if got := loFetch(t, ctx, upConn, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("the surviving object changed under the garbage collector")
	}
}

// TestLargeObjectBackupRestore is the issue's third acceptance item:
// a full backup and an incremental on it, plain and sealed, carry large
// objects — what was committed at the backup's time and nothing else.
func TestLargeObjectBackupRestore(t *testing.T) {
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	const page = sql.LargeObjectPageSize
	conn := pgConn(t, ctx, tc, 0)

	kept := randomBytes(t, 6*page+11)
	keptOID := loStore(t, ctx, conn, kept, 1<<20)
	doomed := randomBytes(t, 2*page)
	doomedOID := loStore(t, ctx, conn, doomed, 1<<20)
	// Unlinked but not yet swept: its pages and its pending row ride
	// the backup, and the restored cluster's sweeper finishes the job.
	gone := loStore(t, ctx, conn, randomBytes(t, 3*page), 1<<20)
	if _, err := conn.Exec(ctx, `SELECT lo_unlink($1)`, gone); err != nil {
		t.Fatal(err)
	}
	// An upload in flight when the backup is taken: not in the backup as
	// an object, whatever became of it afterwards.
	flightConn := pgConn(t, ctx, tc, 1)
	flight, err := flightConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	flightOID, err := loOf(flight).Create(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	fobj, err := loOf(flight).Open(ctx, flightOID, pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatal(err)
	}
	inFlight := randomBytes(t, 3*page)
	if _, err := fobj.Write(inFlight[:2*page+5]); err != nil {
		t.Fatal(err)
	}

	dirFull, dirSealed := t.TempDir(), t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirFull, "", false, false); err != nil {
		t.Fatalf("full backup: %v", err)
	}
	key := newBackupKey(t)
	if _, err := tc.Nodes[0].RunBackupWith(ctx, server.BackupOptions{Dest: dirSealed, Key: key}); err != nil {
		t.Fatalf("sealed backup: %v", err)
	}

	// After the full backup: the upload commits, one object changes, one
	// is unlinked, one is new.
	if _, err := fobj.Write(inFlight[2*page+5:]); err != nil {
		t.Fatal(err)
	}
	if err := flight.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	keptAfter := append([]byte(nil), kept...)
	patch := randomBytes(t, page+500)
	if _, err := conn.Exec(ctx, `SELECT lo_put($1, $2, $3)`, keptOID, 2*page-250, patch); err != nil {
		t.Fatal(err)
	}
	copy(keptAfter[2*page-250:], patch)
	if _, err := conn.Exec(ctx, `SELECT lo_unlink($1)`, doomedOID); err != nil {
		t.Fatal(err)
	}
	fresh := randomBytes(t, page/3)
	freshOID := loStore(t, ctx, conn, fresh, 1<<20)
	dirInc := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirInc, dirFull, false, false); err != nil {
		t.Fatalf("incremental backup: %v", err)
	}

	expect := func(what string, c *pgx.Conn, oid uint32, want []byte) {
		t.Helper()
		if want == nil {
			if _, err := c.Exec(ctx, `SELECT lo_open($1, 262144)`, oid); sqlState(err) != "42704" {
				t.Fatalf("%s: object %d should not exist: %v", what, oid, err)
			}
			return
		}
		if got := loFetch(t, ctx, c, oid, 1<<20); !bytes.Equal(got, want) {
			t.Fatalf("%s: object %d reads back %d bytes that differ from the %d backed up", what, oid, len(got), len(want))
		}
	}
	// settle runs the restored cluster's sweeper until what is left is
	// exactly the live objects' pages: the backup's leftovers (the
	// unlinked object, the upload that was in flight) are reclaimed
	// there as they would have been at the source.
	settle := func(what string, tc *TestCluster, c *pgx.Conn, objects, pages int64) {
		t.Helper()
		deadline := time.Now().Add(loTestGrace + 60*time.Second)
		for {
			if _, err := tc.Nodes[0].RunLargeObjectSweepOnce(ctx); err != nil {
				t.Fatalf("%s: sweep: %v", what, err)
			}
			o, p, pend := loCounts(t, ctx, c)
			if o == objects && p == pages && pend == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %d objects, %d pages, %d pending; want %d, %d, 0", what, o, p, pend, objects, pages)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}

	// Full + incremental, plain.
	tc2 := startLOCluster(t)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dirFull, dirInc}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	c2 := pgConn(t, ctx, tc2, 2)
	expect("full+incremental", c2, keptOID, keptAfter)
	expect("full+incremental", c2, doomedOID, nil)
	expect("full+incremental", c2, gone, nil)
	expect("full+incremental", c2, flightOID, inFlight)
	expect("full+incremental", c2, freshOID, fresh)
	settle("full+incremental", tc2, c2, 3, 7+3+1)
	expect("full+incremental, after the sweep", c2, keptOID, keptAfter)
	// The restored cluster goes on working: a new object, and a restored
	// one changed.
	more := randomBytes(t, page+1)
	expect("full+incremental, new object", c2, loStore(t, ctx, c2, more, 1<<20), more)
	if _, err := c2.Exec(ctx, `SELECT lo_put($1, 0, '\x00010203')`, freshOID); err != nil {
		t.Fatal(err)
	}

	// The sealed full backup alone: the state at its time.
	tc3 := startLOCluster(t)
	if _, err := tc3.Nodes[0].RunRestoreWith(ctx, []string{dirSealed}, [][]byte{key}); err != nil {
		t.Fatalf("sealed restore: %v", err)
	}
	c3 := pgConn(t, ctx, tc3, 1)
	expect("sealed full", c3, keptOID, kept)
	expect("sealed full", c3, doomedOID, doomed)
	expect("sealed full", c3, gone, nil)
	expect("sealed full", c3, flightOID, nil)
	expect("sealed full", c3, freshOID, nil)
	settle("sealed full", tc3, c3, 2, 7+2)
	expect("sealed full, after the sweep", c3, keptOID, kept)
	expect("sealed full, after the sweep", c3, doomedOID, doomed)
}

// TestLargeObjectBackgroundSweeper: with the node's own loop running,
// as it does outside the tests, the tables are there before anyone
// stores an object, and an unlinked object's pages go without anyone
// asking.
func TestLargeObjectBackgroundSweeper(t *testing.T) {
	sql.TestingLargeObjectHeartbeat = loTestGrace / 10
	t.Cleanup(func() { sql.TestingLargeObjectHeartbeat = 0 })
	tc := StartWithOptions(t, 3, func(c *server.Config) {
		c.LargeObjectSweepInterval = 100 * time.Millisecond
		c.LargeObjectUploadGrace = loTestGrace
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 1)
	deadline := time.Now().Add(60 * time.Second)
	for {
		var n int64
		err := conn.QueryRow(ctx, `SELECT count(*) FROM datax_largeobject_metadata`).Scan(&n)
		if err == nil {
			break
		}
		if sqlState(err) != "42P01" || time.Now().After(deadline) {
			t.Fatalf("the nodes never created the large-object tables: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	oid := loStore(t, ctx, conn, randomBytes(t, 3*sql.LargeObjectPageSize), 1<<20)
	if _, err := conn.Exec(ctx, `SELECT lo_unlink($1)`, oid); err != nil {
		t.Fatal(err)
	}
	// And an upload that is rolled back.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT lo_from_bytea(0, $1)`, randomBytes(t, 2*sql.LargeObjectPageSize)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(60 * time.Second)
	for {
		objects, pages, pending := loCounts(t, ctx, conn)
		if objects == 0 && pages == 0 && pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the background sweeper left %d objects, %d pages, %d pending", objects, pages, pending)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
