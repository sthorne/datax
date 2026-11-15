package testcluster

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestDropTableReclaimsRows (issue #313): DROP TABLE used to leave the
// table's rows on disk for good. Now they stay for the table's history
// window — AS OF SYSTEM TIME below the drop still reads them — and once
// it has passed the reclaim job clears the span, so nothing of the
// table is left; a read below the drop is then refused, never answered
// short. The cleared range merges into its neighbour only once the
// neighbour's own window has passed the reclaim's threshold.
func TestDropTableReclaimsRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := StartWithOptions(t, 1, func(cfg *server.Config) { cfg.DroppedTableReclaimInterval = -1 })
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE gone (id INT8 PRIMARY KEY, v TEXT) WITH (gc_ttl = '1s')`)
	execSQL(t, ctx, s, `CREATE TABLE kept (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s, `INSERT INTO gone VALUES (1, 'marker-313'), (2, 'b'), (3, 'c')`)
	execSQL(t, ctx, s, `INSERT INTO kept VALUES (1)`)
	var id uint64
	if err := n.DB().RunTxn(ctx, "lookup", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.NewAccessor().LookupIn(ctx, txn, catalog.DefaultDatabase, "gone")
		if err == nil {
			id = d.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	start, end := keys.TableDataSpan(id)
	rows := func() int {
		kvs, err := n.DB().Scan(ctx, start, end, 0)
		if err != nil {
			t.Fatal(err)
		}
		return len(kvs)
	}
	before := strconv.FormatInt(n.Clock().Now().WallTime, 10)
	execSQL(t, ctx, s, `DROP TABLE gone`)

	// Inside the window: the rows stay, and a read below the drop finds them.
	if got := n.RunDroppedTableReclaimOnce(ctx); got != 0 {
		t.Fatalf("reclaimed %d table(s) inside the window", got)
	}
	if got := rows(); got != 3 {
		t.Fatalf("%d rows on disk right after the drop, want 3", got)
	}
	if res := execSQL(t, ctx, s, `SELECT count(*) FROM gone AS OF SYSTEM TIME '`+before+`'`); res.Rows[0][0].I != 3 {
		t.Fatalf("read below the drop: %+v", res.Rows)
	}
	if raw, err := n.DB().Get(ctx, keys.DroppedTableKey(id)); err != nil || raw == nil {
		t.Fatalf("no dropped-table record: %v", err)
	}

	// Past it (1s and the job's slack): the span is cleared.
	deadline := time.Now().Add(30 * time.Second)
	for n.RunDroppedTableReclaimOnce(ctx) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the dropped table was never reclaimed")
		}
		time.Sleep(500 * time.Millisecond)
	}
	if got := rows(); got != 0 {
		t.Fatalf("%d rows left after the reclaim", got)
	}
	if raw, err := n.DB().Get(ctx, keys.DroppedTableKey(id)); err != nil || raw != nil {
		t.Fatalf("the record outlived the reclaim: %q %v", raw, err)
	}
	if _, serr := trySQL(ctx, s, `SELECT count(*) FROM gone AS OF SYSTEM TIME '`+before+`'`); serr == nil {
		t.Fatal("a read below the drop was answered after the rows were reclaimed")
	}
	// Nothing of the table is left in the files: no live key, version or
	// tombstone on any replica.
	rep, err := n.PurgeSpan(ctx, start, end)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rep.Replicas {
		if r.Error != "" || r.LiveKeys != 0 || r.Garbage != 0 || r.Tombstones != 0 {
			t.Fatalf("after the reclaim: %+v", r)
		}
	}
	if got := execSQL(t, ctx, s, `SELECT count(*) FROM kept`).Rows[0][0].I; got != 1 {
		t.Fatalf("the neighbour lost rows: %d", got)
	}

	// The emptied range and its neighbours: kept apart while the
	// neighbours' window (here 3s) has not passed the reclaim, merged
	// after.
	const window = 3 * time.Second
	st := n.Store()
	gcAndMerge := func() {
		st.RunGCOnce(ctx, window)
		st.RunRangeMergeOnce(ctx)
	}
	dropped := func() kvpb.RangeDescriptor {
		var d kvpb.RangeDescriptor
		st.VisitReplicas(func(r *kvserver.Replica) bool {
			if desc := r.Desc(); desc.ContainsKey(start) {
				d = desc
				return false
			}
			return true
		})
		return d
	}
	gcAndMerge()
	if d := dropped(); !keys.Key(d.StartKey).Equal(start) || !keys.Key(d.EndKey).Equal(end) {
		t.Fatalf("the cleared range merged inside its neighbours' window: %s", &d)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		gcAndMerge()
		if d := dropped(); !keys.Key(d.StartKey).Equal(start) || !keys.Key(d.EndKey).Equal(end) {
			break
		}
		if time.Now().After(deadline) {
			d := dropped()
			t.Fatalf("the cleared range never merged: %s", &d)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if got := execSQL(t, ctx, s, `SELECT count(*) FROM kept`).Rows[0][0].I; got != 1 {
		t.Fatalf("the neighbour after the merge: %d", got)
	}
}

// TestRestoreOverADroppedTable (issue #313): a restore of everything
// needs a cluster with no tables, but one that has dropped tables still
// holds their rows, under ids a backup's tables can carry — a fresh
// cluster hands out the same first ids as any other. Those rows used to
// mix into the restored table; and with dropped tables reclaimed, the
// record left behind would have the reclaim job delete the restored rows
// once its window ran out. The restore clears both first.
func TestRestoreOverADroppedTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	src := Start(t, 1)
	s := sql.NewSession(src.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE x (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO x VALUES (1, 'backed up')`)
	dir := t.TempDir()
	sum, err := src.Nodes[0].RunBackup(ctx, dir, "", false, false)
	if err != nil {
		t.Fatal(err)
	}

	dst := StartWithOptions(t, 1, func(cfg *server.Config) { cfg.DroppedTableReclaimInterval = -1 })
	n := dst.Nodes[0]
	s2 := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s2)
	execSQL(t, ctx, s2, `CREATE TABLE y (id INT8 PRIMARY KEY, v TEXT) WITH (gc_ttl = '1s')`)
	execSQL(t, ctx, s2, `INSERT INTO y VALUES (1, 'dropped'), (2, 'dropped')`)
	execSQL(t, ctx, s2, `DROP TABLE y`)
	id := tableSummary(t, sum, "x").ID
	if raw, err := n.DB().Get(ctx, keys.DroppedTableKey(id)); err != nil || raw == nil {
		t.Fatalf("the dropped table does not share the backed-up table's id %d (%v): the test sets up nothing", id, err)
	}
	rsum, err := n.RunRestore(ctx, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tableSummary(t, rsum, "x").SHA256, tableSummary(t, sum, "x").SHA256; got != want {
		t.Fatalf("restored checksum %s, backup's %s", got, want)
	}
	requireSameRows(t, "restored rows", rowsText(t, ctx, s2, `SELECT id, v FROM x ORDER BY id`), []string{"1|backed up"})
	if raw, err := n.DB().Get(ctx, keys.DroppedTableKey(id)); err != nil || raw != nil {
		t.Fatalf("the dropped table's record outlived the restore: %q %v", raw, err)
	}
	time.Sleep(6500 * time.Millisecond) // past y's window and the job's slack
	n.RunDroppedTableReclaimOnce(ctx)
	requireSameRows(t, "restored rows after a reclaim pass", rowsText(t, ctx, s2, `SELECT id, v FROM x ORDER BY id`), []string{"1|backed up"})
}
