package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestSweepReclaimsTablesDroppedBeforeTheirRecord (issue #316): a table
// dropped by a binary from before #313 left its rows and nothing naming
// them — the descriptor and name gone, no dropped-table record. The
// one-time sweep finds such a span, records it as dropped when it first
// sees it, and the reclaim job clears it once the GC TTL has passed from
// then; a live table, the rows of a table restore in flight, and a span
// in the reserved range are never touched, and the sweep, run again,
// records nothing twice.
func TestSweepReclaimsTablesDroppedBeforeTheirRecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	const gcTTL = time.Second
	tc := StartWithOptions(t, 1, func(cfg *server.Config) {
		cfg.DroppedTableReclaimInterval = -1 // passes run by hand
		cfg.GCTTL = gcTTL
	})
	n := tc.Nodes[0]
	db := n.DB()
	s := sql.NewSession(db, catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	for _, q := range []string{
		`CREATE TABLE live_t (id INT8 PRIMARY KEY, v TEXT)`,
		`CREATE TABLE old_t (id INT8 PRIMARY KEY, v TEXT)`,
		`INSERT INTO live_t VALUES (1, 'a'), (2, 'b')`,
		`INSERT INTO old_t VALUES (1, 'a'), (2, 'b'), (3, 'c')`,
	} {
		execSQL(t, ctx, s, q)
	}
	liveID, oldID := tableID(t, ctx, n, "live_t"), tableID(t, ctx, n, "old_t")

	// What DROP TABLE did before #313: the descriptor and the name go,
	// the rows stay, nothing records them.
	if err := db.RunTxn(ctx, "old-drop", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.NewAccessor().LookupIn(ctx, txn, catalog.DefaultDatabase, "old_t")
		if err != nil {
			return err
		}
		if err := txn.Delete(ctx, keys.TableDescKey(d.ID)); err != nil {
			return err
		}
		return txn.Delete(ctx, keys.TableNamespaceKey(d.DatabaseID, d.Name))
	}); err != nil {
		t.Fatal(err)
	}

	// A table restore in flight: rows under a fresh id no descriptor
	// names yet, and the restore's record naming it (a node that is not
	// this one, heartbeating: the restore janitor leaves it alone).
	restoreID, err := db.Increment(ctx, keys.DescIDGenKey(), 1)
	if err != nil {
		t.Fatal(err)
	}
	rid := uint64(restoreID)
	if err := db.Put(ctx, append(keys.TableDataPrefix(rid), 0x89, 0x01), []byte("restoring")); err != nil {
		t.Fatal(err)
	}
	prog := fmt.Sprintf(`{"op_id":"sweep-test","node_id":99,"table_ids":[%d],"heartbeat":%d}`, rid, n.Clock().Now().WallTime)
	if err := db.Put(ctx, keys.RestoreProgressKey("sweep-test"), []byte(prog)); err != nil {
		t.Fatal(err)
	}
	// A key in the reserved range, where the cluster's own tables live
	// as spans before their descriptors do.
	reserved := append(keys.TableDataPrefix(catalog.MetricsTableID+100), 0x89, 0x01)
	if err := db.Put(ctx, reserved, []byte("reserved")); err != nil {
		t.Fatal(err)
	}

	rows := func(id uint64) int {
		start, end := keys.TableDataSpan(id)
		kvs, err := db.Scan(ctx, start, end, 0)
		if err != nil {
			t.Fatal(err)
		}
		return len(kvs)
	}
	record := func(id uint64) *catalog.DroppedTable {
		raw, err := db.Get(ctx, keys.DroppedTableKey(id))
		if err != nil {
			t.Fatal(err)
		}
		if raw == nil {
			return nil
		}
		rec, err := catalog.DecodeDroppedTable(raw)
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	sweepState := func() map[string]any {
		raw, err := db.Get(ctx, keys.DroppedTableSweepKey())
		if err != nil || raw == nil {
			t.Fatalf("sweep record: %v %v", raw, err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}

	before := n.Clock().Now().WallTime
	for i := 0; !n.RunDroppedTableSweepOnce(ctx); i++ {
		if i > 50 {
			t.Fatal("the sweep never finished")
		}
	}
	rec := record(oldID)
	if rec == nil || !rec.Swept || rec.DroppedAt < before || rec.WindowSeconds != 0 {
		t.Fatalf("the old table's record: %+v", rec)
	}
	for what, id := range map[string]uint64{"the live table": liveID, "the restoring table": rid, "the reserved span": catalog.MetricsTableID + 100} {
		if r := record(id); r != nil {
			t.Fatalf("%s was recorded as dropped: %+v", what, r)
		}
	}
	if st := sweepState(); st["done"] != true || st["found"] != float64(1) {
		t.Fatalf("sweep record: %v", st)
	}

	// Run again from the start, as a sweep cut short would be resumed
	// from wherever its record says: nothing is recorded twice, and the
	// record keeps the time the sweep first saw the rows.
	raw := sweepState()
	raw["next"], raw["done"] = 1, false
	again, _ := json.Marshal(raw)
	if err := db.Put(ctx, keys.DroppedTableSweepKey(), again); err != nil {
		t.Fatal(err)
	}
	n.TestingForgetDroppedSweepDone()
	for i := 0; !n.RunDroppedTableSweepOnce(ctx); i++ {
		if i > 50 {
			t.Fatal("the sweep, run again, never finished")
		}
	}
	if st := sweepState(); st["found"] != float64(1) {
		t.Fatalf("run again, the sweep found more: %v", st)
	}
	if r := record(oldID); r == nil || r.DroppedAt != rec.DroppedAt {
		t.Fatalf("run again, the sweep rewrote the record: %+v, was %+v", r, rec)
	}

	// Inside the window from when the sweep saw it: kept.
	if got := n.RunDroppedTableReclaimOnce(ctx); got != 0 {
		t.Fatalf("reclaimed %d table(s) inside the window", got)
	}
	if rows(oldID) != 3 {
		t.Fatalf("the old table's rows went inside the window: %d", rows(oldID))
	}
	// Past it (the GC TTL plus the reclaim's slack): cleared.
	deadline := time.Now().Add(30 * time.Second)
	for n.RunDroppedTableReclaimOnce(ctx) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the old table was never reclaimed")
		}
		time.Sleep(250 * time.Millisecond)
	}
	if got := rows(oldID); got != 0 {
		t.Fatalf("the old table has %d rows after the reclaim", got)
	}
	if record(oldID) != nil {
		t.Fatal("the old table's record outlived the reclaim")
	}
	if rows(liveID) != 2 || rows(rid) != 1 || rows(catalog.MetricsTableID+100) != 1 {
		t.Fatalf("rows touched: live %d (want 2), restoring %d (want 1), reserved %d (want 1)",
			rows(liveID), rows(rid), rows(catalog.MetricsTableID+100))
	}
	execSQL(t, ctx, s, `SELECT * FROM live_t`)

	if err := db.Delete(ctx, keys.RestoreProgressKey("sweep-test")); err != nil {
		t.Fatal(err)
	}
}
