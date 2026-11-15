package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

func census(t *testing.T, eng *storage.Engine, tableID uint64) storage.SpanCensus {
	t.Helper()
	lo, hi := keys.TableDataSpan(tableID)
	c, err := storage.MVCCSpanCensus(eng, lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// historyServed reports whether a read of the table at ts is answered.
func historyServed(ctx context.Context, n *server.Node, tableID uint64, ts hlc.Timestamp) bool {
	lo, hi := keys.TableDataSpan(tableID)
	_, err := n.DB().NewHistoricalTxn("history", ts).Scan(ctx, lo, hi, 0)
	return err == nil
}

// TestGCTTLPerTable is issue #295's third acceptance item: a short GC
// window on one table collects that table's history and no other's — not
// a table with the node's window, not one with a longer window of its
// own. Checked in the engine, where the versions are, and by a read at a
// timestamp from before: refused for the collected table, answered for
// the others. ALTER TABLE sets and resets the window on a table with
// data.
func TestGCTTLPerTable(t *testing.T) {
	n, eng := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE brief (id INT8 PRIMARY KEY, v TEXT) WITH (gc_ttl = '1s')`)
	execSQL(t, ctx, s, `CREATE TABLE plain (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE lasting (id INT8 PRIMARY KEY, v TEXT) WITH (gc_ttl = '1h')`)
	ids := map[string]uint64{}
	for _, tbl := range []string{"brief", "plain", "lasting"} {
		// Two rows; one is updated and one deleted, so each table holds
		// a superseded version, a tombstone and the value under it.
		execSQL(t, ctx, s, `INSERT INTO `+tbl+` VALUES (1, 'a'), (2, 'b')`)
		ids[tbl] = tableID(t, ctx, n, tbl)
	}
	before := n.Clock().Now()
	for _, tbl := range []string{"brief", "plain", "lasting"} {
		execSQL(t, ctx, s, `UPDATE `+tbl+` SET v = 'a2' WHERE id = 1`)
		execSQL(t, ctx, s, `DELETE FROM `+tbl+` WHERE id = 2`)
		if c := census(t, eng, ids[tbl]); c.LiveKeys != 1 || c.Garbage != 2 || c.Tombstones != 1 {
			t.Fatalf("%s before GC: %+v", tbl, c)
		}
	}

	// One pass at the node's window (a day), a little over a second
	// later: only the table whose own window is a second is collected.
	time.Sleep(1200 * time.Millisecond)
	n.Store().RunGCOnce(ctx, 24*time.Hour)
	if c := census(t, eng, ids["brief"]); c.LiveKeys != 1 || c.Garbage != 0 || c.Tombstones != 0 {
		t.Fatalf("the table with a 1s window was not collected: %+v", c)
	}
	if historyServed(ctx, n, ids["brief"], before) {
		t.Fatal("a read below the short window was served")
	}
	for _, tbl := range []string{"plain", "lasting"} {
		if c := census(t, eng, ids[tbl]); c.Garbage != 2 || c.Tombstones != 1 {
			t.Fatalf("%s lost history to another table's window: %+v", tbl, c)
		}
		if !historyServed(ctx, n, ids[tbl], before) {
			t.Fatalf("%s no longer answers a read from before", tbl)
		}
	}
	if res := execSQL(t, ctx, s, `SELECT v FROM brief`); len(res.Rows) != 1 || res.Rows[0][0].S != "a2" {
		t.Fatalf("brief's current rows: %v", res.Rows)
	}

	// A pass at a node window of one second: the plain table goes by
	// it, the table with an hour of its own does not.
	n.Store().RunGCOnce(ctx, time.Second)
	if c := census(t, eng, ids["plain"]); c.Garbage != 0 || c.Tombstones != 0 {
		t.Fatalf("plain at a 1s node window: %+v", c)
	}
	if c := census(t, eng, ids["lasting"]); c.Garbage != 2 || c.Tombstones != 1 || !historyServed(ctx, n, ids["lasting"], before) {
		t.Fatalf("a table with a longer window of its own was collected at the node's: %+v", c)
	}

	// ALTER on a table with data: the hour becomes a second.
	execSQL(t, ctx, s, `ALTER TABLE lasting SET (gc_ttl = '1s')`)
	show := execSQL(t, ctx, s, `SHOW CREATE TABLE lasting`)
	if ddl := show.Rows[0][len(show.Rows[0])-1].S; !strings.Contains(ddl, "WITH (gc_ttl = '1s')") {
		t.Fatalf("SHOW CREATE TABLE:\n%s", ddl)
	}
	n.TestingRefreshRetention()
	n.Store().RunGCOnce(ctx, 24*time.Hour)
	if c := census(t, eng, ids["lasting"]); c.Garbage != 0 || c.Tombstones != 0 {
		t.Fatalf("lasting after SET (gc_ttl = '1s'): %+v", c)
	}
	// And RESET returns it to the node's.
	execSQL(t, ctx, s, `ALTER TABLE lasting RESET (gc_ttl)`)
	execSQL(t, ctx, s, `UPDATE lasting SET v = 'a3' WHERE id = 1`)
	time.Sleep(1200 * time.Millisecond)
	n.TestingRefreshRetention()
	n.Store().RunGCOnce(ctx, 24*time.Hour)
	if c := census(t, eng, ids["lasting"]); c.Garbage != 1 {
		t.Fatalf("lasting after RESET (gc_ttl): %+v", c)
	}

	for _, q := range []string{
		`ALTER TABLE plain SET (gc_ttl = '0s')`,
		`ALTER TABLE plain SET (gc_ttl = 'soon')`,
		`ALTER TABLE plain RESET (shards)`,
		`CREATE TABLE ts (id INT8, at TIMESTAMPTZ, PRIMARY KEY (id, at)) WITH (timeseries = true, retention = '1d', gc_ttl = '1h')`,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil {
			t.Errorf("%s was accepted", q)
		}
	}
}

// TestTableOptionsNeedV21: before the cluster has finalized v21 a node
// may be running a binary that reads a descriptor without the new
// fields, so nothing writes them and nothing purges.
func TestTableOptionsNeedV21(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V20 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE old (a INT8 PRIMARY KEY, at TIMESTAMPTZ)`)
	for _, q := range []string{
		`CREATE TABLE new1 (a INT8 PRIMARY KEY) WITH (gc_ttl = '1h')`,
		`CREATE TABLE new2 (a INT8 PRIMARY KEY, at TIMESTAMPTZ) WITH (ttl_expiration = at)`,
		`ALTER TABLE old SET (gc_ttl = '1h')`,
		`ALTER TABLE old SET (ttl_column = at, ttl = '1 day')`,
		`ALTER TABLE old PURGE`,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
			t.Fatalf("%s at v20: %v, want 0A000", q, serr)
		}
	}
	lo, hi := keys.TableDataSpan(tableID(t, ctx, tc.Nodes[0], "old"))
	if _, err := tc.Nodes[0].PurgeSpan(ctx, lo, hi); err == nil || !strings.Contains(err.Error(), "v21") {
		t.Fatalf("a purge at v20: %v", err)
	}
}
