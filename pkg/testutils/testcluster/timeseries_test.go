package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestTimeseriesShardedTable: a sharded timeseries table hides its _shard
// column, spreads inserts across bucket prefixes (pre-split into ranges),
// keeps fully-pinned lookups point reads, fans constrained scans across
// the buckets, and sorts fanned ORDER BY results.
func TestTimeseriesShardedTable(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	countRanges := func() int {
		c := 0
		n.Store().VisitReplicas(func(*kvserver.Replica) bool { c++; return true })
		return c
	}
	before := countRanges()

	execSQL(t, ctx, s, `CREATE TABLE metrics (
		series TEXT, ts TIMESTAMPTZ, val FLOAT8, PRIMARY KEY (series, ts)
	) WITH (timeseries = true, shards = 8)`)

	// Pre-splits: 7 interior bucket boundaries + the 2 table edges.
	if after := countRanges(); after < before+8 {
		t.Fatalf("expected pre-split ranges: %d -> %d", before, after)
	}

	// Option validation.
	for _, bad := range []string{
		`CREATE TABLE b1 (a INT PRIMARY KEY, ts TIMESTAMPTZ) WITH (retention = '7d')`,
		`CREATE TABLE b2 (a INT PRIMARY KEY) WITH (timeseries = true)`,
		`CREATE TABLE b3 (series INT, ts TIMESTAMPTZ, PRIMARY KEY (ts, series)) WITH (timeseries = true)`,
		`CREATE TABLE b4 (ts TIMESTAMPTZ PRIMARY KEY) WITH (timeseries = true, shards = 1)`,
		`CREATE TABLE b5 (ts TIMESTAMPTZ PRIMARY KEY) WITH (timeseries = true, shards = 300)`,
		`CREATE TABLE b6 (ts TIMESTAMPTZ PRIMARY KEY, _shard INT) WITH (timeseries = true, shards = 4)`,
		`CREATE TABLE b7 (ts TIMESTAMPTZ PRIMARY KEY) WITH (timeseries = true, nope = 1)`,
		`CREATE TABLE b8 (ts TIMESTAMPTZ PRIMARY KEY) WITH (timeseries = true, retention = 'xyz')`,
	} {
		if _, serr := trySQL(ctx, s, bad); serr == nil {
			t.Fatalf("accepted %q", bad)
		}
	}

	for h := 0; h < 8; h++ {
		execSQL(t, ctx, s, `INSERT INTO metrics VALUES ('cpu', $1, 0.5), ('mem', $1, 1.5)`,
			mustTS(t, 2026, 8, 30, h))
	}

	// SELECT * hides the shard column.
	res := execSQL(t, ctx, s, `SELECT * FROM metrics WHERE series = 'cpu' AND ts = '2026-08-30 03:00:00Z'`)
	if len(res.Columns) != 3 || res.Columns[0].Name != "series" {
		t.Fatalf("SELECT * columns: %+v", res.Columns)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("point rows: %+v", res.Rows)
	}
	// ... and the fully-pinned lookup stays a single point read.
	if p := explainPlan(t, ctx, s, `SELECT val FROM metrics WHERE series = 'cpu' AND ts = '2026-08-30 03:00:00Z'`); p != "point lookup on primary key" {
		t.Fatalf("point plan: %q", p)
	}

	// _shard is not a valid insert target or update target.
	if _, serr := trySQL(ctx, s, `INSERT INTO metrics (series, ts, val, _shard) VALUES ('x', '2026-08-30 00:00:00Z', 1.0, 3)`); serr == nil {
		t.Fatal("explicit _shard insert accepted")
	}
	if _, serr := trySQL(ctx, s, `UPDATE metrics SET _shard = 2 WHERE series = 'cpu'`); serr == nil {
		t.Fatal("_shard update accepted")
	}

	// A (series, time-window) query fans out over the buckets and still
	// returns exactly the window.
	q := `SELECT val FROM metrics WHERE series = 'cpu' AND ts >= '2026-08-30 02:00:00Z' AND ts < '2026-08-30 06:00:00Z'`
	p := explainPlan(t, ctx, s, q)
	if !strings.Contains(p, "fan-out over 8 shard buckets") {
		t.Fatalf("fan plan: %q", p)
	}
	if res = execSQL(t, ctx, s, q); len(res.Rows) != 4 {
		t.Fatalf("window rows: %+v", res.Rows)
	}

	// Fanned results are not naturally ordered: ORDER BY must sort (and
	// stay correct).
	oq := `SELECT ts FROM metrics WHERE series = 'mem' AND ts >= '2026-08-30 00:00:00Z' ORDER BY ts`
	if p := explainPlan(t, ctx, s, oq); !strings.Contains(p, "in-memory sort") {
		t.Fatalf("fan+order plan: %q", p)
	}
	res = execSQL(t, ctx, s, oq)
	if len(res.Rows) != 8 {
		t.Fatalf("ordered rows: %d", len(res.Rows))
	}
	for i := 1; i < len(res.Rows); i++ {
		if res.Rows[i-1][0].I >= res.Rows[i][0].I {
			t.Fatalf("not sorted at %d: %+v", i, res.Rows)
		}
	}

	// LIMIT across the fan-out is a global limit.
	res = execSQL(t, ctx, s, `SELECT val FROM metrics WHERE series = 'cpu' AND ts >= '2026-08-30 00:00:00Z' LIMIT 3`)
	if len(res.Rows) != 3 {
		t.Fatalf("limit rows: %d", len(res.Rows))
	}

	// Aggregates see every shard.
	res = execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM metrics WHERE series = 'cpu'`)
	if res.Rows[0][0].I != 8 {
		t.Fatalf("count: %+v", res.Rows)
	}

	// An unsharded timeseries table works too, without fan-out.
	execSQL(t, ctx, s, `CREATE TABLE plain_ts (ts TIMESTAMPTZ PRIMARY KEY, v INT) WITH (timeseries = true)`)
	execSQL(t, ctx, s, `INSERT INTO plain_ts VALUES ('2026-08-30 00:00:00Z', 1)`)
	if p := explainPlan(t, ctx, s, `SELECT v FROM plain_ts WHERE ts >= '2026-08-30 00:00:00Z'`); strings.Contains(p, "fan-out") {
		t.Fatalf("unsharded plan fans: %q", p)
	}
}

// TestTimeseriesRetention: rows of a retention table age out through the
// GC housekeeping pass with zero SQL DELETEs — the survivor versions are
// expired, not kept — while a plain table on the same store keeps its
// rows, and reads below the new threshold are rejected.
func TestTimeseriesRetention(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE events (id INT8, at TIMESTAMPTZ, v TEXT, PRIMARY KEY (id, at))
		WITH (timeseries = true, retention = '1s')`)
	execSQL(t, ctx, s, `CREATE TABLE keeper (id INT8 PRIMARY KEY, v TEXT)`)

	for i := 0; i < 5; i++ {
		execSQL(t, ctx, s, `INSERT INTO events VALUES ($1, $2, 'old')`,
			types.NewInt(int64(i)), mustTS(t, 2026, 8, 30, i))
		execSQL(t, ctx, s, `INSERT INTO keeper VALUES ($1, 'kept')`, types.NewInt(int64(i)))
	}

	readTS := n.DB().Clock().Now() // pre-expiry snapshot timestamp

	// Age past the 1s retention, then run the housekeeping GC pass with
	// the store-wide default TTL — the retention override supplies the 1s.
	time.Sleep(1200 * time.Millisecond)
	n.Store().RunGCOnce(ctx, 24*time.Hour)

	res := execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM events`)
	if res.Rows[0][0].I != 0 {
		t.Fatalf("events not expired: %+v", res.Rows)
	}
	res = execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM keeper`)
	if res.Rows[0][0].I != 5 {
		t.Fatalf("keeper rows lost: %+v", res.Rows)
	}

	// A historical read below the ratcheted threshold is rejected, not
	// answered with silently-missing rows.
	htxn := n.DB().NewHistoricalTxn("ts-old-read", readTS)
	lo, hi := keys.TableDataSpan(tableID(t, ctx, n, "events"))
	if _, err := htxn.Scan(ctx, lo, hi, 0); err == nil {
		t.Fatal("read below the GC threshold succeeded")
	}

	// New rows live normally after expiry.
	execSQL(t, ctx, s, `INSERT INTO events VALUES (99, $1, 'new')`, types.NewTimestamp(time.Now().UnixNano()))
	res = execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM events`)
	if res.Rows[0][0].I != 1 {
		t.Fatalf("fresh insert after expiry: %+v", res.Rows)
	}
}

func tableID(t *testing.T, ctx context.Context, n *server.Node, name string) uint64 {
	t.Helper()
	cat := catalog.NewAccessor()
	var id uint64
	err := n.DB().RunTxn(ctx, "test-table-id", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := cat.Lookup(ctx, txn, name)
		if err != nil {
			return err
		}
		id = desc.ID
		return nil
	})
	if err != nil {
		t.Fatalf("looking up table %q: %v", name, err)
	}
	return id
}
