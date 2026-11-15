package testcluster

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestMetricsRecorderScale measures, for issue #316, the rows the metrics
// recorder writes per tick as the catalog grows: a table.ranges row per
// table (table.rows too, for an analyzed one) and an index.scans and
// index.rows row per index a node has read through. Like
// TestCatalogScanScale it is off unless DATAX_CATALOG_SCALE names the
// table counts:
//
//	DATAX_CATALOG_SCALE=1000,5000 go test ./pkg/testutils/testcluster \
//	    -run TestMetricsRecorderScale -v -timeout 60m
//
// One node; every table is read once first, so every primary index has
// counters — the most index rows a tick can carry. The ticks are run by
// hand, stamped one interval apart.
func TestMetricsRecorderScale(t *testing.T) {
	spec := os.Getenv("DATAX_CATALOG_SCALE")
	if spec == "" {
		t.Skip("set DATAX_CATALOG_SCALE=1000,5000 to run the metrics recorder measurements")
	}
	for _, f := range strings.Split(spec, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 10 {
			t.Fatalf("DATAX_CATALOG_SCALE: %q is not a table count of at least 10", f)
		}
		t.Run(fmt.Sprintf("tables=%d", n), func(t *testing.T) { runMetricsRecorderScale(t, n) })
	}
}

// createScaleTables creates count tables named prefix_00000... with eight
// sessions, retrying serialization failures.
func createScaleTables(t *testing.T, ctx context.Context, n *server.Node, prefix string, count int) {
	t.Helper()
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s := sql.NewSession(n.DB(), catalog.NewAccessor())
			for i := w; i < count; i += workers {
				stmt := fmt.Sprintf(`CREATE TABLE %s_%05d (id INT8 PRIMARY KEY, v TEXT)`, prefix, i)
				for attempt := 0; ; attempt++ {
					_, serr := trySQL(ctx, s, stmt)
					if serr == nil {
						break
					}
					if serr.Code != sql.CodeSerializationFailure || attempt == 50 {
						errs <- fmt.Sprintf("%s: [%s] %s", stmt, serr.Code, serr.Msg)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	select {
	case e := <-errs:
		t.Fatal(e)
	default:
	}
}

func runMetricsRecorderScale(t *testing.T, tables int) {
	tc := StartWithOptions(t, 1, func(c *server.Config) {
		c.DroppedTableReclaimInterval = -1
		c.StatsRefreshInterval = -1
		c.MetricsRecordInterval = -1 // ticks run by hand
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Minute)
	defer cancel()
	n := tc.Nodes[0]
	root := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)
	start := time.Now()
	createScaleTables(t, ctx, n, "met_t", tables)
	for i := 0; i < tables; i++ {
		execSQL(t, ctx, root, fmt.Sprintf(`SELECT * FROM met_t_%05d`, i))
	}
	t.Logf("metrics[%d tables] created and read in %s", tables, time.Since(start).Round(time.Second))

	const interval = 10 * time.Second
	at := time.Now().Truncate(interval).Add(interval)
	tick := func() int {
		at = at.Add(interval)
		began := time.Now()
		rows := n.RecordMetricsAt(ctx, interval, at)
		if rows < 0 {
			t.Fatalf("tick at %s wrote nothing", at)
		}
		if d := time.Since(began); d > 5*time.Second {
			t.Logf("metrics[%d tables] a tick took %s", tables, d.Round(time.Millisecond))
		}
		return rows
	}
	// The first ticks start the table gauges' background reading; wait
	// until a tick carries a row per table.
	for deadline := time.Now().Add(2 * time.Minute); tick() < tables; {
		if time.Now().After(deadline) {
			t.Fatal("the table gauges never reached the recorder")
		}
		time.Sleep(200 * time.Millisecond)
	}
	measure := func(what string, ticks int, between func(i int)) {
		minR, maxR, sum := -1, 0, 0
		began := time.Now()
		for i := 0; i < ticks; i++ {
			if between != nil {
				between(i)
			}
			r := tick()
			sum += r
			if minR < 0 || r < minR {
				minR = r
			}
			if r > maxR {
				maxR = r
			}
		}
		t.Logf("metrics[%d tables] %s: %d ticks, rows written per tick min %d / mean %d / max %d, %s per tick",
			tables, what, ticks, minR, sum/ticks, maxR, (time.Since(began) / time.Duration(ticks)).Round(time.Millisecond))
	}
	measure("idle", 20, nil)
	const active = 10
	measure(fmt.Sprintf("%d tables read between ticks", active), 20, func(i int) {
		for j := 0; j < active; j++ {
			execSQL(t, ctx, root, fmt.Sprintf(`SELECT * FROM met_t_%05d`, (j*97+i)%tables))
		}
	})
}
