package testcluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestCatalogScanScale measures, for issue #313, what the statements and
// background passes that used to read every table descriptor cost as the
// catalog grows: DROP TABLE (which asks which views read the table), the
// retention provider's refresh behind GC, and the re-shard janitor's
// pass. Like TestCatalogScale (#311) it is off unless
// DATAX_CATALOG_SCALE names the table counts:
//
//	DATAX_CATALOG_SCALE=1000,5000 go test ./pkg/testutils/testcluster \
//	    -run TestCatalogScanScale -v -timeout 60m
//
// It runs one node, and counts the rows read out of the table descriptor
// and statistics spans (by scans and exports alike) while each operation
// runs. The statistics sampler's passes (#316) are measured too.
func TestCatalogScanScale(t *testing.T) {
	spec := os.Getenv("DATAX_CATALOG_SCALE")
	if spec == "" {
		t.Skip("set DATAX_CATALOG_SCALE=1000,5000 to run the catalog scan measurements")
	}
	for _, f := range strings.Split(spec, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 10 {
			t.Fatalf("DATAX_CATALOG_SCALE: %q is not a table count of at least 10", f)
		}
		t.Run(fmt.Sprintf("tables=%d", n), func(t *testing.T) { runCatalogScanScale(t, n) })
	}
}

func runCatalogScanScale(t *testing.T, tables int) {
	tc := StartWithOptions(t, 1, func(c *server.Config) {
		c.DroppedTableReclaimInterval = -1
		c.StatsRefreshInterval = -1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Minute)
	defer cancel()
	n := tc.Nodes[0]
	root := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)

	start := time.Now()
	createScaleTables(t, ctx, n, "scan_t", tables)
	execSQL(t, ctx, root, `CREATE TABLE scan_ttl (id INT8 PRIMARY KEY) WITH (gc_ttl = '1h')`)
	execSQL(t, ctx, root, `CREATE VIEW scan_v AS SELECT id FROM scan_t_00000`)
	t.Logf("scan[%d tables] created in %s", tables, time.Since(start).Round(time.Second))

	var rows, requests atomic.Int64
	lo, hi := keys.TableDescSpan()
	slo, shi := keys.TableStatsSpan()
	in := func(k, lo, hi keys.Key) bool { return bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 }
	n.DB().TestingSetSendObserver(func(_ context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse) {
		for i, ru := range ba.Requests {
			k := ru.GetInner().Header().Key
			if !(in(k, lo, hi) || in(k, slo, shi)) || br == nil || i >= len(br.Responses) {
				continue
			}
			requests.Add(1)
			switch {
			case br.Responses[i].Scan != nil:
				rows.Add(int64(len(br.Responses[i].Scan.Rows)))
			case br.Responses[i].Export != nil:
				rows.Add(int64(len(br.Responses[i].Export.Records)))
			case br.Responses[i].Get != nil && br.Responses[i].Get.Value != nil:
				rows.Add(1)
			}
		}
	})
	defer n.DB().TestingSetSendObserver(nil)
	measureDesc := func(what string, op func()) {
		rows.Store(0)
		requests.Store(0)
		began := time.Now()
		op()
		t.Logf("scan[%d tables] %s: %d descriptor/statistics rows read in %d requests, %s",
			tables, what, rows.Load(), requests.Load(), time.Since(began).Round(time.Millisecond))
	}
	// Before the view records are complete, DROP TABLE asks every
	// descriptor which views read the table, as it always did.
	measureDesc("DROP TABLE, view records not yet complete", func() { execSQL(t, ctx, root, `DROP TABLE scan_t_00004`) })
	if !n.RecordViewDependenciesOnce(ctx) {
		t.Fatal("the view records were not completed")
	}
	measureDesc("DROP TABLE", func() { execSQL(t, ctx, root, `DROP TABLE scan_t_00001`) })
	measureDesc("DROP TABLE (another)", func() { execSQL(t, ctx, root, `DROP TABLE scan_t_00002`) })
	for i := 0; i < 2; i++ {
		measureDesc(fmt.Sprintf("retention refresh #%d", i+1), func() {
			n.TestingRefreshRetention()
			n.TestingRetentionSpans()
		})
	}
	execSQL(t, ctx, root, `DROP TABLE scan_t_00003`)
	measureDesc("retention refresh after a DROP", func() {
		n.TestingRefreshRetention()
		n.TestingRetentionSpans()
	})
	for i := 0; i < 2; i++ {
		measureDesc(fmt.Sprintf("re-shard janitor pass #%d", i+1), func() { n.RunReshardJanitorOnce(ctx) })
	}
	for i := 0; i < 3; i++ {
		measureDesc(fmt.Sprintf("statistics sampler pass #%d", i+1), func() { n.RunStatsRefreshOnce(ctx) })
	}
	execSQL(t, ctx, root, `CREATE TABLE scan_late (id INT8 PRIMARY KEY)`)
	measureDesc("statistics sampler pass after a CREATE", func() { n.RunStatsRefreshOnce(ctx) })
}
