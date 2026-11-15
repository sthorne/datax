package testcluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// TestCatalogScale is the measurement harness of issue #311: what the
// catalog costs as the number of tables in the cluster grows. It is off
// unless DATAX_CATALOG_SCALE names the table counts to run, because each
// one builds a cluster of that many tables and then sits watching an idle
// gateway for a couple of minutes:
//
//	DATAX_CATALOG_SCALE=1000,5000 go test ./pkg/testutils/testcluster \
//	    -run TestCatalogScale -v -timeout 90m
//
// The tables are spread fifty to a database, so "one database" is the
// same size at every scale and only the rest of the cluster grows. Costs
// are counted in routed KV batches and the rows they returned
// (kvclient's send observer) as well as timed: on a shared machine the
// counts are the measurement and the times are an indication.
func TestCatalogScale(t *testing.T) {
	spec := os.Getenv("DATAX_CATALOG_SCALE")
	if spec == "" {
		t.Skip("set DATAX_CATALOG_SCALE=1000,5000 to run the catalog scale measurements")
	}
	for _, f := range strings.Split(spec, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < scaleTablesPerDB {
			t.Fatalf("DATAX_CATALOG_SCALE: %q is not a table count of at least %d", f, scaleTablesPerDB)
		}
		t.Run(fmt.Sprintf("tables=%d", n), func(t *testing.T) { runCatalogScale(t, n) })
	}
}

const scaleTablesPerDB = 50

// scaleTag marks a context whose KV traffic is being counted.
type scaleTag struct{}

// kvCost is the KV traffic of one measured operation.
type kvCost struct {
	Batches, Requests, Rows, Bytes int64
}

func (c kvCost) String() string {
	return fmt.Sprintf("%d batches, %d requests, %d rows, %d bytes", c.Batches, c.Requests, c.Rows, c.Bytes)
}

// scaleObserver counts one node's routed batches: those sent under a
// tagged context into the tag's kvCost, and — whoever sent them — the
// requests addressed to the descriptor lease span and the rows scanned
// out of the table descriptor span, which is how the background traffic
// (lease renewal, schema document builds) is seen.
type scaleObserver struct {
	leaseRequests  atomic.Int64
	descScanRows   atomic.Int64
	descScans      atomic.Int64
	leasePrefix    keys.Key
	descLo, descHi keys.Key
}

func newScaleObserver() *scaleObserver {
	a, _ := keys.DescLeaseSpan(1)
	b, _ := keys.DescLeaseSpan(1 << 62)
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	o := &scaleObserver{leasePrefix: a[:i].Clone()}
	o.descLo, o.descHi = keys.TableDescSpan()
	return o
}

func (o *scaleObserver) observe(ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse) {
	cost, _ := ctx.Value(scaleTag{}).(*kvCost)
	if cost != nil {
		atomic.AddInt64(&cost.Batches, 1)
		atomic.AddInt64(&cost.Requests, int64(len(ba.Requests)))
	}
	for i, ru := range ba.Requests {
		key := ru.GetInner().Header().Key
		if bytes.HasPrefix(key, o.leasePrefix) {
			o.leaseRequests.Add(1)
		}
		if br == nil || i >= len(br.Responses) {
			continue
		}
		var rows, size int64
		switch {
		case br.Responses[i].Scan != nil:
			for _, kv := range br.Responses[i].Scan.Rows {
				rows++
				size += int64(len(kv.Key) + len(kv.Value))
			}
			if ru.Scan != nil && bytes.Compare(key, o.descLo) >= 0 && bytes.Compare(key, o.descHi) < 0 {
				o.descScans.Add(1)
				o.descScanRows.Add(rows)
			}
		case br.Responses[i].Get != nil && br.Responses[i].Get.Value != nil:
			rows, size = 1, int64(len(key)+len(br.Responses[i].Get.Value))
		}
		if cost != nil {
			atomic.AddInt64(&cost.Rows, rows)
			atomic.AddInt64(&cost.Bytes, size)
		}
	}
}

// measure runs op reps times under a counted context and reports the
// cheapest run's KV cost (a retry or a routing miss only ever adds) and
// the median wall time.
func measure(t *testing.T, reps int, op func(ctx context.Context) error) (kvCost, time.Duration) {
	t.Helper()
	var best kvCost
	var times []time.Duration
	for i := 0; i < reps; i++ {
		cost := &kvCost{}
		ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), scaleTag{}, cost), 5*time.Minute)
		start := time.Now()
		err := op(ctx)
		times = append(times, time.Since(start))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 || cost.Requests < best.Requests {
			best = *cost
		}
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	return best, times[len(times)/2]
}

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func replicaCount(n *server.Node) int {
	c := 0
	n.Store().VisitReplicas(func(*kvserver.Replica) bool { c++; return true })
	return c
}

func runCatalogScale(t *testing.T, tables int) {
	// The metrics recorder runs at its production cadence: it is one of
	// the things that asks for the schema cache on every node.
	tc := StartWithOptions(t, 3, func(c *server.Config) { c.MetricsRecordInterval = 0 })
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Minute)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)

	observers := make([]*scaleObserver, len(tc.Nodes))
	for i, n := range tc.Nodes {
		observers[i] = newScaleObserver()
		n.DB().TestingSetSendObserver(observers[i].observe)
	}
	report := func(format string, args ...any) {
		t.Logf("scale[%d tables] "+format, append([]any{tables}, args...)...)
	}

	heapBefore, replicasBefore := heapInUse(), replicaCount(tc.Nodes[0])

	// N one-row tables, fifty to a database.
	dbs := tables / scaleTablesPerDB
	dbName := func(d int) string { return fmt.Sprintf("scale_db_%04d", d) }
	tableName := func(i int) string { return fmt.Sprintf("t_%02d", i) }
	start := time.Now()
	for d := 0; d < dbs; d++ {
		execSQL(t, ctx, root, `CREATE DATABASE `+dbName(d))
	}
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s := sql.NewSession(tc.Nodes[w%len(tc.Nodes)].DB(), catalog.NewAccessor())
			for d := w; d < dbs; d += workers {
				for i := 0; i < scaleTablesPerDB; i++ {
					name := dbName(d) + "." + tableName(i)
					for _, stmt := range []string{
						`CREATE TABLE ` + name + ` (id INT8 PRIMARY KEY, v TEXT)`,
						`INSERT INTO ` + name + ` VALUES (1, 'row')`,
					} {
						// Eight sessions allocating descriptor IDs at once
						// push one another; a restart is retried.
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
	report("created %d databases and %d tables in %s", dbs, dbs*scaleTablesPerDB, time.Since(start).Round(time.Second))

	gateway := tc.Nodes[(tc.LeaderIndex(1)+1)%len(tc.Nodes)]
	db := gateway.DB()
	target := dbName(dbs / 2)

	// Listing one database's tables.
	var listed int
	cost, dur := measure(t, 5, func(ctx context.Context) error {
		return db.RunTxn(ctx, "scale-list", func(ctx context.Context, txn *kvclient.Txn) error {
			d, err := catalog.LookupDatabase(ctx, txn, target)
			if err != nil {
				return err
			}
			descs, err := catalog.NewAccessor().ListIn(ctx, txn, d)
			listed = len(descs)
			return err
		})
	})
	if listed != scaleTablesPerDB {
		t.Fatalf("ListIn(%s) returned %d tables, want %d", target, listed, scaleTablesPerDB)
	}
	report("ListIn one database (%d tables): %s, %s", listed, cost, dur.Round(10*time.Microsecond))

	// The same question as SQL asks it.
	shower := sql.NewSession(db, catalog.NewAccessor())
	execSQL(t, ctx, shower, `USE `+target)
	cost, dur = measure(t, 5, func(ctx context.Context) error {
		res, serr := trySQL(ctx, shower, `SHOW TABLES`)
		if serr != nil {
			return fmt.Errorf("SHOW TABLES: %s", serr.Msg)
		}
		if len(res.Rows) != scaleTablesPerDB {
			return fmt.Errorf("SHOW TABLES returned %d rows", len(res.Rows))
		}
		return nil
	})
	report("SHOW TABLES in one database: %s, %s", cost, dur.Round(10*time.Microsecond))

	// Resolving one table by name, cold (a fresh accessor has cached
	// neither the database nor the table).
	cost, dur = measure(t, 5, func(ctx context.Context) error {
		return db.RunTxn(ctx, "scale-resolve", func(ctx context.Context, txn *kvclient.Txn) error {
			_, err := catalog.NewAccessor().LookupIn(ctx, txn, target, tableName(7))
			return err
		})
	})
	report("resolve one table: %s, %s", cost, dur.Round(10*time.Microsecond))

	// One build of the console's schema document.
	var inDoc int
	cost, dur = measure(t, 3, func(ctx context.Context) error {
		inDoc = gateway.TestingBuildSchemaDoc(ctx)
		return nil
	})
	if inDoc < dbs*scaleTablesPerDB {
		t.Fatalf("the schema document lists %d tables, want at least %d", inDoc, dbs*scaleTablesPerDB)
	}
	report("schema document build (%d tables): %s, %s", inDoc, cost, dur.Round(time.Millisecond))

	// A cluster nobody is browsing: how often does each node build the
	// document anyway, and how many descriptors does it scan to do it?
	const idleWindow = 30 * time.Second
	builds := make([]int64, len(tc.Nodes))
	scanned := make([]int64, len(tc.Nodes))
	for i, n := range tc.Nodes {
		builds[i], scanned[i] = n.TestingSchemaBuilds(), observers[i].descScanRows.Load()
	}
	time.Sleep(idleWindow)
	for i, n := range tc.Nodes {
		report("idle n%d over %s: %d schema document builds, %d descriptors scanned",
			i+1, idleWindow, n.TestingSchemaBuilds()-builds[i], observers[i].descScanRows.Load()-scanned[i])
	}

	// A gateway that touches every table once and then goes idle.
	gi := 0
	for i, n := range tc.Nodes {
		if n == gateway {
			gi = i
		}
	}
	leaseRate := func(window time.Duration) float64 {
		before := observers[gi].leaseRequests.Load()
		time.Sleep(window)
		return float64(observers[gi].leaseRequests.Load()-before) / window.Seconds()
	}
	report("lease requests before the gateway touches anything: %.1f/s", leaseRate(10*time.Second))
	cat := catalog.NewAccessor()
	if err := cat.StartLeasing(db, gateway.Clock(), gateway.Stopper(), 0); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for d := w; d < dbs; d += workers {
				for i := 0; i < scaleTablesPerDB; i++ {
					if err := db.RunTxn(ctx, "scale-touch", func(ctx context.Context, txn *kvclient.Txn) error {
						_, err := cat.LookupIn(ctx, txn, dbName(d), tableName(i))
						return err
					}); err != nil {
						errs <- fmt.Sprintf("touching %s.%s: %v", dbName(d), tableName(i), err)
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
	touched := time.Now()
	report("gateway touched every table in %s; descriptor cache holds %d", touched.Sub(start).Round(time.Second), cat.TestingCacheSize())
	report("lease requests in the 30s after: %.1f/s; descriptor cache holds %d", leaseRate(30*time.Second), cat.TestingCacheSize())
	time.Sleep(time.Until(touched.Add(60 * time.Second)))
	report("lease requests from 60s to 90s idle: %.1f/s; descriptor cache holds %d", leaseRate(30*time.Second), cat.TestingCacheSize())

	// What the tables cost the process: three nodes share this heap.
	heapAfter, replicasAfter := heapInUse(), replicaCount(tc.Nodes[0])
	n := float64(dbs * scaleTablesPerDB)
	report("heap %d MiB -> %d MiB across 3 nodes: %.1f KiB per table per node; n1 replicas %d -> %d (%.3f ranges per table)",
		heapBefore>>20, heapAfter>>20, float64(heapAfter-heapBefore)/n/3/1024, replicasBefore, replicasAfter, float64(replicasAfter-replicasBefore)/n)
}
