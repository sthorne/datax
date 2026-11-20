package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/server"
)

// Range ID allocation under concurrent splits (issue #389): on a single
// node under heavy DDL load, two load splits 4 ms apart — one of a table
// range, one of range 1 in the system span — were both given r5976. The
// second overwrote the first's descriptor, nothing served the system
// span holding the database catalog any more, and every new connection
// failed with "no replica covering key /system/dbns/"datax"".

// rangeIDRecorder collects the right-hand range IDs splits report and
// flags any ID given twice.
type rangeIDRecorder struct {
	t   *testing.T
	mu  sync.Mutex
	ids map[base.RangeID]string
}

func (r *rangeIDRecorder) record(sr *kvpb.AdminSplitResponse) {
	if sr == nil || sr.Left.RangeID == 0 {
		return // the boundary was there already
	}
	r.note(sr.Right.RangeID, "the split at "+sr.Right.StartKey.String())
}

func (r *rangeIDRecorder) note(id base.RangeID, to string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if prev, ok := r.ids[id]; ok {
		r.t.Errorf("range ID %s handed out twice: to %s and to %s", id, prev, to)
	}
	r.ids[id] = to
}

// drawRangeIDs draws n range IDs straight from the counter, as a split
// does, recording each: more draws in flight while the counter's range
// changes hands than the splits alone produce.
func drawRangeIDs(ctx context.Context, db *kvclient.DB, rec *rangeIDRecorder, who string, n int) {
	for i := 0; i < n; i++ {
		if v, err := db.Increment(ctx, keys.RangeIDGenKey(), 1); err == nil {
			rec.note(base.RangeID(v), fmt.Sprintf("draw %d by %s", i, who))
		}
	}
}

// rangeTilingProblems lists what is wrong with a node's view of the
// ranges: its replicas must tile the keyspace — distinct range IDs, no
// gap, no overlap — and every /meta record must name a range the node
// serves, with exactly that span.
func rangeTilingProblems(ctx context.Context, n *server.Node) ([]string, error) {
	var out []string
	var descs []kvpb.RangeDescriptor
	n.Store().VisitReplicas(func(r *kvserver.Replica) bool {
		descs = append(descs, r.Desc())
		return true
	})
	sort.Slice(descs, func(i, j int) bool { return descs[i].StartKey.Compare(descs[j].StartKey) < 0 })
	byID := map[base.RangeID]kvpb.RangeDescriptor{}
	for i, d := range descs {
		if prev, ok := byID[d.RangeID]; ok {
			out = append(out, fmt.Sprintf("range ID %s serves both [%s, %s) and [%s, %s)", d.RangeID, prev.StartKey, prev.EndKey, d.StartKey, d.EndKey))
		}
		byID[d.RangeID] = d
		if i == 0 && !d.StartKey.Equal(keys.MinKey) {
			out = append(out, fmt.Sprintf("no replica covers [%s, %s)", keys.MinKey, d.StartKey))
		}
		if i > 0 && !descs[i-1].EndKey.Equal(d.StartKey) {
			out = append(out, fmt.Sprintf("%s ends at %s, the next range %s starts at %s", descs[i-1].RangeID, descs[i-1].EndKey, d.RangeID, d.StartKey))
		}
	}
	if len(descs) == 0 || !descs[len(descs)-1].EndKey.Equal(keys.MaxKey) {
		out = append(out, fmt.Sprintf("the last range does not reach %s", keys.MaxKey))
	}
	start, end := keys.MetaSpan()
	rows, err := n.DB().Scan(ctx, start, end, 0)
	if err != nil {
		return nil, fmt.Errorf("scanning /meta: %w", err)
	}
	for _, row := range rows {
		var md kvpb.RangeDescriptor
		if err := json.Unmarshal(row.Value, &md); err != nil {
			return nil, fmt.Errorf("corrupt meta record at %s: %w", row.Key, err)
		}
		d, ok := byID[md.RangeID]
		if !ok || !d.StartKey.Equal(md.StartKey) || !d.EndKey.Equal(md.EndKey) {
			out = append(out, fmt.Sprintf("/meta routes [%s, %s) to %s, which serves %v (%v)", md.StartKey, md.EndKey, md.RangeID, d, ok))
		}
	}
	return out, nil
}

// checkRangesTiled fails t with every problem rangeTilingProblems finds
// on n, once n has had up to wait to converge (followers apply splits
// and merges, and the leader repairs /meta, asynchronously).
func checkRangesTiled(t *testing.T, ctx context.Context, n *server.Node, wait time.Duration) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		problems, err := rangeTilingProblems(ctx, n)
		if err != nil {
			t.Fatalf("n%d: %v", n.NodeID(), err)
		}
		if len(problems) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for _, p := range problems {
				t.Errorf("n%d: %s", n.NodeID(), p)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// churnCounterRange splits the range holding the range ID counter at
// the counter and merges it back, over and over, until the returned stop
// is called: the counter changes hands between two ranges while splits
// elsewhere draw from it.
func churnCounterRange(ctx context.Context, db *kvclient.DB, rec *rangeIDRecorder) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			select {
			case <-done:
				return
			default:
			}
			if sr, err := db.AdminSplit(ctx, keys.RangeIDGenKey()); err == nil {
				rec.record(sr)
			}
			_, _ = db.AdminMerge(ctx, keys.DatabaseNamespaceKey("datax"))
		}
	}()
	return func() {
		close(done)
		wg.Wait()
	}
}

// TestConcurrentSplitsGetDistinctRangeIDs drives the reported shape on
// one node: many concurrent splits of table ranges and of the system
// span, merges among them, while the range that holds the range ID
// counter is itself split and merged back. Every split gets its own ID;
// afterwards the ranges tile the keyspace, /meta agrees, and a new SQL
// connection (which reads the database catalog) still works.
func TestConcurrentSplitsGetDistinctRangeIDs(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	rec := &rangeIDRecorder{t: t, ids: map[base.RangeID]string{}}

	const tables, perTable = 16, 20
	tableKey := func(tbl, i int) keys.Key {
		return append(keys.TableDataPrefix(uint64(900+tbl)), fmt.Sprintf("k%03d", i)...)
	}
	var wg sync.WaitGroup
	for tbl := 0; tbl < tables; tbl++ {
		wg.Add(1)
		go func(tbl int) {
			defer wg.Done()
			for i := 0; i < perTable; i++ {
				if sr, err := db.AdminSplit(ctx, tableKey(tbl, i)); err == nil {
					rec.record(sr)
				}
				if i%3 == 2 {
					_, _ = db.AdminMerge(ctx, tableKey(tbl, i-1))
				}
			}
		}(tbl)
	}
	// Range 1's system span splits too, as it did in the report — at
	// database descriptor keys, below the catalog's namespace.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			if sr, err := db.AdminSplit(ctx, keys.DatabaseDescKey(uint64(20000+i))); err == nil {
				rec.record(sr)
			}
		}
	}()
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			drawRangeIDs(ctx, db, rec, fmt.Sprintf("drawer %d", g), 150)
		}(g)
	}
	// The counter's own range changes hands over and over meanwhile.
	stopChurn := churnCounterRange(ctx, db, rec)
	wg.Wait()
	stopChurn()
	if ctx.Err() != nil {
		t.Fatal("timed out")
	}
	if len(rec.ids) < tables*perTable/2+4*150/2 {
		t.Fatalf("only %d splits landed; the load did not run", len(rec.ids))
	}
	checkRangesTiled(t, ctx, tc.Nodes[0], 5*time.Second)

	if _, err := db.Get(ctx, keys.DatabaseNamespaceKey("datax")); err != nil {
		t.Fatalf("reading the catalog after the splits: %v", err)
	}
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("a new connection after the splits: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("SELECT 1: %d, %v", one, err)
	}
}

// TestConcurrentSplitsAcrossNodesGetDistinctRangeIDs: splits driven
// through every node of a three-node cluster at once, each node leading
// some of the ranges, still get distinct IDs, and each node's replicas
// tile the keyspace once it has caught up.
func TestConcurrentSplitsAcrossNodesGetDistinctRangeIDs(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	rec := &rangeIDRecorder{t: t, ids: map[base.RangeID]string{}}
	var wg sync.WaitGroup
	for n := range tc.Nodes {
		for g := 0; g < 5; g++ {
			wg.Add(1)
			go func(n, g int) {
				defer wg.Done()
				db := tc.Nodes[n].DB()
				for i := 0; i < 15; i++ {
					k := append(keys.TableDataPrefix(uint64(950+5*n+g)), fmt.Sprintf("k%03d", i)...)
					if sr, err := db.AdminSplit(ctx, k); err == nil {
						rec.record(sr)
					}
				}
			}(n, g)
		}
	}
	for n := range tc.Nodes {
		for g := 0; g < 2; g++ {
			wg.Add(1)
			go func(n, g int) {
				defer wg.Done()
				drawRangeIDs(ctx, tc.Nodes[n].DB(), rec, fmt.Sprintf("drawer %d on n%d", g, n+1), 100)
			}(n, g)
		}
	}
	stopChurn := churnCounterRange(ctx, tc.Nodes[0].DB(), rec)
	wg.Wait()
	stopChurn()
	if ctx.Err() != nil {
		t.Fatal("timed out")
	}
	if len(rec.ids) < 15*15/2+6*100/2 {
		t.Fatalf("only %d splits landed; the load did not run", len(rec.ids))
	}
	for _, n := range tc.Nodes {
		checkRangesTiled(t, ctx, n, 30*time.Second)
	}
}
