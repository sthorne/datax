package testcluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/server"
)

// heapPeak samples the process's live heap while a step runs and
// reports the highest value seen above the heap at the start. All three
// nodes and the client share the process, so this is the cluster's
// growth, not one gateway's: an upper bound on any one of them.
type heapPeak struct {
	base uint64
	peak atomic.Uint64
	stop chan struct{}
	done chan struct{}
}

func startHeapPeak() *heapPeak {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	h := &heapPeak{base: ms.HeapAlloc, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(h.done)
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-tick.C:
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > h.peak.Load() {
					h.peak.Store(ms.HeapAlloc)
				}
			}
		}
	}()
	return h
}

// growth stops the sampler and returns the peak growth in bytes.
func (h *heapPeak) growth() int64 {
	close(h.stop)
	<-h.done
	if p := h.peak.Load(); p > h.base {
		return int64(p - h.base)
	}
	return 0
}

func mib(n int64) string { return fmt.Sprintf("%.0f MiB", float64(n)/(1<<20)) }

// largeValueSizes is the set of sizes the measurements run at, in MiB:
// DATAX_LARGE_VALUE_SIZES overrides the default (a comma-separated
// list), for a machine with room for more.
func largeValueSizes(t *testing.T, def []int) []int {
	raw := os.Getenv("DATAX_LARGE_VALUE_SIZES")
	if raw == "" {
		return def
	}
	var out []int
	for _, f := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n <= 0 {
			t.Fatalf("DATAX_LARGE_VALUE_SIZES: %q is not a size in MiB", f)
		}
		out = append(out, n)
	}
	return out
}

// TestLargeValueMeasureInline records what a large value does as an
// ordinary BYTEA column value on a 3-node cluster: the time and the
// process's peak heap growth of an INSERT, a SELECT and an UPDATE of
// another column, and what refuses it. It is the "today" half of issue
// #303's numbers (TestLargeObjectMeasure is the other half) and asserts
// nothing beyond the bytes read back: run it with DATAX_LARGE_VALUE_BENCH=1
// and read the log.
func TestLargeValueMeasureInline(t *testing.T) {
	if os.Getenv("DATAX_LARGE_VALUE_BENCH") == "" {
		t.Skip("set DATAX_LARGE_VALUE_BENCH=1 to measure large inline values")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	// On disk, like TestLargeObjectStream, so that the two measure the
	// same thing: an in-memory store's data would be in the heap.
	tc := startDiskCluster(t, ctx, 3)
	url := "postgres://root@" + tc.Nodes[0].SQLAddr() + "/datax?sslmode=disable"
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE blobs (k INT8 PRIMARY KEY, note TEXT, v BYTEA)`); err != nil {
		t.Fatal(err)
	}
	for i, size := range largeValueSizes(t, []int{1, 8, 32, 64}) {
		val := make([]byte, size<<20)
		if _, err := rand.Read(val); err != nil {
			t.Fatal(err)
		}
		step := func(op string, fn func(ctx context.Context) error) bool {
			// A statement the cluster can neither finish nor refuse is a
			// result too: it is given a bounded time and recorded.
			stepCtx, stepCancel := context.WithTimeout(ctx, 90*time.Second)
			defer stepCancel()
			h := startHeapPeak()
			start := time.Now()
			err := fn(stepCtx)
			took, grew := time.Since(start), h.growth()
			outcome := "ok"
			if err != nil {
				outcome = err.Error()
				if len(outcome) > 160 {
					outcome = outcome[:160] + "..."
				}
				// A refused statement can leave the connection unusable
				// (a message the server would not read to its end).
				if conn.IsClosed() {
					if conn, err = pgx.Connect(ctx, url); err != nil {
						t.Fatal(err)
					}
				}
			}
			t.Logf("inline %3d MiB  %-22s %8s  peak heap +%s  %s", size, op, took.Round(time.Millisecond), mib(grew), outcome)
			return outcome == "ok"
		}
		if !step("INSERT", func(ctx context.Context) error {
			_, err := conn.Exec(ctx, `INSERT INTO blobs VALUES ($1, 'a', $2)`, i, val)
			return err
		}) {
			continue
		}
		step("SELECT", func(ctx context.Context) error {
			var got []byte
			if err := conn.QueryRow(ctx, `SELECT v FROM blobs WHERE k = $1`, i).Scan(&got); err != nil {
				return err
			}
			if !bytes.Equal(got, val) {
				return fmt.Errorf("read back %d bytes that differ from the %d written", len(got), len(val))
			}
			return nil
		})
		step("UPDATE of another col", func(ctx context.Context) error {
			_, err := conn.Exec(ctx, `UPDATE blobs SET note = 'b' WHERE k = $1`, i)
			return err
		})
		step("SELECT octet_length", func(ctx context.Context) error {
			var n int64
			return conn.QueryRow(ctx, `SELECT octet_length(v) FROM blobs WHERE k = $1`, i).Scan(&n)
		})
	}
}

// patternReader yields a reproducible stream of incompressible bytes
// without holding it: what a test of a large upload reads from.
type patternReader struct {
	rng  *mrand.Rand
	left int64
}

func newPatternReader(seed, n int64) *patternReader {
	return &patternReader{rng: mrand.New(mrand.NewSource(seed)), left: n}
}

func (p *patternReader) Read(b []byte) (int, error) {
	if p.left == 0 {
		return 0, io.EOF
	}
	if int64(len(b)) > p.left {
		b = b[:p.left]
	}
	n, _ := p.rng.Read(b)
	p.left -= int64(n)
	return n, nil
}

// startDiskCluster starts n nodes on disk (a large object held by an
// in-memory store would be in the heap this test measures) and returns
// the cluster, ready once every range has n replicas.
func startDiskCluster(t *testing.T, ctx context.Context, n int, opts ...func(*server.Config)) *TestCluster {
	t.Helper()
	opts = append([]func(*server.Config){withPG}, opts...)
	// The directories first: cleanups run last-registered-first, and the
	// nodes must stop before their directories are removed.
	dirs := make([]string, n)
	for i := range dirs {
		dirs[i] = t.TempDir()
	}
	tc := &TestCluster{T: t}
	t.Cleanup(tc.StopAll)
	first := startDiskNode(t, dirs[0], true, "", opts...)
	tc.Nodes = append(tc.Nodes, first)
	for i := 1; i < n; i++ {
		tc.Nodes = append(tc.Nodes, startDiskNode(t, dirs[i], false, first.Addr(), opts...))
	}
	if n > 1 {
		if err := tc.waitForReplication(ctx, n, ""); err != nil {
			t.Fatal(err)
		}
	}
	return tc
}

// TestLargeObjectStream is the issue's first acceptance item at a size
// a shared machine can afford on every run, and at the issue's own size
// when asked: an object is streamed in through pgx and streamed back
// out, the bytes match, and the whole process — every node of the
// cluster and the client, which is more than the gateway — grows by
// much less than the object while it happens.
//
// DATAX_LARGE_OBJECT_MIB sets the size (default 64, or 16 under the race
// detector; 1024 is the issue's
// 1 GiB, which wants about 8 GiB of free disk on three nodes) and
// DATAX_LARGE_OBJECT_NODES the cluster size (default 3).
func TestLargeObjectStream(t *testing.T) {
	sizeMiB, nodes := 64, 3
	if raceEnabled {
		// The bound below is about memory that does not grow with the
		// object; 16 MiB still streams 64 pages through every node.
		sizeMiB = 16
	}
	if v := os.Getenv("DATAX_LARGE_OBJECT_MIB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("DATAX_LARGE_OBJECT_MIB=%q", v)
		}
		sizeMiB = n
	}
	if v := os.Getenv("DATAX_LARGE_OBJECT_NODES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("DATAX_LARGE_OBJECT_NODES=%q", v)
		}
		nodes = n
	}
	size := int64(sizeMiB) << 20
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	tc := startDiskCluster(t, ctx, nodes)
	conn := diskSQL(t, ctx, tc.Nodes[0])
	defer func() { _ = conn.Close(ctx) }()

	// Warm the path with a small object first, so one-time allocations
	// (the tables, memtables, connection buffers) are not counted as the
	// large object's.
	loStore(t, ctx, conn, randomBytes(t, 4<<20), 1<<20)

	buf := make([]byte, 1<<20)
	wrote := sha256.New()
	h := startHeapPeak()
	start := time.Now()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oid, err := loOf(tx).Create(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := loOf(tx).Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := io.CopyBuffer(struct{ io.Writer }{io.MultiWriter(obj, wrote)}, newPatternReader(303, size), buf); err != nil || n != size {
		t.Fatalf("upload: %d of %d bytes, %v", n, size, err)
	}
	if err := obj.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	wTook, wGrew := time.Since(start), h.growth()

	// Read back through another gateway when there is one.
	rconn := diskSQL(t, ctx, tc.Nodes[len(tc.Nodes)-1])
	defer func() { _ = rconn.Close(ctx) }()
	read := sha256.New()
	h = startHeapPeak()
	start = time.Now()
	rtx, err := rconn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	robj, err := loOf(rtx).Open(ctx, oid, pgx.LargeObjectModeRead)
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.CopyBuffer(read, struct{ io.Reader }{robj}, buf)
	if err != nil || n != size {
		t.Fatalf("download: %d of %d bytes, %v", n, size, err)
	}
	if err := rtx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	rTook, rGrew := time.Since(start), h.growth()

	t.Logf("large object %4d MiB on %d node(s)  write %8s (%.0f MiB/s)  peak heap +%s", sizeMiB, nodes, wTook.Round(time.Millisecond), float64(sizeMiB)/wTook.Seconds(), mib(wGrew))
	t.Logf("large object %4d MiB on %d node(s)  read  %8s (%.0f MiB/s)  peak heap +%s", sizeMiB, nodes, rTook.Round(time.Millisecond), float64(sizeMiB)/rTook.Seconds(), mib(rGrew))
	if !bytes.Equal(wrote.Sum(nil), read.Sum(nil)) {
		t.Fatal("the bytes read back are not the bytes written")
	}
	// The bound. What is being ruled out is memory in proportion to the
	// object: as a row value, 32 MiB grew this process by about 1.7 GiB
	// on INSERT and 0.75 GiB on SELECT (TestLargeValueMeasureInline).
	// What remains is the stores' own working memory — memtables and
	// block caches on every node, the Raft logs' tails — which does not
	// depend on the object's size; the floor allows for it, and for a
	// garbage collector that has not yet run.
	bound := size / 4
	if floor := int64(largeObjectHeapFloor); bound < floor {
		bound = floor
	}
	if wGrew > bound || rGrew > bound {
		t.Fatalf("streaming %s grew the heap by %s (write) and %s (read); the bound is %s", mib(size), mib(wGrew), mib(rGrew), mib(bound))
	}
}

// largeObjectHeapFloor is the heap growth TestLargeObjectStream allows
// whatever the object's size.
const largeObjectHeapFloor = 512 << 20
