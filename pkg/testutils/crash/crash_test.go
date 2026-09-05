package crash

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestCrashNodeChild is the child entry point (see ChildMain); it is a
// no-op in the parent.
func TestCrashNodeChild(t *testing.T) { ChildMain(t) }

// insertUntilDead inserts rows one at a time, recording every key whose
// INSERT was acknowledged, until the node dies or limit acks are in.
func insertUntilDead(t *testing.T, n *Node, start, limit int) []int {
	t.Helper()
	acked, err := insertKeys(n, start, 1, limit)
	if err != nil {
		t.Fatal(err)
	}
	return acked
}

// insertConcurrently runs workers inserters over interleaved keys (the
// group commit joins their appends into shared syncs when the table is
// pre-split), until the node dies or limit acks are in overall.
func insertConcurrently(t *testing.T, n *Node, start, limit, workers int) []int {
	t.Helper()
	var (
		mu    sync.Mutex
		acked []int
		errs  []error
		wg    sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			got, err := insertKeys(n, start+w, workers, limit/workers)
			mu.Lock()
			acked = append(acked, got...)
			if err != nil {
				errs = append(errs, err)
			}
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatal(errs[0])
	}
	sort.Ints(acked)
	return acked
}

// insertKeys inserts start, start+stride, ... until the node dies or
// limit acks are in.
func insertKeys(n *Node, start, stride, limit int) (acked []int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, n.URL())
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS acked (k INT8 PRIMARY KEY, pad TEXT)`); err != nil {
		return nil, err
	}
	pad := fmt.Sprintf("%0512d", 0)
	for k := start; len(acked) < limit; k += stride {
		if _, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO acked VALUES (%d, '%s')`, k, pad)); err != nil {
			if n.Exited() {
				return acked, nil
			}
			if strings.Contains(err.Error(), "23505") {
				// A write the crash made ambiguous (durable in the log,
				// the acknowledgment lost) is present after the restart:
				// as good as acknowledged.
				acked = append(acked, k)
				continue
			}
			// A transient error while the node is alive: retry the key.
			k -= stride
			time.Sleep(20 * time.Millisecond)
			continue
		}
		acked = append(acked, k)
	}
	return acked, nil
}

// presplit carves the acked table into ranges every step keys from start.
func presplit(t *testing.T, n *Node, start, step, ranges int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, n.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS acked (k INT8 PRIMARY KEY, pad TEXT)`); err != nil {
		t.Fatal(err)
	}
	var tuples []string
	for i := 1; i < ranges; i++ {
		tuples = append(tuples, fmt.Sprintf("(%d)", start+i*step))
	}
	if _, err := conn.Exec(ctx, "ALTER TABLE acked SPLIT AT VALUES "+strings.Join(tuples, ", ")); err != nil {
		t.Fatal(err)
	}
}

// verifyAcked checks that every acknowledged key is readable after the
// restart, and that the log and state machine agree.
func verifyAcked(t *testing.T, n *Node, acked []int) {
	t.Helper()
	ranges, err := n.WaitApplied(60 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, n.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var count int64
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM acked`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count < int64(len(acked)) {
		t.Fatalf("%d rows after the restart, %d were acknowledged", count, len(acked))
	}
	for _, k := range acked {
		var got int64
		if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT k FROM acked WHERE k = %d`, k)).Scan(&got); err != nil || got != int64(k) {
			t.Fatalf("acknowledged key %d missing after the restart: %v", k, err)
		}
	}
	t.Logf("%d acknowledged rows present; %d ranges with applied == last index", len(acked), len(ranges))
}

// TestCrashConsistency (issue #100): SIGKILL at a fault point inside the
// node (after entries are synced but before they apply; after an entry
// applies; as a memtable flush begins) and from the outside mid-run —
// every acknowledged write survives the restart and the state machine
// catches up with the raft log.
func TestCrashConsistency(t *testing.T) {
	if IsChild() {
		t.Skip("child process")
	}
	if testing.Short() {
		t.Skip("spawns node processes")
	}
	scenarios := []struct {
		name  string
		opts  Options
		limit int
		kill  bool // the parent kills after limit acks
		// ranges > 1 pre-splits the table and inserts from eight
		// connections, so the store's group commit joins several
		// ranges' appends in every synced batch (issue #102): the
		// fault point then fires at a group-commit boundary.
		ranges int
	}{
		{name: "raft-append", opts: Options{FaultPoint: "raft-append:150"}, limit: 100000},
		{name: "raft-apply", opts: Options{FaultPoint: "raft-apply:150"}, limit: 100000},
		{name: "flush-begin", opts: Options{FaultPoint: "flush-begin:1", MemTableSize: 256 << 10}, limit: 100000},
		{name: "external-kill", opts: Options{}, limit: 200, kill: true},
		{name: "group-commit", opts: Options{FaultPoint: "raft-append:300"}, limit: 100000, ranges: 16},
		{name: "group-commit-kill", opts: Options{}, limit: 800, kill: true, ranges: 16},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			n := Start(t, sc.opts)
			var acked []int
			if sc.ranges > 1 {
				presplit(t, n, 1, 64, sc.ranges)
				acked = insertConcurrently(t, n, 1, sc.limit, 8)
			} else {
				acked = insertUntilDead(t, n, 1, sc.limit)
			}
			if sc.kill {
				n.Kill()
			} else {
				if err := n.WaitExit(60 * time.Second); err != nil {
					t.Fatalf("the fault point never fired: %v (%d acks)", err, len(acked))
				}
			}
			if len(acked) == 0 {
				t.Fatal("no write was acknowledged before the crash")
			}
			t.Logf("%s: node died after %d acknowledged writes", sc.name, len(acked))
			n.Restart("")
			verifyAcked(t, n, acked)
			// A second crash on the reopened store, then a clean restart
			// (fresh keys: the crash may have made the next key durable
			// without acknowledging it).
			more := insertUntilDead(t, n, 1_000_000, 50)
			n.Kill()
			n.Restart("")
			verifyAcked(t, n, append(acked, more...))
			n.Kill()
		})
	}
}
