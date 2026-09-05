package crash

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestCrashNodeChild is the child entry point (see ChildMain); it is a
// no-op in the parent.
func TestCrashNodeChild(t *testing.T) { ChildMain(t) }

// insertUntilDead inserts rows one at a time, recording every key whose
// INSERT was acknowledged, until the node dies or limit acks are in.
func insertUntilDead(t *testing.T, n *Node, start, limit int) (acked []int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, n.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS acked (k INT8 PRIMARY KEY, pad TEXT)`); err != nil {
		t.Fatal(err)
	}
	pad := fmt.Sprintf("%0512d", 0)
	for k := start; len(acked) < limit; k++ {
		if _, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO acked VALUES (%d, '%s')`, k, pad)); err != nil {
			if n.Exited() {
				return acked
			}
			if strings.Contains(err.Error(), "23505") {
				// A write the crash made ambiguous (durable in the log,
				// the acknowledgment lost) is present after the restart:
				// as good as acknowledged.
				acked = append(acked, k)
				continue
			}
			// A transient error while the node is alive: retry the key.
			k--
			time.Sleep(20 * time.Millisecond)
			continue
		}
		acked = append(acked, k)
	}
	return acked
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
	}{
		{name: "raft-append", opts: Options{FaultPoint: "raft-append:150"}, limit: 100000},
		{name: "raft-apply", opts: Options{FaultPoint: "raft-apply:150"}, limit: 100000},
		{name: "flush-begin", opts: Options{FaultPoint: "flush-begin:1", MemTableSize: 256 << 10}, limit: 100000},
		{name: "external-kill", opts: Options{}, limit: 200, kill: true},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			n := Start(t, sc.opts)
			acked := insertUntilDead(t, n, 1, sc.limit)
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
