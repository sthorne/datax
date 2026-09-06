package testcluster

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/version"
)

// withPG gives a disk node a SQL listener.
func withPG(c *server.Config) { c.PGListen = "127.0.0.1:0" }

// diskSQL connects to a disk node over pgwire.
func diskSQL(t *testing.T, ctx context.Context, n *server.Node) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, "postgres://root@"+n.SQLAddr()+"/datax?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// TestSplitStoreMigration (issue #105): a store bootstrapped by a v12
// binary runs on one engine; after the cluster finalizes v13 its next
// start moves the raft state onto the raft engine and drops the state
// engine's WAL, with every row intact and the store refusing a v12
// binary from then on.
func TestSplitStoreMigration(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	asV12 := func(c *server.Config) { c.BinaryVersionOverride = version.V12 }

	n := startDiskNode(t, dir, true, "", withPG, asV12)
	if n.EngineMode() != "single" {
		t.Fatalf("a v12 store: engine mode %q", n.EngineMode())
	}
	conn := diskSQL(t, ctx, n)
	if _, err := conn.Exec(ctx, `CREATE TABLE t (k INT8 PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO t VALUES ($1, $2)`, int64(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close(ctx)
	n.Stop()
	if _, err := os.Stat(filepath.Join(dir, "raft")); err == nil {
		t.Fatal("a v12 store has a raft directory")
	}

	// The v13 binary on a v12 cluster: still one engine.
	n = startDiskNode(t, dir, false, "", withPG)
	if n.EngineMode() != "single" {
		t.Fatalf("before finalize: engine mode %q", n.EngineMode())
	}
	resp := adminCall(t, ctx, n.Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: int(version.V13)})
	if resp.Error != "" {
		t.Fatalf("finalize v13: %+v", resp)
	}
	// The node observes (and persists) the finalized version within a
	// heartbeat.
	deadline := time.Now().Add(30 * time.Second)
	for n.ClusterVersion() < version.V13 {
		if time.Now().After(deadline) {
			t.Fatalf("node never observed v13 (at %s)", n.ClusterVersion())
		}
		time.Sleep(100 * time.Millisecond)
	}
	n.Stop()

	// The next start migrates.
	n = startDiskNode(t, dir, false, "", withPG)
	if n.EngineMode() != "split" {
		t.Fatalf("after finalize: engine mode %q", n.EngineMode())
	}
	// A Pebble store has a MANIFEST (v2 no longer writes CURRENT).
	if m, _ := filepath.Glob(filepath.Join(dir, "raft", "MANIFEST-*")); len(m) == 0 {
		t.Fatalf("no raft engine under the store: %s/raft has no MANIFEST", dir)
	}
	conn = diskSQL(t, ctx, n)
	var count int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM t`).Scan(&count); err != nil || count != 500 {
		t.Fatalf("rows after the migration: %d, %v", count, err)
	}
	for i := 500; i < 1000; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO t VALUES ($1, $2)`, int64(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close(ctx)
	n.Stop()

	// A v12 binary refuses the migrated store.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Start(server.Config{Dir: dir, Listener: lis, BinaryVersionOverride: version.V12, MetricsRecordInterval: -1}); err == nil || !strings.Contains(err.Error(), "downgrading") {
		t.Fatalf("v12 binary on a split store: %v", err)
	}
	_ = lis.Close()

	// And a clean restart keeps everything (nothing to replay).
	n = startDiskNode(t, dir, false, "", withPG)
	defer n.Stop()
	if n.EngineMode() != "split" {
		t.Fatalf("restart: engine mode %q", n.EngineMode())
	}
	conn = diskSQL(t, ctx, n)
	defer func() { _ = conn.Close(ctx) }()
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM t`).Scan(&count); err != nil || count != 1000 {
		t.Fatalf("rows after the restart: %d, %v", count, err)
	}
}

// TestSplitStoreReplay (issue #105): a split store closed the way a crash
// leaves it — the state engine's memtable unflushed — restarts by
// replaying the committed raft entries the state engine had not kept,
// exactly the missing ones, and every acknowledged row is back.
func TestSplitStoreReplay(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	n := startDiskNode(t, dir, true, "", withPG)
	if n.EngineMode() != "split" {
		t.Fatalf("a fresh v13 store: engine mode %q", n.EngineMode())
	}
	conn := diskSQL(t, ctx, n)
	if _, err := conn.Exec(ctx, `CREATE TABLE t (k INT8 PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	// Flush what exists so far, then write rows that only the raft log
	// will hold at the "crash".
	if _, err := conn.Exec(ctx, `INSERT INTO t VALUES (0, 'flushed')`); err != nil {
		t.Fatal(err)
	}
	if err := n.Store().Engine().Flush(); err != nil {
		t.Fatal(err)
	}
	const rows = 300
	for i := 1; i <= rows; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO t VALUES ($1, $2)`, int64(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close(ctx)

	// Close without the shutdown flush: the state engine forgets the rows.
	storage.TestingSkipFlushOnClose = true
	n.Stop()
	storage.TestingSkipFlushOnClose = false

	replayedBefore := testutil.ToFloat64(metrics.RaftReplayedEntries)
	n = startDiskNode(t, dir, false, "", withPG)
	defer n.Stop()
	replayed := testutil.ToFloat64(metrics.RaftReplayedEntries) - replayedBefore
	if replayed < rows {
		t.Fatalf("replayed %.0f entries, want at least the %d unflushed inserts", replayed, rows)
	}
	conn = diskSQL(t, ctx, n)
	defer func() { _ = conn.Close(ctx) }()
	var count int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM t`).Scan(&count); err != nil || count != rows+1 {
		t.Fatalf("rows after the replay: %d, %v", count, err)
	}
	var v string
	if err := conn.QueryRow(ctx, `SELECT v FROM t WHERE k = $1`, int64(rows)).Scan(&v); err != nil || v != fmt.Sprintf("v%d", rows) {
		t.Fatalf("last row after the replay: %q, %v", v, err)
	}
	t.Logf("replayed %.0f committed entries after an unflushed close", replayed)

	// A clean restart replays nothing.
	n.Stop()
	replayedBefore = testutil.ToFloat64(metrics.RaftReplayedEntries)
	n2 := startDiskNode(t, dir, false, "", withPG)
	defer n2.Stop()
	if d := testutil.ToFloat64(metrics.RaftReplayedEntries) - replayedBefore; d != 0 {
		t.Fatalf("a clean restart replayed %.0f entries", d)
	}
	conn2 := diskSQL(t, ctx, n2)
	defer func() { _ = conn2.Close(ctx) }()
	if err := conn2.QueryRow(ctx, `SELECT count(*) FROM t`).Scan(&count); err != nil || count != rows+1 {
		t.Fatalf("rows after the clean restart: %d, %v", count, err)
	}
}

// TestSplitStoreDeferredTruncation (issue #105): on a split store an
// applied TruncateLog does not delete log entries until the state engine
// has flushed the applies they cover; after a flush the truncation
// lands.
func TestSplitStoreDeferredTruncation(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE t (k INT8 PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	// Enough entries on the table's range for a truncation to be worth
	// proposing (256 reclaimable past a 64-entry floor).
	for i := 0; i < 400; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO t VALUES ($1)`, int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Every node proposes for the ranges it leads; the truncation applies
	// everywhere but deletes nothing yet: no state engine has flushed.
	for _, n := range tc.Nodes {
		n.Store().RunLogTruncationOnce(ctx)
	}
	time.Sleep(500 * time.Millisecond)
	before := testutil.ToFloat64(metrics.RaftDeferredTruncations)
	var pending int
	for _, n := range tc.Nodes {
		n.Store().VisitReplicas(func(r *kvserver.Replica) bool {
			if r.TruncatedIndex() == 0 && r.AppliedIndex() > 320 {
				pending++
			}
			return true
		})
	}
	if pending == 0 {
		t.Fatal("no replica with an applied log past the truncation threshold and an untruncated log")
	}
	// Flush every state engine; the housekeeping tick then performs the
	// deferred truncations.
	for _, n := range tc.Nodes {
		if err := n.Store().Engine().Flush(); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range tc.Nodes {
		n.Store().RunLogTruncationOnce(ctx)
	}
	if d := testutil.ToFloat64(metrics.RaftDeferredTruncations) - before; d < 1 {
		t.Fatalf("deferred truncations after the flush: %.0f", d)
	}
	truncated := 0
	for _, n := range tc.Nodes {
		n.Store().VisitReplicas(func(r *kvserver.Replica) bool {
			if r.TruncatedIndex() > 0 {
				truncated++
			}
			return true
		})
	}
	if truncated == 0 {
		t.Fatal("no replica truncated its log after the flush")
	}
	// The cluster is fine afterwards.
	if _, err := conn.Exec(ctx, `INSERT INTO t VALUES (1000)`); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM t`).Scan(&count); err != nil || count != 401 {
		t.Fatalf("rows: %d, %v", count, err)
	}
}

// TestColumnarBlocksRatchet (issue #166): a store created by a v13 binary
// runs at Pebble format 16; the v14 binary keeps it there while the
// cluster is at v13; the finalize of v14 ratchets both engines to format
// 19 online, within a heartbeat, no restart; the store stays there across
// a restart with every row intact.
func TestColumnarBlocksRatchet(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	asV13 := func(c *server.Config) { c.BinaryVersionOverride = version.V13 }

	n := startDiskNode(t, dir, true, "", withPG, asV13)
	if n.StoreFormat() != storage.FormatBase || n.RaftStoreFormat() != storage.FormatBase {
		t.Fatalf("a v13 store: formats %d / %d, want %d", n.StoreFormat(), n.RaftStoreFormat(), storage.FormatBase)
	}
	conn := diskSQL(t, ctx, n)
	if _, err := conn.Exec(ctx, `CREATE TABLE t (k INT8 PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO t VALUES ($1, $2)`, int64(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close(ctx)
	n.Stop()

	// The v14 binary on a v13 cluster: the format is the cluster's, not
	// the binary's.
	n = startDiskNode(t, dir, false, "", withPG)
	if n.StoreFormat() != storage.FormatBase {
		t.Fatalf("before finalize: format %d, want %d", n.StoreFormat(), storage.FormatBase)
	}
	resp := adminCall(t, ctx, n.Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: int(version.V14)})
	if resp.Error != "" {
		t.Fatalf("finalize v14: %+v", resp)
	}
	deadline := time.Now().Add(30 * time.Second)
	for n.StoreFormat() != storage.FormatColumnarBlocks || n.RaftStoreFormat() != storage.FormatColumnarBlocks {
		if time.Now().After(deadline) {
			t.Fatalf("formats after finalize: %d / %d (cluster version %s)", n.StoreFormat(), n.RaftStoreFormat(), n.ClusterVersion())
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Serving while ratcheted, and after a restart.
	conn = diskSQL(t, ctx, n)
	for i := 300; i < 600; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO t VALUES ($1, $2)`, int64(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close(ctx)
	n.Stop()
	n = startDiskNode(t, dir, false, "", withPG)
	defer n.Stop()
	if n.StoreFormat() != storage.FormatColumnarBlocks || n.RaftStoreFormat() != storage.FormatColumnarBlocks {
		t.Fatalf("after a restart: formats %d / %d", n.StoreFormat(), n.RaftStoreFormat())
	}
	conn = diskSQL(t, ctx, n)
	defer func() { _ = conn.Close(ctx) }()
	var count int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM t`).Scan(&count); err != nil || count != 600 {
		t.Fatalf("rows after the ratchet and a restart: %d, %v", count, err)
	}

	// A fresh store bootstrapped by a v14 binary starts at format 19.
	fresh := startDiskNode(t, t.TempDir(), true, "", withPG)
	defer fresh.Stop()
	if fresh.StoreFormat() != storage.FormatColumnarBlocks {
		t.Fatalf("a fresh v14 store: format %d", fresh.StoreFormat())
	}
}

// TestPrefixBloomFollowsClusterVersion (issue #161): a v14 store under
// the v15 binary runs without prefix mode until the cluster finalizes
// v15, still without it until the node restarts (the comparer is fixed
// at open), and with it from the next start — serving the same rows,
// its legacy tables rewritten in the background. A fresh v15 store is
// in prefix mode from the start.
func TestPrefixBloomFollowsClusterVersion(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	asV14 := func(c *server.Config) { c.BinaryVersionOverride = version.V14 }

	n := startDiskNode(t, dir, true, "", withPG, asV14)
	if n.StorePrefixBloom() {
		t.Fatal("a v14 store opened in prefix mode")
	}
	conn := diskSQL(t, ctx, n)
	if _, err := conn.Exec(ctx, `CREATE TABLE t (k INT8 PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO t VALUES ($1, $2)`, int64(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close(ctx)
	n.Stop()

	// The v15 binary on a v14 cluster: no prefix mode; after the finalize
	// still none until a restart.
	n = startDiskNode(t, dir, false, "", withPG)
	if n.StorePrefixBloom() {
		t.Fatal("before finalize: prefix mode")
	}
	resp := adminCall(t, ctx, n.Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: int(version.V15)})
	if resp.Error != "" {
		t.Fatalf("finalize v15: %+v", resp)
	}
	deadline := time.Now().Add(30 * time.Second)
	for n.ClusterVersion() != version.V15 {
		if time.Now().After(deadline) {
			t.Fatalf("cluster version after finalize: %s", n.ClusterVersion())
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n.StorePrefixBloom() {
		t.Fatal("prefix mode switched on without a restart")
	}
	conn = diskSQL(t, ctx, n)
	for i := 300; i < 600; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO t VALUES ($1, $2)`, int64(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close(ctx)
	n.Stop()

	n = startDiskNode(t, dir, false, "", withPG)
	defer n.Stop()
	if !n.StorePrefixBloom() {
		t.Fatal("after a restart at v15: no prefix mode")
	}
	conn = diskSQL(t, ctx, n)
	defer func() { _ = conn.Close(ctx) }()
	var count int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM t`).Scan(&count); err != nil || count != 600 {
		t.Fatalf("rows under prefix mode: %d, %v", count, err)
	}
	var v string
	if err := conn.QueryRow(ctx, `SELECT v FROM t WHERE k = 123`).Scan(&v); err != nil || v != "v123" {
		t.Fatalf("point read under prefix mode: %q, %v", v, err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM t WHERE k = 100000`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("absent key under prefix mode: %d, %v", count, err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO t VALUES (123, 'dup')`); err == nil {
		t.Fatal("a duplicate key was accepted under prefix mode")
	}
	// The legacy tables are rewritten in the background.
	deadline = time.Now().Add(60 * time.Second)
	for n.PrefixBloomRewriteRemaining() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("legacy tables remaining after the rewrite: %d", n.PrefixBloomRewriteRemaining())
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM t`).Scan(&count); err != nil || count != 600 {
		t.Fatalf("rows after the rewrite: %d, %v", count, err)
	}

	// A fresh store bootstrapped by a v15 binary is in prefix mode, and
	// restarts (its first open at start is a plain one).
	freshDir := t.TempDir()
	fresh := startDiskNode(t, freshDir, true, "", withPG)
	if !fresh.StorePrefixBloom() {
		t.Fatal("a fresh v15 store is not in prefix mode")
	}
	fconn := diskSQL(t, ctx, fresh)
	if _, err := fconn.Exec(ctx, `CREATE TABLE f (k INT8 PRIMARY KEY, v TEXT); INSERT INTO f VALUES (1, 'one')`); err != nil {
		t.Fatal(err)
	}
	_ = fconn.Close(ctx)
	fresh.Stop()
	fresh = startDiskNode(t, freshDir, false, "", withPG)
	defer fresh.Stop()
	if !fresh.StorePrefixBloom() {
		t.Fatal("the fresh v15 store restarted without prefix mode")
	}
	fconn = diskSQL(t, ctx, fresh)
	defer func() { _ = fconn.Close(ctx) }()
	if err := fconn.QueryRow(ctx, `SELECT v FROM f WHERE k = 1`).Scan(&v); err != nil || v != "one" {
		t.Fatalf("after the restart: %q, %v", v, err)
	}
}
