package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// intentMetaFirstBytes returns the first byte of every intent metadata
// record under the table's data span on the node's store.
func intentMetaFirstBytes(t *testing.T, n *server.Node, tableID uint64) []byte {
	t.Helper()
	prefix := keys.TableDataPrefix(tableID)
	lower := storage.EncodeMVCCKey(prefix, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(prefix.PrefixEnd(), hlc.Timestamp{})
	it := n.Store().Engine().NewIter(lower, upper)
	defer func() { _ = it.Close() }()
	var out []byte
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		if _, ts, err := storage.DecodeMVCCKey(it.Key()); err == nil && ts.IsEmpty() && len(it.Value()) > 0 {
			out = append(out, it.Value()[0])
		}
	}
	return out
}

// openIntent begins a transaction that writes one row and leaves it open,
// returning the transaction so the caller can inspect the intent first.
func openIntent(t *testing.T, ctx context.Context, conn *pgx.Conn, id int64) pgx.Tx {
	t.Helper()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO bm VALUES ($1, 'v')`, id); err != nil {
		t.Fatal(err)
	}
	return tx
}

// TestBinaryMetaFollowsClusterVersion (issue #141): a transaction's
// intent metadata is JSON while the cluster is at v13 — under a v13
// binary and under the v14 binary alike — and protobuf once v14 is
// finalized, with the JSON intents from before still resolving.
func TestBinaryMetaFollowsClusterVersion(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	asV13 := func(c *server.Config) { c.BinaryVersionOverride = version.V13 }

	n := startDiskNode(t, dir, true, "", withPG, asV13)
	conn := diskSQL(t, ctx, n)
	if _, err := conn.Exec(ctx, `CREATE TABLE bm (k INT8 PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	tableID := lookupDescriptor(t, ctx, n.DB(), "bm").ID
	tx := openIntent(t, ctx, conn, 1)
	if got := intentMetaFirstBytes(t, n, tableID); len(got) != 1 || got[0] != '{' {
		t.Fatalf("v13 cluster, v13 binary: intent metadata bytes %q, want one JSON record", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(ctx)
	n.Stop()

	// The v14 binary on a v13 cluster still writes JSON.
	n = startDiskNode(t, dir, false, "", withPG)
	conn = diskSQL(t, ctx, n)
	tx = openIntent(t, ctx, conn, 2)
	if got := intentMetaFirstBytes(t, n, tableID); len(got) != 1 || got[0] != '{' {
		t.Fatalf("v13 cluster, v14 binary: intent metadata bytes %q, want one JSON record", got)
	}
	// Finalize v14 while that JSON intent is open; the node observes the
	// version within a heartbeat.
	resp := adminCall(t, ctx, n.Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: int(version.V14)})
	if resp.Error != "" {
		t.Fatalf("finalize v14: %+v", resp)
	}
	deadline := time.Now().Add(30 * time.Second)
	for n.ClusterVersion() < version.V14 {
		if time.Now().After(deadline) {
			t.Fatalf("node never observed v14 (at %s)", n.ClusterVersion())
		}
		time.Sleep(100 * time.Millisecond)
	}
	// A transaction begun at v14 lays down protobuf metadata beside the
	// open JSON intent; both resolve.
	conn2 := diskSQL(t, ctx, n)
	tx2 := openIntent(t, ctx, conn2, 3)
	got := intentMetaFirstBytes(t, n, tableID)
	json, binary := 0, 0
	for _, b := range got {
		if b == '{' {
			json++
		} else {
			binary++
		}
	}
	if json != 1 || binary != 1 {
		t.Fatalf("after finalize: intent metadata bytes %q, want one JSON and one binary record", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing the JSON intent under v14: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("committing the binary intent: %v", err)
	}
	var count int64
	if err := conn2.QueryRow(ctx, `SELECT count(*) FROM bm`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("rows: %d, %v", count, err)
	}
	if got := intentMetaFirstBytes(t, n, tableID); len(got) != 0 {
		t.Fatalf("intents left after both commits: %q", got)
	}
	_ = conn.Close(ctx)
	_ = conn2.Close(ctx)
	n.Stop()
}
