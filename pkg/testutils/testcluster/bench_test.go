package testcluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// BenchmarkClusterKVPut drives non-transactional Puts through a NON-leader
// gateway of a 3-node cluster, so every operation crosses real gRPC to the
// leader (JSON batch marshal/unmarshal on both sides) and then replicates
// through raft (JSON raftCommand on the proposer, JSON unmarshal at apply
// on all three replicas). This is the profiling workload for issue #8:
//
//	go test ./pkg/testutils/testcluster -run - -bench BenchmarkClusterKVPut \
//	  -benchtime 20s -cpuprofile cpu.out
func BenchmarkClusterKVPut(b *testing.B) {
	tc, _ := StartWithEngines(b, 3)
	ctx := context.Background()
	prefix := keys.TableDataPrefix(880)
	leader := tc.LeaderIndex(1)
	db := tc.Nodes[(leader+1)%3].DB()
	val := []byte("0123456789abcdef0123456789abcdef") // 32B, row-sized-ish
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := append(prefix.Clone(), fmt.Sprintf("k%06d", i%4096)...)
		if err := db.Put(ctx, key, val); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTxnSingleKeyCommit measures the client-visible cost of the
// smallest transactional write — RunTxn + a one-key WriteBatch + commit —
// through a non-leader gateway of a 3-node cluster. This is the workload
// the parallel-commit and one-phase-commit fast paths exist for.
func BenchmarkTxnSingleKeyCommit(b *testing.B) {
	tc, _ := StartWithEngines(b, 3)
	ctx := context.Background()
	prefix := keys.TableDataPrefix(882)
	leader := tc.LeaderIndex(1)
	db := tc.Nodes[(leader+1)%3].DB()
	val := []byte("0123456789abcdef0123456789abcdef")
	// Settle elections and the closed-timestamp floor: both legitimately
	// knock cold commits off fast paths.
	for i := 0; i < 20; i++ {
		key := append(prefix.Clone(), fmt.Sprintf("w%03d", i)...)
		if err := db.RunTxn(ctx, "bench-warmup", func(ctx context.Context, txn *kvclient.Txn) error {
			var wb kvclient.WriteBatch
			wb.Put(key, val)
			return txn.RunBatch(ctx, &wb)
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := append(prefix.Clone(), fmt.Sprintf("k%08d", i)...)
		if err := db.RunTxn(ctx, "bench", func(ctx context.Context, txn *kvclient.Txn) error {
			var wb kvclient.WriteBatch
			wb.Put(key, val)
			return txn.RunBatch(ctx, &wb)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTxnMultiRangeBatchCommit measures a transactional batch whose 8
// writes interleave across 4 ranges (r0,r1,r2,r3,r0,...) — the shape a SQL
// INSERT with secondary indexes produces. Today the DistSender sends the
// per-range sub-batches sequentially; issue #50's parallel fan-out targets
// exactly this.
func BenchmarkTxnMultiRangeBatchCommit(b *testing.B) {
	tc, _ := StartWithEngines(b, 3)
	ctx := context.Background()
	prefix := keys.TableDataPrefix(883)
	tc.LeaderIndex(1)
	db := tc.Nodes[0].DB()
	// Split into 4 ranges: [.., s1), [s1, s2), [s2, s3), [s3, ..).
	for _, s := range []string{"s1", "s2", "s3"} {
		if _, err := db.AdminSplit(ctx, append(prefix.Clone(), s...)); err != nil {
			b.Fatal(err)
		}
	}
	val := []byte("0123456789abcdef0123456789abcdef")
	shards := []string{"s0", "s1", "s2", "s3"}
	run := func(i int) error {
		return db.RunTxn(ctx, "bench-multi", func(ctx context.Context, txn *kvclient.Txn) error {
			var wb kvclient.WriteBatch
			for j := 0; j < 8; j++ {
				key := append(prefix.Clone(), fmt.Sprintf("%s-k%08d-%d", shards[j%4], i, j)...)
				wb.Put(key, val)
			}
			return txn.RunBatch(ctx, &wb)
		})
	}
	for i := 0; i < 20; i++ {
		if err := run(-i - 1); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := run(i); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkClusterKVGet is the read-path counterpart: reads through a
// non-leader gateway are forwarded to the leader over gRPC and served via
// its lease — no raft proposal, so the profile isolates the batch RPC
// encoding share.
func BenchmarkClusterKVGet(b *testing.B) {
	tc, _ := StartWithEngines(b, 3)
	ctx := context.Background()
	prefix := keys.TableDataPrefix(881)
	leader := tc.LeaderIndex(1)
	db := tc.Nodes[(leader+1)%3].DB()
	for i := 0; i < 4096; i++ {
		key := append(prefix.Clone(), fmt.Sprintf("k%06d", i)...)
		if err := db.Put(ctx, key, []byte("0123456789abcdef0123456789abcdef")); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := append(prefix.Clone(), fmt.Sprintf("k%06d", i%4096)...)
		if _, err := db.Get(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}
