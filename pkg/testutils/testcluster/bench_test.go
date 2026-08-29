package testcluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
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
