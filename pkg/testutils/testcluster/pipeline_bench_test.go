package testcluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
)

// BenchmarkRangeWritePipeline (issue #106) measures one range's write
// ceiling below SQL: one node on disk (real syncs), W writers each
// committing one-phase transactions of B puts to their own keys, so no
// two proposals conflict and the only serialization left is the range's
// own pipeline (append, sync, apply). It reports proposals per second,
// raft entries per synced commit, syncs per second and the mean apply
// time; the nosync variants turn the raft log's sync into a no-op
// (storage.TestingNoSync) to separate the disk's share from the
// protocol's.
//
//	go test ./pkg/testutils/testcluster -run - -bench RangeWritePipeline -benchtime 3s
func BenchmarkRangeWritePipeline(b *testing.B) {
	for _, nosync := range []bool{false, true} {
		for _, batch := range []int{1, 10, 100, 1000} {
			for _, writers := range []int{1, 4, 16, 64} {
				name := fmt.Sprintf("sync/b%d/w%d", batch, writers)
				if nosync {
					name = fmt.Sprintf("nosync/b%d/w%d", batch, writers)
				}
				b.Run(name, func(b *testing.B) { benchRangeWritePipeline(b, batch, writers, nosync) })
			}
		}
	}
}

func benchRangeWritePipeline(b *testing.B, batch, writers int, nosync bool) {
	storage.TestingNoSync = nosync
	defer func() { storage.TestingNoSync = false }()
	dir := b.TempDir()
	n := startDiskNode(b, dir, true, "")
	defer n.Stop()
	ctx := context.Background()
	db := n.DB()
	prefix := keys.TableDataPrefix(906)
	val := make([]byte, 64)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	write := func(w int, seq uint64) error {
		return db.RunTxn(ctx, "pipeline", func(ctx context.Context, txn *kvclient.Txn) error {
			var wb kvclient.WriteBatch
			key := make([]byte, 0, len(prefix)+16)
			for i := 0; i < batch; i++ {
				key = append(key[:0], prefix...)
				key = binary.BigEndian.AppendUint64(key, uint64(w))
				key = binary.BigEndian.AppendUint64(key, seq*uint64(batch)+uint64(i))
				wb.Put(key, val)
			}
			return txn.RunBatch(ctx, &wb)
		})
	}
	// Warm up: elect, settle the closed-timestamp floor, warm the range
	// cache so commits take the one-phase path.
	for w := 0; w < writers; w++ {
		if err := write(w, 0); err != nil {
			b.Fatal(err)
		}
	}
	syncs0 := testutil.ToFloat64(metrics.RaftLogSyncs)
	entries0 := testutil.ToFloat64(metrics.RaftEntriesAppended)
	applySum0, applyN0 := histSum(metrics.RaftApplyLatency)
	onePC0 := testutil.ToFloat64(metrics.OnePhaseCommits)

	b.ResetTimer()
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	per := b.N / writers
	if per == 0 {
		per = 1
	}
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for seq := uint64(1); seq <= uint64(per); seq++ {
				if err := write(w, seq); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	select {
	case err := <-errs:
		b.Fatal(err)
	default:
	}
	props := float64(per * writers)
	syncs := testutil.ToFloat64(metrics.RaftLogSyncs) - syncs0
	entries := testutil.ToFloat64(metrics.RaftEntriesAppended) - entries0
	applySum, applyN := histSum(metrics.RaftApplyLatency)
	applySum -= applySum0
	applyN -= applyN0
	onePC := testutil.ToFloat64(metrics.OnePhaseCommits) - onePC0
	b.ReportMetric(props/elapsed.Seconds(), "props/s")
	b.ReportMetric(props*float64(batch)/elapsed.Seconds(), "rows/s")
	if syncs > 0 {
		b.ReportMetric(syncs/elapsed.Seconds(), "syncs/s")
		b.ReportMetric(entries/syncs, "entries/sync")
	}
	if applyN > 0 {
		b.ReportMetric(applySum/applyN*1e6, "apply-µs")
	}
	b.ReportMetric(onePC/props, "1pc-frac")
}

// histSum reads a histogram's sample sum (seconds) and count.
func histSum(h prometheus.Histogram) (sum, count float64) {
	var m dto.Metric
	if err := h.Write(&m); err != nil || m.Histogram == nil {
		return 0, 0
	}
	return m.Histogram.GetSampleSum(), float64(m.Histogram.GetSampleCount())
}
