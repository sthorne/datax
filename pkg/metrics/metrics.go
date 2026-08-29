// Package metrics defines datax's Prometheus metrics. Process-wide series
// (transaction outcomes, KV latencies, housekeeping activity) live on one
// dedicated registry; per-node gauges are registered per node and gathered
// together by the node's /metrics endpoint. client_golang was already in
// the dependency tree via Pebble.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Registry holds the process-wide datax series.
var Registry = prometheus.NewRegistry()

var (
	TxnCommits = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_txn_commits_total", Help: "Transactions committed.",
	})
	TxnAborts = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_txn_aborts_total", Help: "Transactions rolled back or aborted.",
	})
	TxnRetries = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_txn_retries_total", Help: "Retryable transaction errors surfaced to clients (40001s).",
	})
	TxnRefreshes = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_txn_refreshes_total", Help: "Successful read refreshes (restarts avoided).",
	})

	KVBatchLatency = promauto.With(Registry).NewHistogram(prometheus.HistogramOpts{
		Name:    "datax_kv_batch_latency_seconds",
		Help:    "Routed KV batch latency, as seen by the gateway.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16), // 100µs .. ~3.2s
	})

	GCRuns = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_gc_runs_total", Help: "Replicated GC commands proposed.",
	})
	LogTruncations = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_raft_log_truncations_total", Help: "Replicated log truncations proposed.",
	})
	AutoSplits = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_auto_splits_total", Help: "Size-triggered range splits performed.",
	})
	DeadNodeRepairs = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_dead_node_repairs_total", Help: "Replicas rebuilt away from dead nodes.",
	})
	LeaseTransfers = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_lease_transfers_total", Help: "Completed range leadership (lease) transfers.",
	})
	Rebalances = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_rebalances_total", Help: "Replicas moved by automatic load rebalancing.",
	})
	DecommissionMoves = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_decommission_replicas_moved_total", Help: "Replicas drained off decommissioning nodes.",
	})
	CatchupSnapshots = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_catchup_snapshots_total", Help: "Raft catch-up snapshots streamed to lagging followers.",
	})
	RangeMerges = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_range_merges_total", Help: "Adjacent underfull ranges merged.",
	})
	FollowerReads = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_follower_reads_total", Help: "Stale reads served by non-leader replicas at their closed timestamp.",
	})
	DeadlockAborts = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_deadlock_aborts_total", Help: "Transactions aborted as chosen victims of detected deadlock cycles.",
	})
)
