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
	TxnAmbiguousResends = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_txn_ambiguous_resends_total", Help: "Transactional batches re-sent because a leadership change left their proposal's fate unknown.",
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
	LoadSplits = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_load_splits_total", Help: "Load-triggered range splits performed.",
	})
	DeadNodeRepairs = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_dead_node_repairs_total", Help: "Replicas rebuilt away from dead nodes.",
	})
	LeaseTransfers = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_lease_transfers_total", Help: "Completed range leadership (lease) transfers.",
	})
	ConsistencyChecks = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_consistency_checks_total", Help: "Replica consistency probes proposed.",
	})
	ConsistencyFailures = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_consistency_failures_total", Help: "Consistency probes where a replica's checksum diverged — replicated state corruption.",
	})
	Rebalances = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_rebalances_total", Help: "Replicas moved by automatic load rebalancing.",
	})
	LeaseSheds = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_lease_sheds_total", Help: "Leases transferred off overloaded nodes by the load rebalancer.",
	})
	ByteRebalances = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_byte_rebalances_total", Help: "Replicas moved by byte-weighted rebalancing.",
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
	FollowerReadFallbacks = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_follower_read_fallbacks_total", Help: "Stale-read sub-batches the gateway could not serve from its local replica (none held, or its closed timestamp lagged) and sent to the leader instead.",
	})
	DeadlockAborts = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_deadlock_aborts_total", Help: "Transactions aborted as chosen victims of detected deadlock cycles.",
	})
	ParallelCommits = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_parallel_commits_total", Help: "Transactions committed via the pipelined (STAGING) fast path.",
	})
	OnePhaseCommits = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_one_phase_commits_total", Help: "Transactions committed via the single-proposal one-phase fast path (no record, no intents).",
	})
	TxnRecoveries = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_txn_recoveries_total", Help: "Status recoveries run against STAGING transaction records.",
	})
	SQLRowsScanned = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_sql_rows_scanned_total", Help: "KV pairs fetched by SQL table and index scans (before filtering).",
	})
	StatsRefreshes = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_stats_refreshes_total", Help: "Table statistics collections completed (ANALYZE and the background sampler).",
	})
	RetentionRowsExpired = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_retention_rows_expired_total", Help: "MVCC versions expired by row-level retention on mixed ranges (keyed on the row's timestamp column).",
	})
	StatsRowsScanned = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_stats_rows_scanned_total", Help: "Rows swept by statistics collection.",
	})
	StorageBackpressure = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_storage_backpressure_total", Help: "Table-data writes shed with a retryable error while the engine was overloaded.",
	})
	RPCRoundTrip = promauto.With(Registry).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "datax_rpc_rtt_seconds",
		Help:    "Round-trip time of this node's periodic ping to each peer.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14), // 100µs .. ~0.8s
	}, []string{"peer"})
	ClockOffset = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "datax_clock_offset_seconds", Help: "Measured physical clock offset of each peer relative to this node (positive: the peer runs ahead); compare with --max-offset.",
	}, []string{"peer"})
	PeerReachable = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "datax_peer_reachable", Help: "1 while this node's last ping to the peer succeeded, 0 once one failed or timed out.",
	}, []string{"peer"})

	SQLConnections = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "datax_sql_connections", Help: "SQL connections by state: open (all), active (a statement in flight), idle_in_txn (idle inside an open transaction, holding its intents).",
	}, []string{"state"})
	SQLStatements = promauto.With(Registry).NewCounterVec(prometheus.CounterOpts{
		Name: "datax_sql_statements_total", Help: "Statements executed, by kind (select, insert, update, delete, copy, txn, ddl, other).",
	}, []string{"kind"})
	SQLStatementLatency = promauto.With(Registry).NewHistogram(prometheus.HistogramOpts{
		Name:    "datax_sql_statement_latency_seconds",
		Help:    "Statement latency as seen by the SQL server (parse to result).",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 18), // 100µs .. ~13s
	})
	SQLSerializationFailures = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_sql_serialization_failures_total", Help: "Statements that ended in SQLSTATE 40001 (the client must retry the transaction).",
	})
	SQLCopyRows = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_sql_copy_rows_total", Help: "Rows loaded through COPY FROM STDIN.",
	})

	TableRanges = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "datax_table_ranges", Help: "Ranges covering each table's key space (refreshed with the schema browser's cache).",
	}, []string{"table"})
	TableRows = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "datax_table_rows", Help: "Row count from each table's statistics, as of their collection.",
	}, []string{"table"})
	TableStatsAge = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "datax_table_stats_age_seconds", Help: "Age of each table's statistics; the planner falls back to structural estimates without them.",
	}, []string{"table"})

	StorageBackpressureCause = promauto.With(Registry).NewCounterVec(prometheus.CounterOpts{
		Name: "datax_storage_backpressure_cause_total",
		Help: "Table-data writes shed, by cause: leader (this node's engine gates), debt (this node's latched compaction-debt gate), follower (an overloaded quorum member's piggybacked verdict).",
	}, []string{"cause"})
	ReencryptionRewritten = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_reencryption_rewritten_bytes_total", Help: "Bytes of stale-key sstables rewritten under the active data key by background re-encryption passes.",
	})
	AuthFailures = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_auth_failures_total", Help: "Failed authentication attempts (SQL and HTTP).",
	})
	AdminDenied = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_admin_denied_total", Help: "Admin operations refused because the principal lacks the admin role.",
	})
)
