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
	RaftSchedulerLatency = promauto.With(Registry).NewHistogram(prometheus.HistogramOpts{
		Name:    "datax_raft_scheduler_latency_seconds",
		Help:    "Time a replica waited in the store's raft scheduler queue before a worker picked it up.",
		Buckets: prometheus.ExponentialBuckets(0.00001, 4, 10), // 10µs .. ~2.6s
	})
	RaftReadyPasses = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_raft_ready_passes_total", Help: "Raft Readies (one replica's persist, send and apply) handled by the store's scheduler workers.",
	})
	RaftDeferredTruncations = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_raft_deferred_truncations_total", Help: "Log truncations performed on the raft engine once the state engine had flushed past them (split stores).",
	})
	RaftTruncationFlushes = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_raft_truncation_flushes_total", Help: "State-engine flushes the housekeeping tick forced so that a log truncation pending past its bound could proceed (split stores).",
	})
	RaftReplayedEntries = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_raft_replayed_entries_total", Help: "Committed log entries re-applied at startup because the state engine had not flushed them before the last shutdown.",
	})
	RaftLogSyncs = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_raft_log_syncs_total", Help: "Synced commits of raft log entries and HardStates: one per scheduler pass, however many replicas the pass grouped.",
	})
	RaftReadiesPerSync = promauto.With(Registry).NewHistogram(prometheus.HistogramOpts{
		Name:    "datax_raft_readies_per_sync",
		Help:    "Replicas whose raft log writes shared one synced commit.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
	})
	RaftHeartbeatsCoalesced = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_raft_heartbeats_coalesced_total", Help: "Raft heartbeats and responses carried inside coalesced per-node envelopes (cluster v12).",
	})
	RaftHeartbeatEnvelopes = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_raft_heartbeat_envelopes_total", Help: "Coalesced heartbeat envelopes sent (one per peer node per scheduler pass with heartbeats pending).",
	})
	RaftQuiesces = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_raft_quiesces_total", Help: "Replicas that went quiescent (stopped ticking and heartbeating while idle).",
	})
	RaftUnquiesces = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_raft_unquiesces_total", Help: "Quiescent replicas woken by a message, a proposal or a request.",
	})
	ClosedTimestampSideUpdates = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_closed_timestamp_side_updates_total", Help: "Per-range closed timestamps published off the raft log (an awake range whose log did not grow, or a quiescent range's registration with a follower).",
	})
	ClosedTimestampGroupUpdates = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_closed_timestamp_group_updates_total", Help: "Group closed-timestamp promises sent (one per follower node per publication round, covering every quiescent range registered there).",
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

	HealthProblems = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "datax_health_problems", Help: "Problems the dashboard's health panel currently lists, by severity and check (refreshed with the panel, at most every 3s).",
	}, []string{"severity", "check"})

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
	SQLStreamedRows = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_sql_streamed_rows_total", Help: "Result rows delivered by streaming SELECTs (pulled from KV page by page as the client reads).",
	})
	SQLStreamRestarts = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_sql_stream_restarts_total", Help: "Streaming SELECTs re-run from the start after a retryable error before any row reached the client.",
	})
	SQLMemoryLimitHits = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_sql_memory_limit_hits_total", Help: "Statements refused with 53200 for exceeding statement_memory_limit.",
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
	MetricsRecordRows = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_metrics_record_rows_total", Help: "Rows this node has written to the datax_metrics table.",
	})
	MetricsRecordSkipped = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_metrics_record_skipped_total", Help: "Metrics-recorder ticks skipped because this node's store was shedding writes.",
	})
	MetricsRecordErrors = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "datax_metrics_record_errors_total", Help: "Metrics-recorder ticks whose write failed (retried next tick).",
	})
)
