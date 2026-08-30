# Storage profiles, health metrics, and write backpressure

datax opens its Pebble store with a per-node tuning **profile**
(`--storage-profile`, default `balanced`).

## Profiles

- **balanced** (default): Pebble's own defaults, untouched — exactly the
  historical behavior, so the default can never regress an existing
  workload.
- **ingest**: tuned for sustained high-rate keyed writes:

  | Option | balanced (Pebble default) | ingest |
  |---|---|---|
  | MemTableSize | 4 MiB | 64 MiB |
  | MemTableStopWritesThreshold | 2 | 4 |
  | L0CompactionThreshold | 4 | 2 (drain L0 eagerly) |
  | L0StopWritesThreshold | 12 | 1000 |
  | LBaseMaxBytes | 64 MiB | 256 MiB |
  | MaxConcurrentCompactions | 1 | NumCPU/2, clamped to [2, 6] |
  | BytesPerSync | 512 KiB | 1 MiB |

  The rationale: fewer, larger flushes; eager L0 compaction with real
  concurrency (read amplification is secondary for ingest); and a hard
  write-stall ceiling raised far above the soft backpressure gate below,
  so datax sheds load with retryable errors long before Pebble would
  freeze every write on the store.

## Storage health metrics

Per node, on `/metrics` (`pkg/storage/health.go`; the snapshot behind
them refreshes at most once per second because `pebble.DB.Metrics()`
takes the DB mutex):

- `datax_storage_l0_files`, `datax_storage_l0_sublevels`
- `datax_storage_compaction_debt_bytes`
- `datax_storage_memtable_count` (Pebble's queue length — observability
  only; it idles above 1 and is not a backlog signal),
  and internally memtable *bytes*, which is
- `datax_storage_write_stalls_total` — Pebble hard stalls (should stay 0)
- `datax_storage_disk_slow_total`
- `datax_storage_backpressure_total` (process-wide) — writes shed by the
  gate below

## Backpressure instead of stalls

Each profile carries soft thresholds (ingest: L0 sublevels ≥ 20, L0
files ≥ 1500, memtable bytes ≥ 3 full memtables). When any is crossed —
or a Pebble write stall is actually in progress — the KV write path
rejects **user table-data writes** with a retryable
"storage overloaded" error (`StorageOverloadedError` + 40001-class
retry); `pkg/kvclient` retries them with jittered exponential backoff
(10ms → 1s), so an overloaded store sees its load thin out instead of a
hot retry storm.

Deliberately **not** gated:

- `/system` and `/meta` writes — liveness heartbeats, descriptor and
  range-metadata updates must keep flowing exactly when the store is
  struggling, or backpressure would trigger dead-node repair storms.
- Transaction-record operations (EndTxn, pushes, resolves) — intent
  cleanup is what un-wedges contended keys.
- GC batches — they are how the store digs itself out.

Known limitations: the gate reads only the **leader's** engine — an
overloaded follower simply lags raft (log growth, eventually a catch-up
snapshot); and compaction debt is exported but not gated on (no honest
threshold without more production data).

## WAL cost note (measurement first)

State-machine apply batches already commit **unsynced**
(`pkg/kvserver/apply.go` — durability comes from the raft log, which is
synced in `handleReady`), but they still pay WAL write bandwidth for
every applied command, roughly doubling ingest write amplification. A
WAL bypass for the state machine would need the raft state split into
its own synced store; deferred until the numbers below justify it.

## Numbers (16 workers, 100-row batches, 256 B values, single node, 60s)

| | balanced | ingest |
|---|---|---|
| ingest throughput | 7,952 rows/s, declining (8.6k→6.9k over the run) | 10,245 rows/s, steady |
| ingest batch p99 | 406 ms | 319 ms |
| hard write stalls | 0 | 0 |
| bench kv (mixed 95/5) | 13.8k ops/s, p99 3.8 ms | 15.7k ops/s, p99 3.2 ms |

The balanced profile's decline is compaction debt compounding — longer
runs widen the gap. Recorded runs live in issue #28.
