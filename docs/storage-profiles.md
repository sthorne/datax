# Storage profiles, health metrics, and write backpressure

datax opens its Pebble store with a per-node tuning **profile**
(`--storage-profile`, default `balanced`).

## Profiles

Both profiles share the read-path settings (issue #101): a block cache
sized from the machine's memory (balanced: 25 % capped at 8 GiB; ingest:
10 % capped at 2 GiB; `--cache-size` overrides; one cache per process,
shared by every engine, released when the last closes), bloom filters at
10 bits per key on every level (a missing-key point read — the
uniqueness probe and the intent lookup on every write — skips the
sstables that cannot hold the key; consulted for a user key, rather
than for one exact engine key, from cluster version v15 — see [Prefix
bloom filters](#prefix-bloom-filters)), `FormatMajorVersion` pinned at
`FormatVirtualSSTables` (16 — what every store is created at; the
engine is Pebble v2 since 0.41.0, whose newest is 24, so the pin is what
keeps a dependency bump from switching the on-disk format as a side
effect, issue #166: each format is its own cluster-version-gated step —
v14 ratchets a store to 19, columnar blocks, online; value separation
at 24 is not adopted yet; a store already at a higher format keeps it),
and `MaxOpenFiles` at half the descriptor limit (1000–16384). Block sizes and per-level compression are left at
Pebble's defaults for now — not measured in this pass; zstd on the
bottom level would trade CPU the write path needs for space.

- **balanced** (default): Pebble's own flush and compaction defaults —
  the historical shape, so the default never regresses an existing
  write workload.
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

## Prefix bloom filters

Pebble consults an sstable's bloom filter only on a prefix seek, and
only for the part of the key its comparer's `Split` returns. Until
cluster version v15 datax had no `Split`, so the filters #101
configured hashed whole engine keys and only the exact-key probes of
`Engine.Get` ever asked them; a version read of K — a seek to K's
metadata key, then to a version — walked every level's sstables (issue
#161). From v15 a store opens with the MVCC comparer
(`pkg/storage/comparer.go`): `Split` cuts an engine key at the
terminator of the user key's encoding in O(1) — no key-format change —
so K's metadata key and every version of K share one prefix, the point
reads of `MVCCGet`, the `Getter` and the write path's intent probe are
prefix seeks, and a filter rules out the sstables that hold nothing of
K at all. The filters of L6 are consulted too (Pebble skips them by
default); the store's bulk rests there and the reads that gain most are
misses.

The comparer keeps Pebble's default name and ordering (the name is
stamped in the MANIFEST and every table and promises the ordering,
which is unchanged). What `Split` leaves on disk is versioned by name:
the filter policy (`datax.mvcc-prefix-bloom`; a filter under another
name is not consulted, so a whole-key filter is never asked a prefix
question) and the columnar key schema (`datax.mvcc.v1`; the old schema
stays registered, and each schema's seeker splits keys with the
comparer it was built with, so tables from either mode read under
either). A store therefore switches with no rewrite and no downtime
beyond the restart, and the tables written before are rewritten in the
background by the same bounded manual compactions re-encryption uses
(`prefix_bloom_rewrite` in the node document,
`datax_prefix_bloom_remaining_bytes`). `datax_storage_bloom_hits_total`
and `_misses_total` count, from then on, the point reads the filters
answered and the ones they had to admit.

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
- `datax_storage_debt_gate` (0/1) and
  `datax_storage_debt_gate_entered_total` — the compaction-debt gate's
  latch state and entry count
- `datax_storage_backpressure_total` (process-wide) — writes shed by the
  gate below; `datax_storage_backpressure_cause_total{cause=leader|debt|
  follower}` says which limit was hit

## Backpressure instead of stalls

Each profile carries soft thresholds (ingest: L0 sublevels ≥ 20, L0
files ≥ 1500, memtable bytes ≥ 3 full memtables). When any is crossed —
or a Pebble write stall is actually in progress — the KV write path
rejects **user table-data writes** with a retryable
"storage overloaded" error (`StorageOverloadedError` + 40001-class
retry); `pkg/kvclient` retries them with jittered exponential backoff
(10ms → 1s), so an overloaded store sees its load thin out instead of a
hot retry storm.

Two further gates ride the same shed path (the per-cause counter says
which fired):

- **Compaction debt** (`cause=debt`): sustained ingest can keep L0
  shallow while compaction debt compounds without bound. The gate
  latches when `datax_storage_compaction_debt_bytes` crosses the
  profile's high water (balanced 2 GiB, ingest 8 GiB) and — hysteresis,
  so it cannot flap on every compaction — releases only below the low
  water (half the high). While latched, table-data writes shed exactly
  like an L0 trip.
- **Quorum health** (`cause=follower`): every node piggybacks its own
  soft-gate verdict on its outgoing raft envelopes
  (`rpcpb.StorageHealth`), so leaders know each replica-set member's
  health with no extra traffic. A leader sheds a range's table-data
  writes when ANY member of that range's replica set is overloaded —
  an overloaded follower otherwise lags raft silently until it needs a
  catch-up snapshot, or quietly rides the range one node from quorum
  loss. Verdicts older than 5s (or absent — old binaries, silent peers)
  read as healthy: a node that stops talking is the liveness system's
  problem, and stale shedding must not wedge writes after it recovers.

Deliberately **not** gated:

- `/system` and `/meta` writes — liveness heartbeats, descriptor and
  range-metadata updates must keep flowing exactly when the store is
  struggling, or backpressure would trigger dead-node repair storms.
- Transaction-record operations (EndTxn, pushes, resolves) — intent
  cleanup is what un-wedges contended keys.
- GC batches — they are how the store digs itself out.

Known limitation: the debt thresholds are first-cut constants (chosen
conservatively — well past where a healthy store's debt cycles); tune
with production data.

## WAL cost note (resolved by the split store)

State-machine apply batches commit **unsynced** (`pkg/kvserver/apply.go`
— durability comes from the raft log, which the scheduler syncs), but
on a single-engine store they still paid WAL write bandwidth for every
applied command, roughly doubling ingest write amplification. From
cluster version v13 the store is split (issue #105): the raft log has an
engine of its own and the state engine runs with `DisableWAL`, replaying
the log after a crash (see docs/replication-and-placement.md, "Raft log
truncation", for the durability model). The raft engine keeps a small
memtable (16 MiB) since the log is appended and truncated, never read
in bulk. `datax_storage_bytes_written_total{engine,kind}` shows each
engine's WAL, flush and compaction bytes.

## Numbers (16 workers, 100-row batches, 256 B values, single node, 60s)

| | balanced | ingest |
|---|---|---|
| ingest throughput | 7,952 rows/s, declining (8.6k→6.9k over the run) | 10,245 rows/s, steady |
| ingest batch p99 | 406 ms | 319 ms |
| hard write stalls | 0 | 0 |
| bench kv (mixed 95/5) | 13.8k ops/s, p99 3.8 ms | 15.7k ops/s, p99 3.2 ms |

The balanced profile's decline is compaction debt compounding — longer
runs widen the gap. Recorded runs live in issue #28.
