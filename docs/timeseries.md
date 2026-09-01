# Time-series tables

A table created `WITH (timeseries = true, ...)` is optimized for the
time-series shape: append-heavy inserts whose primary key ends in a
timestamp, windowed per-series reads, and age-based expiry.

```sql
CREATE TABLE metrics (
  series TEXT,
  ts     TIMESTAMPTZ,
  val    FLOAT8,
  PRIMARY KEY (series, ts)
) WITH (timeseries = true, retention = '7d', shards = 8);
```

Options (all require `timeseries = true`; the last primary-key column
must be `TIMESTAMPTZ`):

- `retention = '<N><d|h|m|s>'` — rows older than this are expired by the
  GC housekeeping pass, with zero SQL DELETEs.
- `shards = N` (2–256) — spread the write hot tail over N key prefixes.
  Changeable later with the online re-shard (`ALTER TABLE ... SET
  (shards = M)`, below).

Pair a timeseries node with `--storage-profile ingest`
(docs/storage-profiles.md) for sustained append throughput.

## Shard buckets: spreading the hot tail

A monotone `(series, ts)` tail concentrates every insert on the last
range — one raft group serializes the whole table's write load. With
`shards = N` the table gets a hidden `_shard INT` column **leading** the
primary key:

- `_shard = fnv32a(key-encoding of the logical PK values) % N`, computed
  by the executor on insert. **The hash, its key-encoding input, the
  column order, and the mod are frozen on-disk format**
  (`pkg/sql/rowenc/shard.go`); changing any of them would strand every
  existing sharded table.
- The column is invisible: `SELECT *` doesn't return it, `INSERT` can't
  target it, `UPDATE` can't touch it (it's a PK column).
- CREATE pre-splits the table span at each bucket prefix and at the
  table's edges, so ingest parallelizes across ranges immediately (the
  edge splits also make "range fully inside the table" the common case
  for retention GC). The splits are best-effort and outlive an aborted
  enclosing transaction — harmless empty ranges the merger re-absorbs.

The planner plans against the **logical** primary key:

- All logical PK columns pinned by `=` → the bucket is recomputed from
  the pinned values and the lookup stays a **single point read**.
- A pinned prefix or time window → the scan runs once per bucket
  (`EXPLAIN` shows `(fan-out over N shard buckets)`) and `LIMIT`
  re-applies globally. Each bucket's scan is in logical-PK order, so an
  `ORDER BY` on the logical key (equality-pinned columns skipped) is
  delivered by a **K-way merge** of the per-bucket scans — no in-memory
  sort — with `LIMIT n` stopping after at most `buckets × n` scanned
  rows. `ORDER BY ts DESC LIMIT n` (the dashboard query) rides
  per-bucket **reverse scans** through the same merge: measured 1.23 ms
  vs 27.9 ms for the full-scan-and-sort it replaces on a 5,000-row
  series (22×; the gap grows with table size). `EXPLAIN` says
  `order satisfied by K-way merge across shard buckets [(reverse
  scans)]`. Reverse scans need cluster version v3; below it (a
  not-yet-finalized upgrade) descending orders fall back to the
  in-memory sort.

## Retention

Per-table retention rides the GC machinery with one twist: ranges that
lie entirely inside a retention table's span run in **expiry** mode —
every version at or below the threshold is collected, *including* the
newest one. (Ordinary MVCC GC keeps that "survivor" version, which would
mean a never-updated row never expires.)

- The threshold ratchets forward on each housekeeping tick; reads below
  it are rejected (`AS OF SYSTEM TIME` past the retention fails cleanly
  rather than returning silently-missing rows).
- A range that only **partially** overlaps a retention table (it also
  holds other data) never expires rows and is never GC'd earlier than
  `max(default TTL, every overlapping retention)` — mixed ranges never
  delete early.
- The automatic range merger skips merges whose two sides have different
  retention policies, so a long-retention range never absorbs a
  short-retention neighbor's threshold.
- The node maps ranges to retentions by scanning table descriptors,
  cached ~30s and served stale on errors — a new table's retention takes
  effect within a tick or two.

Caveat: expiry is a replicated GC command, not a transactional DELETE. A
transaction that reads the same aged-out window twice, straddling the GC
tick, can see rows disappear between statements. Retention windows are
normally days; treat the boundary as fuzzy by one housekeeping interval.

## Numbers (single node, ingest profile, 16 workers, 100-row batches, 60s)

| | unsharded | shards = 8 |
|---|---|---|
| insert throughput | 10,238 rows/s | 12,177 rows/s (+19%) |
| insert batch p99 | 291 ms | 260 ms |
| 60s-window read (1 series, ~60 rows) p50 | 418 µs | 929 µs |
| rows scanned for 200 windowed reads | exactly the window (12,000) | exactly the window (12,000) |

The write win comes from spreading the hot tail across 8 ranges — on a
multi-node cluster the unsharded table's single hot range also pins the
whole write load to one leader node, so the gap widens with cluster
size. The read cost is the fan-out (8 spans instead of 1); the scan
bounds stay tight either way. Recorded runs live in issue #29.

Benchmark with `datax bench timeseries [--shards N --series M]` — it
appends monotone per-series timestamps (deliberately the worst case for
an unsharded PK) and then times windowed reads.

## Re-sharding

`ALTER TABLE t SET (shards = M)` changes the bucket count **online**, on
the same state machine as online CREATE INDEX. Primary rows live at an
index ID; the new layout is built at a freshly allocated ID (IDs are
never reused, so the keyspaces cannot collide):

1. A `Reshard` marker publishes + lease-drains — every gateway then
   dual-writes both layouts.
2. The new layout's bucket prefixes are pre-split.
3. A frozen-timestamp sweep re-keys the old rows in parallel chunked
   transactions (recompute the bucket mod M, re-encode at the new
   index; value bytes copy verbatim — values hold only non-PK columns).
   Concurrent dual-writes are idempotent against it; concurrent deletes
   invalidate the chunk's read and force a rescan.
4. One transaction swaps `ShardBuckets` + the live primary index,
   clears the marker, and stamps `ReshardedAt`; after the drain, reads,
   writes, and the planner's fan-out all follow the new layout.
5. The old layout is wiped (batched, intent-aware; emptied ranges merge
   away).

Secondary indexes ride the same machinery: their entries embed the shard
bucket in the primary-key suffix they point back with, so each index is
rebuilt at a freshly allocated shadow ID — dual-writes mirror every
entry mutation with the bucket recomputed, the backfill emits shadow
entries from the same decoded rows, and the swap adopts all the shadow
IDs together with the primary layout (uniqueness stays enforced on the
live copy throughout). A re-shard and an online CREATE INDEX exclude
each other: whichever is in flight, the other is refused with SQLSTATE
`25001`.

Measured (single node, ingest profile): idle backfill ~8,800 rows/s
(260k rows in 30s); under a live 4,000 rows/s ingest the re-shard of a
growing 120k→400k-row table took ~129s with foreground throughput
dipping to ~2,200 rows/s (dual-write amplification) and recovering to
full speed on the new buckets the moment the swap landed — writes never
stopped. Recorded runs live in issue #33.

**Historical reads work across the swap.** The superseded layout is not
wiped at the swap: it is recorded in the descriptor (`RetiredLayouts`)
and stays on disk. An `AS OF SYSTEM TIME` below the swap reads the
descriptor as of its own timestamp (descriptors are ordinary MVCC
values), plans against the pre-swap layout — old bucket count, old
primary index, old index generations — and returns exactly the data
committed then. A background janitor (range-1 leader; keep window
`ReshardRetireFor`, default the GC TTL — the deepest timestamp a
historical read can use anyway) removes the descriptor entry first and
then wipes the retired keyspaces; from that point a historical read
below the swap is refused with a clear error instead of coming back
short. Historical catalog lookups bypass the gateway's descriptor cache
and take no lease, so they can never publish a backdated version.

Scope: already-sharded timeseries tables only (the PK column list
must stay identical so both layouts decode during dual-write); a handful
of old-layout keys from statements in flight at the swap can survive the
eventual wipe as unreachable garbage.

## Limitations

- Retention expiry is range-granular and best-effort in timing (one
  housekeeping tick, default 30s, plus the ~30s descriptor cache).
- Secondary indexes on sharded tables are unsharded (their own hot-tail
  characteristics apply).
