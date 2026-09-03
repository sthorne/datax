# Operations

## The dashboard

Start nodes with `--http-listen` (or `demo -http-port`) and each node
serves, on that address:

- **`/`** — a self-contained web UI: node liveness, per-node leader/QPS/byte
  load, range tables with replica placement, storage health. The cluster
  ranges table drills down: clicking a range fetches every holding node's
  view of it (leader, applied index, size, QPS, closed timestamp) over
  internode RPC, so any node's dashboard can inspect any range.
- **`/metrics`** — Prometheus text format.
- **`/status`** — JSON: this node's identity, locality, and every range it
  holds (leader, applied index, size, QPS, closed timestamp — the newest
  fixed timestamp the replica can serve follower reads at).
  `datax debug status --url ...` pretty-prints it.
- **`/api/cluster`** — JSON: the whole cluster as this node sees it (node
  liveness, heartbeat age, leader QPS/counts, replica bytes, hot ranges).
- **`/api/range?id=N`** — JSON: the cross-node drill-down document behind
  the range detail view. Admin role required in secure mode.

In secure mode all of it requires HTTP Basic credentials of any database
user, or a client certificate; `/api/range` additionally requires the
admin role ([Security](security.md)).

## Metrics worth alerting on

Full list: scrape `/metrics`. The load-bearing ones:

| Metric | Alert when | Meaning |
|---|---|---|
| `datax_storage_backpressure_total` | increasing | writes are being shed — `datax_storage_backpressure_cause_total{cause=leader\|debt\|follower}` says which limit; see [Backpressure](#backpressure) |
| `datax_storage_write_stalls_total` | increasing at all | Pebble hard-stalled writes; you're past backpressure |
| `datax_storage_l0_sublevels` / `datax_storage_l0_files` | sustained ≥ 10 / ≥ 400 | compaction falling behind |
| `datax_storage_compaction_debt_bytes` | growing without bound | ingest exceeds compaction budget |
| `datax_storage_debt_gate` | 1 for long stretches | the compaction-debt gate is latched (writes shed with `cause=debt` until debt halves) |
| `datax_storage_disk_slow_total` | increasing | disk latency spikes |
| `datax_txn_retries_total` vs `datax_txn_commits_total` | ratio ≫ a few % | heavy contention; look for missing `FOR UPDATE` or hot rows |
| `datax_deadlock_aborts_total` | increasing | lock cycles between transactions |
| `datax_dead_node_repairs_total` | any change | a node was declared dead and its replicas re-homed |
| `datax_consistency_failures_total` | **any change — page someone** | a replica's checksum diverged: replicated-state corruption (requires the sweep: `--consistency-interval`) |
| `datax_ranges` vs `datax_range_leaders` per node | leaders very skewed | lease shedding isn't keeping up (check `datax_lease_sheds_total`) |

Also useful: `datax_kv_batch_latency_seconds` (histogram — p99 of the
replication path), `datax_follower_reads_total` vs
`datax_follower_read_fallbacks_total` (are your `AS OF` /
`with_max_staleness` reads actually staying local, or falling back to
leaders), `datax_parallel_commits_total`,
`datax_sql_rows_scanned_total` (a jump usually means a query lost its
index), `datax_stats_refreshes_total` / `datax_stats_rows_scanned_total`
(the table-statistics sampler's pace and cost),
`datax_retention_rows_expired_total` (row-level retention expiry on
mixed ranges),
`datax_reencryption_remaining_bytes` (encrypted stores: live sstable
bytes still under retired data keys — 0 attests re-encryption is
complete),
`datax_auto_splits_total` / `datax_load_splits_total` /
`datax_range_merges_total`, `datax_gc_runs_total`.

## Everyday admin: `datax debug`

All subcommands talk to a running node (`--addr`, default
`127.0.0.1:26257`) except where noted. Against a **secure** cluster add
`--certs-dir` (and `--user`, default `root`) to present a client
certificate; state-changing subcommands require the admin role, and each
one lands in the node's audit log with the acting principal
([Security](security.md#admin-rpcs-in-secure-mode)).

```sh
datax debug nodes                      # liveness, locality, last heartbeat
datax debug ranges                     # every range: span, replicas, leader
datax debug status --url http://127.0.0.1:8080/status

datax debug split --table 100          # manual split (also: --key <hex>)
datax debug merge --key <hex>          # manual merge
datax debug transfer-lease --range 5 --node 2
datax debug rebalance --range 5 [--from 1]
```

Splits and merges also happen automatically (by size: 64 MiB; by load:
sustained 500 QPS); the manual commands are for pre-splitting before a bulk
load and for tests.

## Consistency checking

Start nodes with `--consistency-interval 10m` (off by default) and each
node periodically checksums one range it leads across all its replicas —
a background tripwire for silent corruption. A divergence logs every
replica's digest (`CONSISTENCY FAILURE` in the node log) and increments
`datax_consistency_failures_total`; alert on it. The check reads the
whole range, so pick an interval that spreads the IO — at `10m` a node
leading 100 ranges re-verifies everything roughly every 17 hours.

## Backup and restore

`datax backup` asks a node to write a **consistent** backup of the whole
cluster — every table (data and indexes, raw), descriptors with their
grants, and users — captured at a single timestamp, correct even under
concurrent write load:

```sh
datax backup  --addr 10.0.0.1:26257 --dest /backups/2026-08-31
datax backup  --addr 10.0.0.1:26257 --dest /backups/2026-08-31-noon \
              --base /backups/2026-08-31          # incremental since the base
```

- Paths are on the **serving node's** filesystem (a mounted shared
  filesystem makes them portable).
- A backup directory holds a `BACKUP.json` manifest plus one data file per
  table; the manifest is written last, so its presence marks the backup
  complete. The summary prints per-table record counts and SHA-256
  checksums over the live data.
- Incrementals capture only keys changed since the base — deletions
  included — and must chain within the MVCC GC window (25h by default): an
  older base is refused with "incremental base too old". Take a fresh full
  backup at least daily.
- On an encrypted store, backup files are written in plaintext; the
  command refuses unless you pass `--allow-plaintext`. Protect the backup
  location accordingly.

`datax restore` applies a chain (full first, then incrementals in order)
into an **empty** cluster — it refuses if any table exists:

```sh
datax restore --addr 10.0.1.1:26257 --src /backups/2026-08-31,/backups/2026-08-31-noon
```

Table IDs are preserved, so secondary indexes and timeseries shard layouts
restore as-is with no backfill; the descriptor ID generator is bumped past
every restored ID. Users, admin-role markers, and per-table grants come
from the backup — after a restore, **credentials are the source cluster's**.
The restore summary re-exports every table and prints fresh checksums:
compare them against the backup's to verify the restore byte-for-byte.

MVCC history is not preserved (rows are rewritten at restore time), so
`AS OF SYSTEM TIME` cannot see below the restore.

## Rolling upgrades

A cluster upgrades with no downtime, one node at a time. Each binary has a
protocol version (shown at startup and in `datax debug nodes`); a cluster
has a **finalized cluster version** that only moves when you say so.
Adjacent versions only: upgrade one major version at a time.

```sh
# For each node, one at a time:
#   1. stop the node (optionally decommission-drain first for zero blips)
#   2. restart it on the new binary
#   3. wait until `datax debug nodes` shows it heartbeating with the new
#      version and the cluster is healthy
# Then, once EVERY node runs the new binary:
datax debug upgrade                # finalize; refuses while old nodes remain
datax debug nodes                  # shows: cluster version: v4
```

Rules of the road:

- **Before finalize** you can freely roll a node back to the old binary —
  mixed-version clusters are supported for the duration of the roll.
- **After finalize there is no downgrade.** A node restarted on an older
  binary refuses to start ("downgrading a node after the cluster upgrade
  was finalized is not supported"); a too-old binary is also refused at
  join time.
- Finalize is deliberate, not automatic: verify the upgraded cluster looks
  healthy first, because finalize is the point of no return.
- `datax debug upgrade` names any node still on the old binary instead of
  finalizing — nothing to time or coordinate.

## Decommissioning a node

```sh
datax debug decommission --node 3          # drain: move all replicas off n3
# watch: datax debug nodes / datax_decommission_replicas_moved_total
datax debug decommission --node 3 --cancel # changed your mind
```

Once `debug ranges` shows no replicas on the node, stop the process.
Keep at least 3 live nodes (or ranges can't hold 3 replicas).

## When a node dies

Nothing to do for the data: after a liveness timeout the cluster declares
the node dead and re-replicates its ranges onto survivors
(`datax_dead_node_repairs_total` ticks). Restarting the node with its data
directory intact rejoins it as itself — even on a new address.

**Quorum loss** (2 of 3 replicas gone) makes affected ranges unavailable.
The last resort, on a **stopped** surviving node, discards the lost
replicas and rewrites the survivor as a 1-replica group:

```sh
datax debug unsafe-recover --dir /var/lib/datax --yes    # [--range N]
```

This can lose recently committed writes on the dead replicas — hence
"unsafe". `datax debug metadata --dir ...` inspects a store's metadata
(works on encrypted stores with `--enc-key`).

## Backpressure

When the LSM falls behind (L0 too deep, memtables full), nodes shed
table-data writes with a retryable storage-overload error rather than
letting the engine hard-stall; clients see higher latency, not failures
(the built-in retry loop absorbs it). Three limits feed the same shed
path, told apart by `datax_storage_backpressure_cause_total`:

- **`cause=leader`** — the leaseholder's own engine crossed its
  profile's L0/memtable thresholds (or Pebble is mid-stall).
- **`cause=debt`** — the leaseholder's compaction debt latched above the
  profile's high water (`datax_storage_debt_gate` = 1); it releases only
  once debt halves, so sustained ingest cannot outrun compaction
  indefinitely.
- **`cause=follower`** — some OTHER member of the range's replica set is
  overloaded. Nodes piggyback their health verdict on raft traffic, and
  leaders shed rather than let a sick follower lag raft without bound
  (unbounded lag ends in catch-up snapshots, or one more failure away
  from quorum loss). The error names the node; check that node's
  storage tiles.

Sustained backpressure means a disk can't take the write rate: switch
heavy loaders to the `ingest`
[storage profile](deployment.md#storage-profiles), slow the load, or add
nodes. Retention GC and re-shard backfills compete for the same LSM budget
— schedule bulk loads away from them.

## Benchmarking

`datax bench` drives a running cluster over pgwire:

```sh
datax bench kv --url ... --concurrency 16 --read-pct 95 --duration 30s
datax bench bank [--for-update]           # contended transfers
datax bench ingest --batch 100 --payload-bytes 256 [--rate N] [--metrics-url ...]
datax bench timeseries --series 1000 --shards 8
```

`ingest` writes random keys (LSM stress); `timeseries` writes per-series
monotone timestamps — the hot-tail shape — and is the honest way to compare
`--shards` settings.

Two counters show which commit fast path your workload rides:
`datax_one_phase_commits_total` (single-range implicit transactions — one
raft proposal, the cheapest commit) and `datax_parallel_commits_total`
(multi-range pipelined commits). A write-heavy workload where neither
moves is paying the classic two-round commit — usually explicit
`BEGIN` blocks.

## Capacity planning

Rules of thumb, from measured single-node numbers (NVMe, 100-row batches):

- **A single range sustains roughly 8–10k inserted rows/s.** Every write
  in a range goes through one raft group and one fsync'd log.
- **A sequential primary key caps the whole table at that single-range
  rate**, no matter the cluster size — new keys always land in the last
  range. Use UUID keys or a [timeseries table](sql.md#timeseries-tables)
  (measured: +19% at `shards=8` even on one node; the real win is spreading
  shards across nodes).
- **Secondary indexes multiply write cost**: each row insert writes every
  index too, and each **unique** index adds a read-before-write check.
  Unique-index-heavy tables ingest at point-read speed, not write speed.
- **Reads scale with replicas** for historical (`AS OF SYSTEM TIME`)
  queries, which any replica can serve; current reads go to leaseholders,
  which the load balancer spreads by QPS.
- Budget disk at **~2× the logical write rate** (raft log + LSM) plus
  compaction; when in doubt run `datax bench ingest --metrics-url ...`
  against a staging cluster and watch the storage metrics it reports.
