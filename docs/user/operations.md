# Operations

## The dashboard

Start nodes with `--http-listen` (or `demo -http-port`) and each node
serves, on that address:

- **`/`** — a self-contained web UI: node liveness, per-node leader/QPS/byte
  load, range table with replica placement, storage health. The range
  detail view shows the serving node's own replicas; there is no cross-node
  drill-down yet, so check each node's dashboard for its ranges.
- **`/metrics`** — Prometheus text format.
- **`/status`** — JSON: this node's identity, locality, and every range it
  holds (leader, applied index, size, QPS). `datax debug status --url ...`
  pretty-prints it.
- **`/api/cluster`** — JSON: the whole cluster as this node sees it (node
  liveness, heartbeat age, leader QPS/counts, replica bytes, hot ranges).

In secure mode all of it requires HTTP Basic credentials of any database
user, or a client certificate ([Security](security.md)).

## Metrics worth alerting on

Full list: scrape `/metrics`. The load-bearing ones:

| Metric | Alert when | Meaning |
|---|---|---|
| `datax_storage_backpressure_total` | increasing | writes are being shed; LSM can't keep up — see [Backpressure](#backpressure) |
| `datax_storage_write_stalls_total` | increasing at all | Pebble hard-stalled writes; you're past backpressure |
| `datax_storage_l0_sublevels` / `datax_storage_l0_files` | sustained ≥ 10 / ≥ 400 | compaction falling behind |
| `datax_storage_compaction_debt_bytes` | growing without bound | ingest exceeds compaction budget |
| `datax_storage_disk_slow_total` | increasing | disk latency spikes |
| `datax_txn_retries_total` vs `datax_txn_commits_total` | ratio ≫ a few % | heavy contention; look for missing `FOR UPDATE` or hot rows |
| `datax_deadlock_aborts_total` | increasing | lock cycles between transactions |
| `datax_dead_node_repairs_total` | any change | a node was declared dead and its replicas re-homed |
| `datax_ranges` vs `datax_range_leaders` per node | leaders very skewed | lease shedding isn't keeping up (check `datax_lease_sheds_total`) |

Also useful: `datax_kv_batch_latency_seconds` (histogram — p99 of the
replication path), `datax_follower_reads_total` (are your `AS OF` reads
actually staying local), `datax_parallel_commits_total`,
`datax_sql_rows_scanned_total` (a jump usually means a query lost its
index), `datax_auto_splits_total` / `datax_load_splits_total` /
`datax_range_merges_total`, `datax_gc_runs_total`.

## Everyday admin: `datax debug`

All subcommands talk to a running node (`--addr`, default
`127.0.0.1:26257`) except where noted.

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
(the built-in retry loop absorbs it). Sustained backpressure means the disk
can't take the write rate: switch heavy loaders to the `ingest`
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
