# Operations

## The dashboard

Start nodes with `--http-listen` (or `demo -http-port`) and each node
serves, on that address:

- **`/`** — a self-contained web UI: node liveness, per-node leader/QPS/byte
  load, each node's host figures (CPU, load, memory, free space on the
  store's disk, file descriptors — colored when they deserve a look), the
  serving node's full machine picture (disk and network throughput, Go
  runtime, uptime), a network matrix (every node's round trip to every
  other, with clock offsets judged against `--max-offset`), a schema
  browser (every table with its columns, primary key, indexes,
  time-series options, grants, statistics and their age, and range
  footprint; the users for admins; a filter box that narrows the tables
  and both range lists, which name the table each range belongs to), SQL
  activity (connections by state with the oldest idle transaction,
  statements per second by kind, `40001` rate, latency percentiles per
  node; for admins the serving node's statements in flight and its
  slowest recent ones), a problems panel at the top (every finding of
  the health checks below, colored by severity, each linking to the
  section that shows the figure; a green line when there are none), an
  events feed (the serving node's recent splits, merges, rebalances,
  lease sheds, repairs, snapshots, backups, upgrades, key rotations and
  consistency failures, newest first, with a kind filter; admins also
  see the audit stream), range tables with replica placement, storage
  health, and a **Metrics view** (`/#/metrics`) that charts any of the
  series the cluster records about itself over the last 15 minutes to 7
  days, one chart per series with one line per node, from the
  `datax_metrics` table described under [Metrics history](#metrics-history);
  every tile on the overview links to its own series charted, and the
  tiles' sparklines are the last 15 minutes from that table. Clicking a
  node in the Nodes table opens its **detail page** (`/#/node/N`):
  identity (address, locality, release and protocol version, the
  cluster version it has mirrored, uptime), its machine tiles, its
  last 15 minutes of CPU, QPS, statements and KV latency, storage
  (engine figures, the debt gate, the overload verdict, encryption and
  re-encryption progress), the replicas it holds with their raft log
  depth, its SQL summary (and, for admins, its statements in flight and
  slow statements), its row of the network matrix, its settings, and
  its recent events. The cluster
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
  A node that cannot reach the meta range (a partition) still answers
  within a couple of seconds, with the last range list it fetched and an
  `error` saying how old it is.
- **`/api/range?id=N`** — JSON: the cross-node drill-down document behind
  the range detail view. Admin role required in secure mode.
- **`/api/activity`** — JSON: this node's SQL connections, statements in
  flight, and the slow-statement ring (past `--slow-statement-threshold`,
  default 500 ms). Admin role required in secure mode; statement text can
  carry data.
- **`/api/health`** — JSON: the problems panel's document: the findings
  of the health checks (see [Health checks](#health-checks)), sorted
  critical first, and how many checks ran. Empty `problems` with a
  non-zero `checks` count means green. Recomputed at most every 3 s.
- **`/api/events?since=N&limit=M`** — JSON: the serving node's recent
  operational events (the last 500 are kept in memory; `since` returns
  only those after sequence `N`, which is how the dashboard tails). In
  secure mode audit records (authentication failures, admin operations,
  privilege DDL) are included only for the admin role.
- **`/api/node?id=N`** — JSON: the node detail page's document. The
  serving node answers for itself to any authenticated user; another
  node's document is fetched from that node over the internode RPC and
  needs the admin role (403 otherwise). Statement text and audit
  events are included only for admins.
- **`/api/metrics`** — JSON: without parameters, the catalog of recorded
  series and the label values this node knows; with
  `?series=a,b&node=1,2&since=1h&step=30s&rate=1`, aligned `[t, v]`
  arrays per node from the `datax_metrics` table, downsampled to at most
  500 points per series (the average for gauges; with `rate=1`, the
  per-second rate for counters). `from`/`to` in unix milliseconds
  replace `since`. Any database user may read it, the same rule as
  `/metrics`. An unknown series is a 404; before the cluster has
  finalized v5 it is a 503.
- **`/api/schema`** — JSON: the schema browser's document. In secure mode
  root and admins see every table and the user list; another user sees
  the tables it holds a grant on. Rebuilt at most every 5 s per node.

In secure mode all of it requires HTTP Basic credentials of any database
user, or a client certificate; `/api/range`, `/api/activity` and
another node's `/api/node` additionally require the admin role ([Security](security.md)). The dashboard header shows who it
is signed in as and how (`signed in as ops (basic)`, with an **admin**
badge when the role is held); without the role the cluster ranges are
not clickable and the note under them says which user is signed in and
how to proceed (`GRANT ADMIN TO ops`, or sign in as `root` from a private
window, since browsers cache Basic credentials per site). `/api/cluster`
carries the same identity in its `principal` field.

## Health checks

Every node runs the same set of checks against data it already holds
(the node registry, the `/meta` range list, its own store, the peer
pinger, the schema cache) and publishes the findings on the dashboard,
in `/api/health`, and as the gauge `datax_health_problems{severity,check}`
(one series per finding; a check that finds nothing has no series).
Alert on `datax_health_problems{severity="critical"} > 0` for the
page-worthy ones and on `severity="warning"` for the rest, and the panel
on any node's dashboard says what and where. The checks, with the
section the dashboard row links to:

| Check | Severity | Fires when |
|---|---|---|
| `node-down` | critical | a node's heartbeat is older than the dead-node threshold (30 s); its replicas are being repaired away (nodes) |
| `node-unresponsive` | warning | a node's heartbeat is older than the liveness grace (15 s) but not yet the dead-node threshold (nodes) |
| `node-draining` | info | a decommission is in progress (nodes) |
| `mixed-binaries` | warning | nodes run different binary versions (nodes) |
| `upgrade-unfinalized` | warning | every node runs a newer binary than the cluster version; `datax admin upgrade` has not been run (nodes) |
| `quorum-lost` | critical | a range has fewer live replicas than a majority; it cannot serve until a node returns (ranges) |
| `under-replicated` | warning | a range has fewer live replicas than its replication factor (ranges) |
| `diversity` | warning | a range's replicas share a locality tier they could spread across (ranges) |
| `meta-unavailable` | critical | the serving node cannot read the `/meta` range list, so it cannot route (ranges) |
| `backpressure`, `debt-gate`, `write-stalls`, `storage-errors` | warning / critical | the storage counters in the table below are moving over the last five minutes (storage) |
| `follower-overloaded` | warning | a node reports itself overloaded and writes to the ranges it replicates are being shed (storage) |
| `disk-low`, `disk-full` | warning / critical | a node's store has under 15% / 5% of its disk free (nodes) |
| `fd-limit` | warning | a node holds over 80% of its file-descriptor limit (nodes) |
| `memory-low` | warning | a node's host has under 10% of its memory available (nodes) |
| `peer-unreachable` | warning | the serving node's pings to a peer fail: a partition, a firewall, or the peer is down, in which case its heartbeat row says so (network) |
| `clock-offset` | warning / critical | a peer's clock is past half of / past `--max-offset` (network) |
| `consistency-failure` | critical | a consistency sweep found diverged replica checksums since this node started; the events feed carries the range (events) |
| `auth-failures` | warning | more than one authentication or admin-authorization failure per second over the last five minutes (events) |
| `stale-statistics` | info | tables with over a thousand rows have statistics older than an hour (schema) |

## Metrics history

The cluster keeps its own metrics in a table, so history survives
restarts, is queryable with plain SQL from any client, and the
dashboard charts it from one place. Every node writes one batch of
rows every `--metrics-record-interval` (default 10 s; `0` disables
recording on that node) into `datax_metrics`, a sharded time-series
table the nodes create themselves once the cluster has finalized v5
(clusters upgraded from an earlier version need `datax debug upgrade`
first):

```sql
CREATE TABLE datax_metrics (
  node  INT8 NOT NULL,        -- 0 for cluster-level series (table sizes)
  name  TEXT NOT NULL,        -- 'node.cpu_percent', 'rpc.rtt_us{peer=n2}', ...
  at    TIMESTAMPTZ NOT NULL, -- the node's clock, truncated to the interval
  value FLOAT8,
  PRIMARY KEY (node, name, at)
) WITH (timeseries = true, retention = '7d', shards = 8);
```

The series: host figures (`node.cpu_percent`, `node.load1`,
`node.mem_available`, `node.rss`, `store.disk_free`, disk and network
throughput, `node.open_fds`, `go.goroutines`, `go.heap_in_use`),
storage (`store.l0_files`, `store.l0_sublevels`, `store.compaction_debt`,
`store.memtable_bytes`, `store.write_stalls`, `store.debt_gated`,
`storage.backpressure`), ranges (`node.ranges`, `node.leaders`,
`node.leader_qps`, `node.replica_bytes`), the network matrix
(`rpc.rtt_us{peer=nN}`, `rpc.clock_offset_us{peer=nN}`,
`rpc.reachable{peer=nN}`), transactions (`txn.commits`, `txn.aborts`,
`txn.retries`, `kv.batch_p99_us`), SQL (`sql.connections`,
`sql.idle_in_txn`, `sql.statements`, `sql.serialization_failures`,
`sql.rows_scanned`, `sql.p99_us`) and, recorded once per cluster as
node 0 by the leader of range 1, the table gauges
(`table.rows{table=t}`, `table.ranges{table=t}`). Counters are stored
cumulative; the query side (and the dashboard) differentiate them. The
full list, with kinds and units, is the `/api/metrics` catalog.

The same data by SQL, for `psql` or a Grafana PostgreSQL data source
(pass an absolute timestamp; `now() - interval` needs the INTERVAL type):

```sql
SELECT at, value FROM datax_metrics
WHERE node = 1 AND name = 'node.cpu_percent' AND at >= '2026-09-04 15:00:00Z'
ORDER BY at;
```

Cost, bounded by construction: about 50 series per node every 10 s is
5·N rows per second; a 3-node cluster writes 15 rows per second and
keeps about 13 M rows at 7 days, tens of MB after compaction. The
recorder writes through the SQL layer on the node's own KV client, under
backpressure like any writer: when the store sheds writes it skips the
tick (`datax_metrics_record_skipped_total`) rather than adding load;
`datax_metrics_record_rows_total` and
`datax_metrics_record_errors_total` count what it wrote and what
failed.

The table is reserved: `CREATE TABLE datax_metrics`, `DROP TABLE`,
`ADD COLUMN` and `DROP COLUMN` are refused; admins may `DELETE FROM` it,
`ALTER TABLE datax_metrics SET (retention = '30d')` and
`SET (shards = 16)` (the online re-shard, while the recorder keeps
writing) work as for any time-series table; `GRANT SELECT ON
datax_metrics TO grafana` lets a reporting user read it and nobody but
admins writes to it. Backups leave it out unless asked
(`datax backup --include-metrics`), since it is bulky and regenerable;
a restore into a cluster whose nodes have already created the table
proceeds (the table sits at a reserved descriptor ID no user table can
collide with, and a backup that carries it lands on top).

## Metrics worth alerting on

Full list: scrape `/metrics`. The load-bearing ones:

| Metric | Alert when | Meaning |
|---|---|---|
| `datax_storage_backpressure_total` | increasing | writes are being shed — `datax_storage_backpressure_cause_total{cause=leader\|debt\|follower}` says which limit; see [Backpressure](#backpressure) |
| `datax_storage_write_stalls_total` | increasing at all | Pebble hard-stalled writes; you're past backpressure |
| `datax_storage_l0_sublevels` / `datax_storage_l0_files` | sustained ≥ 10 / ≥ 400 | compaction falling behind |
| `datax_storage_block_cache_hits_total` / `_misses_total` | hit rate under ~90 % on a read-heavy node | the block cache is smaller than the read working set: `--cache-size` (the dashboard's storage section shows the rate and `datax_storage_bloom_hits_total` / `_misses_total`, the share of point reads the bloom filters answered) |
| `datax_storage_compaction_debt_bytes` | growing without bound | ingest exceeds compaction budget |
| `datax_storage_debt_gate` | 1 for long stretches | the compaction-debt gate is latched (writes shed with `cause=debt` until debt halves) |
| `datax_storage_disk_slow_total` | increasing | disk latency spikes |
| `datax_txn_retries_total` vs `datax_txn_commits_total` | ratio ≫ a few % | heavy contention; look for missing `FOR UPDATE` or hot rows |
| `datax_deadlock_aborts_total` | increasing | lock cycles between transactions |
| `datax_dead_node_repairs_total` | any change | a node was declared dead and its replicas re-homed |
| `datax_consistency_failures_total` | **any change — page someone** | a replica's checksum diverged: replicated-state corruption (requires the sweep: `--consistency-interval`) |
| `datax_ranges` vs `datax_range_leaders` per node | leaders very skewed | lease shedding isn't keeping up (check `datax_lease_sheds_total`) |
| `datax_store_disk_bytes{kind="free"}` | < 15% of `kind="total"` | the store's disk is filling; Pebble needs headroom to compact (a full disk is a hard stall) |
| `datax_node_cpu_percent{scope="host"}` / `datax_node_load1` vs `datax_node_cores` | sustained > 80% / load > cores | the node is CPU-bound; check `scope="process"` to see whether datax or something else is using it |
| `process_open_fds` vs `datax_process_fd_limit` | > 80% | raise the limit (`ulimit -n`); Pebble holds one descriptor per open sstable |
| `datax_node_memory_bytes{kind="available"}` | < 10% of `kind="total"` | the host is running out of memory; the block cache and memtables are the usual tenants |
| `datax_clock_offset_seconds{peer}` | \|offset\| > half of `--max-offset` (0.25 s by default) | a node's clock is drifting; past the tolerance the node refuses the peer's timestamps and exits — fix NTP now (the node also logs a warning at this point) |
| `datax_peer_reachable{peer}` | 0 | this node's pings to the peer fail: a partition, a firewall, or the peer is down (its heartbeat will say which) |
| `datax_rpc_rtt_seconds{peer}` | p99 rising | the link to that peer is degrading; every raft round trip to it pays this |
| `datax_table_stats_age_seconds{table}` | > 1h on a table that changes | statistics are not refreshing (the sampler needs the table to be readable and the node to lead); the planner is estimating structurally |
| `datax_sql_connections{state="idle_in_txn"}` | > 0 for minutes | a client is holding a transaction open and idle; its write intents block every other writer to those keys (see the oldest-idle-txn age on the dashboard). `SET idle_in_transaction_session_timeout` (per session, or in the application's connection setup) ends such a connection and releases its intents; `SELECT pg_cancel_backend(pid)` / `pg_terminate_backend(pid)` with the pid from `pg_stat_activity` does it by hand, and `SET lock_timeout` bounds how long the blocked writers wait ([Session settings](sql.md#session-settings)) |
| `datax_sql_serialization_failures_total` vs `datax_sql_statements_total` | ratio ≫ a few % | contention on hot rows; the client-side view of `datax_txn_retries_total` |
| `datax_metrics_record_errors_total` | increasing | the node cannot write its metrics history; the table was dropped and recreated, or writes to it fail (check `datax_metrics_record_skipped_total` for backpressure first) |
| `datax_health_problems{severity="critical"}` | > 0 | a health check found something page-worthy; `check` names it and the dashboard's problems panel says where (see [Health checks](#health-checks)) |
| `datax_sql_statement_latency_seconds` | p99 far above `datax_kv_batch_latency_seconds` | time is going into planning, retries or result materialization rather than replication; check the slow statements on `/api/activity` |
| `datax_raft_replayed_entries_total` | jumps after a restart | the node came back from a crash (or an unflushed close) and re-applied that many committed entries from its raft log — expected after a crash, a sign the shutdown was not clean otherwise |
| `datax_storage_bytes_written_total{engine="state",kind="wal"}` | increasing on a split store | the state engine is writing a WAL it should not have; the store did not migrate (`datax_storage_split` is 0: restart the node after the v13 finalize) |
| `datax_sql_memory_limit_hits_total` | increasing | statements are failing with `53200`: a query sorts, aggregates or joins more than `statement_memory_limit` allows on the gateway — narrow it, add an index that delivers the order, or raise the limit for that session (`datax_sql_streamed_rows_total` vs the statement count says how much of the read traffic streams and never counts against the limit) |

Each node also pings every peer every 2 seconds (the NTP exchange, so
one ping yields both the round trip and the peer's clock offset); the
matrix on the dashboard and the `datax_rpc_rtt_seconds`,
`datax_clock_offset_seconds` and `datax_peer_reachable` series come from
it. A ping that fails or times out (1 s) marks the peer unreachable
rather than recording a huge round trip.

The host series (`datax_node_*`, `datax_store_disk_*`, and the standard
`go_*` / `process_*` collectors) come from a sampler each node runs
every 5 seconds; rates are averages over that interval. Host CPU, load,
memory, network and disk throughput need `/proc` (Linux); elsewhere
`/status` lists them under `machine.unavailable` and the dashboard says
so.

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
`datax_range_merges_total`, `datax_gc_runs_total`,
`datax_raft_scheduler_latency_seconds` (how long replicas wait for a raft
worker — a growing tail means the store's raft workers are saturated),
`datax_raft_log_syncs_total` / `datax_raft_readies_per_sync` (one synced
commit per scheduler pass however many ranges appended; the histogram
shows how many shared each), `datax_quiescent_ranges` (idle ranges that
stopped ticking and heartbeating; on a quiet cluster this approaches
`datax_ranges`), `datax_raft_heartbeat_envelopes_total` /
`datax_raft_heartbeats_coalesced_total` (the per-peer message rate and
how many heartbeats each message carried),
`datax_raft_entries_appended_total` / `datax_raft_entries_applied_total`
(the log's write and apply rates; applied lagging appended for long is a
store whose apply workers cannot keep up), `datax_raft_apply_seconds`
(per-entry apply time — the single-range write ceiling is its inverse,
see [Capacity planning](#capacity-planning)),
`datax_raft_apply_backpressure_total` (raft passes deferred because a
replica had more than 64 MiB of committed entries queued for apply — a
follower falling behind its leader), `datax_latch_wait_seconds` (time
requests spent waiting for a conflicting in-flight request's latch: key
contention).

## Everyday admin: `datax debug`

All subcommands talk to a running node (`--addr`, default
`127.0.0.1:26257`) except where noted. Against a **secure** cluster add
`--certs-dir` (and `--user`, default `root`) to present a client
certificate; state-changing subcommands require the admin role, and each
one lands in the node's audit log with the acting principal
([Security](security.md#admin-rpcs-in-secure-mode)).

Connecting is its own phase: the client dials (and, in secure mode,
completes the TLS handshake) under `--connect-timeout` (default 10s),
reporting `still connecting to 10.0.0.1:26257 (admin rpc) ... 5s` on
stderr while it waits — rewritten in place on a terminal, appended as
lines in a log — and failing with `could not connect to <addr> (...)`
plus the cause. Only then does the operation run under its own budget
(30s for `debug`, 30 minutes for `backup`/`restore`), so a dead node is
reported in seconds rather than after that budget expires. The same
applies to `datax sql` and `datax debug status`.

```sh
datax debug nodes                      # liveness, locality, last heartbeat
datax debug ranges                     # every range: span, replicas, leader
#   r7 [/table/orders/primary/1000, /table/orders/by_city) gen=3 replicas=[...]
datax debug status --url http://127.0.0.1:8080/status

datax debug split --table 100          # manual split (also: --key <hex>)
datax debug merge --key <hex>          # manual merge
datax debug transfer-lease --range 5 --node 2
datax debug rebalance --range 5 [--from 1]
```

Splits and merges also happen automatically (by size: 64 MiB; by load:
sustained 500 QPS); the manual commands are for pre-splitting before a bulk
load and for tests. From SQL, `ALTER TABLE t SPLIT AT VALUES (1000),
(2000), ...` carves a table at primary-key tuples (a prefix of the key is
allowed) and returns the boundaries; `datax bench ... --presplit N` uses
it to spread a workload over N ranges from the start.

Keys print as paths everywhere — logs, `datax debug`, the dashboard and
its API: `/Min` and `/Max` bound the keyspace, `/meta/...` are the range
addressing records, `/system/desc/7` and friends the catalog, and
`/table/orders/primary/1000` is row 1000 of `orders` (`/table/orders/by_city/"oslo"/42`
an index entry). Where the printer has no schema (a log line written
before the node has read the catalog) a table shows by ID and key values
by shape, e.g. `/table/3/1/1000`.

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

## Stopping a node

`SIGTERM` or Ctrl-C stops a node cleanly. The node first **drains**, for
up to `--drain-timeout` (default 10s): it tells its peers it is leaving
(they hand it no new leases and place nothing on it), transfers every
lease it holds to a live peer so no range waits for a lease to expire,
closes its SQL listener, and ends its SQL connections — an idle
connection gets PostgreSQL's `FATAL 57P01 admin_shutdown` at once, one
running a statement or inside a transaction is left to finish and told
the same at its next idle point. When the deadline arrives the
connections still open are closed (a transaction left open gets the
`57P01` too) and the leases still held stay held; then the node stops
and prints what the drain achieved:

```
shutting down: draining for up to 10s (signal again to skip)...
drained: 12 leases transferred (0 kept), 3 SQL connections closed (0 cut)
stopped
```

Clients that retry on `57P01` (every pool does; pgx, psycopg and JDBC
reconnect on the next statement) see nothing else; a serializable
write caught by a lease handoff can surface as `40001`, the retry every
datax client already handles. A second signal skips the rest of the
drain; a third — or a stop that has not completed one drain timeout
after the second — exits without waiting. `--drain-timeout 0` stops at
once (the pre-0.16 behavior); data is never at risk either way, Raft
and applied state are synced on every commit. The dashboard and
`/api/health` show a draining node as **stopping**; its replicas stay
where they are, unlike a [decommission](#decommissioning-a-node).

For systemd, `KillSignal=SIGTERM` (the default) and a `TimeoutStopSec`
above the drain timeout are all it takes.

## Rolling upgrades

A cluster upgrades with no downtime, one node at a time. Each binary has a
protocol version (shown at startup and in `datax debug nodes`); a cluster
has a **finalized cluster version** that only moves when you say so.
Adjacent versions only: upgrade one major version at a time.

```sh
# For each node, one at a time:
#   1. stop the node (SIGTERM: it drains its leases and connections first)
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
- **v13 needs one more rolling restart after finalize**: each node's next
  start migrates its store to the split layout (the raft log on its own
  engine under `--dir/raft`, the state engine without a WAL; see
  [Deployment → Store layout](deployment.md#storage-profiles)). Until
  that restart the node keeps running on one engine. A migrated store
  refuses a v12 binary.

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

## Profiles

Every node serves Go's `net/http/pprof` under `/debug/pprof/` on its HTTP
port, gated on the admin role in secure mode like the other drill-downs
(a profile exposes statement text and key bytes). Mutex and block
profiles are always on at low sampling rates (1 in 100 contended mutex
events; blocking events of 10 ms and up), so contention is visible
without a restart.

```sh
datax debug profile --kind cpu --seconds 30 --url http://10.0.0.1:8080 --certs-dir certs --user ops
datax debug profile --kind heap|allocs|mutex|block|goroutine --url ...
datax debug profile --kind trace --seconds 5 --url ...
go tool pprof -http=:0 cpu.pprof
```

`datax bench ... --server-url http://10.0.0.1:8080 --server-profile cpu`
pulls the node's CPU profile for exactly a benchmark run's duration.

## Benchmarking

`datax bench` drives a running cluster over pgwire:

```sh
datax bench kv --url ... --concurrency 16 --read-pct 95 --duration 30s
datax bench bank [--for-update]           # contended transfers
datax bench ingest --keys random|sequential|uuid --batch 100 --payload-bytes 256 [--rate N]
datax bench timeseries --series 1000 --shards 8
datax bench index-join --preload 20000 --groups 1000   # secondary-index fan-out to wide rows
datax bench scan --preload 20000                        # large result sets through pgwire
```

`ingest` writes batches of keys (random for LSM stress); `timeseries`
writes per-series monotone timestamps — the hot-tail shape — and is the
honest way to compare `--shards` settings. Every run takes `--seed`
(fixed by default, so two runs draw the same keys), `--json out.json`
for a record with throughput, p50/p95/p99, errors, retries and the
deltas of every server counter that moved (`--server-url` or
`--metrics-url`), and `--cpuprofile` / `--memprofile` / `--trace` for
the client.

`make bench` runs the checked-in set (`bench/workloads.json`: the kv
mixes, bank, three ingest key orders, timeseries, index-join, scan)
against a fresh single-node store and a fresh 3-node local cluster and
writes a record per workload; `datax bench compare BEFORE AFTER` prints
the deltas and flags anything beyond ±5 %. `bench/README.md` says how a
PR records its before/after; a nightly workflow runs the set on `main`
and keeps the records as an artifact.

Two counters show which commit fast path your workload rides:
`datax_one_phase_commits_total` (single-range implicit transactions — one
raft proposal, the cheapest commit) and `datax_parallel_commits_total`
(multi-range pipelined commits). A write-heavy workload where neither
moves is paying the classic two-round commit — usually explicit
`BEGIN` blocks.

## Capacity planning

Rules of thumb, from measured single-node numbers (NVMe, 100-row batches):

- **One range's write ceiling is its apply rate, not its disk.** Every
  write in a range goes through one raft group; its log is group-
  committed (one fsync serves every entry that arrived during the
  previous sync, so the disk's sync rate is rarely the limit — a few
  thousand a second here), and its entries apply one at a time, in log
  order, on the range's replicas. Measured below SQL on one range of one
  node (`BenchmarkRangeWritePipeline`, 64-byte values, 16 writers):
  single-row commits **~16k/s**, 100-row commits **~1,700/s** (~170k
  rows/s), 1,000-row commits **~260/s** — about **4 µs of apply per
  row** plus ~50 µs per commit, with the sync stubbed out changing the
  numbers by under 10%. Roughly: rows/s per range ≈ 1 / (4 µs +
  50 µs / rows-per-commit), so batch inserts of 100+ rows are within
  15% of the ceiling and single-row commits reach a third of it.
- **Through SQL an INSERT costs more than its KV write** — parsing,
  encoding, the uniqueness probe (one read round trip per statement) and
  the timestamp cache's bookkeeping — so a single range ingests roughly
  **40k rows/s** of 100-row batched INSERTs on one node, and the SQL
  layer's CPU, not the range, is what saturates first (`datax bench
  ingest --server-url ... --server-profile cpu` shows where).
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
