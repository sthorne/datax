# Changelog

Releases of datax, newest first. The version is `pkg/version.Release`,
bumped in the pull request that changes behavior (minor for a new
capability, patch for a fix); a git tag `vX.Y.Z` on `main` marks the
release, and the build workflow stamps binaries with the tag or with
`vX.Y.Z+<commit>` between tags. The cluster protocol version (`v1`, `v2`,
... in `pkg/version`) is separate: it changes only when the replicated
state or the internode protocol does, and an entry below says so.

## 0.9.0 — unreleased

### Added
- `datax sql` is a line editor when run on a terminal: the up and down
  arrows recall earlier lines, kept across sessions in
  `~/.datax_sql_history` (or `$DATAX_SQL_HISTORY`, the last 1000 lines);
  Left/Right, Home/End and the usual control keys edit the line; `\?`,
  `\h` and `help` print the keys, meta-commands and statement families;
  `\dt` lists tables. Ctrl-D quits, or cancels a multi-line statement in
  progress. Piped input keeps the plain line-by-line reader. Adds the
  `golang.org/x/term` dependency (its `x/sys` sibling was already one).

## 0.8.0 — unreleased

### Added
- A node detail page on the dashboard (`/#/node/N`, from a click on the
  Nodes table): identity and versions, machine tiles, the node's last 15
  minutes of CPU, QPS, statements and KV latency from the metrics
  table, storage with the debt gate, overload verdict and encryption
  status, the replicas it holds with their raft log depth, its SQL
  summary and (for admins) statements, its network row, its settings
  and its recent events. `/api/node?id=N` serves the document: the
  serving node's own to any user, another node's through the internode
  RPC for admins (a new `node-detail` admin op). Sub-KiB byte figures on
  the dashboard are rounded (#86).

## 0.7.0 — unreleased

Cluster version **v5**: the `datax_metrics` system table. Clusters
upgraded from earlier versions record metrics only after
`datax debug upgrade` finalizes v5.

### Added
- The cluster records its own metrics: every node writes about fifty
  series (host, storage, ranges, the network matrix, transactions, SQL,
  and once per cluster the table gauges) every `--metrics-record-interval`
  (default 10 s) into `datax_metrics`, a sharded time-series table with
  a 7-day retention that the nodes create at a reserved descriptor ID
  once the cluster has finalized v5. History survives restarts, is
  queryable with plain SQL from any client, and `/api/metrics` serves it
  aligned and downsampled per node (rates for counters). The dashboard
  gains a Metrics view (`/#/metrics`): a time-range picker, a grouped
  series picker, one chart per series with one line per node, a
  crosshair readout, a per-node mode, and a table view; every overview
  tile links to its series and draws its sparkline from the table. The
  table is reserved (create, drop and column DDL refused; retention and
  shards settable; a `SELECT` grant for reporting users; only admins
  write), excluded from backups unless `datax backup --include-metrics`,
  and tolerated by restore. `ALTER TABLE ... SET (retention = '...')` now
  works for any time-series table. `/metrics` gains
  `datax_metrics_record_rows_total`, `datax_metrics_record_skipped_total`
  and `datax_metrics_record_errors_total` (#115).

## 0.6.0 — unreleased

### Added
- Health checks and an events feed on the dashboard: every node runs a
  fixed set of checks against data it already holds (node liveness and
  draining, mixed binaries and unfinalized upgrades, lost quorum,
  under-replication and locality diversity, `/meta` reachability,
  storage backpressure, debt gate, write stalls and errors, overloaded
  followers, disk, file-descriptor and memory headroom, peer
  reachability and clock offset, consistency failures, authentication
  failure rate, stale statistics) and shows the findings in a problems
  panel at the top of the page, each linking to the section with the
  figure. A per-node ring of operational events (splits, merges,
  auto-splits, rebalances, lease sheds, dead-node repairs, snapshots,
  decommissions, upgrades, key rotations, backups and restores,
  consistency failures; the audit stream for admins) feeds an events
  section with a kind filter. New endpoints `/api/health` and
  `/api/events?since=N`; `/metrics` gains
  `datax_health_problems{severity,check}` (#85).

### Fixed
- Scans and reverse scans retried stale range routing thirty times with
  no pause, so a read that met a range mid-move or mid-merge could
  exhaust its retries in microseconds and fail with "scan routing did
  not converge" while the meta repair was still landing. They now back
  off between retries the way batches already did (10 ms per retry
  after the third, capped at 200 ms).

## 0.5.0 — unreleased

### Added
- SQL activity on the dashboard: the wire server now accounts for its
  connections by state (idle, active, idle inside an open transaction
  with the age of the oldest), statements by kind, statement latency
  percentiles, serialization failures and COPY rows; the summary rides
  each node's heartbeat, the dashboard's SQL section shows per-node
  connections, statements per second, the statement mix, the `40001`
  rate and p50/p99, and admins see the serving node's statements in
  flight and its slowest recent ones (`/api/activity`, threshold
  `SlowStatementThreshold`, default 500 ms). `/metrics` gains
  `datax_sql_connections{state}`, `datax_sql_statements_total{kind}`,
  `datax_sql_statement_latency_seconds`,
  `datax_sql_serialization_failures_total` and
  `datax_sql_copy_rows_total` (#84).

## 0.4.0 — unreleased

### Added
- Schema browser on the dashboard and `/api/schema`: every table with
  its columns, primary key, indexes (and whether one is still being
  built), time-series options, grants, statistics with their age, and
  range footprint (ranges cluster-wide; replicas, leaders and bytes on
  the serving node); the users for admins; a filter box that narrows the
  tables and both range lists. Ranges in `/api/cluster` and `/status` now
  name the table their keys belong to. Secure mode shows a non-admin
  user only the tables it holds a grant on. `/metrics` gains
  `datax_table_ranges{table}`, `datax_table_rows{table}` and
  `datax_table_stats_age_seconds{table}` (#83).

### Fixed
- `/api/cluster` and `/status` on a node cut off from the meta range's
  leader answered only when the client gave up: the range listing
  retried until then. The listing is now bounded (2 s) and falls back to
  the last list the node fetched, with its age noted in `error`; the
  table-name refresh runs in the background and `/api/schema`'s catalog
  scan is bounded (5 s) and reports the catalog unavailable instead of
  hanging.

## 0.3.0 — unreleased

### Added
- Inter-node latency and clock offset: each node pings every peer every
  2 s (an NTP-style exchange yielding both the round trip and the peer's
  clock offset), advertises its row on the heartbeat, and the dashboard
  shows the whole matrix with offsets judged against `--max-offset`;
  `/metrics` gains `datax_rpc_rtt_seconds{peer}`,
  `datax_clock_offset_seconds{peer}` and `datax_peer_reachable{peer}`, and
  a node logs a warning once a peer's offset passes half the tolerance
  (#82). New internode RPC `Ping`; a node on an older binary answers
  "unimplemented" and reads as unreachable until upgraded.

## 0.2.0 — 2026-09-04

Cluster protocol v4 (ordered range-addressing repair). Binaries from
0.1.x can join a v3 cluster and are finalized to v4 with `datax debug
upgrade`.

### Added
- Machine-level metrics per node: each node samples its host (CPU, load,
  memory, the store disk's size, throughput and utilization, network,
  file descriptors, Go runtime) and advertises a summary on its
  heartbeat; the dashboard's Nodes table shows every node's figures with
  warning colors, a Machine section shows the local node in full, and
  `/metrics` exports `datax_node_*`, `datax_store_disk_*` and
  `datax_process_*` next to the standard Go and process collectors (#81).
- The dashboard shows who it is signed in as and, without the admin
  role, explains the range drill-down refusal in those terms (#79).
- `datax sql --certs-dir DIR --user NAME` connects with a client
  certificate, like `debug`, `backup` and `restore` (#77).
- Every CLI client reports progress while connecting, under a separate
  `--connect-timeout` (default 10 s), and names the address and cause on
  failure; `datax sql` previously had no connect timeout at all (#78).
- Cluster version v4: split and merge repair `/meta` with
  generation-ordered updates, so a late repair can no longer resurrect a
  stale record (#74).
- Staged store keys: `--enc-key old.key,new.key` lets a node start with
  either key after an online rotation; background re-encryption of files
  under retired keys with bounded chunks (#67, #69).
- `datax version` prints the release and the cluster protocol range the
  binary speaks.

### Fixed
- Online index builds could miss a row written by a gateway whose lease
  renewals had stalled: the descriptor cache now shares the lease
  record's expiration, transactions take the lease's expiration as a
  commit deadline, and the backfill's chunk reads cover the whole key
  span (#110).
- A merge that raced a split of its right-hand range absorbed the
  pre-split span and left two ranges claiming the same keys; the merged
  descriptor is now built from the right-hand range as it stands after
  the subsume, checked again at apply (#111).
- Meta lookups retry a record that does not cover the key (the transient
  state of an addressing repair) instead of failing the batch (#111).
- The intermittent re-shard stall: write evaluation reports every
  conflicting intent at once and the client pushes each blocker once;
  proposals orphaned across a coalesced leadership change are answered
  and re-sent (#74).
- Follower overload verdicts stay sticky until the follower reports
  healthy; overlapping store-key rotations are serialized; merge apply
  exits when the right-hand raft loop stops; replicas are all loaded
  before any raft loop starts at restart (#65, #66, #70).
- Re-encryption cost is bounded per chunk and reported per span; the
  cleartext-rejection assertion in the encryption tests is meaningful
  (#69, #71); wall-time comment, health cache atomics, debt-gate refresh
  (#72).

### Docs
- README states the cluster version and a Scope section in place of the
  prototype status; security, getting-started and operations guides
  cover certificate auth for `datax sql`, connection feedback, the
  signed-in badge and the host metrics.

## 0.1.0

The initial tree: MVCC storage over Pebble, Raft-replicated ranges with
splits and merges, serializable transactions with parallel commit,
rack-aware placement, secondary indexes, a cost-based planner, sharded
time-series tables with online re-shard, follower reads, encryption at
rest, TLS + SCRAM with admin authorization and audit logging, backup and
restore, rolling upgrades (cluster protocol v3), Prometheus metrics and
the dashboard.
