# Operations

## The dashboard

Start nodes with `--http-listen` (or `demo -http-port`) and each node
serves, on that address:

- **`/`** — a self-contained web UI that reports the **cluster**, not
  the node that served the page (the header names that node as
  provenance, `served by n1 · region=r1`). Eight views sit behind one
  persistent nav, each a real route: **overview** (`#/`), **nodes**
  (`#/nodes`, and each node's page at `#/node/N`), **data** (`#/data`),
  **sql** (`#/sql`), **schema** (`#/schema`), **metrics**
  (`#/metrics`), **ops** (`#/ops`) and **security** (`#/security`).
  Three controls in the header cross every view: a **node scope** (the
  whole cluster, or one named node) that the node-scoped panels —
  statements, the event ring, audit — read instead of assuming the
  serving node, and which they name rather than leaving it to be
  guessed; a **time range** shared by every chart on the page; and
  **jump to** (`⌘K` or `Ctrl-K`), which resolves `n3`, `r128`, a table
  name or a locality like `rack=b` to the right view, and which is the
  practical navigation once a cluster has more ranges than fit on a
  screen. The scope, the range, each view's filter and an open range
  drill-down are all in the URL, so a view is a link worth sharing, and
  each view remembers where it was scrolled to.

  The overview's tiles are the
  cluster rollup summed over every live node's heartbeat — QPS, data,
  ranges and replicas, leases, connections (by state and top users),
  statements and `40001` per second, the worst p99 and the node that owns
  it — each with the count of contributing nodes beside it when a node
  is down, so a smaller number is never mistaken for a quieter cluster.
  Below them: a problems panel (every finding of the health checks
  below, colored by severity, each saying when this node's checks first
  found it and linking to the **view** that shows the figure and to its
  **history** on the operations view; a green line when there are
  none), one card per node
  (status, locality, CPU, load, memory, free space on the store's disk,
  file descriptors — colored and marked `!`/`!!` when they deserve a
  look — leases, data, QPS, connections, heartbeat age) opening that
  node's page, and the newest operations the cluster has performed on
  itself.

  The other views hold the rest. **nodes**: every node's row with the
  same figures, filterable by locality or address, plus the network
  matrix (each node's round trip to every other, with clock offsets
  judged against `--max-offset`; on a narrow screen a worst-pairs list
  instead). **data**: replication status, failure domains, and the
  cluster's ranges with replica placement and load, filterable by table
  or range id. The replication tiles bucket every
  range by state — healthy, under-replicated, over-replicated, no
  quorum, undiverse — measured against each range's own target replica
  count, so a database with a placement policy is judged by its policy
  and not by the cluster default; selecting a bucket lists its ranges.
  A replica on a live node that the node reports in its heartbeat as
  stopped by a storage failure or never loaded counts as not live, as
  in the health check, and its range is also listed under **dead
  replicas**. A heartbeat lists at most 64 such replicas per kind; a
  node that reports more is asked for the rest. If it does not answer,
  a range that would be in one state or another depending on its
  unlisted replicas is counted under **unknown** (a tile shown only
  while it counts a range), not as healthy.
  The **failure domains** table is what rack-aware placement is for:
  per locality tier value, the nodes, replicas and leases it holds, and
  what its loss would cost — how many ranges would drop below a
  majority, and how many would survive with no margin left, which is
  the question asked before a maintenance window. The **ranges** table
  lists every range, the heaviest first: its span and replicas from the
  descriptors, and its leader, QPS and size as each node advertises
  them for its heaviest and largest ranges — a quiet, small range shows
  a dash for those. QPS there is the leader's own measurement of ranges
  it leads and is never summed across nodes. Like the nodes, failure
  domains and schema tables it sorts from a column's header: the small
  arrow beside the header's words sorts by that column, again reverses
  it, and a third time returns the table's own order; a row with no
  figure in that column stays last either way. On a narrow screen the
  nodes, failure domains and schema tables are drawn one card per row
  with no header, so a **sort by** list above the cards offers the same
  columns, with a button beside it that reverses the direction; it is
  the same choice as the header's, and carries when the window widens.
  A range's id there, like a table's name in the schema list, is a real
  link: the row still opens on a click or Enter, and a ctrl-, cmd- or
  middle-click on the name opens it in a new tab. All of it is computed from the range descriptors
  and the nodes' declared localities that the page already fetches, so
  it costs no fan-out and answers from a partitioned node. **sql**: connections by state with
  the oldest idle transaction, statements per second by kind, `40001`
  rate and latency percentiles per node, with the scoped node's
  statements in flight and its slowest recent ones (admin). It also
  lists the **statement shapes** the cluster runs, grouped by the form
  the parser normalises each statement to — literals and parameters
  replaced, so every execution of one query is one row — ranked by total
  time, because a statement taking 8 ms that runs forty thousand times
  an hour costs more than a slow one run rarely and never appears among
  the slowest. Each row carries executions, mean latency, rows returned
  and rows scanned (far more scanned than returned is a scan), and
  retries; expanding one shows each node's own percentiles, the tables
  it touches, a representative statement, and an `EXPLAIN` of it —
  described, never run. The tables are per node and bounded: what a node
  drops to stay within its bound is counted, and the view says so rather
  than implying the list is every shape that ever ran. It also
  carries the transactions the cluster is running: commit, abort and
  retry rates over the header's range from the recorded series, and KV
  batch latency charted beside statement latency, which is what tells a
  slow statement from a slow cluster. A retry rate does not say what to
  change, so beneath it are the statement shapes producing the `40001`s
  — the statements with their literals replaced, counted per shape and
  per user since the node started — and the sessions sitting inside an
  open transaction, with the user, the client, the application, how long
  they have been idle and the statement they last ran, because their
  intents block every other writer to those keys. The rates need no role;
  the shapes and the sessions carry statement text and stay behind the
  admin gate. Both are the scoped node's own counts and are not summed
  across the cluster. **schema**:
  every table with its columns, primary key, indexes, time-series
  options, grants, statistics and their age, and range footprint.
  The **nodes** table adds a **fills in** column beside free space: how
  long the store has at its recent rate, fitted over the recorded
  free-space history. It is blank where free space is flat, rising, or
  too noisy to be a trend — a projection is only offered where the
  series has one to give, and hovering the cell says why there is none.
  A store on course to fill within a fortnight becomes a health problem,
  and within three days a critical one.

  **ops**: the operations in flight and the ones that have finished,
  above the scoped node's event ring with a kind filter. Under the
  whole-cluster scope the two operation tables are every node's,
  merged, each row naming the node it is on — a decommission is driven
  by the draining node and a backup runs where it was started, so the
  operation worth watching is usually on a node other than the one
  serving the page; a node that did not answer is named beside the rows
  that did (admin role; without it, and under a node's scope, the
  tables are that node's own). A long-running
  operation — a backup, a restore, a re-encryption, a node's drain —
  records both of its ends in the ring, so the view pairs them and shows
  the elapsed time of the ones still running and the duration and
  outcome of the ones that finished. Elapsed time is a figure, not a
  progress bar: nothing here knows how much work is left. The ring is
  bounded, so an operation whose start has already aged out of it is
  listed by its end alone and claims no duration rather than inventing
  one. In the timeline, a range or node an event names is a link to it,
  and a burst — four or more events of one kind within a minute of
  each other, as when the balancer sheds a dozen leases after an
  ingest — is one row that says how many and opens in place, so the
  split or merge behind it stays on screen. Findings are never folded:
  health, consistency-failure, security and auth-failure events each
  keep a row, since a burst of those is an incident and its order is
  what you came to read. **security**:
  how the cluster authenticates, who is signed in and by what, the
  certificates this node loaded with their expiry dates, the roles and
  what each one effectively holds through membership, who is connected
  and by which method, the store's encryption state and any
  re-encryption still to do, and the audit records separated from the
  operational ones. Certificate expiry and role membership are open to
  any authenticated user; the per-user connection breakdown and the
  client certificates this node has been shown need the admin role, as
  the audit records do. **metrics** charts any of the series the cluster records about
  itself over the header's time range — or over a window dragged out on any chart, which the page's address then carries, so a narrowed chart is a link — one chart per series with one
  line per node, from the `datax_metrics` table described under [Metrics
  history](#metrics-history); every tile on the overview links to its own
  series charted, and the tiles' sparklines are the last 15 minutes from
  that table. Both axes are labelled at round values: the time axis at
  multiples of a round step for the window (every 15 seconds, every 10
  minutes, every 6 hours, each midnight) on the reader's own clock,
  three to seven of them. The charts can mark the events that explain a change in
  shape: a dashed line at each event in the window, the structural kinds
  (splits, merges, rebalances, lease transfers, repairs, restarts,
  upgrades) by default and any other kind on request. The marks come
  from the serving node's event ring, which is bounded and per node, and
  the chart says so: when the ring does not reach back to the start of
  the window, a note gives the oldest event it holds, so an unmarked
  stretch reads as unknown rather than as quiet.

  Each node's **page** (`#/node/N`, from its card, the nodes view or
  jump-to) holds everything that describes that one node: identity
  (address, locality, release and protocol version, the cluster version
  it has mirrored, uptime), its machine tiles (disk and network
  throughput, Go runtime, uptime), its last 15 minutes of CPU, QPS,
  statements and KV latency, its storage (engine figures, the debt gate,
  the overload verdict, encryption, re-encryption and prefix-filter
  rewrite progress), the replicas it holds with their raft log depth, its
  SQL summary (and, for admins, its statements in flight and slow
  statements), its row of the network matrix, its settings, and its
  recent events (splits, merges, rebalances, lease sheds, repairs,
  snapshots, backups, upgrades, key rotations, consistency failures;
  admins also see the audit stream). The cluster ranges table drills
  down: a click, or Enter on a focused row, fetches every holding node's
  view of the range (leader, applied index, size, QPS, closed timestamp)
  over internode RPC, so any node's dashboard can inspect any range —
  and the open range is in the URL, so the drill-down is shareable too.

  The page is usable from a keyboard (a skip link, a focus ring on every
  control, real links and controls where a mouse was once required),
  reads properly with a screen reader (captions and column scope on
  every table, the problems panel as a live region), and fits a phone
  (the wide tables become one card per row). Its favicon takes the
  colour of the worst open health problem and its title carries the
  count — `(2) datax — n1` — so a cluster gone critical shows in the tab
  strip without switching to it. It polls only what the current view
  shows, stops polling while the tab is hidden and refreshes the moment
  it is shown, backs off when a node stops answering (the header says how
  long since the last good update and when it retries), and notices a
  rolling upgrade under a long-lived tab: when the node's console differs
  from the one the tab runs, the header offers a reload. A table cell can
  be selected and copied — the page patches what changed rather than
  rebuilding itself every poll, and never under a selection, focus or an
  open disclosure.
- **`/api/overview`** — JSON: what the overview draws, in one document —
  the `/api/cluster` document, the `/api/health` problems and the tail of
  `/api/events` (`?limit=`, default 50) — with an `errors` map naming any
  section that could not be produced (the cluster document's own
  partial-data note is mirrored there), so one slow subsystem degrades a
  section rather than failing the request.
- **`/metrics`** — Prometheus text format.
- **`/status`** — JSON: this node's identity, locality, and every range it
  holds (leader, applied index, size, QPS, closed timestamp — the newest
  fixed timestamp the replica can serve follower reads at).
  `datax debug status --url ...` pretty-prints it. It answers 200
  whenever the HTTP port does, so it is a liveness check, not a
  readiness one.
- **`/ready`** — the readiness check for a load balancer or an
  orchestrator's probe. `200 {"ready":true}` once the node has joined
  (a heartbeat of its own reached the registry in range 1, so the
  cluster answers it), its store is open and its SQL listener, when one
  is configured, is serving. Before that, and from the moment it begins
  draining for a stop (SIGTERM, Ctrl-C), `503` with a `reason`. It needs
  no credentials, in secure mode too, and says nothing beyond that.
  Send SQL clients to a node only while its `/ready` is 200; restart it
  on its `/status`.
- **`/api/cluster`** — JSON: the whole cluster as this node sees it (node
  liveness, heartbeat age, leader QPS/counts, replica bytes, hot ranges,
  and the `rollup` summed over the live nodes with the count of nodes
  each sum covers; `console_version` is the digest of the console page
  this node serves, also the page's `ETag`). A node that cannot reach
  the meta range (a partition) still answers within a couple of
  seconds, with the last range list it fetched and an `error` saying how
  old it is.
- **`/api/range?id=N`** — JSON: the cross-node drill-down document behind
  the range detail view. Admin role required in secure mode.
- **`/api/activity`** — JSON: this node's SQL connections, statements in
  flight, and the slow-statement ring (past `--slow-statement-threshold`,
  default 500 ms). Admin role required in secure mode; statement text can
  carry data.
- **`/api/health`** — JSON: the problems panel's document: the findings
  of the health checks (see [Health checks](#health-checks)), sorted
  critical first, each with `since_unix_ms` (when this node's checks
  first found it open), and how many checks ran. Empty `problems` with
  a non-zero `checks` count means green. Recomputed at most every 3 s.
- **`/api/events?since=N&limit=M`** — JSON: the serving node's recent
  operational events (the last 500 are kept in memory; `since` returns
  only those after sequence `N`, which is how the dashboard tails). In
  secure mode audit records (authentication failures, admin operations,
  privilege DDL) are included only for the admin role, and a split or
  merge event names its keys in full only to a caller who may read
  every table they belong to — to anyone else the keys appear at their
  table-and-index prefix by id (the boundary is a row value; the same
  rule as the range lists on `/status` and `/api/cluster`). In full
  means as those lists print a key, the table by name and the index by
  name (`/table/orders/primary/42`); the node log keeps the raw form
  (`/table/7/1/42`), which is all the layer that records the event
  knows.
- **`/api/node?id=N`** — JSON: the node detail page's document. The
  serving node answers for itself to any authenticated user; another
  node's document is fetched from that node over the internode RPC and
  needs the admin role (403 otherwise). Statement text and audit
  events are included only for admins.
- **`/api/operations`** — JSON: every node's operations — the pairs of
  start and end records in its event ring plus what it knows is still
  open — merged over the internode RPC, running first, each with the
  node it is on. `nodes` of `nodes_asked` answered; a node that did not
  is named in `errors` with its heartbeat age, and the rest of the
  document stands. Admin role required.
- **`/api/statements`** — JSON: the statement shapes the cluster ran,
  unioned from every node over the internode RPC and re-ranked by total
  time; each entry carries the summable figures plus each node's own row
  with its percentiles (there is no p99 of two p99s, so they are not
  summed). **`/api/explain?fingerprint=`** plans one shape's stored
  representative statement — `EXPLAIN`, never `EXPLAIN ANALYZE`, and the
  text comes from the node's own accounting, never from the request.
  Both need the admin role: a representative statement can carry data.
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
  the tables it holds a grant on. Built when requested and cached for
  5 s per node, with concurrent requests sharing one build: a table
  created while the page is open shows within that, and a node whose
  console nobody has open does not read the catalog for it.

In secure mode an unauthenticated browser gets a sign-in page, and the
console's user menu shows who you are, how you got in, which roles you
hold, and signs you out — the credentials are the database credentials
`datax sql` uses (see [Security → Signing in to the
console](security.md#signing-in-to-the-console)). Scripted clients are
unaffected: HTTP Basic credentials of any database user, or a client
certificate, authenticate every route as before. `/api/range`,
`/api/activity`, `/api/operations` and another node's `/api/node`
additionally require the admin role ([Security](security.md)). Without that role the cluster
ranges are not clickable and the note under them says which user is
signed in and how to proceed (`GRANT ADMIN TO ops`, or sign out and back
in as `root`). `/api/cluster` carries the same identity in its
`principal` field, with the session's expiry when there is one.

## Health checks

Every node runs the same set of checks against data it already holds
(the node registry, the `/meta` range list, its own store, the peer
pinger, the schema cache) and publishes the findings on the dashboard,
in `/api/health`, and as the gauge `datax_health_problems{severity,check}`
(one series per finding; a check that finds nothing has no series).
Alert on `datax_health_problems{severity="critical"} > 0` for the
page-worthy ones and on `severity="warning"` for the rest, and the panel
on any node's dashboard says what and where.

A finding has a history. Each row carries when this node's checks first
found it (`since_unix_ms`; a problem is the same problem while its check,
node and range agree, whatever its summary's figures now say), and the
two transitions — the problem appearing, and the same problem clearing,
with how long it was open — are recorded on the node's event ring under
the kind `health` and nowhere in between, so a check that flaps leaves
a pair of records per flap rather than a stream. The row's **history**
link opens the operations view filtered to that check's records
(`#/ops?kind=health&q=<check>`), which is where "has this been
flapping" and "did it clear on its own" are answered. Both are as one
node sees them: every node runs the checks for itself when something
asks it for the document, so two nodes' consoles may date a problem a
few seconds apart, and a restarted node dates every open problem from
its first run.

Acknowledging a finding is deliberately not a console feature. An
acknowledgement is state that outlives a process and belongs to the
cluster, and the console does not write to the cluster; it belongs to
whatever alerts on `datax_health_problems`, which has silences with an
owner and an expiry. The console's job is to report the state
truthfully, and a row that says how long it has been open is one an
operator can decide about at a glance. The checks, with the section the
dashboard row links to:

| Check | Severity | Fires when |
|---|---|---|
| `node-down` | critical | a node's heartbeat is older than the dead-node threshold (30 s); its replicas are being repaired away (nodes) |
| `node-unresponsive` | warning | a node's heartbeat is older than the liveness grace (15 s) but not yet the dead-node threshold (nodes) |
| `node-draining` | info | a decommission is in progress (nodes) |
| `mixed-binaries` | warning | nodes run different binary versions (nodes) |
| `upgrade-unfinalized` | warning | every node runs a newer binary than the cluster version; `datax admin upgrade` has not been run (nodes) |
| `replicas-failed` | critical | a live node has replicas whose raft group a storage failure stopped (a raft-engine write error such as ENOSPC or EIO, a failed apply); they serve nothing until the node restarts, and the node keeps heartbeating, so dead-node repair does not replace them. Check the node's log and disk, then restart it (nodes) |
| `replicas-missing` | warning | a live node has been named in a range's descriptor for over a minute with no replica of the range loaded (one lost in a crash, say); it counts toward the range's quorum but cannot vote (nodes) |
| `replicas-unverified` | warning | a live node reports more failed or missing replicas than its heartbeat lists (at most 64 of each), and did not answer when asked for the rest; whether its replicas of higher-numbered ranges serve is unknown (nodes) |
| `replication-unknown` | warning | ranges that would be under-replicated or without quorum, or not, depending on replicas a `replicas-unverified` node did not report; counted in neither of those checks (ranges) |
| `quorum-lost` | critical | a range has fewer serving replicas than a majority; it cannot serve until a node returns (ranges). A serving replica is one on a live node that the node does not report failed or missing |
| `under-replicated` | warning | a range has fewer serving replicas than its replication factor (ranges) |
| `diversity` | warning | a range's replicas share a locality tier they could spread across (ranges) |
| `placement-unsatisfiable` | critical | a range names a database placement policy that no live node satisfies; the data stays where it is until such a node joins (ranges) |
| `placement-misplaced` | warning | a range holds a replica outside its database's placement policy; the allocator moves them home one per pass (ranges) |
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

Two groups of series are keyed by what they measure rather than by the
node alone. Index usage (`index.scans{index=t.i}`,
`index.rows{index=t.i}`; the primary key is `t.primary`) counts the
scans opened and rows read through each index on the node, charged by
the executor where the scan opens; the table page's indexes list sums
them over the retention window across every node that recorded, and
`/api/index-usage?table=t` (admins) is the same figure by API. Zero
there means no recorded scans in the window on the nodes that
recorded, not an index nobody uses: a node with recording off, a
workload whose cycle is longer than seven days, or a cluster younger
than the window all leave a used index at zero.

Once the cluster has finalized v22, the per-table and per-index series
(`table.*`, `index.*`) are written only when they change — plus once
every 60 ticks (ten minutes at the default interval) whatever happens,
at a tick that depends on the series' name — rather than every tick: a
cluster of thousands of mostly quiet tables otherwise wrote a row per
table and two per index it had read, every tick, repeating the last
values. A series that ends (its table dropped or renamed, its index
dropped) gets one row with a NULL value, and every tick writes a
`recorder.tick` row for each node it records for (node 0 too, on the
leader of range 1). `/api/metrics` and the dashboard put the values
back — at each `recorder.tick` without a row of its own, a series takes
its last value, when that is no older than the 60-tick period and not
NULL — so an idle table charts its value at every tick, as before, and
a restart still shows as a gap. A plain SQL query sees the rows as
written: to read an idle table's value at a moment, take the newest row
at or before it, within the last ten minutes, and treat NULL as "ended".

Statement shapes (`stmt.count{fp=…}`, `stmt.total_us{fp=…}`,
`stmt.rows_scanned{fp=…}`, `stmt.retries{fp=…}`) are the fingerprint
table's counters with a time dimension: every tick each node writes
its 50 busiest shapes by total time into `datax_metrics_shapes`, a
second time-series table of the same shape as `datax_metrics` with a
day's retention (the rows are numerous and go stale fast; retention is
per table), once the cluster has finalized v19. A shape that falls out
of a node's top 50 stops being written, never written as zero, so its
chart shows the gap it is. The fingerprint's text lives in
`datax_statement_shapes` (`fingerprint`, `shape`, `kind`,
`relations`, `first_seen`, `last_seen`), written when a shape is first
recorded and swept after two days unseen; the metrics catalog resolves
it for admins, so the picker and the chart title read as the
statement, and the shapes view's detail has a "chart this shape" link.
The representative statement text, which can carry data, is not
persisted. Both tables are reserved like `datax_metrics`, excluded
from backups unless `--include-metrics`, and the `stmt.*` series are
served from the shapes table by the same `/api/metrics`.

The same data by SQL, for `psql` or a Grafana PostgreSQL data source
(pass an absolute timestamp; `now() - interval` needs the INTERVAL type):

```sql
SELECT at, value FROM datax_metrics
WHERE node = 1 AND name = 'node.cpu_percent' AND at >= '2026-09-04 15:00:00Z'
ORDER BY at;
```

Cost, bounded by construction: about 50 series per node every 10 s is
5·N rows per second (plus, per tick, the table and index rows that
changed and a sixtieth of the rest); a 3-node cluster writes 15 rows per second and
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

## Audit table

`datax_audit` is the other table the nodes write on their own account:
every audit record, one hash chain per node. What it holds, who may
read it and how to verify it are in [Security → Audit
table](security.md#audit-table); what an operator needs to know:

- **Volume.** A record is a few hundred bytes. An ordinary cluster
  writes a handful an hour (DDL, role changes, admin operations,
  sign-ins); a cluster under a password-guessing attack writes at most
  20 refusal records a second per node, and usually far fewer because
  repeats are folded. Writes are batched, one `INSERT` a second per node
  at most.
- **Retention.** 90 days by default; `ALTER TABLE datax_audit SET
  (retention = '400d')` changes it (admins, at least a day). Ageing out
  is done by the nodes, ten minutes apart, at most 10,000 records per
  chain per pass.
- **Backups** include the table. A restore replaces it: see the
  security guide for what that does to the chains.
- **Export the heads.** `datax debug audit heads` prints the last record
  of every chain. Saved outside the cluster on a schedule, it is what
  makes `datax debug audit verify --heads` proof against someone
  rewriting the table underneath SQL.
- **`--audit-table=false`** on `datax start` leaves the table alone on
  that node; its audit records are then in its log only.

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
| `datax_txn_pushes_total` vs `datax_txn_commits_total` | ratio climbing well above 1 | transactions are queueing behind each other's locks; each push is a blocked transaction asking the holder's record whether it has finished |
| `datax_cert_expiry_seconds{kind,subject}` | under 30 days (2592000), urgently under 7 (604800) | a TLS certificate is running out: `kind=ca` takes the whole cluster with it, `kind=node` takes that node off the internode and SQL listeners, `kind=client` stops one identity authenticating. Absent on an insecure cluster, which has nothing to expire |
| `datax_dead_node_repairs_total` | any change | a node was declared dead and its replicas re-homed |
| `datax_replicas_failed` | above 0 | replicas on this node were stopped by a storage failure (ENOSPC, EIO) and serve nothing until it restarts; the node still heartbeats, so nothing repairs them (health check `replicas-failed`) |
| `datax_replicas_missing` | above 0 | ranges whose descriptor has named this node for over a minute with no replica loaded here (health check `replicas-missing`) |
| `datax_placement_replicas_moved_total` | rising long after an `ALTER DATABASE ... SET` | replicas are still being moved onto nodes their database's placement policy admits; it should stop once the ranges converge, and `datax_health_problems{check="placement-unsatisfiable"}` says it never will (see [region-restricted replication](sql.md#region-restricted-replication)) |
| `datax_row_ttl_lag_seconds` | far above the 30s pass interval for long | expired rows are waiting to be deleted: the row expiry job is behind or stopped (see [Row expiry](#row-expiry)); `datax_row_ttl_rows_blocked_total` rising says foreign keys are holding rows |
| `datax_consistency_failures_total` | **any change — page someone** | a replica's checksum diverged: replicated-state corruption (requires the sweep: `--consistency-interval`) |
| `datax_ranges` vs `datax_range_leaders` per node | leaders very skewed | lease shedding isn't keeping up (check `datax_lease_sheds_total`) |
| `datax_store_disk_bytes{kind="free"}` | < 15% of `kind="total"` | the store's disk is filling; Pebble needs headroom to compact (a full disk is a hard stall) |
| `datax_node_cpu_percent{scope="host"}` / `datax_node_load1` (`load5`, `load15` for the trend) vs `datax_node_cores` | sustained > 80% / load > cores | the node is CPU-bound; check `scope="process"` to see whether datax or something else is using it |
| `process_open_fds` vs `datax_process_fd_limit` | > 80% | raise the limit (`ulimit -n`); Pebble holds one descriptor per open sstable |
| `datax_node_memory_bytes{kind="available"}` | < 10% of `kind="total"` | the host is running out of memory; the block cache and memtables are the usual tenants |
| `datax_clock_offset_seconds{peer}` | \|offset\| > half of `--max-offset` (0.25 s by default) | a node's clock is drifting; past the tolerance the node refuses the peer's timestamps and exits — fix NTP now (the node also logs a warning at this point) |
| `datax_peer_reachable{peer}` | 0 | this node's pings to the peer fail: a partition, a firewall, or the peer is down (its heartbeat will say which) |
| `datax_rpc_rtt_seconds{peer}` | p99 rising | the link to that peer is degrading; every raft round trip to it pays this |
| `datax_rpc_locality_fallbacks_total` | increasing | a peer's [locality address](deployment.md#locality-addresses) (`--locality-advertise`) does not answer, so this node reaches the peer over its public address. Check the private network, the peer's `--listen` and its certificate's hosts. The node's log names the peer and the address |
| `datax_table_stats_age_seconds{table}` | > 1h on a table that changes | statistics are not refreshing (the sampler needs the table to be readable and the node to lead); the planner is estimating structurally. Like `datax_table_rows` and `datax_table_ranges` it is read from the catalog when `/metrics` is scraped (at most every 5 s; a scrape waits up to 2 s for the reading, then serves the previous one), so every node reports the same current value and a node nobody scrapes reads nothing for it |
| `datax_sql_connections{state="idle_in_txn"}` | > 0 for minutes | a client is holding a transaction open and idle; its write intents block every other writer to those keys (see the oldest-idle-txn age on the dashboard). `SET idle_in_transaction_session_timeout` (per session, or in the application's connection setup) ends such a connection and releases its intents; `SELECT pg_cancel_backend(pid)` / `pg_terminate_backend(pid)` with the pid from `pg_stat_activity` does it by hand, and `SET lock_timeout` bounds how long the blocked writers wait ([Session settings](sql.md#session-settings)) |
| `datax_largeobject_pages_reclaimed_total` flat while `SELECT count(*) FROM datax_largeobject_pending` stays above zero | for more than a few minutes | the large-object sweeper is not reclaiming: unlinked objects and failed uploads are keeping their disk (see [Large objects](#large-objects)) |
| `datax_sql_serialization_failures_total` vs `datax_sql_statements_total` | ratio ≫ a few % | contention on hot rows; the client-side view of `datax_txn_retries_total` |
| `datax_replica_gc_removed_total` | increasing | the housekeeping pass found replicas that `/meta` says are members of nothing — preseeds whose membership change never landed — and removed them with their data; expected after a refused replica change, otherwise worth a look at the log for which ranges |
| `datax_raft_nonmember_messages_dropped_total` | increasing steadily | a replica id the range has moved past keeps sending — a removed replica that has not noticed, or a leftover replica GC has not yet collected (60 s grace) |
| `datax_audit_write_errors_total`, `datax_audit_buffered` | errors increasing while the buffer grows | the node cannot write its audit records to `datax_audit` (no quorum, a partition, the table's range unavailable); they are in its log and wait in memory to be written — see `audit.last_error` in `/api/security` |
| `datax_audit_dropped_total{cause="buffer-full"}` | increasing | the audit table has been unwritable for long enough that this node's buffer filled: these events are in the node's log only, and the table will carry an `audit-gap` record saying how many. `cause="rate"` is refusals (failed logins, denied calls) arriving faster than they are recorded — look at who is knocking |
| `datax_metrics_record_errors_total` | increasing | the node cannot write its metrics history; the table was dropped and recreated, or writes to it fail (check `datax_metrics_record_skipped_total` for backpressure first) |
| `datax_health_problems{severity="critical"}` | > 0 | a health check found something page-worthy; `check` names it and the dashboard's problems panel says where (see [Health checks](#health-checks)) |
| `datax_sql_statement_latency_seconds` | p99 far above `datax_kv_batch_latency_seconds` | time is going into planning, retries or result materialization rather than replication; check the slow statements on `/api/activity` |
| `datax_raft_replayed_entries_total` | jumps after a restart | the node came back from a crash (or an unflushed close) and re-applied that many committed entries from its raft log — expected after a crash, a sign the shutdown was not clean otherwise |
| `datax_storage_bytes_written_total{engine="state",kind="wal"}` | increasing on a split store | the state engine is writing a WAL it should not have; the store did not migrate (`datax_storage_split` is 0: restart the node after the v13 finalize) |
| `datax_sql_memory_limit_hits_total` | increasing | statements are failing with `53200`: a query sorts, aggregates or joins more than `statement_memory_limit` allows on the gateway — narrow it, add an index that delivers the order, or raise the limit for that session (`datax_sql_streamed_rows_total` vs the statement count says how much of the read traffic streams and never counts against the limit) |
| `datax_sql_statement_panics_total` | **any change — file a bug** | a statement panicked inside the SQL layer (a planner, executor, builtin or encoder bug). The barrier on the statement path turned it into an `XX000` for that statement, failed its transaction and kept the connection and the node serving; the log has the stack under `panic executing`. It never happens by design, so every count is a bug to report with that stack |

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
leaders), `datax_read_attempt_timeouts_total` (reads that waited out a
whole per-attempt deadline on one replica and were sent again — occasional
under heavy load, a steady rate means some leader keeps reads waiting for
seconds; such a read keeps trying until its own deadline, so one with no
`statement_timeout` waits rather than fails), `datax_parallel_commits_total`,
`datax_sql_rows_scanned_total` (a jump usually means a query lost its
index), `datax_stats_refreshes_total` / `datax_stats_rows_scanned_total`
(the table-statistics sampler's pace and cost),
`datax_retention_rows_expired_total` (row-level retention expiry on
mixed ranges),
`datax_reencryption_remaining_bytes` (encrypted stores: live sstable
bytes still under retired data keys — 0 attests re-encryption is
complete),
`datax_auto_splits_total` / `datax_load_splits_total` /
`datax_range_merges_total`, `datax_range_merges_deferred_total`,
`datax_range_merges_unwound_total`,
`datax_range_id_collisions_total` (splits that drew a range ID already in
use and were given another; it should stay at 0, and any count is a bug
to report), `datax_gc_runs_total`,
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
store whose apply workers cannot keep up),
`datax_raft_entry_bytes_appended_total` (the payload bytes of the entries
written), `datax_raft_apply_seconds`
(per-entry apply time — the single-range write ceiling is its inverse,
see [Capacity planning](#capacity-planning)),
`datax_raft_apply_backpressure_total` (raft passes deferred because a
replica had more than 64 MiB of committed entries queued for apply — a
follower falling behind its leader), `datax_latch_wait_seconds` (time
requests spent waiting for a conflicting in-flight request's latch: key
contention; it also counts the waits of the range-wide operations below
when they acquire), `datax_range_latch_wait_seconds{op,outcome}` (time a
split, merge, range clear or closed-timestamp promise waited for in-flight
requests, `outcome="acquired"` or `"abandoned"` at its deadline — an
abandoned `closed_ts` wait means the range's closed timestamp, and with it
follower reads, fell behind), `datax_sql_plan_cache_hits_total` /
`datax_sql_plan_cache_misses_total` (prepared statements reusing their
plan; a low hit rate under an ORM workload means statements are not
being prepared, or schemas or statistics change constantly),
`datax_sql_parse_cache_hits_total` (repeated simple-protocol texts).

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
#   r8 [...) gen=4 replicas=[...] sticky         <- starts at a manual split
#   r9 [...) gen=4 replicas=[...] sticky-until=2026-12-01T00:00:00Z
datax debug status --url http://127.0.0.1:8080/status

datax debug split --table 100          # manual split (also: --key <hex>); sticky
datax debug unsplit --table 100        # stop holding it (also: --key <hex>)
datax debug merge --range 7            # manual merge of r7 with its right neighbor
datax debug purge --table-name app.documents   # collect and compact deleted data now (also: --table <id>, --key/--end-key <hex>)
datax debug transfer-lease --range 5 --node 2
datax debug rebalance --range 5 [--from 1]
```

Splits and merges also happen automatically (by size: 64 MiB; by load:
sustained 500 QPS); the manual commands are for pre-splitting before a bulk
load and for tests. From SQL, `ALTER TABLE t SPLIT AT VALUES (1000),
(2000), ...` carves a table at primary-key tuples (a prefix of the key is
allowed) and returns the boundaries; `datax bench ... --presplit N` uses
it to spread a workload over N ranges from the start.

A manual split is **sticky**: housekeeping merges adjacent ranges that
are both small, which would undo a pre-split within seconds, so a
boundary made by `debug split` or `SPLIT AT` is marked and left alone
(`debug ranges` shows `sticky`; `/api/cluster` and `/api/range` carry
`sticky_until`). It stays until it is released — `datax debug unsplit`,
`ALTER TABLE t UNSPLIT AT VALUES (...)` / `UNSPLIT ALL` — or until the
expiration given with `SPLIT AT ... WITH EXPIRATION`; releasing merges
nothing by itself, it only lets housekeeping merge the ranges again when
they are small. Automatic splits are never sticky. `debug merge` merges
the range it is given whether or not its neighbor is sticky. Dropping or
truncating a table releases its manual boundaries, so the emptied ranges
merge away. All of this needs cluster version v20; before finalize a
manual split is an ordinary one.

### Purging deleted data

`datax debug purge` is `ALTER TABLE ... PURGE`
([SQL guide](sql.md#gc-window-and-purge)) for an operator, and for a key
range that is not a whole table. For one tenant's or one key prefix's
rows, prefer `ALTER TABLE t PURGE WHERE tenant = 'acme'`: it computes
the spans from the primary key, including the matching spans of
secondary indexes that lead with the same columns, and purges the
indexes it cannot narrow whole, so the purged rows' old versions are
gone from every index. A `--key`/`--end-key` purge covers exactly the
raw span it is given and nothing else, so on its own it leaves those
rows' index entries' history in place.

```sh
datax debug purge --table-name app.documents
# purged /table/documents to /table/8 at 2026-11-15T09:12:44.31Z: 31 version(s) removed over 1 range(s)
#   r12 n1  live keys 90210, superseded versions 0, tombstones 0, intents 0, compacted true
#   r12 n2  live keys 90210, superseded versions 0, tombstones 0, intents 0, compacted true
#   r12 n3  live keys 90210, superseded versions 0, tombstones 0, intents 0, compacted true
# not covered by a purge:
#   - backups taken before the purge still hold the data; destroy or rewrite them separately
#   - ...
# complete: every replica answered and holds no superseded version, tombstone or intent in the span
```

What it does, in order: splits at the two ends of the span, so the
ranges it touches hold nothing else; on each range's leader, garbage
collects at a threshold of now and truncates the raft log; on every
replica, waits for that to apply, compacts the span's sstables and the
range's log, and counts the span's keys below the MVCC layer; on every
node involved, retires the write-ahead log files written before the
purge. The command exits non-zero unless the purge is **complete** —
every replica answered, and none holds a superseded version, a
tombstone or an intent in the span — so a script cannot mistake "ran"
for "gone". The whole report goes to the audit log as a `purge` event,
and the purge shows as an operation on the operations view.

Things to know before relying on it:

- **It cuts off older readers of the span.** The span's GC threshold
  becomes the purge time: `AS OF SYSTEM TIME` before it, follower reads
  before it, and a transaction that began before it all fail on that
  span, with an error. A whole-table purge does this for every row of
  the table; `PURGE WHERE` only for the named keys and for any secondary
  index it had to purge whole (it names those in a `NOTICE`). An
  incremental backup whose base is older does not fail: it holds the
  purged table in full (see [Backup and restore](#backup-and-restore)),
  so incrementals need no full backup after a purge.
- **Its split points stay** until the merge queue folds them away, which
  it does once the neighbouring range's own GC window has passed the
  purge: a merged range adopts the higher threshold, so merging earlier
  would cut off the neighbour's history. The splits are not sticky and
  need no `UNSPLIT`.
- **A backup in progress is never cut off.** A running backup protects
  the timestamp it reads at (below). A purge first waits up to 10
  seconds for the backups in progress to end. If one is still running,
  the purge collects up to just below the oldest backup's timestamp —
  everything deleted before that backup began is gone — and reports
  itself **incomplete**, naming the backups (`INCOMPLETE: held back by
  ...`, `held by the backup to <dir> on n<id>, reading at <ts>`): what
  was deleted after the backup's timestamp is still on disk. The node
  that ran the purge purges the span again by itself as soon as those
  backups have ended, as a purge of its own with its own audit record
  and operation; the CLI's message says so, and the command still exits
  non-zero, since the purge it ran was not complete. If that node
  restarts first, the rerun is lost: run the purge again. A purge never
  reports a held purge as done. During a rolling upgrade, a range whose
  leader is on a binary from before this is not purged while a backup
  is running (it is reported, and the purge can be run again).
- **A node that is down** makes the purge incomplete, by name. When it
  restarts it applies the collection from the log (or is given a
  snapshot without the data) and serves no follower read until it has
  heard from the range's leader again, so it never answers from its old
  copy; run the purge again to compact it and get its count. A node
  that is partitioned but still running is not covered by that rule
  until it reconnects.
- **Backups are untouched.** A backup taken before the purge contains
  the data and restores it. Deleting those is a separate act.
- **The span must be table data**; the cluster's own key space is
  refused. A `--key`/`--end-key` purge splits at exactly those keys.
- **Cost:** a flush and a compaction of the span on every replica, and
  a few small flushes of each node's raft engine. Seconds for a small
  table; for a large one, proportional to its size. `--connect-timeout`
  aside, the command waits up to 30 minutes.
- **Encrypted stores** purge the same way: the span's files are
  rewritten and the old ones deleted whichever data key they were
  written under.
- A freed disk block is the file system's: the purge removes the data
  from every file the database owns, not from the device beneath.

`datax_purge_versions_removed_total` counts what purges on a node
removed. The same protection holds every purge, whoever asks for it:
`ALTER TABLE ... PURGE`, `datax debug purge` (a key range included) and
the reclaim of a dropped table. The reclaim also waits to clear a
dropped table's rows while a backup that began before the drop is
running. A dropped table's ranges are emptied by a range clear before
their purge, once the cluster is at v22 (`datax_range_clears_total`
counts them on the leaders that proposed them); the purge then has
nothing to collect and removes the bytes from the files and logs.

**A GC window per table.** Garbage collection normally waits out the
node's window (25h). `ALTER TABLE t SET (gc_ttl = '10m')` gives one
table its own, shorter or longer; the housekeeping pass picks it up
within its descriptor cache (~30s). The window governs ranges that hold
only that table, and those ranges are not merged into neighbours with a
different window. A range that straddles the table and something else
keeps the longest window of what it holds. The GC pass splits the
table's edges when the window would change what it collects: at once
when the table holds data older than its window or the window is longer
than the node's, and not while the table is younger than a shorter
window, which has nothing to collect yet — so a table created and
dropped within its window never costs a split and a merge (#395).
`CREATE TABLE ... WITH (gc_ttl = ...)` splits the empty table's edges
at once, so every write of its rows goes through its own ranges (and a
`PURGE` truncates every log that held them). `debug ranges` shows
whether the table's ranges start and end at its edges;
`datax_gc_window_splits_total` counts the splits the GC pass made.

### Row expiry

Tables with a row TTL ([SQL guide](sql.md#row-expiry-ttl)) are served by
one job for the cluster, on the node leading range 1. Every 30 seconds
it visits each such table, reads expired rows oldest-first through the
index on the expiry column, and deletes them 100 to a transaction with
a 20ms pause between transactions — at most about 5,000 rows a second
per table, and no more than one interval spent on a table before the
next gets its turn. A backlog larger than that (a TTL just added to a
big table) drains over several passes; steady expiry keeps up inside
one.

- **Lag** — how long after its expiry time a row is actually deleted —
  is therefore about the interval under steady writes.
  `datax_row_ttl_lag_seconds` is the most overdue row the last pass
  deleted; alert on it staying far above 30.
- `datax_row_ttl_rows_deleted_total` is the work done.
  `datax_row_ttl_rows_blocked_total` counts expired rows a restricting
  foreign key kept, once per pass they were found in: steadily rising
  means rows that cannot expire until what references them goes.
- Each pass that deleted or was blocked on something is a `row-ttl`
  operation on the operations view, on the node that ran it, with the
  table and the counts; a pass that failed says why.
- The deletes are normal transactions: they appear in SQL statistics,
  can conflict with your own writes to those rows (the job retries),
  and leave old versions for garbage collection like any delete.
- If range 1's leadership moves mid-pass, two nodes may delete for a
  moment; the second finds the rows gone.

### Large objects

[Large objects](sql.md#large-objects) are rows of three system tables
— `datax_largeobject` (the pages, 256 KiB each),
`datax_largeobject_metadata` (one row per object) and
`datax_largeobject_pending` (contents waiting to be removed) — created
by the nodes a few seconds after the cluster reaches v21. What that
means for running a cluster:

- **Ranges.** Pages are keyed by content and page number, so one object
  is a contiguous run of keys and a busy cluster's uploads interleave by
  content, not by time. The page table splits by size like any table; a
  range boundary may fall inside an object and nothing notices. A range
  of pages holds about 250 of them at the default range size, and no
  page is ever too big to split around — which a single multi-megabyte
  row value is.
- **Disk.** An object takes its size times the replication factor, plus
  the Raft log's copy until the log is truncated (briefly about twice
  that during an upload). Overwritten pages and the pages of unlinked
  objects stay as old versions until the page table's GC window passes;
  `ALTER TABLE` is refused on these tables, so that window is the
  node's (25h by default).
- **The sweeper.** One job for the cluster, on the node leading range 1,
  runs every minute and deletes the pages of contents that no object
  stands for: unlinked objects, uploads that rolled back or lost their
  connection (marked on the way out, so reclaimed on the next pass), and
  uploads that have shown no sign of life for 10 minutes (a gateway that
  died mid-upload). `datax_largeobject_contents_reclaimed_total` and
  `datax_largeobject_pages_reclaimed_total` count its work, and a pass
  that reclaimed something logs one line. A non-empty
  `datax_largeobject_pending` that does not drain means the sweeper is
  not running (no range-1 leader) or cannot write.
- **Load.** An upload is a stream of 256 KiB replicated writes, about
  20 MiB/s per upload on modest disks; it holds no locks while it runs
  and does not delay backups. Several at once are a bulk load like any
  other and meet the same [backpressure](#backpressure).
  `datax_largeobject_bytes_written_total` and `..._read_total` are the
  traffic, per gateway.
- **Backups.** A full or incremental backup carries the three tables;
  a full restore brings the objects back, and the restored cluster's
  sweeper finishes whatever removals were pending at the backup's time.
  An object whose upload had not committed at the backup's time is not
  in the backup. `datax restore --table` restores user tables only: it
  does not bring back the large objects a restored table's rows refer
  to (restore everything into a scratch cluster and copy them out).
- **Memory.** Gateways need nothing extra: a transfer holds a page or a
  read's worth at a time.

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
  complete. The data files are written into a hidden
  `.backup-in-progress-*` directory inside the destination and moved
  beside the manifest only once every table is written: a backup that
  fails removes what it wrote and leaves the destination as it found it,
  and one whose node dies mid-way leaves only that directory (delete it).
  Every data file and the manifest are fsynced before they are renamed
  into place, and the destination directory after, so a backup reported
  written survives a power loss; the node's `metadata-backup.json` is
  written the same way. The directory backup creates is mode 0700 and every file 0600, sealed
  or not; a destination that already exists keeps its mode. The summary prints per-table record counts and SHA-256
  checksums over the live data. Restore reads only the names a backup
  writes, derived from each table's id, and only a regular file at one
  of them: a manifest naming any other file is refused before anything
  is opened, and a symlink or a directory at the manifest's or a data
  file's name is refused without being followed, so a directory prepared
  by someone else cannot point the node at a path of their choosing.
  The system records a backup carries (users, roles, databases,
  sequences, enum types) are written back only to the key spans backup collected
  them from; a manifest carrying any other key — the cluster's
  authentication secret, say — is refused before anything is applied.
- Incrementals capture only keys changed since the base — deletions
  included. That needs the history since the base, which garbage
  collection removes after the GC window (25h by default, or a table's
  own `gc_ttl`) and a purge removes at once. When a table's history
  since the base has been collected, the incremental holds **that table
  in full**, as it stands at the incremental's timestamp, and the rest
  as changes; the summary marks it `(held in full)` and says why in a
  note. A restore of the chain then takes the table from that
  incremental and from no backup before it, so rows a purge removed are
  not brought back from the full backup beneath. Such an incremental is
  as big as the table, so still chain within the GC window, and take a
  fresh full backup at least daily. A backup with a table held in full
  has its own manifest format (`DXBK4`): a binary from before it refuses
  it ("not a datax backup") rather than restoring the table wrongly.
- **A running backup is protected.** Before it reads anything a backup
  records the timestamp it reads at, cluster-wide, and renews the record
  every 30 seconds while it runs. Garbage collection and purges do not
  collect at or above that timestamp while the record is live, so a
  short `gc_ttl` or a purge (see [Purging deleted data](#purging-deleted-data))
  does not fail a backup in progress. The record goes when the backup
  ends; one left by a backup whose node died lapses after 2 minutes, so
  a crashed backup holds collection back for no longer than that. Only
  the backup's own timestamp is protected, not an incremental's base: a
  purge during an incremental makes it hold the purged table in full.
- **Time-series retention and backups.** Retention expires rows beneath
  SQL by commit time and leaves no tombstone, so an incremental taken
  after rows expired has nothing to say about them. A backup therefore
  keeps the commit time of every row of a time-series table, and a restore
  applies the table's retention (as of the chain's last backup) as the
  source did: a row whose commit time is at or before the chain's end less
  the retention had expired there, and is not restored. It is the commit
  time that decides, not the row's timestamp column: a row backfilled with
  an old timestamp shortly before the backup was held by the source and is
  restored, and a restore of a chain holds what the source held at the
  chain's end once its retention had caught up (expiry at the source is
  best-effort by a housekeeping tick or two). The rest are written in
  sixteen bands of commit time, oldest first, and the table's descriptor
  records when each band's rows were first written, so retention expires
  a restored row when it would have expired at the source — at most a
  sixteenth of the retention late, never early — rather than a retention
  period after the restore. The same holds for a table restore and a
  restore to a point in time (where the end is the moment restored to).
  The summary says, per time-series table, how many rows were left out.
  The cost is reads: a time-series table's data files are read once per
  band. A backup keeps commit times, and a restore records the bands, only
  at cluster version v22; before it (or for a chain with a backup taken
  before it, which the summary names) the table is restored as it was:
  expired rows may come back, and every restored row is kept a retention
  period from the restore. To apply the retention then, delete the old
  rows after the restore: `DELETE FROM metrics WHERE at < now() -
  INTERVAL '7 days'` (the retention, over the timestamp column — which
  also drops rows backfilled with old timestamps). An incremental whose
  base the retention has already passed is refused, like one older than
  the GC window. (Issues #313, #316.)
- An incremental holds each changed key as it stood at the incremental's
  own timestamp. Taken with `--revision-history` it holds every version
  written since the base instead, and the chain restores to any moment
  in between (see [Restoring to a point in time](#restoring-to-a-point-in-time)).
- Without a key, backup files are written in plaintext. On an encrypted
  store the command refuses to do that silently: pass `--backup-key` to
  seal the backup (below), or `--allow-plaintext` to write an unsealed
  one and protect the backup location yourself. Passing both is refused.

### Sealed backups

A backup is where a cluster's data leaves the protection of its stores:
rows, and also everything about them — which tables and accounts exist,
how many rows each holds, role names and password verifiers.
`--backup-key <file>` seals every file of the backup, the manifest
included, with authenticated encryption (AES-256-GCM) under a key you
supply. The key file is 32 raw bytes or 64 hex characters, like a store
key file (`openssl rand -hex 32 > backup.key`); it is a key of its own,
not the store key, and works the same on an unencrypted store.

```sh
datax backup  --addr 10.0.0.1:26257 --dest /backups/full --backup-key backup.key
datax backup  --addr 10.0.0.1:26257 --dest /backups/inc1 --base /backups/full \
              --backup-key backup.key
datax restore --addr 10.0.1.1:26257 --src /backups/full,/backups/inc1 \
              --backup-key backup.key
```

- **What is sealed.** `BACKUP.json` becomes a small envelope holding the
  backup's own random data key, wrapped under your key, and the sealed
  manifest; each `table_<id>.dxbk` is sealed in 64 KiB chunks under a
  key derived from the data key and the file's name. The file's name,
  each chunk's position and whether it is the last are part of what is
  authenticated, so a changed byte, a truncated file (at a chunk
  boundary too), chunks reordered, two files exchanged, or a file or
  manifest taken from another backup all fail to open. The summary's
  record counts and checksums are computed over the plaintext, so a
  sealed backup and an unsealed one of the same data print the same.
- **What is still visible** to someone holding the directory: that it is
  a sealed datax backup, how many tables it has, their numeric ids (in
  the file names — never their names), and each file's size.
- **Restore refuses before it applies.** Every manifest is opened and
  every data file read to its end and authenticated before the first
  record is written. A wrong key, a missing key, or any of the damage
  above leaves the target cluster untouched.
- **Sealed and plaintext do not stand in for each other.** A sealed
  backup is never read without its key, and a plaintext directory given
  to a restore together with a key is refused — anyone who can write
  the backup location can write a plaintext backup, and only a holder
  of the key can write a sealed one, so a plaintext directory where you
  named a key is not the backup you meant.
- **Keys in a chain.** Each backup of a chain may have its own key. For
  restore, `--backup-key` takes one key file, used for every directory
  in `--src`, or a comma-separated list with exactly one entry per
  directory, in the same order. Keys are paired by position; none is
  tried against another directory. An empty entry says that directory
  is a plaintext backup: `--src /backups/old-full,/backups/inc1
  --backup-key ,backup.key`.
- **An incremental's base.** `--base` is read only for its cluster id and
  end timestamp. A sealed base is opened with `--backup-key`; if it was
  sealed under a different key, name that with `--base-key <file>`. With
  `--base-key`, the base must be sealed under it; without, a plaintext
  base is accepted (so sealing can start mid-chain).
- **How the key travels.** The serving node writes and reads the backup,
  so the command reads the key file and sends the key in the admin
  request. On a secure cluster that is mutual TLS. On an insecure
  cluster the request is cleartext, so a node accepts a backup key only
  from its own machine: run the command there, against a loopback
  address. The key is never logged or written to the audit log (the
  audit record notes only that a key was given), is not stored on the
  node, and is not written into the backup. During a rolling upgrade,
  point a sealed backup or restore at an upgraded node: the command asks
  for it under an op name (`backup-sealed`, `restore-sealed`) that a node
  from before sealed backups refuses, where it would otherwise have
  ignored the key and written plaintext.
- **Losing the key loses the backup.** There is no escrow and no second
  way in. Keep the key apart from the backup location; a key stored
  beside the files it seals protects nothing.
- `--enc-root` roots (a KMS command, operator shares) do not hold backup
  keys yet: a backup key is always a key file.

### Restoring everything

`datax restore` applies a chain (full first, then incrementals in order)
into an **empty** cluster — it refuses if any table exists:

```sh
datax restore --addr 10.0.1.1:26257 --src /backups/2026-08-31,/backups/2026-08-31-noon
```

Table IDs are preserved, so secondary indexes and timeseries shard layouts
restore as-is with no backfill; the descriptor ID generator is bumped past
every restored ID. Enum types come back with their ids, so the restored
columns are still of them. A backup taken before 0.59.0 holds no enum
types: restoring everything from one brings back the enum columns
(their labels travel with the column) but not the `CREATE TYPE`. A
table restore (`--table`, `--database`) from such a backup makes the
type from the column's labels, so that is the way to bring an enum
column back from one with its type. Users, admin-role markers, and per-table grants come
from the backup — after a restore, **credentials are the source cluster's**.
Before anything is written, every data file of the chain is read to its
end and held to its manifest: the record count and checksum the backup
recorded for it, its table's key span and key order. A damaged file, or
one swapped for another backup's file of the same table, is refused and
the target stays empty. (This is not a seal: whoever can rewrite a file
can rewrite the manifest beside it. A sealed backup authenticates both.)
The files are read once more as they are applied, and a file changed in
between fails that check at its end, after its earlier rows are written;
start again from an empty cluster. The restore summary re-exports every
table and prints fresh checksums: compare them against the backup's to
verify the restore byte-for-byte.

MVCC history is not preserved (rows are rewritten at restore time), so
`AS OF SYSTEM TIME` cannot see below the restore.

### Restoring a table into a running cluster

`--table` brings named tables back from a chain into a cluster that is
serving traffic, beside what is there. Nothing else is touched: no other
table, no user or role, no setting.

```sh
# one table, under a new name, to compare and copy rows from
datax restore --addr 10.0.0.1:26257 --src /backups/full,/backups/inc1 \
              --table shop.orders --into orders_restored

# several at once (names pair up in order; an empty --into entry keeps the name)
datax restore --addr 10.0.0.1:26257 --src /backups/full,/backups/inc1 \
              --table shop.orders,shop.customers --into orders_r,customers_r

# every table of a database, as a new database
datax restore --addr 10.0.0.1:26257 --src /backups/full --database shop --into shop_restored
```

A table is named `db.table` (or `table`, in the default database
`datax`). `--into` takes `table` to stay in the same database or
`db.table` to land in another, which must exist. `--backup-key` works as
for any restore. Needs cluster version v21.

- **It never overwrites.** A name that exists is refused. To replace a
  live table with its restored copy, restore under another name, check
  it, then `DROP TABLE orders; ALTER TABLE orders_restored RENAME TO
  orders`. There is no flag that does the swap for you. A database is
  always restored as a new one: `--database shop` without `--into` is
  for a database that is gone.
- **A new table id.** The restored table takes a fresh id and its rows
  are re-keyed to it; index ids are kept, so secondary indexes come
  back as stored, with no backfill. The summary prints the new id, the
  table it came from, and a checksum computed over the restored rows
  *under the keys the backup knows them by* — the same checksum a full
  backup of the source taken at that moment prints for the table.
- **What comes with a table.** Its columns, indexes, constraints,
  comment, timeseries options and retention; the sequences its `SERIAL`
  and identity columns own, as new sequences named for the new table,
  continuing from the backup's counter; its grants and owner, for the
  roles this cluster has — grants to roles it lacks are left out and
  listed in the summary, and a table whose owner is missing is owned by
  `root`. An enum column's type: the type of that name in the target
  database when its labels are the same, in the same order; created
  there from the backup when the database has none (the summary says
  so). Statistics are not in a backup; the table is sampled like any
  new one.
- **Foreign keys.** A foreign key to a table restored in the same run
  follows it: restore `orders` and `customers` together and the copy of
  `orders` references the copy of `customers`. A foreign key to a table
  that is *not* being restored is **refused** — the restored rows would
  be checked against a table that has moved on since the backup. Pass
  `--drop-foreign-keys` to restore the table without those constraints
  (the summary lists each one dropped). Tables that referenced the
  original keep referencing the original.
- **What is refused,** each with the reason: a view (restore its tables
  and recreate it; a database restore skips views and prints their
  definitions); a table with an enum column whose target database has
  a type of that name with other labels (a value is stored as its
  label's position); a table that was mid-re-shard, mid-`CREATE INDEX` or mid-`ALTER COLUMN
  TYPE` when backed up; the cluster's own tables; and a table whose
  descriptor carries a feature this version's table restore does not
  know. For all of these, restore the whole backup into an empty
  cluster and copy the rows from there.
- **Checked before anything is written.** The chain, the names, the
  foreign keys, and every data file the restore needs: each is read to
  its end first — which authenticates a sealed file — and held to its
  manifest's record count and checksum, to its table's key span, and to
  key order. A refusal leaves nothing behind, not even a table id.
- **Appears all at once, or not at all.** Rows are loaded under the new
  id in ordinary transactions of 1024 writes — replicated, encrypted
  and versioned like any write, and unreachable by any statement,
  because no table has that id yet. They are then read back and their
  checksum compared with the backup's. One transaction publishes
  everything: descriptors, names, sequences, the database. Before it
  commits the tables do not exist; after it they are complete.
- **If it is interrupted.** A restore that fails removes what it loaded.
  If the node running it dies, the rows stay invisible and a janitor
  removes them: the node itself shortly after it restarts, or any other
  node five minutes after the restore's last sign of life. Either way
  the operations view shows a `restore-reclaim`. A restore is not
  resumed: run it again.
- **Cost.** The data files are read twice (checked, then loaded) and the
  loaded rows once more. The load is ordinary write
  traffic on new ranges and competes with other writes like any bulk
  insert. Placement policy applies to the new table once it is
  published; a database restore brings the database's policy with it.
- A timeseries table with a retention keeps it: rows already past it
  are restored and then expire as they would have.

### Restoring to a point in time

A chain restores to the end of any backup in it. Incrementals taken with
`--revision-history` also keep every version written since their base,
and `--as-of` then reaches any moment of that interval:

```sh
datax backup  --addr 10.0.0.1:26257 --dest /backups/sun-full
datax backup  --addr 10.0.0.1:26257 --dest /backups/mon-06 --base /backups/sun-full --revision-history
datax backup  --addr 10.0.0.1:26257 --dest /backups/mon-12 --base /backups/mon-06   --revision-history

datax restore --addr 10.0.0.1:26257 --src /backups/sun-full,/backups/mon-06,/backups/mon-12 \
              --as-of 2026-11-16T09:41:00Z --table shop.orders --into orders_0941
```

- `--as-of` takes an RFC 3339 time or Unix nanoseconds — the same
  timestamp `AS OF SYSTEM TIME` takes, so a moment you checked with a
  query is the moment you restore to. Backup and restore summaries
  print their timestamps in the nanosecond form (`as of: ...`), with
  `,<n>` appended in the rare case the clock's logical counter was not
  zero; pass that back verbatim to restore to exactly a backup's end.
- It works with `--table` / `--database` (into a running cluster) and
  without (everything, into an empty one).
- **The restored data is the committed data at that moment.** A
  transaction's writes all carry its commit timestamp, so a restore
  shows every transaction whole or not at all, across however many
  ranges it wrote.
- **What is refused,** with the times the chain does restore to: a
  moment before the full backup, after the last backup, or inside an
  interval whose incremental was taken without `--revision-history`.
  `--revision-history` on a full backup is refused too: a full backup
  is the floor of the window.
- **A table created or redefined inside the interval** is refused at a
  moment inside it: a backup holds each table's definition at its end
  only, so the definition at the moment is not known. Restore to either
  end of that interval instead. The cluster's own tables (`datax_audit`,
  the metrics table) are the exception when a node created them inside
  the interval. Their definition comes with the binary, so they come
  back with their rows as of the moment, or empty if they did not exist
  yet. A restore of everything into a cluster that already has tables
  is refused before any of this is checked.
- **History costs space in proportion to the write rate**: a history
  incremental holds one record per version, where a plain one holds one
  per changed key. The summary prints both (`records`, `versions`).
- **History must still exist to be backed up.** Versions older than the
  GC TTL (25h by default) are collected, and a purge collects a table's
  at once. An incremental whose base is older than what a table still
  has holds that table in full instead of its history, rather than
  writing it with a hole in it: the chain then restores that table to
  the incremental's end, and a moment inside its interval is refused
  for it ("holds it only as it stood then"), as for a table created in
  the interval. So take history incrementals more often than the GC
  TTL, and leave margin: nothing holds collection back for a chain that
  falls behind. Rows a timeseries table's retention has expired are
  gone from history like any other collected version.
- **The schema is not versioned.** A backup holds each table's
  definition, and the cluster's roles and sequences, as of the backup's
  end. A restore to a moment *inside* an interval therefore refuses a
  table that was created or altered in that interval (grants and
  comments do not count) — restore it to either end of the interval
  instead — and a table dropped in the interval is restorable up to the
  last backup that still has it. For a restore of everything, one such
  table refuses the restore; use `--table` for the others. Roles,
  grants and sequence counters come from the interval's end.
- Needs cluster version v21 to take; a sealed chain restores to a moment
  exactly like a plaintext one. A binary from before v21 refuses a
  history backup ("not a datax backup") rather than misreading it.

**Worked example: a dropped table.** `shop.orders` was dropped at about
14:20; the last backup before that is `/backups/mon-12`.

```sh
datax restore --addr 10.0.0.1:26257 --src /backups/sun-full,/backups/mon-06,/backups/mon-12 \
              --table shop.orders --drop-foreign-keys
```

The name is free, so the table comes back under it, as of 12:00, with
its indexes, its `SERIAL` sequence and its grants. `--drop-foreign-keys`
is there because `orders` referenced `customers`, which is live and is
not being restored; put the constraint back once you have looked at the
rows (`ALTER TABLE orders ADD CONSTRAINT ... FOREIGN KEY ...`). Rows
written between 12:00 and the drop are in no backup: a table dropped
inside an interval is restorable to the interval's start, not into it.

**Worked example: a bad UPDATE.** At 09:42 an `UPDATE orders SET status =
'cancelled'` ran without its `WHERE`. The 12:00 incremental was taken
with history, so:

```sh
# 1. find the moment: the last time the data was right
datax sql -e "SELECT count(*) FROM shop.orders AS OF SYSTEM TIME '2026-11-16T09:41:00Z' WHERE status = 'cancelled'"

# 2. bring the table back as of then, beside the live one
datax restore --addr 10.0.0.1:26257 --src /backups/sun-full,/backups/mon-06,/backups/mon-12 \
              --as-of 2026-11-16T09:41:00Z --table shop.orders --into orders_0941 --drop-foreign-keys
```

```sql
-- 3. compare, then put back what the UPDATE destroyed
SELECT count(*) FROM orders_0941 WHERE status <> 'cancelled';
BEGIN;
DELETE FROM orders WHERE id IN (SELECT id FROM orders_0941);
INSERT INTO orders SELECT * FROM orders_0941;
COMMIT;
DROP TABLE orders_0941;
```

Orders created after 09:41 are not in `orders_0941` and are left as they
are. While step 1 still works — the mistake is younger than the GC TTL —
the cluster has the history itself and can be queried `AS OF SYSTEM
TIME` directly; the backup is for when it no longer does, and for
having the old rows as a table to join against.

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
  was finalized is not supported"), including a node that was down
  during the finalize (its peers tell it the version); a too-old binary
  is also refused at join time. A running node on an older binary that
  sees the version finalized past it stops serving rather than run at
  it.
- Finalize is deliberate, not automatic: verify the upgraded cluster looks
  healthy first, because finalize is the point of no return.
- `datax debug upgrade` names any node still on the old binary instead of
  finalizing — nothing to time or coordinate. That counts **every** node
  that may hold replicas, not only the live ones: a node that is down
  counts as running the binary it last reported, and a draining node
  counts until no replica is left on it. For a node that is down, either
  bring it back on the new binary or decommission it (`datax debug
  decommission --node N`) and let its replicas move off (a dead node's
  replicas move once it is declared dead); a fully decommissioned node
  no longer blocks finalize.
- **v13 needs one more rolling restart after finalize**: each node's next
  start migrates its store to the split layout (the raft log on its own
  engine under `--dir/raft`, the state engine without a WAL; see
  [Deployment → Store layout](deployment.md#storage-profiles)). Until
  that restart the node keeps running on one engine. A migrated store
  refuses a v12 binary.
- **v24 makes an `INSERT` check uniqueness in its write.** Once it is
  finalized, a plain `INSERT` (no `ON CONFLICT`, not an `UPSERT`)
  writes each row's primary key, and each unique index entry, with a
  conditional put that fails if the key already holds a value, so the
  check and the write are one KV batch: an autocommit single-row
  `INSERT` is one one-phase commit instead of a read and then a commit.
  Errors are the same as before. Until finalize a node on the new
  binary reads the keys first, as before: an older node does not know
  the conditional put.
- **v25 makes transaction status recovery check the record it ends.**
  Once it is finalized, the request that ends the recovery of a
  crashed parallel commit names the staged record it checked, and a
  record that has changed since is left for a fresh recovery. Nothing
  changes for clients. Until finalize recovery ends the record as
  before: an older node would ignore the check.
- **v26 makes console sessions revocable.** Once it is finalized,
  root can run `datax debug rotate-session-secret` to revoke every
  outstanding console session (see
  [Security → Signing in to the console](security.md#signing-in-to-the-console)).
  Until finalize the command is refused: an older node would keep
  accepting the revoked tokens. Nothing else changes.
- **v23 turns on range-safety checks at apply** (issue #389). Before
  finalize, a node on the new binary already re-checks that a write
  still belongs to its range once it holds its latches, and a split draws
  a new range ID if the one it drew is in use. Three checks run on every
  replica only after finalize. A replica refuses a write ordered after a
  split or merge that moved its key, a split whose new range ID is
  already in use on the store, and a merge of the wrong right-hand range.
  An older binary would apply what these refuse, so they wait for
  finalize.
- **v21 starts the audit table.** Until it is finalized audit records
  are in the node logs only, as before; from finalize on every node also
  writes them to `datax_audit` (see [Security → Audit
  table](security.md#audit-table)). Records from before finalize are not
  copied in.
- **v21 also gates two backup features**: `datax backup --revision-history`
  and `datax restore --table` / `--database` are refused until it is
  finalized. Plain backups and restores work as before on either side.
- **v20 makes manual splits sticky.** Until it is finalized `ALTER TABLE
  ... SPLIT AT` and `datax debug split` split as before and housekeeping
  may merge the ranges back; boundaries made before finalize are not
  marked afterwards either — run the `SPLIT AT` again (it is idempotent)
  to mark them.
- **v21 adds per-table GC windows, row expiry and purge.** Until it is
  finalized the `gc_ttl` and `ttl` table options, `ALTER TABLE ...
  PURGE` and `datax debug purge` are refused (`0A000`): a node on the
  old binary would collect a table at the node's window rather than the
  table's, expire nothing, and drop the options from the next
  descriptor it rewrote. v21 binaries also change one thing without the
  finalize: a restarted node serves no follower reads from a range
  until it has heard from that range's leader again (a few seconds).
- **v21 also allows GIN indexes on JSONB columns.** Until it is finalized
  `CREATE INDEX ... USING GIN` is refused (`0A000`): a node on the older
  binary would maintain such an index as an ordinary one. No existing data is
  rewritten.
- **v21 also allows full-text search columns and indexes.** Until it is
  finalized a table cannot be given a `TSVECTOR` or `TSQUERY` column
  (`CREATE TABLE`, `ADD COLUMN`, `ALTER COLUMN TYPE` and `CREATE TABLE
  AS` are refused with `0A000`), and neither kind of full-text GIN index
  can be created: a node on the older binary cannot decode such a row.
  The functions and the `@@` operator store nothing and work at once.
- **v21 also adds `VECTOR` columns and their `ivfflat` index.** Until
  it is finalized a `vector` column (`CREATE TABLE`, `ADD COLUMN`,
  `ALTER COLUMN TYPE`) and `CREATE INDEX ... USING ivfflat` are refused
  (`0A000`): a node on the older binary cannot read a row holding a
  vector, and would maintain the index as an ordinary one. The distance
  operators and functions work on values at either version.

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

A node can also lose replicas while it stays up: a storage failure
(a full disk, an I/O error) stops each affected replica's raft group
until the node restarts, and a replica that never reached disk before a
crash is not loaded at all. The node keeps heartbeating, so dead-node
repair leaves those replicas alone, but it reports them: the health
checks `replicas-failed` and `replicas-missing`, the gauges
`datax_replicas_failed` and `datax_replicas_missing`, and
`quorum-lost` / `under-replicated` count them as unavailable. Fix the
disk and restart the node. A replica still missing after the restart
does not come back on its own: decommission the node so its ranges are
re-replicated elsewhere, or use `unsafe-recover` below if quorum is
gone.

**Quorum loss** (2 of 3 replicas gone) makes affected ranges unavailable.
The last resort, on a **stopped** surviving node, discards the lost
replicas and rewrites the survivor as a 1-replica group:

```sh
datax debug unsafe-recover --dir /var/lib/datax --yes    # [--range N]
```

This can lose recently committed writes on the dead replicas — hence
"unsafe". `datax debug metadata --dir ...` inspects a store's metadata
(works on encrypted stores with `--enc-key`).

- **v21 also adds large objects.** Until it is finalized the `lo_*`
  functions are refused (`0A000`) and nothing creates their tables: a
  node on the older binary knows nothing of the reservation and would
  let the tables' owner delete pages out from under an object.
- **This release changes three behaviours without a version gate**, all
  found by measuring large values and all safe in a mixed cluster. A
  gateway refuses a write batch of more than 60 MiB for one range
  (`54000` to the SQL client) instead of sending it: such a batch could
  not be replicated, failed with `XX000`, and left the range's
  leadership churning around the entry. A
  store on the balanced profile now starts an L0 compaction at 200
  files rather than 500: files that do not overlap (a stream of writes
  in key order) used to pile up to the 400-file backpressure gate and
  stay there, shedding writes for good. And a transaction's record is
  created with a current heartbeat: a transaction that made its first
  write more than five seconds after `BEGIN` could be aborted by any
  other transaction that met its row in the following second.

## Backpressure

When the LSM falls behind (L0 too deep, memtables full), nodes shed
table-data writes with a retryable storage-overload error rather than
letting the engine hard-stall. A short burst is absorbed: the gateway
retries a shed write with jittered backoff (10ms doubling to 1s) for up
to 5 seconds, and the client sees latency. Overload that lasts longer
fails the statement with SQLSTATE `40001` ("restart transaction: ...
storage overloaded, write shed: ..."), and so does a statement whose own
deadline (`statement_timeout`) would pass during the next backoff — it
fails before the deadline, not at it. Retry such a statement as any
other `40001`, with backoff; the statement is not retried again inside
the server, since that would only multiply the wait. Three limits feed
the same shed path, told apart by `datax_storage_backpressure_cause_total`:

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

Dropped tables are not reclaimed while the node is shedding writes:
the reclaim of each one forces a memtable flush and a compaction, more
small L0 files for an engine whose L0 is the problem. They are taken up
again once it recovers.

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
datax bench jsonb-contain --preload 50000 [--jsonb-index none]   # JSONB containment: through a GIN index, or as a filter
datax bench text-search --preload 1000000 --groups 20000 [--text-query word|phrase|prefix|ranked] [--text-index none]   # full-text search: through a GIN index, or as a filter
datax bench vector-knn --preload 50000 --dims 64 [--vector-index none] [--lists 200] [--probes 5]   # nearest neighbours: through an ivfflat index (with its recall and build time), or exactly
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

A run exits non-zero when its measured phase counts more errors than
`--max-errors` (default 0; `-1` to never fail on errors). Serialization
retries (40001) are counted separately and are not errors. `datax bench
run` still runs every workload of the set and then names the ones that
failed, each with its error count and most frequent error; a workload
in the set can carry its own tolerance (`"max_errors"`). The nightly is
red on exactly that exit status.

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
  encoding, the uniqueness check (from v24 a conditional put in the
  write batch itself; before it, one read round trip per statement) and
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
- **Many tables cost the catalog little; many ranges cost what ranges
  cost.** Listing one database's tables and resolving one table read
  the same number of keys with 1,000 tables in the cluster as with
  5,000 (measured on an in-process three-node cluster, fifty tables to
  a database: 53 point reads and one scan to list a database, 4 point
  reads to resolve a table cold). A gateway caches at most 1,024
  descriptors and gives up the lease of any it has not used for 30 s,
  so one that touched every table once is back to no lease traffic half
  a minute later; a workload that keeps more than 1,024 tables hot on
  one gateway pays two point reads per statement for the overflow. The
  schema page costs one build per five seconds while somebody has it
  open — 51 ms and 1 MB of descriptors at 5,000 tables — and it, the
  per-table gauges and a
  few background passes (statistics refresh, retention, backup) are
  what still read every descriptor. Small tables do not get a range
  each: ranges split by size, and 5,000 one-row tables shared nine
  ranges in that test, at roughly 4–16 KiB of heap per table per node
  (the lower figure after this change; it includes storage engine and
  raft state and is an observation, not a guarantee). A table does
  occupy at least one range of its own when it is a timeseries table or
  its database carries a placement policy, and a range is a raft group
  on three nodes; tens of thousands of such tables are a range-count
  question (see the range and raft numbers above), not a catalog one.
  Beyond 5,000 tables is untested.
- **Reads scale with replicas** for historical (`AS OF SYSTEM TIME`)
  queries, which any replica can serve; current reads go to leaseholders,
  which the load balancer spreads by QPS.
- Budget disk at **~2× the logical write rate** (raft log + LSM) plus
  compaction; when in doubt run `datax bench ingest --metrics-url ...`
  against a staging cluster and watch the storage metrics it reports.
