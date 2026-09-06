# Changelog

Releases of datax, newest first. The version is `pkg/version.Release`,
bumped in the pull request that changes behavior (minor for a new
capability, patch for a fix); a git tag `vX.Y.Z` on `main` marks the
release, and the build workflow stamps binaries with the tag or with
`vX.Y.Z+<commit>` between tags. The cluster protocol version (`v1`, `v2`,
... in `pkg/version`) is separate: it changes only when the replicated
state or the internode protocol does, and an entry below says so.

## 0.51.0 — unreleased

### Added
- **Transactions and contention on the console's `#/sql` view** (#154).
  A serializable database lives and dies on retries, and the console had
  no transactions section: `txn.commits`, `txn.aborts`, `txn.retries` and
  `kv.batch_p99_us` were recorded every interval and reachable only by
  knowing to pick them out of the metrics picker. The one
  transaction-shaped figure on the page was a `40001/s` column.

  The view now opens the four series over the header's time range, one
  line per node, with tiles for the cluster's commit, abort and retry
  rates and the share of work being redone. KV batch latency is charted
  beside statement latency, which is what tells a slow statement from a
  slow cluster; the two latency tiles name the worst node rather than
  averaging, because there is no p99 of two p99s.

  A rate does not say what to change, so two panels say who and what.
  **The retry hot list** attributes every serialization failure to the
  statement shape that produced it — the statement with its literals
  replaced, so a retry storm is one row rather than a thousand — and to
  the user who ran it. **Idle in transaction** lists the sessions holding
  an open transaction with the user, the client, the application, how
  long they have been idle, how long the block has been open, and the
  statement they last ran: "oldest idle transaction: 3m" named nobody to
  talk to.

  The split follows the data, not the panel: rates are not sensitive and
  need no role, while statement shapes and sessions carry data and stay
  behind the admin gate that already covers `/api/activity`. Both new
  panels are one node's own counts and say so; nothing here is summed
  across the cluster.

  The shape table is bounded, and failures past the bound are counted in
  an overflow total rather than dropped or allowed to grow the map
  without limit.

### Fixed
- Charts drawn outside the metrics view no longer carry that view's
  event marks. The marks come from a fetch `#/metrics` makes for its own
  window, so a node page or the new transactions charts could show
  whichever window that view last looked at. Annotation is now something
  a chart asks for (#155 follow-up).

## 0.50.0 — unreleased

### Added
- **An operations timeline** on the console's `#/ops` view (#153). The
  event ring recorded instants, so a decommission that started twenty
  minutes ago and one that finished last week read identically: a
  timestamp, a kind, a summary. A rebalance storm looked like a quiet
  afternoon with a lot of old rows in it.

  An event may now carry an operation id, a phase and an outcome, and
  four long-running operations record both ends under one id — backup,
  restore, re-encryption sweeps and decommission drains. The view splits
  them into **in flight**, with how long each has been running, and
  **recently completed**, with its outcome and duration. Progress that
  cannot be measured is shown as elapsed time, never as a bar with a
  number nobody measured.

  The ring stays the audit trail it is: the pairing is a label on the
  records and the view is derived from it, not a job store. An operation
  whose start has already aged out of the ring is still listed, with no
  duration claimed for it.
- **Cluster events marked on the metrics charts** (#155). "Why did p99
  jump at 14:20" was a two-window investigation — read the chart on
  `#/metrics`, scroll the event log on `#/ops`, match timestamps by eye —
  although both sets are on the same page and the same clock.

  Charts now carry a tick per event in the window they draw, hovering to
  the kind, time and summary, drawn beneath the data lines so a mark
  never obscures the line it explains. Structural kinds (splits, merges,
  rebalances, repairs, decommissions, restarts, upgrades) are marked by
  default, with a control for every kind or none: a layer that draws
  three hundred marks is noise.

  `/api/events` grows a time window (`?from=`) and answers with the
  timestamp of the oldest record the ring still holds. A chart drawing a
  seven-day window over a ring covering two hours says so, rather than
  implying five quiet days.

## 0.49.0 — unreleased

### Added
- **Replication status and a failure-domain view** in the console
  (#152). Rack-aware placement is the product's headline claim and the
  console could not show whether it was holding: localities were a text
  column, and the health checks reported one example range each.

  `#/data` now opens with every range bucketed by replication state —
  healthy, under-replicated, over-replicated, no quorum, undiverse —
  each bucket expanding to its ranges. A range is measured against its
  own target replica count, so a database carrying a placement policy
  (#176) is judged by that policy rather than the cluster default, and
  counted once in its worst state.

  Below it, a **failure domain** table: per locality tier value, the
  nodes, replicas and leases it holds, and what losing the whole domain
  would cost — the ranges that would drop below a majority, and the
  ranges that would survive with no margin left. That is the question
  asked before every maintenance window, and it was previously
  answerable only by hand from the range list.

  And **range hotspots**, the heaviest and largest ranges each node
  advertises in its heartbeat. QPS there is the leader's own rate over
  the ranges it leads; it is not summed across nodes, and the view says
  so rather than implying a cluster total.

  The whole section is computed from the range descriptors and the node
  registry the serving node already holds — no fan-out, so it answers
  from a partitioned node like everything else on the page — and it is
  computed on the server rather than in the page, because it is a
  contract every node must agree on: two nodes reading the same
  descriptors bucket them identically, which is what the new
  `testcluster` test holds them to.

  `/api/cluster` gains a `replication` section and per-node
  `hot_ranges` / `big_ranges` (straight from the heartbeats the
  allocator already reads).

## 0.48.1 — unreleased

### Fixed
- A point read could miss a key that is there, on any table written
  before cluster version v15 (#178). `mvccSplit` decided the prefix
  boundary from the key's tail, which is ambiguous for a key datax did
  not write: an index-block separator in a pre-v15 table is a valid
  prefix followed by the *first few bytes* of a timestamp, because
  Pebble's default comparer truncated it with no suffix to respect.
  Reading such a key's whole length as its prefix made that "prefix" a
  strict extension of the prefix of the very keys it separates, so the
  two ordered one way whole and the other way by prefix — and
  `SeekPrefixGE` could then walk a legacy table's index to the wrong
  block and report the key absent.

  `Split` now finds the boundary by searching for the first `0x00 0x01`
  from the left. The escaping guarantees that is the terminator (a
  `0x00` in the user key becomes `0x00 0xff`), and it makes the prefixes
  prefix-free, which is the property `Compare` needs: a key whose prefix
  extended another's would carry that other terminator and be cut there
  instead. Under `-race` — where Pebble compiles in its assertion
  comparer — the old rule panicked instead of answering wrongly, which
  is how this surfaced; a non-race `go test ./...` never showed it.

  Existing stores need no rewrite and the key schema keeps its name: the
  two rules agree on every key datax writes, and differ only on the
  separators older tables already carry.
- `Comparer.ImmediateSuccessor` no longer loops forever (#178). It
  appended `0x00` until the result was its own prefix, which under the
  corrected `Split` never happens — a terminated prefix has no prefix-key
  extensions, because its own terminator stays the first one. The
  smallest prefix key above `esc(K) 0x00 0x01` is `esc(K) 0x00 0x02`, so
  it increments the terminator rather than extending the key. Pebble asks
  only for "the smallest prefix key larger than a"; nothing relied on the
  successor extending its prefix.

## 0.48.0 — unreleased

**Cluster protocol v16.** A database descriptor may carry a replica
placement policy. A v15 node reads the descriptor but not the policy, so
it would allocate replicas anywhere and undo the placement a v16 node
just made; the DDL that writes a policy is refused until the cluster
finalizes v16 with `datax debug upgrade`. Nothing else changes: a
descriptor written before v16 reads the same at either version.

### Added
- **Region-restricted replication** (#176). A database can say where its
  replicas may live and how many there are, and the allocator honours it
  for every range of that database's tables:

  ```sql
  CREATE DATABASE eu WITH (replicas = 3, constraints = ('region=eu-west-1', 'region=eu-central-1'));
  ALTER DATABASE eu SET (constraints = ('region=eu-west-1'));
  ALTER DATABASE eu SET (replicas = 5);      -- constraints untouched
  ALTER DATABASE eu SET (constraints = ());  -- lift the restriction
  SHOW PLACEMENT FOR DATABASE eu;
  ```

  `constraints` is a disjunction — a replica may live on any node whose
  locality carries any one of the listed `key=value` tiers — so naming
  two regions means "either of these". `replicas` overrides the cluster
  replication factor for that database alone; it must be odd and at most
  9. An option an `ALTER` does not name is left as it was, so the count
  and the constraints are set independently, and an empty constraint
  list is how a restriction is lifted.

  Up-replication, dead-node repair, decommission drain and both
  rebalancing passes now allocate within the policy, maximizing failure-
  domain diversity inside what the policy admits — a database pinned to
  one region still spreads across that region's racks. A new pass moves
  replicas that a policy no longer admits onto nodes that do, one range
  per tick, which is what makes `ALTER DATABASE ... SET` take effect on
  data that already exists.

  Writing a policy splits its tables into their own ranges and stops
  those ranges merging with neighbours under a different policy: a range
  inherits a policy only when it lies wholly inside one table, since a
  range straddling two tables could belong to two databases asking for
  different things.
- Two health findings and a counter for placement (#176):
  `placement-unsatisfiable` (critical) when no live node satisfies a
  range's policy, `placement-misplaced` (warning) while the allocator is
  still moving replicas home, and
  `datax_placement_replicas_moved_total`. When a policy cannot be met the
  allocator does nothing rather than placing a replica outside a region
  an operator named — the finding is how that is reported.
- `SHOW PLACEMENT [FOR DATABASE name]` reports the replica count the
  allocator will actually use, so a database with no policy of its own
  shows the cluster default and says where the number came from (#176).

## 0.47.0 — unreleased

### Fixed
- `datax sql` could not edit a statement once it spanned lines (#175).
  The shell composed a multi-line statement by calling a single-line
  editor once per line, so by the time the cursor reached column 0 of a
  continuation line the line above was already gone and backspace simply
  stopped. The whole statement is now the editor's buffer: backspace at
  the start of a line joins it to the one above and leaves the cursor at
  the join, forward delete at the end of a line pulls the next one up,
  and the arrows move through the statement rather than within one line
  of it.

### Added
- `Ctrl+←` and `Ctrl+→` move by a word in `datax sql` (#175). They did
  nothing before: the editor decoded `ESC [ 1 ; 3 C/D`, which is
  Alt+arrow, and had no case for `ESC [ 1 ; 5 C/D`, which is what
  Ctrl+arrow sends in every common terminal. `Alt+←`/`Alt+→`, `Alt-b`/
  `Alt-f` and `Ctrl+Alt+arrow` are accepted as the same motion, and a
  word is a SQL identifier part, so `Ctrl+←` through
  `orders.customer_id` stops at each half.
- The shell keeps its history, `Ctrl-A/E/K/U/W`, `Ctrl-L` and `Ctrl-D`,
  and gains `Ctrl-C` to abandon the statement being typed without
  leaving, `Alt+Backspace` and `Ctrl+Delete` for word-wise deletion, and
  bracketed paste — a pasted multi-line statement lands as text instead
  of running itself a line at a time (#175).

## 0.46.0 — unreleased

### Fixed
- The console showed the overview's sections underneath every other
  view, and the node page's own SQL, network, settings and events
  sections on the overview instead of on the node page: 0.44.0 shipped
  two `<main id="view-main">` containers, and the router could only ever
  hide the first (#151).

### Added
- The console is eight views behind one persistent nav, each a real
  route (#151): overview, nodes, data, sql, schema, metrics, ops and
  security, with each node's page at `#/node/N`. Three controls cross
  every view from the header — a **node scope** the node-scoped panels
  (statements, the event ring, audit) read instead of assuming the
  serving node, and which they name rather than leaving it to be
  guessed; a **time range** shared by every chart; and **jump to**
  (`⌘K`), resolving `n3`, `r128`, a table name or `rack=b` to the right
  view. The scope, the range, each view's filter and an open range
  drill-down all live in the URL, so a view is a link worth sharing, and
  each view remembers its scroll position. Health findings now link to
  the view that shows the figure: they used to link to `#sec-*` anchors,
  which the router read as an unknown route and resolved to the
  overview, stopping the other views' polls on the way.
- The console's script is kept as one file per view plus a shared core
  under `pkg/server/ui/js`, assembled into the single page the node
  serves (#151). No build step and no second request: the page stays
  self-contained for airgapped nodes. A missing, empty or misordered
  script file fails the build or a test rather than blanking the
  console.
## 0.45.0 — unreleased

### Added
- The console has a front door (#158). An unauthenticated browser
  navigation now gets a sign-in page instead of the browser's own
  credential dialog: it names the cluster, node, locality and version
  actually reached, says whether the cluster is secure, and explains
  that the credentials are database credentials. `POST /api/login`
  verifies them through the same path HTTP Basic uses and sets a session
  cookie (`HttpOnly`, `Secure`, `SameSite=Strict`, 12 hours);
  `POST /api/logout` and the console's new user menu — who you are, how
  you got in, which roles you hold — clear it. The cookie is a signed
  token carrying no password and needing no session store, keyed from
  the cluster's authentication secret, so any node accepts a token any
  other minted; roles are still resolved per request, so revoking
  `LOGIN` or admin takes effect at once rather than at expiry. Sign-ins,
  sign-outs and every refused or lapsed token are audited. Machine
  clients are untouched: `curl`, Prometheus and `datax debug` still get
  the `WWW-Authenticate` challenge (the doors are told apart by
  `Accept`, never by user agent), and HTTP Basic and client certificates
  authenticate every route exactly as before.
- The console states insecure mode across the top of the page rather
  than in a grey aside: a cluster that authenticates nobody says so
  (#158).

## 0.44.0 — unreleased

The console umbrella (#144), first batch.

### Changed
- Two published JSON names in `/status`, `/api/cluster` and `/api/node`
  (#146): the machine sample's load averages are `load1`, `load5`,
  `load15` (they were the untagged `Load1`, and the other two were not
  exposed), matching the heartbeat's `load1`; and the storage snapshot
  (`storage`, `raft_storage`) is snake_case throughout (`l0_files`,
  `compaction_debt_bytes`, `block_cache_hits`, ...) instead of Go-cased.
  Anything that read the old spellings must move; the console did in the
  same change.

### Added
- The console reports the cluster, not the node that served the page
  (#145). `/api/cluster` carries a `rollup` summed over the live nodes'
  heartbeats — QPS, data, leases, ranges and replicas, connections (by
  state and by user), statements and serialization failures (the page
  differences polls for rates), the worst p99 and the node that owns it
  — with the count of contributing nodes beside each sum, so a node that
  is down shows as a smaller count rather than a smaller figure; the same
  numbers come out whichever node is asked (`TestClusterRollup`). The
  overview's tiles are the rollup, the node table is a strip of cards
  (status, locality, cpu, load, memory, disk headroom, fds, leases, data,
  qps, connections) linking to each node's page, and the sections that
  described the serving node — its replicas, events, machine and storage
  health — live only on that page now; the header says which node served
  the page, as provenance. Health findings about the serving node's
  storage or events link to its page.
- `GET /api/overview` (#147): the cluster document, the health problems
  and the tail of the event ring in one document — what the overview
  draws, one request per poll instead of one per section — with a
  per-section `errors` map, so a section that cannot be produced is
  absent and named rather than failing the request; the individual
  endpoints stay. The console runs every poll through one scheduler:
  each view starts only the fetches it shows (the Metrics view fetches
  no schema and no statements), a hidden tab polls nothing and fetches
  everything once when shown, a failing fetch backs off exponentially
  (capped at a minute), and the header says how long since the last good
  update and when the next try is.
- The console's favicon takes the colour of the worst open health
  problem and the title carries the count — `(2) datax — n1` — so a
  cluster gone critical shows in the tab strip and the window switcher
  without switching to it; steady, never animated (#150).
- The console is usable on a keyboard, with a screen reader and on a
  phone (#149): a skip link, a visible focus ring on every control,
  range rows as real controls (Tab, Enter or Space), captions and column
  scope on every table, the problems panel as a live region, a text cue
  beside every colour-coded figure, a fluid container instead of a fixed
  width, and below 640 px the wide tables become one card per row and the
  network matrix a worst-pairs list.
- `datax_node_load5` and `datax_node_load15` on `/metrics`, beside
  `datax_node_load1` (#146).
- The console notices a rolling upgrade under a long-lived tab (#146):
  the page and every `/api/cluster` document carry `console_version`, a
  digest of the page the node embeds, and the tab offers a reload when
  they differ; the page is served with that digest as its `ETag` and
  `Cache-Control: no-cache`, so a reload after an upgrade fetches the
  new page and one before it is a 304.

### Fixed
- The console no longer destroys the page every three seconds (#148):
  a render replaces an element's markup only when it changed and never
  while it holds the text selection, the focused element or an open
  disclosure (the new markup lands when the interaction ends), and the
  tables keep their rows by identity across polls, so a range key can be
  selected and copied, an open drill-down or disclosure stays open and
  keyboard focus survives a refresh; the range drill-down's cached-HTML
  workaround is gone.
- The console's SQL statements panel explains a 403 from
  `/api/activity` the way the range drill-down does — the signed-in user
  and the `GRANT` that fixes it — instead of hiding (#146).
- The heartbeat's machine summary is checked to be a projection of the
  node's sample (`TestMachineSummaryProjectsTheSample`): every summary
  field exists in the sample under the same JSON name and is copied, so
  the console reads one shape and a figure cannot exist in only one
  (#146).

## 0.43.0 — unreleased

Cluster version **v15**.

### Added
- Prefix bloom filters (#161). Pebble consults a bloom filter only on
  a prefix seek and only for what the comparer's `Split` returns; with
  no `Split`, the filters #101 configured hashed whole engine keys and
  the MVCC read path — a seek to a key's metadata key, then to a
  version — never asked them. From cluster version v15 a store opens
  with the MVCC comparer, whose O(1) `Split` cuts an engine key at the
  terminator of the user key's encoding (no key-format change), the
  point reads and the write path's intent probe are prefix seeks, and
  a filter rules out the sstables that hold nothing of the key. The
  comparer keeps Pebble's name and ordering; the filter policy and the
  columnar key schema carry new names, so the tables from before read
  as they are (their filters unconsulted, never asked a prefix
  question) and are rewritten in the background by the re-encryption
  pass's machinery (`prefix_bloom_rewrite`, `store_prefix_bloom` in the
  node document; `datax_prefix_bloom_remaining_bytes`,
  `datax_prefix_bloom_rewritten_bytes_total`). The comparer is fixed at
  open, so a node switches at its first restart after the finalize
  (logged); a v14 binary does not know the key schema and the store's
  version gate refuses it. A point read that finds no intent and whose
  newest version is visible now returns it from the first seek.
  Measured on the storage benchmarks (100k rows, 128-byte values, the
  rows compacted to L6 as a store's bulk is): a point read of an absent
  key 3.18 → 1.90 µs, through a reused iterator 2.07 → 0.84 µs (the
  filters exclude 1.98 tables per read); a present key 3.49 → 3.87 µs
  and 2.13 → 2.36 µs — the one filter probe of the table that holds it,
  which is why the filters of L6 are consulted (Pebble's default skips
  them: without them a miss costs 4.2 µs here, more than before). On the
  harness (fresh stores, 20 s, two alternating rounds) `ingest-random`
  328 / 321 → 375 / 365 rows·100/s (+14 %: every batch's uniqueness
  probes are misses), `kv-95-5` 26.5k / 27.2k → 25.6k / 26.3k ops/s
  (−3.5 %: its reads all find their key, and pay the probe), `hot-row`,
  `bank` and `scan` at parity.

### Fixed
- Re-encryption and the table rewrite above seed a file with a
  tombstone so a lone file is rewritten rather than moved; Pebble v2's
  manual compaction no longer flushes the memtable first (v1 did), so
  the seed stayed in memory and single files were moved with their
  retired key. The pass now flushes the seed before compacting.

## 0.42.0 — unreleased

Cluster version **v14**.

### Added
- Pebble's columnar-block sstable format (format major version 19)
  behind cluster version v14 (#166, the first of the gated format
  steps). Finalizing v14 ratchets both of every node's engines online,
  within a heartbeat and with no restart; new sstables — flushes,
  compactions — are written in the format from then on and existing
  ones are read as they are. A fresh store bootstrapped by, or joining,
  a v14 cluster starts there. A v13 binary bundles a Pebble that does
  not know the format, so a store cannot go back to it after the
  finalize (the store's version gate refuses, as for v13). The node
  document reports `store_format`. `TestColumnarBlocksRatchet`.
  Measured: on the storage benchmarks (100k rows, 128-byte values, a
  store built at 19 vs 16) a point read 3.71 → 3.24 µs (hit) and
  3.22 → 2.72 µs (miss), through a reused iterator 2.55 → 2.11 µs
  (miss), a 1,000-row reverse scan 1.35 → 1.07 ms, a forward scan
  unchanged; on the harness (kv-95-5, index-join, scan, hot-row,
  ingest-random; fresh stores, two alternating rounds) every shape is at
  parity — the read path's cost sits above the sstable blocks there.

### Changed
- Intent metadata and transaction records are protobuf-encoded from
  cluster version v14 (#141); JSON before it, where `encoding/json` was
  about 45 % of the intent path. The coordinator flags each transaction
  (`TxnMeta.BinaryMeta`) when the cluster is at v14, the flag rides in
  every command so every replica encodes alike, and readers tell the
  encodings apart by the first byte (`kvpb.UnmarshalTxnRecord`,
  `storage.DecodeMVCCMetadata`, shared by the request path and GC), so
  records from before the finalize stay readable for as long as they
  live — no migration, no rewrite.
  Measured: decoding a record 4.5 → 0.6 µs, encoding 0.85 → 0.65 µs, an
  intent laid down, rewritten and read back 13.9 → 6.2 µs; on the
  harness (two alternating rounds) `hot-row` 310 / 319 → 347 / 349
  ops/s, `bank` p50 4.2 / 4.6 → 3.7 / 4.0 ms with its throughput inside
  its own contention noise, `kv-50-50` and `ingest-random` at parity
  (single-statement writes commit in one phase and lay no intent).

## 0.41.0 — unreleased

### Fixed
- A gateway's descriptor lease is taken in the transaction that read the
  descriptor. Written in a separate transaction, the lease record could
  claim a version a schema change had already superseded: a gateway
  whose previous lease had lapsed read version 1, the change committed
  version 2 and drained (a lapsed lease is nothing to wait for), and the
  gateway then recorded a fresh lease at version 1 and served it from
  its cache — `column does not exist` on the new column — for a whole
  TTL. Found by the race suite; `TestLeaseClaimsTheVersionItRead` holds
  a gateway between its read and its write while the change commits.

### Changed
- The MVCC read path allocates a fraction of what it did (#163). A user
  key's two iterator bounds come out of one allocation (the upper bound
  is the metadata key with its terminator bumped — exactly the end of
  the key's engine keys — instead of encoding the key's successor), the
  version keys a read seeks to are appended onto that buffer's spare
  capacity, `encoding.EncodeBytes` and `DecodeBytes` allocate once at
  the exact size, and a scan copies each row's prefix off the iterator
  rather than re-encoding the decoded key (and no longer copies the
  decoded key a second time). `storage.Getter` serves the point reads
  of one server batch — the read path's Gets and the write path's Gets
  and Increments — through one iterator re-bounded per key, refreshed
  past the batch's own writes where the reader is a batch. Measured on
  the storage benchmarks (100k rows, 128-byte values): a point read
  4.28 → 3.60 µs and 16 → 5 allocations (2.75 µs and 3 through a
  Getter), a miss 3.66 → 3.12 µs (2.63 reused) and 12 → 3 (1); a
  1,000-row scan over 3 versions 735 → 518 µs and 8,019 → 2,016
  allocations, in reverse 1,472 → 1,252 µs.
- A transaction's intent history is bounded to what a savepoint
  rollback could restore (#162). Every same-epoch rewrite of a key
  appended the superseded value to the intent's history and rewrote the
  whole history with it, so a transaction writing one key K times
  stored K copies and wrote O(K²) bytes — for data only
  `ROLLBACK TO SAVEPOINT` reads. The coordinator now tells the servers,
  in `TxnMeta.HistoryFloor` on every batch, how far back a rollback
  could reach: with no live savepoint nothing is kept; with the oldest
  live savepoint at sequence F, the newest entry at or below F and every
  entry above it; two entries at one sequence collapse to the later. A
  coordinator from before the field leaves it 0 and gets the old
  behavior, so no cluster-version gate is needed. Measured on
  `BenchmarkIntentRewriteDepth`: one more write to a key already
  written 64 times in the transaction 136 → 12 µs and 45.9 KB → 1.6 KB
  allocated, flat from depth 1 to 64 (under a savepoint taken before
  the first write the history is kept as before). `bench/workloads.json`
  gains `hot-row` — one row updated 16 times per transaction — for the
  harness. `TestIntentHistoryBounded`.
- The store runs on Pebble v2 (`github.com/cockroachdb/pebble/v2`
  v2.1.7, from v1.1.5; #166). The port is mechanical — the encrypting
  `vfs.FS` takes the disk-write category its methods gained and returns
  `vfs.FileInfo`, the option and metric renames, `Compact` with a
  context — and the on-disk format stays at 16 (`FormatVirtualSSTables`,
  the pin below; v2 opens every existing store as it is, since its
  minimum supported format is 13). Columnar blocks (19) and value
  separation (24) are not adopted here: each is a separate,
  cluster-version-gated step with its own before/after. Pebble's own
  log lines — v2 reports the WALs it finds at open and their replay at
  info — now go through the node's log at debug level instead of to
  stderr.
- The Pebble format version is pinned at `FormatVirtualSSTables` (16)
  instead of tracking `FormatNewest` (#166): the formats past it change
  what lands on disk (columnar blocks, value separation), and adopting
  one is a deliberate, cluster-version-gated step with its own
  measurements, not a side effect of a dependency bump. Nothing changes
  for existing stores (16 is what the bundled Pebble's newest is).

## 0.40.1 — unreleased

### Fixed
- `Stopper.Stop` returns to every caller only once the shutdown has
  finished (#139). A concurrent second caller waited for the workers
  only and could return while the first caller was still running the
  closers — with the engine still open for a caller that then removed
  the data directory or asserted on final state. A closer registered
  after Stop had taken the closers was appended to a list nobody read
  again, silently; it now runs at once (one registered while the
  workers wind down runs in Stop, as before).
- HTTP client-certificate authentication checks that the certificate's
  CommonName is a role that may log in (#138), as the SQL port always
  did. A CA-verified certificate was accepted as its principal with no
  role lookup, so `ALTER ROLE ... NOLOGIN` or `DROP ROLE` revoked SQL
  and HTTP Basic access but left a certificate holder the read-only
  HTTP endpoints until the certificate expired (five years, with no
  revocation). A refused certificate now gets the `401` the Basic path
  gives, Basic credentials on the same request still get their turn,
  and the cluster's own node certificate is admitted as before.
  `TestHTTPCertAuthChecksLogin`.
- The SCRAM exchange no longer tells which user names exist (#137).
  Unknown users authenticated against one shared stand-in verifier, so
  the salt in `server-first` — sent before any proof — was one constant
  for every name that is not a user and a random value for every name
  that is: one probe of an impossible name, then one `client-first` per
  candidate, enumerated users without a password guess. The stand-in
  salt is now derived from the user name under a cluster-wide secret
  (`/system/auth-secret`, created by the first node that needs it), so
  it is stable per name across handshakes and nodes and indistinguishable
  from a real one; authentication fails as uniformly as before, and HTTP
  Basic, which never shows a salt, keeps the shared stand-in as a timing
  equalizer. `TestSCRAMStandInSaltPerUser`, `TestMockVerifier`.
- A panic on the SQL statement path no longer kills the node (#136).
  The connection goroutine had no recover, so a bug in the planner,
  the executor, a builtin or an encoder — reached by one statement —
  ended the process with every connection on it. The statement path
  is now a panic barrier: the statement fails with `XX000` (its stack
  in the log, `datax_sql_statement_panics_total` counting it), its
  transaction fails as on any other error, and the connection and node
  keep serving; streamed results are covered on every pull. Stack
  exhaustion and out-of-memory remain fatal, as Go makes them.
  `TestStatementPanicBarrier`.
- Binary `NUMERIC` parameters are bounded on decode (#140): `weight` and
  `dscale` came off the wire unchecked, so an eight-byte parameter with
  no digit groups expanded to ~200 KB of zeros per value. Both are now
  limited to what `NUMERIC(p, s)` can hold (1,000 digits of integer part
  or scale) and refused with `22P03` past that — the SQLSTATE every
  undecodable binary parameter now carries, in place of `08P01`; the
  encoder refuses the same bounds instead of wrapping the weight.
- A statement nested more than 1,000 levels deep (parentheses,
  subqueries, derived tables, `CASE`) is refused with a syntax error
  (`42601`, "statement nests too deeply") instead of exhausting the
  goroutine stack — a fatal error that took the node and every
  connection on it down, reachable by any client with a ~240 KB
  statement (#135). One depth counter in the parser covers every
  recursive production.
- A split's right-hand range keeps the timestamp-cache protection for
  reads the parent served on its span (#134). The RHS inherited the
  parent's closed timestamp as its cache floor, which trails now() by
  the closed-timestamp lag (3 s); reads the parent served inside that
  window lived only in its in-memory cache, so a write at one of those
  timestamps could land on the fresh RHS beneath a read already served
  — a serializability violation for readers in the window. Every
  replica now bumps the RHS's cache floor to now() as it applies the
  split (as a merge does for an absorbed span); the one-time push this
  costs a transaction that began before the split and writes to the RHS
  after it is the same as a leadership change's.
  `TestSplitKeepsServedReadsProtected`.
- The one-phase commit path honors the transaction's commit deadline
  (#133). The deadline — a schema lease's expiration, pinned when a
  statement plans against the leased descriptor — was checked only on
  the classic commit path; the one-phase path every implicit
  single-range `INSERT`, `UPDATE` and `DELETE` takes skipped it, so a
  statement planned under a lease an index build had since drained could
  commit past the drain and leave a row the index never saw. The check
  now runs inside that path's retry loop too (a refresh moves the write
  timestamp, and the moved timestamp is what commits).
  `TestOnePhaseCommitHonorsDeadline`,
  `TestOnlineCreateIndexUnderLapsedLeaseImplicit`.

### Changed
- `TestDecommissionDrainsReplicas` asserts its steady state (#167): the
  "no churn after stopping the drained node" check sampled the range
  generations while the drain's last moves could still be settling, so
  a descriptor bump in flight counted as churn (about one run in ten).
  It now samples once the generations have held still. The other test
  #167 names, `TestMergeFrozenAndRecovery`, already waits for the RHS
  leader before it subsumes (since 0.35.0); the new
  `TestSplitKeepsServedReadsProtected` had the same race and now waits
  the same way (`waitForLeader`). The crash tests' child node inherits
  its listeners from the parent instead of binding ports the parent
  picked and released a process start earlier — a race with the
  packages testing alongside, whose nodes and clients take ephemeral
  ports on the same loopback, that killed the child on "address
  already in use" before it served (one CI run in a few); a child that
  fails to serve now has its log quoted in the failure. `make test` and
  `make test-race` pass `-timeout 30m`: the cluster suite outruns
  `go test`'s default 10 minutes on a small machine.
- CI runs `staticcheck` (pinned at 2025.1.1, before the suite so a
  failure is quick; `make staticcheck` / `make lint` run the same) on
  top of gofmt and `go vet` (#142). Its first run is the baseline: the
  17 findings it made — 13 unused functions, fields and types, a
  redundant `| 0`, two simplifications — are gone.
- A `vulncheck` workflow runs `govulncheck` (pinned at v1.7.0) on every
  push and pull request and weekly on a schedule, as a gate (#143): it
  reports only advisories whose vulnerable symbols this module reaches,
  and advisories appear against unchanged code, hence the timer.
  `make vulncheck` runs it locally. Its first run found three reachable
  advisories — GO-2026-6061 and GO-2026-4762 in `google.golang.org/grpc`
  v1.71.0 (the HTTP/2 transport server, an authorization bypass via
  the `:path` header), GO-2026-5970 in `golang.org/x/text` v0.29.0 (an
  infinite loop in normalization, reached from SASLprep) — so gRPC
  moves to v1.82.1 and x/text to v0.39.0 (with `x/net` v0.53.0 and
  `x/sync` v0.21.0 as they require).

## 0.40.0 — unreleased

### Changed
- Scans step instead of seeking (#160). `MVCCScan` advanced to the next
  row with an LSM seek and found each row's visible version with
  another; both are now bounded walks with `Next` (a reverse scan steps
  back with `Prev`), seeking only past a chain of more than eight
  versions, and versions are recognized by their encoded suffix rather
  than decoded per key. A 1,000-row scan over single-version rows takes
  under half the time it did; `TestScanStepMatchesSeekPerRow` checks the
  stepping scan against the seeking one over version chains, tombstones,
  intents and uncertainty windows, forward and reverse.

## 0.39.0 — unreleased

### Added
- A plan cache for prepared statements (#107). Each session keeps a
  bounded LRU (128) of what its single-table `SELECT`, `UPDATE` and
  `DELETE` statements planned against — the descriptor, the statistics,
  the projection and the shape of a primary-key point lookup — keyed by
  the statement as prepared and the current database; an execution
  whose lookups return the same descriptor and statistics reuses it,
  binding the parameters into the point plan without re-planning, and
  any schema change, `ANALYZE`, drop or re-create misses. A data
  statement resolves each table name once per execution. `EXPLAIN`
  appends `(cached plan)`; `datax_sql_plan_cache_hits_total`,
  `_misses_total`, `_evictions_total`; the hit rate on the console's
  node page and in the internal metrics table. Connections keep a parse
  cache of their last 64 simple-protocol texts
  (`datax_sql_parse_cache_hits_total`). Measured first: planning was
  ~8 % of a gateway's CPU on point lookups through the extended
  protocol, so the cache is scoped to that; the statement activity
  tracker's per-statement connection walk (3 %) is gone too.
- `datax bench kv` and `bank` send parameterized statements over the
  extended protocol (`--protocol simple|extended|auto`).

## 0.38.0 — unreleased

### Changed
- The timestamp cache is indexed by key (#108, with the latch index that
  landed in 0.34.0). A generation holds its point reads in a map — one
  entry per key, the newest read of it — and only its ranged reads in a
  slice, so a point write looks its keys up instead of scanning every
  entry against every span: a 100-key write against two full generations
  takes ~2 µs instead of ~1.5 ms, which was a quarter of a leader's CPU
  under batched ingest (each INSERT's uniqueness probes put one entry per
  row in the cache). Generations hold 4,096 entries (1,024 before) and a
  key read repeatedly costs one, so a hot key set no longer rotates the
  cache — and rotation is what briefly pushes every writer on the range.

## 0.37.0 — unreleased

### Changed
- The single-range write pipeline (#106). Measured first, one range on
  one node below SQL (`BenchmarkRangeWritePipeline`): the ceiling was
  the apply, not the disk — every MVCC write cost two LSM reads on a
  fresh iterator (~10 µs a row), serialized per range inside the raft
  pass that committed it, so a range topped out near 100k rows/s at any
  batch size with the sync on or stubbed out. Two changes: each write
  now finds what it lands on with one bounded seek on an iterator its
  batch keeps (`Batch.writeState`; a 100-row commit applies in ~0.4 ms
  instead of ~1 ms), and committed entries apply on a pool of apply
  workers off the raft pass, so a range's next append and sync run while
  its previous entries apply (conf changes still apply inline; a replica
  with more than 64 MiB queued gets no pass until it drains,
  `datax_raft_apply_backpressure_total`). One range on one node, 16
  writers: 100-row commits 857 → 1,739/s, single-row commits 12.6k →
  16.3k/s, 1,000-row commits 100 → 262/s. New metrics:
  `datax_raft_entries_appended_total`, `datax_raft_entries_applied_total`,
  `datax_raft_apply_seconds`, `datax_latch_wait_seconds`. A proposal whose
  replica stops (shutdown, removal, a failed apply) is answered with an
  ambiguous error instead of waiting out its context. Test-only:
  `DATAX_TESTING_NOSYNC=1` commits the raft log unsynced for a
  measurement. Capacity planning in `docs/user/operations.md` restates
  the per-range ceiling from the measured costs.

## 0.36.0 — unreleased

### Changed
- The split store (#105; cluster version **v13**). A store's raft state —
  every replica's HardState, log entries and truncated state — moves to
  a raft engine of its own under `--dir/raft`, and the state-machine
  engine under `--dir` runs without a write-ahead log: a replicated write
  reaches disk once, through the synced group-committed raft log, instead
  of twice. What a crash takes from the state engine's memtable is
  replayed from the log (`datax_raft_replayed_entries_total`); a clean
  shutdown flushes first. Log truncation is deferred until the state
  engine has flushed past the entries it removes
  (`datax_raft_deferred_truncations_total`; a truncation pending past
  30 s has the housekeeping tick flush for it,
  `datax_raft_truncation_flushes_total`); merges, replica removals and
  catch-up snapshots flush before touching the raft engine; raft state
  orphaned by a crash is swept at startup. A store created by a
  v13 binary or joining a v13 cluster is split from the start; an older
  store migrates on its first start after the finalize and then refuses
  a v12 binary (the one upgrade step that cannot roll back). Both
  engines are encrypted, rotated and re-encrypted together.
  `datax_storage_split`, `datax_storage_bytes_written_total{engine,kind}`;
  `/api/node` reports `engine_mode` and the raft engine's metrics. On
  the harness's single node, batched ingest writes about half as many
  bytes to disk per row (balanced profile: 7.5× → 3.5× of the row
  bytes with sequential keys, 14.2× → 6.9× with UUID keys) at 16–19 %
  more rows per second; the ingest profile goes 3.2× → 2.3× and
  6.2× → 4.1× at 3–5 % more.

## 0.35.0 — unreleased

### Changed
- Streaming SELECT execution (#104). A scan-shaped `SELECT` — one
  table, no join, aggregate, `DISTINCT`, window, set operation,
  correlated subquery or in-memory sort — no longer materializes its
  result on the gateway: the wire layer pulls rows from KV in pages of
  512 as it writes them, flushing every 64 kB, so the first row leaves
  before the last is read and a full-table `SELECT` holds one page at a
  time. A row-limited `Execute` (JDBC fetch sizes) pulls its rows on
  demand and a suspended portal keeps the scan open. An error after
  rows have gone out (a bad row, a cancellation, `statement_timeout`)
  arrives after them, as in PostgreSQL; an implicit transaction re-runs
  the statement on a retryable error only while nothing has been
  flushed, otherwise the `40001` is surfaced. `datax_sql_streamed_rows_total`,
  `datax_sql_stream_restarts_total`.
- `statement_memory_limit` (default `64MB`; `0` = none). The paths that
  do materialize — sorts, aggregates, joins, `DISTINCT`, `WITH` members,
  derived tables, index joins that collect their rows — charge what they
  hold against it and fail with `53200` beyond it instead of growing
  without bound. `SET`, `SET LOCAL`, `RESET`, `SHOW`, `pg_settings`;
  `datax_sql_memory_limit_hits_total`.
- `datax bench` records the time to the first row of the `scan` and
  `index-join` workloads (`first_row_p50_us`, `first_row_p99_us`;
  `bench compare` shows them).

## 0.34.0 — unreleased

### Changed
- Batched point reads in the executor (#103). An index join fetches the
  primary rows behind its index entries in pages of 256, each page one
  routed batch fanned out per range, in index order (`EXPLAIN ANALYZE`
  reports the batches); a lookup matching 1,000 entries is four round
  trips instead of 1,000. `INSERT` builds every row first and reads all
  its primary-key and unique-index uniqueness probes and its foreign-key
  parent lookups in one batch (as `COPY` did for primary keys; `COPY`
  now batches the unique-index and foreign-key probes too); `UPDATE`
  computes every row's new values first and batches the moved
  unique-index entries' probes and the changed keys' parent lookups;
  `SELECT ... FOR UPDATE` locks the selected rows in one batch. Each key
  still records its read timestamp for refresh, and a key the batch was
  not primed with still reads on its own. Before/after on the harness in
  the PR; the set gains `index-join-1pct` and `index-join-10pct` (200 and
  2,000 rows per lookup).
- The latch manager indexes point spans by key (#108, latch part). Its
  conflict check was a linear scan of every held latch's spans against
  every span of the new request, allocating per comparison; with the
  wide batches of #103 (100-key probes under the 8-way `ingest` load) it
  reached 40% of a node's CPU and cost ingest a quarter of its throughput.
  A point span now checks the holders under its key plus the ranged
  holders; only ranged spans (scans, splits, merges) still scan every
  holder, and overlap checks no longer allocate.

## 0.33.0 — unreleased

### Changed
- Coalesced heartbeats and range quiescence (#102, part c; cluster
  version **v12**). Heartbeats and their responses travel as one
  envelope per peer node per scheduler pass instead of one each, and an
  idle range — no proposal, read-index request or snapshot for 2 s,
  every follower caught up and answering — stops ticking and heartbeating
  on every replica until a message, a proposal or a client request wakes
  it; a woken leader heartbeats at once and re-establishes follower
  contact before its first lease read. An idle range's closed timestamp
  now travels off the log (with the leader's term and log index, honored
  by a follower only while it still follows that leader at that term and
  has applied that index; in memory only), so follower reads stay fresh
  on quiescent ranges without a raft entry and an fsync per range per
  second — and for quiescent ranges it is grouped: one promise per
  follower node per round covers every range registered there, so an
  idle store publishes a few envelopes a second however many ranges it
  holds. `/status` reports `quiescent` per range; new series
  `datax_quiescent_ranges`, `datax_raft_quiesces_total`,
  `datax_raft_unquiesces_total`, `datax_raft_heartbeat_envelopes_total`,
  `datax_raft_heartbeats_coalesced_total`,
  `datax_closed_timestamp_side_updates_total`,
  `datax_closed_timestamp_group_updates_total`. Both stay off until
  `datax debug upgrade` finalizes v12 (a v11 node reads neither).
  Before/after on the harness in the PR.
- Lease-based reads take a fast path: a leader that has committed an
  entry in its own term answers the read index with its commit index at
  once — what raft's lease-based read would put in the next Ready —
  instead of a scheduler pass and a Ready per read.

### Added
- `--merge-size-threshold` on `datax start` and `datax demo` (negative
  disables merging, e.g. to keep an empty pre-split for a benchmark).
  `datax bench` records carry `error_samples` (the distinct messages
  behind `errors`), and a `--presplit` run uses tables of its own
  (`bench_kv_r1000`, ...) so it neither inherits an earlier workload's
  rows nor collides with its keys. `ALTER TABLE ... SPLIT AT` waits out
  a merge in flight on the range instead of failing.

## 0.32.0 — unreleased

### Changed
- Store-level raft scheduler with group commit (#102, parts a and b): a
  node's raft groups are driven by one fixed pool of workers
  (`GOMAXPROCS`; `StoreConfig.RaftWorkers`) and one 100 ms ticker
  instead of a goroutine and a ticker per replica. A worker takes a
  group of queued replicas, handles one Ready each, and stages every
  HardState and log entry into one synced Pebble batch — ten ranges
  appending in the same moment cost one fsync, not ten — before any of
  them sends a message or applies. New series:
  `datax_raft_scheduler_latency_seconds`, `datax_raft_ready_passes_total`,
  `datax_raft_log_syncs_total`, `datax_raft_readies_per_sync`. The
  crash-consistency test kills the node at group-commit boundaries with
  eight writers over sixteen ranges. Before/after on the harness in the PR.

### Added
- `ALTER TABLE t SPLIT AT VALUES (k, ...), ...` carves ranges at
  primary-key tuples (a prefix of the key is allowed) and returns the
  boundaries; idempotent, refused inside a transaction block (`25001`)
  and on sharded timeseries tables (`0A000`, carved by shard). `datax
  bench ... --presplit N` uses it; the checked-in set gains
  `kv-50-50-1000-ranges` and `ingest-random-1000-ranges`.

## 0.31.0 — unreleased

### Changed
- Pebble tuning (#101): every store gets a block cache sized from the
  machine's memory (25 % capped at 8 GiB for `balanced`, 10 % capped at
  2 GiB for `ingest`; `--cache-size`; one cache per process, shared by
  every engine and released when the last closes), bloom filters
  (10 bits per key) on every level, the newest sstable format the
  bundled Pebble supports, and an open-file budget of half the process's
  descriptor limit (1000–16384) instead of Pebble's 8 MiB cache, no
  filters and 1000 files. `StorageMetrics`, `/metrics`
  (`datax_storage_block_cache_{bytes,size_bytes,hits_total,misses_total}`,
  `datax_storage_bloom_{hits,misses}_total`), the metrics table
  (`store.block_cache_*`, `store.bloom_*`) and the dashboard's storage
  section show the cache hit rate and bloom utility. Before/after on the
  harness in the PR.

### Fixed
- A scan whose rows exceeded gRPC's 4 MiB default message limit never
  came back from a range led by another node: every attempt ran into
  the per-attempt timeout and the statement hung until the lease moved
  (the harness's `scan` workload took 13 minutes per query on a 3-node
  cluster). A range now pages scan responses at 8 MiB with a resume
  key, the client stitches the pages, and the internode message limit
  is 64 MiB in both directions.
- A read whose leadership confirmation (the raft read index) timed out —
  a freshly split range still electing, a briefly partitioned quorum —
  failed the statement with `XX000: read index abandoned`. The replica
  now answers NotLeader, so the client re-routes and retries under the
  statement's own deadline. `datax bench` retries transient failures
  during its preload for the same reason.

## 0.30.0 — unreleased

### Added
- Profiling and benchmark harness (#100): `net/http/pprof` under
  `/debug/pprof/` on the HTTP port (admin-gated), mutex and block
  profiling always on at low rates, `datax debug profile --kind
  cpu|heap|allocs|mutex|block|goroutine|trace`. `datax bench` gains
  `--seed` (fixed by default), `--json` records (throughput, p50/p95/p99,
  errors, retries, the server counter deltas), `--cpuprofile` /
  `--memprofile` / `--trace` for the client, `--server-url` and
  `--server-profile cpu` for the node, `--keys
  random|sequential|uuid` for ingest, and the `index-join` and `scan`
  workloads. The checked-in set `bench/workloads.json`, `make bench`
  (a fresh single node and a fresh 3-node cluster), `datax bench run`,
  `datax bench compare BEFORE AFTER` (±5 % flags), a nightly workflow
  that keeps `main`'s records, `bench/README.md`. A crash-consistency
  helper (`pkg/testutils/crash`: a child node killed with SIGKILL at a
  fault point — `pkg/util/faultpoint`: after the raft log sync, after
  an entry applies, as a memtable flush begins — or from outside, then
  restarted; every acknowledged write present, applied index caught up
  with the log) with `TestCrashConsistency`; `/status` reports each
  range's `last_index`.

### Fixed
- `datax bench` keys are `INT8` again: `INT` became 32-bit in 0.24.0,
  so the ingest workloads' random keys were refused.

## 0.29.0 — unreleased

Cluster version **v11**: role descriptors (`/system/roles`) supersede
the user credential records and admin markers; `datax debug upgrade`
rewrites them at finalize.

### Added
- Roles and privilege scopes (#98): `CREATE / ALTER / DROP ROLE` and
  `USER` (`LOGIN` / `NOLOGIN`, `PASSWORD`, `INHERIT` / `NOINHERIT`, `IN
  ROLE`, `IF [NOT] EXISTS`; a role may change its own password), role
  membership (`GRANT role TO role [WITH ADMIN OPTION]`, `REVOKE [ADMIN
  OPTION FOR]`, inheritance, cycles refused), `SET [LOCAL] ROLE` /
  `RESET ROLE` with `current_user` vs `session_user`. Ownership: tables,
  views, sequences, types and databases record their creator, who
  holds every privilege and alone (with admins) may alter, drop or
  grant them; `ALTER ... OWNER TO`, `REASSIGN OWNED BY`, `DROP OWNED
  BY`; `DROP ROLE` refuses an owner (`2BP01`) and takes the role's
  grants and memberships with it. Scopes: `GRANT ... ON DATABASE`
  (`CONNECT`, `CREATE`), `ON SCHEMA public` (`USAGE`, `CREATE`; `USAGE`
  revocable from `PUBLIC`), `ON ALL TABLES | SEQUENCES IN SCHEMA
  public`, `ON SEQUENCE` (`USAGE`, `SELECT`, `UPDATE`; `nextval` /
  `currval` / `setval` now check them, a `SERIAL` column's sequence
  following `INSERT` on its table), the `TRUNCATE` privilege, `WITH
  GRANT OPTION` / `GRANT OPTION FOR`, `PUBLIC` as a grantee, `ALTER
  DEFAULT PRIVILEGES [FOR ROLE r] [IN SCHEMA public] GRANT | REVOKE ...
  ON TABLES | SEQUENCES`. A view's query runs with its owner's
  privileges (definer semantics). Built-in roles `admin` (the old
  `ADMIN` marker, `root` an implicit member), `read_all`, `write_all`,
  `metrics` (HTTP `/metrics` only). HTTP and admin-RPC authorization
  resolve through membership; audit records carry the session user and
  the current role. `SHOW ROLES`, `SHOW USERS` (`member_of`), `SHOW
  GRANTS` (`database_name, schema_name, relation_name, grantee,
  privilege_type, is_grantable`; `ON DATABASE`, `ON ROLE`), `pg_roles`
  (`rolcanlogin`, `rolinherit`), `pg_auth_members`, `pg_user`,
  `information_schema.role_table_grants` (`grantor`, `is_grantable`),
  psql's `\du`; the dashboard schema browser's user list follows.

### Changed
- `GRANT` / `REVOKE` name existing roles (`42704` otherwise) — in
  insecure mode too. `/metrics` takes the `metrics` role (or admin)
  instead of any user. `SHOW GRANTS` gained the schema and grantable
  columns; `SHOW ROLES` lists every role, `SHOW USERS` the login ones.
  `ALTER` / `DROP` of a table, view, index, sequence or type, `COMMENT
  ON`, `CREATE INDEX` and `TRUNCATE` are for the object's owner (and
  admins) rather than admins only; `DROP DATABASE` / `ALTER DATABASE`
  for its owner. A caller's own context deadline during a statement
  reports `canceling statement due to user request` (the statement
  timeout message is reserved for `statement_timeout`).

## 0.28.0 — unreleased

### Added
- Session and wire (#97): query cancellation works — every connection
  gets a process ID (the node in its high bits) and a secret, a
  `CancelRequest` (psql's Ctrl-C, pgx's context cancellation, pools)
  stops the statement in flight with `57014` and rolls its transaction
  back, and one landing on another node is forwarded over the internode
  admin RPC. `statement_timeout` (`57014`), `lock_timeout` (`55P03`
  instead of waiting the conflict budget out on a live intent),
  `idle_in_transaction_session_timeout` (`25P03`, the idle block rolled
  back and its intents released). Honored variables with `SET` / `SET
  LOCAL` / `RESET` / `RESET ALL` / `SHOW` / `SHOW ALL` / `pg_settings`:
  `application_name` (the startup parameter too; shown by the activity
  views), `TimeZone` (TIMESTAMPTZ text output rendered in the zone),
  `search_path`, `DateStyle`, `client_encoding`,
  `default_transaction_read_only` / `transaction_read_only` / `SET
  TRANSACTION READ ONLY` (`25006` on a write), `transaction_isolation`
  (every level accepted, `SHOW` says serializable), `SET SESSION
  CHARACTERISTICS AS TRANSACTION ...`, `SET TIME ZONE`, `SET NAMES`;
  changed reported parameters are announced with `ParameterStatus`.
  `pg_backend_pid()` is real; `pg_cancel_backend(pid)` /
  `pg_terminate_backend(pid)` (admin, any node); `SHOW SESSIONS` and
  `pg_stat_activity` list the node's sessions; `pg_sleep(seconds)`.

### Changed
- `SET` of an unknown variable is `42704` (it was silently accepted);
  an invalid value is `22023`.

## 0.27.0 — unreleased

### Added
- Type system, part four (#96, closing it): enums. `CREATE TYPE [IF NOT
  EXISTS] name AS ENUM ('a', 'b', ...)`, `ALTER TYPE name ADD VALUE [IF
  NOT EXISTS] 'c'` (appended; every column of the type learns the label
  in the same statement and its tables drain, so the label is usable at
  once on every gateway), `DROP TYPE [IF EXISTS] name` (refused while a
  column uses it). A column of the type stores the label's ordinal with
  the label (cluster version v10), orders by declaration in `ORDER
  BY`, `min` / `max`, indexes and primary keys, reads and writes labels,
  refuses an unknown one (`22P02`), takes `'a'::name` casts, `LIKE`,
  `CREATE TABLE AS` and `ALTER COLUMN TYPE` from and to text. `pg_type`
  (`typtype = 'e'`, an OID past the builtin range), `pg_enum`,
  `information_schema.columns` (`USER-DEFINED`), `format_type`, `SHOW
  CREATE TABLE`, psql's `\dT` and `\d`; the wire describes the type's
  OID and carries labels in both formats.

## 0.26.0 — unreleased

### Added
- Type system, part three (#96): arrays of every scalar family but
  `JSONB` as column types (`INT8[]`, `TEXT ARRAY`, `VARCHAR(3)[]`;
  cluster version **v10**, like `INTERVAL` and `TIME`). Literals
  (`'{1,2}'`, `'{a,"b c",NULL}'`), `ARRAY[...]`, subscripts `a[i]`,
  `ANY` / `ALL` and the comparison operators over arrays, `@>` / `<@` /
  `&&`, `||` concatenation, element-wise equality and ordering (`GROUP
  BY`, `ORDER BY`, `DISTINCT`), `unnest` (`FROM` and select list) with
  typed rows, `array_agg` returning a real array, `array_length`,
  `cardinality`, `array_append` / `array_prepend` / `array_cat` /
  `array_position` / `array_remove` / `array_to_string` /
  `string_to_array` / `array_upper` / `array_lower` / `array_ndims`,
  `::int8[]` casts, `array(SELECT ...)` as a typed array. The wire
  describes PostgreSQL's array OIDs (`_int8` 1016, `_text` 1009, ...)
  and speaks the text and binary array formats in both directions, so
  pgx scans into and binds Go slices and `WHERE id = ANY($1)` takes a
  slice (the parameter describes as the column's array type); `pg_type`
  carries the array types with `typelem` / `typarray`, `pg_attribute`
  `attndims`, `information_schema.columns` `ARRAY` / `_int8`. `CREATE
  TABLE AS`, `LIKE` and `ALTER COLUMN TYPE` from text carry arrays.
  Arrays are not indexable and cannot be keys.

### Changed
- `array_agg` and `array(SELECT ...)` return an array type (`_int8`,
  `_text`, ...) instead of text.

## 0.25.0 — unreleased

### Added
- Type system, part two (#96): `INTERVAL` and `TIME` as column types
  (cluster version **v10**: a v9 node cannot decode their rows, so a
  column of either is refused until the upgrade is finalized). An
  interval is PostgreSQL's months / days / clock triple: every input
  form (verbose, `'2h30m'`, `'... ago'`, SQL standard `'1-2 3
  04:05:06'`, ISO 8601), the `INTERVAL '...'` / `DATE '...'` / `TIME
  '...'` / `TIMESTAMP '...'` typed literals, PostgreSQL's rendering and
  comparison rule, `timestamp - timestamp` and `age()` now return an
  interval (they were text), `interval ± interval`, `interval * / n`,
  `time ± interval`, `time - time`, `date + time`, `extract` over
  intervals and times, `justify_hours` / `justify_days` /
  `justify_interval`, `make_interval` / `make_time`, `sum` / `avg` /
  `min` / `max` over intervals, indexes and primary keys on both,
  `interval` (1186) / `time` (1083) text and binary wire codecs (pgx's
  `pgtype.Interval`, `time.Duration`, `pgtype.Time`), `pg_type` rows,
  `ALTER COLUMN TYPE` from text. The timeseries `retention` option and
  `with_max_staleness` accept interval text.

### Changed
- `timestamp - timestamp`, `age()`, `make_interval()` and
  `justify_hours()` return `INTERVAL` (OID 1186) instead of text.

## 0.24.0 — unreleased

### Added
- Type system, part one (#96): the type modifiers a column declares
  are enforced and described. Integer widths — `INT2` / `SMALLINT`,
  `INT4` / `INT` / `INTEGER`, `INT8` / `BIGINT` — bound the values
  (`22003`) and describe with PostgreSQL's OIDs (21 / 23 / 20) and
  binary sizes, so drivers scan into `int16` / `int32`; `SERIAL` is
  `INT4`, `SMALLSERIAL` `INT2`. `VARCHAR(n)` / `CHAR(n)` refuse a
  longer value (`22001`; excess spaces are dropped) and `CHAR(n)` renders
  blank-padded (`varchar` 1043 / `bpchar` 1042 with the typmod).
  `TIMESTAMP` is now `TIMESTAMP WITHOUT TIME ZONE` (OID 1114: an input
  offset is ignored, the output carries none), `TIMESTAMPTZ` /
  `TIMESTAMP WITH TIME ZONE` is unchanged, and `TIMESTAMP(p)` /
  `TIMESTAMPTZ(p)` round to `p` digits on write. `SHOW CREATE TABLE`,
  `information_schema.columns`, `pg_attribute`, `pg_type` (five new
  rows), `format_type`, `LIKE` and `CREATE TABLE AS` carry the
  modifiers; `ALTER COLUMN TYPE` changes them — a widening is one
  descriptor write, a narrowing rewrites and checks every value. Storage
  is unchanged (the modifiers ride on the descriptor; no cluster
  version bump); until v9 is finalized a new column keeps the earlier
  meaning of its declaration.

### Changed
- `INT` / `INTEGER` columns are 32-bit (they were `INT8`): a value past
  ±2³¹ into a column created from now on is `22003`. Existing columns
  keep their width. `TIMESTAMP` columns created from now on render
  without the `+00` offset and ignore an input offset.

## 0.23.0 — unreleased

### Added
- DDL completeness, part three (#95, closing it): `CREATE TABLE ... AS
  query [WITH NO DATA]` (the query's shape and rows, streamed through
  the COPY chunk path; a hidden `rowid` key unless `PRIMARY KEY (cols)`
  is written among the column names; `SELECT ... INTO` refused with a
  pointer), `CREATE TABLE ... (LIKE t [INCLUDING | EXCLUDING DEFAULTS |
  CONSTRAINTS | INDEXES | COMMENTS | ALL])`, `ALTER TABLE ... ALTER
  COLUMN c [SET DATA] TYPE t` as an online rewrite (a hidden shadow
  column every write fills, a chunked conversion of the existing rows,
  a swap; widening and text conversions; cluster version v9), and
  `COMMENT ON TABLE | VIEW | INDEX | COLUMN ... IS 'text' | NULL` with
  `obj_description`, `col_description` and `pg_description` (psql's
  `\d+`, `\dt+`).

## 0.22.0 — unreleased

### Added
- DDL completeness, part two (#95): views. `CREATE [OR REPLACE] VIEW
  name [(cols)] AS query` stores the query; a statement that names the
  view runs it and reads the rows like a table (as a base, join side,
  subquery, set-operation member, `INSERT ... SELECT` source, inside
  `WITH`; a view over a view expands the same way). `DROP VIEW [IF
  EXISTS] ... [CASCADE]`; `DROP TABLE`, `DROP VIEW`, `RENAME TO`,
  `RENAME COLUMN` and `DROP COLUMN` refuse (`2BP01`) while a view
  depends on the relation unless `CASCADE` drops the views; DML and
  physical DDL on a view are `42809`. `SHOW VIEWS`, `SHOW CREATE VIEW`,
  `pg_class` (`relkind = 'v'`), `pg_views`, `information_schema.tables`
  and `.views`, the dashboard's schema browser, psql's `\dv` and `\d
  view`. Reading a view needs `SELECT` on the view and on the tables
  its query reads. Cluster version **v9**: `CREATE VIEW` is refused
  until `datax debug upgrade` finalizes it.

## 0.21.0 — unreleased

### Added
- DDL completeness, part one (#95): `DROP INDEX [IF EXISTS]` (the
  index leaves the schema at once; its entries are reclaimed after the
  commit and lease drain), `ALTER INDEX ... RENAME TO`, `ALTER TABLE
  ... RENAME TO / RENAME [COLUMN] / RENAME CONSTRAINT` (foreign keys,
  sequences and grants follow the table's ID; `CHECK` expressions are
  rewritten for a renamed column; a `UNIQUE` constraint and its index
  rename together), `ALTER TABLE ... ALTER COLUMN SET DEFAULT | DROP
  DEFAULT` (constants and expressions; a fill-on-read column keeps
  filling its old rows from the original constant), `TRUNCATE [TABLE] t
  [, ...] [RESTART IDENTITY] [CASCADE]` as a transactional layout swap
  (one descriptor write for any table size; the old layout serves `AS OF
  SYSTEM TIME` until the re-shard janitor reclaims it; referencing
  tables refused without `CASCADE`), and `IF [NOT] EXISTS` on `ALTER
  TABLE`, `ADD COLUMN`, `DROP COLUMN`, `CREATE INDEX`, `ALTER INDEX`,
  `ALTER SEQUENCE`, `CREATE USER` and `DROP USER`. The reference
  documents which DDL runs inside a transaction block (every
  single-descriptor-write statement) and which is refused (`25001`: the
  multi-transaction online statements).

### Changed
- `CREATE INDEX` on a taken name and `ADD COLUMN` on a taken name report
  `42710` (duplicate object) instead of `42601`.

## 0.20.0 — unreleased

### Added
- Query shapes, part four (#94, closing it): `IN` and `EXISTS`
  subqueries inside `OR` (uncorrelated ones evaluated once, correlated
  ones per row); correlated subqueries up to 8 nesting levels; scalar
  subqueries in `ORDER BY`; `EXPLAIN ANALYZE`, which runs the statement
  and reports each stage's actual rows and time (scans with their
  paths, join levels, group / window / set-operation and sort stages)
  and the total.

## 0.19.0 — unreleased

### Added
- Query shapes, part three (#94): window functions — `row_number`,
  `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`,
  `lead`, `first_value`, `last_value`, `nth_value` and every aggregate
  `OVER ([PARTITION BY ...] [ORDER BY ...] [ROWS | RANGE frame])`, a
  `WINDOW` clause, window calls inside expressions and predicates
  (`amount - lag(amount) OVER (...)`), over plain, joined and grouped
  queries; derived tables as join members (`JOIN (SELECT ...) AS d ON
  ...`); `EXPLAIN` notes the window stage.

## 0.18.0 — unreleased

### Added
- Query shapes, part two (#94): `WITH` — members materialized once, in
  order, readable anywhere a table is (base, join side, subquery
  source, set-operation member, `INSERT` source), with column lists,
  on `SELECT`, `INSERT`, `UPDATE` and `DELETE` and inside subqueries;
  data-modifying members with `RETURNING`; `WITH RECURSIVE` (seed
  `UNION [ALL]` step, capped at 10000 rounds and a million rows);
  `INSERT ... SELECT`; and derived tables as join sides (`FROM (SELECT
  ...) AS d JOIN t ...`).

## 0.17.0 — unreleased

### Added
- Query shapes, part one (#94): `OFFSET n`, `LIMIT ALL`, `FETCH FIRST
  n ROWS ONLY` on every query shape (`LIMIT 0` returns no rows);
  `ORDER BY ... NULLS FIRST | LAST`, positions and expressions over
  grouped and set-operation output, and aggregate calls (`ORDER BY
  count(*)`) in grouped queries; `INTERSECT` and `EXCEPT`, each
  `[ALL]`, with PostgreSQL's precedence, parenthesized members that keep
  their own `ORDER BY` / `LIMIT`, `VALUES` as a member or a statement,
  and column types unified across members; `RIGHT` and `FULL [OUTER]
  JOIN`, `JOIN ... USING (cols)` and `NATURAL JOIN` (the merged column
  shows once and reads as `COALESCE` across an outer join). `EXPLAIN`
  names the join kind and the offset.

## 0.16.0 — unreleased

### Added
- Graceful shutdown (#124): on `SIGTERM` or Ctrl-C a node drains before
  it stops — it announces itself as leaving (`shutting_down` in its
  registry row: peers hand it no leases and place nothing on it),
  transfers every lease it holds to a live peer, closes its SQL
  listener and ends its connections with `FATAL 57P01` (idle ones at
  once, busy ones at their next idle point, open transactions at the
  deadline) — bounded by `--drain-timeout` (default 10s; 0 stops at
  once). A second signal skips the rest of the drain; a third, or a
  stop that hangs past the timeout, exits. `datax demo` drains the same
  way. The dashboard and `/api/health` show a stopping node;
  `Node.Drain` returns what the drain achieved and the node logs it.

## 0.15.0 — unreleased

### Added
- The expression language and builtin functions (#93): a registry of
  88 scalar functions (`pkg/sql/builtins`) — conditionals (`coalesce`,
  `nullif`, `greatest`, `least`), strings (`substring`, `position`,
  `trim`, `lpad`, `split_part`, `format`, `md5`, `sha256`, `encode`,
  `initcap`, `translate`, ...), math (`round`, `trunc`, `mod`, `power`,
  `sqrt`, `ln`, `log`, `random`, `width_bucket`, ...), date and time
  (`date_trunc`, `extract` / `date_part`, `to_char`, `to_timestamp`,
  `to_date`, `make_date`, `make_timestamp`, `age`, `clock_timestamp`),
  JSON (`jsonb_build_object`, `jsonb_build_array`, `to_jsonb`,
  `jsonb_set`, `jsonb_typeof`, `jsonb_extract_path[_text]`, ...) and
  the session functions — with their arity checked by the parser,
  `pg_proc` (`provolatile`, `proisstrict`) and `SHOW FUNCTIONS` listing
  them, and the [Functions reference](docs/user/functions.md) generated
  from the same registry. Casts are now **performed** (`CAST(x AS t)`,
  `x::t`, chains) with PostgreSQL's text forms and error codes,
  `DECIMAL(p,s)` and `VARCHAR(n)` typmods applied on the cast, and
  `regclass` resolved. Operators `%`, `^`, integer overflow detection
  (`22003`), date arithmetic (`date + n`, `date - date`, `ts + '2
  hours'`, month steps clamp), predicates `BETWEEN [SYMMETRIC]` (index
  bounds on a keyed column), `IS [NOT] TRUE / FALSE / UNKNOWN`, `IS
  [NOT] DISTINCT FROM`, `LIKE ... ESCAPE`, `SIMILAR TO`, and a literal
  `LIKE` prefix becoming index bounds. The SQL-form calls
  `substring(s FROM n FOR m)`, `position(a IN b)`, `trim(BOTH x FROM
  s)`, `extract(f FROM ts)`. JSONB `#>` / `#>>` paths, array-index
  `->`, `<@`, `?`, `?|`, `?&` everywhere an expression goes, HAVING
  included. Aggregates over expressions with `DISTINCT` and `FILTER
  (WHERE ...)`: `string_agg`, `array_agg`, `bool_and` / `bool_or` /
  `every`, `stddev*` / `var*`, `percentile_cont` / `percentile_disc
  ... WITHIN GROUP`, `json[b]_agg`, `json[b]_object_agg`. Computed
  outputs describe with their real type on the wire (`qty * 2` is
  INT8, `qty > 3` BOOL, `now()` TIMESTAMPTZ), a cast column keeps its
  name, and `now()`, `current_timestamp` and `current_date` share one
  statement clock. Predicates used as values are three-valued (`NULL
  BETWEEN 1 AND 2` is NULL, not false).
- `CHECK` constraints may use the stable session functions (`CHECK (at
  <= now())`, `CHECK (who = current_user)`), evaluated per statement.

### Changed
- `AVG` of an `INT8` column is a `DECIMAL` (exact, 6 fractional
  digits), as in PostgreSQL, where it was a `FLOAT8`.

### Fixed
- A timestamp outside the representable years 1678 to 2261 (int64
  nanoseconds) was silently wrapped (`'2999-01-01'` became 1829); it
  is now refused.

## 0.14.0 — unreleased

### Added
- Table constraints (#92): `CHECK (expr)`, column and table `UNIQUE`,
  and `FOREIGN KEY ... REFERENCES t (cols) [ON DELETE | ON UPDATE
  RESTRICT | NO ACTION | CASCADE | SET NULL]` (MATCH SIMPLE), named or
  auto-named as PostgreSQL does; `ALTER TABLE ... ADD CONSTRAINT`
  (publishes, then validates the existing rows in bounded chunks; `NOT
  VALID` defers that to `VALIDATE CONSTRAINT`), `DROP CONSTRAINT [IF
  EXISTS]`, `ALTER COLUMN ... SET NOT NULL` (sweeps first) / `DROP NOT
  NULL`, `DROP TABLE ... CASCADE`. A CHECK passes on NULL and is
  `23514` otherwise; a foreign key is checked by a point read of the
  parent in the writing transaction (`23503`) and, on the parent side,
  through an index the constraint creates on the referencing columns
  when none covers them, so a parent delete never scans the child;
  cascades are bounded per statement by `SET
  foreign_key_cascade_limit` (default 10000, `54000` beyond it). `COPY`
  respects every constraint and names the failing row. The catalogs
  show them (`pg_constraint` with `conparentid`, `confupdtype`,
  `confdeltype`; `information_schema.check_constraints`,
  `referential_constraints`, `constraint_column_usage`; `pg_class`
  `relchecks` / `relhastriggers`), so psql's `\d` lists check
  constraints, foreign keys and "Referenced by". For those queries:
  `oid::regclass` on a column, `VALUES` as a `UNION` member,
  `pg_partition_ancestors`, table functions as join members (parsed),
  and selects over an always-empty catalog (`pg_trigger`, ...) answer
  empty whatever shape they take. **Cluster version v8**: descriptors
  gain the constraint fields, which a v7 node would ignore on write, so
  the DDL is refused with `0A000` until `datax debug upgrade` finalizes
  v8.
- `SET name = <number>` is accepted (numeric settings).

## 0.13.0 — unreleased

### Added
- Sequences, `SERIAL` / `BIGSERIAL` / `SMALLSERIAL`, `GENERATED
  {ALWAYS | BY DEFAULT} AS IDENTITY` columns and expression `DEFAULT`s
  (#91): `CREATE / ALTER / DROP SEQUENCE` with `INCREMENT`, `MINVALUE`,
  `MAXVALUE`, `START`, `CACHE`, `CYCLE`, `OWNED BY` and `RESTART`;
  `nextval`, `currval`, `lastval`, `setval`; `unique_rowid()` and
  `gen_random_uuid()`; `DEFAULT` as a value, `INSERT ... DEFAULT
  VALUES`, `OVERRIDING SYSTEM VALUE`, `UPDATE ... SET col = DEFAULT`;
  `SHOW SEQUENCES`, a sequence as a one-row relation, and the
  `pg_sequence(s)` / `pg_attrdef` / `information_schema.columns` rows
  psql and ORMs read. `nextval` advances one counter key with an atomic
  increment outside the transaction (never rolled back, gaps normal);
  each gateway serves `CACHE` values (default 32) per increment.
  Backups carry sequences and their counters. **Cluster version v7**:
  descriptors gain the default-expression, identity and owned-sequence
  fields, which older nodes cannot evaluate, so the DDL is refused with
  `0A000` until `datax debug upgrade` finalizes v7.

## 0.12.0 — unreleased

### Added
- `RETURNING` on `INSERT`, `UPDATE` and `DELETE` (#90): any expression
  over the written row, `*`, aliases; rows come from the values the
  statement has in hand, so `INSERT ... RETURNING id` costs no read.
- `ON CONFLICT (columns | ON CONSTRAINT name) DO NOTHING | DO UPDATE SET
  ... [WHERE ...]` with `EXCLUDED`, arbitrated by the primary key or a
  unique index (`42P10` otherwise; a conflict on another unique key stays
  `23505`), `ON CONFLICT DO NOTHING` without a target, and `UPSERT INTO`.
  Command tags are PostgreSQL's (`INSERT 0 n` counts inserted and
  updated rows; under `DO NOTHING`, inserted rows only).

## 0.11.0 — unreleased

### Added
- Keys and range spans print as readable paths everywhere (logs, `datax
  debug ranges|split|merge`, the dashboard's range tables and `/api/*`):
  `/Min`, `/Max`, `/meta/...`, `/system/desc/7`,
  `/table/orders/primary/1000`, `/table/orders/by_city/"oslo"/42` —
  with table and index names and typed key values where the schema is
  known, IDs and shape-decoded values where it is not (`/table/3/1/1000`)
  — instead of escaped bytes (`"\x04\x00...\x80..."`).
- PostgreSQL catalogs and `SHOW` (#89): read-only `pg_catalog` and
  `information_schema` virtual tables over the live schema
  (`pg_database`, `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
  `pg_index`, `pg_constraint`, `pg_attrdef`, `pg_roles`, `pg_settings`,
  `pg_tables`, `pg_indexes`, `pg_collation`, `pg_tablespace`, ...,
  `information_schema.tables` / `columns` / `table_constraints` /
  `key_column_usage` / `statistics` / `role_table_grants`, and empty
  stand-ins for the catalogs of features datax lacks), the catalog
  functions tools call (`format_type`, `pg_get_indexdef`,
  `pg_get_constraintdef`, `pg_get_expr`, `pg_get_userbyid`,
  `pg_table_is_visible`, `current_setting`, `array_to_string`,
  `pg_size_pretty`, `has_*_privilege`, ...), and `SHOW COLUMNS FROM t`,
  `SHOW INDEXES FROM t`, `SHOW CREATE TABLE t`, `SHOW USERS`, `SHOW
  GRANTS [ON t] [FOR user]`, `SHOW TABLES FROM db`, `SHOW ALL` and `SHOW
  <setting>` (unknown settings are `42704`). psql's `\d`, `\dt`, `\di`,
  `\l`, `\du`, `\dn`, `\dp` and their `+` forms render; ORM introspection
  sees the schema. `server_version` now reports 14.0.
- SQL the catalog queries (and everyone else) needed: `UNION [ALL]`;
  `[NOT] LIKE` / `ILIKE`; `= ANY | SOME | ALL (array)` and `= ANY
  (SELECT ...)`; `||`; `CASE`; comparisons and boolean expressions as
  values; `CAST(x AS type)` and `::type` (absorbed) with
  `'name'::regclass` resolving a table; `E'...'` strings; regular
  expression operators `~ !~ ~* !~*`; `OPERATOR(pg_catalog.op)`;
  `COLLATE` (ignored); `ORDER BY` expressions and output aliases;
  parenthesized `JOIN ... ON` with non-equality conjuncts as join
  filters; `CROSS JOIN` and `FROM a, b`; `array(SELECT ...)`; `FROM
  unnest(array) AS s(x)`; scalar subqueries as predicates and inside
  `OR`; correlated subqueries in the select list, in `CASE` arms and in
  `array(...)`, and correlated subqueries over joins; any `f(...)` parses
  and an unknown function is `42883`.

## 0.10.0 — unreleased

### Added
- Databases (#88): `CREATE DATABASE`, `DROP DATABASE [CASCADE]`, `ALTER
  DATABASE ... RENAME TO`, `SHOW DATABASES`, `USE` / `SET database`,
  `current_database()` and `current_schema()`; the connection URL's
  database selects the session's database and an unknown one is refused
  with `3D000`; qualified names (`db.t`, `db.public.t`, `public.t`) in
  every statement that names a table; `GRANT CREATE | CONNECT | ALL ON
  DATABASE ... TO user | PUBLIC`, with CONNECT checked when a session
  opens a database and CREATE letting a non-admin create tables. A new
  cluster has `datax` and a reserved `system` database. `SHOW TABLES` and
  a bare `ANALYZE` act on the current database; the schema browser shows
  each table's database; backups carry the database catalog. **Cluster
  version v6**: descriptors gain a database ID and table names move
  under their database; until `datax debug upgrade` finalizes v6 every
  table stays in the flat namespace (which v5 nodes read) and database
  DDL is refused; finalize migrates the catalog in one transaction.

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
