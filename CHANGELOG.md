# Changelog

Releases of datax, newest first. The version is `pkg/version.Release`,
bumped in the pull request that changes behavior (minor for a new
capability, patch for a fix); a git tag `vX.Y.Z` on `main` marks the
release, and the build workflow stamps binaries with the tag or with
`vX.Y.Z+<commit>` between tags. The cluster protocol version (`v1`, `v2`,
... in `pkg/version`) is separate: it changes only when the replicated
state or the internode protocol does, and an entry below says so.

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
