# datax architecture

datax is a layered system. A SQL statement enters over the PostgreSQL wire
protocol and descends through five layers, each with a narrow interface to the
one below:

```
pgwire     protocol framing, auth, result encoding        pkg/pgwire
sql        parse → plan-less execute over KV              pkg/sql
kvclient   transactions + request routing                 pkg/kvclient
kvserver   consensus, per-range state machines            pkg/kvserver
storage    MVCC over Pebble                               pkg/storage
```

## The keyspace

Everything — user rows, schema descriptors, cluster metadata — lives in one
ordered key space, split into **ranges**. Each range is replicated by its own
Raft group. The layout (see `pkg/keys`):

| Prefix | Contents | Replicated? |
|---|---|---|
| `/local/store/...` | store ident: cluster ID, node ID, store ID | no (per store) |
| `/local/r/<rangeID>/...` | Raft state: HardState, log, applied index, descriptor copy | no (per replica) |
| `/meta/<endKey>` | range addressing: end key → RangeDescriptor | range 1 |
| `/system/nodes/<id>` | node registry: address, locality, liveness | range 1 |
| `/system/desc/<tableID>`, `/system/db/<dbID>`, `/system/dbns/<name>`, `/system/nsdb/<dbID>/<name>` | SQL catalog: table descriptors, database descriptors, database names, table names per database (`/system/ns/<name>` is the pre-v6 flat table namespace, empty once v6 is finalized) | range 1 |
| `/system/idgen` | descriptor / table ID counter | range 1 |
| `/system/seqdesc/<seqID>`, `/system/seqns/<dbID>/<name>`, `/system/seq/<seqID>` | sequences (v7): descriptors, names per database, and each sequence's counter, advanced by non-transactional increments of one `CACHE` block at a time | range 1 |
| `/t/<tableID>/<pk...>` | user table rows | user ranges |

A row is one key and one value: every non-key column of the row is in
that value (`docs/sql.md`, "Row encoding"), written, replicated and read
as a unit. Nothing is stored out of line, so a row is as large as its
largest column value, and the practical ceiling on a value is what one
Raft proposal and one internode message can carry (see "Large objects"
in `docs/sql.md`). Values meant to be larger than that are large
objects: pages of 256 KiB, each an ordinary row of a system table under
`/t/<1<<40 + 5>/...`.

Range 1 initially spans the whole keyspace; splits carve off new ranges.
Because all replicas of all ranges on a node share one Pebble store, and range
membership of a key is purely logical (descriptor bounds), **splitting a range
moves no data**.

## Life of a write

1. `pgwire` reads `UPDATE accounts SET ... WHERE id = 1` off the socket.
2. `sql` parses it, resolves the table descriptor, computes the KV key for
   primary key 1, reads the current row, and issues a `Put` through the
   session's transaction.
3. `kvclient`'s **TxnCoordinator** stamps the request with the transaction's
   metadata; the **DistSender** looks up which range owns each key (range
   cache, backed by `/meta/`) and sends a `BatchRequest` to a replica of
   that range — a batch spanning several ranges partitions into per-range
   sub-batches sent **in parallel** (the record-creating one first).
4. `kvserver` on the Raft leader checks the timestamp cache, then proposes the
   write to the range's Raft group. Once a quorum has appended it, each replica
   **applies** it — on an apply worker, off the raft pass that committed
   it, so the range's next append is already syncing meanwhile
   (docs/replication-and-placement.md, "Pipelined apply"): an MVCC *write
   intent* lands in Pebble.
5. At `COMMIT`, the coordinator flips the transaction record to COMMITTED — a
   single Raft write — then asynchronously resolves intents to plain values.
   Fast paths skip stages of this: an implicit transaction stages its
   record in parallel with its writes (**parallel commit**), and one whose
   whole write set fits a single range commits in **one Raft proposal**
   with no record or intents at all (**one-phase commit**) — see
   [transactions](transactions.md).

## Life of a read

Reads are served by the Raft **leader** of each range (leaseholder =
leader) using Raft **ReadIndex** to stay linearizable across leadership
changes: the leader confirms its leadership — by default from its
CheckQuorum **lease** with a wall-clock backstop, or with a quorum round
trip when lease reads are disabled (see
[replication & placement](replication-and-placement.md)) — waits until its
applied index reaches the confirmed commit index, then reads from local
Pebble at the transaction's timestamp — no log entry needed. Concurrent
readers coalesce their confirmations.

## Correctness invariants

These are the load-bearing rules; tests assert them.

1. **Raft durability order**: HardState and log entries are synced to Pebble
   *before* any outbound Raft message that acknowledges them. Applied state and
   the applied index are written in one atomic batch, so crash-recovery replay
   is idempotent. On a **split store** (v13, issue #105) the log has its own
   engine and the state engine runs without a write-ahead log: what a crash
   takes from the state engine's memtable is replayed from the log, whose
   truncation waits for the state engine to flush past the entries it removes
   (see [replication-and-placement.md](replication-and-placement.md#raft-log-truncation)).
2. **Linearizable reads**: only via ReadIndex on the leader. On leadership
   acquisition the timestamp cache floor is bumped to `now()`, because a new
   leader cannot know what reads the old leader served. The one non-leader
   read path is a **follower read**: a read pinned to a fixed timestamp at
   or below the range's replicated closed timestamp (`AS OF SYSTEM TIME`,
   whose `with_max_staleness` form picks that timestamp from the gateway's
   own closed timestamps), which is linearizable *at that timestamp* by
   construction.
3. **Clocks**: every RPC carries an HLC timestamp; receivers ratchet their
   clock. A remote clock further than `--max-offset` ahead is a fatal error.
   Reads treat values in `(readTS, readTS+maxOffset]` as uncertain and restart.
4. **Intents**: an intent is never resolved except according to its
   transaction record's state. Readers/writers who find an abandoned intent
   push the record (which aborts expired transactions) and then resolve.
5. **Serializability**: a transaction commits only if its write timestamp
   still equals its read timestamp; anything that bumps the write timestamp
   forces a retry (see [transactions.md](transactions.md)).

## The catalog, and what its costs follow

The catalog lives in range 1, and most of what reads it is meant to cost
in proportion to what is asked for, not to the number of tables in the
cluster (issue #311):

- **Resolving a table** is four point reads cold (the database's name
  and descriptor, the table's name, its descriptor) and none from a
  gateway's cache.
- **Listing a database's tables** (`catalog.Accessor.ListIn`: `SHOW
  TABLES`, `DROP DATABASE`, `GRANT ... ON ALL TABLES`, `ANALYZE`, the
  `pg_catalog` views) scans that database's names under
  `/system/nsdb/<dbID>/` — plus the flat pre-v6 namespace for the
  default database — and reads those descriptors in batched point
  reads.
- **Descriptor leases** follow the tables a gateway is using: an entry
  unused for three lease TTLs is evicted and its lease deleted, and the
  cache holds at most 1,024 descriptors (see
  [sql.md](sql.md), "versions and leases").
- **The console's schema document** is built when `/api/schema` (or a
  page that needs it) is requested, cached for five seconds, and shared
  by concurrent requests. No node builds it on a timer.
- **Names for range labels, rendered keys, log lines and index-usage
  series** come from a per-node cache of descriptors read by ID, holding
  only the tables that node has been asked about.
- **Placement** is a per-node map refreshed every few seconds from the
  database descriptors and, for databases that carry a policy, the names
  keyed under them; a cluster with no policy reads only the database
  descriptors.

- **The dependent-view check** on `DROP` and `ALTER` (issue #313) scans
  the views recorded under the relation (`keys.ViewDependencySpan`,
  written with each view) and reads those, from cluster version v22 once
  the node leading range 1 has recorded every view; before that it reads
  every descriptor.
- **The retention provider behind GC and the re-shard janitor** (issue
  #313) read every descriptor once and after that only what changed:
  each pass is an incremental export of the descriptor span since the
  last (`server.descWatcher`), the read an incremental backup makes, and
  each keeps only what it needs (the tables with a GC window or a
  retention; those with a retired layout). An idle pass returns no rows.
- **The statistics sampler** (issue #316) follows the descriptor and
  statistics spans the same way, both as of one timestamp: an idle tick
  returns no rows, and a tick that collects a table reads two (the
  record it wrote last tick, the descriptor it collects).
- **The dropped-table reclaim** reads the records of dropped tables
  waiting to be reclaimed, not the catalog. The one-time sweep for
  tables dropped before those records (issue #316) walks the ids that
  hold rows once per cluster, a bounded number per pass.
- **The metrics recorder's per-table and per-index series** (issue
  #316) are written only when they change, plus a keyframe every 60
  ticks, from v22; `/api/metrics` fills in the ticks between
  (`server/metrics_sparse.go`). At 5,000 tables a tick wrote about
  13,200 rows and writes about 370.

What still reads every table descriptor: `catalog.Accessor.List`, used
by `information_schema.tables`, `.columns` and `.views` (which list
every database), role DDL that must find everything a role owns or is
granted (`DROP ROLE`, `REASSIGN OWNED`, `DROP OWNED`),
`ALTER TYPE ... ADD VALUE` and `DROP TYPE`,
`regclass` name resolution, a build of the schema document, and the
table gauges (`table.rows`, `table.ranges`, `datax_table_*`), which the
leader of range 1 reads once per recorder tick and any node reads when
`/metrics` is scraped — a series per table costs a row per table read,
though from v22 only the changed ones are written. Backup and recovery
scan the descriptor span directly. The statistics sampler, the
retention provider and the re-shard janitor read it whole only when
they cannot resume (below).
Sequences, types and roles are still listed whole by the catalog views.
`TestCatalogScanScale` measures the passes above (rows read out of the
descriptor span, one node): at 1,000 and 5,000 tables, DROP TABLE reads
1,004 and 5,004 descriptor rows by the old check and 2 by the records;
a retention refresh or a re-shard janitor pass reads about 1,000 and
5,000 on its first pass and 0 on an idle one after (1 after a DROP); a
statistics sampler tick read about 1,000 and 5,000 (descriptors and
statistics records) on every tick and reads 2 while collecting. After
a restart, their first passes read 2 to 7 rows at either size, what
changed (below). `TestMetricsRecorderScale` measures the rows the recorder
writes per tick.

**Resuming after a start** (issue #316). Each of the three jobs keeps
its state beside its watcher (`server.catalogFollower`) and persists a
resume record: a format version, the watcher's timestamp, and the state
as of that timestamp, in one value. The retention provider, which every
node runs for its own GC, keeps the record on the node's engine
(`keys.StoreCatalogResumeKey`); the janitor and the sampler, which run
on range 1's leader, keep theirs in the replicated keyspace
(`keys.CatalogResumeKey`), so a node that takes over the leadership
resumes from the last leader's record, and a node that loses it drops
its state. The first pass after a start loads the record and reads what
changed since its timestamp, as any later pass does.

- *What is persisted.* The retention provider and the janitor keep only
  a few descriptors (the tables with a GC window or a retention; those
  with a retired layout) and persist them raw, so the binary that
  resumes derives its windows and layouts itself. The sampler persists
  its table set and collection times compactly (about 45 KB at 5,000
  tables).
- *Why the result equals a full read.* A record's state is what a full
  read at its timestamp derives: it is written straight after a pass
  applied the changes up to that timestamp, before the job acts on it.
  The export after it covers (timestamp, now], and no write commits at
  or below a timestamp the spans were read at. Persisting state and
  timestamp as one value means a crash leaves either the new record or
  the one before, and each starts from a state and the timestamp it is
  as of, so changes are read twice at worst and never skipped. Any
  record is a valid starting point, so two nodes that both think they
  lead range 1 for a moment can both write one. A node's clock is moved
  past a record's timestamp before it reads from it, and a record too
  far ahead of the clock is not used.
- *When the job reads everything instead.* When the record is missing or
  does not decode, when its format version is not the job's (an upgrade
  that changes the state, or what a job keeps of a descriptor, bumps the
  version), or when GC has passed its timestamp (the export is refused
  below the GC threshold). These are the same full reads as before
  records existed.
- *Checked against a full read.* However a state was resumed, each
  follower checks it against a full read at least once a day (plus up
  to a quarter of jitter, per node and job, so that the reads do not
  coincide; the time of the last check travels in the record, so
  restarts do not postpone it). That pass reads what changed and
  everything as of one timestamp; if the two states differ the follower
  logs a warning, counts it in
  `datax_catalog_follower_divergence_total{job}` and keeps the full
  read's. A bug that leaves a state wrong is therefore corrected within
  about a day, as a restart's full read used to correct it, and is
  visible: the counter should stay at zero.
- *Cost.* A record is written after a pass at most every minute (the
  retention provider: a few hundred bytes on the node's engine), ten
  minutes (the janitor: usually an empty list) or hour (the sampler,
  whose state changes every tick while it collects). So the first pass
  after a start reads what changed in that long plus the time the node
  was down. At 5,000 tables the sampler writes about 1 MB a day. In
  exchange, each start or leadership move skips reading 5,000
  descriptors (1 MB at their smallest) and as many statistics records.

`TestCatalogScanScale` measures the first passes after a restart, and
`TestCatalogFollowersResumeAfterRestart`,
`TestCatalogFollowersLeadershipMove` and `TestCatalogFollowersFallBack`
check that the states equal a full read, and
`TestCatalogFollowerFullReadCorrects` that the daily check finds and
replaces a wrong one.

## Versioning and rolling upgrades

Each binary has an integer protocol version with a supported window
(`pkg/version`: `[MinSupported, Current]`, adjacent versions only). The
cluster persists one **cluster version** at `/system/cluster-version`,
seeded at bootstrap and advanced only by the operator-triggered
`upgrade-cluster` admin op once every live node's heartbeat advertises a
new-enough binary. Bootstrap also seeds whatever catalog state that
version implies (from v6, the default databases), so a fresh cluster
never runs a migration against its own first transactions. Gates: join (a node whose window excludes the cluster
version is refused), startup (each store mirrors the last observed cluster
version locally and refuses to start under an older binary — the
no-downgrade-after-finalize rule, enforceable even with quorum down), and
re-announce (advisory disjoint-window rejection).

What makes the mixed-version window safe is a set of compatibility rules
codified in `pkg/version`'s package doc and enforced by golden decode
tests across packages: JSON payloads grow additively (`omitempty`, safe
zero values), proto fields are add-only with never-reused numbers, format
bytes only gain values behind a cluster-version gate and decode forever,
and unknown payloads degrade to errors, never crashes or silent misapplies.

Version-gated request shapes follow rule 4 through a client-side gate:
`kvclient.DB` holds the node's mirrored cluster version and refuses to
SEND a gated shape below its introduction version. The first such shape
is the reverse scan (`ScanRequest.Reverse`, introduced under **v3**): a
v2 node would silently ignore the field and run a forward scan, so until
the operator finalizes v3 the SQL layer transparently falls back to the
pre-v3 plan (an in-memory sort). The second is the ordered
range-addressing repair (`UpdateMetaRequest`, introduced under **v4**):
a v3 leader of the meta range does not know the request, so until v4 is
finalized splits and merges repair `/meta` with the blind writes they
always used. The sticky bit of a manual split (**v20**) is gated the
same way and for a second reason: besides a new request
(`AdminUnsplitRequest`) it adds a field to the range descriptor and a
trigger to the raft log, and a v19 replica applying either would end up
with a different descriptor than its v20 peers — so until v20 is
finalized no descriptor is written with the bit, manual splits are
merged back like automatic ones, and unsplit is refused (see
[replication-and-placement.md](replication-and-placement.md)). **v21**
gates descriptor fields only — a table's GC window and its row expiry
(`TableDescriptor.GCTTLSeconds`, `RowTTL`) — and the purge admin ops: a
v20 node would act on such a table by the node's settings, and as a
gateway would write the descriptor back without the fields, so the DDL
and the purge are refused until finalize
([transactions.md](transactions.md), "A window per table" and "Purge";
[sql.md](sql.md), "Table options").

Exports are the read a backup is made of (`ExportRequest`, evaluated by
`storage.MVCCExport` as a consistent read at the backup's timestamp):
for each key that changed in a window `(start, end]`, its newest version
at `end`, tombstones included. With `AllRevisions` (**v21**) the same
walk returns every version in the window, newest first within a key,
each with its timestamp — what a backup with revision history stores and
a point-in-time restore replays up to a chosen moment. A key's versions
are never split across responses. The window is whole only while garbage
collection has not passed its start, so a replica refuses an export
whose start timestamp is below its GC threshold; that refusal, already
there for incrementals (a collected tombstone is a missed deletion), is
what keeps a history backup from being written with versions missing.
The flag is gated like the others: a v20 replica would ignore it and
answer with the newest versions only, which a backup would record as
the whole history.

A table restore into a running cluster writes through the ordinary
transactional path, under table ids nothing names until one transaction
publishes the descriptors; a replicated progress record
(`/system/restore/<op>`, also **v21**) lets any node remove the rows of
a restore whose node died (see `pkg/server/restore_table.go`).

Large objects (**v21**) are three system tables at reserved ids and the
`lo_*` functions over them; nothing below SQL is new. The gate exists
for the tables: a v20 node knows nothing of the reservation, would
treat them as ordinary tables their owner may write, and could delete
pages out from under an object.

## Modules

- `pkg/base` — shared ID types (NodeID, StoreID, RangeID) and configuration.
- `pkg/version` — the protocol version, support window, and compat rules.
- `pkg/util/hlc` — hybrid logical clock.
- `pkg/util/encoding` — order-preserving key encodings.
- `pkg/util/stop` — goroutine lifecycle / graceful shutdown (`server.Node.Drain` sheds leases and SQL connections before `Stop`).
- `pkg/keys` — keyspace layout helpers.
- `pkg/storage` — Pebble engine wrapper + MVCC (get/put/delete/scan, intents).
- `pkg/kvpb` — the KV API: `BatchRequest` / `BatchResponse` and request types.
- `pkg/rpc` — gRPC server/clients: Raft transport, KV batches, admin.
- `pkg/cluster` — bootstrap, join, node registry, liveness heartbeats.
- `pkg/kvserver` — `Replica` (one per range per store): Raft integration,
  command application, ReadIndex serving, txn record ops, splits.
- `pkg/kvclient` — `DistSender` (routing) and `TxnCoordinator` (transactions).
- `pkg/placement` — locality, the diversity allocator, upreplication.
- `pkg/sql` — parser, catalog, row encoding, executor, sessions.
- `pkg/pgwire` — PostgreSQL wire protocol server (built on `pgproto3`).
- `pkg/server` — the composition root wiring a node together.
