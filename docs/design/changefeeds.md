# Changefeeds (gap CR-CDC-001)

Status: proposed. This is a design only: nothing here is implemented.
The open questions for the owner are at the end. It also covers the
parts of CR-CDC-002 (`SHOW CHANGEFEED JOBS`, job control, metrics,
protected timestamps) that a durable changefeed cannot do without.
PostgreSQL logical decoding and the replication protocol (PG-W-001) are
out of scope; so are CDC queries (`CREATE CHANGEFEED ... AS SELECT`),
Avro and the enriched envelope, which are listed under phase 5.

Today both entry points fail in the parser (probed at `e8085c3` with
`parser.Parse`):

| Statement | Today |
|---|---|
| `CREATE CHANGEFEED FOR TABLE t INTO 'webhook-https://...'` | `42601 syntax error: expected TABLE, found "changefeed"` |
| `EXPERIMENTAL CHANGEFEED FOR t` | `42601 syntax error: unsupported statement "experimental"` |
| `SHOW CHANGEFEED JOBS` | parses as `SHOW <variable>`, then `42704` at execution |
| `PAUSE JOB 1`, `CANCEL JOB 1` | `42601 syntax error: unsupported statement "pause"` / `"cancel"` |

The workarounds in the register are `LISTEN` / `NOTIFY` from the
application, which is at most once and not durable (`pkg/sql/notify`:
"A notification exists only in memory"), and polling with `AS OF
SYSTEM TIME` over an `updated_at` column, which misses deletes.

CockroachDB is the reference for the statements, options and message
formats, as known to the author from its v24.x and v25.x documentation
(not re-fetched for this design). Where this document states a
CockroachDB format, the implementation must check it against that
documentation before it freezes a golden test (open question 9).

## Summary

* **Change events come from MVCC history, not from the raft log.** The
  raft log carries logical `BatchRequest`s that every replica evaluates
  at apply (`kvserver.applyCommand` → `evalWriteBatch`), and an intent
  becomes a committed version only when a later `ResolveIntent` applies.
  So a log entry does not say what committed. Two sources are proposed:
  * **A poller** (phases 1–2): every interval, an incremental export of
    each watched span over the window since the last one, with every
    revision (`ExportRequest.AllRevisions`, v21), at a timestamp below
    the closed timestamp. It reuses `Node.exportSpan`, which pushes and
    waits out intents, and the window logic of `descWatcher`. It needs
    no KV change. Its cost is a scan of the whole span per interval.
  * **Rangefeeds** (phase 3): each replica records the logical MVCC
    operations its applies perform (writes, intent commits and aborts)
    and streams them to registrations, with a resolved timestamp from
    the range's closed timestamp and its unresolved intents. Because
    datax evaluates at apply on every replica, any replica, not just the
    leader, can serve one.
* **Guarantees, as CockroachDB's:** at least once; a key's changes in
  timestamp order; a `resolved` message at `T` means no row at or below
  `T` will be emitted for the first time after it. datax adds one rule
  that makes the last guarantee survive restarts: the checkpoint is
  persisted **before** the resolved message is sent.
* **Durable jobs** are records under a new system span, claimed by a
  node under a renewable lease, with a checkpoint (high-water and a
  span frontier) written by compare-and-swap on the claim. They resume
  from the checkpoint after a restart, a leadership move, a split or a
  merge, and re-emit at most what lies above it.
* **History is protected.** The backup protection records of issue #376
  (`server/backup_protect.go`) are generalized to span-scoped protected
  timestamps that garbage collection honours, so a lagging feed does not
  lose its window. Purge (`ALTER TABLE ... PURGE`) still wins.
* **Schema changes** are seen by following the table's descriptor over
  the same windows (`catalogFollower`). Column changes follow
  `schema_change_policy` (`backfill`, `nobackfill`, `stop`). A layout swap
  (`TRUNCATE`, re-shard, primary-key change) moves the primary rows to a
  new index ID, so the feed follows `LivePrimaryIndex()` per timestamp.
  `TRUNCATE` and `DROP` stop the feed.
* **Sinks, in order:** sinkless `EXPERIMENTAL CHANGEFEED FOR` over
  pgwire (phase 1); webhook (phase 2); Kafka and cloud storage (phase 4),
  written on the standard library and modules already in `go.sum`, as
  the KMS roots were (`pkg/storage/enc/kms_*.go`, `sigv4.go`).
* **Options:** `updated`, `resolved`, `diff`, `initial_scan`,
  `envelope` (`wrapped`, `bare`, `key_only`, `row`), `format = json`,
  `cursor`, `end_time`, `mvcc_timestamp`, `schema_change_policy`,
  `on_error`, `gc_protect_expires_after`, and the webhook's own.
* **Security:** `SELECT` on every watched table and the `REPLICATION`
  role attribute (PostgreSQL's gate for replication, which datax
  records but which grants nothing today), or `admin`. Tables with row
  security that applies to the creator, and sealed tables, are refused
  in phases 1–2. Outbound URLs are checked against loopback, private,
  link-local and metadata addresses **at connect time**, redirects are
  not followed, and sink secrets are redacted everywhere they are shown.
* **A new cluster version** gates the job records and the protection
  records (phase 2), and the rangefeed RPC (phase 3). Phase 1 needs none:
  it uses only v21/v22 KV features and writes nothing.

## What the code does today

| Area | Today (where) | Consequence |
|---|---|---|
| Raft log | Entries carry a proto `RaftCommand` with the logical `BatchRequest` and triggers (`kvserver/raftcmd.go`). Every replica evaluates the batch at apply (`applyCommand` → `evalWriteBatch`, `apply.go`). | The log is not a change stream. A `PutRequest` in a transaction writes an intent; whether it commits, and at which timestamp, is decided by a later `ResolveIntentRequest` on the same range. But every replica sees exactly the same operations, so every replica can derive the same events. |
| MVCC layout | A key has a meta key (an intent's `MVCCMetadata`, with `Txn` and `Timestamp`) and versions at `EncodeMVCCKey(key, ts)`. An intent's provisional value is a version at its timestamp; commit at a moved timestamp rewrites it (`MVCCResolveIntentFound`, `storage/mvcc.go`). | A committed version's timestamp is its commit time. Intent resolution is "the ONLY way intents disappear" (comment on `MVCCResolveIntent`), so there is one place to observe commits and aborts. |
| One writer per key | `mvccWrite` refuses a transactional write when a committed version is at or above its timestamp (`WriteTooOldError`) and on any foreign intent. | One key's versions are created in timestamp order, so per-key ordering of events is free. |
| Locks | `MVCCLock` lays an intent carrying the observed value; on commit it becomes a version with the same bytes (`storage/mvcc.go`; see `docs/design/read-committed.md`, fact 3). | `SELECT ... FOR UPDATE` that commits produces a version the feed will see as an update with an unchanged value. |
| Incremental export | `ExportRequest` with `StartTS` returns the keys changed in `(StartTS, ts]`; `AllRevisions` (v21) returns every version, newest first within a key; every record carries its `Timestamp` (v22) (`kvpb/api.go`, `storage/mvcc_export.go`). An intent at or below `ts` is a `WriteIntentError`. | This is a poll-based change source with exact semantics already. |
| Export cost | `mvccExport` seeks once per key in `[start, end)` whatever the window, and `MaxRecords` bounds records, not keys scanned. The leader evaluates it under a shared latch on the request's span (`Replica.Execute`, `latches.Acquire`, released on return). | A poll scans the whole table. On a range with no changes one request scans every key while writers to that range wait for the latch. See "Poller cost". |
| Export helpers | `Node.exportSpan` chunks, pushes intents (`PushAndResolveIntents`) and waits 100 ms on live ones (`server/backup.go`). `descWatcher` / `catalogFollower` follow descriptor spans by incremental export over `(since, now]` and persist where they are (`server/desc_watch.go`, issues #313 and #316). | The poller and the descriptor follower for schema changes are compositions of existing code. |
| Closed timestamps | Each leader closes `now − 3s` every second, through the log or off it (`kvserver/closedts.go`). Closing bumps the timestamp cache over the whole range, so no later write lands at or below it. `EndTxn` is exempt from the cache check, so an intent laid **before** the bump may still commit at or below the closed timestamp. | Closed is not resolved: a feed must also wait out intents at or below the closed timestamp (the export does; a rangefeed must track them). `sql.FollowerReadLag` (4.8 s) is a timestamp every range has closed. |
| Splits, merges | A split carries the LHS closed timestamp to the RHS (`splitTrigger.ClosedTS`). A merge freezes the RHS (`Subsume`), absorbs its GC threshold and bumps the LHS timestamp cache over the absorbed span to the driver's `now` (`merge.go`, `finishMerge`); the merged range keeps the LHS closed timestamp. | Per-range closed timestamps can move backwards for a span across a merge. A feed's frontier keeps the maximum it has seen per span. |
| Leadership | Leaseholder is the raft leader; a lease transfer is a leadership transfer, and a new leader bumps its timestamp cache (`lease_transfer.go`). Reads go to the leader, except stale reads at or below the closed timestamp (`executeStaleRead`). | The poller is unaffected by moves (the client re-routes). A rangefeed served by a leader would be cut at every move; one served by any replica is cut only when the replica goes. |
| GC | Leaders collect versions older than the GC TTL (default `base.DefaultGCTTL` = 25 h; per table `gc_ttl`, at least 1 s). A replicated threshold refuses reads at or below it, and an incremental export whose `StartTS` is below it (`Replica.Execute`). `StoreConfig.GCLimit` caps every threshold; today it is fed by backup protection records only, one cluster-wide limit (`gcLimit`, `backup_protect.go`). | A feed lagging past a table's `gc_ttl` fails. Protection exists, but only for backups and only cluster-wide. |
| Purge | `PurgeSpan` waits `PurgeBackupWait` for backups, then purges below the oldest and reports itself incomplete (`purgeHold`). The dropped-table reclaim removes a span with one replicated range deletion (`ClearRangeRequest`); its comment says "There are no rangefeeds or changefeeds to tell" (`clearrange.go`, `docs/transactions.md`). | A purge destroys history a feed may still need. |
| Row expiry | The row-TTL job deletes expired rows with ordinary SQL `DELETE` transactions on the node leading range 1 (`server/rowttl.go`). Time-series retention removes versions beneath SQL as a GC rule, with no tombstone (`retention.go`). | TTL deletes reach a feed as deletes, indistinguishable from the application's. Time-series retention never does. |
| Rows | One KV per row in the primary index: key `/t/<table>/<index>/<pk>`, value self-describing per column ID, unknown IDs skipped (`sql/rowenc/rowenc.go`). The primary rows live at `TableDescriptor.LivePrimaryIndex()`, which `TRUNCATE`, re-shard and primary-key changes move to a fresh index ID, retiring the old layout (`catalog.go`, `docs/sql.md`). Hash-sharded tables have a hidden `_shard` key column. | No column families to stitch. The span to watch depends on the descriptor at each timestamp. |
| Schema changes | Online DDL publishes write-only pieces, drains leases, backfills in chunks, flips (`online_ddl.go`, `alter_multi.go`). A column added with a non-volatile default rewrites no row (`Column.FillDefault`); a volatile default backfills every row. | A feed sees a backfill as an update of every row unless it filters writes to non-public columns and indexes. |
| Sealed tables | Row values of a sealed table are AES-GCM under the table's (or tenant's) key; KV, raft, backups see ciphertext only (`sql/rowseal`). | A feed must decrypt to emit, which takes plaintext out of the cluster. |
| Row security | Applied as WHERE conjuncts at planning; root, node sessions, `BYPASSRLS`, and owner/`admin` without `FORCE` are exempt (`sql/rls.go`). | A feed that emits every row bypasses policies unless the creator is exempt. |
| Jobs | No jobs framework. Background work is a loop on every node where the leader of range 1 acts (`sealJobLoop`, `rowTTLLoop`), with progress in a system record (`catalog.SealJob`, `keys.SealJobKey`). | Changefeeds need per-job ownership instead, or every feed runs on one node. |
| Outbound HTTP | Only the KMS roots, configured by the operator on the command line: timeouts, bounded retries, no redirects ("it would carry the credential to a host the operator did not name"), secrets struck from messages (`storage/enc/kms.go`, `kmsHTTP`). | The client conventions exist. Nothing yet takes a URL from SQL, so there is no SSRF guard. |
| pgwire streaming | A `RowStream` result is written in 64 KiB batches (`pgwire/conn.go`, `sendStream`, `streamFlushBytes`). Cancel requests cancel the statement's context (`pgwire/cancel.go`). | A sinkless feed needs a flush that does not wait for 64 KiB. |
| Settings | `SET CLUSTER SETTING`, admin only, from a registry (`sql/clustersettings.go`); `ReadClusterSetting` is a KV read per call. | Feeds need a node-local cached read. |
| Version | `pkg/version`: `V22` is current. The read-committed and functions designs both propose `V23`. | See open question 1. |
| Dependencies | `go.mod` has eleven direct requirements. `pkg/metrics` notes client_golang "was already in the dependency tree via Pebble"; the Snowball stemmer was "written from the algorithm's description — no new dependency" (`docs/sql.md`); the AWS, GCP, Azure and Vault clients are hand-written on `net/http` with a local SigV4 (`storage/enc/sigv4.go`). The repository has no CONTRIBUTING file; the stance is the code's. | No Kafka or cloud SDK. `go.sum` already has `klauspost/compress`, `golang/snappy` and `DataDog/zstd` (indirect, via Pebble) for Kafka's codecs. |

## Goals and constraints

1. **CockroachDB's surface and guarantees** for the statements and
   options in scope: at-least-once delivery, per-key order, the
   resolved-timestamp promise, the JSON formats. Where datax differs,
   the difference is refused (`0A000`) or documented.
2. **No cost to clusters without feeds.** No new work on the write or
   apply path unless a feed watches the range. The rangefeed's
   operation capture is off until a registration exists.
3. **No silent loss.** A feed that cannot deliver a change (GC passed
   its window, a purge, a `TRUNCATE`, an undecryptable row) fails or
   pauses with an error naming why; it never skips and moves on.
4. **No new dependency** for any sink.
5. **Safe outbound traffic.** A SQL user cannot use a changefeed to
   reach addresses the operator has not allowed, or to read back what
   an internal service answers.
6. **Mixed versions are safe.** Nothing a node of an older binary would
   ignore or misread (a protection record, a job, a new RPC) is written
   or sent before the version gate.

## Event sources

### What a change is

A change is a committed MVCC version of a primary-index key of a
watched table: key, commit timestamp, value or tombstone, and with
`diff` the version before it. Intents are not changes. A version is
emitted once it is known to be committed and its timestamp is above the
feed's frontier for its span.

### Source A: the poller (phases 1–2)

Per feed, for each watched span `S` (the primary-index span of each
table as of the window, see "Schema changes"):

1. Pick `T = now − FollowerReadLag` (4.8 s, `sql/asof.go`). Every range
   has closed `T` unless publication has failed, so the export bumps
   nothing in the timestamp cache and pushes no writer. If a range has
   not closed it, the export's read on the leader bumps the cache to
   `T` (the standard read rule), which is still correct.
2. `exportSpan(S, startTS = frontier(S), endTS = T, revisions = true)`.
   Intents at or below `T` are pushed and waited out, as for backups.
   After it returns, no version at or below `T` can appear in `S`: a
   live intent above `T` resolves above `T`, and the cache keeps new
   writes above `T`.
3. Reverse each key's versions (the export gives newest first), decode
   with the descriptor valid at each version's timestamp, and hand the
   events to the encoder. Within a window, the events are sorted by
   `(timestamp, key)` before they are emitted, in memory bounded by
   `changefeed.memory_budget`; a window that does not fit is split in
   time (the export is retried for `(start, mid]`). Sorting is not
   required by the guarantees, but it gives a consumer transaction
   order within a window for free.
4. Advance `frontier(S)` to `T`.

`diff` needs each key's version before the oldest one in the window.
That is read with a `Get` at `frontier(S)` per changed key, batched.
Phase 2 adds `ExportRequest.WithPrevious` (behind the gate): the export
then also returns the newest version at or below `StartTS` of every key
it emits, which costs one more step of the same iterator.

**Poller cost.** `mvccExport` visits every key of `S` in every window,
and evaluates each sub-request under a shared latch on its span. A 1 s
interval on a large table therefore means a full scan per second and
writer stalls on every range for the length of that scan. Phase 1 adds
two things, both additive:

* `ExportRequest.MaxKeysScanned`: the replica stops after that many
  keys and returns `Resume`, whatever it found. An older replica
  ignores it and behaves as today, so it is sent only from the gate,
  and phase 1 works without it.
* An adaptive interval: the next window starts no sooner than twice
  the last window's scan time (`changefeed.poll_interval` is a floor).
  `SHOW CHANGEFEED JOBS` reports the scan time, and a metric counts the
  keys scanned per emitted row.

The poller is meant for small and medium tables and for proving the
rest of the system. Phase 3 replaces it. Time-bound iteration (open
question 4) would make it proportional to change instead.

### Source B: rangefeeds (phase 3)

**Operation capture.** `storage.Batch` gains an optional operation log,
set by the replica when its feed processor has registrations, and
filled by the MVCC functions that create or settle versions:

| Op | Emitted by | Fields |
|---|---|---|
| `WriteValue` | `mvccWriteCommitted` (one-phase commit), non-transactional `MVCCPut` / `MVCCDelete`, `Increment` outside a transaction | key, ts, value or tombstone |
| `WriteIntent` | `mvccWrite` with a transaction (first write of the key in the txn), `MVCCLock` | key, txn ID, ts |
| `UpdateIntent` | `mvccWrite` rewriting its own intent at a new timestamp | key, txn ID, new ts |
| `CommitIntent` | `MVCCResolveIntentFound` with `COMMITTED` that found the intent | key, txn ID, commit ts, value or tombstone, and with `diff` the previous version |
| `AbortIntent` | `MVCCResolveIntentFound` with `ABORTED` that found it; `MVCCRollbackIntent` when it removes the intent | key, txn ID |
| `Clear` | `ClearRangeRequest` that applied (the dropped-table reclaim) | span — the registrations over it end with an error |

GC produces no op: it removes only versions below the threshold, which
a feed above the threshold never needs. A `GCRequest` that raises the
threshold above a registration's frontier ends it with an error; a
purge raises the threshold the same way (`purge.go`), so it is covered.

The log is drained by the replica after `b.Commit` in `applyCommand`,
under `applyMu`, which is held for the whole apply. Ops are therefore
delivered in apply order, which is the same on every replica. The
`MVCCLock` op carries a flag so that its commit can be marked as a lock
(see open question 6).

**Registration and catch-up.** `RangeFeed(span, startTS, withDiff)` is a
streaming RPC. The replica takes `applyMu`, takes an engine snapshot,
adds the registration, and releases `applyMu`. Everything applied after
the snapshot reaches the registration as ops; everything at or before it
comes from a catch-up scan of the snapshot: `MVCCExportRevisions` over
`(startTS, snapshot]`, plus a scan of the meta keys to seed the
registration's set of unresolved intents. Nothing is missed or
delivered twice between the two. A catch-up below the GC threshold is
refused, as the export is today.

**Resolved timestamp.** Per range,
`resolved = min(closedTS, min over unresolved intents of ts) − 1 logical`.
The processor keeps the unresolved intents in a min-heap by timestamp,
fed by `WriteIntent`, `UpdateIntent`, `CommitIntent` and `AbortIntent`.
The closed timestamp is the replica's (`ClosedTimestamp()`, which takes
the logged and the off-log promises into account), read after each
apply and on each publication. Intents laid before a closed-timestamp
bump and committed below it (the `EndTxn` exemption in `closedts.go`)
are exactly what the heap holds back. An intent older than
`changefeed.push_txn_after` (default 10 s) is pushed through the
existing `PushTxn` / recovery path, so an abandoned transaction does
not stall the feed; the resolution that follows arrives as an op.

**Which replica serves.** Because ops are derived at apply, any replica
that has applied an op can emit it, and every replica has the closed
timestamp. Phase 3 serves from the replica nearest the feed's
aggregator, as `replicaOrder` already ranks replicas for stale reads.
A follower's off-log closed timestamp is honoured only while it follows
the same leader at the same term (`acceptClosedTimestamp`), so a
follower's resolved timestamp may stall for a publication interval
after a leadership change; that is acceptable.

**When a registration ends.** With a retryable error, and the client
re-registers from its per-span frontier:

| Event | Why |
|---|---|
| Split or merge applies | The span now belongs to other range IDs. |
| Catch-up snapshot installed (`catchup.go`) | The state jumped; ops between are lost. |
| Replica removed or GC'd | It no longer exists here. |
| Registration buffer full | Back-pressure: a slow consumer is caught up by a catch-up scan instead of holding memory. |
| Node draining | The replica is going away. |

With a permanent error: the GC threshold or a `Clear` passed the
frontier. The client (`kvclient.RangeFeed`) keeps one frontier per
span, re-resolves the span through the range cache on a retryable error
and registers on each new range from that frontier. Duplicates from a
re-registration are allowed by at-least-once delivery.

**Cost when unused.** One nil check per apply, plus one branch per MVCC
write. The processor starts on the first registration and stops after
the last.

### Comparison

| | Poller | Rangefeed |
|---|---|---|
| New KV code | none (two optional request fields) | op capture, processor, RPC, client |
| Cost | O(table) per interval | O(changes) |
| Latency | interval + 4.8 s lag | closed-timestamp lag (3 s + 1 s) |
| Writer impact | shared latch for each scan | none beyond op capture |
| Long transactions | the export waits them out | the heap holds resolved back; pushes after 10 s |
| Version gate | none for phase 1 | yes (new RPC) |

Phase 3 keeps the poller as the fallback while any node predates the
gate, and as a debugging aid (`WITH experimental_poll_only`, unlisted).

## Ordering, resolved timestamps and delivery

* **Per key:** events are emitted in timestamp order within one run of
  a feed. This follows from one writer per key (above) for rangefeeds,
  and from the per-key reversal of the export for the poller.
* **Across keys:** no order, except that the poller sorts each window.
  Consumers that need transaction order buffer until a resolved
  message.
* **Resolved:** the feed's frontier is a span frontier
  (`ts per span, resolved = min`). A message `{"resolved": "T"}` is sent
  only when the frontier has reached `T`, every row at or below `T`
  has been acknowledged by the sink, and the checkpoint at `T` is
  durable. Messages are rate-limited by the `resolved` option's
  interval (default 1 s for sinkless, the checkpoint interval for jobs).
* **At least once:** a row may be emitted again after a restart, a
  re-registration, a sink retry or a job moving between nodes. The
  `updated` option (or `mvcc_timestamp`) gives each row its timestamp,
  and `(key, updated)` identifies a change, so consumers can deduplicate.
* **The persist-then-announce rule.** The order is: rows acknowledged →
  checkpoint written at `T` → resolved `T` sent. A crash after the
  checkpoint and before the message loses only the message. A crash
  before the checkpoint re-emits rows above the old checkpoint, and
  none of those is at or below any resolved message already sent.
  So "no new row at or below a resolved timestamp" holds across
  restarts, which a consumer that applies rows up to each resolved
  message relies on.

## Jobs and durability (phase 2)

### The record

```
/system/changefeed/<jobID>          → JSON catalog.ChangefeedJob
/system/changefeed-protect/<jobID>  → protection record (below)
```

`keys.ChangefeedJobKey(id)` and `ChangefeedJobSpan()` follow the
`SealJobKey` pattern. IDs come from a new counter beside
`keys.DescIDGenKey`. The record holds:

* the statement as written, with secrets redacted, and the parsed
  targets: table IDs, never names (a rename does not retarget a feed);
* the sink URI and options, with the secret parts in a separate field
  that is never shown (see "Security");
* the owner role, the creation time and the cluster version it was
  created at;
* status (`running`, `paused`, `failed`, `canceled`, `succeeded` for
  `end_time` / `initial_scan = 'only'`), the last error, and
  `on_error` behaviour;
* the claim: `node_id`, `claim_epoch`, `expires_at`;
* the checkpoint: `high_water` and a compact frontier of the spans
  ahead of it (at most `changefeed.checkpoint_spans`, default 256,
  merged by taking the lower timestamp when over), and the descriptor
  version each target was last decoded with.

The record is excluded from backups by construction: backup copies
only the groups `manifestKeyGroups` lists, and none is added.

### Ownership

Every node runs `changefeedLoop`. Each tick it reads the job span in a
transaction (pushing a dead holder's lock, as `RunSealJobsOnce` does)
and claims any `running` job whose claim has expired, writing its own
node ID, a new `claim_epoch` and `expires_at = now +
changefeed.claim_ttl` (default 30 s). It renews its claims at a third
of the TTL. Claims are spread by preferring the node with the fewest
claims (each node publishes its count in the record it claims under;
ties broken by node ID), so that feeds do not all land on one node as
range-1 singletons do. A drain gives up claims first.

Every checkpoint write is a transaction that reads the record, checks
`node_id` and `claim_epoch`, and writes the new checkpoint. A node that
has lost its claim (a partition, a long GC pause) fails its next
checkpoint write and stops. Until then it may emit rows another owner
also emits: duplicates, which at-least-once allows. It cannot send a
resolved message the new owner has not persisted, because of the
persist-then-announce rule.

### Resume

A claimed job starts from its checkpoint: the poller's windows start at
each span's frontier; rangefeeds register from it. It decodes with the
descriptor versions recorded there, so a schema change that happened
while it was down is seen in order.

| Event | Poller | Rangefeed |
|---|---|---|
| Node running the job dies | Claim expires, another node resumes from the checkpoint. | Same. |
| Leader of a watched range moves | `kvclient` re-routes the export; nothing else. | A registration on the old leader is unaffected if it is on a follower; otherwise it ends and re-registers. |
| Split | The export is stitched across ranges by `sendExport`. | Registration ends; the client registers on both halves from the span frontier. |
| Merge | Same as split. | Same; the frontier keeps the higher of the two halves' timestamps per sub-span. |
| Raft log truncation | Irrelevant: events come from MVCC. | Irrelevant: ops are produced at apply. |
| Catch-up snapshot | Irrelevant. | Registration ends and re-registers with a catch-up scan. |

### Protected timestamps

Generalize `backupProtection` into protection records with a kind
(`backup`, `changefeed`), a span list and an expiry:

* `StoreConfig.GCLimit` becomes span-aware:
  `GCLimit(ctx, start, end) (limit, ok, err)`, evaluated per range in
  `RunGCOnce`. A lagging feed on one table then holds back only that
  table, not the cluster as a backup does today. The records are read
  once per pass and indexed by span.
* A running job protects its high-water mark on its target spans,
  raising the record before advancing the checkpoint past it
  (`raise`, as backups do). The record's expiry is
  `gc_protect_expires_after` (default 24 h) beyond its last renewal, so a
  failed or paused job does not hold history forever; a paused job keeps
  protecting only with `protect_data_from_gc_on_pause`.
* The race between GC reading the records and a feed choosing its
  start is closed by time, as `backup_protect.go` closes it: a new feed
  writes its record, waits twice the maximum clock offset, then takes
  its start timestamp.
* **Purge wins.** A purge is an erasure tool. It waits
  `PurgeBackupWait` for backups only. If it purges below a feed's
  frontier, the feed fails with `changefeed history for table t was
  purged at <ts>; recreate the changefeed with a cursor after it`. See
  open question 7.
* A feed whose frontier falls below a span's GC threshold (protection
  expired or disabled) fails with the same kind of message, naming the
  threshold.

This is the protection half of CR-CDC-002.

## Schema changes

### Following the descriptor

The feed follows each target's descriptor key over the same windows as
its rows, with an incremental export of `keys.TableDescKey(id)` with
revisions (what `descWatcher` does for whole spans). For every
descriptor version at timestamp `D` it knows the descriptor in force
for rows at timestamps at or above `D`. Rows are decoded with
`rowenc` using that descriptor's public columns only: write-only
columns (`Column.Adding`), the `RetypeFrom` shadow and hidden columns
are not emitted, and `FillDefault` columns are filled as a read would.

A row written at or above `D` may have been written by a gateway still
holding the previous lease version. `rowenc` values are
self-describing and skip unknown column IDs, so decoding with the newer
descriptor is still exact for the public columns.

### Which span

The span watched for a table at timestamp `t` is
`/t/<id>/<LivePrimaryIndex() as of t>/`. Writes to an index that is not
yet the primary (a re-shard's or primary-key change's backfill) are not
watched, so they emit nothing.

### Policies

| Change | Default (`schema_change_policy = backfill`) | `nobackfill` | `stop` |
|---|---|---|---|
| `ADD COLUMN`, `DROP COLUMN`, `ALTER COLUMN TYPE` (flip at `D`) | After the flip, every row is emitted again as of `D` (an initial scan at `D`), then changes resume. The volatile-default backfill itself emits nothing. | Nothing is re-emitted; later rows carry the new shape. | The feed ends (`succeeded`) with high-water `D − 1`. |
| Re-shard (layout swap at `D`, same PK columns) | Nothing re-emitted: the logical keys are the same. The span switches at `D`. | Same. | Same. |
| Primary-key change (`ALTER PRIMARY KEY`, layout swap) | Every row re-emitted as of `D` under the new key. | The span switches; rows before `D` keep their old keys. | Ends at `D − 1`. |
| `TRUNCATE` (layout swap to an empty index) | Fails: `table t was truncated`. The rows vanish with no tombstones, so the feed cannot express it. | Same. | Same. |
| `DROP TABLE` | Fails: `table t was dropped`. | Same. | Same. |
| `RENAME` | No effect on rows; the topic name keeps the name at creation unless `full_table_name` is given, then follows. | Same. | Same. |
| Index, constraint, comment, privilege changes | Nothing. | Nothing. | Nothing. |

`schema_change_events = column_changes` (the default) limits the
policies above to the column changes; `default` also stops on any
descriptor change under `stop`. The emitted key never includes hidden
columns (`_shard`), so a re-shard does not change any key (open
question 8).

## Interactions

* **Backup and restore.** Job and protection records are not backed up.
  A table restore creates a new table ID (`restore_table.go`), which no
  feed targets, so it emits nothing. A full restore goes into an empty
  cluster, which has no feeds. A backup's protection and a feed's are
  independent records under the same GC rule.
* **Row expiry.** TTL deletes are SQL deletes and reach feeds as
  deletes. CockroachDB lets a table opt out of that. datax cannot tell
  who wrote a version: neither a value nor an intent records an origin.
  Doing so needs an origin flag on intents and versions, which is an
  on-disk format change (open question 5). Until then the user doc says
  so.
* **Time-series retention** removes rows by GC, with no tombstone, so a
  feed never sees those rows go. `CREATE CHANGEFEED` on a time-series
  table succeeds with a `NOTICE` saying so.
* **Locks.** A committed `SELECT ... FOR UPDATE` lock is a version with
  unchanged bytes. The poller cannot tell it from an update. A rangefeed
  can, from the lock flag on its `WriteIntent` op, and drops it (open
  question 6).
* **Sealed tables.** Refused in phases 1–2 (`0A000 changefeeds on sealed
  tables are not supported`). See "Security".
* **Temporary tables, views, sequences and system tables** are refused
  (`0A000` / `42809`, as CockroachDB refuses views).
* **Isolation.** All changefeed reads are non-transactional exports or
  rangefeeds. Weak isolation (read-committed design) changes nothing
  here: commit timestamps are still version timestamps.

## Statements

```
CREATE CHANGEFEED FOR [TABLE] t [, ...] INTO '<sink URI>' [WITH option [= value] [, ...]]
EXPERIMENTAL CHANGEFEED FOR [TABLE] t [, ...] [WITH option [= value] [, ...]]
SHOW CHANGEFEED JOBS | SHOW CHANGEFEED JOB <id>
PAUSE JOB <id> | RESUME JOB <id> | CANCEL JOB <id>
```

* `CREATE CHANGEFEED` returns one row, `job_id INT8`. It runs outside a
  transaction block (`25001` inside one, as CockroachDB refuses it),
  because the job record must not roll back after the feed starts.
* `EXPERIMENTAL CHANGEFEED FOR` (sinkless) returns rows
  `(table STRING, key BYTES, value BYTES)` until the client cancels or
  disconnects, `end_time` passes, or an error occurs. Resolved messages
  are rows with `table` and `key` NULL. It runs in the session, with no
  job and no checkpoint, on the poller in phase 1 and on rangefeeds from
  phase 3. `25001` inside a transaction block. `statement_timeout`
  applies as to any statement; the user doc says to set it to 0.
* `ALTER CHANGEFEED` is phase 5.
* Errors: unknown table `42P01`; an option datax does not support yet
  `0A000 option "x" is not supported`; an unknown option `22023`;
  privileges `42501`; a sink scheme not supported `0A000`.

### Sinkless flushing

`sendStream` writes rows once 64 KiB have accumulated. A feed with a
trickle of changes would sit on them. `RowStream` gains a flush hint
that the changefeed stream sets after each window's last row and after
each resolved row; `sendStream` flushes when the hint is set. The
change is local to `pgwire/conn.go` and `sql/stream.go` and does not
alter other streams. A stream that has flushed is never restarted
(`Flushed`), which is already the rule.

## Options

| Option | Phase | Meaning |
|---|---|---|
| `updated` | 1 | Add `"updated": "<ts>"` (HLC as `wall.logical` decimal string) to the value. |
| `mvcc_timestamp` | 1 | Add `"mvcc_timestamp"`, the version's own timestamp (same as `updated` except on an initial or backfill scan, where `updated` is the scan time). |
| `resolved [= interval]` | 1 | Emit resolved messages, at most once per interval. |
| `diff` | 1 | Add `"before"`: the row before the change, or `null`. |
| `initial_scan = 'yes' \| 'no' \| 'only'` | 1 | Emit every row as of the start before changes. Default `yes` without `cursor`, `no` with it; `only` ends the feed after the scan. |
| `cursor = '<ts>'` | 1 | Start from a past timestamp. Refused below the GC threshold of any target span (`22023`). |
| `end_time = '<ts>'` | 1 | End when the frontier passes it. |
| `envelope = wrapped \| bare \| key_only \| row` | 1 | `wrapped` (default): `{"after": {...}}`. `bare`: the row's columns at top level with a `__crdb__` object for `updated` / `before` (CockroachDB's name, kept for consumer compatibility; open question 9). `key_only`: no value. `row`: the row only. |
| `format = json` | 1 | The only format until phase 5 (`avro` → `0A000`). |
| `key_in_value` | 1 | Add `"key"` to the value. Implied for webhook and cloud storage, which have no message key. |
| `topic_in_value` | 2 | Add `"topic"`. |
| `full_table_name` | 2 | Topic `db.schema.table` instead of `table`. |
| `schema_change_policy`, `schema_change_events` | 1 | As above. |
| `on_error = fail \| pause` | 2 | What a non-retryable error does to the job. |
| `gc_protect_expires_after`, `protect_data_from_gc_on_pause` | 2 | Protection, as above. |
| `min_checkpoint_frequency` | 2 | Floor on checkpoint writes (default 30 s). |
| `webhook_auth_header`, `webhook_sink_config`, `webhook_client_timeout`, `ca_cert`, `client_cert`, `client_key` | 2 | Webhook, below. |
| `kafka_sink_config`, `sasl_*`, `tls_enabled` | 4 | Kafka. |
| `split_column_families` | — | Accepted and ignored: datax has one family. |
| `avro_*`, `confluent_schema_registry`, `envelope = enriched`, `AS SELECT` | 5 | `0A000` until then. |

### JSON encoding

Keys are a JSON array of the visible primary-key columns in key order;
values are objects keyed by column name. Each datum is encoded by
`builtins.ToJSONB`, the function behind `to_jsonb`, so the mapping is
PostgreSQL's (numerics as JSON numbers, timestamps as ISO 8601 strings,
`bytea` as `"\\x..."`). Golden tests pin one value of every type.
Where CockroachDB's changefeed encoding differs from `to_jsonb` (it
does for some types), open question 9 decides.

## Sinks

### Sinkless (phase 1)

Above. It is the test harness for everything else: every option is
first tested through it.

### Webhook (phase 2)

`webhook-https://host[:port]/path[?options]`. Rows are batched into
`POST` bodies `{"payload": [<row>, ...], "length": n}`; a resolved
message is its own body `{"resolved": "<ts>"}`. Batching follows
`webhook_sink_config`: `{"Flush": {"Messages": n, "Bytes": b,
"Frequency": "1s"}, "Retry": {"Max": n, "Backoff": "500ms"}}`.

* A 2xx acknowledges the batch. 408, 429, 5xx and transport errors are
  retried with exponential backoff and jitter up to `Retry.Max`, then
  the job follows `on_error`. Any other 4xx is not retried.
* Batches for one key are sent in order: a key's next batch waits until
  the previous one is acknowledged. With `Flush.Messages` per worker and
  keys hashed to workers, that keeps per-key order with parallelism.
* The client is the KMS client's shape (`kmsHTTP`): per-request timeout
  (`webhook_client_timeout`, default 3 s), no redirects, TLS 1.2 or
  later, `ca_cert` or the system roots, optional client certificate,
  and every secret struck from messages. It does **not** use
  `http.ProxyFromEnvironment` unless the operator sets
  `changefeed.sink.use_proxy`, because a proxy would defeat the address
  check below.
* `webhook-http://` is refused unless `changefeed.sink.allow_insecure`
  is set by an admin.

### Kafka and cloud storage (phase 4)

Both are written on the standard library and modules already in
`go.sum`, as the KMS roots are:

* **Kafka:** a producer for the subset needed: `ApiVersions`,
  `Metadata`, `Produce` (v3+ record batches), idempotent producer IDs
  (`InitProducerId`), TLS, SASL PLAIN and SCRAM-SHA-256/512 (datax
  already implements SCRAM in `pkg/security/scram.go` for its server
  side; the client side is small). Compression `none`, `gzip`
  (stdlib), `snappy` (`golang/snappy`), `zstd` (`klauspost/compress`),
  each already in `go.sum` through Pebble; making one direct is a
  `go.mod` change with no new module (open question 3). Partitioning by
  key hash (murmur2, as Kafka's default partitioner, so keys land where
  other producers would put them). Per-key order holds because a key
  maps to one partition and the idempotent producer keeps order within
  a partition across retries.
* **Cloud storage:** `s3://`, `gs://`, `azure://`, and `nodelocal://`
  (for tests and air-gapped sites; admin only). Files are newline
  delimited JSON, named `<ts>-<job>-<node>-<seq>-<topic>.ndjson`, with
  resolved timestamps as `<ts>.RESOLVED` marker files, as CockroachDB
  lays them out. Credentials: explicit in the URI, or `AUTH=implicit`
  (the node's own identity: instance metadata, workload identity), which
  is **admin only**, because it lends the node's cloud role to a SQL
  user. The SigV4 signer (`storage/enc/sigv4.go`) and the token flows
  (`kms_gcp.go`, `kms_azure.go`, `kms_aws.go`) move to a shared package,
  `pkg/util/cloudauth`, used by both.

Google Pub/Sub, Azure Event Hubs and the others are not planned; each
would be its own question.

## Security

### Privileges

* **To create a feed:** `SELECT` on every target table, **and** either
  the `REPLICATION` role attribute or membership in `admin`.
  PostgreSQL requires `REPLICATION` (or superuser) to stream changes,
  and datax already records it (`docs/user/postgres-differences.md`:
  "`REPLICATION` ... recorded and shown but grant nothing"). This
  avoids a new table privilege, which `pg_dump` and other tools would
  not understand (CockroachDB's `CHANGEFEED` privilege; open question 2).
* **Sinkless** needs the same.
* **To see a job:** its owner, or `admin`. `SHOW CHANGEFEED JOBS` lists
  only those.
* **Pause, resume, cancel:** the owner or `admin`.
* **The job runs as its owner.** Each window re-checks `SELECT` on each
  target as of the window. A `REVOKE` therefore stops the feed within
  one window (`42501`, job `failed`). Dropping the owner fails the job.
* **Row security.** If row security applies to the owner on a target
  (not exempt by `rls.go`'s rules), creation is refused with `0A000
  changefeeds on tables with row security that applies to the role are
  not supported`. Filtering events through policies needs the CDC query
  machinery (phase 5). Enabling row security on a target later fails
  the feed at its next window, unless the owner is exempt.
* **Sealed tables** are refused in phases 1–2. Emitting them takes
  plaintext out of the cluster, which is what sealing exists to
  prevent, and a destroyed tenant key would make rows unreadable in the
  middle of a feed. Open question 10.
* **Audit.** Create, alter, pause, resume, cancel, and fail are written
  to the audit table (`pkg/audit`), with the sink's host but never its
  secrets.

### Outbound URLs (SSRF)

A changefeed makes the node open connections to a URL a SQL user
chose. Without limits a user could reach the cloud metadata service
(`169.254.169.254`, `fd00:ec2::254`), the node's own HTTP API or
anything on the cluster's network, and read back what it answered
through error messages. The rules:

1. **Address check at connect time.** The dialer's `Control` hook
   refuses a connection whose resolved IP is loopback, unspecified,
   private (RFC 1918, RFC 4193), link-local (including the metadata
   addresses), shared (`100.64.0.0/10`), multicast, or an IPv4-mapped
   form of any of them. Checking the IP being connected to, not the
   name, defeats DNS rebinding.
2. **Operator overrides,** admin only:
   `changefeed.sink.allowed_cidrs` (addresses allowed despite rule 1)
   and `changefeed.sink.allowed_hosts` (if set, only these host names).
3. **No redirects,** as in `kmsHTTP`.
4. **No response content in errors.** A failed request is reported as
   its status code and reason phrase only. The body is logged at
   `Debug` on the node, truncated, with secrets struck. Otherwise a feed
   pointed at an internal service would print its answer in `SHOW
   CHANGEFEED JOBS`.
5. **Schemes:** `webhook-https` (phase 2); `kafka`, `s3`, `gs`, `azure`
   (phase 4); `nodelocal` admin only; everything else `0A000`.
   `insecure_tls_skip_verify` is refused unless the creator is `admin`.
6. **Validation at creation:** the URI is parsed and the host resolved
   and checked when the statement runs, so obvious mistakes fail at
   once; the connect-time check is what enforces it.
7. **Secrets** (`webhook_auth_header`, passwords in the URI, SASL
   passwords, `client_key`, cloud keys) are kept in a separate field of
   the job record, replaced by `redacted` in every display (`SHOW`, the
   description, audit, events, logs), and struck from error messages as
   `kmsHTTP.redact` does.

The same rules apply to the Kafka bootstrap and every broker address
learned from `Metadata`, since a hostile broker can name internal ones.

## Observability

* **`SHOW CHANGEFEED JOBS`:** `job_id`, `description` (redacted),
  `user_name`, `status`, `running_status`, `created`, `started`,
  `finished`, `modified`, `high_water_timestamp`,
  `readable_high_water_timestamptz`, `error`, `sink_uri` (redacted),
  `full_table_names`, `topics`, `format`, and two datax columns:
  `node_id` (current owner) and `source` (`poller` or `rangefeed`).
* **Metrics** (`pkg/metrics`, `datax_changefeed_*`): `running` (gauge),
  `emitted_messages_total`, `emitted_bytes_total`, `flushes_total`,
  `sink_errors_total` and `sink_retries_total` by sink kind,
  `max_behind_nanos` (now − the lowest high-water on this node),
  `checkpoint_lag_nanos`, `commit_latency_seconds` (version timestamp
  to sink acknowledgement), `poll_keys_scanned_total`,
  `poll_scan_seconds`, `rangefeed_registrations`,
  `rangefeed_catchup_scans_total`, `rangefeed_buffer_overflows_total`,
  `rangefeed_unresolved_intents`, `rangefeed_txn_pushes_total`,
  `protected_ts_age_seconds`.
* **Events and operations:** a feed's start, claim, pause, failure and
  end are events of kind `changefeed` in the node's ring
  (`pkg/util/events`), and an open feed is an operation in
  `/api/operations`, so an operator sees which node runs it.
* **The console** gets a changefeeds page in phase 2, from the same
  `SHOW` data.

## Configuration and rollout

* **Cluster settings** (registered in `clusterSettings`, read through a
  node-local cache refreshed every 10 s): `changefeed.enabled` (default
  `true` once released), `changefeed.poll_interval` (1 s),
  `changefeed.memory_budget` (64 MiB per node), `changefeed.claim_ttl`,
  `changefeed.checkpoint_spans`, `changefeed.push_txn_after`, and the
  `changefeed.sink.*` security settings above.
* **Version gate:** the next free cluster version, written `Vcdc` here
  (open question 1). Below it, `CREATE CHANGEFEED` is refused with
  `0A000 changefeeds need cluster version Vcdc: finalize the upgrade
  first`. Behind it:
  * the job and protection records (an older node's GC would not read
    the protections, and an older node would not run the jobs);
  * `GCLimit` per span (every node must read the records before any is
    written);
  * `ExportRequest.MaxKeysScanned` and `WithPrevious`;
  * the `RangeFeed` RPC (phase 3; a later version if phase 3 ships in a
    later release).
  Phase 1 (sinkless, poller) needs v21 (revision exports) and is
  otherwise ungated: it writes nothing and sends no new field.
* **Golden decode tests** for the job record, the protection record and
  the `RangeFeed` messages (version rule 5).
* **Docs:** a "Changefeeds" section in `docs/user/sql.md`; the
  differences (no Avro, no CDC queries, TTL deletes not filterable,
  time-series retention not emitted, sealed and row-security tables
  refused) in `docs/user/postgres-differences.md`; the comment in
  `clearrange.go` and the sentence in `docs/transactions.md` updated
  when rangefeeds exist; CR-CDC-001 and the covered parts of CR-CDC-002
  closed in the register.

## Phased plan

| Phase | Content | Size |
|---|---|---|
| 0 — groundwork | Span-aware protection records and `GCLimit`; the descriptor-at-timestamp reader on `catalogFollower`; the JSON row encoder; the `RowStream` flush hint; the outbound HTTP client with the address check; cached cluster settings | M |
| 1 — sinkless | Parser for `EXPERIMENTAL CHANGEFEED FOR`; the poller; options `updated`, `mvcc_timestamp`, `resolved`, `diff`, `initial_scan`, `cursor`, `end_time`, `envelope`, `format = json`, `key_in_value`, schema-change policies; privileges, RLS and sealed refusals; `MaxKeysScanned` | L |
| 2 — jobs and webhook | `CREATE CHANGEFEED`; job records, claims, checkpoints, resume; protection; `SHOW CHANGEFEED JOBS`, `PAUSE` / `RESUME` / `CANCEL JOB`; webhook sink; SSRF rules; metrics, events, audit; `WithPrevious`; version gate | L |
| 3 — rangefeeds | Operation capture; per-replica processor; catch-up scans; resolved timestamps with intent tracking and pushes; the `RangeFeed` RPC and `kvclient.RangeFeed`; feeds switch source; poller kept as fallback | XL |
| 4 — more sinks | Kafka producer; cloud storage (S3, GCS, Azure, nodelocal); `pkg/util/cloudauth` | L (each M) |
| 5 — later | Avro and a schema registry client; `envelope = enriched`; CDC queries (`AS SELECT`, which also gives row-security filtering); `ALTER CHANGEFEED`; aggregators on the nodes holding the data; origin flags for TTL deletes | XL |

Phases 1–2 close CR-CDC-001 for the webhook and sinkless forms and most
of CR-CDC-002; phase 4 closes the Kafka and cloud-storage parts.
Phase 3 is what makes feeds usable on large tables.

### Phase 0 tests

* `RunGCOnce` with span-scoped protections: a protected span keeps its
  versions; an unprotected neighbour is collected; an expired record
  holds nothing (`pkg/kvserver`, and `gcttl_test.go` in
  `pkg/testutils/testcluster`).
* Backup protection unchanged: the existing `backup_purge_test.go` and
  `restore_retention_test.go` pass as they are.
* The encoder: one golden JSON value per type, NULLs, hidden and
  write-only columns excluded, `FillDefault` columns filled.
* The address check: a table of addresses (v4, v6, mapped, link-local,
  metadata, `100.64/10`) against a local listener; a DNS name that
  resolves to loopback is refused at connect; a redirect is not
  followed; `allowed_cidrs` lets one through.
* The flush hint: a stream with one small row reaches the client
  without 64 KiB behind it.

### Phase 1 tests

* **Parser:** every form and option, round-tripped through `format.go`;
  the refused options and their codes.
* **Units (`pkg/sql`):** window arithmetic and frontier; per-key
  reversal and the `(ts, key)` sort; window split under the memory
  budget; `diff` across a window boundary.
* **Integration (`pkg/testutils/testcluster`, new
  `changefeed_test.go`):**
  * insert, update, delete, upsert, multi-row transactions; the events
    match, and each `updated` is a timestamp at which `AS OF SYSTEM
    TIME` reads the new row and just below which it reads the old one;
  * **the validator:** a goroutine applies each event to an in-memory
    copy of the table; at every resolved `T` the copy equals `SELECT *
    ... AS OF SYSTEM TIME T`. Per key, `updated` never decreases.
    Run under the bank workload from `chaos_test.go`, where the copy's
    sum must be the invariant total at every resolved timestamp;
  * a long transaction holding an intent below `T` delays the window
    and is then emitted;
  * splits, merges and leadership transfers during a feed
    (`splitat_test.go`, `merge_test.go` helpers);
  * each schema change in the policy table, under each policy;
  * `cursor` below the GC threshold refused; GC passing a sinkless
    feed's frontier fails it with the message;
  * privilege, `REPLICATION`, RLS and sealed refusals; `REVOKE` during
    a feed;
  * cancel from another connection ends the statement;
  * `-race` on the new code.

### Phase 2 tests

* Golden decode tests for the job and protection records.
* Claim CAS: two nodes claiming one job; a zombie owner's checkpoint
  write fails; duplicates appear, the validator still holds.
* **Crash tests** (`pkg/testutils/crash`): kill the owner at fault
  points between rows-acknowledged, checkpoint-written and
  resolved-sent; after restart the consumer sees no new row at or below
  any resolved message it already received.
* Drain moves claims.
* A webhook test server (on loopback, allowed through
  `allowed_cidrs` for the test) that returns 500s, times out, returns
  429, closes connections and acknowledges slowly; per-key order holds
  and every row arrives at least once.
* Protection: a paused job keeps or releases history as configured;
  GC elsewhere is not held; purge fails the feed.
* `SHOW CHANGEFEED JOBS` visibility and redaction; audit records carry
  no secret; error messages carry no response body.
* Mixed version: below the gate, creation is refused; a node at the
  old binary in the cluster does not collect protected history after
  finalize.

### Phase 3 tests

* **Storage units:** each MVCC function logs the op the table says, and
  none when the log is off; `MVCCResolveIntentFound` logs only when it
  found the intent.
* **Processor units:** registration under `applyMu` with concurrent
  applies loses and duplicates nothing (checked against an export);
  resolved timestamp with intents laid before a closed-timestamp bump;
  push of an abandoned transaction; buffer overflow ends the
  registration retryably.
* **Integration:** the phase 1 validator, with rangefeeds, under the
  chaos scenarios (node kill, partition, leadership churn); splits and
  merges at high rate (`autosplit_test.go` settings); catch-up snapshot
  installation; follower-served registrations across a leadership
  change.
* **Equivalence:** the same workload through the poller and through
  rangefeeds produces the same set of `(key, ts, value)`.
* **Cost:** the `bench/` write benchmark with no feed shows no
  regression beyond noise; with one feed, the overhead is reported.

### Phase 4 tests

* Kafka: against a local broker in CI where available, and against a
  protocol fixture (recorded exchanges) otherwise; idempotent retries
  keep partition order; SASL SCRAM against test vectors.
* Cloud storage: against local S3- and GCS-compatible fixtures; file
  naming and `RESOLVED` markers; implicit auth refused for non-admins.
* The SSRF rules for every broker address learned from `Metadata`.

## Effort and risks

| Phase | Effort |
|---|---|
| 0 | M (2 engineer-weeks) |
| 1 | L (4–5) |
| 2 | L (5–6) |
| 3 | XL (8–10) |
| 4 | L (3–4 each sink) |

**Risks**

1. **Poller cost on large tables.** A full scan per interval under a
   shared latch. *Mitigation:* `MaxKeysScanned`, the adaptive interval,
   the scan metric, and documenting phase 1–2 for small tables; phase 3
   removes it.
2. **Missed or duplicated ops at registration.** *Mitigation:* the
   registration under `applyMu`, and the equivalence test against
   exports.
3. **Resolved timestamps stalled by intents.** A long transaction holds
   every feed on its range. *Mitigation:* pushes after 10 s; the
   `rangefeed_unresolved_intents` metric; `max_behind_nanos` alerts.
4. **GC held by a forgotten feed.** *Mitigation:* expiry on every
   protection record; span scope; the age metric.
5. **SSRF through new paths** (Kafka brokers, cloud endpoints, proxies).
   *Mitigation:* one dialer used by every sink, with the check in
   `Control`; the proxy off by default.
6. **Encoding drift from CockroachDB.** Consumers written for it may
   parse datax's JSON. *Mitigation:* golden tests checked against its
   documentation (open question 9).

## Open questions for the owner

1. **Which cluster version?** The read-committed and functions designs
   both claim V23. Recommendation: whichever ships first takes V23 and
   the others take the next; changefeeds' phase 2 records and phase 3
   RPC share one version if they ship in the same release.
2. **Privilege model:** `SELECT` plus the `REPLICATION` attribute (as
   PostgreSQL gates replication), or a new `CHANGEFEED` table privilege
   (as CockroachDB does)? Recommendation: `REPLICATION`. A new table
   privilege would appear in `pg_dump` output and `aclitem` strings that
   PostgreSQL tools cannot parse.
3. **Kafka compression codecs:** making `klauspost/compress` and
   `golang/snappy` direct requirements adds no module to `go.sum` but
   does add lines to `go.mod`'s direct block. Acceptable, or ship Kafka
   with `none` and `gzip` only? Recommendation: accept; they already
   build into every binary through Pebble.
4. **Time-bound iteration in phase 1?** A Pebble block-property
   collector recording each block's MVCC timestamp range would let an
   incremental export skip unchanged blocks, which helps the poller,
   incremental backups and the catalog followers alike. It changes what
   new sstables carry (old ones are read in full until compacted).
   Recommendation: no; go straight to rangefeeds, and consider it for
   incremental backup on its own merits.
5. **Filtering row-expiry deletes** (CockroachDB's opt-out): add an
   origin flag to intents and committed versions (an on-disk format
   change behind a gate), or document that TTL deletes are emitted?
   Recommendation: document for now; revisit with phase 5.
6. **Lock-only commits:** should a rangefeed drop the version a
   committed `FOR UPDATE` lock leaves (it can tell), even though the
   poller cannot? Recommendation: yes, and have the poller drop a
   version whose bytes equal the previous version's when `diff` is on;
   document that without `diff` the poller may emit such no-op updates.
7. **Purge versus a changefeed:** fail the feed (recommended, purge is
   an erasure tool), or make purge wait for feeds as it waits for
   backups?
8. **Hidden key columns:** exclude the hidden `_shard` column from the
   emitted key (recommended: a re-shard changes it, and the user never
   named it), or include it as CockroachDB includes its shard column?
9. **Compatibility of formats:** match CockroachDB's JSON encodings,
   webhook bodies, `__crdb__` field names and file names byte for byte
   (verified against its documentation before the golden tests are
   frozen), or define datax's own? Recommendation: match, since
   consumers written for CockroachDB are the users this gap is for.
10. **Sealed tables:** keep refusing them, or allow them with an
    explicit option (`WITH allow_sealed`) for owners who hold the key's
    unwrap privilege, with a `NOTICE` that plaintext leaves the cluster?
    Recommendation: refuse until someone asks; then the option, audited.
11. **Where jobs run:** least-claims placement (proposed), or near the
    data once phase 3 exists? Recommendation: least-claims in phase 2;
    aggregators near the data in phase 5.
12. **Is phase 1 worth shipping alone?** Sinkless feeds are mostly for
    tests and debugging, as in CockroachDB. Recommendation: yes, behind
    `changefeed.enabled`, because it exercises every option before any
    durable state exists.

## References

* CockroachDB documentation, v24.x–v25.x, as known to the author (not
  re-fetched): "Change Data Capture Overview", `CREATE CHANGEFEED`,
  `EXPERIMENTAL CHANGEFEED FOR`, "Changefeed Messages", "Changefeed
  Sinks", "Monitor and Debug Changefeeds", "Protect Changefeed Data".
* PostgreSQL documentation, chapter 48 "Logical Decoding" and `CREATE
  ROLE` (`REPLICATION`), for the privilege model.
* [transactions.md](../transactions.md), [replication-and-placement.md](../replication-and-placement.md),
  [read-committed.md](read-committed.md), [functions-and-triggers.md](functions-and-triggers.md),
  [schemas.md](schemas.md); `docs/compatibility-gaps.md` rows CR-CDC-001,
  CR-CDC-002, PG-W-001 and priority 6.
