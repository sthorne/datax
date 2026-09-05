# Transactions

datax provides serializable, distributed ACID transactions using the
CockroachDB recipe: MVCC + write intents + a per-transaction record whose
single atomic state flip commits the whole transaction. There is no classic
two-phase commit and no blocking coordinator window.

## Timestamps: the hybrid logical clock

Every node runs an HLC (`pkg/util/hlc`): a `(wallTime, logical)` pair that is
monotonic on each node and ratcheted by every message received, so causally
related events always order correctly even with bounded physical clock skew.

- `--max-offset` (default 500ms) bounds tolerated skew. A message from a clock
  more than max-offset ahead crashes the node — loudly wrong beats silently
  inconsistent.
- **Uncertainty**: a reader at `readTS` that sees a value from another node in
  `(readTS, readTS + maxOffset]` cannot know whether that write "really"
  happened before or after it began, so it restarts at a higher timestamp
  (`ReadWithinUncertaintyInterval`).

## MVCC and write intents

Values are never overwritten; each write creates a version at a timestamp.
Version keys sort newest-first after the bare user key (see
[architecture.md](architecture.md)).

A transactional write lays down a **write intent**:

- a small metadata record at the bare key: `{txnID, epoch, timestamp, ...}`
- the provisional value, stored as a normal version at the txn's write ts.

The metadata record and the transaction record are protobuf-encoded from
cluster version v14 (issue #141; JSON before it — `encoding/json` was
about 45 % of the intent path, a 5 µs decode per intent read or rewrite).
The coordinator flags each transaction (`TxnMeta.BinaryMeta`) when the
cluster is at v14, the flag rides in every command so every replica
encodes alike (replicas must stay byte-identical for the consistency
checker), and a reader tells the encodings apart by the first byte —
JSON opens with `{`, a protobuf record with its first field's tag
(`storage.DecodeMVCCMetadata`, `kvpb.UnmarshalTxnRecord`; every reader,
GC included, goes through them) — so intents and records laid down before
the finalize are read, resolved and eventually reclaimed by the v14 code
for as long as they live. Measured: decode 4.5 → 0.6 µs,
encode 0.85 → 0.65 µs, an intent laid down, rewritten and read back
13.9 → 6.2 µs; the record is well under half the size.

Because the metadata key sorts immediately before all versions, a single seek
finds "intent, then newest version". A scan walks from there with `Next`:
the newest version at or below its timestamp, then across the key's
remaining versions onto the next row (a reverse scan steps back with
`Prev`), and seeks only past a chain of more than eight versions (issue
#160 — a seek per row and another per version lookup had been half of a
scan's time; versions are told apart by their encoded suffix, without
decoding the user key per version). The bookkeeping around those seeks
is allocation-light (issue #163): a key's two iterator bounds come out
of one allocation — the upper bound is the metadata key with its
terminator bumped, which is exactly the end of that key's engine keys —
and the version keys the read seeks to are appended onto the lower
bound's spare capacity; encoding and decoding a key each allocate once
at the exact size; a scan copies each row's prefix off the iterator
rather than re-encoding the row's key. A point read went from 16
allocations to 5 (3 through a `storage.Getter`, which re-bounds one
iterator per key for the Gets of a batch — an index join's primary-row
fetches, a statement's per-row reads — instead of building and tearing
down an iterator for each); a 1,000-row scan from 8 allocations per
row to 2 (the key and the value the caller keeps).

Rules:

- A transaction **reads its own intents** (read-your-writes).
- Any *other* reader or writer that encounters an intent cannot just use or
  ignore it — the intent's fate is owned by its transaction record. The
  encounterer **pushes** the record (below).
- Intents are resolved (metadata dropped; version rewritten at the commit
  timestamp, or removed on abort) only according to the record's state.
  **Invariant: no intent is ever resolved except per its record.**

## The transaction record

Created on the range of the transaction's **first write** (the *anchor*), keyed
by txn ID. States: `PENDING → COMMITTED | ABORTED`.

- The coordinator (the gateway node's `TxnCoordinator`) heartbeats the record
  every 1s. A record whose heartbeat is stale by >5s is **expired**: any pusher
  may abort it.
- **Commit** is `EndTxn(commit=true)` at the anchor: a single Raft-replicated
  compare-and-set PENDING→COMMITTED. The moment it applies, every intent the
  transaction wrote anywhere in the cluster is logically committed — readers
  who find those intents will push, discover COMMITTED, and resolve them.
- After commit the coordinator resolves intents asynchronously and finally
  deletes the record. If the coordinator dies first, resolution happens lazily
  via pushes. This is the **crashed-coordinator story**: no intent outlives
  its record's authority, and expiry guarantees progress.

## Serializability: refresh, then retry

All reads happen at the txn's `readTS`, and the commit condition — enforced
in exactly one place, the transaction record flip — is that `writeTS`
still equals `readTS`. Events that force the write timestamp above
`readTS`:

- the range's **timestamp cache** shows a later read overlapping a key we
  want to write (write after read),
- `WriteTooOld`: a committed version exists above our write timestamp,
- an uncertainty restart.

The coordinator tracks every span the transaction has read (up to a cap;
beyond it refresh is disabled). On any of the conflicts above it attempts a
**read refresh**: for each read span it sends a `Refresh` request verifying
no other transaction wrote into the span within `(readTS, newTS]` — any
committed version there, or any foreign intent (which could commit inside
the window), fails the refresh. Refresh runs on the ordinary read path, so
it holds shared latches and **bumps the timestamp cache to the new read
timestamp before evaluating** — after a success, no write can land beneath
the new `readTS` on that span without being pushed above it. On success the
transaction adopts `readTS = writeTS = newTS` and re-issues the conflicting
operation; the commit condition then holds again without a restart.

Only when refresh fails (a read was actually invalidated) does the conflict
surface:

- an implicit single statement (auto-commit) is retried transparently at a
  new timestamp with bumped priority;
- an explicit transaction gets PostgreSQL error **`40001
  serialization_failure`** — the standard signal that well-behaved PG/CRDB
  applications already retry.

Intents laid at the pre-refresh write timestamp are fine: resolution moves
provisional versions to the final commit timestamp, and moving a write's
timestamp up never violates the write-beneath-read rule.

### Span latches

Each range's leader serializes overlapping requests with per-span latches
(v2; v1 used one range-wide lock). The invariants:

- **L1** — any two operations with overlapping key spans, at least one of
  which writes, are fully serialized from timestamp-cache check to apply
  visibility (a write holds its exclusive latches until it has applied).
- **L2** — a read bumps the timestamp cache *before* evaluating, while
  holding its shared latches.

Together these give the write-beneath-read guarantee per key: an
overlapping write either applied before the read evaluated, or its
timestamp-cache check observed the read's bump and pushed it above the
read. Disjoint operations need no ordering — a write cannot invalidate a
read it does not overlap — so they run in parallel. Transaction-record
operations latch under their **anchor key** (the record's addressed key),
and splits take a whole-range exclusive latch.

The manager keeps the held latches in a set and indexes every **point**
span (a single key: every `Get`, `Put` and `ConditionalPut` in a batch) by
its key, alongside the subset of holders that cover a key **range**. A
point span's conflict check is a lookup under its key plus a scan of the
ranged holders only; a ranged span (a scan, a split, a merge) scans every
holder. Overlap checks do not allocate. Before the index a 100-key batch
under 64 similar holders cost about 60 ms and 1.9 million allocations in
the conflict scan; with it, about 20 µs.

### Timestamp cache

Per range, leader-side, and **interval-based** (v2; v1 kept a single
high-water mark that made every read push every writer on the range). Each
read records its key spans and timestamp; a write consults only entries
that **overlap** its own spans, so disjoint readers and writers never
interact. Structure: a range-wide **floor** timestamp plus two bounded
generations of entries, each holding its **point reads in a map by key**
(one entry per key: the newest read of it; two readers at the same
timestamp leave it unattributed) and its ranged reads (scans) in a
slice. A point write looks its key up in both maps and scans only the
ranged entries; a ranged write scans everything (issue #108: the
full scan of every entry against every write span was a quarter of a
leader's CPU under batched ingest, whose uniqueness probes put one point
entry per row in the cache — a 100-key write against two full
generations went from ~1.5 ms to ~2 µs). Bumps go to the current
generation; when it fills (4,096 entries), the older generation folds
into the floor (the max of its timestamps — conservative, never
incorrect) and rotation continues. Memory stays bounded per range,
recent reads keep full span precision, and old reads age into
range-wide coverage. The floor is also set directly by
whole-range events: leadership acquisition (a new leader cannot know what
the old one served) and, span-scoped to the absorbed keys, range merges.
Entries carry the reader's transaction ID: a transaction writing at
exactly the timestamp of its *own* read is allowed (read-then-write is the
normal pattern); anyone else at or below an overlapping entry is not.

A transactional write that fails the check is **pushed, not rejected**:
its intents simply land above the cache, the response carries the
forwarded write timestamp, and the coordinator settles up at commit —
refresh the reads, then the EndTxn (which writes no MVCC versions and is
exempt from the check) flips the record at the pushed timestamp. Rejecting
instead would let a steady reader starve every writer of a hot key: the
coordinator's refresh round trip always loses the race against the next
read's bump. Non-transactional writes, which have no reads to protect, are
still bounced with a retry timestamp and simply resent above it.

Because a multi-range batch executes as per-range sub-batches, any ONE of
which may be pushed by its own range's cache, the router merges the
forwarded timestamps: the batch response reports the MAXIMUM write
timestamp across sub-batches. Overwriting with the last sub-batch's
response would let an earlier push vanish — the commit would then flip
(and resolution would re-timestamp the pushed intent DOWN to) a timestamp
already served to a reader, silently un-happening a read. The
`TestMultiRangePushCommitsAbovePushedWrite` regression pins this.

### Locking reads (SELECT FOR UPDATE)

The symmetric read-modify-write pattern — two transactions read the same
rows, then each invalidates the other's reads on write — is a doomed race
under plain reads: refresh cannot help (the read spans really were
overwritten), so both restart, repeatedly. `SELECT ... FOR UPDATE` breaks
it by serializing upfront: the row fetch is followed by a **locking read**
per selected row, a Get evaluated on the WRITE path that atomically
re-verifies the row at the transaction's read timestamp (any newer
committed version surfaces as a retryable conflict — the fetch-then-lock
gap cannot admit a stale read) and lays a write intent pinning the
observed state — the current value for an existing row, a tombstone for an
absent one. The second transaction's lock then queues behind the intent
via the ordinary push machinery instead of racing to a restart. The
intent commits as a version carrying the same bytes, invisible to
readers' results. On the bank workload (100 hot accounts, 8 workers) this
raises committed throughput ~4× and roughly halves 40001s.

### Savepoints

`SAVEPOINT name` / `RELEASE SAVEPOINT name` / `ROLLBACK TO SAVEPOINT name`
implement partial rollback with PostgreSQL semantics, including escaping
the in-failed-transaction state (`25P02`) — the recipe driver retry loops
and ORMs rely on. Every write carries a **sequence number** (one per
statement), and each intent keeps the transaction's own superseded
provisional values for its key in an **intent history**. A savepoint
captures the current sequence plus the coordinator's write-set and
read-span positions; `ROLLBACK TO` sends a replicated rollback per
written key that **physically restores** each intent to its newest state
at or below the savepoint's sequence (or removes it when the key was
first written after the savepoint). Because the engine state after the
rollback simply *is* the savepoint state, reads need no sequence
awareness and commit resolves the restored values like any others. A
transaction whose coordinator was already aborted (a serialization
failure) cannot be rescued by savepoint rollback — the 40001 stands, as
in CockroachDB.

The history is bounded to what a rollback could restore (issue #162).
Unbounded, a transaction that wrote one key K times stored K copies of
the value and rewrote O(K²) bytes, for data only `ROLLBACK TO` reads.
The coordinator puts `HistoryFloor` in the transaction metadata of every
batch: negative when no savepoint is live (a rewrite keeps nothing —
the common case), or F+1 when the oldest live savepoint is at sequence
F (a rewrite keeps the newest entry at or below F, which that savepoint
restores, and every entry above it, which a later savepoint the server
does not know about might; two entries at one sequence collapse to the
later, the only one a rollback can reach). Zero — a coordinator from
before the field — keeps everything, so mixed versions need no gate: an
old server ignores the field, an old client gets the old behavior.

### Parallel commits

Commit latency was two sequential consensus rounds: lay the final
intents, then flip the record. Pipelined transactions (the auto-retrying
implicit path uses this; explicit BEGIN blocks commit classically) defer
each statement's write batch — any operation needing read-your-writes
flushes it transparently — and Commit then sends the deferred batch and
an EndTxn IN PARALLEL. The EndTxn carries the batch's keys as an
**in-flight write set** and lands the record in a third state, `STAGING`:
the transaction is **implicitly committed** the moment every in-flight
write has applied at or below the staged timestamp. The coordinator
returns to the client after that one round and finalizes (STAGING →
COMMITTED, then intent resolution) asynchronously. If anything forwarded
the writes above the staged timestamp, the commit settles classically —
refresh, then a finalizing EndTxn — and on failure the record is
explicitly aborted so recovery agrees with the reported error.

A pusher that finds a `STAGING` record runs **status recovery** instead
of aborting it (expiry included — an implicitly committed transaction
must never be aborted): it probes each staged in-flight key with a
**prevention read** at the staged timestamp. The ordinary read path bumps
the timestamp cache before evaluating (invariant L2), so a write found
missing can never land at or below the staged timestamp afterwards — the
verdict is stable. All present → a replicated `RecoverTxn` finalizes the
record COMMITTED; any missing → ABORTED. Recovery and the coordinator's
own finalize are idempotent and commute. GC never reclaims `STAGING`
records.

On the write-only kv workload (implicit UPDATEs), the shorter
intent-hold window roughly triples committed throughput.

Two ordering rules keep the parallelism sound. The record-creating
(anchor) sub-batch of a multi-range write completes before any sibling
sub-batch is sent, so no intent is ever observable before the record
exists — a pusher finding such an intent judges expiry from the
transaction's birth (`MinTimestamp`) and could poison the record ABORTED.
For the same reason, a transaction already older than half the expiry
window (2.5s) forfeits the commit-time parallelism and creates its staged
record before sending the write batch.

### One-phase commits

When a pipelined transaction's **entire** write set — provably so: never
anchored, nothing flushed early — lands on **one range** (by warm-cache
routing only), Commit skips the parallel-commit protocol entirely: the
writes and a committing `EndTxn` marked `All` go out as one BatchRequest,
and the server evaluates the whole transaction in a **single raft
proposal**, writing committed values directly — no transaction record, no
intents, nothing to finalize or resolve. A failed or abandoned attempt
discards the entire engine batch, so there is no state to recover, by
construction. This is the common case for a single-statement INSERT or
UPDATE on an unsplit (or single-range) table, and it halves the measured
single-row commit latency again on top of parallel commits.

The fast path never bends serializability: a one-phase batch is **never
pushed in place**. If the timestamp cache would forward it (or its read
and write timestamps already diverged — the uniqueness-probe-then-write
shape), the server rejects it pre-proposal with a retryable error and the
client's ordinary refresh loop validates its read spans and resends. At
apply, the writes use the transactional conflict rules (foreign intent →
push, newer version → write-too-old), never the non-transactional
timestamp bump. Ineligible or unlucky commits — multi-range write sets, a
cold routing cache, a split racing the send — fall back to the parallel
commit unchanged, with nothing applied. Old servers that predate the
`All` flag simply evaluate the batch classically (record + intents +
commit, still one atomic proposal); the response tells the client to
resolve as usual, so mixed-version clusters need no gating.

## Conflicts and pushes

Encountering someone else's intent triggers `PushTxn(pusher, pushee)` at the
pushee's anchor range:

- Record COMMITTED / ABORTED / expired → resolve the intent, proceed.
- Record PENDING and live:
  - pusher has **higher priority** → write ABORTED into the record (the pushee
    discovers this at its next heartbeat or EndTxn),
  - otherwise poll with backoff up to ~2s, then surface `40001` to the client.

Priorities are random at birth and bumped on retries, so starvation is
unlikely. Genuine deadlocks are broken by **distributed detection over
advertised wait edges**: a coordinator blocked in a push loop publishes
"waiting for X" on its own transaction record (immediately on change, and
with every heartbeat), and each blocked pusher periodically walks the
chain with query-only pushes — reads of the records along the way. A walk
that arrives back at the walker has found a cycle; every walker picks the
same victim deterministically (lowest priority, transaction ID as
tie-break) and force-aborts it — a self-chosen victim aborts its own
record at once so its partners unblock on their next poll. Wait edges are
advisory and may be stale, so a phantom cycle costs at worst one spurious
retryable abort, never an anomaly. Constructed 2- and 3-cycles resolve in
a few poll rounds (hundreds of milliseconds). With detection in place the
conflict-wait timeout is a generous backstop (10s, up from v1's 2s), so
waiters queueing behind a slow-but-live lock holder are no longer aborted
by the clock.

Reads below a committed value's timestamp never block (MVCC). A foreign
intent strictly above both the read timestamp and the uncertainty limit is
**read beneath**, not pushed: resolution only moves a write's timestamp
forward, so however the intent resolves, its version stays invisible to
the read and the committed value below is the correct answer. Only intents
at or below the read timestamp — or inside the uncertainty window, where
like a committed version they might causally precede the read — trigger a
push.

## Garbage collection

Old MVCC versions and finalized transaction records are reclaimed by a
leader-driven, **replicated** GC (v2). Each store's housekeeping loop
periodically computes `threshold = now − GCTTL` (default 25h) and, for every
range it leads, enumerates garbage from one consistent engine snapshot:

- per user key, the newest version at or below the threshold is the
  **survivor** — exactly what a read just above the threshold observes;
  every older version is garbage, and the survivor itself is too if it is a
  deletion tombstone;
- keys holding an unresolved intent are skipped entirely (their history is
  in flux);
- finalized (committed/aborted) transaction records whose timestamps are
  TTL-old are garbage, unless the range still holds an intent of that
  transaction.

The leader proposes a `GCRequest` naming the exact keys to delete plus the
new threshold. Replication is what makes this safe and simple: every
replica deletes the same bytes (cross-replica checksum equality is asserted
in tests), the threshold is replicated state that survives crashes,
leadership changes, and preseed snapshots, and the command's whole-range
exclusive latch serializes it against reads (invariant L1). Enumerating
from a snapshot cannot race concurrent writes: committed versions are
immutable, live transactions write far above the threshold, and intent
resolution only touches keys the enumeration skipped.

Correctness rules enforced around the threshold:

- **Reads at or below the threshold are rejected**, non-retryably — the
  versions they would need may be gone. Live transactions never trip this
  (TTL ≫ transaction lifetime, and refresh/uncertainty only move `readTS`
  forward).
- **Resurrection guard**: `createTxnRecord` rejects any transaction whose
  `MinTimestamp` is at or below the threshold, so a zombie coordinator
  cannot recreate a reclaimed record as PENDING after having possibly been
  aborted. Deterministic, because the threshold is replicated.
- **Committed records are proof, and proof is kept until spent**: a commit
  stores the transaction's write set on its record, and GC resolves every
  one of those intents — wherever their ranges live — before reclaiming a
  COMMITTED record. An intent orphaned by a coordinator crash therefore
  always resolves as committed, never as expired-and-aborted. (ABORTED
  records stay collectible outright: a pusher finding a record-less
  TTL-old intent aborts it, which is the correct outcome either way.)

## Invariants (asserted in tests)

1. An intent is never resolved except per its record's state.
2. Commit is one Raft write (the record flip).
3. At commit, `writeTS == readTS` (retry-only serializability).
4. The uncertainty interval is enforced on every read of non-local data.
5. A transaction's reads observe its own latest writes.
6. After GC, replicas of a range remain byte-identical, and no read ever
   observes a state that GC'd versions could have distinguished.

## Known gaps (deliberate)

Write pipelining WITHIN a transaction (async consensus for every
statement, not just the final batch); ranged (non-point) requests in
deferred batches.
