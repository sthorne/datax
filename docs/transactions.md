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

Because the metadata key sorts immediately before all versions, a single seek
finds "intent, then newest version".

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

### Timestamp cache

Per range, leader-side. v1 keeps a single **high-water mark**: the maximum
read timestamp served. It is bumped on every read, and to `now()` when a
replica acquires leadership (a new leader cannot know what the old one
served). A transactional write at or below the mark is **pushed, not
rejected**: its intents simply land above the cache, the response carries
the forwarded write timestamp, and the coordinator settles up at commit —
refresh the reads, then the EndTxn (which writes no MVCC versions and is
exempt from the check) flips the record at the pushed timestamp. Rejecting
instead would let a steady reader starve every writer on the range: the
coordinator's refresh round trip always loses the race against the next
read's bump. Non-transactional writes, which have no reads to protect, are
still bounced with a retry timestamp and simply resent above it. Coarse —
any read pushes all writers on the range — but small and correct. An
interval cache is future work.

## Conflicts and pushes

Encountering someone else's intent triggers `PushTxn(pusher, pushee)` at the
pushee's anchor range:

- Record COMMITTED / ABORTED / expired → resolve the intent, proceed.
- Record PENDING and live:
  - pusher has **higher priority** → write ABORTED into the record (the pushee
    discovers this at its next heartbeat or EndTxn),
  - otherwise poll with backoff up to ~2s, then surface `40001` to the client.

Priorities are random at birth and bumped on retries, so starvation is
unlikely; the timeout crudely breaks deadlocks. A real deadlock detector is
out of scope for v1.

Reads below a committed value's timestamp never block (MVCC). v1 pushes on
*any* foreign intent found on a read path, even one above the read timestamp —
reading around newer intents is easy future work.

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

Parallel commits; savepoints; interval-based timestamp cache; deadlock
detection; reading below foreign intents' timestamps.
