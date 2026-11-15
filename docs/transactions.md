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
finds "intent, then newest version" — and a point read that finds no
intent and a newest version at or below its timestamp is done there.
From cluster version v15 a point read's seeks are prefix seeks (issue
#161, [storage profiles → prefix bloom
filters](storage-profiles.md#prefix-bloom-filters)): the metadata key and
every version of a key share one prefix under the engine's comparer, so
each seek asks the sstables' bloom filters whether they hold anything of
the key at all. A scan walks from there with `Next`:
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
- A **commit that fails** — the refresh it needed failed, its deadline had
  passed, a pusher had aborted it — aborts the record and resolves the
  intents as aborted before the error is returned
  (`Txn.abortFailedCommit`). Expiry is for coordinators that die; a live
  coordinator that has given up says so at once. The abort is the
  record's compare-and-set, so it cannot overrule a commit that did
  apply (an ambiguous error): on a COMMITTED record it fails, and the
  intents are left to the pushers, who read the verdict from the
  record. Before issue #315 a failed commit only stopped the heartbeat,
  and a SQL session's `ROLLBACK` after the failed `COMMIT` found the
  transaction finished and did nothing, so the intents blocked their keys
  until the record expired. On the bank workload (16 workers, 1,000
  accounts) each such failure held its two accounts for up to five
  seconds, and the transactions queued behind them, and behind those,
  filled two thirds of the workers' time.

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

**SKIP LOCKED and NOWAIT** (issue #296) are the same locking read told
not to queue. Both halves of the statement have to change, because both
wait today: the lock, and before it the fetch — a plain read pushes any
intent at or below its timestamp, and a row another transaction locked a
moment ago carries exactly such an intent.

- *The lock* (`Txn.GetBatchForUpdateNoWait`). The locking Gets go out as
  before; a write-intent conflict comes back to the coordinator as
  always, and the coordinator pushes the intents' transactions — but
  never with the power to abort (declining to wait for a lock is no
  reason to take it), so the push only tells an abandoned intent, which
  it resolves and then locks over, from a held one. A held lock fails
  `NOWAIT` with a lock-not-available error (SQLSTATE `55P03`); under
  `SKIP LOCKED` the key is dropped from the batch and the rest are
  locked. Nothing new is evaluated on the write path, so nothing here is
  version-gated. Until the transaction has its record the keys are
  locked one batch each: a single-key batch lays its intent and the
  record together or does nothing, whereas a multi-range batch refused
  on the anchor's range could leave intents elsewhere naming an anchor
  with no record.
- *The fetch.* Reads sent under `kvclient.WithLockWait` carry the
  policy. For `NOWAIT` the coordinator simply refuses to wait on a live
  intent the read conflicts with. For `SKIP LOCKED` the request itself
  is marked (`skip_locked` on Get and Scan, read path only): the server
  reports a key holding a foreign intent as absent — at any timestamp,
  including one an ordinary read would look beneath, since the lock
  would be refused either way — and returns the intents it stepped
  around; a scan's row limit counts only the rows returned, and a page
  ends after 256 skipped keys. A server that predates the field ignores
  it and answers with the conflict, and the read then waits as it always
  did — slower, never wrong — so the field needs no cluster version.

`SKIP LOCKED` extends "locked" to one more case: a key with a version
**committed** above the transaction's read timestamp. A plain locking
read refuses such a key with WriteTooOld, and a plain read fails with an
uncertainty error when the version is inside the uncertainty window;
both normally mean a refresh or a restart. Here the row is passed over
instead (PostgreSQL's read-committed would re-read the new version and
lock it; a serializable snapshot cannot, and to a queue reader the row
is one another worker just took and finished with). Without this a
drained queue's head is a run of fresh tombstones, and every dequeue
scan restarts on the first one it cannot order itself against.

**Skipped rows and the read set.** A transaction's read spans are what a
refresh re-validates, and a refresh is routine — any reader of a locked
row at a later timestamp pushes the lock holder's next write above its
read timestamp. A skipped row is, almost by definition, about to be
changed by someone else, so leaving it in the read set would turn that
routine refresh into a `40001` over a row the statement never returned.
It is therefore taken out (`Txn.ForgetReads`): a scan records its span
less the keys the server skipped, a skipped point read is not recorded,
and a row refused at the lock step is punched out of the spans the
statement's fetch recorded — at once, before the statement reads again.
The gaps between forgotten keys stay covered, so an insert into the
scanned range is still caught, and reads recorded by *earlier*
statements of the transaction are never touched: a row the transaction
really read keeps its protection even if a later `SKIP LOCKED` passes
it over. What is given up is exactly what the clause gives up in
PostgreSQL — the result is not a serializable read of the skipped rows.
Two limits: an index scan's entries for skipped rows stay in the read
set (the fetch forgets primary keys), so a queue ordered by a secondary
index can still fail a refresh when a skipped row's entry is deleted;
and a statement that skips more keys than the read set holds spans for
(512) loses refresh for the transaction, as overflowing it with plain
reads does.

**LIMIT.** Plain `FOR UPDATE` locks what the fetch returned, and the
fetch stops at the limit only when the access path delivers the order —
with an in-memory sort it locks every matching row and then cuts.
`SKIP LOCKED` sorts first when it must, then locks candidates in order,
only as many as are still wanted, and stops when the limit is met: a row
locked and then cut would be hidden from every other skipping reader for
nothing. If a limit-bounded fetch comes back full and the lock step
still leaves the limit unmet (rows taken between fetch and lock), the
fetch is repeated at four times the size, unbounded after three rounds;
rows fetched beyond the limit in an ordered scan are forgotten like
skipped ones, since a fetch that could stop exactly at the limit would
not have read them.

**Dead lock holders.** Nothing conflicts with an intent under
`SKIP LOCKED`, so the push-on-conflict that normally cleans up after a
crashed coordinator never runs, and a row it had locked would stay
invisible to a table read only by skipping readers. The coordinator
therefore pushes the transactions behind the intents its reads skipped
— only those older than the expiry window (younger ones cannot have
expired, and a live coordinator resolves its own), and each at most once
a second per gateway — and resolves what turns out finished or expired.

Measured on the queue test (8 workers on 3 nodes draining 200 rows with
`ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED` then `DELETE`, one job per
transaction): every row taken once and no `40001` in any of fifteen
runs. The rules above are what that rests on: with skipped rows left in
the read set the same run produced 280 `40001`s for its 200 rows, and
with the read-set rule alone, about five (dequeue scans giving up after
ten refreshes in a row over fresh tombstones).

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
  - otherwise re-push every 100 ms, for at most 10 s (`conflictBudget`,
    `pkg/kvclient/txn.go`), then surface `40001` to the client. A wait
    that can only end in a restart does not wait at all (see below).

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

A wait that cannot end in a commit is not waited (issue #315,
`Txn.cannotOutwait`). A transaction blocked on a live intent **above
its read timestamp on a key it has read** — a write to an account it
read, or a second read of it that finds the intent inside the
uncertainty window — can commit only if the holder aborts: once the
holder commits, its version sits inside one of the waiter's read spans
above its read timestamp, the waiter's write must go above it
(WriteTooOld) or its read finds it uncertain, and the refresh that would
move the waiter's timestamp past it re-reads that span and fails. (A
read can also pass beneath the version if the holder is pushed past the
reader's uncertainty limit before committing — rare, given the
500 ms window.) So after a push that leaves such an intent live — a
pusher with the priority to abort the holder has already done so — the
operation returns a retryable error at once instead of polling until
the holder's commit makes it fail anyway. The same holds for any intent
above the read timestamp once the transaction has read too much for its
read set to be tracked, since every timestamp move then restarts it. A
restart is always safe; what the rule gives up is the rare wait whose
holder would have aborted. Serializability is untouched, since the rule
only ever makes a transaction restart, and so are priorities: a
stronger writer still aborts the holder instead of restarting. On the bank workload these waits were most of the attempts
that ended in a `40001`, each a 100 ms poll or more; they now end in
the push's round trip.

Measured together with the failed-commit fix above (one secure,
encrypted on-disk node, 16 workers, 1,000 accounts, seed 7, four
alternating 30 s rounds each): committed transfers went from a median
of 238 a second (209–252) to 974 (832–1,271). An attempt that ended in a
`40001` ran 3.1 s on average before and 0.02 s after, and those attempts
went from about 70 % of the workers' time to under 3 %. There are a few
more of them (0.018 per committed transfer against 0.016), since a
waiter whose holder would have aborted now restarts too. kv 50 % reads
and hot-row did the same KV work per operation as before and stayed
within their run-to-run noise.

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

- **Reads at or below the threshold are rejected** — the versions they
  would need may be gone — with a typed detail
  (`kvpb.BelowGCThresholdError`; a peer that predates it is recognized
  by the message). What follows depends on who chose the timestamp
  (issue #313). A transaction the cluster timestamped restarts at a new
  one (`kvclient.RetryableError`; SQLSTATE `40001`), which succeeds: it
  tripped because it outlived a short `gc_ttl` or began before a purge.
  A historical transaction's timestamp is the client's, so the refusal
  stands (SQLSTATE `72000`, PostgreSQL's `snapshot_too_old`): no retry
  at that timestamp can succeed. With the default 25h window live
  transactions never trip it (TTL ≫ transaction lifetime, and
  refresh/uncertainty only move `readTS` forward).
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

### A window per table

The window is the node's `GCTTL` unless the table says otherwise
(`WITH (gc_ttl = ...)`, `TableDescriptor.GCTTLSeconds`, cluster version
v21; issue #295). The leader resolves each range through the provider
that already maps ranges to time-series retentions
(`pkg/server/retention.go`):

- a range wholly inside a table with a window collects at that window,
  shorter or longer than the node's, by the ordinary rules above (the
  survivor stays — this is not the expiry mode of a retention);
- a range the table shares with other data collects at
  `max(node window, every overlapping window)`. One table's short window
  therefore cannot reach another table's history; the price is that it
  does not bite on a shared range either, so the DDL splits the table's
  edges, the provider asks for them again whenever a GC pass finds a
  range straddling one (a restored table was never split), and the
  merge queue — which already refuses to merge ranges whose windows
  differ — keeps them apart.

Nothing about a short window is unsafe, because nothing about the
threshold assumes it is far in the past: every batch at or below it is
refused (writes as well as reads, so a write cannot land beneath a
collected tombstone), intents are never collected around, and a
transaction record born at or below it cannot be recreated. What a short
window does is make those refusals reachable by ordinary work — a long
transaction, an old `AS OF SYSTEM TIME`, an incremental backup from an
old base — and they surface as errors, never as short results.

### Purge

A purge (`ALTER TABLE ... PURGE`, `datax debug purge`;
`server.Node.PurgeSpan`, `pkg/kvserver/purge.go`) is collection at a
window of zero, followed by making the collected bytes leave the files.
For each range of the span, after splitting at the span's ends:

1. **Leader** (`Store.PurgeRange`). Resolve the intents of finished
   transactions and wait a bounded time for the rest; collect at
   `threshold = now` with the same `GCRequest` as any other pass; wait
   for the followers to hold it; propose a `TruncateLog` at the applied
   index, with none of housekeeping's floor. The log matters because a
   value is a log entry before it is a version, and stays one until
   truncation.
2. **Each replica** (`Store.PurgeReplica`), once it has applied the
   leader's index: flush and compact the range's MVCC span on the state
   engine — the tombstones GC wrote and the versions under them meet in
   that compaction and neither is written out — perform the deferred
   log truncation the flush has made safe, compact the range's log span
   on the raft engine, and count the span from the raw engine keys
   (`storage.MVCCSpanCensus`): live keys, versions beneath a newer one,
   tombstones, intents.
3. **Each node** (`Store.ScrubLogs`). Pebble keeps obsolete write-ahead
   log files to recycle, and a recycled file keeps its old bytes beyond
   wherever the new writes have reached. The engine's file system
   (`storage.logGate`) marks the log files present at the call and has
   each one deleted or replaced by a fresh file rather than reused,
   rolling the log until none is left.

The purge is complete when every replica reports no superseded version,
no tombstone and no intent. `TestPurgeRemovesDeletedRowsFromDisk` holds
that to a byte search of three nodes' data directories for a marker
value, and fails if either the truncation or the log scrub is removed.

What keeps it correct is the threshold, as above: the collection is
replicated, so from the moment it applies on a replica that replica
refuses every batch at or below the purge time. A transaction older
than the purge fails on its next request to the range. The purge does
not wait for such readers; it refuses them.

A replica that is **down** misses the command and applies it on return,
from the log or by snapshot. Between returning and catching up it would
still hold a persisted closed timestamp entitling it to serve follower
reads below the purge from its old copy, so a restarted replica serves
no stale read until its closed timestamp has passed the time its process
started (`Replica.heardSinceStart`): a promise made after the start
implies the log up to it, the collection included. A replica that is
partitioned but running has no such signal and is the remaining gap; the
purge names every replica that did not confirm, and is not complete
until none is left.

Not reached, and listed in every report: backups; a raft snapshot in
flight; the log of a range the rows were written to before a split moved
them (truncated on that range's own schedule); and the device beneath
the file system. On an encrypted store the same files are rewritten and
deleted, whichever data key they were written under.

### A dropped table's span

`DROP TABLE` leaves the rows and a record of the drop
(`catalog.DroppedTable`); once the table's history window has passed,
the reclaim job (`pkg/server/dropped_tables.go`) deletes the rows and
purges the span, as above (issue #313; docs/sql.md, "A dropped table's
rows"). The purge's threshold of now is what refuses a late `AS OF
SYSTEM TIME` read of the table; the merge queue keeps the emptied range
apart from any neighbour whose own window has not yet passed that
threshold (`Store.mergeWouldHideHistory`), because a merged range
adopts the higher of the two thresholds and would otherwise refuse
reads the neighbour still promises. That rule covers every purged
range, not only a dropped table's: before it, a purged range merged
into its neighbour — the system range among them — as soon as it was
small enough.

**Not done: a replicated range clear** (issue #316). The reclaim deletes
a table row by row — a tombstone per row, through the log, in chunked
transactions — and then purges the span. One command per range that
clears the span outright would cost a log entry instead of one per row,
and is what the reclaim would use. What it needs, and why it was left:

- *A request and a gate.* A new `ClearRangeRequest` (an add-only proto
  field), sent only once the cluster has finalized the version that
  introduces it, since a replica that does not know it would refuse or
  misapply the entry; below it the reclaim deletes row by row, as now.
- *Evaluation on the leader*, under a write latch on the whole span:
  refuse an intent in the span (resolve finished transactions first, as
  the purge does), refuse a span that is not a whole table's, and raise
  the range's GC threshold to the command's timestamp in the same
  command, as the purge's collection does, so a read at or below it is
  refused rather than answered from nothing. The reclaim already waits
  out the table's window, so no admissible read is lost.
- *MVCC statistics.* Stats move by deltas each command carries. A clear
  must subtract exactly what the replicas hold, so the leader counts the
  span at the applied index (every replica holds the same bytes there)
  and the command carries the negated count — a read of the whole span,
  but no write per row.
- *Apply* is a range deletion (`storage.Batch.DeleteRange`, already used
  for snapshots and replica removal) over the span's MVCC keys, plus the
  stats delta and the threshold, identical on every replica; the
  consistency checker's checksum then still agrees, and must be shown to
  in a test that clears while it runs. There are no rangefeeds or
  changefeeds to tell.
- *What still has to happen after it:* a range tombstone hides the keys
  but leaves their bytes in the files until compaction, and the values
  in the log until truncation, so the purge (above) still runs.

It touches raft apply, the stats, the GC threshold and the consistency
checker at once, for a job that runs in the background after a window
of a day; the row-by-row path is slower but uses only commands every
replica already applies, and was kept.

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
