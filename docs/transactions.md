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

The coordinator tracks every span the transaction has read. Past 4,096
spans (512 before; a 1,000-row INSERT's uniqueness probes are 1,000
point reads, and condensed, random keys become spans over most of the
table that every concurrent writer of it fails) it **condenses** them (issue #322): the spans recorded so far are
merged with the earlier condensed ones into at most 64 spans covering
every key read — closing first the gaps between keys that share the
longest prefix, so a merged span stays within the keyspace its reads
were in (one table's rows, the descriptors) instead of reaching across
one others write all the time (the descriptor leases). A condensed span
can only make a refresh fail that would have passed, never pass one
that should fail. Before, the transaction gave its read set up there,
and every later push of its timestamp was a restart: `DROP DATABASE` of
a database of a few hundred tables, which reads each table's name and
descriptor, failed every attempt whose commit came after the
closed-timestamp lag (3 s) — the commit is pushed above the closed
timestamp — and an implicit statement gave up after 21 attempts with
`40001` ("write timestamp diverged from read timestamp"). On any of the
conflicts above the coordinator attempts a
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
operations latch under their **anchor key** (the record's addressed key).
A split takes an exclusive latch over its right-hand side only (from the
split key to the end of the range): left-hand requests apply on the same
range whether they land before or after the split, so they keep running
while it lands. Merges and range clears take a whole-range exclusive
latch. A range-wide latch (split, merge, range clear, closed-timestamp
promise) is **fair**: once it waits, every later request that conflicts
with it waits behind it, so a stream of writes cannot starve it.

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
incorrect) and rotation continues — unless the older generation took
reads within the last second: its timestamps are then as new as the
transactions in flight, and folding them would push every one of them.
The current generation grows instead, its point reads up to 65,536 and
its ranged reads (which every write scans) up to 4,096, and folds once
the second has passed. Batched ingest, 16 workers of 1,000-row INSERTs,
filled a generation every few statements, so nearly every statement was
pushed and had to refresh its probes before it could commit. Memory stays bounded per range,
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
An index scan joined to the primary key forgets the entry of each row
the join passed over as well (issue #322): the row's holder typically
moves or deletes that entry (a queue's `state` changes), and left in the
read set it failed the skipper's refresh just the same. Two limits: a
row the fetch returned but the lock step then refused leaves only its
primary key, not its index entry (it was free when the fetch read it,
so this takes a race), and neither does an inverted scan's; and a
statement that skips more keys than the read set holds spans for (512)
loses refresh for the transaction: the spans a skipping statement splits
are not condensed, unlike plain reads past the limit, which are.

**LIMIT.** Plain `FOR UPDATE` locks what the fetch returned, and the
fetch stops at the limit only when the access path delivers the order —
with an in-memory sort it locks every matching row and then cuts.
`SKIP LOCKED` sorts first when it must, then locks candidates in order,
only as many as are still wanted, and stops when the limit is met: a row
locked and then cut would be hidden from every other skipping reader for
nothing. The fetch's own limit counts only rows it returns: an index
join passes over the rows behind entries it scanned, so under `SKIP
LOCKED` its scan continues past its last entry, in pages growing
fourfold up to 256, until the limit is met or the span ends (issue
#322: the skipped row used to consume the limit, and a short fetch read
as the end of the table). If a limit-bounded fetch comes back full and
the lock step still leaves the limit unmet (rows taken between fetch and lock), the
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

**Reading past the limit.** With a condition the scan cannot use — a
queue's `WHERE runnable ORDER BY id LIMIT 1` — the limit is not pushed
into the KV scan, which reads the queue's whole span; until issue #320
all of it stayed in the read set, and four workers looping
`SELECT ... SKIP LOCKED` / `UPDATE` / `COMMIT` over 200 rows lost a third
of their attempts to refresh failures at commit, over rows far beyond the
one each had taken. A limited scan now takes the part of its span after
the last row it kept out of the read set (`Txn.ForgetSpan`; an index
scan, the rest of its index span and the primary rows it fetched behind
the last entry it used): rows after the limit-th match cannot change the
result, any more than they could have had the scan stopped there. This
holds for any limited scan in scan order, so it applies outside SKIP
LOCKED too; a scan whose rows are sorted afterwards is not limited and
keeps its whole span. The same queue now loses none
(`TestSkipLockedContendedReturnsFreeRow`). The report's empty results
with rows free were the locks of transactions whose commit had failed,
held until their records expired before #315; a SKIP LOCKED read rightly
passes over rows locked by a transaction that still looks alive. What is
left is the moment between a transaction's record turning final and its
coordinator resolving its intents, in which its rows are still passed
over.

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

A multi-range write that fails may still have applied in part: the
anchor sub-batch, and so the record, before a sibling failed, or other
ranges in parallel while one failed — whatever the error (a duplicate
key, a write shed under overload, a deadline, a routing failure). Unless
the gateway knows nothing applied, the coordinator tracks every key of
the batch as written, so `ROLLBACK TO SAVEPOINT` and the transaction's
end remove whatever landed, and a batch that was to create the record
leaves a record that may exist: `COMMIT` and `ROLLBACK` then send their
`EndTxn` (which writes the record if there is none) instead of treating
the transaction as read-only.

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
  - pusher has **higher priority** → write ABORTED into the record and
    resolve the intent (the pushee discovers this at its next heartbeat,
    at its EndTxn, or — from v22 — at its next request on a range whose
    abort span the resolution marked; see [the abort span](#the-abort-span)),
  - otherwise wait for the pushee to finish and push again, for at most
    10 s (`conflictBudget`, `pkg/kvclient/txn.go`), then surface `40001`
    to the client. A wait that can only end in a restart does not wait
    at all (see below).

How a waiter learns that the pushee finished (`pkg/kvclient/conflictwait.go`):

- **Woken, when the pushee's coordinator is on the same node** — every
  conflict on a one-node cluster, and any between two sessions on the
  same gateway. Each node's client keeps a table of the transactions its
  blocked operations are waiting on, registered before the push so a
  finish landing between the push and the wait is not missed. A
  coordinator that commits or aborts wakes the waiters on its
  transaction once the record is final and its intents resolved (a
  parallel commit, once its asynchronous finalization has run), and the
  waiter's next attempt usually goes straight through. A pusher that
  finds a record final, or aborts it (by priority or as a deadlock
  victim), wakes the others on that node waiting on the same
  transaction.
- **Polled, otherwise** — the pushee runs on another gateway, or a wake
  was missed. The waiter pushes again after a backoff: 5 ms, doubling to
  100 ms, each wait drawn at random from the upper half of its interval
  so waiters queued behind one holder do not push in step. Most holders
  finish within milliseconds, which the first short waits catch; a long
  wait costs a push every 75 ms or so, about the fixed 100 ms poll this
  replaces. A wake ends the backoff early.

Nothing about this is replicated or crosses the wire: the wake is a
channel in the waiting node's own process, and the pushes are the same
`PushTxn` requests as before, so mixed-version clusters behave as either
version does. A wake is only a hint to push again now, never a verdict;
the push still reads the record.

Measured on the bank workload (one secure, encrypted on-disk node, 16
workers, 1,000 accounts, seed 7, six alternating 30 s rounds each on a
shared, busy machine): p99 went from 111 ms (103–117) — the poll — to
45 ms (24–54; 24 ms in the two quiet rounds, where p50 and p95 matched
the old poll's to within 0.2 ms). Pushes per committed transfer went
from 0.036 to 0.040 and `40001`s per transfer stayed at 0.018. Committed
transfers a second were within the machine's noise (median 1,148 vs
1,076, ranges 870–1,377 and 821–1,476). hot-row, whose transactions hold
a row for ~30 ms, pushes more (0.007 to 0.018 per transaction: the
backoff's first short waits mostly end before the holder does), still a
push per fifty transactions; kv 50 % reads does the same work per
operation as before.

Priorities are random at birth and bumped on retries, so starvation is
unlikely. Genuine deadlocks are broken by **distributed detection over
advertised wait edges**: a coordinator blocked in a push loop publishes
"waiting for X" on its own transaction record (immediately on change, and
with every heartbeat), and each blocked pusher periodically walks the
chain with query-only pushes — reads of the records along the way. A walk
that arrives back at the walker has found a cycle; every walker picks the
same victim deterministically (lowest priority, transaction ID as
tie-break) and force-aborts it — a self-chosen victim aborts its own
record at once and wakes its partners on its node, so they unblock
without waiting out a poll. Wait edges are
advisory and may be stale, so a phantom cycle costs at worst one spurious
retryable abort, never an anomaly. Every round of a wait — each wake or
backoff — runs the walk again; constructed 2- and 3-cycles resolve in a
few rounds, and since the rounds now start at a few milliseconds rather
than 100, in 10–20 ms on one node (they took hundreds). With detection in place the
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

### The abort span

A pusher that aborts a transaction resolves the intents it ran into, and
an aborted intent is resolved by removing it. Until issue #320 the
aborted transaction went on until its next heartbeat or its commit
found the record ABORTED, and in between it no longer saw what it had
written: four transactions writing one new key and reading it back
(`INSERT`, then `SELECT` of the key) had an `INSERT 0 1` followed by an
empty `SELECT` in about one transaction in a hundred (on main before the
fix: 51 in 6,000; 36 in 200 in the report, before #315). Nothing such a
transaction did could commit, but a client acting on the empty read and
rolling back never saw an error. The defence is CockroachDB's: the
range remembers the abort.

*What.* An ABORTED resolution sent for someone other than the
transaction's coordinator — a transactional pusher, a non-transactional
reader, a purge, a SKIP LOCKED read cleaning up after a dead holder —
carries `Poison` (`kvpb.ResolveIntentRequest`). Where it removes an
intent of the transaction, the same replicated command writes an entry
for the transaction in the range's abort span: `keys.AbortSpanKey(range,
txn)`, under the range's local prefix, value the resolving batch's
timestamp (`pkg/kvserver/abortspan.go`). A resolution that removes
nothing (the intent was gone already) writes nothing: nothing of the
transaction was taken away there.

*The check.* Every batch of a transaction that reads or writes the
range's data (Get, Scan, Put, Delete, Increment, Refresh) looks up its
entry — one point read, whatever the batch reads — and fails with a
`TxnAbortedError` (SQLSTATE `40001`) if it is there. Record operations,
resolutions and savepoint rollbacks are not checked: the coordinator
needs them to learn of the abort and clean up. The check and a removal
cannot pass each other. A write batch is checked at apply, in log order,
against the same replicated state the removal changed, so every replica
decides alike and no removal lands between the check and the batch's own
reads. A read is checked *after* it has evaluated, against the engine as
it is then: the removal and the entry are one engine batch, so a read
that missed the intent ran after that batch had committed, and so does
the check that follows it (checked before, a removal could land in
between). An entry also outranks whatever else the read ran into.

*Who is checked.* Only a transaction that has written can have had a
write removed, so only its batches look the entry up: those whose
transaction carries a non-zero `Sequence` or an anchor `Key`
(`mayHaveWritten`). The coordinator bumps `Sequence` before every
request that can lay an intent (a point write, an increment, a locking
read, a write batch, a parallel commit) and copies the transaction into
the batch after, and never lowers it (a restart and a savepoint rollback
keep it), so a batch with `Sequence` 0 and no anchor comes from a
transaction that has laid no intent anywhere. A read-only transaction's
reads, nearly all of a read-mostly workload, skip the point read, which
cost 4 to 7 µs each. The test reads only the batch, never replica state,
so every replica decides alike and no split, merge, snapshot or restart
can make it wrong; `TestTxnSequenceMarksEveryWrite` holds the
coordinator to its half.

*The coordinator.* Once it knows — a request refused, a heartbeat that
finds the record ABORTED, or its own choice as a deadlock victim — the
coordinator fails every further request itself (also on ranges where
nothing was removed, which have no entry) and resolves its remaining
intents as aborted at once, without `Poison`. That resolution also
removes the range's entry. Before, a heartbeat's verdict was left for
"the next operation" to discover, which happened only at the commit, and
the SQL session's `ROLLBACK` after a failed statement found the
transaction finished and resolved nothing.

*Replicated state.* The entries are state of the range like its GC
threshold, so they follow the range wherever its state goes:

- *Snapshots* stream the range's abort span after its data; the receiver
  clears its own entries for the range (a catch-up install, or a fresh
  replica where an earlier one of the range lived) before writing the
  stream's.
- *Splits* copy every entry to the right-hand range, keeping the left's:
  an entry records that some write of the transaction in the parent's
  span was removed, not which, and a copy on the half that held none only
  refuses a transaction that is aborted anyway.
- *Merges* copy the right-hand range's entries into the merged range
  before its local keys go (it is frozen at the subsume, so its entries
  are the same on every replica), keeping the later timestamp where both
  sides have one.
- *Replica removal* wipes them with the rest of the range's local keys.
- *The consistency checker* hashes the abort span after the range's
  other two spans; a range with no entries hashes exactly as it did
  before, so replicas on binaries from either side of the change agree
  until v22, when entries can first be written.

*GC.* An entry is needed only until the coordinator learns of the abort,
which its heartbeat tells it within a second, and the coordinator's own
cleanup usually removes it within milliseconds. What is left — the
coordinator died, or a split copied an entry to a half the cleanup did
not visit — the leader's GC pass removes once it is older than
`AbortSpanTTL` (an hour), with a GC command of its own carrying the
cutoff (`kvpb.GCRequest.AbortSpanCutoff`) that moves no threshold; each
replica picks the entries from its own state at the command's position.
The age is deliberately not tied to the transaction record: an aborted
record is kept a whole GC TTL (25h by default), and on a contended range
that would keep an entry for every transaction aborted there in the last
day; and an entry is only ever written for a transaction whose record
says ABORTED, which a record never stops saying except by being
reclaimed, so a record lookup could not keep alive an entry the age
would not.

*Versions.* A v21 replica would drop both new fields and its state would
part from its peers', so `Poison` and the cutoff are sent only from
cluster version v22. Below it no range refuses an aborted transaction;
the coordinator's own refusal after its heartbeat's verdict, which
nothing else needs to understand, applies at every version.

With it, the program above has no transaction see its write reported
and then miss it, and every one that fails a statement fails with
`40001` (or `23505` for a plain `INSERT` that lost)
(`TestTxnAbortedReadsOwnWrites`); committed transactions per round are
the same as before.

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
transaction, an old `AS OF SYSTEM TIME` — and they surface as errors,
never as short results. An incremental backup from an old base is the
exception: it holds such a table whole instead (below).

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

### Backups in progress

A backup reads every table at one timestamp for as long as it runs, so
a threshold raised past that timestamp mid-backup — by a purge, or by a
short `gc_ttl` — would fail it (issue #376). A backup therefore writes a
record (`keys.BackupProtectionKey`, `pkg/server/backup_protect.go`)
before it reads anything: a timestamp at or below the one it reads at,
and a lease it renews every quarter of `BackupProtectionTTL` (2m) and
removes when it ends. A lapsed record protects nothing, so a backup
whose node died holds collection back for one lease at most.

- **GC** (`StoreConfig.GCLimit`) reads the records once per pass and
  sets no threshold at or above the oldest live one; if it cannot read
  them it skips the pass.
- **A purge** (`PurgeSpan`, so whole-table, key-range and dropped-table
  purges alike) waits up to `PurgeBackupWait` (10s) for the backups in
  progress to end, then collects at `min(read timestamp of the records,
  just below the oldest live record)`, passed to the leader as
  `purge-range-below` (a node from before it refuses that op name rather
  than collecting at its own now). A purge held back this way is
  reported incomplete, with the backups (`PurgeReport.HeldBy`), and the
  purging node runs it again once they have ended.
- **The race** between a purge reading the records and a backup writing
  one is closed by the clock bound, like a transaction's uncertainty: a
  purge never collects above the timestamp it read the records at, and
  a backup takes its timestamp only after its record is written and
  twice the maximum clock offset has passed, so a record the purge did
  not see belongs to a backup reading above anything it collected.
- **Only the backup's own timestamp** is protected, never an
  incremental's base: holding purges to the base would keep deleted data
  for a whole backup interval. An incremental export refused for its
  base (`incremental base too old`) is taken again from zero instead,
  and the table is marked `full` in the manifest (magic `DXBK4`, which
  older binaries refuse); restore reads such a table from the last
  backup of the chain that holds it whole and from none before it.
- The dropped-table reclaim does not clear a table's span while a
  backup that began before the drop is running.

Not reached, and listed in every report: backups; a raft snapshot in
flight; the log of a range the rows were written to before a split moved
them (truncated on that range's own schedule); and the device beneath
the file system. On an encrypted store the same files are rewritten and
deleted, whichever data key they were written under.

### A dropped table's span

`DROP TABLE` leaves the rows and a record of the drop
(`catalog.DroppedTable`); once the table's history window has passed,
the reclaim job (`pkg/server/dropped_tables.go`) clears the span and
purges it, as above (issues #313, #316; docs/sql.md, "A dropped table's
rows"). The clear's and the purge's thresholds are what refuse a late
`AS OF SYSTEM TIME` read of the table; the merge queue keeps the emptied
range apart from any neighbour whose own window has not yet passed that
threshold (`Store.mergeWouldHideHistory`), because a merged range
adopts the higher of the two thresholds and would otherwise refuse
reads the neighbour still promises. That rule covers every purged
range, not only a dropped table's: before it, a purged range merged
into its neighbour — the system range among them — as soon as it was
small enough.

**The range clear** (cluster version v22; `kvpb.ClearRangeRequest`,
`pkg/kvserver/clearrange.go`, `server.Node.ClearSpan`). Before it the
reclaim deleted a table row by row — a tombstone per row, through the
log, in chunked transactions — and the purge then collected every
version and tombstone, a thousand to a command. Now each range of the
span is emptied by one command. The driver splits at the span's ends,
as the purge does, and asks each range's leader (the `clear-range`
admin op) to:

- *Check what it is clearing.* The span must be one table's whole data
  span, and the table must have a dropped-table record and no
  descriptor of any kind under its id. The job takes the span from the
  record and has already checked the table descriptor; the leader reads
  the catalog again, through the routed sender, under the latch below:
  a restore that brings the id back removes the record before it writes
  a row, and its rows wait on the latch, so either the record is gone
  and the clear is refused, or the rows land after it, above its time.
- *Evaluate under an exclusive latch on the whole range*, which drains
  every write in flight (writes hold their latches until they apply,
  L1) and keeps every new request out until the command has applied —
  and only once an entry of the leader's own term has applied, so no
  entry written under an earlier leader's latches is still on its way
  to the state machine. An intent anywhere in the span refuses the clear
  (the intents of finished transactions are resolved first, as the purge
  does; the job runs again on its next pass if one is left).
- *Pick the clear's time T*: at least the leader's clock, the newest
  version in the span, every read the timestamp cache holds for the
  range — which includes every closed-timestamp promise, on the log or
  off it, since each bumps the cache under a shared latch this one
  excludes — and the closed timestamp, and above the current threshold.

Applying it is a range deletion of the span's MVCC keys
(`storage.Batch.DeleteRange`, the tool snapshots and replica removal
use) in the batch that also raises the GC threshold to T, sets the
range's size to zero, and advances the applied index. So the command is
a deletion of every key at T collected at once at a threshold of T:
every read at or below T is refused from then on, writes too; every
read above T sees nothing, and no read above T was served before it
(the timestamp cache) nor any version above T deleted. A follower
refuses a stale read at or below T once it has applied the command;
before that its closed timestamp is below T, because a promise at or
above T is made only after the latch is released, at a later index, so
it sends the read to the leader.

*Statistics.* The design note this replaced had the leader count the
span and the command carry the negated count. Because a clear acts on a
whole range, its span is empty afterwards and the range's size is
exactly zero; the command sets it so, which needs no count the replicas
would have to agree with and corrects whatever the approximate deltas
had drifted. What must hold is that the command covers the whole range
on every replica that applies it: a replica applies it only while its
range still has the bounds and generation the leader evaluated at and
is not frozen for a merge, and only while its threshold is below T —
the command raises it to T, so a replayed or re-proposed entry finds it
there and applies nothing. That is replicated state read at the
command's log position, so every replica decides alike. The consistency
checker now compares each replica's size and threshold at the probe's
index as well as the data's checksum (a node that predates that answers
without them and only the checksum is compared), and stays clean while
clears run (`TestClearRangeReplicasAgree`). A snapshot taken after the
clear streams no key of the span and carries the threshold and the zero
size; one taken before carries the keys and an earlier index, and the
command applies on top of it (`TestSnapshotAfterClearRange`,
`TestClearRangeReplicaCaughtUpBySnapshot`). Transaction records are
left to GC: a committed one is proof for intents elsewhere. There are no
rangefeeds or changefeeds to tell.

*The purge still runs after it.* A range tombstone hides the keys but
leaves their bytes in the sstables until a compaction, and the values
are in the raft log until it is truncated, and in the write-ahead log
files until those roll. The purge's collection then finds nothing, and
its compaction, truncation and log scrub are the parts that remove the
bytes; a cheaper compaction alone would leave the log.

*Below v22* a replica could run a binary that does not know the
command, so the reclaim deletes row by row as before
(`TestDropTableReclaimBelowV22DeletesRows`).

On three nodes, a table of 100,000 rows: row by row the whole reclaim,
purge included, wrote 939 raft entries with 43.9 MB of payload summed
over the three stores and took 3.0 s; with the range clear, 24 entries,
4.1 kB and 0.30 s (`TestDropTableReclaimMeasure`, opt-in).

## Invariants (asserted in tests)

1. An intent is never resolved except per its record's state.
2. Commit is one Raft write (the record flip).
3. At commit, `writeTS == readTS` (retry-only serializability).
4. The uncertainty interval is enforced on every read of non-local data.
5. A transaction's reads observe its own latest writes, or fail (an
   abort that removed them is recorded in the range's abort span).
6. After GC, replicas of a range remain byte-identical, and no read ever
   observes a state that GC'd versions could have distinguished.

## Known gaps (deliberate)

Write pipelining WITHIN a transaction (async consensus for every
statement, not just the final batch); ranged (non-point) requests in
deferred batches.
