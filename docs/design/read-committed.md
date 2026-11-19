# Read committed and repeatable read (gaps PG-X-001, CR-TX-001)

Status: proposed. Nothing here is implemented. The open questions for
the owner are at the end.

Every datax transaction is serializable. PostgreSQL's default is
`READ COMMITTED`, and code written for it usually has no retry loop.
Such code works on datax only after a `40001` retry loop is added
around every transaction. CockroachDB added `READ COMMITTED` in
v23.2–v24.1 for the same reason (CR-TX-001). This document:

* describes the PostgreSQL semantics to match;
* describes what datax's transaction layer does today, and which of
  its properties help or get in the way;
* proposes a design for `REPEATABLE READ` (snapshot isolation) and
  `READ COMMITTED`, built in that order and enabled together behind
  one cluster version.

Throughout, "weak isolation" means either of the two new levels, and
"SER" means datax's existing serializable isolation.

## Goals and constraints

1. **PostgreSQL semantics** for the two levels, as chapter 13.2 of the
   PostgreSQL documentation defines them. Under RC, the common
   write-write conflict does not fail with `40001`. Under RR, the first
   updater wins and the loser gets PostgreSQL's `40001` message. Where
   datax cannot match PostgreSQL exactly, the difference is listed here
   and in the user documentation.
2. **SER stays the default and stays unchanged.** A serializable
   transaction behaves exactly as it does today, including when weak
   transactions run alongside it. Internal transactions (descriptor
   leases, advisory locks, backfills, jobs) stay serializable, because
   they are written against serializability.
3. **No silent anomaly in a mixed-version cluster.** Every request field
   whose meaning a replica must honour is sent only after the new
   cluster version is finalized.
4. **Opt-in.** A cluster setting enables the weaker levels. Without it,
   `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` keeps running the
   transaction serializable, as it does today.

## Today

### The settings

| What | Behaviour now | Code |
|---|---|---|
| `BEGIN ISOLATION LEVEL x`, `SET TRANSACTION ISOLATION LEVEL x` | parsed for all four levels and stored in `transaction_isolation`; no effect | `parser.parseTransactionModesAsOf`, `vars.go` |
| `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL x`, `SET default_transaction_isolation` | accepted and stored | `pgvars.go` (`show:` forces the reported value) |
| `SHOW transaction_isolation`, `SHOW default_transaction_isolation`, `current_setting(...)` | always `serializable` | `vars.go` (`"the truth, whatever was requested"`), `pgvars.go` |
| `SET TRANSACTION ISOLATION LEVEL` after the block's first query | accepted; PostgreSQL refuses it with `25001` | no check (`txnQueried` exists, but only `SET TRANSACTION SNAPSHOT` and `AS OF SYSTEM TIME` consult it) |

The gap register's row for PG-X-001 says that
`default_transaction_isolation` is `42704`. That is out of date: commit
`784cede` made it a known setting. The tests that pin the current
behaviour are in `pkg/testutils/testcluster/session_test.go` (the
`transaction_isolation` and `default_transaction_isolation` cases) and
`begin_modes_test.go`. Both must change when the levels become real.

### The transaction layer

The full account is in [transactions.md](../transactions.md). This
table lists the parts that matter here.

| Mechanism | What it does now | Where |
|---|---|---|
| Timestamps | One `ReadTimestamp` and one `WriteTimestamp` per transaction (`kvpb.Transaction`). The read timestamp is taken at `BEGIN` (`db.NewTxn`). PostgreSQL takes its snapshot at the first statement. | `kvclient/txn.go` `NewTxn` |
| Commit rule | Enforced **on the server**. `evalEndTxn`, `evalOnePhase`, and the one-phase pre-check in `Replica` refuse a commit whose write timestamp differs from its read timestamp (`TxnRetryError`). | `kvserver/txn.go`, `kvserver/replica.go` |
| Write conflicts | `mvccWrite` refuses a transactional write when a committed version is at or above the **write** timestamp (`WriteTooOldError`), and on any foreign intent. A version committed between the read and write timestamps is not a conflict at the storage level. It is caught by the commit-time refresh of the read set, but only if the key was read. | `storage/mvcc.go` `mvccWrite` |
| Timestamp cache | A write below a recorded read is **pushed**, not refused. The coordinator adopts the pushed write timestamp. | `Replica` `tsCache.AllowsWrite` |
| Read refresh | Every read span is tracked (512, then condensed to 64). When a timestamp moves, each span is re-validated over `(old, new]` and the read timestamp moves forward. A failed refresh gives `40001`. | `Txn.maybeRefresh`, `RefreshRequest` |
| Uncertainty | The server computes the limit as `ReadTimestamp + max offset` from the batch it receives (`evalReadOnlyRequests`). The window therefore follows the read timestamp. | `kvserver/apply.go` |
| Locks | A lock is a **write intent**. `MVCCLock` lays one with the observed value (a tombstone if the row is absent). It checks for versions above the read timestamp, and the lock commits as a version carrying the same bytes. Locks are exclusive only, and there is no shared lock. | `storage/mvcc.go` `MVCCLock`, `apply.go` `ForUpdate` |
| Pushes | `PushTxn` reports the record's status, or aborts it if the pusher has higher priority (`PushAbort`, writes only), is the chosen deadlock victim, or finds the record expired. There is **no push of the timestamp**: a reader that finds a live intent at or below its timestamp waits for it. | `kvserver/txn.go` `evalPushTxn` |
| Waits | Pushes are woken on the same node and back off otherwise. There is a 10 s `conflictBudget` (then `40001`). `cannotOutwait` returns at once when only the holder's abort could let the waiter commit. | `kvclient/conflictwait.go`, `Txn.sendWith` |
| Deadlocks | Wait-for edges are published on the transaction record and walked by blocked pushers. The victim is deterministic and gets `40001`, where PostgreSQL uses `40P01`. | `Txn.detectDeadlock` |
| Retries | Implicit statements are retried in a new transaction up to 20 times (`DB.RunTxn`, `executeStreamingSelect`). An explicit block is never retried: any error fails it, and `40001` reaches the client. | `sql/session.go` |
| Savepoints | Physical. `ROLLBACK TO` sends one `RollbackIntentRequest` for **every key the transaction ever wrote**. `Savepoint()` copies the whole write set. Both are O(write set). | `Txn.Savepoint`, `Txn.RollbackToSavepoint` |
| `FOR UPDATE` | The fetch reads rows; a locking `Get` per row then re-checks the row at the read timestamp. `SKIP LOCKED` and `NOWAIT` modify both halves. A row committed after the snapshot is skipped by `SKIP LOCKED`, and is a retryable conflict otherwise. | `sql/exec.go` (`GetBatchForUpdate[NoWait]`, `fetchSkipLocked`) |
| SQL to KV | `UPDATE` and `DELETE` fetch rows with plain reads (`fetchRowsRaw`), compute the new rows in the session, and write a `WriteBatch`. `INSERT` probes the primary key, the unique indexes and the FK parents in one batch of point reads (`probeSet`), then writes. `ON CONFLICT` reads the arbiter key, then updates or inserts. | `sql/exec.go` |

## Semantics to match

The reference is PostgreSQL's documentation, chapter 13.2
"Transaction Isolation", with Table 13.1. Anomaly names follow
Berenson et al. ("A Critique of ANSI SQL Isolation Levels", 1995) and
Adya (1999), as Hermitage and Elle use them.

| Anomaly | RC (PG) | RR (PG) | SER (PG, datax) |
|---|---|---|---|
| Dirty write (G0), dirty read (G1a/G1b), circular information flow (G1c) | no | no | no |
| Non-repeatable read (P2, "fuzzy read") | **yes**, between statements | no | no |
| Phantom (P3) | **yes**, between statements | no (Table 13.1: "Allowed, but not in PG") | no |
| Lost update (P4) | **yes**, read in one statement and written from the client in a later one. **No** within a single `UPDATE` (EvalPlanQual) | no (`40001`) | no |
| Read skew (A5A), G-single | **yes**, between statements | no | no |
| Write skew (A5B), G2-item | yes | **yes** | no |
| Read-only anomaly (Fekete et al.; 13.2.2's batch example) | yes | **yes** | no |

**READ COMMITTED (13.2.1).** A plain `SELECT` sees only data committed
before the query began. Two statements of one transaction can see
different data. `UPDATE`, `DELETE`, `SELECT FOR UPDATE / SHARE` and
`MERGE` find their target rows in the statement's snapshot. If a target
row has since been changed by a concurrent transaction, the statement
waits for that transaction:

* if it rolled back, the statement goes on with the row it found;
* if it committed a deletion, the row is skipped;
* otherwise the statement re-evaluates its `WHERE` on the **newest
  version** of the row (EvalPlanQual) and, if the row still matches,
  applies its change to that version.

The re-check covers only rows the snapshot found. A row that comes to
match only through a concurrent change is not seen, so one statement
can act on a mixed state. 13.2.1 shows this with `hits` and with
`DELETE ... WHERE hits = 10`.

`INSERT ... ON CONFLICT DO UPDATE` either inserts or updates every
proposed row. If the conflicting row comes from a transaction whose
effects the statement cannot see, the statement waits for it and then
updates that row, even though no version of it is visible in the
statement's snapshot. `DO NOTHING` likewise skips a row because of a
conflict it cannot see. No `40001` arises at this level except from
deadlock (`40P01`) and from explicit locking clauses on a row that the
statement cannot reach.

**REPEATABLE READ (13.2.2).** The transaction sees one snapshot, taken
at the start of its first statement that is not a transaction-control
statement. PostgreSQL implements this level as snapshot isolation.
`UPDATE`, `DELETE`, `MERGE` and `SELECT FOR UPDATE / SHARE` on a row
changed since the snapshot wait for the other transaction:

* if it rolled back, they proceed;
* if it committed, they fail with `40001` `could not serialize access
  due to concurrent update` (first updater wins).

Write skew is allowed. 13.2.2 notes that even a read-only transaction
can see a state that no serial order produces.

**READ UNCOMMITTED** behaves as `READ COMMITTED` in PostgreSQL. `SHOW
transaction_isolation` still reports `read uncommitted`, and pgx's
`TestBeginIsoLevels` and its stdlib counterpart assert that the
requested level is reported back. datax does the same.

## What the KV layer makes easy, and what it makes hard

Seven facts from the code shape the design.

1. **The commit rule lives on the servers.** `ReadTimestamp ==
   WriteTimestamp` is checked in three places in `pkg/kvserver`.
   Relaxing it needs an isolation field the servers read, and so a
   cluster version gate. This is the cheap part.
2. **Writes are checked against the write timestamp, not the read
   timestamp.** SER is protected by the refresh. Under weak isolation
   there is no refresh, so a write whose timestamp the timestamp cache
   pushed could land above a version committed after its snapshot,
   without any error. That is a **silent lost update**.

   For a weak-isolation transaction, `mvccWrite` must therefore report
   `WriteTooOld` for any committed version **above the read timestamp**
   (the statement's, under RC). This is what `MVCCLock` already does
   with `readTS.Next()`. An old leader ignoring the new field would let
   the lost update through, so this is the main reason for the version
   gate.
3. **Locks are intents.** This gives exclusive, replicated locks, the
   wait queue (pushes) and deadlock detection with nothing new. But
   there is no shared lock, and a `FOR UPDATE` lock commits a new
   version with the same bytes. Under RR, a transaction that locked a
   row and committed without changing it therefore makes a concurrent
   RR writer of that row fail with `40001`. PostgreSQL's lock-only
   `xmax` does not do that.
4. **Readers wait for writers.** With no push of the timestamp, a read
   that meets a live intent at or below its timestamp waits for the
   intent's transaction. Under RC each statement reads at "now", so
   nearly every intent of a concurrent writer is below it. RC readers
   will therefore block on writers, where PostgreSQL's readers never
   do. This is true of SER today as well; weak isolation only makes it
   more visible. Pushing a weak writer's timestamp is cheap, because
   such a writer may commit above its read timestamp without a
   refresh. That is the fix, in phase 4.
5. **Per-statement uncertainty comes free.** The server derives the
   uncertainty limit from the batch's read timestamp. A per-statement
   read timestamp therefore gets a per-statement window with no server
   change. The refresh machinery that today handles uncertainty for the
   whole transaction can do it for one statement's read spans.
6. **Savepoint rollback is physical and correct, but O(write set).**
   Statement-level retries can be built as an internal savepoint per
   statement with nothing new on the servers. The client cost has to be
   fixed first, though. Today, each `Savepoint()` copies the write map
   and each rollback re-sends every key ever written. That makes a
   savepoint per statement O(n²) over a transaction.
7. **Serializability is relied on in the SQL layer.** The places that
   say so:
   * `catalog.Accessor.Update` ("serializable isolation restarts the
     second").
   * The uncached descriptor path in `catalog/lease.go`: "a schema
     change committing after that read fails the transaction's
     refresh".
   * Foreign keys (`constraint.go`: "serializable isolation already
     refuses a concurrent parent delete, so no extra locking").
   * Uniqueness, through the probe-then-write in `execInsert`.

   Backfills (`online_ddl.go`), `reshard.go`, lease acquisition,
   advisory locks, sealing jobs and tenant keys run their own internal
   transactions, which stay serializable. What they need from user
   transactions is that writes are intents and versions, and that does
   not change.

## Mechanism

### Isolation on the transaction

* `enginepb.TxnMeta` gains `IsoLevel` (0 = serializable; snapshot;
  read committed). It is a protobuf field with a fresh number and
  `omitempty` in JSON. Because it travels in `TxnMeta`, every request,
  intent and record carries it, and pushers and resolvers can read it
  from an intent.
* `kvclient.DB.NewTxn` stays serializable. A new
  `NewTxnWithIsolation(name, level)` is used only by the SQL session,
  for user transactions. A level other than serializable is refused
  below the new cluster version.
* The session chooses the level when the block runs its first
  statement, not at `BEGIN`. `SET TRANSACTION ISOLATION LEVEL` is
  allowed until then. RR takes its snapshot at that point too, as
  PostgreSQL does. Restamping the read timestamp at the first statement
  costs nothing, because nothing has been read yet. SER should do the
  same, for the same reason.

### Read timestamps

| | Read timestamp | Moves when |
|---|---|---|
| SER | one per transaction | refresh (any push), restart |
| RR | one per transaction, taken at the first statement | an uncertainty refresh before the first row is returned; otherwise never |
| RC | one per **statement**: `S_i = clock.Now()` at statement start, never below `S_{i-1}` | a refresh scoped to the statement (below) |

For RC, `kvclient.Txn` gains `Step()`, which the session calls before
each statement. It stamps a new read timestamp, starts a new
per-statement read set, and forwards the **write** timestamp to `S_i`.
The forward keeps the commit timestamp at or above every snapshot the
transaction read. Without it, the transaction could commit "before"
data it read, and a reader between the two timestamps would see its
writes without their causes.

A result that streams after the statement ends (`res.Stream`, a portal
suspended by `max_rows`) keeps the timestamp of its own statement. So
batches are stamped from a per-statement snapshot handle, not from the
transaction's current field. Cursors are already materialized at
`DECLARE` (`cursor.go`), which is PostgreSQL's per-cursor snapshot.

Own intents are visible regardless of timestamp (`MVCCGet` matches
`meta.Txn.ID`), so read-your-writes needs no change.

### Write timestamp, intents and the commit rule

* **Writes** are laid at `WriteTimestamp` as today. The timestamp cache
  may push them. Under weak isolation the push is simply adopted: there
  is nothing to refresh.
* **Conflict floor.** For a weak-isolation transaction, `mvccWrite`,
  `MVCCLock` and the one-phase path (`mvccWriteCommitted`) refuse with
  `WriteTooOld` when a committed version is **above the read
  timestamp** the batch carries, as well as at or above the write
  timestamp. `kvpb.Transaction.ReadTimestamp` must therefore reach
  `mvccWrite`, which today receives only the `TxnMeta`.

  `WriteTooOldError` also gains the conflicting key, which it does not
  carry now. The RC re-check below needs it.

  `Increment` reads the newest value and is an atomic counter at every
  level, so it is exempt.
* **Commit.** `evalEndTxn` accepts `WriteTimestamp > ReadTimestamp`
  when `IsoLevel` is weak, and commits at the write timestamp.
* **One-phase commit.** A weak one-phase batch is still never pushed in
  place: the commit deadline (`checkDeadline`, which pins descriptor
  leases) is authoritative only on the client. The server's pre-check
  still refuses a pushed one-phase batch. A weak transaction then
  re-stamps and resends **without** a refresh, one round trip rather
  than a refresh.
* **Parallel commits** need no change: status recovery reads the staged
  timestamp, whatever the isolation.

### Uncertainty

A read under RC that finds a value in `(S_i, S_i + max offset]` gets
`ReadWithinUncertaintyInterval`, as today. The coordinator refreshes
**the statement's** read spans to above the value and carries on. If
that refresh fails (something the statement read changed), the
statement is retried at a new snapshot (below), never the transaction.
Uncertainty restarts thus become statement retries, and are rarely
even that.

Under RR, an uncertainty error must move the transaction's single
snapshot, so it refreshes the whole read set. It is transparent before
the transaction's first row has been returned. After that, a failed
refresh is `40001`. PostgreSQL, a single server, has no such case.
This is a documented difference, and rare with a 500 ms maximum
offset.

Historical (`AS OF SYSTEM TIME`) transactions keep `StaleRead` and no
uncertainty window, whatever their level.

### What is left of read refresh

At RC and RR the refresh is no longer what makes a commit valid. What
remains of it:

1. **Statement-scoped refresh (RC):** for uncertainty, and for a push
   that arrives while a statement is still reading, so that one
   statement reads one snapshot.
2. **Validated reads (both weak levels).** Some reads must still hold
   at the commit timestamp, whatever the level:
   * the catalog's descriptor and name keyspaces, for the DDL
     guarantees quoted under fact 7;
   * the foreign-key checks (see [Locking](#locking)).

   The coordinator keeps these in a separate read set, each span with
   its own `FromTS`, and at commit refreshes them from there to the
   commit timestamp.

   Catalog reads are recognized by key prefix in one function in
   `kvclient`, so the catalog code needs no change. The prefixes are the
   descriptor, namespace, schema, type and role spans. The lease records
   are excluded, because renewals rewrite them constantly. FK reads are
   marked by the caller, through a context flag the way
   `kvclient.WithLockWait` is.

   A failed validation is `40001`. It is rare: a concurrent DDL on
   something the transaction used, or the FK races below.

   Today the commit-time refresh sends one `RefreshRequest` per span,
   one after another. The validated set should be batched per range.

So the refresh code stays. It is scoped either to a statement or to a
small validated set, never to the whole read history of a weak
transaction.

### Closed timestamps and follower reads

A closed timestamp bumps the timestamp-cache floor, which pushes later
writes above it. Today that costs a SER transaction older than the lag
(3 s) a refresh at commit. Long transactions such as `DROP DATABASE`
used to fail exactly there (#322). A weak transaction just commits at
the pushed timestamp. Long RC and RR transactions therefore become
cheaper than SER ones, not dearer.

Follower reads serve only historical transactions (`StaleRead`). RC
statements read at "now" and go to the leader, as SER reads do. A
weak transaction whose intents were laid before a closed-timestamp
publication may still commit at or below it. That is already safe
(closedts.go's argument rests on the drain of in-flight writes, not on
isolation).

### Commit-time validation, by level

| | SER | RR | RC |
|---|---|---|---|
| Read set refreshed at commit | all reads | catalog + FK validated reads | catalog + FK validated reads |
| Write-write conflict | refresh fails on a read key; a blind write commits above | `WriteTooOld` from the read timestamp → `40001` (first updater wins) | `WriteTooOld` from the statement's read timestamp → re-check or statement retry |
| Commit timestamp | `== read ts` (after refresh) | `>= read ts`, no refresh | `>= max S_i`, no refresh |
| Typical `40001` sources | any read-write conflict | write-write conflicts, validated reads, late uncertainty | validated reads, retry exhaustion, deadlock (`40P01`) |

### Prior art: CockroachDB

CockroachDB is listed here as the author knows it from its v23.2–v24.1
design and documentation. It was not re-fetched for this document. It
is a reference for ideas only.

| Concern | CockroachDB | datax today | This design |
|---|---|---|---|
| Per-statement read snapshots | yes. The commit timestamp may exceed the read timestamp without refresh at weak levels. | one read ts; commit equality on servers | same idea; uncertainty per statement for free (fact 5) |
| Write-write conflicts at RC | transparent statement retry from a savepoint, bounded by `max_retries_for_read_committed` (default 10) | n/a | statement retry first; a row-level re-check (EvalPlanQual) fast path for single-table DML |
| Lost-update protection | write-write conflicts against the read snapshot | protected by refresh only | conflict floor in `mvccWrite` (fact 2) |
| Locks | lock table: unreplicated and replicated locks, shared and exclusive; replicated ("durable") at RC | exclusive intents only | intents for exclusive locks; replicated shared lock records in phase 4 |
| FK checks at RC | pessimistic: shared locks on the parent row | serializable read | **optimistic first**: validated existence reads, refreshed at commit; shared locks as phase 4 option |
| Readers vs. writers | push the writer's timestamp; free for weak writers | reader waits | waits; push of weak writers' timestamps in phase 4 |
| DDL in a RC transaction | refused, as the author recalls (24.1) | n/a | allowed: catalog reads are validated |
| Enablement | cluster setting; when off, the level is upgraded to SERIALIZABLE | n/a | the same, with a notice |

What datax's layer makes easier than CockroachDB's:

* The uncertainty window already follows the read timestamp.
* Savepoints are physical, so a statement retry restores exact engine
  state.
* The refresh machinery is general enough to validate a chosen subset
  of reads, which makes optimistic FK and catalog checks cheap.

What it makes harder:

* There is no lock table or shared lock.
* There is no timestamp push.
* Savepoints cost O(write set).
* `WriteTooOld` does not carry its key.

## Write-write conflicts under RC

### Re-check versus statement retry

Two ways to give PostgreSQL's "wait, then act on the newest version":

| | **Row re-check (EvalPlanQual)** | **Statement retry** |
|---|---|---|
| How | On a conflict at key `k`, lock `k` at its **newest** version (waiting for the holder). Re-evaluate the `WHERE` and the `SET` on that version. Write, or skip if the row no longer matches or is deleted. | Roll back to a savepoint taken before the statement, take a new `S_i`, and run the whole statement again. Bounded; then `40001`. |
| Semantics | Exactly PostgreSQL's for a single-table statement whose `WHERE` and `SET` read only the row. | Stronger: the whole statement sees one newer snapshot. |
| Observable differences from PostgreSQL | None for the eligible shapes. | Rows the new snapshot adds or removes are seen, which PostgreSQL's re-check would not see. Volatile functions (`random()`, `clock_timestamp()`, `nextval()`) run again, so sequence gaps appear. The rows a statement returns can differ. Under heavy contention the retries can run out and give `40001`. |
| Hot rows | One wait in the lock queue, then one write. | N waiters on one row retry once per holder ahead of them: O(N²) work, with exhaustion possible. |
| Scope | Single-table `UPDATE`, `DELETE` and `SELECT ... FOR UPDATE` with no subquery, join, CTE or correlated `SET` that reads other rows. Also the `ON CONFLICT` arbiter. | Every statement. |
| Cost to build | A KV primitive (below) and a per-shape executor path. | Session plumbing plus the savepoint cost fix. |

**Recommendation: both.** Statement retry is the general mechanism,
and comes first. The re-check is the fast path for the shapes it
covers, which are the shapes hot-row contention comes from. CockroachDB
shipped statement retry alone. The re-check is what removes the hot-row
retry storm.

### Statement retry

* Each statement of an RC block runs under an internal savepoint. The
  savepoint gets a name no SQL can spell, and does not cancel the
  user's savepoints.
* On `WriteTooOld` for a key the statement had read, on a failed
  statement-scoped refresh, or on `cannotOutwait`, the session rolls
  back to that savepoint and steps the snapshot. It also runs the
  session-side hooks that `ROLLBACK TO SAVEPOINT` already runs:
  * `notifyRollbackToSavepoint`
  * `advisoryRollbackToSavepoint` (the advisory rewind already exists
    for internal retries: `advisoryState.next`)
  * `loRollbackToSavepoint`
  * forgetting `createdTables`
  * clearing `extraDDL` and `extraDrops`

  Then it runs the statement again.
* The bound is a session variable. CockroachDB's name,
  `max_retries_for_read_committed`, defaults to 10. Past it the error
  is `40001`.
* A statement whose rows have already been flushed to the client
  cannot be retried. A plain `SELECT` meets no write-write conflict,
  and its statement-scoped refresh fails only on a late uncertainty
  error, so this case is rare. It is `40001`.
* **Required client fixes** (`kvclient`):
  * A savepoint records a position in an append-only write log instead
    of copying the write map.
  * A rollback sends `RollbackIntent` only for keys written since that
    position, not for the whole write set.
  * The statement's read spans are dropped at `Step()`.

  With these, a statement savepoint costs O(1), and a retry costs
  O(the statement's own writes).
* A statement savepoint makes `HistoryFloor` non-negative, so a
  rewritten intent keeps one history entry. This is bounded by the
  existing `boundedHistory`.

### Row re-check (EvalPlanQual)

The new KV primitive is a locking read **at the newest version**. It
is a `GetRequest` / `ScanRequest` with `ForUpdate` and a new
`LockLatest` flag, allowed only for RC:

1. Wait in the ordinary push queue for any foreign intent.
2. Lay the lock intent above the newest committed version, instead of
   failing with `WriteTooOld`.
3. Return the newest value and its timestamp.

The session re-evaluates the statement's `WHERE` (row security's
`USING` included) and the `SET` on that value:

* still matches: write;
* gone or no longer matching: drop the lock with a single-key
  `RollbackIntent` to the pre-statement sequence, which the savepoint
  mechanism already supports, and skip the row.

The key is then taken out of the statement's read set
(`Txn.ForgetReads`, as `SKIP LOCKED` does), so a later
statement-scoped refresh does not fail on it.

`SELECT ... FOR UPDATE` under RC uses the same path:

* It locks and returns the newest version of a row that changed after
  the snapshot. Today that row is a retryable conflict.
* `SKIP LOCKED` locks the newest version instead of passing over a row
  committed after the snapshot. That is PostgreSQL's RC behaviour, and
  it removes half of PG-X-007's `SKIP LOCKED` note at this level. The
  other half (rows inserted after `BEGIN` do not appear) goes away with
  per-statement snapshots.

### Lost updates and `x = x + 1`

* `UPDATE t SET x = x + 1 WHERE id = 1` under RC:
  * The fetch reads `x` at `S_i`.
  * If a concurrent transaction committed a new `x`, the write gets
    `WriteTooOld` because of the conflict floor.
  * The re-check then reads the newest `x` under the lock and writes
    `newest + 1`. With statement retry instead, the statement runs
    again at a newer snapshot.

  No increment is lost. Without the conflict floor (fact 2), a pushed
  write timestamp would let the second update overwrite the first. That
  is the bug this design must not ship.
* Read in one statement and written from the client in a later one
  (`SELECT x` then `UPDATE ... SET x = $1`): the update is lost under
  RC, as in PostgreSQL. This is an expected anomaly, and a test.
* Under RR the second writer gets `40001` `could not serialize access
  due to concurrent update`.

### `INSERT ... ON CONFLICT` under RC

The arbiter probe (the primary key or a unique index entry,
`insertOnConflict`) becomes the `LockLatest` locking read:

* It waits for an in-flight inserter or updater of that key.
* If a row exists at the newest version, `DO UPDATE` applies to that
  row, with its `WHERE` re-evaluated. This holds even when the row is
  invisible to `S_i`, which is exactly 13.2.1's rule. `DO NOTHING`
  skips it.
* If the key is absent, the lock is a tombstone intent (as
  `GetForUpdate` already lays for absent rows). The insert then cannot
  race.

For a unique-index arbiter, the index entry is locked first, then the
row it names. Under RR an arbiter row invisible to the snapshot is
`40001`, as in PostgreSQL. This needs no new primitive: the plain probe
plus the conflict floor gives it.

## Locking

### Foreign keys

**Why SER needs no locks.** The child's insert reads the parent's key.
A concurrent delete of the parent either:

* commits before that read, and the child sees the parent gone; or
* commits after it. The child's read is then refreshed at commit and
  fails, or the delete's write is pushed above the read by the
  timestamp cache, which serializes the delete after the child. The
  delete's scan of the child index then finds the child's intent or
  version.

**Why weak isolation breaks it.** Under RC both sides read at statement
snapshots and neither is refreshed. The child can read the parent at
`S`, and the delete can scan the children at `S'` beneath the child's
not-yet-committed intent (an intent above a read timestamp is read
beneath). Both then commit, leaving an orphan. RR has the same race.

**Design (phase 2): validated FK reads, both directions.**

* *Child side:* the parent lookup is an **existence-validated read**.
  At commit it is refreshed from its `FromTS` to the commit timestamp
  with a new `RefreshRequest.ExistenceOnly` flag:
  * a deletion tombstone in the window fails it;
  * a foreign intent is pushed (waited for) and then judged by its
    outcome;
  * a new value version (a non-key update of the parent) passes.

  This works because the referenced key is the parent's primary key or
  unique index entry, which changes only when the referenced columns
  change. It is the validation form of PostgreSQL's `FOR KEY SHARE`,
  which does not conflict with `FOR NO KEY UPDATE`.
* *Parent side:* the scan of the child index for a deleted or re-keyed
  parent is a **validated span read**. Any new child entry committed in
  the window fails it.

Either way exactly one side fails. The child side gets `40001` when the
parent was deleted. The parent side gets `40001`, then `23503` on retry,
where PostgreSQL waits and then gives `23503`. These are the only
`40001`s the design adds to RC's common paths, and they need a delete
racing an insert of the same key relationship.

Under RR this matches PostgreSQL closely: its RI checks also raise
`40001` under RR when the parent or child changed after the snapshot.

The alternative is CockroachDB's: replicated **shared** locks on the
parent row, taken at the child's insert. It blocks the deleter instead
of failing anyone, at the cost of a replicated lock write per checked
parent per statement. That waits for phase 4 and a measured need. An
exclusive lock (the existing intent) would be correct today, but it
serializes every insert of children of one parent and blocks the
parent's updates. A lookup table referenced by every row would become
a single point of contention.

### Unique checks

A plain `INSERT` probes the key at `S_i`, finds it absent, and writes.
A concurrent inserter of the same key either:

* holds an intent: the write waits for it (as today); or
* has committed above `S_i`: the conflict floor gives `WriteTooOld`.

In either case, once the holder has finished, the session re-reads the
key under its own lock (`LockLatest`):

* present: `23505`, with the existing message;
* absent: the insert proceeds.

This is PostgreSQL's behaviour at every level: it waits, then gives
`23505` or succeeds. It also closes PG-X-007's first item for weak
transactions, and could be adopted by SER later. Unique secondary
indexes work the same way on the index entry key.

Statement-end checking (PG-X-012) is unchanged.

### `FOR SHARE`, `FOR KEY SHARE`, `FOR NO KEY UPDATE` (PG-X-004, CR-TX-002)

These are not needed for RC or RR to be correct; FKs are covered
above. Phase 4 adds them as real locks:

* **Lock records.** `keys.LockKey(key, txnID)` under the range-local
  *addressed* prefix, the same shape as `keys.TransactionKey`:
  * one record per holder, `{strength, txn meta}`;
  * replicated, and moving with splits, merges and snapshots, as
    transaction records already do;
  * invisible to user scans, and not exported by backups.
* **Conflicts.**
  * A write, or an exclusive lock, checks the key's lock records as
    well as its intent.
  * A shared lock checks for an intent, or for an exclusive record of
    another transaction.
  * The strengths follow PostgreSQL's conflict table (13.3.2).
* **Lifecycle.**
  * A lock record is part of the write set, so it is resolved at commit
    or abort.
  * Pushers resolve the records of finished or expired holders.
  * GC skips keys with live records, as it skips intents.
* **Lock-only intents.** Exclusive locks stay intents, with a new
  `LockOnly` metadata flag. At commit a lock-only intent is removed
  instead of committed as a same-bytes version, and the timestamp cache
  at that key is bumped to the commit timestamp. That removes the
  spurious RR `40001` (fact 3) and keeps SER's read protection.
* **Upgrade.** A holder of a shared lock that writes waits for the
  other shared holders. Two shared holders that both upgrade form a
  cycle, which deadlock detection breaks.

The SQL forms are then honoured at every level:

* `FOR SHARE`, `FOR NO KEY UPDATE`, `FOR KEY SHARE`;
* `FOR UPDATE OF table`, which is parser work.

Under SER, `FOR SHARE` may stay a validated read (it is already
serializable); taking the real lock is a session option, as CockroachDB
does.

### Waits, priorities, deadlocks

* **No priority aborts for weak transactions.** A weak transaction
  pushes with `PushAbort = false`, and its record is not aborted by a
  higher-priority pusher. PostgreSQL's RC and RR wait; an abort by
  priority would be a `40001` that PostgreSQL never gives. Expiry and
  deadlock victims still abort it. `evalPushTxn` reads the pushee's
  `IsoLevel` from the request.
* **No `conflictBudget` for weak transactions.** They wait until the
  lock is granted, `lock_timeout` (`55P03`) or `statement_timeout`
  (`57014`), as PostgreSQL does. Deadlock detection is what breaks
  cycles; the 10 s budget was a backstop for it.
* **`cannotOutwait`** stays for SER. Under RR it would give `40001`
  without waiting, where PostgreSQL waits and may succeed if the holder
  rolls back. Under weak isolation it is off, and the wait is taken.
* **Deadlock victims** get `40P01` `deadlock detected` when the victim
  is a weak transaction (PostgreSQL's code). SER keeps `40001`, which
  its retry loops already handle.
* With shared locks, a waiter blocked on several holders waits on them
  one at a time. It publishes its edge to the one it is waiting on now,
  so the single `waitingFor` per record still describes a real edge,
  and a cycle through any holder is found when that edge is current.

## Interactions

| Area | Under weak isolation |
|---|---|
| **Mixed with SER** | SER transactions stay serializable among themselves. Anomalies involving a weak transaction are allowed, as in PostgreSQL, where SSI guarantees only SER-to-SER. A SER read beneath or before a weak write pushes that write's timestamp through the timestamp cache, and the weak transaction commits higher at no cost. A SER reader of a weak intent waits, until phase 4's timestamp push. A weak writer never aborts a SER holder by priority, and a SER writer never aborts a weak one. |
| **Schema changes** | DDL is allowed in RC and RR blocks. Catalog reads are validated at commit, so `Accessor.Update`'s compare-and-swap and the uncached-descriptor guarantee keep holding. The CAS's descriptor write also conflicts write-write. Under RC each statement plans against the leased version current at its snapshot, as PostgreSQL uses a fresh catalog snapshot per statement, and the online-DDL states are designed to tolerate writes under adjacent versions. |
| **DDL drains** | Unchanged. The drain waits on leases, and the lease deadline (`UpdateDeadline`, `checkDeadline`) bounds the commit timestamp at any level. Because a weak transaction's commit timestamp only moves up, the deadline remains the binding check, and it stays on the client (one-phase batches are never pushed in place). A long RC transaction that spans a DDL fails at commit when its earliest lease has expired, as a SER one does. |
| **Temp tables** (PG-D-009) | Their rows are reachable by one session only, so isolation does not apply to them. `ON COMMIT DELETE ROWS / DROP` runs in the transaction (`tempPreCommit`) as now. Their DDL is catalog work, validated as above. A statement retry forgets `createdTables`, as `ROLLBACK TO SAVEPOINT` already does. |
| **Schemas** (PG-D-001) | `CREATE SCHEMA` and `ALTER SCHEMA ... RENAME` write name entries. Two racers conflict write-write: under RC the retry finds the entry and gives `42P06`; under RR the loser gets `40001`. Name resolution reads the namespace at the statement's snapshot, and these reads are validated catalog reads. |
| **`AS OF SYSTEM TIME`** | A historical transaction is read-only at one fixed timestamp, and all levels coincide there. The requested level is accepted and reported, and every statement reads at the `AS OF` timestamp. Statement-level `AS OF SYSTEM TIME` inside a block stays refused (PG-X-006). |
| **Sequences** | `nextval` advances outside the transaction (`sequence.go`) and is unaffected. A statement retry draws new values, so there are gaps, as PostgreSQL also has. `setval` is a transactional `Put`, so concurrent `setval`s conflict write-write. |
| **Advisory locks** | Held through their own serializable transactions (`pkg/sql/advisory`), so they are unaffected. A statement retry replays the statement's calls through the existing rewind (`advisoryState.next`, rewound to the statement's first call rather than to 0). `_xact_` locks end with the transaction as now. |
| **Large objects** | `AdvanceTo`, which keeps the row making an object visible above the pages written outside the transaction, becomes a plain forward of the write timestamp. No refresh is needed, so it cannot fail. |
| **Backups** | A backup reads at one timestamp in its own transaction and is unaffected. Lock records (phase 4) are not data and are not exported. Weak intents in the backup's way are waited for, as now, or pushed, in phase 4. |
| **Closed timestamps, GC** | Covered above. A weak transaction cannot read below the GC threshold for longer than a statement, so `BelowGCThreshold` restarts become statement retries under RC. |
| **Changefeeds** | Not implemented (CR-CDC-001). For later: a weak transaction's commit timestamp is above all its snapshots, and its versions resolve at that timestamp, so a changefeed sees nothing new. |

## Configuration and rollout

* **Cluster settings**, registered in `clusterSettings`: they gate the
  feature, not the default level.
  * `sql.txn.read_committed_isolation.enabled` and
    `sql.txn.repeatable_read_isolation.enabled` (CockroachDB's names).
  * Both default to `false` in the first release.
  * They are read when a transaction chooses its level. That needs a
    node-local cached copy, refreshed every few seconds;
    `ReadClusterSetting` costs a KV read per call.
* **Default level** stays `serializable`. `default_transaction_isolation`
  is honoured per session, and with `ALTER ROLE / DATABASE ... SET`
  (`dbopts.go` already takes `default_transaction_isolation` there, and
  refuses `transaction_isolation`, as PostgreSQL does).
* **Protocol version V23**, a new one: V22 is the version this tree is
  developing and has shipped content.
  * Every new field is sent only from V23:
    * `TxnMeta.IsoLevel`;
    * the read timestamp passed to `mvccWrite`;
    * `LockLatest`;
    * `RefreshRequest.ExistenceOnly`;
    * the key in `WriteTooOldError`;
    * the lock records and the `LockOnly` flag.
  * A weak level is refused below V23, and the transaction runs
    serializable, as when the setting is off.
  * Golden decode tests freeze each new field (rule 5).
* **Before enablement**, or below V23, `SET TRANSACTION ISOLATION LEVEL
  READ COMMITTED` and friends run the transaction **serializable**, as
  today. The difference is that the session now sends a `NOTICE`
  saying so, as CockroachDB's upgrade does, and `SHOW
  transaction_isolation` keeps reporting `serializable` (the truth).
  Refusing the statement would break drivers that set a level on
  connect.
* **Independent of enablement** (phase 0):
  * `SET TRANSACTION ISOLATION LEVEL` after the block's first query is
    refused with `25001` `SET TRANSACTION ISOLATION LEVEL must be
    called before any query`, as PostgreSQL does.
  * Inside a savepoint it is refused with `25001` `SET TRANSACTION
    ISOLATION LEVEL must not be called in a subtransaction`.
* **Implicit statements** run at `default_transaction_isolation`.
  Under RC, a single-statement `UPDATE` on a hot row then waits and
  re-checks instead of retrying up to 20 times and giving `40001`. DDL
  run as an implicit statement stays serializable, as DDL's own
  internal phases are.
* **Docs:**
  * a "Weak isolation" section in [transactions.md](../transactions.md);
  * in `docs/user/sql.md`, "Transactions and retries" (what each level
    retries, and which `40001`s remain) and the settings table;
  * in `docs/user/postgres-differences.md`, the "Serializable only"
    bullet, `SKIP LOCKED` and the concurrent-insert bullet;
  * the gap register: PG-X-001 and CR-TX-001 closed, PG-X-007 narrowed
    to SER, and PG-X-004 and CR-TX-002 when phase 4 lands.

## Phasing

**Recommendation: build RR first, then RC, and release them together
under V23.** RC is what applications need. RR is the smaller part, all
of which RC needs anyway:

* the isolation field;
* the relaxed commit rule;
* the conflict floor (the lost-update risk);
* validated catalog and FK reads;
* the wait and priority policy;
* the gate.

Building RR first puts every server-side change behind a level with one
snapshot, where an Elle-style snapshot-isolation checker can verify it.
Only then does RC add per-statement snapshots and statement retries.
Shipping RR on its own would give applications little, since RR code
must retry `40001` too. RR can go out as a preview behind its setting
if RC slips.

| Phase | Content | Size |
|---|---|---|
| 0 | `25001` for a late or nested `SET TRANSACTION ISOLATION LEVEL`; the snapshot taken at the first statement (all levels); the notice; the cached setting; the register row corrected | S |
| 1 — RR | `IsoLevel`; commit rule; conflict floor in every MVCC write path; validated catalog reads; validated FK reads with `ExistenceOnly`; waits without priority aborts, budget or `cannotOutwait`; `40P01`; `40001` message wording; V23 gate; anomaly suite at SER and RR | L |
| 2 — RC | `Step()` and per-statement snapshot handles; statement-scoped refresh; O(1) savepoints and O(statement) rollback; statement savepoints and retries with session hooks; `max_retries_for_read_committed`; unique checks with `23505` after waiting; `ON CONFLICT` via `LockLatest`; implicit statements at the session level | L |
| 3 — re-check | `LockLatest` for single-table `UPDATE`, `DELETE` and `SELECT FOR UPDATE`; RC `SKIP LOCKED` on the newest version; `WriteTooOld` key; shape eligibility in the planner | M |
| 4 — locks | lock records; `FOR SHARE`, `FOR KEY SHARE` and `FOR NO KEY UPDATE`; `FOR UPDATE OF`; lock-only intents; optionally FK checks with shared locks; push of weak writers' timestamps so readers do not block | L |

Phases 0–3 close PG-X-001 and CR-TX-001. Phase 4 closes PG-X-004 and
CR-TX-002 and removes the reader-blocking difference.

## Testing

* **Storage and KV units.**
  * `mvccWrite` and `MVCCLock` with the conflict floor: a version
    between the read and write timestamps is `WriteTooOld` for weak
    transactions and not for SER.
  * `evalEndTxn` and `evalOnePhase` with diverged timestamps per level.
  * `ExistenceOnly` refresh: a tombstone fails, a value version passes,
    an intent waits.
  * `LockLatest`.
  * The savepoint cost: rollback sends only keys written since the
    savepoint.
  * Golden decodes for the new fields.
* **Hermitage cases** (Kleppmann's per-level PostgreSQL results) in a
  new `pkg/testutils/testcluster/isolation_test.go`. Each case is a
  scripted two- or three-session interleaving with barriers:
  * G0, G1a, G1b, G1c;
  * OTV, PMP and PMP-write;
  * P4 (lost update);
  * G-single (read skew) and G-single-write;
  * G2-item (write skew) and G2.

  Each is asserted per level against PostgreSQL's recorded outcome
  (blocks, error, or commits with a given result). This is the
  authoritative "semantics to match" check.
* **Elle-style histories.** A list-append workload over SQL: `UPDATE t
  SET v = v || $x WHERE k = $k`, with reads of whole lists. It runs
  with random transactions per level and records invocations and
  completions. A Go checker builds the dependency graph from them
  (ww, wr and rw edges, with real-time order for SER) and classifies
  its cycles. Assertions:
  * SER: no cycle.
  * RR: no G0, G1 or G-single; G2 permitted.
  * RC: no G0 or G1; G-single and G2 permitted. Each statement is
    checked as internally consistent (one snapshot).

  The history can also be written as EDN for an offline run of the
  real Elle; that is not part of CI.
* **Expected anomalies actually occur.** Deterministic interleavings
  assert that each of these does happen, so a level that is
  accidentally stronger fails:
  * write skew under RR (on-call doctors: both commit);
  * read skew and lost update (read, then write an absolute value)
    under RC.
* **Bank invariant.** `TestBankTransfers`' workload at RR and at RC,
  with relative updates (`balance = balance - $1`); the total holds at
  every level. A variant with absolute writes must break the total
  under RC and hold under RR (with `40001`s). The audit query (`SUM`)
  is consistent within a statement at RC and within the transaction at
  RR.
* **Hot row and queue.** Under RC with the re-check, N workers
  incrementing one row give exactly N increments, with no `40001`. The
  existing `SKIP LOCKED` queue test is extended to RC: every row is
  taken once, including rows inserted after the workers' `BEGIN`.
* **FK and unique races.**
  * A parent delete racing a child insert: never an orphan. One side
    fails with `40001`, or `23503` after a retry.
  * Concurrent inserts of one key: `23505` after a wait, never `40001`,
    under RC and RR.
* **pgx.**
  * pgx's `TestBeginIsoLevels` (`tx_test.go`) and the stdlib isolation
    test. Both skip on CockroachDB "because it always reports
    serializable"; once the feature is enabled they pass unmodified.
  * Testcluster tests through pgx with `pgx.TxOptions{IsoLevel: ...}`,
    including `pgx.BeginFunc` retry behaviour.
* **Faults.**
  * The chaos suite (`chaos_test.go`: the bank workload through
    partitions and node kills, then the consistency checker) runs with
    RR and RC workers mixed with SER ones.
  * The crash harness (`pkg/testutils/crash`, kills at fault points
    such as `raft-apply:N`) kills a node mid-way through RC
    transactions that roll back statement savepoints, and a node
    holding lock records. Every acknowledged commit must survive, and
    no lock may outlive its record.
* **Upgrade.** A V22 cluster refuses weak levels and runs them
  serializable. After finalize they take effect. No weak-isolation
  request reaches a node before V23.
* **Existing tests that assert the old behaviour.** These change with
  the feature:
  * `session_test.go`'s `transaction_isolation` and
    `default_transaction_isolation` cases;
  * `begin_modes_test.go`'s `SHOW transaction_isolation` checks.

  Before enablement they stay valid.
* **Benchmarks.** `bench/` bank, hot-row and kv at SER (no regression)
  and at RR and RC.

## Effort and risks

Total: XL, about 3–4 months for one engineer through phase 3. Phase 4
is another 4–6 weeks.

| Risk | Mitigation |
|---|---|
| A write path without the conflict floor gives a silent lost update | One helper used by every transactional MVCC write (`Put`, `Delete`, `MVCCLock`, one-phase committed writes); a unit test per request type; the Elle P4 check |
| Hidden reliance on serializability in the SQL layer | The audit list in fact 7; validated catalog reads by key prefix rather than per call site; Hermitage and Elle runs with DDL in the mix |
| Statement retries leak session side effects | Reuse of the `ROLLBACK TO SAVEPOINT` hooks; a test per hook (notify, advisory, large object, temp table) |
| Savepoint-per-statement cost | The write-log and rollback fix lands before statement retries; benchmarked |
| RC readers block on writers, where PostgreSQL's do not | Documented; phase 4's timestamp push for weak writers |
| More waiting means more deadlocks | Existing detection; `40P01`; deadlock tests at RC |
| Mixed-version misapplication | V23 gate, golden decodes, upgrade test |
| Users expect "never `40001`" under RC | The remaining sources are listed in the user docs: FK races, retry exhaustion, late uncertainty on a flushed stream, DDL validation |

## Open questions for the owner

1. **Default level?** Recommended: keep `serializable`. Weak levels are
   opt-in per session, role or database.
2. **Settings' defaults?** Recommended: off in the first release that
   carries V23. Turn on after one release of field use. The default
   level stays serializable either way.
3. **What does a weak level do when disabled?** Recommended: run
   serializable with a `NOTICE`, and `SHOW` says `serializable`. Not an
   error, because drivers set levels on connect, and not silent as
   today.
4. **Do implicit statements follow `default_transaction_isolation`?**
   Recommended: yes. DDL stays serializable. Hot-row single statements
   are where RC helps most.
5. **RC write-write conflicts: statement retry, row re-check, or both?**
   Recommended: both. Statement retry is general and comes first. The
   re-check is the fast path for single-table DML and `ON CONFLICT`.
6. **FK checks under weak isolation: optimistic or locks?**
   Recommended: optimistic validated reads (`ExistenceOnly`) in phases
   1–2. Shared locks only if measurements of the rare `40001` race
   justify a replicated lock write per checked parent.
7. **DDL inside RC and RR blocks: allow or refuse?** Recommended: allow,
   with validated catalog reads. Migration tools run DDL in
   transactions at whatever level the connection uses.
8. **Statement retry bound and its variable name?** Recommended:
   CockroachDB's `max_retries_for_read_committed`, default 10, then
   `40001`.
9. **Priority aborts and the 10 s budget for weak transactions?**
   Recommended: neither. They wait, as in PostgreSQL. Deadlocks give
   `40P01`; `lock_timeout` and `statement_timeout` bound the wait.
10. **RR uncertainty: retry transparently?** Recommended: transparently
    until the first row has been returned, `40001` after. This is
    documented.
11. **`SKIP LOCKED` at RR:** keep today's "skip a row committed after
    the snapshot", or fail with `40001` as PostgreSQL would?
    Recommended: keep skipping (queue consumers prefer it) and document
    it. At RC the newest version is locked, as in PostgreSQL.
12. **Phase 4 in scope for closing PG-X-001?** Recommended: no. Phases
    0–3 close it. Shared locks, lock-only intents and non-blocking reads
    are their own gaps (PG-X-004, CR-TX-002, and a new row for "readers
    wait for writers").
13. **The snapshot at the first statement for SER too?** Recommended:
    yes, in phase 0. It is PostgreSQL's rule, costs nothing, and
    removes the "rows inserted after `BEGIN` do not appear" note for
    `SKIP LOCKED`.

## References

* PostgreSQL documentation, §13.2 "Transaction Isolation" (Table 13.1;
  13.2.1 Read Committed, 13.2.2 Repeatable Read, 13.2.3 Serializable)
  and §13.3.2 "Row-Level Locks" (the lock-mode conflict table).
* H. Berenson et al., "A Critique of ANSI SQL Isolation Levels",
  SIGMOD 1995. A. Adya, "Weak Consistency", MIT PhD thesis, 1999.
* M. Kleppmann, Hermitage: testing transaction isolation levels
  (per-level PostgreSQL results for G0 to G2).
* K. Kingsbury and P. Alvaro, "Elle: Inferring Isolation Anomalies
  from Experimental Observations", VLDB 2020.
* CockroachDB's read committed design and documentation, v23.2–v24.1,
  as known to the author (not re-fetched).
* [transactions.md](../transactions.md), [schemas.md](schemas.md),
  `docs/compatibility-gaps.md` rows PG-X-001, PG-X-004, PG-X-007,
  PG-X-012, CR-TX-001, CR-TX-002.
