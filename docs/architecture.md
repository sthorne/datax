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
| `/system/desc/<tableID>`, `/system/ns/<name>` | SQL catalog | range 1 |
| `/system/idgen` | descriptor / table ID counter | range 1 |
| `/t/<tableID>/<pk...>` | user table rows | user ranges |

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
   metadata; the **DistSender** looks up which range owns the key (range cache,
   backed by `/meta/`) and sends a `BatchRequest` to a replica of that range.
4. `kvserver` on the Raft leader checks the timestamp cache, then proposes the
   write to the range's Raft group. Once a quorum has appended it, each replica
   **applies** it: an MVCC *write intent* lands in Pebble.
5. At `COMMIT`, the coordinator flips the transaction record to COMMITTED — a
   single Raft write — then asynchronously resolves intents to plain values.

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
   is idempotent.
2. **Linearizable reads**: only via ReadIndex on the leader. On leadership
   acquisition the timestamp cache floor is bumped to `now()`, because a new
   leader cannot know what reads the old leader served. The one non-leader
   read path is a **follower read**: a read pinned to a fixed timestamp at
   or below the range's replicated closed timestamp (`AS OF SYSTEM TIME`),
   which is linearizable *at that timestamp* by construction.
3. **Clocks**: every RPC carries an HLC timestamp; receivers ratchet their
   clock. A remote clock further than `--max-offset` ahead is a fatal error.
   Reads treat values in `(readTS, readTS+maxOffset]` as uncertain and restart.
4. **Intents**: an intent is never resolved except according to its
   transaction record's state. Readers/writers who find an abandoned intent
   push the record (which aborts expired transactions) and then resolve.
5. **Serializability**: a transaction commits only if its write timestamp
   still equals its read timestamp; anything that bumps the write timestamp
   forces a retry (see [transactions.md](transactions.md)).

## Modules

- `pkg/base` — shared ID types (NodeID, StoreID, RangeID) and configuration.
- `pkg/util/hlc` — hybrid logical clock.
- `pkg/util/encoding` — order-preserving key encodings.
- `pkg/util/stop` — goroutine lifecycle / graceful shutdown.
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
