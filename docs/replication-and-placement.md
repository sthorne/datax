# Replication, membership, and rack-aware placement

## Ranges and multi-raft

The keyspace is divided into contiguous **ranges**, each described by a
`RangeDescriptor{RangeID, StartKey, EndKey, Replicas, NextReplicaID,
Generation}`. Each range is an independent Raft consensus group
([etcd-io/raft](https://github.com/etcd-io/raft)); a node hosts one `Replica`
object per range it carries — "multi-raft".

Per-replica Raft state lives in unreplicated local keys
(`/local/r/<rangeID>/...`) in the node's shared Pebble store:
HardState, log entries, applied index, and a copy of the descriptor.

**Durability contract** (the classic etcd-raft rules, enforced in the ready
loop):

1. Persist HardState + new entries (synced batch) **before** sending any Raft
   messages from the same Ready.
2. Apply committed entries and advance the stored applied index in **one**
   Pebble batch, so replay after a crash is idempotent (entries at or below
   the applied index are skipped).

A single 200ms ticker drives all Raft groups on a node. `PreVote` and
`CheckQuorum` are enabled.

**Transport**: one gRPC bidirectional stream per node pair; each message is
`{RangeID, To, From, opaque raftpb bytes}`. Raft messages are opaque to the
transport.

**Reads** are linearizable via ReadIndex on the leader (see
[architecture.md](architecture.md)). v1 has no separate lease mechanism:
leaseholder = Raft leader.

**Splits** happen automatically by size (v2; see below) or manually
(`datax debug split <key>`): the split is proposed as a replicated command;
at apply time each replica atomically writes both descriptors and creates
the right-hand side's Raft state. No data moves, because range membership
of a key is logical. The `/meta/` addressing records are then updated
transactionally.

**Merges** are the inverse: when a range and its right neighbor are both
below the merge threshold (default: a quarter of the split threshold) and
have identical replica sets, the housekeeping pass on the node leading the
left-hand side pulls the right-hand side's leadership to itself and merges
in two replicated phases. First a **Subsume** command freezes the RHS —
every replica persists the flag, and the range refuses all traffic from
then on (serving is leader-only, and every future leader or restart
reloads the flag), which also pins its membership. Then a **merge trigger**
on the LHS: at apply, each replica waits for its local RHS replica to
reach the subsume index (the RHS's final command, so engines are
identical), quiesces the RHS group *keeping its data*, and atomically
widens the LHS descriptor, absorbing the RHS's size and GC threshold.
Transaction records are addressed keys and follow their anchors with no
rewriting; the surviving leader bumps its timestamp cache over the
absorbed span so nothing can write beneath a read the RHS served. `/meta`
shrinks in one atomic batch (the merged record overwrites the RHS's — same
end key — and the old LHS record is deleted); a failed cleanup is benign
and re-repaired. An interrupted merge is re-driven (or unfrozen, if
membership diverged) by the same pass; `datax debug merge --range N`
drives one by hand.

## Peer discovery

Range addressing lives in the KV layer, but node *addresses* deliberately do
not depend on it — two mechanisms break what would otherwise be a
circularity (an election needs peer addresses; addresses in a registry
range; the registry range needs an election):

- **Address piggybacking**: every Raft envelope carries the sender's node
  ID and RPC address; receivers learn peers from Raft traffic itself. An
  address learned this way also counts as a liveness observation — live
  traffic is stronger evidence than any registry row, so a stale row can
  never clobber it.
- **Persisted registry**: each node saves its last known node registry to a
  local store key and reloads it at startup, so a fully restarted cluster
  can re-form with no leader anywhere and no `--join` flags.
- **Re-announce**: an already-initialized node may come back on a
  *different* address (rescheduling, port churn). At startup it re-sends
  the join RPC — with its node ID, so no new ID is allocated — to its
  configured `--join` target and every persisted-registry peer. Receivers
  adopt the address into their in-memory registries with **no KV writes**,
  so this works while quorum is still down, and the response carries every
  fresh address the receiver has already learned from other announcers.
  A whole cluster restarted on all-new addresses therefore re-forms as
  long as nodes share a reachable announce target (the usual static
  `--join` config); from there, piggybacking and registry rows converge
  the rest.

The registry rows in range 1 (with localities and liveness) remain the
authority for the allocator; these mechanisms only guarantee reachability.
A restarted node publishes its row (with its current address) on its first
heartbeat, immediately at startup rather than a tick later.

## Membership and bootstrap

No gossip protocol in v1; membership is join-based:

- `datax init` creates the store ident (new cluster UUID, node ID 1) and
  bootstraps **range 1** spanning the whole keyspace as a single-replica Raft
  group, writing the initial `/meta/` record and node registry entry directly
  (transactions don't exist yet at that instant — bootstrap ordering matters).
- `datax start --join=<addr>` calls Join on any live node: the joiner is
  allocated a node ID (a counter in range 1, incremented transactionally),
  and receives the cluster ID, the node registry, and the current range-1
  descriptor — the **routing bootstrap**. All other range addressing goes
  through `/meta/` records stored *in* range 1.
- Every node heartbeats a liveness timestamp into `/system/nodes/<id>` every
  3s; this doubles as the allocator's liveness signal.

**Upreplication**: a background loop on the node leading range 1 scans all
range descriptors; any range below the replication factor (default 3), when
enough live distinct nodes exist, gets a replica added via the allocator +
Raft ConfChange. So a cluster grows organically: node 1 alone runs everything
at RF=1; as nodes 2 and 3 join, every range is raised to RF=3.

## Dead-node repair

The same loop repairs replicas stranded on dead nodes (v2). A node whose
registry heartbeat is staler than `DeadNodeThreshold` (default 30s) is dead;
the threshold deliberately exceeds the allocator's liveness grace (15s), so
a briefly-restarting node causes zero replica churn. For each affected
range the repair is **add-then-remove** — an allocator-picked (diversity-
valid) live target is added first, so membership never dips below its
starting size mid-repair — with at most one repair per range per tick.
Two guards make it idle safely instead of flailing:

- **quorum guard**: if the range's live replicas are not a strict majority,
  no ConfChange could commit anyway — skip and warn;
- **no-spare guard**: if no live node without a replica exists, skip.

A dead voter no longer pins Raft log truncation (returning voters are
caught up by snapshot), so repair is purely about restoring redundancy.

## Node decommission

`datax debug decommission --node N [--wait]` retires a node gracefully:
instead of killing it and waiting for dead-node repair (which loses
redundancy for the repair window), the node is marked **draining** and the
allocator proactively moves its replicas away — one per tick, via the same
add → transfer-lease-if-leading → remove sequence as rebalancing — while
the node is still alive to serve and vote. Draining nodes never receive new
replicas (from upreplication, repair, or rebalancing), still count as live
voters for quorum math, and a drain that would leave a range without a
diversity-valid target stalls safely (the replica stays; the range never
drops below its replication factor) until a node joins. Once the count
reaches zero — `--wait` follows it — the process can be stopped with zero
repair churn. `--cancel` un-drains.

The draining flag lives in the node's own registry row. Since a node
overwrites its whole row on every heartbeat, the decommission op is
forwarded to the target node itself, which holds the flag in-process and
re-asserts it on every beat (surviving restarts by re-adopting it from its
row); only an unreachable node's row is written directly, and it adopts the
flag if it ever comes back. A draining node that dies anyway is finished by
ordinary dead-node repair.

## Quorum-loss recovery (unsafe)

Losing a range's quorum — worst of all range 1's, which carries `/meta`
addressing, descriptors, and users — normally leaves it permanently
unavailable. Two tools turn that into a recoverable incident:

- **Metadata export**: every disk-backed node writes
  `<dir>/metadata-backup.json` on each heartbeat — decoded `/meta` records,
  table descriptors, the namespace, user credential verifiers, and the node
  registry — atomically (tmp+rename). `datax debug metadata --dir` prints
  it, online or offline.
- **`datax debug unsafe-recover --dir <dir> [--range N] --yes`**: with the
  node STOPPED, rewrites its range descriptors to single-replica
  membership (its own replica, generation bumped). On restart each
  recovered range derives a single-voter ConfState from its descriptor,
  elects itself, and serves; upreplication restores RF as fresh nodes
  join. This **discards the removed replicas' votes and any writes only
  they acknowledged** — run it on exactly one survivor per range, and
  never restart the removed peers with their old data (wipe and rejoin
  fresh).

## Snapshots: preseed and raft catch-up

A new replica is seeded by streaming a snapshot (the range's data span +
descriptor + applied index) over gRPC **before** the ConfChange commits
("preseed"), avoiding the stall where a new member can't vote until it gets a
snapshot through Raft. Configuration changes happen one replica at a time (no
joint consensus in v1).

The same stream also serves **raft catch-up snapshots**: when a follower
needs entries the leader has truncated, raft requests a snapshot — the
storage answers with metadata only (index, term, voters) — and the replica
intercepts the resulting MsgSnap. Snapshot bytes never ride inside raft
messages (the transport caps message sizes and sheds under pressure):
the leader streams the state machine out of band, the receiver **stages**
the install as an uncommitted batch, and only when the forwarded
metadata-only MsgSnap comes back through raft's own restore flow does the
replica commit the staged batch and swap its in-memory state — mutating
raft's storage underneath a live node is a protocol violation. The leader
then reports the outcome to raft (retry on failure) and, while any stream
is in flight, log truncation holds at the streamed index so the receiver
can be served the entries after its install.

One sharp edge: a replica whose range **bounds** changed while it was away
(it missed a split) cannot be caught up by snapshot — its stale span may
overlap sibling replicas on the same store. Such a replica is refused and
repaired by removal and re-add, the same remedy as a dead node.

## Internode security

With `--certs-dir` set, all internode gRPC runs over **mutual TLS**: every
node presents its node certificate and requires a CA-signed client
certificate from callers, so an attacker on the network can neither read
nor join the cluster's replication traffic. `datax cert
create-ca|create-node|create-client` generates the material with standard
library crypto (ECDSA P-256). Insecure mode (no certs dir) keeps v1's
cleartext behavior for development.

## Lease-based reads

Reads are leader-only and linearizable. v1 confirmed leadership with a full
quorum round trip per read (raft's `ReadOnlySafe` ReadIndex); v2 defaults to
**lease-based** ReadIndex (`ReadOnlyLeaseBased`): the leader answers from
its CheckQuorum lease, eliminating the per-read network round trip. The
read-your-writes property is unchanged — every read still waits until the
applied index reaches the confirmed commit index — and concurrent readers
**coalesce**: waiters arriving while a confirmation is in flight share the
next one (registered before it is issued, so its index covers everything
committed before they arrived).

The correctness argument: with CheckQuorum, a leader steps down when it
loses contact with a majority for an election timeout, and PreVote keeps
disruptive candidates from starting elections while a live leader exists.
On top of that sits a **wall-clock backstop**: the leader serves lease
reads only while a majority (itself included) has answered within
`electionTimeout − MaxOffset`. A follower that answered at time T cannot
vote for a new leader before T + electionTimeout on its own tick clock, so
within that window no new leader can exist; `MaxOffset` absorbs modest
tick-rate skew.

Two hardenings close the sleeping-leader gap (a GC pause, cgroup
throttling, or VM freeze that stops the whole process): a **stall
detector** — the raft ticker notes when a gap far exceeds its interval and
invalidates all pre-stall follower contact, so a leader that just woke
cannot serve until a majority answers *again* — and a **post-evaluation
re-check** of the backstop immediately before read results are returned,
so a stall during evaluation cannot let pre-stall contact vouch for the
result. The failure mode of any false negative is only a NotLeader retry.
`--disable-lease-reads`-style configuration (`DisableLeaseReads`) restores
the v1 quorum path.

## Follower reads (closed timestamps)

Followers hold full copies of every range; closed timestamps let them
serve reads. The leader periodically (default every 1s) publishes "no
write at or below T will ever commit on this range", with T lagging now by
a few seconds (default 3s). The publication rides the **raft log itself**
as a tiny replicated command, which buys two properties for free:

- **Catch-up is log order**: by the time a replica applies the closed-ts
  command, every write below T has applied too — no applied-index
  bookkeeping, no side channel.
- **It survives leader failure**: the closed timestamp is replicated state
  (persisted in the replica state), so a follower keeps serving reads
  below T with the leader gone or an election in flight.

Publication is made safe by the same machinery ordinary reads use: the
leader takes a whole-range **shared latch** (draining in-flight writes,
whose exclusive latches are held until they apply — invariant L1), bumps
the timestamp-cache floor to T (every later write is forwarded above it),
releases, and proposes. A split hands the parent's closed timestamp to the
new right-hand range so nothing can write beneath reads the parent already
served on that span.

A follower serves a read-only batch pinned at a fixed timestamp exactly
when that timestamp is at or below its closed timestamp; anything else —
including any unresolved intent it encounters (only the leader can push) —
redirects to the leader as always. Pinned historical reads need no
uncertainty interval: nothing can commit at or below the read timestamp
anywhere. The staleness window is bounded below by the publication lag and
above by the GC TTL (reads at or below the GC threshold are rejected).
Clients opt in per statement with `AS OF SYSTEM TIME` (see docs/sql.md);
the gateway then prefers its local replica.

## Raft log truncation

The log is bounded by leader-driven, **replicated** `TruncateLog` commands
(v2). Each housekeeping tick the leader computes

```
truncIdx = leader's applied index − floor (64)
```

clamped to the index of any in-flight snapshot stream, and proposes a
truncation when at least 256 entries would be reclaimed. Each replica
deletes its own (unreplicated) log prefix when the command applies, at
which point it has durably applied everything at or below the index;
`TruncatedIndex/Term` persist atomically with the applied index, so
restarts resume from the truncation point.

A dead or lagging voter no longer pins the log: a voter that needs a
truncated entry is caught up by a raft snapshot instead (see the snapshot
section above), so the log stops growing during an outage and a returning
voter recovers via one snapshot stream plus the retained tail. The floor
bounds how often a barely-behind follower needs a snapshot rather than a
plain append.

## Size-based auto-splitting

Each range's approximate data size is **replicated state**
(`replicaState.SizeBytes`): every applied write adds a deterministic
per-command estimate, GC subtracts the exact bytes its enumerating leader
measured (carried in the GC command), and a split recomputes both sides
exactly from the staged state — so replicas always agree on the number.
When a led range exceeds `SplitSizeThreshold` (default 64 MiB) the
housekeeping loop splits it at the byte-midpoint clamped to a user-key
boundary, through the ordinary replicated split. A range mid-membership-
change fails the admin op safely and retries next tick. Splits also seed
the right-hand side's replicated state — including the **GC threshold it
inherits** from the left-hand side, since its keyspace was subject to the
same GC.

## Rack-aware placement

Nodes declare an ordered locality at startup:

```
datax start --locality=region=us-east,zone=b,rack=12 ...
```

The tiers are stored in the node registry and drive the **allocator**
(`pkg/placement`), used at upreplication and manual rebalance:

- Candidates: live nodes not already holding a replica of the range
  (hard constraint: at most one replica per node).
- Score: **diversity** — for each candidate, sum over existing replicas of the
  locality distance (number of tiers minus shared prefix length, normalized).
  Maximizing the sum spreads replicas across the widest failure domains:
  three racks beat two, two zones beat one.
- Tie-breaks: fewest ranges hosted, then lowest node ID (determinism for
  tests).

The effect: with three nodes in racks a/b/c, every range gets exactly one
replica per rack, so a rack failure costs at most one replica of any range —
quorum survives.

Zone/lease *preferences* (pinning leaseholders near clients) are parsed and
stored but not yet acted on automatically.

**Lease transfer** is `datax debug transfer-lease --range N --to nodeX`.
Leaseholder = raft leader in datax, so the transfer is raft's
`TransferLeadership`: the leader stops proposing, catches the transferee up,
and tells it to campaign. The new leader's timestamp-cache bump (any
leadership change) preserves read-your-writes across the hand-off, at the
cost of possibly restarting transactions in flight at that moment. A
transfer to a lagging replica that cannot complete within an election
timeout aborts with a retryable error. Transferring the lease first is how a
replica is moved *off* the range's own leader — `debug rebalance` does the
add → transfer → remove sequence automatically when the source leads.

**Rebalancing** is automatic: the allocator (the range-1 leader's loop, the
same one that upreplicates and repairs) watches per-node range counts across
live nodes — empty spares included — and when the spread between the most-
and least-loaded node reaches the rebalance threshold (default 2), it moves
one replica per tick from a fullest node to the emptiest via the same
add → transfer-if-leading → remove sequence. A threshold of 2 is the
hysteresis: one move narrows the spread by 2, so a cluster balanced to a
spread of ≤ 1 never moves anything and oscillation is impossible. Moves are
diversity-non-regressing — a move that would lower a range's
failure-domain spread is skipped even when counts favor it, so
one-replica-per-rack placement always survives balancing. The pass stands
down entirely while any node is dead (repair has priority), and a range
left over-replicated by an interrupted move is trimmed back first.
`datax debug rebalance --range N --to nodeX [--from nodeY]` remains for
manual moves; `Config.RebalanceThreshold` < 0 disables the automatic pass.

**Load- and byte-weighted balancing** runs behind the count pass, at most
ONE balancing op per tick across all three passes (acting on statistics a
just-performed move already invalidated only causes churn). Every node
advertises load aggregates through its registry heartbeat — total
leader QPS over *mature* trackers, leader count, total replica bytes, and
its top-8 hot and big ranges (`kvpb.NodeDescriptor`, from
`Store.LoadSummary`). Two passes act on them:

- **Lease shedding**: a node whose leader QPS exceeds the live-set mean
  by the shed factor (default 1.5×, with a 100 QPS absolute spread floor
  so idle clusters never shuffle) transfers the lease of an advertised
  hot range to the coolest live node already holding a replica —
  membership unchanged, no data moves, no diversity question. A transfer
  must *shrink* the imbalance: handing the hot lease to a node that
  would end up hotter than the source is refused, which is what stops a
  single dominant range from ping-ponging.
- **Byte rebalancing**: when counts are level but the replica-byte
  spread exceeds the threshold (default 64 MiB, with a 20%-of-mean
  floor), the biggest advertised range moves off the fullest node to the
  emptiest, diversity-gated like every replica move.

QPS is leader-local and resets to zero for a full measurement window
after every transfer, so both passes stamp a per-range cooldown (default
60s, deliberately longer than the maturity window) before acting — the
anti-oscillation story, on top of the projection check and the absolute
floors. `--lease-shed-factor` and `--rebalance-bytes-threshold` tune the
triggers (negative bytes threshold disables byte moves).
