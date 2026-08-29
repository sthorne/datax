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
transactionally. Range merges are future work.

## Peer discovery

Range addressing lives in the KV layer, but node *addresses* deliberately do
not depend on it — two mechanisms break what would otherwise be a
circularity (an election needs peer addresses; addresses in a registry
range; the registry range needs an election):

- **Address piggybacking**: every Raft envelope carries the sender's node
  ID and RPC address; receivers learn peers from Raft traffic itself.
- **Persisted registry**: each node saves its last known node registry to a
  local store key and reloads it at startup, so a fully restarted cluster
  can re-form with no leader anywhere and no `--join` flags.

The registry rows in range 1 (with localities and liveness) remain the
authority for the allocator; these mechanisms only guarantee reachability.

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

Repairing a dead voter away is also what un-pins Raft log truncation.

**Known gaps**: no node decommission UX (a stated limitation, not an
accident).

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

## Adding a replica: snapshots

A new replica is seeded by streaming a snapshot (the range's data span +
descriptor + applied index) over gRPC **before** the ConfChange commits
("preseed"), avoiding the stall where a new member can't vote until it gets a
snapshot through Raft. Configuration changes happen one replica at a time (no
joint consensus in v1).

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

## Raft log truncation

The log is bounded by leader-driven, **replicated** `TruncateLog` commands
(v2). Each housekeeping tick the leader computes

```
truncIdx = min(every voter's Match, leader's applied index) − floor (64)
```

and proposes a truncation when at least 256 entries would be reclaimed.
`Match` is what a voter has *durably appended*, so the invariant is: **no
live voter ever needs a truncated entry** — including across elections,
since any electable peer's log already covers the truncated prefix. Each
replica deletes its own (unreplicated) log prefix when the command applies,
at which point it has durably applied everything at or below the index;
`TruncatedIndex/Term` persist atomically with the applied index, so
restarts resume from the truncation point.

A dead voter's `Match` freezes and **pins truncation** — deliberate, since
datax has no raft-internal snapshot delivery: the log must stay sufficient
to catch up any configured voter. Dead-node repair (removing the dead
replica) is what unpins it.

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

**Rebalance** is manual: `datax debug rebalance --range N --to nodeX
[--from nodeY]` performs add-then-remove (with a lease transfer in between
when the source is the leader). There is no automatic rebalancing loop
(only the RF-repair upreplication loop above).
