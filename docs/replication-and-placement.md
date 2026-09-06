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

**Durability contract** (the classic etcd-raft rules, enforced when a
Ready is handled):

1. Persist HardState + new entries (synced batch) **before** sending any Raft
   messages from the same Ready.
2. Apply committed entries and advance the stored applied index in **one**
   Pebble batch, so replay after a crash is idempotent (entries at or below
   the applied index are skipped).

**The store's raft scheduler** (`pkg/kvserver/scheduler.go`) drives every
group on a node from one fixed pool of workers (`GOMAXPROCS`;
`StoreConfig.RaftWorkers`) instead of a goroutine and a ticker per
replica. A message, a proposal, a read-index request or the store's one
100 ms ticker *enqueues* a replica; a worker takes a group of queued
replicas (up to 64), ticks or steps each `RawNode`, and handles one Ready
per replica. Their HardStates and entries are staged into **one Pebble
batch and synced once** — group commit: ten ranges appending in the same
moment cost one fsync — before any of them sends a message or satisfies
a read-index waiter; then each is advanced, and one that still has work
is re-queued so every range gets a turn. A replica is never handled by
two workers at once (work arriving during its pass only raises its
flags). The scheduler's queue wait is
`datax_raft_scheduler_latency_seconds`; `datax_raft_log_syncs_total` and
`datax_raft_readies_per_sync` show how many replicas each sync served.
`PreVote` and `CheckQuorum` are enabled.

**Pipelined apply** (issue #106). Committed entries are not applied
inside the pass that committed them: the pass hands them to a second
pool of **apply workers** (as many as raft workers), which drain each
replica's queue in log order, one worker per replica at a time. A
range's next append and sync therefore proceed while its previous
entries apply, and a slow apply on one range never holds up the group
commit of the others. Applying stays what it was — evaluate, then the
MVCC effects and the applied index in one state-engine batch — and the
proposer is answered when its entry has applied. Two things stay inside
the pass: a Ready whose committed entries include a **conf change**
applies them inline after draining the queue (raft learns of the change
through `ApplyConfChange`, which must precede `Advance` admitting the
next one), and a replica whose queue is over its bound (64 MiB of
entries) gets no pass until an apply run drains it
(`datax_raft_apply_backpressure_total`), so a follower whose apply falls
behind its leader holds a bounded backlog. `datax_raft_apply_seconds` is
the per-entry apply time and `datax_raft_entries_applied_total` /
`datax_raft_entries_appended_total` the rates; a leader's quiescence
check reads the replica's own applied index, since raft's advances when
entries are handed over.

**The cost of a write at apply.** Each MVCC write looks at what it lands
on — an intent to conflict with, or the newest committed version to
stay above — with one bounded seek at the key's MVCC prefix on an
iterator the batch keeps for all its writes (`Batch.writeState`); an
iterator per key, with the stack of memtable and sstable iterators
behind it, was most of what a write cost (a 100-row one-phase commit
applied in ~1 ms before, ~0.4 ms after).

**Coalesced heartbeats and quiescence** (cluster version v12;
`pkg/kvserver/quiesce.go`). Heartbeats are the one cost that scales with
the number of ranges whatever the workload, so two things make it a
constant per peer node. A heartbeat or a heartbeat response is queued
per destination and every scheduler pass flushes the queue as *one*
envelope carrying every range's heartbeats (`RaftHeartbeat`: the five
fields raft reads); the receiver fans them out. And a leader that has
seen no proposal, read-index request or snapshot for 2 s, with every
follower holding its whole log and having answered within the lease
window, tells its followers it is going idle (a heartbeat with the
`quiesce` flag) and stops ticking; a follower that holds the leader's
commit index stops ticking too. Nothing is sent for an idle range and no
election timer runs — a quiescent follower cannot campaign, which is
what keeps the leader's lease reads safe once contact is re-established.
A replica wakes on any raft message but a heartbeat response, on a
proposal, read-index or leadership request, and on a client request
landing on it (so a follower asked for a range whose leader is gone
ticks, times out and campaigns). A woken leader heartbeats at once and
its lease backstop forgets pre-sleep contact, so the first read after a
long idle waits one round trip instead of trusting stale answers. An
unreachable follower keeps its range awake: it is not idle, and its
return would wake everyone anyway. `/status` reports `quiescent` per
range; `datax_quiescent_ranges`, `datax_raft_quiesces_total`,
`datax_raft_unquiesces_total`, `datax_raft_heartbeat_envelopes_total`
and `datax_raft_heartbeats_coalesced_total` count the effect. Both stay
off until the cluster finalizes v12: a v11 node reads neither.

**Transport**: one gRPC bidirectional stream per node pair; each message is
`{RangeID, To, From, opaque raftpb bytes}`. Raft messages are opaque to the
transport.

**Reads** are linearizable via ReadIndex on the leader (see
[architecture.md](architecture.md)). v1 has no separate lease mechanism:
leaseholder = Raft leader.

**Splits** happen automatically by size (v2; see below) or manually
(`datax debug split <key>`): the split is proposed as a replicated command;
at apply time each replica atomically writes both descriptors and creates
the right-hand side's Raft state, and bumps the RHS's timestamp-cache
floor to now(): the parent served reads on that span up to this moment,
and only those at or below its closed timestamp travel in the trigger,
so without the bump a write could land on the fresh RHS beneath a read
already served (issue #134; the one-time push this costs a transaction
that began before the split is a leadership change's). No data moves,
because range membership of a key is logical. The `/meta/` addressing records are then repaired
in one batch, after the split commits and outside its latch. Repairs
from a split and a merge on the same key lineage can land in either
order, so each is **ordered by descriptor generation** (cluster version
v4, `UpdateMetaRequest`): a record is replaced only by a newer
generation, and a merge's delete of the old left-hand record applies
only while that record still names the range it meant to remove — a
late repair can no longer resurrect a range that no longer exists and
send every lookup into "no replica" until it gives up.

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
detector** — the scheduler's tick notes when a gap far exceeds its interval and
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

A range whose log has not grown since its last logged promise publishes
the next one **off the log** (v12): the same latch drain and cache bump,
but the promise travels in the coalesced heartbeat envelope with the
leader's term and last log index, and a follower honors it only while it
still follows that leader at that term and has applied that index —
raft's vote lease means no other leader can have committed anything it
has not heard of meanwhile, which is what log order gave the replicated
path. The value lives in memory only (never persisted or checksummed;
a restart falls back to the last logged promise and re-learns within a
publication interval), so an idle range keeps serving fresh follower
reads without a raft entry, an fsync or a wake every second — which is
what lets it stay quiescent. The first promise after new entries rides
the log again.

For **quiescent** ranges the off-log promise is grouped, so an idle
store's publication cost is a few envelopes a second however many
ranges it holds: a sleeping leader's term and last index cannot change,
so it registers once per follower node (a per-range entry with an
explicit promise), and from then on each round sends every follower
node one group entry — "every range you hold registered from me is
closed at T" — which the follower applies to its registry, re-validating
each range and dropping one that fails. A range that wakes is dropped
from the group by a per-range entry ahead of the next group promise, and
until the followers see it the leader honors every promise they may
still apply by forwarding its own timestamp-cache floor to the store's
latest promise as it wakes (the promise is advanced before the woken set
is collected, so a wake either lands in the set or bumps to the promise
being sent). `datax_closed_timestamp_side_updates_total` counts the
per-range entries, `datax_closed_timestamp_group_updates_total` the
group ones.

A follower serves a read-only batch pinned at a fixed timestamp exactly
when that timestamp is at or below its closed timestamp; anything else —
including any unresolved intent it encounters (only the leader can push) —
redirects to the leader as always. Pinned historical reads need no
uncertainty interval: nothing can commit at or below the read timestamp
anywhere. The staleness window is bounded below by the publication lag and
above by the GC TTL (reads at or below the GC threshold are rejected).
Clients opt in per statement with `AS OF SYSTEM TIME` (see docs/sql.md);
the gateway then prefers its local replica.

**Bounded staleness** (`AS OF SYSTEM TIME with_max_staleness('10s')`)
turns the choice around: instead of the client naming a timestamp and
hoping followers have closed past it, the client names the staleness it
tolerates and the gateway picks the freshest timestamp its own replicas
can serve. The gateway computes

```
ts = max(now − bound, min over local replicas of ClosedTimestamp)
```

taking the minimum over the store's replicas that have a **non-empty**
closed timestamp — a replica that has never applied a closed-ts command
can't serve any follower read, so letting it pin `ts` to zero would make
every read maximally stale for nothing; those ranges simply fall back. A
gateway with no replicas at all reads at `now − bound`. The result is one
fixed timestamp for the whole statement (multi-range results stay
transactionally consistent — same MVCC snapshot everywhere), never staler
than the bound, and as fresh as local serving allows: when the local
replicas have closed past `now − bound`, the read is exactly a
`now − bound` read; when they lag less than the bound, the read tracks
their edge and is served entirely locally, leaders unreachable or not.

Per range, serving is all-or-nothing at that timestamp: a range whose
local replica has closed past `ts` is served locally; one that hasn't —
or that the gateway holds no replica of — goes to its leader through the
ordinary NotLeader retry path. Whether the gateway holds a replica is
answered by the **store**, not the routing cache: a node that joined an
existing cluster cached the pre-upreplication descriptor at join, and
nothing refreshes a descriptor that keeps routing to the leader
successfully — trusting it would hide the local replica forever (and did,
before the store became the authority; the store's descriptor also
repairs the cache when consulted).

Two counters tell the story. `datax_follower_reads_total` (server-side)
counts reads a replica served **as a follower**. Its counterpart
`datax_follower_read_fallbacks_total` (gateway-side) counts ranges a
stale read could *not* serve locally — no local replica, or a local
replica that answered NotLeader. The two are asymmetric on purpose: a
stale read served by a local replica that happens to be the **leader**
increments neither (it was served locally, but not as a follower, and it
wasn't a fallback). A rising fallback rate means the bound is tighter
than the local closed-timestamp lag, or the gateway's store simply
doesn't hold the ranges being read.

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

On a **split store** (v13, issue #105; `pkg/kvserver/raftengine.go`) the
log lives on a raft engine of its own and the state engine has no
write-ahead log, so "applied" is not "durably applied": a state-engine
write reaches disk when its memtable flushes. The truncation therefore
applies like any command but **defers** its deletion: each replica
remembers the pending index and deletes the entries at or below it — and
writes the truncated state to the raft engine's own key — only once the
state engine's flushed sequence number (Pebble's flush watermark) covers
the batch that applied that index; the apply path and the housekeeping
tick both check. Until then a crash could still need those entries: the
replica restarts at its last flushed applied index and raft re-delivers
the committed entries above it. The rare structural changes — a merge
absorbing its RHS, a replica removed from its range, a catch-up
snapshot replacing a state — flush the state engine before touching the
raft engine, so the two never disagree about which replicas exist in a
way replay cannot repair; raft state a crash orphaned on the raft engine
is swept at startup when the state engine holds the range's tombstone,
and kept otherwise (an RHS whose split the LHS is about to replay). A
clean shutdown flushes the state engine, so a normal restart replays
nothing; `datax_raft_replayed_entries_total` counts what a crash cost,
`datax_raft_deferred_truncations_total` the truncations that landed.

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

QPS is leader-local, but a lease transfer no longer zeroes the view: the
outgoing leaseholder proposes a **load handoff** through the raft log
just before transferring (`raftCommand.Load`), every replica stores it,
and whichever replica next becomes leader seeds its tracker from it (if
fresh — 30s) as an immediately *mature* rate that decays over one window
unless real traffic sustains it. The split-key reservoir still starts
fresh (samples don't transfer), so both passes keep the per-range
cooldown (default 60s) — the anti-oscillation story, on top of the
projection check and the absolute floors. `--lease-shed-factor` and
`--rebalance-bytes-threshold` tune the triggers (negative bytes
threshold disables byte moves).

## Region-restricted replication (per-database placement)

Rack-aware placement above answers "spread these replicas as widely as
possible". A **placement policy** answers a different question: "keep
this database's data in these places, and only these". It is a property
of a database, inherited by its tables, applied to every range of those
tables:

```sql
CREATE DATABASE eu WITH (replicas = 3, constraints = ('region=eu-west-1', 'region=eu-central-1'));
ALTER DATABASE eu SET (constraints = ('region=eu-west-1'));
ALTER DATABASE eu SET (replicas = 5);          -- the constraints are left alone
ALTER DATABASE eu SET (constraints = ());      -- lift the restriction
SHOW PLACEMENT FOR DATABASE eu;
```

- **`constraints`** is a **disjunction**: a replica may live on any node
  whose locality carries any one of the listed `key=value` tiers. Naming
  two regions means "either of these", which is how a policy spans a
  pair of regions without pinning to one.
- **`replicas`** overrides the cluster's replication factor for this
  database alone. It must be odd (a majority of an even count tolerates
  no more failures than the odd count below it) and at most 9.
- An option not named by an `ALTER` is left as it was, so the replica
  count and the constraints are set independently. `constraints = ()` is
  how a restriction is lifted; there is no separate `RESET`.
- `SHOW PLACEMENT` (bare, for the session's database, or
  `FOR DATABASE name`) reports the count the allocator will actually
  use, so a database with no policy of its own shows the cluster default
  and says where that number came from.

Writing a policy needs cluster version **v16** and the database's
ownership (or admin). It is a catalog fact: the statement moves no data.

### How a range finds its policy

A range belongs to the table its start key names, a table belongs to a
database, and the database carries the policy. Every node keeps that
table → policy map beside its schema cache (`pkg/server/placement.go`),
rebuilt by the same catalog scan that names ranges for the console, so
the allocator resolves a policy with a map lookup rather than a catalog
read on every tick. A range that resolves to nothing — a system range, a
meta range, a table whose database has no policy, or anything at all
before the first catalog scan lands — gets the zero policy, which is
exactly the pre-v16 behaviour: the cluster default factor, any node.

### What the allocator does with it

Every pass that chooses a node consults the policy
(`placement.AllocateTargetFor`): candidates the policy does not admit are
dropped **first**, and the diversity score is then maximized within what
is left. A database pinned to one region still spreads its replicas
across that region's racks. Up-replication, dead-node repair, decommission
drain and both rebalancing passes all use it, and all use the policy's
replica count in place of the cluster default.

One pass is new. **Placement enforcement** moves a replica that a policy
does not admit onto a node that does — add, transfer if leading, remove,
one range per tick, and only while the range is otherwise healthy. It is
what makes `ALTER DATABASE ... SET` take effect on data that already
exists: a range whose replicas are merely in the wrong region is not
short a replica, has no dead one and is not on an overloaded node, so no
other pass would ever touch it. It runs after repair and drain and ahead
of the load passes: a replica in the wrong region is wrong, a replica on
a busy node is merely expensive.

### When a policy cannot be met

If no live node satisfies the policy, the allocator **does nothing** —
it never places a replica outside a region an operator named, and it
never drops a replica to satisfy one. The data stays where it is and the
condition is reported instead:

- `placement-unsatisfiable` (critical) — a range names a placement no
  live node satisfies. Typically a region's nodes are all down, or a
  constraint names a locality tier no node declares.
- `placement-misplaced` (warning) — replicas are outside the policy but
  a home exists; the enforcement pass is moving them, one per pass.

Both appear in `/api/health` and on the console's health view. The
counter `datax_placement_replicas_moved_total` tracks the moves.

A policy asking for more replicas than the admitted set can hold (three
replicas, two nodes in the region) is a stored, legal policy that simply
cannot converge: the range keeps its third replica outside the region and
reports `placement-unsatisfiable` until a node joins there. That is the
deliberate choice — a policy is a restriction on where data may live, and
the allocator would rather leave a replica in the wrong place, loudly,
than move data somewhere the operator excluded.

## Replica consistency checking

Replicas of a range are byte-identical by construction: they apply the
same log prefix, and even GC replicates the exact versions it deletes.
The consistency checker turns that invariant into a tripwire. The leader
proposes a checksum trigger through the log (`raftCommand.Checksum`);
each replica, on applying it, captures an engine snapshot at exactly
that applied index and SHA-256s the range's replicated content (the
MVCC data span plus range-local transaction records) asynchronously.
The proposing node then collects the followers' digests over the admin
channel and compares: a mismatch is corruption — it logs every
replica's digest and increments `datax_consistency_failures_total`
(`datax_consistency_checks_total` counts probes).

The sweep is **off by default** (hashing a range reads all of it);
`--consistency-interval 10m` probes one led range per node per
interval, round-robin. A lagging replica that misses the collection
window is a liveness note, not a failure — the next probe retries it.
The fault-injection suite (`chaos_test.go`) runs the bank-invariant
workload through partitions, crashes, and injected storage overload and
ends every scenario with a consistency probe.
