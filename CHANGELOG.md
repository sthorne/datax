# Changelog

Releases of datax, newest first. The version is `pkg/version.Release`,
bumped in the pull request that changes behavior (minor for a new
capability, patch for a fix); a git tag `vX.Y.Z` on `main` marks the
release, and the build workflow stamps binaries with the tag or with
`vX.Y.Z+<commit>` between tags. The cluster protocol version (`v1`, `v2`,
... in `pkg/version`) is separate: it changes only when the replicated
state or the internode protocol does, and an entry below says so.

## 0.3.0 — unreleased

### Added
- Inter-node latency and clock offset: each node pings every peer every
  2 s (an NTP-style exchange yielding both the round trip and the peer's
  clock offset), advertises its row on the heartbeat, and the dashboard
  shows the whole matrix with offsets judged against `--max-offset`;
  `/metrics` gains `datax_rpc_rtt_seconds{peer}`,
  `datax_clock_offset_seconds{peer}` and `datax_peer_reachable{peer}`, and
  a node logs a warning once a peer's offset passes half the tolerance
  (#82). New internode RPC `Ping`; a node on an older binary answers
  "unimplemented" and reads as unreachable until upgraded.

## 0.2.0 — 2026-09-04

Cluster protocol v4 (ordered range-addressing repair). Binaries from
0.1.x can join a v3 cluster and are finalized to v4 with `datax debug
upgrade`.

### Added
- Machine-level metrics per node: each node samples its host (CPU, load,
  memory, the store disk's size, throughput and utilization, network,
  file descriptors, Go runtime) and advertises a summary on its
  heartbeat; the dashboard's Nodes table shows every node's figures with
  warning colors, a Machine section shows the local node in full, and
  `/metrics` exports `datax_node_*`, `datax_store_disk_*` and
  `datax_process_*` next to the standard Go and process collectors (#81).
- The dashboard shows who it is signed in as and, without the admin
  role, explains the range drill-down refusal in those terms (#79).
- `datax sql --certs-dir DIR --user NAME` connects with a client
  certificate, like `debug`, `backup` and `restore` (#77).
- Every CLI client reports progress while connecting, under a separate
  `--connect-timeout` (default 10 s), and names the address and cause on
  failure; `datax sql` previously had no connect timeout at all (#78).
- Cluster version v4: split and merge repair `/meta` with
  generation-ordered updates, so a late repair can no longer resurrect a
  stale record (#74).
- Staged store keys: `--enc-key old.key,new.key` lets a node start with
  either key after an online rotation; background re-encryption of files
  under retired keys with bounded chunks (#67, #69).
- `datax version` prints the release and the cluster protocol range the
  binary speaks.

### Fixed
- Online index builds could miss a row written by a gateway whose lease
  renewals had stalled: the descriptor cache now shares the lease
  record's expiration, transactions take the lease's expiration as a
  commit deadline, and the backfill's chunk reads cover the whole key
  span (#110).
- A merge that raced a split of its right-hand range absorbed the
  pre-split span and left two ranges claiming the same keys; the merged
  descriptor is now built from the right-hand range as it stands after
  the subsume, checked again at apply (#111).
- Meta lookups retry a record that does not cover the key (the transient
  state of an addressing repair) instead of failing the batch (#111).
- The intermittent re-shard stall: write evaluation reports every
  conflicting intent at once and the client pushes each blocker once;
  proposals orphaned across a coalesced leadership change are answered
  and re-sent (#74).
- Follower overload verdicts stay sticky until the follower reports
  healthy; overlapping store-key rotations are serialized; merge apply
  exits when the right-hand raft loop stops; replicas are all loaded
  before any raft loop starts at restart (#65, #66, #70).
- Re-encryption cost is bounded per chunk and reported per span; the
  cleartext-rejection assertion in the encryption tests is meaningful
  (#69, #71); wall-time comment, health cache atomics, debt-gate refresh
  (#72).

### Docs
- README states the cluster version and a Scope section in place of the
  prototype status; security, getting-started and operations guides
  cover certificate auth for `datax sql`, connection feedback, the
  signed-in badge and the host metrics.

## 0.1.0

The initial tree: MVCC storage over Pebble, Raft-replicated ranges with
splits and merges, serializable transactions with parallel commit,
rack-aware placement, secondary indexes, a cost-based planner, sharded
time-series tables with online re-shard, follower reads, encryption at
rest, TLS + SCRAM with admin authorization and audit logging, backup and
restore, rolling upgrades (cluster protocol v3), Prometheus metrics and
the dashboard.
