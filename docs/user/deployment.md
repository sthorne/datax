# Deployment

## Starting a cluster

The first node bootstraps the cluster with `init`; every other node joins it
with `start --join`:

```sh
# Node 1 (bootstraps, then serves):
datax init  --dir /var/lib/datax --listen 10.0.0.1:26257 --pg-listen 10.0.0.1:26433 \
            --locality region=eu,rack=a --http-listen 10.0.0.1:8080

# Nodes 2..N (any existing node's RPC address works as --join):
datax start --dir /var/lib/datax --listen 10.0.0.2:26257 --pg-listen 10.0.0.2:26433 \
            --locality region=eu,rack=b --http-listen 10.0.0.2:8080 \
            --join 10.0.0.1:26257
```

`init` and `start` accept the same flags; `init` additionally bootstraps the
cluster on first run. Defaults: RPC `:26257`, SQL `:26433`. An empty
`--dir` runs in-memory (data lost on exit) — fine for tests, not for
deployment. Node identity lives in the data directory, so a node restarted
with the same `--dir` rejoins as itself even if its address changed
(`--advertise` controls the address other nodes use to reach it, when the
listen address isn't routable).

With three or more nodes up, every range is replicated 3×. Until then the
cluster still works, just under-replicated; ranges up-replicate
automatically as nodes arrive.

## Localities

`--locality` declares the node's failure-domain tiers, outermost first:

```
--locality region=eu,rack=a
```

The allocator maximizes diversity across the **last** tier boundary it can:
with one replica per rack, losing a rack loses at most one replica of any
range. Give every node the same tier names in the same order. Leaving
localities off works, but placement then has no failure domains to spread
across.

## Clocks

Nodes tolerate at most `--max-offset` (default `500ms`) of clock skew; a
node that drifts past it shuts down rather than risk consistency. Run NTP
(or equivalent) on every machine. Transaction timestamps come from hybrid
logical clocks, so ordinary skew below the limit is handled transparently.

## Storage profiles

`--storage-profile` tunes the storage engine:

| Profile | Use when | Trade-off |
|---|---|---|
| `balanced` (default) | mixed read/write workloads | Pebble's flush and compaction defaults; write-heavy loads accumulate compaction debt and throughput sags |
| `ingest` | sustained bulk loading | bigger memtables, earlier/more parallel compaction — measured ~10.2k rows/s steady vs ~8k declining on `balanced` for the batched-ingest benchmark, with better read p99 |

Both profiles share the read-path settings every store wants: a block
cache sized from the machine's memory (25 % capped at 8 GiB for
`balanced`, 10 % capped at 2 GiB for `ingest`; `--cache-size 2GiB`
overrides it — watch `datax_storage_block_cache_hits_total` against
`_misses_total` to size it), bloom filters on every level (a point read
for a key that is not there — the uniqueness probe on every `INSERT`,
the intent lookup before every write — skips the levels that cannot hold
it), the newest sstable format the bundled Pebble supports, and an
open-file budget of half the process's descriptor limit (1000 to
16384). The cache is one per process, shared by every store the process
opens.

The profile is per-node and can differ across restarts. Watch
`datax_storage_l0_files` / `datax_storage_compaction_debt_bytes` (see
[Operations](operations.md)) to tell when `ingest` is warranted. Details in
[docs/storage-profiles.md](../storage-profiles.md).

**Store layout.** From cluster version v13 a store is two Pebble engines:
the state machine directly under `--dir`, running without a write-ahead
log, and the raft log under `--dir/raft`, with one. Every replicated
write then reaches disk once — through the synced, group-committed raft
log — instead of twice (the log and the state engine's WAL), which is
what the raft log's durability guarantee makes safe: whatever a crash
takes from the state engine's memtable is replayed from the log on the
next start (a clean shutdown flushes it, so a normal restart replays
nothing). Both directories share the block cache, the profile and the
encryption key; back up, move or size the store as the one directory it
is. A store created by a v13 binary, or joining a v13 cluster, is laid
out this way from the start; an older store migrates on its first start
after the cluster finalizes v13 (see [Upgrading](#upgrading-a-running-cluster)).
`datax_storage_split` is 1 on a split store, and
`datax_storage_bytes_written_total{engine,kind}` shows what each engine
writes.

## Encryption at rest

Give the node a 32-byte key file (raw or hex) and everything it writes to
disk is encrypted:

```sh
head -c 32 /dev/urandom > /etc/datax/store.key
chmod 600 /etc/datax/store.key
datax init --dir /var/lib/datax --enc-key /etc/datax/store.key ...
```

The store key wraps per-file data keys; measured cost is ~13% on a mixed
workload. The store key rotates **online** — the node stays up and
serving:

```sh
datax debug rotate-enc-key --addr 10.0.0.1:26257 \
  --old-key /etc/datax/store.key --new-key /etc/datax/store-v2.key \
  --certs-dir certs [--user ops]     # admin role required
```

Rotation re-wraps the data keys atomically and re-seals the metadata
backup (fast — nothing is re-encrypted). Stage the new key first:
`--enc-key` takes a comma-separated list of key files and the node opens
with whichever one matches the store, so a node started (or set to
restart) with `--enc-key store.key,store-v2.key` survives a restart on
either side of the rotation; drop the old file afterwards. Without the
staging, a restart between the rotation and the key-file swap cannot
open the store. The request carries the store keys, so
online rotation is served only over mutual TLS (a secure cluster); on an
insecure cluster, and for damaged stores, use the offline form
(`--dir /var/lib/datax`, node stopped).

Files written under retired data keys are re-encrypted on demand:

```sh
datax debug reencrypt --addr 10.0.0.1:26257 --wait   # per node
```

paces compactions over stale-key sstables until
`datax_reencryption_remaining_bytes` reaches 0 — the attestation that no
live sstable remains under a retired key. `--wait` exits non-zero if the
worker stops with bytes remaining (files manual compaction cannot
rewrite; they retire with natural churn — re-run later) or the sweep
behind the count failed. Losing the key file means
losing the store — back it up separately from the data. Details in
[docs/encryption.md](../encryption.md).

## Tuning flags (rarely needed)

| Flag | Default | Effect |
|---|---|---|
| `--load-split-threshold` | 500 | sustained per-range QPS that triggers a load-based split (negative disables) |
| `--raft-workers` | one per CPU | workers driving the node's raft groups (the store scheduler's pool) |
| `--raft-quiescence` | true | let idle ranges stop ticking and heartbeating (cluster version v12); `false` keeps v11's steady heartbeats |
| `--merge-size-threshold` | 16 MiB | size below which a range and its right neighbor are merged back together (negative disables; `datax demo` takes it too — disable it to keep an empty pre-split for a benchmark) |
| `--lease-shed-factor` | 1.5 | leader-QPS multiple of the cluster mean at which a node sheds hot leases |
| `--rebalance-bytes-threshold` | 64 MiB | replica-byte spread that triggers byte-weighted replica moves (negative disables) |

The defaults are right for almost everyone; see
[docs/replication-and-placement.md](../replication-and-placement.md) before
touching them.

## Upgrading a running cluster

Rolling, no downtime: restart one node at a time on the new binary, wait
until `datax debug nodes` shows it healthy on the new version, and when
every node is upgraded, finalize deliberately with `datax debug upgrade`.
Before finalize you can roll any node back; after it, never. Full
procedure and rules: [Operations → Rolling
upgrades](operations.md#rolling-upgrades).

Upgrading to v13 has one extra step: after the finalize, restart each
node once more (rolling, as before). That restart migrates the node's
store to the split layout above — its raft state moves to `--dir/raft`,
bounded by the log size that truncation keeps small, and the state
engine drops its WAL. The migration records itself in the store, which a
v12 binary then refuses to open: it is the one upgrade step that cannot
roll back, so finalize v13 only once the upgraded cluster looks healthy.

Finalizing v14 needs no restart: within a heartbeat every node moves both
of its engines to Pebble's columnar-block sstable format (format major
version 19) online, and new sstables — flushes, compactions — use it
from then on. A v13 binary bundles a Pebble that does not know the
format, so, as with v13, a node cannot go back to it after the finalize;
`datax debug nodes` shows each store's format (`store_format`).

## Checklist for production deployments

- One `datax` process per machine, `--dir` on its own disk (NVMe strongly
  preferred — every write commits through an fsync'd raft log).
- `--locality` set consistently on every node; at least 3 failure domains.
- NTP running everywhere.
- [Secure mode](security.md): certs dir + `--root-password` on first init.
- `--http-listen` set, Prometheus scraping `/metrics` on every node.
- The key file (if encrypting) backed up somewhere that is not the data disk.
- Upgrades: same binary version on every node at bootstrap; upgrade
  rolling, one node at a time, and treat `datax debug upgrade` as the
  point of no return.
- Stop nodes with `SIGTERM` (systemd's default) and give the unit a
  `TimeoutStopSec` above `--drain-timeout` (10s): the node hands its
  leases to peers and finishes SQL connections before it exits
  ([Operations → Stopping a node](operations.md#stopping-a-node)).
