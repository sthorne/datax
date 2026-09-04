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
| `balanced` (default) | mixed read/write workloads | Pebble defaults; write-heavy loads accumulate compaction debt and throughput sags |
| `ingest` | sustained bulk loading | bigger memtables, earlier/more parallel compaction — measured ~10.2k rows/s steady vs ~8k declining on `balanced` for the batched-ingest benchmark, with better read p99 |

The profile is per-node and can differ across restarts. Watch
`datax_storage_l0_files` / `datax_storage_compaction_debt_bytes` (see
[Operations](operations.md)) to tell when `ingest` is warranted. Details in
[docs/storage-profiles.md](../storage-profiles.md).

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
