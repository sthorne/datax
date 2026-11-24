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
cluster on first run. Defaults: RPC `:26257`, SQL `:26433`. Without
`--dir` (or with an empty one) the node runs in-memory and its data is
lost on exit — fine for tests, not for deployment. Such a node prints a
warning banner to stderr, and a `WARN` log line, at startup, so a
service file that forgot `--dir` (or whose `--dir=$VAR` expanded to
nothing) is noticed before its first restart; `--in-memory` asks for an
in-memory node on purpose and silences the warning (it is refused
together with `--dir`). Node identity lives in the data directory, so a node restarted
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

### Locality addresses

`--locality-advertise` (alias `--locality-advertise-addr`, CockroachDB's
name) gives a node a second address for the peers that share one of its
locality tiers, typically a private network inside a region, while
`--advertise` stays the public address every other node uses:

```
--locality-advertise region=us-mia@192.168.144.22:26257
```

The value is a comma-separated list of `key=value@host:port` entries. Each
`key=value` must be one of the node's own `--locality` tiers, and each tier
may appear once. A malformed entry, a tier the node doesn't have, or a bad
`host:port` stops the node at startup with an error naming the flag.

When a node dials a peer, it uses the peer's first entry whose tier (the
same key **and** value) is also one of its own tiers, and the peer's public
address when no entry matches. This applies to every connection between
nodes: raft, snapshots, KV requests, the latency pings, and the admin and
re-announce calls. `--join` addresses are always dialed as given.

The node has to accept connections on that address, so its `--listen`
must cover it (for example `0.0.0.0:26257`). In secure mode, its node
certificate must also name the address's host (`datax cert create-node
--hosts`). If a dial to a locality address fails, the dialing node uses
the public address instead and logs a warning, at most once a minute per
peer. It tries the locality address again in the background every 10
seconds. So a misconfigured private network slows the cluster down but
doesn't partition it. `datax_rpc_locality_dials_total` and
`datax_rpc_locality_fallbacks_total` on `/metrics` count both outcomes.
The entries are shown wherever a node's address is: `datax debug nodes`,
the console's node list and node page, `/status`, and `/api/node`.

A two-region cluster with a public network between the regions and a
private one within each:

```sh
# us-mia: public 203.0.113.x, private 192.168.144.x
datax init  --dir /var/lib/datax --listen 0.0.0.0:26257 \
            --advertise 203.0.113.21:26257 \
            --locality region=us-mia,zone=a \
            --locality-advertise region=us-mia@192.168.144.21:26257
datax start --dir /var/lib/datax --listen 0.0.0.0:26257 \
            --advertise 203.0.113.22:26257 \
            --locality region=us-mia,zone=b \
            --locality-advertise region=us-mia@192.168.144.22:26257 \
            --join 203.0.113.21:26257

# us-atl: public 198.51.100.x, private 10.20.0.x
datax start --dir /var/lib/datax --listen 0.0.0.0:26257 \
            --advertise 198.51.100.31:26257 \
            --locality region=us-atl,zone=a \
            --locality-advertise region=us-atl@10.20.0.31:26257 \
            --join 203.0.113.21:26257
datax start --dir /var/lib/datax --listen 0.0.0.0:26257 \
            --advertise 198.51.100.32:26257 \
            --locality region=us-atl,zone=b \
            --locality-advertise region=us-atl@10.20.0.32:26257 \
            --join 203.0.113.21:26257
```

The two us-mia nodes reach each other over 192.168.144.x and the two
us-atl nodes over 10.20.0.x. Traffic between the regions, and every
`--join`, uses the public 203.0.113.x and 198.51.100.x addresses.

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
for a key that is not there — the uniqueness check on every `INSERT`,
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

To keep the store key off the node's disks altogether, start the node
with `--enc-root` in place of `--enc-key`: the node makes its own store
key and keeps it only wrapped by a *root* — a command you supply that
talks to a TPM, a cloud KMS or an HSM (`exec:<command>`), or a wrapping
key file kept elsewhere (`file:<path>`). Rotation then needs no key
file and sends no key over the network
(`datax debug rotate-enc-key --addr ...`). See
[encryption](../encryption.md#roots-where-the-store-key-lives).

A root can also be a key management service spoken to directly — no
helper command, and datax's own timeouts, retries, health check and
audit:

```sh
--enc-root 'vault:https://vault.example:8200/transit/datax?token_file=/run/vault/token'
--enc-root 'awskms:arn:aws:kms:eu-west-1:111122223333:key/1234abcd-…'
--enc-root 'gcpkms:projects/acme/locations/europe-west1/keyRings/datax/cryptoKeys/store-root'
--enc-root 'azurekv:datax-prod/store-root'
```

Options follow the key as `?name=value&name=value`; credentials are
never among them — they are a file an option names (a Vault token file,
a service-account key file), or the environment, or the instance's own
identity. What to know before choosing one:

- **A node cannot start while its KMS is unreachable** (it gives up
  after about 45 seconds and says which service and why). A running
  node is unaffected. Do not put the KMS behind the database.
- Each node checks its KMS every minute and exports
  `datax_enc_root_healthy`; alert on 0 — that node is serving and would
  not survive a restart. The console's security view and
  `/api/security` say the same.
- Grant each node encrypt and decrypt on the one key and nothing else;
  the minimum policy for each service is in
  [encryption](../encryption.md#a-root-in-a-key-management-service).
- Each wrapped store key is bound to its cluster and node, so one
  node's is refused as another's. Give each node its own credential
  where you can: that, not the binding, is what keeps a compromised
  node from asking the KMS for a neighbour's key.
- After the service rotates its key, run
  `datax debug enc-root rewrap --addr <node>` on each node; the
  console says when there is a newer version to move to.
- These roots are tested against fakes of each service's API and have
  not been run against the services themselves: prove the root on a
  scratch node first. For an HSM (PKCS#11), use `exec:<command>`.

To keep the root key off every machine, split it among people:
`datax debug enc-root split --shares 2/3` makes a root key, prints
three shares of which any two rebuild it, and keeps nothing. Nodes
started with `--enc-root shares` come up **sealed** — store closed, SQL
and console ports not open — until two share-holders have each run
`datax unseal --addr <node>` and typed their share. See
[a root key held as shares](../encryption.md#a-root-key-held-as-shares).

A node that restarts while the rest of the cluster is up needs nobody:
a running peer hands it the root key once it has been down longer than
the liveness grace (*peer unseal*; `datax debug peer-unseal --off`
turns that off, and then every start needs shares). Read
[what peer unseal gives up](../encryption.md#peer-unseal) before
relying on it: with it on, protect the node certificate's key as you
would the data.

*Runbook: cold start of a sealed cluster.* Start every node; each
prints that it is sealed. With peer unseal on, unseal one node as below
and the others follow by themselves; with it off, do this for each
node. Two share-holders run
`datax unseal --addr <node> --certs-dir certs` (the root user's client
certificate) and enter their share; `datax debug seal-status --addr
<node>` shows how many are in and whose. A node opens as soon as its
second share arrives; the cluster serves once a majority of nodes are
open. A refused share says why (mistyped, already submitted, another
cluster's) and does not count.

*Runbook: a lost share.* With a 2-of-3 split, one lost share costs
nothing yet, and a second lost share costs the root key — so act on the
first. If the share is merely lost, run `datax debug enc-root resplit
--shares 2/3` with two of the remaining holders and hand out the new
shares; destroy the old ones. If it may be in someone else's hands,
replace the root key instead: `split` a new one, have its holders
`datax unseal` each running node, then `datax debug enc-root rewrap
--addr <node> --to-root 2` on each; the old shares then open nothing.
Do this while the nodes are running: a stopped node whose root key
cannot be rebuilt cannot be opened.

An encrypted node keeps its keys in locked memory and turns core dumps
off for its process. Give the service a memory-lock limit of at least
16 KiB (`LimitMEMLOCK=64K` in a systemd unit is ample); with less, the
node warns at start and runs with its keys swappable. Add
`--require-locked-keys` to refuse to start instead, and
`--allow-core-dumps` only while debugging a crash. See
[keys in memory](../encryption.md#keys-in-memory).

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
| `--sql-max-message-size` | 256 MiB | largest single message an authenticated SQL client may send (a statement's text, a Bind's parameters, one CopyData chunk), e.g. `64MiB`; a longer one ends the connection with a FATAL `08P01` before its body is allocated. Accepted range 1 MiB to 1 GiB; PostgreSQL's own limit is 1 GB. A value is bounded well below either by the 60 MiB one replicated write carries, so raising it rarely helps; COPY's total size is not limited by it. Before authentication the limit is 10000 bytes regardless |

The defaults are right for almost everyone; see
[docs/replication-and-placement.md](../replication-and-placement.md) before
touching the replication ones.

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
from then on; transactions begun after the finalize also store their
intents and records in a compact binary encoding, while the ones from
before are read as they are until they resolve. A v13 binary bundles a
Pebble that does not know the format, so, as with v13, a node cannot go
back to it after the finalize; `datax debug nodes` shows each store's
format (`store_format`).

Upgrading to v15 is "finalize, then restart", like v13: a store at v15
opens with prefix bloom filters — Pebble's filters answer point reads
by user key, so a read of a key that is not there (the uniqueness check
of every `INSERT`, the intent lookup before every write) skips the
sstables that hold nothing of it — and the switch is a property of how
the store is opened, so each node picks it up at its next restart after
the finalize (the node logs that it will, and `datax debug nodes` shows
`store_prefix_bloom`). Until a node restarts it serves exactly as at
v14. After the switch the node rewrites the sstables written before it
in the background, paced like re-encryption, so the cold bulk of the
store gains the filters too (`prefix_bloom_rewrite` in the node
document, `datax_prefix_bloom_remaining_bytes` on `/metrics`); reads are
correct throughout, merely without the skip on tables not yet rewritten.
A v14 binary cannot open a store that has run at v15.

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
