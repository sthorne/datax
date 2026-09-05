# datax

**datax** is an open-source, distributed, ACID-compliant SQL database in the
spirit of CockroachDB: data is synchronously replicated across nodes with the
Raft consensus protocol, sharded into ranges for horizontal scale, and placed
**rack-aware** so replicas spread across failure domains (region / zone / rack)
while staying close to each other.

It speaks the **PostgreSQL wire protocol**, so any Postgres client or driver
works out of the box — `psql`, [pgx](https://github.com/jackc/pgx), or
`database/sql`.

> **Status: cluster version v4.** MVCC storage with garbage collection
> and row-level retention, Raft-replicated ranges with log truncation and
> automatic splitting and merging by size and load, serializable
> distributed transactions with read refresh and one-phase commit,
> rack-aware placement with dead-node repair and load rebalancing,
> secondary indexes, a cost-based planner over table statistics, sharded
> time-series tables with online re-sharding, follower reads, encryption
> at rest with online key rotation, mutual TLS + SCRAM with admin
> authorization and audit logging, consistent backup/restore, rolling
> upgrades, and Prometheus metrics with a built-in dashboard. Every push
> runs the full race suite in CI. What is deliberately out of scope is
> listed under [Scope](#scope).

## Architecture at a glance

```
        SQL clients (psql, pgx, database/sql, ...)
                       │  PostgreSQL wire protocol
                ┌──────▼──────┐
                │   pgwire    │  connection handling, auth, encoding
                ├─────────────┤
                │     sql     │  parser, catalog, row encoding, execution
                ├─────────────┤
                │  kvclient   │  txn coordinator, range routing (DistSender)
                ├─────────────┤
                │  kvserver   │  one Raft group per range, timestamp cache,
                │             │  txn records, splits, replica placement
                ├─────────────┤
                │   storage   │  MVCC over Pebble (LSM), write intents, HLC
                └─────────────┘
```

- **Storage**: [Pebble](https://github.com/cockroachdb/pebble) LSM with
  multi-version concurrency control. Every value is versioned by a hybrid
  logical clock (HLC) timestamp. Per-node tuning profiles
  (`--storage-profile balanced|ingest`); **encryption at rest**
  (`--enc-key`: per-file data keys sealed under a store key, online store-key
  rotation with the new key staged beside the old — `--enc-key old.key,new.key`
  — and background re-encryption of files under retired keys); and write
  **backpressure** that sheds table-data writes with a retryable error when
  the leader's engine, or any follower's, is overloaded, plus a latched
  compaction-debt gate with hysteresis.
- **Replication**: the keyspace is split into **ranges**; each range is an
  independent [etcd Raft](https://github.com/etcd-io/raft) consensus group
  (multi-raft). Writes commit only once a quorum has them on disk.
- **Transactions**: CockroachDB-style — write intents plus a transaction
  record whose single atomic flip commits the whole transaction, no matter how
  many ranges it touched. Serializable isolation. Multi-range writes fan
  out to their ranges in parallel; a single-range implicit transaction
  commits in **one raft proposal** (one-phase commit — no record, no
  intents). Current-time reads are served by the leader (lease-based
  ReadIndex); `AS OF SYSTEM TIME` — an exact timestamp or
  `with_max_staleness('10s')` — is served by any replica whose closed
  timestamp covers it (**follower reads**).
- **Placement**: nodes declare a locality (`--locality=region=r1,rack=a`);
  the allocator maximizes diversity across failure domains, so losing a rack
  never loses more than one replica of a range.
- **SQL**: a deliberately small subset (DDL incl. secondary indexes,
  ALTER TABLE with RENAME / SET DEFAULT / online ALTER COLUMN TYPE, TRUNCATE, views, CREATE TABLE AS / LIKE, COMMENT ON, sequences, SERIAL / identity columns, expression
  defaults, CHECK / UNIQUE / FOREIGN KEY constraints with cascading
  actions, INSERT/SELECT/UPDATE/DELETE with ORDER BY — DESC via
  reverse scans — the usual scalar functions, operators and casts
  ([reference](docs/user/functions.md)) and aggregates, transactions,
  joins up to 8 tables,
  GROUP BY — including over joins, correlated subqueries to 4 levels,
  UNION, `COPY FROM STDIN`, EXPLAIN, and ANALYZE / SHOW STATS feeding a
  cost-based planner — a background sampler keeps statistics fresh) over
  ten column types including exact DECIMAL and JSONB with `->`/`->>`
  extraction and `@>` containment, served over the Postgres wire protocol
  with TLS + SCRAM-SHA-256 authentication in secure mode; databases,
  the `pg_catalog` / `information_schema` views and the `SHOW` family,
  so `psql`'s `\d` commands and ORM introspection work as they do
  against PostgreSQL.
- **Time-series tables**: `CREATE TABLE ... WITH (timeseries = true,
  retention = '7d', shards = 8)` — age-based expiry with no SQL DELETEs,
  a hidden hash-shard column that spreads the write hot tail across
  ranges, ordered fan-out across shards, and an **online re-shard**
  (`ALTER TABLE ... SET (shards = 16)`) that rebuilds the layout and its
  secondary indexes under live ingest, keeping the retired layout for
  historical reads until a janitor reclaims it.
- **Operations**: leader-driven housekeeping per range — MVCC garbage
  collection, raft log truncation, splitting and merging by size and
  load — plus dead-node repair, load rebalancing (count, bytes, and lease
  shedding), decommission, a paced replica consistency sweep, consistent
  cluster backup/restore (full + incremental, `datax backup` /
  `datax restore`), rolling upgrades with an explicit finalize
  (`datax debug upgrade`), the `datax debug` toolbox (split, merge,
  transfer-lease, rebalance, status, metadata, unsafe-recover,
  rotate-enc-key, reencrypt), `datax bench`, and a built-in
  observability dashboard with `/metrics` + `/status` + `/api/cluster` +
  `/api/range` endpoints (`--http-listen`; the dashboard at `/` is
  read-only and self-contained, with cross-node range drill-down over
  internode RPC).
- **Security**: `--certs-dir` turns on mutual internode TLS and SQL TLS +
  SCRAM. Internode RPCs accept only the node certificate; the admin RPC
  authenticates operators by client certificate, and state-changing ops
  (and the dashboard drill-down) require the admin role (`root`, or a
  member of `admin`). SQL authorization is PostgreSQL's role model:
  roles as groups with inheritance and `SET ROLE`, object ownership,
  grants on tables, sequences, databases and the schema (`ALL TABLES`,
  default privileges, `GRANT OPTION`, `PUBLIC`), and the built-in
  `admin`, `read_all`, `write_all` and `metrics` roles. Every HTTP
  endpoint takes HTTP Basic credentials of a database user — Prometheus
  `basic_auth` with the `metrics` role — or a client certificate.
  Authentication failures, denied and executed admin ops, and role and
  privilege DDL are audit-logged with their principal. Insecure mode
  stays open, like pgwire trust auth.

**User documentation** — installing, deploying, securing, and operating a
cluster, plus the SQL reference and a differences-from-PostgreSQL guide —
lives in [`docs/user/`](docs/user/).

Design documents live in [`docs/`](docs/):
[architecture](docs/architecture.md) ·
[transactions](docs/transactions.md) ·
[replication & placement](docs/replication-and-placement.md) ·
[sql](docs/sql.md) ·
[storage profiles](docs/storage-profiles.md) ·
[encryption](docs/encryption.md) ·
[time-series tables](docs/timeseries.md)

## Quickstart

```sh
go build -o datax ./cmd/datax
```

Prebuilt binaries on demand: the **build** GitHub Actions workflow
(Actions tab → build → Run workflow, any branch) verifies the tree and
cross-compiles `datax` for linux/darwin/windows on amd64+arm64, attaching
each as a downloadable artifact stamped with the release (`datax
version`; the exact tag on a tagged commit, `vX.Y.Z+<commit>` between
tags). CI (gofmt, vet, the full race suite) runs on every push and pull
request. Releases are listed in [CHANGELOG.md](CHANGELOG.md); the
software version (`pkg/version.Release`) is separate from the cluster
protocol version that rolling upgrades finalize.

```sh

# One process, three in-memory nodes across racks a/b/c
# (-http-port 8080 also serves the observability dashboard per node):
./datax demo

# ... in another terminal:
psql "postgres://root@127.0.0.1:26433/datax?sslmode=disable"
```

```sql
CREATE TABLE accounts (id INT8 PRIMARY KEY, balance INT8);
INSERT INTO accounts VALUES (1, 100), (2, 100);
BEGIN;
UPDATE accounts SET balance = balance - 10 WHERE id = 1;
UPDATE accounts SET balance = balance + 10 WHERE id = 2;
COMMIT;
SELECT * FROM accounts;
```

A real multi-node cluster:

```sh
datax init  --dir data1 --listen :26257 --pg-listen :26433 --locality region=r1,rack=a
datax start --dir data2 --listen :26258 --pg-listen :26434 --join 127.0.0.1:26257 --locality region=r1,rack=b
datax start --dir data3 --listen :26259 --pg-listen :26435 --join 127.0.0.1:26257 --locality region=r1,rack=c
```

Once three nodes are up, every range is automatically replicated 3× with one
replica per rack. Add `--certs-dir` (after `datax cert create-ca` /
`create-node`) for mutual internode TLS and SQL TLS + SCRAM,
`--http-listen :8080` for the observability dashboard (Prometheus `/metrics`, JSON `/status` and
`/api/cluster`, and a self-contained web UI at `/`), `--enc-key store.key`
for encryption at rest, and `--storage-profile ingest` on append-heavy
nodes. `datax sql` is a built-in client for scripts; `datax backup` /
`datax restore` and `datax debug` cover day-2 operations
([operations guide](docs/user/operations.md)).
Benchmark with `datax bench kv|bank|ingest`; on the in-process demo the kv
workload does ~12.6k ops/s at p50 310µs (16 workers, 95% reads).

## Scope

Everything described above is implemented and covered by the test suite.
These are the boundaries, chosen deliberately rather than left unfinished:

| Area | Out of scope today |
|---|---|
| Ranges | load statistics are leader-local samples: a lease transfer hands the measured rate to the new leader, but reservoir split-key samples start fresh (splits and merges are otherwise automatic by size and load, or manual via `datax debug split`/`merge`) |
| Placement | cross-node QPS accounting beyond heartbeat aggregates (lease shedding and byte-weighted moves act on ~3s-stale top-8 advertisements); zone and lease preferences are parsed and stored but not acted on automatically |
| Reads | current-time reads are leader-only (lease-based ReadIndex); follower reads are opt-in per statement — exact-timestamp `AS OF SYSTEM TIME` or bounded staleness `with_max_staleness('10s')` |
| SQL | correlated subqueries past 4 nesting levels or over join/derived shapes; joins beyond 8 tables (INNER joins cost-reorder when statistics exist, LEFT joins keep syntactic order); typmods beyond `DECIMAL(p,s)`, the integer widths, `VARCHAR(n)` / `CHAR(n)` and the `TIMESTAMP` / `TIME` precisions (others are parsed and ignored); `TIME WITH TIME ZONE`; multidimensional arrays, array slices, indexes on array columns, `JSONB[]`; enum `ADD VALUE BEFORE` / `AFTER`, `RENAME VALUE`, arrays of enums; JSONB indexing (`@>` always filters — no inverted indexes — and is single-table only) |
| Wire | `COPY TO` and COPY options beyond `FORMAT`; cursor-style streaming (suspended portals serve materialized results, so a fetch limit bounds wire traffic per round trip, not server memory) |
| Ops | a dedicated audit store (audit records ride the node's structured log); password auth on the RPC port (admin RPCs authenticate by client certificate only) |
| Upgrades | skipping versions (adjacent-version rolls only); auto-finalize (the version bump is a deliberate `datax debug upgrade` — before it, binaries roll back freely; after it, never) |
| Backup | sealed/encrypted backup files (plaintext on disk, gated by `--allow-plaintext` on encrypted stores); restore into a non-empty cluster or of a single table; point-in-time restore between chain elements (a chain restores to its last backup's timestamp) |
| Encryption | key escrow / HSM integration (keys live in process memory); the node never rewrites the operator's key file — stage a new key with `--enc-key old.key,new.key` before rotating online; a stale sstable spanning a single user key waits for natural compaction churn |
| Storage | debt-gate thresholds are first-cut constants (per-cause counters tell the compaction-debt gate and follower-health shedding apart; tune with production data) |
| Time series | row-level retention on mixed ranges skips tables with secondary indexes (their entries carry no timestamp); expiry timing is best-effort (one housekeeping tick + the ~30s descriptor cache) |

## License

Apache 2.0. See [LICENSE](LICENSE).
