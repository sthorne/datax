# datax

**datax** is an open-source, distributed, ACID-compliant SQL database in the
spirit of CockroachDB: data is synchronously replicated across nodes with the
Raft consensus protocol, sharded into ranges for horizontal scale, and placed
**rack-aware** so replicas spread across failure domains (region / zone / rack)
while staying close to each other.

It speaks the **PostgreSQL wire protocol**, so any Postgres client or driver
works out of the box — `psql`, [pgx](https://github.com/jackc/pgx), or
`database/sql`.

> **Status: prototype (v2).** The core is real — MVCC storage with garbage
> collection, Raft-replicated ranges with log truncation and size-based
> auto-splitting, serializable distributed transactions with read refresh,
> rack-aware placement with dead-node repair, secondary indexes, TLS +
> SCRAM authentication, and Prometheus metrics — but this is not production
> software. See [Limitations](#limitations).

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
  logical clock (HLC) timestamp.
- **Replication**: the keyspace is split into **ranges**; each range is an
  independent [etcd Raft](https://github.com/etcd-io/raft) consensus group
  (multi-raft). Writes commit only once a quorum has them on disk.
- **Transactions**: CockroachDB-style — write intents plus a transaction
  record whose single atomic flip commits the whole transaction, no matter how
  many ranges it touched. Serializable isolation.
- **Placement**: nodes declare a locality (`--locality=region=r1,rack=a`);
  the allocator maximizes diversity across failure domains, so losing a rack
  never loses more than one replica of a range.
- **SQL**: a deliberately small subset (DDL incl. secondary indexes and
  ALTER TABLE, INSERT/SELECT/UPDATE/DELETE with ORDER BY and aggregates,
  transactions, joins, GROUP BY, subqueries, EXPLAIN) served over the
  Postgres wire protocol, with TLS + SCRAM-SHA-256 authentication in
  secure mode.
- **Operations**: leader-driven housekeeping per range — MVCC garbage
  collection, raft log truncation, size-based splitting and merging —
  plus dead-node repair, load rebalancing, decommission, `datax bench`, and
  a built-in observability dashboard with `/metrics` + `/status` + `/api/cluster`
  endpoints (`--http-listen`; the dashboard at `/` is read-only and
  self-contained; in secure mode every endpoint requires HTTP Basic
  credentials of any database user — Prometheus `basic_auth` — or a
  CA-verified client certificate, and in insecure mode stays open like
  pgwire trust auth).

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
`create-node`) for mutual internode TLS and SQL TLS + SCRAM, and
`--http-listen :8080` for the observability dashboard (Prometheus `/metrics`, JSON `/status` and
`/api/cluster`, and a self-contained web UI at `/`).
Benchmark with `datax bench kv|bank`; on the in-process demo the kv
workload does ~12.6k ops/s at p50 310µs (16 workers, 95% reads).

## Limitations

This is a prototype. Out of scope so far, deliberately:

| Area | Not yet implemented |
|---|---|
| Ranges | load stats are per-leader only (a leadership transfer resets the QPS view; splits and merges are otherwise automatic by size and load, or manual via `datax debug split`/`merge`) |
| Placement | cross-node QPS accounting beyond heartbeat aggregates (lease shedding and byte-weighted moves act on ~3s-stale top-8 advertisements; count rebalancing, lease shedding, byte moves and decommission are all automatic) |
| Reads | bounded-staleness follower reads (exact-timestamp `AS OF SYSTEM TIME` follower reads are in; current reads are leader-only: lease-based ReadIndex) |
| SQL | multi-level correlated subqueries (one level is in, as an O(outer×inner) memoized nested loop), join reordering (join order = syntactic order, ≤ 8 tables, nested loop), DECIMAL/JSONB types |
| Wire | COPY protocol; portal suspension (partial result fetches) |
| Ops | per-node drill-down across peers (the dashboard's range detail is the serving node's own); per-endpoint authorization (secure-mode HTTP auth accepts any valid user — everything served is read-only) |
| Encryption | online store-key rotation (`datax debug rotate-enc-key` runs against a stopped node); re-encrypting old files under rotated data keys (natural compaction churn only) |
| Storage | backpressure reads only the leader's engine (an overloaded follower just lags raft); compaction debt is exported but not gated on |
| Time series | re-sharding tables that carry secondary indexes, and historical reads below a re-shard (v1 guards); order pushdown through shard fan-out (ORDER BY sorts in memory); sub-range retention granularity (mixed ranges take the max TTL and never expire rows) |

## License

Apache 2.0. See [LICENSE](LICENSE).
