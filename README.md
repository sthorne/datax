# datax

**datax** is an open-source, distributed, ACID-compliant SQL database in the
spirit of CockroachDB: data is synchronously replicated across nodes with the
Raft consensus protocol, sharded into ranges for horizontal scale, and placed
**rack-aware** so replicas spread across failure domains (region / zone / rack)
while staying close to each other.

It speaks the **PostgreSQL wire protocol**, so any Postgres client or driver
works out of the box — `psql`, [pgx](https://github.com/jackc/pgx), or
`database/sql`.

> **Status: early prototype.** The core is real — MVCC storage, Raft-replicated
> ranges, serializable distributed transactions, rack-aware placement, and a
> minimal SQL surface — but this is not production software. See
> [Limitations](#limitations).

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
- **SQL**: a deliberately small subset (CREATE/DROP TABLE, INSERT, SELECT,
  UPDATE, DELETE, transactions) served over the Postgres wire protocol.

Design documents live in [`docs/`](docs/):
[architecture](docs/architecture.md) ·
[transactions](docs/transactions.md) ·
[replication & placement](docs/replication-and-placement.md) ·
[sql](docs/sql.md)

## Quickstart

```sh
go build -o datax ./cmd/datax

# One process, three in-memory nodes across racks a/b/c:
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
replica per rack.

## Limitations

This is a prototype. Out of scope so far, deliberately:

| Area | Not yet implemented |
|---|---|
| Ranges | automatic split/merge (splits are manual: `datax debug split`) |
| Placement | automatic load rebalancing; node decommission |
| Reads | follower reads (reads are leader-only: lease-based ReadIndex) |
| Transactions | parallel commits, savepoints, deadlock *detection* (timeout-based abort only) |
| SQL | secondary indexes, joins, aggregates, ORDER BY, ALTER, most types |
| Wire | TLS, SCRAM auth (trust auth only), binary extended-protocol parameters |
| Ops | observability UI, metrics endpoints; a restarted node must keep its address (no address-change story) |

## License

Apache 2.0. See [LICENSE](LICENSE).
