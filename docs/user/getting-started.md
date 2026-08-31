# Getting started

## Build

datax is a single Go binary with no runtime dependencies:

```sh
git clone https://github.com/sthorne/datax
cd datax
go build -o datax ./cmd/datax
./datax version
```

You need the Go toolchain version pinned in `go.mod` (currently 1.26) or
newer; `go build` will tell you if yours is too old.

## Run the demo cluster

The fastest way to a working cluster is `demo`: one process, three in-memory
nodes spread across simulated racks `a`/`b`/`c`, with every range replicated
3× (one replica per rack):

```sh
./datax demo
```

```
Starting a 3-node in-memory datax cluster across racks...
  n1  rack=a  sql=127.0.0.1:26433  rpc=127.0.0.1:26257
  n2  rack=b  sql=127.0.0.1:26434  rpc=127.0.0.1:26258
  n3  rack=c  sql=127.0.0.1:26435  rpc=127.0.0.1:26259
...
Press Ctrl-C to shut down (in-memory data is discarded).
```

Useful flags: `-nodes N` (1–9), `-http-port 8080` to also serve the
[observability dashboard](operations.md) per node (node *i* listens on
`port+i-1`), `-pg-port` / `-rpc-port` to move the port ranges.

Demo data is in-memory only — everything is discarded on Ctrl-C. For a
persistent cluster see [Deployment](deployment.md).

## Connect

Any PostgreSQL client works. Each node is a SQL gateway; connect to any of
them:

```sh
psql "postgres://root@127.0.0.1:26433/datax?sslmode=disable"
```

There is also a built-in shell:

```sh
./datax sql                                  # defaults to the URL above
./datax sql -e "SHOW TABLES"                 # one statement, then exit
./datax sql -url "postgres://root@127.0.0.1:26434/datax?sslmode=disable"
```

From Go, use [pgx](https://github.com/jackc/pgx) directly or through
`database/sql`:

```go
conn, err := pgx.Connect(ctx, "postgres://root@127.0.0.1:26433/datax?sslmode=disable")
```

Drivers may use the simple or the extended protocol; both are supported,
including binary parameters and results for every column type. Clients
**must** be prepared to retry transactions that fail with SQLSTATE `40001` —
see [transactions](sql.md#transactions-and-retries).

## First schema

```sql
CREATE TABLE users (
  id     INT8 PRIMARY KEY,
  email  TEXT NOT NULL,
  city   TEXT,
  age    INT8
);

INSERT INTO users (id, email, city, age) VALUES
  (1, 'ann@example.com', 'oslo',   34),
  (2, 'bob@example.com', 'bergen', 41),
  (3, 'cai@example.com', 'oslo',   28);

CREATE INDEX by_city ON users (city);

SELECT city, COUNT(*) AS n, AVG(age) AS avg_age
FROM users GROUP BY city ORDER BY n DESC;
```

```
  city  | n | avg_age
--------+---+---------
 oslo   | 2 |      31
 bergen | 1 |      41
```

`EXPLAIN` shows the access path a query takes — worth checking whenever a
query should be using an index:

```sql
EXPLAIN SELECT email FROM users WHERE city = 'oslo';
```

```
 scan of index "by_city" (1 column prefix) + primary key join
```

Every table needs a `PRIMARY KEY`. Rows are distributed across the cluster
by primary-key ranges, so the PK you choose determines write scaling — a
monotonically increasing key funnels all inserts into one range. See
[capacity planning](operations.md#capacity-planning) and
[timeseries tables](sql.md#timeseries-tables) for the two ways around that.

## Where to next

- Real, persistent, multi-node clusters: [Deployment](deployment.md)
- TLS, passwords, and privileges: [Security](security.md)
- The full SQL surface: [SQL reference](sql.md)
- Coming from PostgreSQL: [Differences](postgres-differences.md)
