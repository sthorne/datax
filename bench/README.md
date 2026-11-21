# Benchmarks

`datax bench` drives a running cluster over the PostgreSQL wire protocol,
the path real applications take. This directory holds the checked-in
workload set and the runner that produces comparable records.

## Recording a before/after for a PR

1. On `main` (or the PR's base), build and run the set:

   ```sh
   OUT=/tmp/before make bench
   ```

2. On the PR's branch:

   ```sh
   OUT=/tmp/after make bench
   ```

3. Compare — one table for the single node, one for the cluster:

   ```sh
   bin/datax bench compare /tmp/before/single /tmp/after/single
   bin/datax bench compare /tmp/before/cluster /tmp/after/cluster
   ```

   `!` marks a delta beyond ±5 % (`--threshold`), `!!` a regression:
   lower throughput, or higher latency, errors, retries or KV batches
   per operation.
   `--fail-on-regression` turns that into an exit status.

4. Paste both tables into the PR body under "Before/after", with the
   machine class (cores, disk) they were recorded on. The workload the
   change targets must move; the others must stay within noise.

Both runs must come from the same machine, back to back, with nothing
else running. `DURATION_SCALE=0.1 make bench` is a smoke run to check
the harness, not a measurement.

## The set

`workloads.json` lists each workload with a fixed seed, row count and
duration, so two runs draw the same keys:

| Name | Exercises |
|---|---|
| `kv-95-5`, `kv-50-50` | point reads and updates by primary key |
| `bank` | contended two-row transfers in explicit transactions |
| `hot-row` | one row updated 16 times inside each transaction: the rewrite of a transaction's own intent, whose cost grew with the depth of the chain (#162) |
| `ingest-random`, `ingest-sequential`, `ingest-uuid` | batched INSERTs with random integer, per-worker monotone, and UUID text keys |
| `timeseries` | per-series monotone timestamps across 8 shard buckets, then windowed reads |
| `index-join`, `index-join-1pct`, `index-join-10pct` | secondary-index lookups fanning out to wide primary rows: 20, 200 and 2,000 rows per lookup (the batched primary fetch of #103) |
| `scan`, `scan-200k` | large result sets (20,000 and 200,000 rows of 256 bytes per query) streamed through pgwire; the records carry the time to the first row (`first_row_p50_us`, `first_row_p99_us`) |
| `jsonb-contain`, `jsonb-contain-filter` | JSONB containment (`doc @> '{"tenant": n}'`, 50 of 50,000 documents per query) through a GIN index and as a filter over every document (#300) |
| `text-search`, `text-search-filter` | full-text search (`to_tsvector('english', body) @@ to_tsquery('english', 'topic<n>')`, 50 of 50,000 short documents per query) through a GIN index on the expression and as a filter that reduces every document to lexemes for every query (#299) |
| `vector-knn`, `vector-knn-exact` | the 10 nearest of 50,000 vectors of 64 dimensions (`ORDER BY v <-> q LIMIT 10`) through an `ivfflat` index of 200 lists and by the exact top-k scan (#304); the records carry the index build time and recall@10 (`vector.build_sec`, `vector.recall`) |
| `kv-50-50-1000-ranges`, `ingest-random-1000-ranges` | the same mixes over a table pre-split into 1,000 ranges (`--presplit`): the store's raft scheduler and group commit under many groups |

`--presplit N` carves a table of the run's own (`bench_kv_r1000`, ...)
into N ranges before the run (`ALTER TABLE ... SPLIT AT` at evenly
spaced keys; a sharded timeseries table is carved by `--shards`
instead), so a run measures many raft groups on one store rather than
one hot range. The boundaries are sticky (cluster version v20), so the
housekeeping loop does not merge the empty neighbors back together; on
a cluster that has not finalized v20 it does, within minutes, so there
start the nodes with `--merge-size-threshold -1` for an idle-cluster
measurement; 10,000 is
the largest worth trying on a laptop. A record's `error_samples` lists
the distinct error messages behind its `errors` count, and
`retry_samples` those behind its `retries` (the 40001s). `retry_sec` is
the worker time spent in the attempts that ended in a 40001: on a
contended workload such as `bank` it, not the latency of the operations
that succeed, sets the throughput, so compare two runs by retries per
operation and `retry_sec` per retry before their operations per second.

The `jsonb-contain` pair is sized to keep the set's run time down. The
same comparison over a million documents (the preload is the slow part,
and is skipped when the table already holds the rows):

```sh
bin/datax bench jsonb-contain --preload 1000000 --groups 20000 --concurrency 4 --duration 60s
bin/datax bench jsonb-contain --preload 1000000 --groups 20000 --concurrency 4 --duration 60s --jsonb-index none
```

The `vector-knn` pair loads vectors drawn around `--groups` Gaussian
centres from the seed (so it needs no download and two runs see the
same table), builds the index after the load, and before the measured
phase answers 100 queries both through the index and exactly, recording
recall@10 beside the build time. It is a synthetic set, not one of the
public ANN benchmarks: clustered data flatters a clustering index, and
recall on real embeddings should be measured on them. `--probes` sets
`ivfflat.probes` (default: the server's, the square root of `--lists`),
`--dims` the dimension, `--vector-index none` the exact scan. Recorded
on one node, 4 cores, 50,000 × 64, `--lists 200`, one worker: recall
0.85 / 0.96 / 0.99 / 1.00 at 1 / 5 / 15 (default) / 40 probes with a
median of 0.9 / 3 / 10 / 29 ms, against 164 ms for the exact scan; the
index built in 3.9 s.

```sh
bin/datax bench vector-knn --preload 50000 --dims 64 --groups 1000 --lists 200 --probes 5 --concurrency 1 --duration 20s
bin/datax bench vector-knn --preload 50000 --dims 64 --groups 1000 --vector-index none --concurrency 1 --duration 20s
```

## The large set

`workloads-large.json` is a second checked-in set, for what does not
show on a small table: full-text search over **a million documents**
(#299) — one selective word (50 documents per query), the same narrowed
by a phrase that is decided from positions, a prefix (`serial1234:*`, a
range of the index), and the ten best of a two-word `OR` by `ts_rank`.
The four share one table and the first to run loads it, which takes
most of the run (about ten index entries per document; some ten minutes
on a 4-core machine for the single node, longer for the cluster):

```sh
SET=bench/workloads-large.json OUT=/tmp/large make bench
```

It is not part of the nightly run, whose 40 minutes it would not fit
in. The filter over the same million documents is in neither set — one
query takes over twenty seconds — and can be timed by hand:

```sh
bin/datax bench text-search --preload 1000000 --groups 20000 --concurrency 1 --duration 120s --text-index none
```

## Errors fail a run

An operation that errors during the measured run is not measured, so a
run that shed some describes a different workload than the one asked
for. `datax bench <workload>` and `datax bench run` therefore exit
non-zero when a run counts more errors than `--max-errors` (default 0;
`-1` never fails on errors). The record and the printed results are
written either way; only the exit status changes.

- A serialization conflict (SQLSTATE 40001, "restart transaction") is
  not an error: it is counted under `retries`, and a contended workload
  such as `bank` produces them on every honest run. Everything else is
  an error — a routing failure, a dropped connection, a timeout. The
  whole set against a healthy 3-node local cluster counts none.
- `datax bench run` runs every workload of the set whatever the earlier
  ones did, so one bad workload does not cost the records of the rest,
  and gives its verdict at the end: one `FAIL` line per workload that
  did not complete (its setup failed) or went over its tolerance, with
  the error count and the most frequent error message.
- A workload that is expected to shed a few operations can carry its own
  tolerance in the set, which takes precedence over the runner's flag:
  `{"name": "...", "args": [...], "max_errors": 5}`. None of the
  checked-in workloads has one.
- `bench/run.sh` runs both targets even when the first fails, repeats
  the `FAIL` lines of both as its last lines, and then exits non-zero.
  `MAX_ERRORS=-1 make bench` records a set without the check.

`bench/run.sh` starts a fresh single-node on-disk store
(`datax init --dir`) and a fresh in-memory 3-node cluster (`datax demo`)
and runs the set against each, writing `OUT/single/<name>.json` and
`OUT/cluster/<name>.json`. A record carries throughput, p50/p95/p99, error
and retry counts, the run's arguments and seed, and the deltas of every
server counter that moved (`metrics`), so a storage change shows up as
stalls or backpressure even when throughput hides it.

`kv_batches_per_op` is the KV batches the scraped node routed per
measured operation: the `datax_kv_batch_latency_seconds_count` delta in
`metrics` over `ops` (present only when the run scraped a node's
counters, `--server-url` or `--metrics-url`). It is a count, not a
timing, so it holds on a noisy machine: a change that adds a round trip
to every statement — an uncached catalog read, say — doubles it for a
point-read workload while throughput barely moves. `datax bench compare`
shows it as `kv batches/op` and flags an increase beyond the threshold
as a regression; for a record written before the field it derives the
value from the record's `metrics`. It counts every batch the node
routed during the run, retried attempts and background work included,
and only the scraped node's, one-phase commits (an autocommit write's
batch that carries its own EndTxn) included. Records written before
that were counted read lower on write workloads, so compare a write
workload only against a record from a binary that counts them. The
exact per-statement counts are
pinned by `TestKVRoundTripsPerStatement` (`pkg/testutils/testcluster`).

## Profiles

- `datax bench <workload> --cpuprofile client.pprof --memprofile heap.pprof
  --trace trace.out` profiles the client.
- `--server-url http://host:8080 --server-profile cpu` pulls the node's
  CPU profile for the run's duration (`server-cpu.pprof`; the admin role
  in secure mode: `--certs-dir`, `--user`).
- `datax debug profile --kind cpu|heap|allocs|mutex|block|goroutine|trace
  --seconds 30 --url http://host:8080` fetches one profile at any time;
  mutex and block profiles are always on at low sampling rates.
- `go tool pprof -http=:0 server-cpu.pprof` to inspect.

## Wire protocol

`kv` and `bank` send parameterized statements (`WHERE k = $1`) over
pgx's default extended protocol: each text is prepared once per
connection and then bound and executed, as a driver or an ORM does, so
the server's plan cache is exercised (`datax_sql_plan_cache_hits_total`
in the counter deltas). The literal-text workloads (ingest, timeseries,
scan, index-join) use the simple protocol. `--protocol simple|extended`
overrides either.

## The write pipeline below SQL

`BenchmarkRangeWritePipeline` (`pkg/testutils/testcluster`) measures one
range's write ceiling without SQL: one node on disk, W writers each
committing one-phase transactions of B puts to their own keys, for B in
1, 10, 100, 1000 and W in 1, 4, 16, 64, with the raft log's sync on and
stubbed to a no-op (`storage.TestingNoSync`). It reports proposals per
second, rows per second, syncs per second, entries per synced commit and
the mean apply time per entry:

```
go test ./pkg/testutils/testcluster -run - -bench RangeWritePipeline -benchtime 3s
```

The same knob is available to a running node for a measurement (never
in production: a crash loses acknowledged writes): `DATAX_TESTING_NOSYNC=1
datax start ...` logs a warning and commits the raft log unsynced. The
server-side counters `datax_raft_entries_appended_total`,
`datax_raft_log_syncs_total`, `datax_raft_apply_seconds` and
`datax_latch_wait_seconds` are what `--server-url` reports for a SQL run
of the same shape (`datax bench ingest --keys sequential --batch B
--concurrency W`).

## Nightly

`.github/workflows/bench.yaml` runs the set on `main` every night on a
hosted runner and uploads the records as an artifact. Runner noise makes
it a trend line, not a gate: a PR still records its own before/after.

A night is red when `bench/run.sh` exits non-zero: a workload did not
complete, or counted errors in its measured run beyond its tolerance
(above). The records are uploaded and summarized on a red night too, and
the standing issue the workflow files or updates
(`.github/scripts/standing-issue.sh`) quotes the run's last lines, which
name each failing workload, its error count and its most frequent
error. A clean night closes the issue.
