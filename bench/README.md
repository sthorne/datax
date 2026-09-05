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
   lower throughput, or higher latency, errors or retries.
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
| `kv-50-50-1000-ranges`, `ingest-random-1000-ranges` | the same mixes over a table pre-split into 1,000 ranges (`--presplit`): the store's raft scheduler and group commit under many groups |

`--presplit N` carves a table of the run's own (`bench_kv_r1000`, ...)
into N ranges before the run (`ALTER TABLE ... SPLIT AT` at evenly
spaced keys; a sharded timeseries table is carved by `--shards`
instead), so a run measures many raft groups on one store rather than
one hot range. The housekeeping loop merges empty neighbors back
together within minutes, so start the nodes with
`--merge-size-threshold -1` for an idle-cluster measurement; 10,000 is
the largest worth trying on a laptop. A record's `error_samples` lists
the distinct error messages behind its `errors` count.

`bench/run.sh` starts a fresh single-node on-disk store
(`datax init --dir`) and a fresh in-memory 3-node cluster (`datax demo`)
and runs the set against each, writing `OUT/single/<name>.json` and
`OUT/cluster/<name>.json`. A record carries throughput, p50/p95/p99, error
and retry counts, the run's arguments and seed, and the deltas of every
server counter that moved (`metrics`), so a storage change shows up as
stalls or backpressure even when throughput hides it.

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
