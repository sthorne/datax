# Testing

## The gate

A change merges when all four steps below pass on its head commit:

1. **Lint:** `gofmt -l -s .` prints nothing. `go vet` and staticcheck (`make lint`) are clean.
2. **Race tests:** every package except `pkg/testutils/testcluster`, under the race detector (`go test -race`).
3. **Race cluster tests:** the in-process cluster tests in `pkg/testutils/testcluster`, under the race detector. They run in two shards, `Test[A-L]...` and the rest, each with a 30-minute limit.
4. **Vulnerability check:** govulncheck, pinned to the version in `.github/workflows/vulncheck.yaml`, finds no reachable vulnerability.

On GitHub these are the `ci` workflow's `test` and `cluster (a-l)`/`cluster (m-z)` jobs, plus the `vulncheck` workflow.

### When the hosted workflows are paused

The `ci`, `vulncheck` and `bench` workflows are paused while the account has no GitHub Actions minutes. Their automatic triggers (push, pull request, schedule) are commented out. They can still be run by hand (workflow dispatch), and restoring the triggers turns them back on.

While they are paused, the same gate runs locally:

```sh
scripts/ci-local.sh            # lint, race tests, both race cluster shards
scripts/ci-local.sh vulncheck  # govulncheck (needs vuln.go.dev)
```

- **Required tools:** the script requires the browser test and the pg_dump tests to run rather than skip.
  - Set `DATAX_CHROME` if Chrome isn't on the default path.
  - Set `DATAX_PG_DUMP` to PostgreSQL 16's `pg_dump`. Newer clients' catalog queries aren't answered yet (#370).
- **Output:** each step writes its log to `ci-local/`, which git ignores. The failures are repeated at the end.
- **Run time:** the cluster shards run one after the other. On a typical machine the whole gate takes about an hour.

## The full suite without the race detector

The race detector slows the cluster tests enough that some timing-sensitive tests are skipped under it. Before a pull request is marked ready, also run the whole suite once without it, on the same commit:

```sh
DATAX_REQUIRE_BROWSER=1 DATAX_REQUIRE_PGDUMP=1 go test -count=1 -timeout 90m ./...
```

## The nightly benchmark

`bench/run.sh` runs the workload set the paused `bench` workflow ran nightly (bench/README.md). Run it by hand before and after a change that could affect performance.
