# datax user documentation

Documentation for people **running and using** datax. If you want to know how
it works inside, the design docs live one level up in [`docs/`](../).

| Guide | What it covers |
|---|---|
| [Getting started](getting-started.md) | Build, run the demo cluster, connect with `psql` and drivers, first schema |
| [Deployment](deployment.md) | Real multi-node clusters: `init`/`start`/`--join`, localities, storage profiles, encryption at rest |
| [Security](security.md) | Certificates, secure vs insecure mode, users, `GRANT`/`REVOKE`, Prometheus auth |
| [SQL reference](sql.md) | Every supported statement with examples, the type table, transactions and the retry loop, bulk loading with `COPY`, follower reads, timeseries tables |
| [Differences from PostgreSQL](postgres-differences.md) | What's missing, what behaves differently, and workarounds |
| [Operations](operations.md) | The dashboard, metrics and alerting, backup/restore, rolling upgrades, `datax debug`, decommission, capacity planning |

All examples in these pages were run against a live cluster before being
written down. Prompt conventions: `$` is your shell, `datax>` is
`datax sql`, and unprefixed SQL blocks are `psql` input.
