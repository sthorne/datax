# Security

datax runs in one of two modes, decided by whether nodes are started with
`--certs-dir`:

| | Insecure (default) | Secure (`--certs-dir`) |
|---|---|---|
| Internode RPC | plaintext | mutual TLS |
| SQL wire | plaintext, any username accepted (trust) | TLS + SCRAM-SHA-256 password auth, or client-certificate auth |
| HTTP endpoints | open | HTTP Basic (any database user) or client certificate; `/api/range` admin-only |
| Admin RPCs (`datax debug`, backup/restore) | open | client certificate; state-changing ops require the admin role |
| SQL privileges | statements accepted but identity is unverified | enforced per user via `GRANT`/`REVOKE` |

Insecure mode is for development only: anyone who can reach the SQL port is
`root`.

## Certificates

`datax cert` manages a self-signed CA and the certs derived from it:

```sh
datax cert create-ca     --certs-dir certs
datax cert create-node   --certs-dir certs --hosts db1.internal,10.0.0.1
datax cert create-client --certs-dir certs --user root
```

This produces:

```
certs/ca.crt  ca.key          # the CA (keep ca.key offline after issuing)
certs/node.crt  node.key      # node identity: serves SQL/HTTP, dials peers
certs/client.root.crt  .key   # client cert for user "root" (CN = username)
```

Every node needs `ca.crt`, `node.crt`, `node.key` in its `--certs-dir`;
`create-node`'s `--hosts` must include every DNS name/IP clients and peers
will dial. All nodes must share the same CA.

## Starting secure

```sh
datax init --dir /var/lib/datax --certs-dir certs \
  --listen 10.0.0.1:26257 --pg-listen 10.0.0.1:26433 --http-listen 10.0.0.1:8080 \
  --root-password 'change-me' --locality region=eu,rack=a
```

`--root-password` seeds the `root` user's password on first startup (it's a
no-op if root already has one). Without it, the only way in is the
`client.root.crt` client certificate.

Joining nodes start the same way — `datax start --certs-dir certs --join ...`
with the shared `ca.crt` plus their **own** node cert (run `create-node`
once per machine with that machine's hostnames/IPs in `--hosts`). A node
without certs cannot join a secure cluster, and vice versa.

## Connecting

Password (SCRAM-SHA-256; channel-bound SCRAM-PLUS is negotiated when the
client supports it):

```sh
psql "host=10.0.0.1 port=26433 user=root dbname=datax \
      sslmode=verify-full sslrootcert=certs/ca.crt"
```

Client certificate (no password; the certificate's CN is the username):

```sh
psql "host=10.0.0.1 port=26433 user=root dbname=datax \
      sslmode=verify-full sslrootcert=certs/ca.crt \
      sslcert=certs/client.root.crt sslkey=certs/client.root.key"
```

## Users and privileges

```sql
CREATE USER analyst PASSWORD 'trustno1';
ALTER USER analyst PASSWORD 'rotated';     -- change a password
DROP USER analyst;

GRANT SELECT, INSERT ON users TO analyst;  -- per-table: SELECT INSERT UPDATE DELETE ALL
REVOKE ALL ON users FROM analyst;

GRANT ADMIN TO analyst;                    -- admin role: full access + user management
REVOKE ADMIN FROM analyst;
```

Note the syntax: `CREATE USER name PASSWORD '...'` — no `WITH`. `root` is
always an admin. Table privileges take effect cluster-wide by the time the
statement returns.

## HTTP endpoints in secure mode

Every HTTP route requires either HTTP Basic credentials of a database
user, or a CA-verified client certificate (CN = username). Authorization
is per endpoint:

| Endpoint | Who |
|---|---|
| `/`, `/metrics`, `/status`, `/api/cluster` | any database user (read-only) |
| `/api/range` (cross-node drill-down) | admin role only — it fans out over internode RPC |

Rejected credentials are audited (`datax_auth_failures_total`); an
authenticated non-admin hitting an admin endpoint gets 403 and an
`admin-denied` audit record (`datax_admin_denied_total`).

Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: datax
    scheme: https
    tls_config: { ca_file: /etc/prometheus/datax-ca.crt }
    basic_auth: { username: metrics_user, password: "..." }
    static_configs:
      - targets: ["10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"]
```

(Create a dedicated low-privilege `metrics_user` — HTTP auth accepts any
valid user, and that user needs no table grants.)

The browser dashboard works with the same Basic credentials — the browser
prompts once and same-origin fetches reuse them.

## Admin RPCs in secure mode

The RPC port (`--listen`) requires mutual TLS, so `datax debug`,
`datax backup`, and `datax restore` need `--certs-dir` (and optionally
`--user`, default `root`) to present a client certificate:

```sh
datax cert create-client --certs-dir certs --user ops   # once
datax debug split --table 100 --addr 10.0.0.1:26257 --certs-dir certs --user ops
datax backup --dest /backups/today --addr 10.0.0.1:26257 --certs-dir certs
```

The server authorizes by the certificate's CommonName: read-only ops
(`ranges`, `nodes`) accept any database user's certificate;
state-changing ops (split, merge, rebalance, transfer-lease,
decommission, upgrade, backup, restore) require the **admin role** —
`root`, or a user granted `GRANT ADMIN`. In insecure mode the admin
surface is open, like everything else.

## Audit log

Security-relevant actions are written to the node log as structured
records (`msg=audit`), each with the acting principal:

- authentication failures — SQL (SCRAM) and HTTP Basic
  (`datax_auth_failures_total`)
- denied admin operations (`datax_admin_denied_total`)
- executed state-changing admin RPCs (op, principal, target)
- user and privilege DDL: `CREATE`/`ALTER`/`DROP USER`,
  `GRANT`/`REVOKE` (including `GRANT ADMIN`)
- `datax debug unsafe-recover` (offline: records the OS user)

## What secure mode does not do

- No column- or row-level SQL privileges — grants are per table.
- No certificate revocation — rotate the CA to invalidate issued certs.
