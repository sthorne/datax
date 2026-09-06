# Security

datax runs in one of two modes, decided by whether nodes are started with
`--certs-dir`:

| | Insecure (default) | Secure (`--certs-dir`) |
|---|---|---|
| Internode RPC | plaintext | mutual TLS |
| SQL wire | plaintext, any username accepted (trust) | TLS + SCRAM-SHA-256 password auth, or client-certificate auth |
| HTTP endpoints | open | a signed-in browser session, HTTP Basic (any database user), or client certificate; `/api/range` admin-only |
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

The built-in shell takes the certificate directory directly and picks
`ca.crt` and `client.<user>.crt` from it, whatever `sslmode` the URL
says:

```sh
datax sql --url postgres://10.0.0.1:26433/datax --certs-dir certs --user root
datax sql --url postgres://10.0.0.1:26433/datax --certs-dir certs --user ops -e "SHOW TABLES"
```

`--user` names both the certificate to present and the session user
(they must agree: the server takes the username from the certificate);
without it the URL's user is used. A missing certificate is refused with
the `datax cert create-client` command that creates it.

## Roles and privileges

Authorization follows PostgreSQL's role model (cluster version v11). A
**role** is the unit of authentication and authorization: a role with
`LOGIN` is a user, one without is a group, and a role may be a member of
other roles. Privileges flow along membership — a role holds what it was
granted directly plus, unless it is `NOINHERIT`, what the roles it
belongs to hold. Every object records its **owner** (the role that
created it), who holds every privilege on it and alone — with admins —
may alter, drop or grant it.

```sql
CREATE ROLE app_readers;                                  -- a group (NOLOGIN)
CREATE USER analyst PASSWORD 'trustno1' IN ROLE app_readers;  -- = CREATE ROLE ... LOGIN
CREATE ROLE etl WITH LOGIN PASSWORD '...' NOINHERIT;      -- privileges only via SET ROLE
ALTER ROLE analyst PASSWORD 'rotated';                    -- a role may change its own password
ALTER ROLE etl NOLOGIN;                                   -- lock out; LOGIN reopens
DROP ROLE analyst;                                        -- refused (2BP01) while it owns objects

GRANT app_readers TO analyst;                             -- membership
GRANT app_readers TO lead WITH ADMIN OPTION;              -- lead may grant app_readers on
REVOKE app_readers FROM analyst;
SET ROLE app_readers;  SELECT current_user, session_user; RESET ROLE;

GRANT SELECT, INSERT ON users TO app_readers;             -- SELECT INSERT UPDATE DELETE TRUNCATE ALL
GRANT ALL ON ALL TABLES IN SCHEMA public TO etl;
GRANT SELECT ON orders TO analyst WITH GRANT OPTION;      -- analyst may pass SELECT on
GRANT USAGE ON SEQUENCE order_ids TO etl;                 -- USAGE SELECT UPDATE
GRANT CONNECT, CREATE ON DATABASE app TO etl;             -- CREATE: make tables there
GRANT USAGE, CREATE ON SCHEMA public TO etl;
REVOKE CONNECT ON DATABASE app FROM PUBLIC;               -- PUBLIC: everyone
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO app_readers;

ALTER TABLE users OWNER TO etl;                           -- also VIEW, SEQUENCE, TYPE, DATABASE
REASSIGN OWNED BY analyst TO etl;  DROP OWNED BY analyst;  DROP ROLE analyst;

SHOW ROLES;  SHOW USERS;  SHOW GRANTS [ON t | ON DATABASE d | ON ROLE r] [FOR r];
```

The built-in roles always exist and cannot be altered or dropped:

| Role | Holds |
|---|---|
| `admin` | everything: every privilege on every object, role and grant management, the admin HTTP and RPC surfaces. `root` is an implicit, irrevocable member; `GRANT admin TO ops` (the old `GRANT ADMIN TO ops` spelling still works) |
| `read_all` | `SELECT` on every table, view and sequence (PostgreSQL's `pg_read_all_data`) — reporting accounts |
| `write_all` | `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` on every table (`pg_write_all_data`) — ETL accounts |
| `metrics` | the HTTP `/metrics` endpoint, and nothing else — the Prometheus scrape account needs no table grants and cannot read data |

What each statement needs: creating tables, views, sequences and types
takes `CREATE` on the database (or on `public`), or the admin role;
`ALTER`, `DROP`, `COMMENT ON`, `CREATE INDEX` and `GRANT` on an object
take its owner (or an admin); `TRUNCATE` takes the `TRUNCATE` privilege
(a pre-v11 `DELETE` grant still covers it); `nextval` takes `USAGE` on
the sequence — a `SERIAL` column's sequence follows `INSERT` on the
table; `CREATE DATABASE`, role management and `ANALYZE` take the admin
role. A view's query runs with its **owner's** privileges (PostgreSQL's
rule): a reader needs `SELECT` on the view only. `DROP ROLE` refuses a
role that owns objects (`2BP01`); `REASSIGN OWNED` or `DROP OWNED` first.
Its grants and memberships go with it. Grants take effect cluster-wide
by the time the statement returns; a membership change is seen by the
next statement everywhere.

A cluster upgraded from an earlier version keeps its users and `ADMIN`
grants: `datax debug upgrade` rewrites them as roles in the same
finalize transaction, and until then the old statements keep working
while the new ones (`CREATE ROLE`, `NOLOGIN`, memberships other than
`admin`, ownership, sequence, schema, `ALL TABLES` and default-privilege
grants, `SET ROLE`) are refused with `0A000`.

Note the syntax: `CREATE USER name PASSWORD '...'` — `WITH` is optional.
In insecure (trust) mode the session user is whatever the client
claims, but grants still name existing roles: create them first.

## HTTP endpoints in secure mode

Every HTTP route requires one of three doors: a CA-verified client
certificate (CN = username), HTTP Basic credentials of a database user,
or the session cookie a person gets by signing in to the console. All
three open only for a role that exists and holds `LOGIN`, exactly as on
the SQL port: `ALTER ROLE ... NOLOGIN` or `DROP ROLE` shuts a
certificate holder — or a signed-in browser — out at once, not when the
certificate or the session expires. Authorization is per endpoint:

| Endpoint | Who |
|---|---|
| `/`, `/status`, `/api/cluster` | any database user (read-only) |
| `/metrics` | the `metrics` role (or admin) |
| `/api/range` (cross-node drill-down), `/api/activity`, `/debug/pprof/` | admin role only — the drill-downs fan out over internode RPC; a profile exposes statement text and key bytes |

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

(`CREATE USER metrics_user PASSWORD '...' IN ROLE metrics` — the role
scrapes `/metrics` and nothing else; it cannot read data.)

### Signing in to the console

An unauthenticated browser navigation gets a sign-in page rather than
the browser's own credential dialog; anything scripted — `curl`,
Prometheus, `datax debug` — still gets the `WWW-Authenticate` challenge
it always got, and HTTP Basic keeps working everywhere. The two are told
apart by the `Accept` header, never by the user agent.

The credentials are database credentials, the same pair `datax sql`
connects with. `POST /api/login` verifies them and sets a session
cookie (`HttpOnly`, `Secure`, `SameSite=Strict`, path `/`, 12 hours);
`POST /api/logout`, or **Sign out** in the console's user menu, clears
it. Both are `POST` and require a JSON content type, which with
`SameSite=Strict` is what stops another origin driving either.

The cookie is a signed token — user, issue time, expiry, and a MAC under
a key every node derives from the cluster's authentication secret. It
carries no password and needs no session store, so a token minted by one
node is accepted by every other with nothing replicated between them.
What it asserts is identity only: roles are resolved per request from
the catalog, so revoking `LOGIN` or the admin role takes effect at once
rather than at expiry. Being stateless it cannot be revoked
individually — signing out ends the session on that browser, and
rotating the cluster's authentication secret invalidates every
outstanding token at once. The 12-hour TTL is what bounds a stolen
cookie's life.

A browser that presents a CA-signed client certificate is signed in by
it and never sees the form. A user with no password (certificate-only)
cannot sign in with one, and the refusal says so without confirming
whether any particular name exists: unknown user, wrong password and
certificate-only user are one message, and the endpoint burns the same
work on each.

## Admin RPCs in secure mode

The RPC port (`--listen`) requires mutual TLS, so `datax debug`,
`datax backup`, and `datax restore` need `--certs-dir` (and optionally
`--user`, default `root`) to present a client certificate:

```sh
datax cert create-client --certs-dir certs --user ops   # once
datax debug split --table 100 --addr 10.0.0.1:26257 --certs-dir certs --user ops
datax backup --dest /backups/today --addr 10.0.0.1:26257 --certs-dir certs
```

Every client command dials and completes the TLS handshake before it
sends anything, under `--connect-timeout` (default 10s), so a node that
is down, unreachable, or presenting a certificate that does not match
its address is reported as `could not connect to 10.0.0.1:26257 (admin
rpc, TLS with client certificate): ...` with the cause, instead of the
operation's own timeout expiring in silence.

The server authorizes by the certificate's CommonName: read-only ops
(`ranges`, `nodes`) accept any database user's certificate;
state-changing ops (split, merge, rebalance, transfer-lease,
decommission, upgrade, backup, restore, store-key rotation) and
`node-status` (per-replica internals, the `/api/range` data source)
require the **admin role** — `root`, or a member of `admin` (`GRANT
admin TO ops`), resolved through role membership.
Every admin op except the read-only ones is audited with its outcome.
In insecure mode the admin surface is open, like everything else.

Only the admin RPC accepts user certificates. The internode surfaces on
the same port — KV batches, Raft messages, snapshots, join — require the
cluster's own **node certificate** (CN `node`) and reject any other
principal with `permission denied` (audited as `rpc-denied`), since a raw
KV batch would bypass every SQL privilege check. `node` is therefore
reserved: `datax cert create-client --user node` and `CREATE USER node`
are both refused.

## Audit log

Security-relevant actions are written to the node log as structured
records (`msg=audit`), each with the acting principal:

- authentication failures — SQL (SCRAM), HTTP Basic, a refused
  console sign-in, and a session cookie that is expired, tampered with,
  or names a role that may no longer log in
  (`datax_auth_failures_total`)
- console sign-ins and sign-outs (`http-login`, `http-logout`)
- denied admin operations (`datax_admin_denied_total`)
- executed state-changing admin RPCs (op, principal, target)
- role and privilege DDL: `CREATE`/`ALTER`/`DROP ROLE` and `USER`,
  `GRANT`/`REVOKE` (privileges and memberships), ownership changes,
  `ALTER DEFAULT PRIVILEGES` — each with the session user (`principal`)
  and the current role (`role`, different after `SET ROLE`)
- `datax debug unsafe-recover` (offline: records the OS user)

## What secure mode does not do

- No column- or row-level SQL privileges — grants are per table (or
  sequence, database, schema).
- No certificate revocation — rotate the CA to invalidate issued certs.
