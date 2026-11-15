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
ALTER ROLE auditor BYPASSRLS;                             -- exempt from row-level security; NOBYPASSRLS undoes it
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

**The catalogs are every role's.** As in PostgreSQL, any role may read
`pg_catalog` and `information_schema`, so `psql`'s `\d` and ORM
introspection work for an ordinary role. What they list follows
PostgreSQL: `pg_class`, `pg_attribute`, `pg_constraint`, `pg_indexes`,
`pg_tables`, `pg_views`, `pg_policies` and the rest describe every
table of the current database — names, columns, defaults, constraints,
view definitions and policy expressions — whatever the reader holds;
`information_schema.tables`, `columns` and the other views list the
tables the reader holds some privilege on (and not those of a database
whose `public` schema it may not use), and `role_table_grants` the
grants made to or by one of its roles. What would describe the rows
rather than the schema is shown only to a role that may read them:
`reltuples` and `n_live_tup` read `0` for a table the reader holds no
`SELECT` on (or whose row security filters it), `pg_sequences.last_value`
is `NULL` without `SELECT` or `USAGE` on the sequence, and
`pg_largeobject_metadata` lists the reader's own large objects (all of
them for an admin). No catalog carries a password or verifier:
`pg_roles.rolpassword` and `pg_user.passwd` read `********`, and there is
no `pg_authid` or `pg_shadow`. Another role's statement text in
`pg_stat_activity` reads `<insufficient privilege>` unless the reader is
an admin. Reading a table's rows — the audit table's included — still
takes its privileges.

A cluster upgraded from an earlier version keeps its users and `ADMIN`
grants: `datax debug upgrade` rewrites them as roles in the same
finalize transaction, and until then the old statements keep working
while the new ones (`CREATE ROLE`, `NOLOGIN`, memberships other than
`admin`, ownership, sequence, schema, `ALL TABLES` and default-privilege
grants, `SET ROLE`) are refused with `0A000`.

Note the syntax: `CREATE USER name PASSWORD '...'` — `WITH` is optional.
In insecure (trust) mode the session user is whatever the client
claims, but grants still name existing roles: create them first.

## Row-level security

Table privileges decide *whether* a role may read or write a table.
Row-level security decides *which rows*: a table shared by many tenants
can be read and changed by one role without that role ever seeing
another tenant's rows, enforced by the database rather than by every
query the application writes. The statements and rules are
PostgreSQL's ([SQL reference](sql.md#row-level-security)); it needs
cluster version v21.

### A multi-tenant recipe

One table, the tenant's id leading the primary key so each tenant's
rows are one contiguous span; one role the application connects as; the
tenant chosen per connection (or per transaction) with a setting.

```sql
-- As the owner (the role that runs migrations):
CREATE TABLE invoices (
    tenant_id UUID,
    id        INT8,
    amount    DECIMAL(12,2),
    PRIMARY KEY (tenant_id, id)
);
CREATE ROLE app LOGIN PASSWORD '...';
GRANT SELECT, INSERT, UPDATE, DELETE ON invoices TO app;   -- not TRUNCATE, not ALL

ALTER TABLE invoices ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON invoices
    USING (tenant_id = current_setting('app.tenant')::uuid);

-- As app, once the request's tenant is known:
SET app.tenant = '8b1f...';                       -- or, from a pool, per transaction:
BEGIN; SELECT set_config('app.tenant', $1, true); ... COMMIT;

SELECT * FROM invoices;                            -- this tenant's invoices
INSERT INTO invoices VALUES ('8b1f...', 7, 12.50); -- fine
INSERT INTO invoices VALUES ('ffff...', 7, 12.50); -- 42501: new row violates row-level security policy
UPDATE invoices SET amount = 0;                    -- this tenant's rows only
```

Things to get right:

- **The application must not connect as the owner.** The owner and
  admins are exempt. Use a separate role for migrations and for the
  application; `ALTER TABLE ... FORCE ROW LEVEL SECURITY` if the owner
  must be filtered as well.
- **A session that has not named a tenant gets an error**, not every
  row: `current_setting('app.tenant')` is `42704` until set. Write
  `current_setting('app.tenant', true)` to get no rows instead.
- **Whoever can run SQL on the connection can change the setting.** A
  policy keyed on `app.tenant` protects against application bugs — a
  forgotten `WHERE tenant_id = ...`, a mixed-up join — not against a
  client that issues its own `SET`. If tenants (or anything acting for
  them) hold database credentials, give each a role and key the policy
  on `current_user`, which a session cannot choose:

  ```sql
  CREATE POLICY own_tenant ON invoices USING (tenant_name = current_user);
  ```

- **A pooled connection keeps its setting.** Set it at the start of
  every request, or use `set_config(..., true)` / `SET LOCAL` inside a
  transaction so it ends with it.
- **Views run as their owner.** A view owned by the table's owner shows
  every tenant's rows to whoever may read the view. Have the
  application's role (or another role the policies apply to) own views
  over protected tables.
- **Unique indexes span tenants.** A unique index that does not start
  with the tenant column makes one tenant's value collide with
  another's, and the unique-violation error says so. Lead unique
  indexes with the tenant column.
- **Reporting accounts**: `ALTER ROLE auditor BYPASSRLS` (admins only)
  exempts a role from every policy; it still needs the table grants.

### What it protects, and what it does not

It does: every statement a filtered role runs — reads through any index,
joins, subqueries, aggregates, writes, `ON CONFLICT`, `COPY`,
`RETURNING`, locking reads, prepared statements, historical (`AS OF
SYSTEM TIME`) reads — sees and changes only permitted rows; a write is
refused the same way whether or not the forbidden row exists; table
statistics and the console's range boundaries and row counts are
withheld from a filtered role; `TRUNCATE` is refused to it; and another
role's statement text in `SHOW SESSIONS` / `pg_stat_activity` is hidden
from non-admins.

It does not:

- **It is not encryption.** Rows of every tenant sit in the same ranges
  and files. Anyone with access to the data directory, a backup, or the
  internode RPC sees them all; so does `datax debug`. Use
  [encryption at rest](../encryption.md) and protect backups for that.
- **The owner, admins, `root` and `BYPASSRLS` roles see everything.**
  `FORCE` filters the owner and admins, but either can turn it off
  again: it guards against mistakes, not against a hostile owner. The
  node's own work — row expiry, statistics, backup and restore, the
  audit and metrics recorders — always sees every row.
- **Constraints see the whole table.** A unique violation (`23505`)
  tells a role that some row it cannot see holds that value, and a
  foreign key can reference, or be refused for the lack of, a parent row
  it cannot see. PostgreSQL behaves the same. The messages name the
  constraint (and, for a foreign key, the key the statement itself
  supplied), never a value from the hidden row. Cascades (`ON DELETE
  CASCADE`, `SET NULL`) act on child rows whether or not the role could
  see them.
- **Timing is not hidden.** How long a statement takes can depend on
  rows it does not return — a unique check, a lock held by another
  tenant's transaction, an index shared across tenants. `SKIP LOCKED`
  and `NOWAIT` behave differently when a row is locked, including one
  the role cannot see if the scan reaches it.
- **Estimates are withheld, counts of the role's own rows are not.**
  `EXPLAIN` omits row estimates for a filtered role; `EXPLAIN ANALYZE`
  reports the rows the statement itself read.
- **`DO NOTHING` is quiet.** `INSERT ... ON CONFLICT DO NOTHING` skips a
  row that collides with one the role cannot see, without an error —
  which is itself a way to learn the key exists, as in PostgreSQL.
- **The console lags by a few seconds.** It reuses its copy of the
  schema briefly, so a table whose row security was just enabled may
  show a filtered role its range boundaries for that long.
- **Policies are evaluated with today's definitions.** A historical
  read does not bring back an older, laxer policy.

Policy changes and `BYPASSRLS` grants are audit-logged (`policy-ddl`,
`role-ddl`) with their principal.

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
| `/`, `/status`, `/api/cluster` | any database user (read-only); range boundary keys and table names are shown for the tables the user may read, and at their table-and-index prefix by id for the rest — a boundary is a row value |
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
In insecure mode the admin surface is open, like everything else. One
exception: a request carrying a backup key (`datax backup` /
`datax restore` with `--backup-key`) is accepted on an insecure node only
from the node's own machine, since the key would otherwise cross the
network in the clear.

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
- attempts refused before any password was checked
  (`auth-throttled`, with the door — `path` — and the cause, never the
  username; `datax_auth_throttled_total`), and SQL connections closed
  before authentication completed (`sql-preauth-closed`: a handshake
  that did not finish within `--auth-timeout`, or one refused because
  the node held `--sql-max-pending-auth` unauthenticated connections
  already — a client refused that way sees a FATAL `53300`, or, if it
  was mid-`SSLRequest`, a failed TLS negotiation: pgx reports "server
  refused TLS connection", and this record is what says why;
  `datax_sql_preauth_closed_total`)
- console sign-ins and sign-outs (`http-login`, `http-logout`)
- denied admin operations (`datax_admin_denied_total`)
- executed state-changing admin RPCs (op, principal, target)
- role and privilege DDL: `CREATE`/`ALTER`/`DROP ROLE` and `USER`,
  `GRANT`/`REVOKE` (privileges and memberships), ownership changes,
  `ALTER DEFAULT PRIVILEGES` — each with the session user (`principal`)
  and the current role (`role`, different after `SET ROLE`)
- `datax debug unsafe-recover` (offline: records the OS user)

## Audit table

Once the cluster has finalized v21 the same records are also kept in the
cluster, in the `datax_audit` system table: durable, readable with SQL,
exportable, and built so that a record removed or changed afterwards
shows. The log lines stay exactly as they were; the table is the copy
that outlives a node's log rotation and can be handed to a reviewer.

```sql
SELECT at, node, principal, event, target, outcome, detail
  FROM datax_audit WHERE at > now() - INTERVAL '1 day' ORDER BY at;
```

| column | |
|---|---|
| `node`, `seq` | the node that recorded it and the record's position in that node's chain (the primary key) |
| `at` | when the event happened (not when it was written) |
| `event` | the kind of record — the names the log uses: `role-ddl`, `privilege-ddl`, `sql-auth-failure`, `admin-op`, `admin-denied`, `unseal-share`, ... |
| `principal` | who acted; `node:<id>` for the records the table writes about itself |
| `target` | what it acted on: the role, table or database of a DDL statement, the op of an admin call, the path of an HTTP request |
| `outcome` | `ok`, `failed` (authentication), `denied` (a refusal), or what the operation itself reported (`error`, `refused`, `accepted`, ...) |
| `detail` | the record's remaining attributes, a JSON object |
| `prev_hash`, `hash` | the hash of the node's previous record, and this record's own (SHA-256 over every other column) |

**Who can read it.** Admins, and any role given the table by name:
`GRANT SELECT ON datax_audit TO siem_export` (an admin grants it, and
not `WITH GRANT OPTION`). The records name people and the addresses they
connected from, so `read_all` does not include it.

**Who can change it.** Nobody, through SQL. `INSERT`, `UPDATE`,
`DELETE`, `TRUNCATE`, `DROP TABLE`, every `ALTER TABLE` but the one
below, `CREATE INDEX`, a foreign key that references it, a change of
owner, and any grant but `SELECT` are refused for every role — `root`
and admins included — with `42501`. Only the nodes write it.

**How long records are kept.** 90 days unless set otherwise:

```sql
ALTER TABLE datax_audit SET (retention = '400d');   -- admins; at least '1d'
```

The change is itself a record (`audit-retention-set`). Each node ages
out the old end of its own chain (every ten minutes, a bounded number of
records a pass), and before it removes anything it writes an
`audit-retention` record saying which node's chain was cut and at which
record. The chain of a node that has left the cluster is aged out by the
node leading range 1.

**What is never in it.** No call site passes a credential to the audit
stream — a role's password, an unseal share, a backup key and a store
key are not in the log either — and the table drops any attribute named
like one as a second line of defence. It also holds no row values: the
one place the log carries one (the boundary key of a manual split) is
recorded in the table as `[not recorded]`. Statement text is not
recorded at all; a DDL record is its kind and its target.

**When the table cannot be written.** Recording an event never waits on
the cluster and never fails the thing being recorded: the node writes
the log line, puts the event in a buffer in memory (4096 events), and a
background loop writes the buffer to the table, about once a second. A
node that has lost quorum, or is partitioned, keeps what the buffer
holds and writes it — in order, with the times the events happened —
when it can. Past the buffer's bound events are dropped from the table
(never from the log) and counted; the count becomes an `audit-gap`
record when writing resumes. A node that is killed loses what its buffer
held: the log is then the only record of those events, which is why the
log lines stay.

Refusals are the one kind of event a stranger can produce without
limit, so they are bounded separately: repeats of the same refusal
(same event, principal, address) within a flush become one record with
a `count`, and distinct refusals are recorded at no more than 20 a
second per node after a burst of 200; the rest are counted into the
next `audit-gap` record as `suppressed`. The counters
(`datax_auth_failures_total` and the others above) and the log see
every one.

`/api/security` reports the serving node's state under `audit`: the
head of its chain, the events buffered, dropped and suppressed, and the
last write error. The metrics are `datax_audit_records_total`,
`datax_audit_buffered`, `datax_audit_dropped_total{cause}`,
`datax_audit_write_errors_total` and `datax_audit_aged_out_total`.

### Tamper evidence

Each node keeps one chain: a record carries the hash of the node's
previous record, and its own hash covers all of its columns. There is a
chain per node rather than one for the cluster so that no node waits on
another, or on a single hot row, to record an event, and so a node can
go on recording into its buffer while partitioned.

```sh
datax debug audit export --url postgres://siem_export@db:26433/datax > audit.jsonl
datax debug audit heads  --url ... > heads-2026-11-15.json    # keep this somewhere else
datax debug audit verify --url ... --heads heads-2026-11-01.json
datax debug audit verify --file audit.jsonl                   # no cluster needed
```

`export` writes every record as one JSON object per line (the columns
above, with `at_us` — microseconds since the epoch — for the time).
`verify` recomputes every hash and follows every link, from the cluster
or from an export, and exits non-zero if anything is wrong. It finds:

- a record **altered** (its hash no longer matches);
- a record **removed** from inside a chain (a gap in `seq`), or from its
  old end without an `audit-retention` record that accounts for it;
- records **reordered** or replaced (a record's `prev_hash` is not its
  predecessor's hash).

The newest records of a chain can be removed without breaking any link.
Three things catch that. The node itself remembers the head it last
wrote, checks it on every write, and if the table no longer has it
writes an `audit-chain-restart` record giving the head it lost before
carrying on. The node leading range 1 writes every chain's head into its
own chain every ten minutes (`audit-anchor`), and `verify` holds each
chain to what the others recorded of it. And `verify --heads` holds the
chains to heads you saved earlier.

That last one matters, and here is the limit of the rest: the hashes
are not keyed, so someone who can write the table's keys directly —
which SQL allows nobody, but an operator with the node certificate and
KV access can — could rewrite every chain from some point on,
consistently, and `verify` against the table alone would pass. What they
cannot rewrite is a head that has left the cluster. Export the heads on
a schedule (`datax debug audit heads`, a few lines of JSON) to wherever
your other audit evidence goes; verification against a saved head proves
that every record up to it is the one that was there when it was saved.
`verify` says which of the two it did.

What happens to a chain:

- **A node restart** continues it: the node reads its head from the
  table and the next record follows it.
- **A decommissioned node's** chain simply ends. It stays verifiable,
  and ages out under the retention like any other.
- **A restore** replaces the audit table with the backup's (backups
  include it). Records the target cluster had written are removed from
  the table — they remain in its nodes' logs — and each node then finds
  that the head it remembers is gone and writes `audit-chain-restart`
  before carrying on from the restored chain. Heads saved from the
  source cluster up to the backup still verify.

What the table does not record: the offline commands (`datax debug
unsafe-recover`, `rotate-enc-key`, `enc-root`), which run with no
cluster to write to and stay in the log of the machine they ran on;
successful SQL logins and individual statements, which the audit log
does not record either; and anything from before v21 was finalized.

## What secure mode does not do

- No column-level SQL privileges — grants are per table (or sequence,
  database, schema). Rows are restricted with
  [row-level security](#row-level-security).
- No certificate revocation — rotate the CA to invalidate issued certs.
