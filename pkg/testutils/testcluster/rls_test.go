package testcluster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// Row-level security (issue #294), over the wire with pgx.

const (
	rlsTenantA = "11111111-1111-1111-1111-111111111111"
	rlsTenantB = "22222222-2222-2222-2222-222222222222"
)

// rlsConn connects to a node as user (the test cluster trusts the name).
func rlsConn(t *testing.T, ctx context.Context, tc *TestCluster, node int, user string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s@%s/datax?sslmode=disable", user, tc.Nodes[node].SQLAddr()))
	if err != nil {
		t.Fatalf("connect as %s: %v", user, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func rlsExec(t *testing.T, ctx context.Context, conn *pgx.Conn, stmt string, args ...any) int64 {
	t.Helper()
	tag, err := conn.Exec(ctx, stmt, args...)
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	return tag.RowsAffected()
}

// rlsCode is the SQLSTATE of err ("" for nil).
func rlsCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	if err != nil {
		return "not a server error: " + err.Error()
	}
	return ""
}

// rlsRefused asserts that stmt fails with code, and returns the message.
func rlsRefused(t *testing.T, ctx context.Context, conn *pgx.Conn, code, stmt string, args ...any) string {
	t.Helper()
	_, err := conn.Exec(ctx, stmt, args...)
	if rlsCode(err) != code {
		t.Fatalf("%s: got %v, want SQLSTATE %s", stmt, err, code)
	}
	return err.Error()
}

// rlsRows runs a query and returns each row rendered as text, in the
// order the server returned them.
func rlsRows(t *testing.T, ctx context.Context, conn *pgx.Conn, query string, args ...any) []string {
	t.Helper()
	out, err := rlsTryRows(ctx, conn, query, args...)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return out
}

func rlsTryRows(ctx context.Context, conn *pgx.Conn, query string, args ...any) ([]string, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			switch x := v.(type) {
			case [16]byte:
				parts[i] = fmt.Sprintf("%x-%x-%x-%x-%x", x[0:4], x[4:6], x[6:8], x[8:10], x[10:16])
			default:
				parts[i] = fmt.Sprint(v)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out, rows.Err()
}

// rlsWant asserts a query's rows (as text, in order).
func rlsWant(t *testing.T, ctx context.Context, conn *pgx.Conn, want []string, query string, args ...any) {
	t.Helper()
	if got := rlsRows(t, ctx, conn, query, args...); !reflect.DeepEqual(got, want) {
		t.Fatalf("%s:\n got %v\nwant %v", query, got, want)
	}
}

// rlsSettle polls a read until it returns want: a policy rides the
// table descriptor, and a gateway other than the one that ran the DDL
// may serve one more statement from the version before (issue #61).
func rlsSettle(t *testing.T, ctx context.Context, conn *pgx.Conn, want []string, query string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		got, err := rlsTryRows(ctx, conn, query, args...)
		if err == nil && reflect.DeepEqual(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: never settled:\n got %v (%v)\nwant %v", query, got, err, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func rlsSeq(from, to int) []string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, fmt.Sprint(i))
	}
	return out
}

func rlsTenant(t *testing.T, ctx context.Context, conn *pgx.Conn, tenant string) {
	t.Helper()
	rlsExec(t, ctx, conn, `SET app.tenant = '`+tenant+`'`)
}

// rlsCluster is three nodes with a short descriptor lease. A schema
// change returns once no gateway can still be using the version before
// it, which may mean waiting out a lease; these tests change policies
// dozens of times with readers on every gateway, and at the default ten
// seconds would mostly wait.
func rlsCluster(t *testing.T) *TestCluster {
	t.Helper()
	return StartWithOptions(t, 3, func(cfg *server.Config) { cfg.DescLeaseTTL = time.Second })
}

// rlsFixture builds the multi-tenant schema the tests share: an owner, a
// tenant role whose tenant is the session's app.tenant, and an auditor
// that bypasses row security; two tenants' rows in every table, with
// the same ids so that a leak shows as a duplicate or a wrong value.
type rlsFixture struct {
	tc                        *TestCluster
	root, owner, app, auditor *pgx.Conn
}

func newRLSFixture(t *testing.T, ctx context.Context) *rlsFixture {
	t.Helper()
	tc := rlsCluster(t)
	f := &rlsFixture{tc: tc, root: rlsConn(t, ctx, tc, 0, "root")}
	for _, stmt := range []string{
		`CREATE USER app_owner PASSWORD 'owner-pw-1'`,
		`CREATE USER app PASSWORD 'app-pw-123'`,
		`CREATE USER auditor PASSWORD 'audit-pw-1' BYPASSRLS`,
		`GRANT CREATE ON DATABASE datax TO app_owner`,
		`GRANT CREATE ON DATABASE datax TO app`,
		`GRANT app TO app_owner`,
	} {
		rlsExec(t, ctx, f.root, stmt)
	}
	// The three roles connect through three different gateways.
	f.owner = rlsConn(t, ctx, tc, 0, "app_owner")
	f.app = rlsConn(t, ctx, tc, 1, "app")
	f.auditor = rlsConn(t, ctx, tc, 2, "auditor")
	for _, stmt := range []string{
		`CREATE TABLE docs (tenant_id UUID, id INT8, title TEXT, slug TEXT, n INT8, body JSONB, PRIMARY KEY (tenant_id, id))`,
		`CREATE INDEX docs_title ON docs (title)`,
		`CREATE UNIQUE INDEX docs_slug ON docs (slug)`,
		`CREATE INDEX docs_body ON docs USING GIN (body)`,
		`CREATE TABLE notes (tenant_id UUID, id INT8, doc_id INT8, author TEXT, txt TEXT, PRIMARY KEY (tenant_id, id),
			CONSTRAINT notes_doc FOREIGN KEY (tenant_id, doc_id) REFERENCES docs (tenant_id, id) ON DELETE CASCADE)`,
		`CREATE TABLE items (id INT8 PRIMARY KEY, tenant_id UUID, v INT8)`,
		`CREATE INDEX items_tenant ON items (tenant_id)`,
		// tagged has no index the policy can use: its reads go through
		// the indexes on the columns a query names.
		`CREATE TABLE tagged (id INT8 PRIMARY KEY, tenant_id UUID, tag TEXT, code TEXT, body JSONB)`,
		`CREATE INDEX tagged_tag ON tagged (tag)`,
		`CREATE UNIQUE INDEX tagged_code ON tagged (code)`,
		`CREATE INDEX tagged_body ON tagged USING GIN (body)`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON tagged TO app`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON docs TO app`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON notes TO app`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON items TO app`,
		`GRANT SELECT ON docs TO auditor`,
		`GRANT SELECT ON notes TO auditor`,
		`GRANT SELECT ON items TO auditor`,
	} {
		rlsExec(t, ctx, f.owner, stmt)
	}
	// Tenant A: docs 1..5 (n = 10*id); tenant B: docs 1..7 (n = 100*id).
	for _, tn := range []struct {
		id, tag string
		docs, k int
	}{{rlsTenantA, "a", 5, 10}, {rlsTenantB, "b", 7, 100}} {
		for i := 1; i <= tn.docs; i++ {
			title := fmt.Sprintf("%s-title-%d", tn.tag, i)
			if i == 1 {
				title = "shared-title"
			}
			rlsExec(t, ctx, f.owner, `INSERT INTO docs VALUES ($1, $2, $3, $4, $5, $6)`,
				tn.id, int64(i), title, fmt.Sprintf("%s-%d", tn.tag, i), int64(i*tn.k), fmt.Sprintf(`{"k": %d, "t": "%s"}`, i%2, tn.tag))
		}
	}
	for _, n := range []struct {
		tenant  string
		id, doc int
		author  string
	}{{rlsTenantA, 1, 1, "visible"}, {rlsTenantA, 2, 1, "visible"}, {rlsTenantA, 3, 2, "visible"}, {rlsTenantB, 1, 1, "visible"}, {rlsTenantB, 2, 3, "visible"}} {
		rlsExec(t, ctx, f.owner, `INSERT INTO notes VALUES ($1, $2, $3, $4, 'text')`, n.tenant, int64(n.id), int64(n.doc), n.author)
	}
	// items: ids 1..4 tenant A, 5..10 tenant B.
	for i := 1; i <= 10; i++ {
		tenant := rlsTenantA
		if i > 4 {
			tenant = rlsTenantB
		}
		rlsExec(t, ctx, f.owner, `INSERT INTO items VALUES ($1, $2, $3)`, int64(i), tenant, int64(i))
	}
	// tagged: ids 1..6, alternating tenants; every row has the same tag.
	for i := 1; i <= 6; i++ {
		tenant, tn := rlsTenantA, "a"
		if i%2 == 0 {
			tenant, tn = rlsTenantB, "b"
		}
		rlsExec(t, ctx, f.owner, `INSERT INTO tagged VALUES ($1, $2, 'common', $3, $4)`, int64(i), tenant, fmt.Sprintf("code-%d", i), fmt.Sprintf(`{"t": "%s", "all": 1}`, tn))
	}
	for _, table := range []string{"docs", "notes", "items", "tagged"} {
		rlsExec(t, ctx, f.owner, `ALTER TABLE `+table+` ENABLE ROW LEVEL SECURITY`)
		rlsExec(t, ctx, f.owner, `CREATE POLICY tenant_isolation ON `+table+` USING (tenant_id = current_setting('app.tenant')::uuid)`)
	}
	rlsTenant(t, ctx, f.app, rlsTenantA)
	rlsSettle(t, ctx, f.app, rlsSeq(1, 5), `SELECT id FROM docs ORDER BY id`)
	rlsSettle(t, ctx, f.app, rlsSeq(1, 3), `SELECT id FROM notes ORDER BY id`)
	rlsSettle(t, ctx, f.app, rlsSeq(1, 4), `SELECT id FROM items ORDER BY id`)
	return f
}

// TestRowSecurityReads: every way a statement reads rows returns the
// tenant role its own tenant's rows and nothing of the other's — plain
// and indexed scans, the inverted index, joins and subqueries (the
// policy applies to each table reference), aggregates, EXPLAIN and the
// statistics a role can see — while the owner and the BYPASSRLS role
// read everything.
func TestRowSecurityReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRLSFixture(t, ctx)
	app, owner, auditor := f.app, f.owner, f.auditor

	t.Run("plain select and the three roles", func(t *testing.T) {
		rlsWant(t, ctx, app, rlsSeq(1, 5), `SELECT id FROM docs ORDER BY id`)
		rlsWant(t, ctx, app, []string{rlsTenantA}, `SELECT DISTINCT tenant_id FROM docs`)
		rlsWant(t, ctx, owner, []string{"12"}, `SELECT count(*) FROM docs`)
		rlsWant(t, ctx, auditor, []string{"12"}, `SELECT count(*) FROM docs`)
		rlsWant(t, ctx, f.root, []string{"12"}, `SELECT count(*) FROM docs`)
		// The other tenant, on the same connection: the setting decides.
		rlsTenant(t, ctx, app, rlsTenantB)
		rlsWant(t, ctx, app, rlsSeq(1, 7), `SELECT id FROM docs ORDER BY id`)
		rlsTenant(t, ctx, app, rlsTenantA)
		// Naming the other tenant in WHERE finds nothing.
		rlsWant(t, ctx, app, []string{}, `SELECT id FROM docs WHERE tenant_id = $1`, rlsTenantB)
		rlsWant(t, ctx, app, []string{}, `SELECT id FROM docs WHERE tenant_id = $1 AND id = 6`, rlsTenantB)
		rlsWant(t, ctx, app, []string{}, `SELECT id FROM docs WHERE id > 5`)
		// A session that never named a tenant reads nothing and learns
		// nothing: the policy's own expression fails.
		fresh := rlsConn(t, ctx, f.tc, 2, "app")
		if _, err := rlsTryRows(ctx, fresh, `SELECT id FROM docs`); rlsCode(err) != sql.CodeUndefinedObject {
			t.Fatalf("no tenant set: %v, want 42704", err)
		}
		// LIMIT and ORDER BY ... DESC stay inside the tenant.
		rlsWant(t, ctx, app, []string{"5", "4"}, `SELECT id FROM docs ORDER BY id DESC LIMIT 2`)
		rlsWant(t, ctx, app, []string{"50"}, `SELECT n FROM docs ORDER BY n DESC LIMIT 1`)
	})

	// Issue #345: row_security = off (pg_dump sets it) refuses a query
	// the policies would filter, instead of returning fewer rows; it
	// changes nothing for a role they do not apply to.
	t.Run("row_security off", func(t *testing.T) {
		for _, conn := range []*pgx.Conn{app, owner} {
			if _, err := conn.Exec(ctx, `SET row_security = off`); err != nil {
				t.Fatal(err)
			}
		}
		defer func() {
			for _, conn := range []*pgx.Conn{app, owner} {
				_, _ = conn.Exec(ctx, `RESET row_security`)
			}
		}()
		for _, q := range []string{`SELECT id FROM docs`, `SELECT count(*) FROM docs WHERE id = 1`} {
			_, err := rlsTryRows(ctx, app, q)
			if rlsCode(err) != sql.CodeInsufficientPriv || !strings.Contains(err.Error(), `query would be affected by row-level security policy for table "docs"`) {
				t.Fatalf("%s with row_security off: %v, want 42501", q, err)
			}
		}
		if _, err := app.Exec(ctx, `UPDATE docs SET n = n WHERE id = 1`); rlsCode(err) != sql.CodeInsufficientPriv || !strings.Contains(err.Error(), "query would be affected") {
			t.Fatalf("UPDATE with row_security off: %v, want 42501", err)
		}
		rlsWant(t, ctx, owner, []string{"12"}, `SELECT count(*) FROM docs`)
		if _, err := app.Exec(ctx, `RESET row_security`); err != nil {
			t.Fatal(err)
		}
		rlsWant(t, ctx, app, rlsSeq(1, 5), `SELECT id FROM docs ORDER BY id`)
	})

	t.Run("secondary, unique and inverted indexes", func(t *testing.T) {
		planOf := func(conn *pgx.Conn, q string) string {
			t.Helper()
			return rlsRows(t, ctx, conn, `EXPLAIN `+q)[0]
		}
		// Every row of tagged carries the same tag, half of them the
		// other tenant's. The index finds all six entries; the policy
		// keeps three, whatever columns the statement asks for — there
		// is no index-only answer to leak through.
		byTag := `SELECT id FROM tagged WHERE tag = 'common' ORDER BY id`
		if plan := planOf(app, byTag); !strings.Contains(plan, "tagged_tag") {
			t.Fatalf("not an index scan, so the test proves nothing: %s", plan)
		}
		rlsWant(t, ctx, app, []string{"1", "3", "5"}, byTag)
		rlsWant(t, ctx, app, []string{"common", "common", "common"}, `SELECT tag FROM tagged WHERE tag = 'common'`)
		rlsWant(t, ctx, app, []string{"3"}, `SELECT count(*) FROM tagged WHERE tag = 'common'`)
		rlsWant(t, ctx, app, []string{"1"}, `SELECT id FROM tagged WHERE tag = 'common' ORDER BY id LIMIT 1`)
		rlsWant(t, ctx, app, []string{"3"}, `SELECT count(*) FROM tagged WHERE tag >= 'a' AND tag < 'z'`)
		rlsWant(t, ctx, owner, []string{"6"}, `SELECT count(*) FROM tagged WHERE tag = 'common'`)
		// The unique index: a point read of the other tenant's entry is
		// an empty result — not the row, and not an error about it.
		byCode := `SELECT id FROM tagged WHERE code = 'code-2'`
		if plan := planOf(app, byCode); !strings.Contains(plan, "tagged_code") {
			t.Fatalf("not a unique-index lookup: %s", plan)
		}
		rlsWant(t, ctx, app, []string{}, byCode)
		rlsWant(t, ctx, app, []string{"1"}, `SELECT id FROM tagged WHERE code = 'code-1'`)
		rlsWant(t, ctx, owner, []string{"2"}, byCode)
		// The same through docs' indexes, where the policy's own bound
		// (the tenant's key prefix) is the cheaper path.
		rlsWant(t, ctx, app, []string{"1|10"}, `SELECT id, n FROM docs WHERE title = 'shared-title'`)
		rlsWant(t, ctx, owner, []string{"2"}, `SELECT count(*) FROM docs WHERE title = 'shared-title'`)
		rlsWant(t, ctx, app, []string{}, `SELECT title FROM docs WHERE title >= 'b-' AND title < 'b-zzz'`)
		rlsWant(t, ctx, app, []string{}, `SELECT id FROM docs WHERE slug = 'b-1'`)
		rlsWant(t, ctx, app, []string{"1"}, `SELECT id FROM docs WHERE slug = 'a-1'`)
		// The inverted index, forced: every row matches the containment.
		rlsExec(t, ctx, app, `SET inverted_index_scan = on`)
		contains := `SELECT id FROM tagged WHERE body @> '{"all": 1}' ORDER BY id`
		if plan := planOf(app, contains); !strings.Contains(plan, "tagged_body") {
			t.Fatalf("not an inverted scan: %s", plan)
		}
		rlsWant(t, ctx, app, []string{"1", "3", "5"}, contains)
		rlsWant(t, ctx, app, []string{}, `SELECT id FROM tagged WHERE body @> '{"t": "b"}'`)
		rlsWant(t, ctx, app, []string{"3"}, `SELECT count(*) FROM tagged WHERE body @> '{"t": "a"}'`)
		rlsWant(t, ctx, app, []string{"1", "3", "5"}, `SELECT id FROM docs WHERE body @> '{"k": 1}' ORDER BY id`)
		rlsExec(t, ctx, app, `SET inverted_index_scan = auto`)
		rlsExec(t, ctx, owner, `SET inverted_index_scan = on`)
		rlsWant(t, ctx, owner, []string{"6"}, `SELECT count(*) FROM tagged WHERE body @> '{"all": 1}'`)
		rlsExec(t, ctx, owner, `SET inverted_index_scan = auto`)
	})

	t.Run("joins and subqueries", func(t *testing.T) {
		// Each table reference is filtered: a join that does not mention
		// the tenant still pairs only the tenant's own rows.
		rlsWant(t, ctx, app, []string{"1|1", "1|2", "2|3"}, `SELECT d.id, n.id FROM docs d JOIN notes n ON n.doc_id = d.id ORDER BY d.id, n.id`)
		rlsWant(t, ctx, app, []string{"1|1", "1|2", "2|3"}, `SELECT d.id, n.id FROM notes n JOIN docs d ON n.doc_id = d.id ORDER BY d.id, n.id`)
		rlsWant(t, ctx, app, []string{"5"}, `SELECT count(*) FROM docs a JOIN docs b ON a.id = b.id`)
		rlsWant(t, ctx, app, []string{"1|1", "1|2", "2|3", "3|<nil>", "4|<nil>", "5|<nil>"},
			`SELECT d.id, n.id FROM docs d LEFT JOIN notes n ON n.doc_id = d.id ORDER BY d.id, n.id`)
		rlsWant(t, ctx, app, []string{"3"}, `SELECT count(*) FROM docs d JOIN notes n ON n.tenant_id = d.tenant_id AND n.doc_id = d.id`)
		rlsWant(t, ctx, app, []string{"1", "2"}, `SELECT id FROM docs WHERE id IN (SELECT doc_id FROM notes) ORDER BY id`)
		rlsWant(t, ctx, app, []string{"3", "4", "5"}, `SELECT id FROM docs WHERE id NOT IN (SELECT doc_id FROM notes) ORDER BY id`)
		rlsWant(t, ctx, app, []string{"1", "2"}, `SELECT id FROM docs d WHERE EXISTS (SELECT 1 FROM notes n WHERE n.doc_id = d.id) ORDER BY id`)
		rlsWant(t, ctx, app, []string{"3"}, `SELECT (SELECT count(*) FROM notes)`)
		rlsWant(t, ctx, app, []string{"1|2", "2|1", "3|0", "4|0", "5|0"},
			`SELECT d.id, (SELECT count(*) FROM notes n WHERE n.doc_id = d.id) FROM docs d ORDER BY d.id`)
		rlsWant(t, ctx, app, []string{"5"}, `WITH x AS (SELECT id FROM docs) SELECT count(*) FROM x`)
		rlsWant(t, ctx, app, []string{"5"}, `SELECT count(*) FROM (SELECT id FROM docs) s`)
		rlsWant(t, ctx, app, []string{"8"}, `SELECT count(*) FROM (SELECT id FROM docs UNION ALL SELECT id FROM notes) u`)
		rlsWant(t, ctx, app, rlsSeq(1, 5), `SELECT id FROM docs UNION SELECT id FROM notes ORDER BY id`)
		rlsWant(t, ctx, app, []string{"1|5", "2|5", "3|5", "4|5", "5|5"}, `SELECT id, count(*) OVER () FROM docs ORDER BY id`)
		// The owner's same statements see both tenants.
		rlsWant(t, ctx, owner, []string{"5"}, `SELECT count(*) FROM docs d JOIN notes n ON n.tenant_id = d.tenant_id AND n.doc_id = d.id`)
	})

	t.Run("aggregates and counts", func(t *testing.T) {
		rlsWant(t, ctx, app, []string{"5|150|5|1"}, `SELECT count(*), sum(n), max(id), min(id) FROM docs`)
		rlsWant(t, ctx, app, []string{"0"}, `SELECT count(*) FROM docs WHERE id > 5`)
		rlsWant(t, ctx, app, []string{rlsTenantA + "|5"}, `SELECT tenant_id, count(*) FROM docs GROUP BY tenant_id`)
		rlsWant(t, ctx, app, []string{"common|3"}, `SELECT tag, count(*) FROM tagged GROUP BY tag`)
		rlsWant(t, ctx, app, []string{"1"}, `SELECT count(DISTINCT tenant_id) FROM docs`)
		rlsWant(t, ctx, app, []string{"4|10"}, `SELECT count(*), sum(v) FROM items`)
		rlsWant(t, ctx, owner, []string{"12|2950"}, `SELECT count(*), sum(n) FROM docs`)
	})

	t.Run("statistics are not shown to a confined role", func(t *testing.T) {
		rlsExec(t, ctx, f.root, `ANALYZE docs`)
		if got := rlsRows(t, ctx, owner, `SHOW STATS FOR docs`); len(got) == 0 || !strings.Contains(got[0], "|12|") {
			t.Fatalf("the owner's statistics: %v (the test proves nothing without them)", got)
		}
		if got := rlsRows(t, ctx, auditor, `SHOW STATS FOR docs`); len(got) == 0 {
			t.Fatalf("the BYPASSRLS role sees no statistics: %v", got)
		}
		rlsWant(t, ctx, app, []string{}, `SHOW STATS FOR docs`)
		rlsWant(t, ctx, f.root, []string{"12"}, `SELECT reltuples FROM pg_class WHERE relname = 'docs'`)
	})

	t.Run("EXPLAIN", func(t *testing.T) {
		// The policy bounds the scan to the tenant's primary-key prefix,
		// and a key predicate on top of it is a point lookup.
		plan := rlsRows(t, ctx, app, `EXPLAIN SELECT * FROM docs`)[0]
		if !strings.Contains(plan, "range scan of primary key") || !strings.Contains(plan, "tenant_id = "+rlsTenantA) || !strings.Contains(plan, "row-level security policy applied") {
			t.Fatalf("tenant scan: %s", plan)
		}
		if strings.Contains(plan, "~") || strings.Contains(plan, rlsTenantB) {
			t.Fatalf("the plan carries a table-wide estimate or the other tenant: %s", plan)
		}
		if plan = rlsRows(t, ctx, app, `EXPLAIN SELECT * FROM docs WHERE id = 3`)[0]; !strings.Contains(plan, "point lookup on primary key") {
			t.Fatalf("tenant point lookup: %s", plan)
		}
		// A policy on an indexed, non-key column plans as an index scan.
		if plan = rlsRows(t, ctx, app, `EXPLAIN SELECT * FROM items`)[0]; !strings.Contains(plan, "items_tenant") || strings.Contains(plan, "full table scan") {
			t.Fatalf("policy on an indexed column: %s", plan)
		}
		// The owner's plan is the unfiltered one.
		if plan = rlsRows(t, ctx, owner, `EXPLAIN SELECT * FROM docs`)[0]; !strings.Contains(plan, "full table scan") || strings.Contains(plan, "row-level security") {
			t.Fatalf("owner: %s", plan)
		}
		plan = rlsRows(t, ctx, app, `EXPLAIN SELECT d.id FROM docs d JOIN notes n ON n.doc_id = d.id`)[0]
		if !strings.Contains(plan, "row-level security policy applied") || strings.Contains(plan, "~") {
			t.Fatalf("join: %s", plan)
		}
		// EXPLAIN ANALYZE runs the statement: it reads the tenant's rows.
		analyzed := strings.Join(rlsRows(t, ctx, app, `EXPLAIN ANALYZE SELECT id FROM docs`), "\n")
		if !strings.Contains(analyzed, "5 rows") || strings.Contains(analyzed, "12 rows") {
			t.Fatalf("EXPLAIN ANALYZE: %s", analyzed)
		}
	})

	t.Run("the scan stays inside the tenant", func(t *testing.T) {
		// What a tenant-scoped query costs is its own tenant's rows: the
		// gateway's count of rows read from KV moves by 5 for tenant A
		// and by 7 for tenant B, on a table of 12 that the owner's same
		// statement reads in full. (The counter is the process's, so a
		// reading that something else moved is taken again.)
		scanned := func(conn *pgx.Conn, q string, want float64) {
			t.Helper()
			var got float64
			for attempt := 0; attempt < 5; attempt++ {
				before := testutil.ToFloat64(metrics.SQLRowsScanned)
				rlsRows(t, ctx, conn, q)
				if got = testutil.ToFloat64(metrics.SQLRowsScanned) - before; got == want {
					return
				}
			}
			t.Fatalf("%s read %v rows from KV, want %v", q, got, want)
		}
		scanned(app, `SELECT id, title FROM docs`, 5)
		scanned(app, `SELECT count(*) FROM docs`, 5)
		scanned(app, `SELECT id FROM docs WHERE n >= 0`, 5)
		scanned(app, `SELECT title FROM docs WHERE id = 3`, 0) // a point read: no scan at all
		rlsTenant(t, ctx, app, rlsTenantB)
		scanned(app, `SELECT id, title FROM docs`, 7)
		rlsTenant(t, ctx, app, rlsTenantA)
		scanned(owner, `SELECT id, title FROM docs`, 12)
		// The policy on items is served by the index on its column.
		scanned(app, `SELECT id FROM items`, 4)
		scanned(owner, `SELECT id FROM items`, 10)
		if out := strings.Join(rlsRows(t, ctx, app, `EXPLAIN ANALYZE SELECT id FROM docs`), "\n"); !strings.Contains(out, "5 rows") {
			t.Fatalf("EXPLAIN ANALYZE as the tenant: %s", out)
		}
	})

	t.Run("views run as their owner", func(t *testing.T) {
		// A view owned by the table's owner reads as the owner: every
		// row. That is PostgreSQL's default for views, and the reason a
		// view over a protected table should belong to a confined role.
		rlsExec(t, ctx, owner, `CREATE VIEW all_docs AS SELECT tenant_id, id FROM docs`)
		rlsExec(t, ctx, owner, `GRANT SELECT ON all_docs TO app`)
		rlsSettle(t, ctx, app, []string{"12"}, `SELECT count(*) FROM all_docs`)
		// A view owned by the tenant role is filtered by its policies —
		// for whoever reads it, with the reader's own setting.
		rlsExec(t, ctx, app, `CREATE VIEW my_docs AS SELECT id, n FROM docs`)
		rlsExec(t, ctx, app, `GRANT SELECT ON my_docs TO app_owner`)
		rlsWant(t, ctx, app, []string{"5"}, `SELECT count(*) FROM my_docs`)
		rlsTenant(t, ctx, owner, rlsTenantB)
		rlsSettle(t, ctx, owner, []string{"7"}, `SELECT count(*) FROM my_docs`)
		rlsWant(t, ctx, owner, []string{"12"}, `SELECT count(*) FROM docs`)
		rlsExec(t, ctx, owner, `RESET app.tenant`)
	})

	t.Run("catalogs", func(t *testing.T) {
		rlsWant(t, ctx, f.root, []string{"public|docs|tenant_isolation|PERMISSIVE|{public}|ALL|tenant_id = current_setting('app.tenant')::uuid|<nil>"},
			`SELECT schemaname, tablename, policyname, permissive, roles, cmd, qual, with_check FROM pg_policies WHERE tablename = 'docs'`)
		rlsWant(t, ctx, f.root, []string{"tenant_isolation|*|true"}, `SELECT polname, polcmd, polpermissive FROM pg_policy WHERE polrelid = 'docs'::regclass`)
		rlsWant(t, ctx, f.root, []string{"true|false"}, `SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = 'docs'`)
		rlsWant(t, ctx, f.root, []string{"true"}, `SELECT rowsecurity FROM pg_tables WHERE tablename = 'docs'`)
		rlsWant(t, ctx, f.root, []string{"app|false", "auditor|true", "root|true"}, `SELECT rolname, rolbypassrls FROM pg_roles WHERE rolname IN ('app', 'auditor', 'root') ORDER BY rolname`)
		def := rlsRows(t, ctx, owner, `SHOW CREATE TABLE docs`)[0]
		for _, want := range []string{"ALTER TABLE docs ENABLE ROW LEVEL SECURITY", "CREATE POLICY tenant_isolation ON docs USING (tenant_id = current_setting('app.tenant')::uuid)"} {
			if !strings.Contains(def, want) {
				t.Fatalf("SHOW CREATE TABLE lacks %q: %s", want, def)
			}
		}
	})

	t.Run("another role's statement text", func(t *testing.T) {
		// A statement's text carries its literals: the tenant role reads
		// only its own sessions' in pg_stat_activity and SHOW SESSIONS.
		spy := rlsConn(t, ctx, f.tc, 1, "app")
		other := rlsConn(t, ctx, f.tc, 1, "app_owner")
		rlsExec(t, ctx, other, `SELECT count(*) FROM docs WHERE slug = 'b-secret-literal'`)
		rlsTenant(t, ctx, spy, rlsTenantA)
		own := false
		for _, row := range rlsRows(t, ctx, spy, `SHOW SESSIONS`) {
			if strings.Contains(row, "b-secret-literal") {
				t.Fatalf("SHOW SESSIONS shows the tenant role another role's statement: %s", row)
			}
			own = own || strings.Contains(row, "SHOW SESSIONS")
		}
		if !own {
			t.Fatal("the tenant role does not see its own session's statement")
		}
		seen := false
		// Each node lists its own sessions: ask the one they are on.
		admin := rlsConn(t, ctx, f.tc, 1, "root")
		for _, row := range rlsRows(t, ctx, admin, `SELECT usename, query FROM pg_stat_activity`) {
			seen = seen || strings.Contains(row, "b-secret-literal")
		}
		if !seen {
			t.Fatal("an admin does not see the statement either: the test proves nothing")
		}
	})
}

// TestRowSecurityWrites: no statement lets the tenant role insert,
// update or delete another tenant's row, or move one of its own out of
// its tenant; and what an error tells it about a row it cannot see is
// what PostgreSQL would tell it.
func TestRowSecurityWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRLSFixture(t, ctx)
	app, owner := f.app, f.owner
	const rlsMsg = `new row violates row-level security policy for table`
	// otherTenant is what the owner sees of tenant B, which no statement
	// of the tenant role below may change.
	otherTenant := func() []string {
		return append(rlsRows(t, ctx, owner, `SELECT id, title, slug, n FROM docs WHERE tenant_id = $1 ORDER BY id`, rlsTenantB),
			rlsRows(t, ctx, owner, `SELECT id, v FROM items WHERE tenant_id = $1 ORDER BY id`, rlsTenantB)...)
	}
	before := otherTenant()
	unchanged := func(t *testing.T) {
		t.Helper()
		if got := otherTenant(); !reflect.DeepEqual(got, before) {
			t.Fatalf("tenant B's rows changed:\n got %v\nwant %v", got, before)
		}
	}

	t.Run("INSERT", func(t *testing.T) {
		if n := rlsExec(t, ctx, app, `INSERT INTO docs VALUES ($1, 50, 'mine', 'a-50', 500, '{}')`, rlsTenantA); n != 1 {
			t.Fatalf("own insert: %d rows", n)
		}
		// Another tenant's key is refused the same way whether or not a
		// row with that key exists: the policy is checked before the
		// table is probed, so the refusal is not an existence test.
		exists := rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO docs VALUES ($1, 1, 'x', 'x-1', 1, '{}')`, rlsTenantB)
		absent := rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO docs VALUES ($1, 999, 'x', 'x-2', 1, '{}')`, rlsTenantB)
		if exists != absent || !strings.Contains(exists, rlsMsg) || strings.Contains(exists, rlsTenantB) {
			t.Fatalf("refusals differ or carry row data:\n%s\n%s", exists, absent)
		}
		// NULL in the policy column: the check is not TRUE, so refused.
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO items VALUES (700, NULL, 1)`)
		// Several rows, one of them foreign: nothing is written.
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO items VALUES (701, $1, 1), (702, $2, 1)`, rlsTenantA, rlsTenantB)
		rlsWant(t, ctx, owner, []string{"0"}, `SELECT count(*) FROM items WHERE id >= 700`)
		// INSERT ... SELECT copies what the role can read.
		if n := rlsExec(t, ctx, app, `INSERT INTO items SELECT id + 1000, tenant_id, v FROM items`); n != 4 {
			t.Fatalf("INSERT ... SELECT copied %d rows, want the tenant's 4", n)
		}
		rlsWant(t, ctx, owner, []string{"4"}, `SELECT count(*) FROM items WHERE id > 1000`)
		rlsExec(t, ctx, owner, `DELETE FROM items WHERE id > 1000`)
		// RETURNING hands back the written row.
		rlsWant(t, ctx, app, []string{"51|" + rlsTenantA}, `INSERT INTO docs VALUES ($1, 51, 'r', 'a-51', 1, '{}') RETURNING id, tenant_id`, rlsTenantA)
		rlsExec(t, ctx, owner, `DELETE FROM docs WHERE id >= 50`)
		unchanged(t)
	})

	t.Run("errors about rows the role cannot see", func(t *testing.T) {
		// A unique index spans tenants, so a collision with another
		// tenant's value is a unique violation — as in PostgreSQL, which
		// documents this channel. The message names the constraint and
		// no value of the hidden row.
		msg := rlsRefused(t, ctx, app, sql.CodeUniqueViolation, `INSERT INTO docs VALUES ($1, 60, 'x', 'b-3', 1, '{}')`, rlsTenantA)
		for _, leak := range []string{rlsTenantB, "b-title-3", "300"} {
			if strings.Contains(msg, leak) {
				t.Fatalf("the unique violation carries the hidden row's %q: %s", leak, msg)
			}
		}
		msg = rlsRefused(t, ctx, app, sql.CodeUniqueViolation, `UPDATE docs SET slug = 'b-3' WHERE id = 2`)
		if strings.Contains(msg, rlsTenantB) {
			t.Fatalf("unique violation on update: %s", msg)
		}
		// A foreign key is checked against the whole parent table,
		// policies aside (PostgreSQL does the same): the tenant role can
		// reference a parent row it cannot read, and a missing parent is
		// a foreign-key violation naming only the key it supplied.
		rlsExec(t, ctx, owner, `CREATE TABLE links (id INT8 PRIMARY KEY, tenant_id UUID, target_tenant UUID, target_id INT8,
			CONSTRAINT links_target FOREIGN KEY (target_tenant, target_id) REFERENCES docs (tenant_id, id))`)
		rlsExec(t, ctx, owner, `GRANT SELECT, INSERT ON links TO app`)
		rlsExec(t, ctx, owner, `ALTER TABLE links ENABLE ROW LEVEL SECURITY`)
		rlsExec(t, ctx, owner, `CREATE POLICY tenant_isolation ON links USING (tenant_id = current_setting('app.tenant')::uuid)`)
		rlsSettle(t, ctx, app, []string{"0"}, `SELECT count(*) FROM links`)
		if n := rlsExec(t, ctx, app, `INSERT INTO links VALUES (1, $1, $2, 1)`, rlsTenantA, rlsTenantB); n != 1 {
			t.Fatalf("link to a parent row the role cannot read: %d rows", n)
		}
		msg = rlsRefused(t, ctx, app, sql.CodeForeignKeyViolation, `INSERT INTO links VALUES (2, $1, $2, 999)`, rlsTenantA, rlsTenantB)
		if !strings.Contains(msg, "999") || strings.Contains(msg, "b-title") {
			t.Fatalf("foreign-key violation: %s", msg)
		}
		rlsExec(t, ctx, owner, `DROP TABLE links`)
		unchanged(t)
	})

	t.Run("UPDATE", func(t *testing.T) {
		if n := rlsExec(t, ctx, app, `UPDATE docs SET n = n + 1`); n != 5 {
			t.Fatalf("UPDATE without WHERE changed %d rows, want the tenant's 5", n)
		}
		rlsExec(t, ctx, app, `UPDATE docs SET n = n - 1`)
		for _, stmt := range []string{
			`UPDATE docs SET n = 0 WHERE id = 6`,
			`UPDATE docs SET n = 0 WHERE slug = 'b-1'`,
			`UPDATE docs SET n = 0 WHERE tenant_id = '` + rlsTenantB + `'`,
			`UPDATE docs SET n = 0 WHERE tenant_id = '` + rlsTenantB + `' AND id = 1`,
			`UPDATE docs SET n = 0 WHERE n >= 100`,
			`UPDATE docs SET n = 0 WHERE id IN (SELECT id FROM docs WHERE n >= 100)`,
			`UPDATE items SET v = 0 WHERE id = 7`,
			`UPDATE items SET v = 0 WHERE id > 4`,
		} {
			if n := rlsExec(t, ctx, app, stmt); n != 0 {
				t.Fatalf("%s: changed %d rows of another tenant", stmt, n)
			}
		}
		rlsWant(t, ctx, app, []string{"2|21"}, `UPDATE docs SET n = n + 1 WHERE id = 2 RETURNING id, n`)
		rlsExec(t, ctx, app, `UPDATE docs SET n = 20 WHERE id = 2`)
		// Moving a row out of the tenant is refused, not performed.
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `UPDATE items SET tenant_id = $1 WHERE id = 1`, rlsTenantB)
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `UPDATE items SET tenant_id = $1`, rlsTenantB)
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `UPDATE items SET tenant_id = NULL WHERE id = 1`)
		rlsWant(t, ctx, owner, []string{"4"}, `SELECT count(*) FROM items WHERE tenant_id = $1`, rlsTenantA)
		unchanged(t)
	})

	t.Run("INSERT ON CONFLICT and UPSERT", func(t *testing.T) {
		// items.id is unique across tenants, so a tenant can collide
		// with a row it cannot see. DO NOTHING passes over it quietly, as
		// in PostgreSQL; DO UPDATE is refused and changes nothing.
		if n := rlsExec(t, ctx, app, `INSERT INTO items VALUES (7, $1, 99) ON CONFLICT (id) DO NOTHING`, rlsTenantA); n != 0 {
			t.Fatalf("DO NOTHING over another tenant's row wrote %d rows", n)
		}
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO items VALUES (7, $1, 99) ON CONFLICT (id) DO UPDATE SET v = 99`, rlsTenantA)
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO items VALUES (7, $1, 99) ON CONFLICT (id) DO UPDATE SET v = excluded.v, tenant_id = excluded.tenant_id`, rlsTenantA)
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `UPSERT INTO items VALUES (7, $1, 99)`, rlsTenantA)
		// The DO UPDATE's WHERE cannot be used to probe the hidden row.
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO items VALUES (7, $1, 99) ON CONFLICT (id) DO UPDATE SET v = 99 WHERE items.v = 7`, rlsTenantA)
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO items VALUES (7, $1, 99) ON CONFLICT (id) DO UPDATE SET v = 99 WHERE items.v = 8`, rlsTenantA)
		// The proposed row is checked even when the conflict path is taken.
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO items VALUES (1, $1, 99) ON CONFLICT (id) DO UPDATE SET v = 99`, rlsTenantB)
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO items VALUES (1, $1, 99) ON CONFLICT (id) DO NOTHING`, rlsTenantB)
		// Its own row: the update path works, and may not move the row.
		rlsWant(t, ctx, app, []string{"1|41"}, `INSERT INTO items VALUES (1, $1, 41) ON CONFLICT (id) DO UPDATE SET v = excluded.v RETURNING id, v`, rlsTenantA)
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `INSERT INTO items VALUES (1, $1, 41) ON CONFLICT (id) DO UPDATE SET tenant_id = $2`, rlsTenantA, rlsTenantB)
		if n := rlsExec(t, ctx, app, `UPSERT INTO items VALUES (1, $1, 1)`, rlsTenantA); n != 1 {
			t.Fatalf("UPSERT of its own row: %d", n)
		}
		rlsWant(t, ctx, owner, []string{"1|" + rlsTenantA + "|1"}, `SELECT id, tenant_id, v FROM items WHERE id = 1`)
		unchanged(t)
	})

	t.Run("COPY FROM", func(t *testing.T) {
		rows := [][]any{{int64(801), rlsTenantA, int64(1)}, {int64(802), rlsTenantA, int64(2)}}
		if n, err := app.CopyFrom(ctx, pgx.Identifier{"items"}, []string{"id", "tenant_id", "v"}, pgx.CopyFromRows(rows)); err != nil || n != 2 {
			t.Fatalf("COPY of the tenant's own rows: %d, %v", n, err)
		}
		bad := [][]any{{int64(803), rlsTenantA, int64(3)}, {int64(804), rlsTenantB, int64(4)}}
		if _, err := app.CopyFrom(ctx, pgx.Identifier{"items"}, []string{"id", "tenant_id", "v"}, pgx.CopyFromRows(bad)); rlsCode(err) != sql.CodeInsufficientPriv {
			t.Fatalf("COPY with another tenant's row: %v, want 42501", err)
		}
		rlsWant(t, ctx, owner, []string{"801", "802"}, `SELECT id FROM items WHERE id > 800 ORDER BY id`)
		rlsExec(t, ctx, owner, `DELETE FROM items WHERE id > 800`)
		unchanged(t)
	})

	t.Run("SELECT FOR UPDATE and SKIP LOCKED", func(t *testing.T) {
		tx, err := app.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got, err := rlsTryRows(ctx, tx.Conn(), `SELECT id FROM docs ORDER BY id FOR UPDATE`)
		if err != nil || !reflect.DeepEqual(got, rlsSeq(1, 5)) {
			t.Fatalf("FOR UPDATE: %v, %v", got, err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		// The owner holds every row of tenant B and one of tenant A.
		// SKIP LOCKED passes over the locked row of its own tenant; the
		// other tenant's rows are not there to be skipped or returned.
		otx, err := owner.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := otx.Exec(ctx, `SELECT id FROM docs WHERE tenant_id = $1 FOR UPDATE`, rlsTenantB); err != nil {
			t.Fatal(err)
		}
		if _, err := otx.Exec(ctx, `SELECT id FROM docs WHERE tenant_id = $1 AND id = 1 FOR UPDATE`, rlsTenantA); err != nil {
			t.Fatal(err)
		}
		tx, err = app.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got, err = rlsTryRows(ctx, tx.Conn(), `SELECT id FROM docs ORDER BY id FOR UPDATE SKIP LOCKED`)
		if err != nil || !reflect.DeepEqual(got, rlsSeq(2, 5)) {
			t.Fatalf("SKIP LOCKED: %v, %v", got, err)
		}
		_ = tx.Rollback(ctx)
		_ = otx.Rollback(ctx)
		unchanged(t)
	})

	t.Run("DELETE and the cascade", func(t *testing.T) {
		for _, stmt := range []string{
			`DELETE FROM docs WHERE id = 7`,
			`DELETE FROM docs WHERE slug = 'b-2'`,
			`DELETE FROM docs WHERE tenant_id = '` + rlsTenantB + `'`,
			`DELETE FROM items WHERE id > 4`,
			`DELETE FROM items WHERE v >= 5`,
		} {
			if n := rlsExec(t, ctx, app, stmt); n != 0 {
				t.Fatalf("%s: deleted %d rows of another tenant", stmt, n)
			}
		}
		unchanged(t)
		// A restrictive policy hides some of the tenant's own notes from
		// the tenant role. Deleting their document still removes them:
		// the cascade is the foreign key's doing and runs without the
		// policies, as PostgreSQL's does.
		rlsExec(t, ctx, owner, `UPDATE notes SET author = 'hidden' WHERE tenant_id = $1 AND id = 3`, rlsTenantA)
		rlsExec(t, ctx, owner, `CREATE POLICY visible_only ON notes AS RESTRICTIVE USING (author = 'visible')`)
		rlsSettle(t, ctx, app, []string{"1", "2"}, `SELECT id FROM notes ORDER BY id`)
		if n := rlsExec(t, ctx, app, `DELETE FROM notes`); n != 2 {
			t.Fatalf("DELETE FROM notes removed %d rows, want the 2 the role can see", n)
		}
		rlsWant(t, ctx, owner, []string{"3"}, `SELECT id FROM notes WHERE tenant_id = $1`, rlsTenantA)
		rlsWant(t, ctx, app, []string{"2"}, `DELETE FROM docs WHERE id = 2 RETURNING id`)
		rlsWant(t, ctx, owner, []string{"0"}, `SELECT count(*) FROM notes WHERE tenant_id = $1`, rlsTenantA)
		rlsWant(t, ctx, owner, []string{"2"}, `SELECT count(*) FROM notes WHERE tenant_id = $1`, rlsTenantB)
		// DELETE without WHERE removes the tenant's rows and no others.
		if n := rlsExec(t, ctx, app, `DELETE FROM items`); n != 4 {
			t.Fatalf("DELETE FROM items removed %d rows, want the tenant's 4", n)
		}
		unchanged(t)
	})

	t.Run("TRUNCATE and DDL stay with the owner", func(t *testing.T) {
		for _, stmt := range []string{
			`TRUNCATE items`,
			`ALTER TABLE docs DISABLE ROW LEVEL SECURITY`,
			`ALTER TABLE docs NO FORCE ROW LEVEL SECURITY`,
			`DROP POLICY tenant_isolation ON docs`,
			`ALTER POLICY tenant_isolation ON docs USING (true)`,
			`CREATE POLICY open ON docs USING (true)`,
			`ALTER ROLE app BYPASSRLS`,
		} {
			rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, stmt)
		}
		// TRUNCATE reads no row, so no policy can filter it: a confined
		// role is refused it even holding the privilege outright. The
		// owner truncates as ever (not here: the rows are needed).
		rlsExec(t, ctx, owner, `GRANT TRUNCATE ON items TO app`)
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `TRUNCATE items`)
		rlsRefused(t, ctx, app, sql.CodeInsufficientPriv, `TRUNCATE docs CASCADE`)
		unchanged(t)
	})
}

// TestRowSecurityPlansAndTime: a plan made for one role, one value of a
// setting or one version of a policy is never used for another; an AS
// OF SYSTEM TIME read answers to today's policies; and the node's own
// sessions and root read everything even under FORCE.
func TestRowSecurityPlansAndTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRLSFixture(t, ctx)
	app, owner := f.app, f.owner

	t.Run("a prepared statement across a change of setting", func(t *testing.T) {
		// pgx prepares each text once per connection: the same server
		// statement runs under both tenants.
		const point = `SELECT n FROM docs WHERE id = $1`
		const count = `SELECT count(*) FROM docs`
		for i := 0; i < 3; i++ {
			rlsWant(t, ctx, app, []string{"30"}, point, int64(3))
			rlsWant(t, ctx, app, []string{"5"}, count)
		}
		rlsTenant(t, ctx, app, rlsTenantB)
		rlsWant(t, ctx, app, []string{"300"}, point, int64(3))
		rlsWant(t, ctx, app, []string{"7"}, count)
		rlsWant(t, ctx, app, []string{"600"}, point, int64(6))
		if _, err := app.Exec(ctx, `SELECT set_config('app.tenant', $1, false)`, rlsTenantA); err != nil {
			t.Fatal(err)
		}
		rlsWant(t, ctx, app, []string{"30"}, point, int64(3))
		rlsWant(t, ctx, app, []string{}, point, int64(6))
		// SET LOCAL lasts for the transaction.
		tx, err := app.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SET LOCAL app.tenant = '`+rlsTenantB+`'`); err != nil {
			t.Fatal(err)
		}
		if got, err := rlsTryRows(ctx, tx.Conn(), point, int64(6)); err != nil || !reflect.DeepEqual(got, []string{"600"}) {
			t.Fatalf("under SET LOCAL: %v, %v", got, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		rlsWant(t, ctx, app, []string{}, point, int64(6))
		rlsWant(t, ctx, app, []string{"5"}, count)
		// The same for prepared UPDATE and DELETE.
		const upd = `UPDATE docs SET n = n WHERE id = $1`
		if n := rlsExec(t, ctx, app, upd, int64(6)); n != 0 {
			t.Fatalf("tenant A updated id 6: %d", n)
		}
		rlsTenant(t, ctx, app, rlsTenantB)
		if n := rlsExec(t, ctx, app, upd, int64(6)); n != 1 {
			t.Fatalf("tenant B could not update its id 6: %d", n)
		}
		rlsTenant(t, ctx, app, rlsTenantA)
		if n := rlsExec(t, ctx, app, upd, int64(6)); n != 0 {
			t.Fatalf("tenant A updated id 6 after tenant B's execution: %d", n)
		}
	})

	t.Run("a cached plan across a change of role", func(t *testing.T) {
		// The owner's point lookup is the plan cache's bread and butter.
		// After SET ROLE to the tenant role the same prepared statement
		// must be filtered, and unfiltered again after RESET ROLE.
		const q = `SELECT n FROM docs WHERE tenant_id = $1 AND id = $2`
		// A cached plan is valid while the table's descriptor and
		// statistics are the ones it was planned against, so a background
		// statistics refresh (the UPDATEs above invite one) or a lease
		// renewal can cost a hit. Run until two hits, within a bound.
		h0, _ := planCacheCounts()
		for i := 0; i < 10; i++ {
			rlsWant(t, ctx, owner, []string{"600"}, q, rlsTenantB, int64(6))
			if h1, _ := planCacheCounts(); h1-h0 >= 2 {
				break
			}
		}
		if h1, _ := planCacheCounts(); h1-h0 < 2 {
			t.Fatalf("the owner's statement did not use the plan cache (%v hits in 10 runs): the test proves nothing", h1-h0)
		}
		rlsTenant(t, ctx, owner, rlsTenantA)
		rlsExec(t, ctx, owner, `SET ROLE app`)
		rlsWant(t, ctx, owner, []string{"app"}, `SELECT current_user`)
		rlsWant(t, ctx, owner, []string{}, q, rlsTenantB, int64(6))
		rlsWant(t, ctx, owner, []string{"30"}, q, rlsTenantA, int64(3))
		rlsWant(t, ctx, owner, []string{"5"}, `SELECT count(*) FROM docs`)
		if n := rlsExec(t, ctx, owner, `UPDATE docs SET n = n WHERE tenant_id = $1 AND id = $2`, rlsTenantB, int64(6)); n != 0 {
			t.Fatalf("as the tenant role: updated %d rows of tenant B", n)
		}
		rlsExec(t, ctx, owner, `RESET ROLE`)
		rlsWant(t, ctx, owner, []string{"600"}, q, rlsTenantB, int64(6))
		rlsWant(t, ctx, owner, []string{"12"}, `SELECT count(*) FROM docs`)
		rlsExec(t, ctx, owner, `RESET app.tenant`)
	})

	t.Run("a prepared statement across a change of policy", func(t *testing.T) {
		const q = `SELECT count(*) FROM docs WHERE id >= $1`
		rlsWant(t, ctx, app, []string{"5"}, q, int64(1))
		rlsExec(t, ctx, owner, `ALTER POLICY tenant_isolation ON docs USING (tenant_id = current_setting('app.tenant')::uuid AND id <= 2)`)
		rlsSettle(t, ctx, app, []string{"2"}, q, int64(1))
		rlsExec(t, ctx, owner, `ALTER POLICY tenant_isolation ON docs USING (tenant_id = current_setting('app.tenant')::uuid)`)
		rlsSettle(t, ctx, app, []string{"5"}, q, int64(1))
		rlsExec(t, ctx, owner, `DROP POLICY tenant_isolation ON docs`)
		rlsSettle(t, ctx, app, []string{"0"}, q, int64(1))
		rlsExec(t, ctx, owner, `ALTER TABLE docs DISABLE ROW LEVEL SECURITY`)
		rlsSettle(t, ctx, app, []string{"12"}, q, int64(1))
		rlsExec(t, ctx, owner, `ALTER TABLE docs ENABLE ROW LEVEL SECURITY`)
		rlsSettle(t, ctx, app, []string{"0"}, q, int64(1))
		rlsExec(t, ctx, owner, `CREATE POLICY tenant_isolation ON docs USING (tenant_id = current_setting('app.tenant')::uuid)`)
		rlsSettle(t, ctx, app, []string{"5"}, q, int64(1))
	})

	t.Run("AS OF SYSTEM TIME answers to today's policies", func(t *testing.T) {
		// A table that was open when the timestamp was taken and is
		// protected now: reading it as of then must not undo that.
		rlsExec(t, ctx, owner, `CREATE TABLE hist (id INT8 PRIMARY KEY, tenant_id UUID, v INT8)`)
		rlsExec(t, ctx, owner, `GRANT SELECT ON hist TO app`)
		rlsExec(t, ctx, owner, `INSERT INTO hist VALUES (1, $1, 1), (2, $1, 2), (3, $2, 3)`, rlsTenantA, rlsTenantB)
		rlsSettle(t, ctx, app, []string{"3"}, `SELECT count(*) FROM hist`)
		time.Sleep(50 * time.Millisecond)
		open := f.tc.Nodes[1].Clock().Now().WallTime
		time.Sleep(50 * time.Millisecond)
		rlsExec(t, ctx, owner, `ALTER TABLE hist ENABLE ROW LEVEL SECURITY`)
		rlsExec(t, ctx, owner, `CREATE POLICY tenant_isolation ON hist USING (tenant_id = current_setting('app.tenant')::uuid)`)
		rlsExec(t, ctx, owner, `INSERT INTO hist VALUES (4, $1, 4)`, rlsTenantA)
		rlsSettle(t, ctx, app, []string{"1", "2", "4"}, `SELECT id FROM hist ORDER BY id`)
		asOf := fmt.Sprintf(`SELECT id FROM hist AS OF SYSTEM TIME '%d' ORDER BY id`, open)
		rlsWant(t, ctx, app, []string{"1", "2"}, asOf)
		rlsWant(t, ctx, app, []string{"2"}, fmt.Sprintf(`SELECT count(*) FROM hist AS OF SYSTEM TIME '%d'`, open))
		rlsWant(t, ctx, owner, []string{"1", "2", "3"}, asOf)
		// And the other way: a policy since dropped does not come back.
		time.Sleep(50 * time.Millisecond)
		protected := f.tc.Nodes[1].Clock().Now().WallTime
		time.Sleep(50 * time.Millisecond)
		rlsExec(t, ctx, owner, `ALTER TABLE hist DISABLE ROW LEVEL SECURITY`)
		rlsSettle(t, ctx, app, []string{"4"}, `SELECT count(*) FROM hist`)
		rlsWant(t, ctx, app, []string{"4"}, fmt.Sprintf(`SELECT count(*) FROM hist AS OF SYSTEM TIME '%d'`, protected))
	})

	t.Run("FORCE, root and the node's own sessions", func(t *testing.T) {
		rlsExec(t, ctx, owner, `ALTER TABLE items FORCE ROW LEVEL SECURITY`)
		rlsTenant(t, ctx, owner, rlsTenantA)
		rlsSettle(t, ctx, owner, []string{"4"}, `SELECT count(*) FROM items`)
		rlsRefused(t, ctx, owner, sql.CodeInsufficientPriv, `INSERT INTO items VALUES (900, $1, 1)`, rlsTenantB)
		rlsWant(t, ctx, f.root, []string{"true|true"}, `SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = 'items'`)
		// Root and the BYPASSRLS role are exempt whatever the table says.
		rlsWant(t, ctx, f.root, []string{"10"}, `SELECT count(*) FROM items`)
		rlsWant(t, ctx, f.auditor, []string{"10"}, `SELECT count(*) FROM items`)
		// So is the node's own session — the one the row-expiry job, the
		// recorders and restore run their statements in.
		for name, sess := range map[string]*sql.Session{
			"system": sql.NewSystemSession(f.tc.Nodes[2].DB(), catalog.NewAccessor()),
			"root":   sql.NewSession(f.tc.Nodes[2].DB(), catalog.NewAccessor()),
		} {
			if res := execSQL(t, ctx, sess, `SELECT count(*) FROM items`); res.Rows[0][0].I != 10 {
				t.Fatalf("the %s session reads %d of 10 rows under FORCE", name, res.Rows[0][0].I)
			}
			res := execSQL(t, ctx, sess, `DELETE FROM items WHERE id = 10`)
			if res.Tag != "DELETE 1" {
				t.Fatalf("the %s session's DELETE under FORCE: %s", name, res.Tag)
			}
			execSQL(t, ctx, sess, `INSERT INTO items VALUES (10, '`+rlsTenantB+`', 10)`)
		}
		rlsExec(t, ctx, owner, `ALTER TABLE items NO FORCE ROW LEVEL SECURITY`)
		rlsSettle(t, ctx, owner, []string{"10"}, `SELECT count(*) FROM items`)
		rlsExec(t, ctx, owner, `RESET app.tenant`)
		// An admin is exempt as an owner is: not under FORCE.
		rlsExec(t, ctx, f.root, `CREATE USER ops PASSWORD 'ops-pw-123'`)
		rlsExec(t, ctx, f.root, `GRANT admin TO ops`)
		ops := rlsConn(t, ctx, f.tc, 1, "ops")
		rlsSettle(t, ctx, ops, []string{"10"}, `SELECT count(*) FROM items`)
		// Statistics are cached per gateway: analyze on the one ops uses.
		rlsExec(t, ctx, rlsConn(t, ctx, f.tc, 1, "root"), `ANALYZE items`)
		rlsWant(t, ctx, ops, []string{"10"}, `SELECT reltuples FROM pg_class WHERE relname = 'items'`)
		rlsExec(t, ctx, owner, `ALTER TABLE items FORCE ROW LEVEL SECURITY`)
		rlsTenant(t, ctx, ops, rlsTenantA)
		rlsSettle(t, ctx, ops, []string{"4"}, `SELECT count(*) FROM items`)
		// Confined, it is shown no statistics: they count every row.
		rlsWant(t, ctx, ops, []string{"0"}, `SELECT reltuples FROM pg_class WHERE relname = 'items'`)
		rlsWant(t, ctx, ops, []string{"0"}, `SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'items'`)
		rlsWant(t, ctx, ops, []string{}, `SHOW STATS FOR items`)
		rlsRefused(t, ctx, ops, sql.CodeInsufficientPriv, `TRUNCATE items`)
		rlsExec(t, ctx, owner, `ALTER TABLE items NO FORCE ROW LEVEL SECURITY`)
	})

	t.Run("policy DDL", func(t *testing.T) {
		rlsRefused(t, ctx, owner, sql.CodeUndefinedColumn, `CREATE POLICY p ON items USING (nosuch = 1)`)
		rlsRefused(t, ctx, owner, sql.CodeUndefinedColumn, `CREATE POLICY p ON items USING (items.v = 1)`)
		rlsRefused(t, ctx, owner, sql.CodeFeatureNotSupported, `CREATE POLICY p ON items USING (id IN (SELECT id FROM docs))`)
		rlsRefused(t, ctx, owner, sql.CodeFeatureNotSupported, `CREATE POLICY p ON items USING (v = nextval('s'))`)
		rlsRefused(t, ctx, owner, sql.CodeUndefinedObject, `CREATE POLICY p ON items TO nobody_here USING (true)`)
		rlsRefused(t, ctx, owner, sql.CodeDuplicateObject, `CREATE POLICY tenant_isolation ON items USING (true)`)
		rlsRefused(t, ctx, owner, sql.CodeUndefinedObject, `DROP POLICY nosuch ON items`)
		rlsRefused(t, ctx, owner, sql.CodeUndefinedObject, `ALTER POLICY nosuch ON items USING (true)`)
		rlsRefused(t, ctx, owner, sql.CodeUndefinedTable, `CREATE POLICY p ON nosuch_table USING (true)`)
		rlsExec(t, ctx, owner, `CREATE VIEW pol_view AS SELECT id FROM items`)
		rlsRefused(t, ctx, owner, sql.CodeWrongObjectType, `CREATE POLICY p ON pol_view USING (true)`)
		rlsRefused(t, ctx, owner, sql.CodeWrongObjectType, `ALTER TABLE pol_view ENABLE ROW LEVEL SECURITY`)
		rlsExec(t, ctx, owner, `DROP VIEW pol_view`)
		rlsExec(t, ctx, owner, `DROP POLICY IF EXISTS nosuch ON items`)
		// A column a policy reads cannot be dropped; renaming it carries
		// the policy along.
		rlsRefused(t, ctx, owner, sql.CodeDependentObjectsExist, `ALTER TABLE items DROP COLUMN tenant_id`)
		rlsExec(t, ctx, owner, `CREATE POLICY small ON items AS RESTRICTIVE FOR SELECT TO app USING (v < 1000) `)
		rlsRefused(t, ctx, owner, sql.CodeDependentObjectsExist, `ALTER TABLE items DROP COLUMN v`)
		rlsExec(t, ctx, owner, `ALTER TABLE items RENAME COLUMN v TO amount`)
		rlsWant(t, ctx, f.root, []string{"small|RESTRICTIVE|{app}|SELECT|amount < 1000"},
			`SELECT policyname, permissive, roles, cmd, qual FROM pg_policies WHERE tablename = 'items' AND policyname = 'small'`)
		rlsSettle(t, ctx, app, []string{"4"}, `SELECT count(*) FROM items WHERE amount > 0`)
		rlsExec(t, ctx, owner, `ALTER TABLE items RENAME COLUMN amount TO v`)
		// A role a policy names cannot be dropped from under it.
		if msg := rlsRefused(t, ctx, f.root, sql.CodeDependentObjectsExist, `DROP ROLE app`); !strings.Contains(msg, "small") && !strings.Contains(msg, "owner") {
			t.Fatalf("DROP ROLE: %s", msg)
		}
		rlsExec(t, ctx, owner, `ALTER POLICY small ON items RENAME TO tiny`)
		rlsExec(t, ctx, owner, `ALTER POLICY tiny ON items TO PUBLIC`)
		rlsWant(t, ctx, f.root, []string{"tiny|{public}"}, `SELECT policyname, roles FROM pg_policies WHERE tablename = 'items' AND policyname = 'tiny'`)
		rlsRefused(t, ctx, owner, sql.CodeSyntaxError, `ALTER POLICY tiny ON items WITH CHECK (true)`)
		rlsExec(t, ctx, owner, `DROP POLICY tiny ON items`)
	})
}

// TestRowSecurityConformance: PostgreSQL's rules for permissive and
// restrictive policies, the FOR variants, TO roles, default deny, FORCE
// and BYPASSRLS, as its documentation states them. Each step's expected
// result is what PostgreSQL returns for the same statements.
func TestRowSecurityConformance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tc := rlsCluster(t)
	root := rlsConn(t, ctx, tc, 0, "root")
	for _, stmt := range []string{
		`CREATE USER cown PASSWORD 'cown-pw-12'`,
		`CREATE USER alice PASSWORD 'alice-pw-1'`,
		`CREATE USER bob PASSWORD 'bob-pw-123'`,
		`CREATE USER carol PASSWORD 'carol-pw-1'`,
		`CREATE ROLE staff`,
		`GRANT staff TO carol`,
		`GRANT CREATE ON DATABASE datax TO cown`,
	} {
		rlsExec(t, ctx, root, stmt)
	}
	cown := rlsConn(t, ctx, tc, 0, "cown")
	alice := rlsConn(t, ctx, tc, 1, "alice")
	bob := rlsConn(t, ctx, tc, 2, "bob")
	carol := rlsConn(t, ctx, tc, 1, "carol")
	reset := func(t *testing.T) {
		t.Helper()
		_, _ = cown.Exec(ctx, `DROP TABLE c`)
		for _, stmt := range []string{
			`CREATE TABLE c (id INT8 PRIMARY KEY, owner TEXT, v INT8, pub BOOL)`,
			`GRANT SELECT, INSERT, UPDATE, DELETE ON c TO alice`,
			`GRANT SELECT, INSERT, UPDATE, DELETE ON c TO bob`,
			`GRANT SELECT, INSERT, UPDATE, DELETE ON c TO staff`,
			`INSERT INTO c VALUES (1, 'alice', 10, false), (2, 'alice', -5, true), (3, 'bob', 20, false), (4, 'bob', 200, true), (5, NULL, 0, NULL)`,
		} {
			rlsExec(t, ctx, cown, stmt)
		}
		rlsSettle(t, ctx, alice, rlsSeq(1, 5), `SELECT id FROM c ORDER BY id`)
		rlsSettle(t, ctx, bob, rlsSeq(1, 5), `SELECT id FROM c ORDER BY id`)
		rlsSettle(t, ctx, carol, rlsSeq(1, 5), `SELECT id FROM c ORDER BY id`)
	}
	ids := `SELECT id FROM c ORDER BY id`
	ddl := func(t *testing.T, stmt string) {
		t.Helper()
		rlsExec(t, ctx, cown, stmt)
	}

	t.Run("default deny", func(t *testing.T) {
		reset(t)
		ddl(t, `ALTER TABLE c ENABLE ROW LEVEL SECURITY`)
		rlsSettle(t, ctx, alice, []string{}, ids)
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `INSERT INTO c VALUES (9, 'alice', 1, false)`)
		for _, stmt := range []string{`UPDATE c SET v = 0`, `DELETE FROM c`, `UPDATE c SET v = 0 WHERE id = 1`, `DELETE FROM c WHERE id = 1`} {
			if n := rlsExec(t, ctx, alice, stmt); n != 0 {
				t.Fatalf("%s: %d rows", stmt, n)
			}
		}
		// The owner is exempt; the table is as it was.
		rlsWant(t, ctx, cown, rlsSeq(1, 5), ids)
		rlsWant(t, ctx, cown, []string{"225"}, `SELECT sum(v) FROM c`)
		// Disabling row security opens the table to its grantees again,
		// and the policies (none here) play no part.
		ddl(t, `ALTER TABLE c DISABLE ROW LEVEL SECURITY`)
		rlsSettle(t, ctx, alice, rlsSeq(1, 5), ids)
	})

	t.Run("FOR variants", func(t *testing.T) {
		reset(t)
		ddl(t, `ALTER TABLE c ENABLE ROW LEVEL SECURITY`)
		// A SELECT policy alone: reads, and nothing else.
		ddl(t, `CREATE POLICY sel ON c FOR SELECT USING (owner = current_user)`)
		rlsSettle(t, ctx, alice, []string{"1", "2"}, ids)
		rlsSettle(t, ctx, bob, []string{"3", "4"}, ids)
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `INSERT INTO c VALUES (10, 'alice', 1, false)`)
		if n := rlsExec(t, ctx, alice, `UPDATE c SET v = v + 1 WHERE id = 1`); n != 0 {
			t.Fatalf("UPDATE with only a SELECT policy: %d rows", n)
		}
		if n := rlsExec(t, ctx, alice, `DELETE FROM c WHERE id = 1`); n != 0 {
			t.Fatalf("DELETE with only a SELECT policy: %d rows", n)
		}
		// INSERT: WITH CHECK on the new row.
		ddl(t, `CREATE POLICY ins ON c FOR INSERT WITH CHECK (owner = current_user)`)
		rlsSettle(t, ctx, alice, []string{"10"}, `INSERT INTO c VALUES (10, 'alice', 1, false) RETURNING id`)
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `INSERT INTO c VALUES (11, 'bob', 1, false)`)
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `INSERT INTO c VALUES (11, NULL, 1, false)`)
		// UPDATE: USING picks the rows and, with no WITH CHECK, also
		// judges the new row.
		ddl(t, `CREATE POLICY upd ON c FOR UPDATE USING (owner = current_user)`)
		rlsSettle(t, ctx, alice, []string{"1|11"}, `UPDATE c SET v = v + 1 WHERE id = 1 RETURNING id, v`)
		if n := rlsExec(t, ctx, alice, `UPDATE c SET v = v + 1 WHERE id = 3`); n != 0 {
			t.Fatalf("UPDATE of bob's row: %d", n)
		}
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `UPDATE c SET owner = 'bob' WHERE id = 1`)
		if n := rlsExec(t, ctx, alice, `UPDATE c SET v = v`); n != 3 {
			t.Fatalf("UPDATE of all her rows: %d, want 3", n)
		}
		// DELETE: its own USING, narrower than SELECT's here.
		ddl(t, `CREATE POLICY del ON c FOR DELETE USING (owner = current_user AND v < 0)`)
		rlsSettle(t, ctx, alice, []string{"2"}, `DELETE FROM c WHERE v < 100 RETURNING id`)
		if n := rlsExec(t, ctx, alice, `DELETE FROM c`); n != 0 {
			t.Fatalf("DELETE of rows the DELETE policy does not admit: %d", n)
		}
		rlsWant(t, ctx, cown, []string{"1", "3", "4", "5", "10"}, ids)
		// Altering a policy's expression takes effect.
		ddl(t, `ALTER POLICY del ON c USING (owner = current_user)`)
		rlsSettle(t, ctx, alice, []string{"10"}, `DELETE FROM c WHERE id = 10 RETURNING id`)
	})

	t.Run("UPDATE and DELETE policies without a SELECT policy", func(t *testing.T) {
		// PostgreSQL applies the SELECT policies to an UPDATE or DELETE
		// only when the statement reads the table — a WHERE clause,
		// RETURNING, a SET computed from a column. A blind update passes
		// on the UPDATE policy alone; one with a WHERE finds nothing.
		reset(t)
		ddl(t, `ALTER TABLE c ENABLE ROW LEVEL SECURITY`)
		ddl(t, `CREATE POLICY upd ON c FOR UPDATE USING (owner = current_user)`)
		ddl(t, `CREATE POLICY del ON c FOR DELETE USING (owner = current_user)`)
		rlsSettle(t, ctx, alice, []string{}, ids)
		if n := rlsExec(t, ctx, alice, `UPDATE c SET v = 7 WHERE id = 1`); n != 0 {
			t.Fatalf("UPDATE ... WHERE with no SELECT policy: %d", n)
		}
		if n := rlsExec(t, ctx, alice, `UPDATE c SET v = v + 1`); n != 0 {
			t.Fatalf("UPDATE computing from a column with no SELECT policy: %d", n)
		}
		if n := rlsExec(t, ctx, alice, `UPDATE c SET v = 7`); n != 2 {
			t.Fatalf("blind UPDATE: %d, want her 2 rows", n)
		}
		if n := rlsExec(t, ctx, alice, `DELETE FROM c WHERE id = 1`); n != 0 {
			t.Fatalf("DELETE ... WHERE with no SELECT policy: %d", n)
		}
		if n := rlsExec(t, ctx, alice, `DELETE FROM c`); n != 2 {
			t.Fatalf("blind DELETE: %d, want her 2 rows", n)
		}
		rlsWant(t, ctx, cown, []string{"3", "4", "5"}, ids)
	})

	t.Run("permissive policies are OR-ed, restrictive AND-ed", func(t *testing.T) {
		reset(t)
		ddl(t, `ALTER TABLE c ENABLE ROW LEVEL SECURITY`)
		ddl(t, `CREATE POLICY own ON c FOR SELECT USING (owner = current_user)`)
		rlsSettle(t, ctx, alice, []string{"1", "2"}, ids)
		ddl(t, `CREATE POLICY public_rows ON c AS PERMISSIVE FOR SELECT USING (pub)`)
		rlsSettle(t, ctx, alice, []string{"1", "2", "4"}, ids)
		rlsSettle(t, ctx, bob, []string{"2", "3", "4"}, ids)
		// A NULL result admits nothing: row 5 has a NULL owner and pub.
		ddl(t, `CREATE POLICY nonneg ON c AS RESTRICTIVE FOR SELECT USING (v >= 0)`)
		rlsSettle(t, ctx, alice, []string{"1", "4"}, ids)
		rlsSettle(t, ctx, bob, []string{"3", "4"}, ids)
		ddl(t, `CREATE POLICY small ON c AS RESTRICTIVE FOR SELECT USING (v < 100)`)
		rlsSettle(t, ctx, alice, []string{"1"}, ids)
		rlsSettle(t, ctx, bob, []string{"3"}, ids)
		// Restrictive policies alone admit nothing: there must be a
		// permissive one to restrict.
		ddl(t, `DROP POLICY own ON c`)
		ddl(t, `DROP POLICY public_rows ON c`)
		rlsSettle(t, ctx, alice, []string{}, ids)
		rlsSettle(t, ctx, bob, []string{}, ids)
		ddl(t, `CREATE POLICY everything ON c FOR SELECT USING (true)`)
		rlsSettle(t, ctx, alice, []string{"1", "3", "5"}, ids)
	})

	t.Run("FOR ALL, USING and WITH CHECK", func(t *testing.T) {
		reset(t)
		ddl(t, `ALTER TABLE c ENABLE ROW LEVEL SECURITY`)
		// USING alone stands in for WITH CHECK.
		ddl(t, `CREATE POLICY mine ON c USING (owner = current_user)`)
		rlsSettle(t, ctx, alice, []string{"1", "2"}, ids)
		rlsExec(t, ctx, alice, `INSERT INTO c VALUES (20, 'alice', 1, false)`)
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `INSERT INTO c VALUES (21, 'bob', 1, false)`)
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `UPDATE c SET owner = 'bob' WHERE id = 20`)
		if n := rlsExec(t, ctx, alice, `DELETE FROM c WHERE id IN (20, 3)`); n != 1 {
			t.Fatalf("DELETE: %d, want only her row", n)
		}
		// A separate WITH CHECK narrows what may be written, not read.
		ddl(t, `ALTER POLICY mine ON c WITH CHECK (owner = current_user AND v < 1000)`)
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `INSERT INTO c VALUES (22, 'alice', 5000, false)`)
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `UPDATE c SET v = 5000 WHERE id = 1`)
		rlsExec(t, ctx, alice, `INSERT INTO c VALUES (22, 'alice', 999, false)`)
		rlsWant(t, ctx, alice, []string{"1", "2", "22"}, ids)
		// The new row of an UPDATE must also stay visible to the SELECT
		// policies when the statement read the table.
		ddl(t, `DROP POLICY mine ON c`)
		ddl(t, `CREATE POLICY see ON c FOR SELECT USING (owner = current_user OR pub)`)
		ddl(t, `CREATE POLICY change ON c FOR UPDATE USING (true) WITH CHECK (true)`)
		rlsSettle(t, ctx, alice, []string{"1", "2", "4", "22"}, ids)
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `UPDATE c SET owner = 'bob' WHERE id = 1`)
		if n := rlsExec(t, ctx, alice, `UPDATE c SET owner = 'bob' WHERE id = 2`); n != 1 {
			t.Fatalf("UPDATE to a row that stays visible (public): %d", n)
		}
	})

	t.Run("TO roles", func(t *testing.T) {
		reset(t)
		ddl(t, `ALTER TABLE c ENABLE ROW LEVEL SECURITY`)
		ddl(t, `CREATE POLICY bobs ON c TO bob USING (true)`)
		rlsSettle(t, ctx, bob, rlsSeq(1, 5), ids)
		rlsSettle(t, ctx, alice, []string{}, ids)
		rlsSettle(t, ctx, carol, []string{}, ids)
		// A role is reached through membership.
		ddl(t, `CREATE POLICY staff_public ON c FOR SELECT TO staff USING (pub)`)
		rlsSettle(t, ctx, carol, []string{"2", "4"}, ids)
		rlsSettle(t, ctx, alice, []string{}, ids)
		// Several roles, then PUBLIC.
		ddl(t, `ALTER POLICY staff_public ON c TO staff, alice`)
		rlsSettle(t, ctx, alice, []string{"2", "4"}, ids)
		ddl(t, `ALTER POLICY staff_public ON c TO PUBLIC`)
		rlsSettle(t, ctx, alice, []string{"2", "4"}, ids)
		rlsSettle(t, ctx, bob, rlsSeq(1, 5), ids)
		// A restrictive policy for one role narrows only that role.
		ddl(t, `CREATE POLICY bob_small ON c AS RESTRICTIVE TO bob USING (v < 100)`)
		rlsSettle(t, ctx, bob, []string{"1", "2", "3", "5"}, ids)
		rlsSettle(t, ctx, alice, []string{"2", "4"}, ids)
	})

	t.Run("FORCE and BYPASSRLS", func(t *testing.T) {
		reset(t)
		ddl(t, `ALTER TABLE c ENABLE ROW LEVEL SECURITY`)
		ddl(t, `CREATE POLICY mine ON c USING (owner = current_user)`)
		rlsSettle(t, ctx, alice, []string{"1", "2"}, ids)
		rlsWant(t, ctx, cown, rlsSeq(1, 5), ids)
		ddl(t, `ALTER TABLE c FORCE ROW LEVEL SECURITY`)
		rlsSettle(t, ctx, cown, []string{}, ids)
		rlsRefused(t, ctx, cown, sql.CodeInsufficientPriv, `INSERT INTO c VALUES (30, 'alice', 1, false)`)
		rlsExec(t, ctx, cown, `INSERT INTO c VALUES (30, 'cown', 1, false)`)
		rlsWant(t, ctx, cown, []string{"30"}, ids)
		ddl(t, `ALTER TABLE c NO FORCE ROW LEVEL SECURITY`)
		rlsSettle(t, ctx, cown, []string{"1", "2", "3", "4", "5", "30"}, ids)
		// BYPASSRLS exempts a role outright, FORCE or not; it is the
		// role's own attribute and is not inherited through membership.
		rlsExec(t, ctx, root, `ALTER ROLE staff BYPASSRLS`)
		rlsSettle(t, ctx, carol, []string{}, ids)
		rlsExec(t, ctx, root, `ALTER ROLE carol BYPASSRLS`)
		rlsSettle(t, ctx, carol, []string{"1", "2", "3", "4", "5", "30"}, ids)
		ddl(t, `ALTER TABLE c FORCE ROW LEVEL SECURITY`)
		rlsSettle(t, ctx, cown, []string{"30"}, ids)
		rlsWant(t, ctx, carol, []string{"1", "2", "3", "4", "5", "30"}, ids)
		rlsExec(t, ctx, root, `ALTER ROLE carol NOBYPASSRLS`)
		rlsSettle(t, ctx, carol, []string{}, ids)
		rlsExec(t, ctx, root, `ALTER ROLE staff NOBYPASSRLS`)
		// Only an admin hands the attribute out.
		rlsRefused(t, ctx, cown, sql.CodeInsufficientPriv, `ALTER ROLE cown BYPASSRLS`)
		rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `ALTER ROLE alice BYPASSRLS`)
	})

	t.Run("current_user follows SET ROLE", func(t *testing.T) {
		reset(t)
		rlsExec(t, ctx, root, `GRANT alice TO bob`)
		ddl(t, `ALTER TABLE c ENABLE ROW LEVEL SECURITY`)
		ddl(t, `CREATE POLICY mine ON c USING (owner = current_user)`)
		rlsSettle(t, ctx, alice, []string{"1", "2"}, ids)
		rlsSettle(t, ctx, bob, []string{"3", "4"}, ids)
		rlsExec(t, ctx, bob, `SET ROLE alice`)
		rlsWant(t, ctx, bob, []string{"1", "2"}, ids)
		rlsExec(t, ctx, bob, `RESET ROLE`)
		rlsWant(t, ctx, bob, []string{"3", "4"}, ids)
		rlsExec(t, ctx, root, `REVOKE alice FROM bob`)
	})
}

// TestRowSecurityDifferential: random statements run by a tenant role
// under a policy must do exactly what the same statements do when the
// table's owner runs them against an unprotected copy with the policy's
// predicate written out by hand.
func TestRowSecurityDifferential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tc := rlsCluster(t)
	root := rlsConn(t, ctx, tc, 0, "root")
	rlsExec(t, ctx, root, `CREATE USER tn PASSWORD 'tenant-pw-1'`)
	const ddl = ` (tenant INT8, id INT8, grp INT8, v INT8, tag TEXT, PRIMARY KEY (tenant, id))`
	for _, stmt := range []string{
		`CREATE TABLE d_rls` + ddl, `CREATE TABLE d_ref` + ddl,
		`CREATE INDEX d_rls_grp ON d_rls (grp)`, `CREATE INDEX d_ref_grp ON d_ref (grp)`,
		`CREATE INDEX d_rls_tag ON d_rls (tag)`, `CREATE INDEX d_ref_tag ON d_ref (tag)`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON d_rls TO tn`,
		`ALTER TABLE d_rls ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY tenant ON d_rls USING (tenant = current_setting('app.tenant')::int8)`,
	} {
		rlsExec(t, ctx, root, stmt)
	}
	seed := time.Now().UnixNano()
	t.Logf("seed %d", seed)
	rng := newRLSRand(seed)
	const tenants = 3
	for tenant := 1; tenant <= tenants; tenant++ {
		for id := 1; id <= 30; id++ {
			if rng.intn(4) == 0 {
				continue // gaps differ per tenant
			}
			args := []any{int64(tenant), int64(id), int64(rng.intn(5)), int64(rng.intn(100)), fmt.Sprintf("t%d", rng.intn(4))}
			rlsExec(t, ctx, root, `INSERT INTO d_rls VALUES ($1, $2, $3, $4, $5)`, args...)
			rlsExec(t, ctx, root, `INSERT INTO d_ref VALUES ($1, $2, $3, $4, $5)`, args...)
		}
	}
	tn := rlsConn(t, ctx, tc, 1, "tn")
	rlsExec(t, ctx, tn, `SET app.tenant = '1'`)
	rlsSettle(t, ctx, tn, []string{"1"}, `SELECT DISTINCT tenant FROM d_rls`)

	preds := []func() string{
		func() string { return fmt.Sprintf("id = %d", 1+rng.intn(32)) },
		func() string { return fmt.Sprintf("id > %d", rng.intn(30)) },
		func() string { return fmt.Sprintf("id BETWEEN %d AND %d", rng.intn(15), 15+rng.intn(15)) },
		func() string { return fmt.Sprintf("grp = %d", rng.intn(5)) },
		func() string { return fmt.Sprintf("tag = 't%d'", rng.intn(4)) },
		func() string { return fmt.Sprintf("v < %d", rng.intn(100)) },
		func() string { return fmt.Sprintf("grp = %d OR v > %d", rng.intn(5), rng.intn(100)) },
		func() string { return fmt.Sprintf("tenant = %d", 1+rng.intn(tenants)) },
		func() string { return fmt.Sprintf("tenant = %d AND id = %d", 1+rng.intn(tenants), 1+rng.intn(30)) },
		func() string { return fmt.Sprintf("id IN (%d, %d, %d)", rng.intn(30), rng.intn(30), rng.intn(30)) },
		func() string { return fmt.Sprintf("tag LIKE 't%d%%' AND grp != %d", rng.intn(4), rng.intn(5)) },
	}
	// where renders a predicate for the protected table (as written) and
	// for the reference (with the policy's predicate in front).
	where := func(tenant int, pred string) (string, string) {
		by := fmt.Sprintf("tenant = %d", tenant)
		if pred == "" {
			return "", " WHERE " + by
		}
		return " WHERE " + pred, " WHERE " + by + " AND (" + pred + ")"
	}
	contents := func(conn *pgx.Conn, table, filter string) []string {
		out := rlsRows(t, ctx, conn, `SELECT tenant, id, grp, v, tag FROM `+table+filter)
		sort.Strings(out)
		return out
	}
	for step := 0; step < 250; step++ {
		tenant := 1 + rng.intn(tenants)
		rlsExec(t, ctx, tn, fmt.Sprintf(`SET app.tenant = '%d'`, tenant))
		pred := ""
		if rng.intn(6) > 0 {
			pred = preds[rng.intn(len(preds))]()
		}
		w, wRef := where(tenant, pred)
		var stmt, ref string
		ordered := false
		switch k := rng.intn(12); {
		case k < 3:
			stmt, ref = `SELECT tenant, id, grp, v, tag FROM d_rls`+w, `SELECT tenant, id, grp, v, tag FROM d_ref`+wRef
		case k < 5:
			stmt, ref = `SELECT count(*), sum(v), min(id), max(id) FROM d_rls`+w, `SELECT count(*), sum(v), min(id), max(id) FROM d_ref`+wRef
		case k == 5:
			stmt, ref = `SELECT grp, count(*), sum(v) FROM d_rls`+w+` GROUP BY grp`, `SELECT grp, count(*), sum(v) FROM d_ref`+wRef+` GROUP BY grp`
		case k == 6:
			lim := 1 + rng.intn(6)
			dir := []string{"ASC", "DESC"}[rng.intn(2)]
			stmt = fmt.Sprintf(`SELECT id, v FROM d_rls%s ORDER BY id %s LIMIT %d`, w, dir, lim)
			ref = fmt.Sprintf(`SELECT id, v FROM d_ref%s ORDER BY id %s LIMIT %d`, wRef, dir, lim)
			ordered = true
		case k == 7:
			// A self-join: both references are filtered.
			stmt = `SELECT a.id, b.id FROM d_rls a JOIN d_rls b ON a.grp = b.v`
			ref = fmt.Sprintf(`SELECT a.id, b.id FROM d_ref a JOIN d_ref b ON a.grp = b.v AND b.tenant = %d WHERE a.tenant = %d`, tenant, tenant)
		case k == 8:
			stmt = `SELECT id FROM d_rls WHERE v IN (SELECT grp FROM d_rls)`
			ref = fmt.Sprintf(`SELECT id FROM d_ref WHERE tenant = %d AND v IN (SELECT grp FROM d_ref WHERE tenant = %d)`, tenant, tenant)
		case k == 9:
			set := fmt.Sprintf("v = v + %d, tag = 't%d'", rng.intn(10), rng.intn(4))
			stmt, ref = `UPDATE d_rls SET `+set+w, `UPDATE d_ref SET `+set+wRef
		case k == 10:
			stmt, ref = `DELETE FROM d_rls`+w, `DELETE FROM d_ref`+wRef
		default:
			// An insert for a random tenant: its own succeeds (or
			// collides, in both tables alike); another's is refused and
			// the reference is left alone.
			target := 1 + rng.intn(tenants)
			vals := fmt.Sprintf("(%d, %d, %d, %d, 't%d')", target, 1+rng.intn(40), rng.intn(5), rng.intn(100), rng.intn(4))
			_, err := tn.Exec(ctx, `INSERT INTO d_rls VALUES `+vals)
			if target != tenant {
				if rlsCode(err) != sql.CodeInsufficientPriv {
					t.Fatalf("step %d: tenant %d inserted %s: %v, want 42501", step, tenant, vals, err)
				}
				continue
			}
			_, refErr := root.Exec(ctx, `INSERT INTO d_ref VALUES `+vals)
			if rlsCode(err) != rlsCode(refErr) {
				t.Fatalf("step %d: INSERT %s: tenant got %v, reference got %v", step, vals, err, refErr)
			}
			continue
		}
		if strings.HasPrefix(stmt, "SELECT") {
			got, err := rlsTryRows(ctx, tn, stmt)
			want, refErr := rlsTryRows(ctx, root, ref)
			if rlsCode(err) != rlsCode(refErr) {
				t.Fatalf("step %d: %s: %v; reference %s: %v", step, stmt, err, ref, refErr)
			}
			if !ordered {
				sort.Strings(got)
				sort.Strings(want)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("step %d (tenant %d):\n%s\n got %v\n%s\nwant %v", step, tenant, stmt, got, ref, want)
			}
			continue
		}
		tag, err := tn.Exec(ctx, stmt)
		refTag, refErr := root.Exec(ctx, ref)
		if rlsCode(err) != rlsCode(refErr) || tag.RowsAffected() != refTag.RowsAffected() {
			t.Fatalf("step %d (tenant %d):\n%s: %d rows, %v\n%s: %d rows, %v", step, tenant, stmt, tag.RowsAffected(), err, ref, refTag.RowsAffected(), refErr)
		}
	}
	// After everything, the two tables hold the same rows — every
	// tenant's, including the ones no statement was allowed to touch.
	if got, want := contents(root, "d_rls", ""), contents(root, "d_ref", ""); !reflect.DeepEqual(got, want) {
		t.Fatalf("the tables diverged:\n got %v\nwant %v", got, want)
	}
}

// rlsRand is a small deterministic generator (the seed is logged).
type rlsRand struct{ state uint64 }

func newRLSRand(seed int64) *rlsRand { return &rlsRand{state: uint64(seed)*2654435761 + 1} }

func (r *rlsRand) intn(n int) int {
	r.state ^= r.state << 13
	r.state ^= r.state >> 7
	r.state ^= r.state << 17
	return int(r.state % uint64(n))
}
