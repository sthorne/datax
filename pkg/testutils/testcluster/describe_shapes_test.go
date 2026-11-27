package testcluster

import (
	"context"
	"fmt"
	"go/ast"
	goparser "go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/sql/parser"
)

// describeSample is one statement walked through the extended protocol.
type describeSample struct {
	sql string
	// rows: the statement must return at least one row, so the path that
	// sends them is exercised and not just described.
	rows bool
	// fails: the statement is expected to be refused (at Parse or
	// Execute); what is checked is that no rows arrive undescribed.
	fails bool
}

// describeSamples lists, for every statement kind the parser produces
// (every type with a stmt() method, found from the parser's source by
// statementKinds), statements of that kind in the order they run on one
// connection. A kind that returns rows only in some forms (ALTER TABLE
// ... PURGE and SPLIT AT, SHOW's forms) has a sample of each.
var describeSamples = []struct {
	kind    string
	samples []describeSample
}{
	{"CreateDatabase", []describeSample{{sql: `CREATE DATABASE shapes_other`}}},
	{"AlterDatabase", []describeSample{{sql: `ALTER DATABASE shapes_other RENAME TO shapes_other2`}}},
	{"CreateSchema", []describeSample{{sql: `CREATE SCHEMA shapes_s`}}},
	{"AlterSchemaRename", []describeSample{{sql: `ALTER SCHEMA shapes_s RENAME TO shapes_s2`}}},
	{"CreateTable", []describeSample{{sql: `CREATE TABLE shapes (id INT8 PRIMARY KEY, v TEXT)`}, {sql: `CREATE TABLE shapes_s2.moved (a INT8)`}}},
	{"SetSchema", []describeSample{{sql: `ALTER TABLE shapes_s2.moved SET SCHEMA public`}}},
	{"Insert", []describeSample{
		{sql: `INSERT INTO shapes VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')`},
		{sql: `INSERT INTO shapes VALUES (5, 'e') RETURNING id, v`, rows: true},
	}},
	{"Update", []describeSample{
		{sql: `UPDATE shapes SET v = 'z' WHERE id = 5`},
		{sql: `UPDATE shapes SET v = 'y' WHERE id = 5 RETURNING v`, rows: true},
		{sql: `UPDATE shapes s SET v = x.v FROM (VALUES (1, 'q')) AS x(id, v) WHERE s.id = x.id`},
		{sql: `UPDATE shapes AS s SET v = x.v || '!' FROM (VALUES (1, 'q')) AS x(id, v) WHERE s.id = x.id RETURNING s.id, x.v, s.v`, rows: true},
	}},
	{"Delete", []describeSample{
		{sql: `DELETE FROM shapes WHERE id = 4`},
		{sql: `DELETE FROM shapes WHERE id = 5 RETURNING *`, rows: true},
		{sql: `DELETE FROM shapes s USING (VALUES (3)) AS x(id) WHERE s.id = x.id RETURNING *`, rows: true},
	}},
	{"Select", []describeSample{
		{sql: `SELECT * FROM shapes ORDER BY id`, rows: true},
		{sql: `SELECT 1, now(), 'x'::text`, rows: true},
		{sql: `SELECT * FROM unnest(ARRAY[1, 2, 3]) AS u(x)`, rows: true},
		{sql: `SELECT s.id, u.x FROM shapes s LEFT JOIN unnest(ARRAY[1, 2]) AS u(x) ON u.x = s.id ORDER BY s.id`, rows: true},
		{sql: `SELECT ae.name, ae.default_version FROM pg_available_extensions() AS ae`},
		{sql: `SELECT count(*) FROM information_schema.tables`, rows: true},
		{sql: `SELECT relname FROM pg_class LIMIT 1`, rows: true},
		{sql: `SELECT v, SUM(id) * 2 / COUNT(*), ROUND(AVG(id), 1) FROM shapes GROUP BY v HAVING COUNT(*) > 0 ORDER BY SUM(id) DESC`, rows: true},
		{sql: `SELECT id % 2, GROUPING(id % 2), COUNT(*) FROM shapes GROUP BY ROLLUP (id % 2)`, rows: true},
		{sql: `SELECT DISTINCT ON (id % 2) id % 2, v FROM shapes ORDER BY id % 2, v DESC`, rows: true},
	}},
	{"CreateIndex", []describeSample{{sql: `CREATE INDEX shapes_v ON shapes (v)`}}},
	{"AlterIndex", []describeSample{{sql: `ALTER INDEX shapes_v RENAME TO shapes_v2`}}},
	{"AlterTable", []describeSample{
		{sql: `ALTER TABLE shapes ADD COLUMN w INT8`},
		{sql: `ALTER TABLE shapes SPLIT AT VALUES (2)`, rows: true},
		{sql: `ALTER TABLE shapes UNSPLIT AT VALUES (2)`, rows: true},
		{sql: `ALTER TABLE shapes SPLIT AT VALUES (3)`, rows: true},
		{sql: `ALTER TABLE shapes UNSPLIT ALL`, rows: true},
		{sql: `ALTER TABLE shapes PURGE`, rows: true},
		// IF EXISTS on a missing table: described as returning rows,
		// completes with none.
		{sql: `ALTER TABLE IF EXISTS shapes_missing PURGE`},
	}},
	{"CreateView", []describeSample{{sql: `CREATE VIEW shapes_view AS SELECT id, v FROM shapes`}}},
	{"CreateSequence", []describeSample{{sql: `CREATE SEQUENCE shapes_seq`}}},
	{"AlterSequence", []describeSample{{sql: `ALTER SEQUENCE shapes_seq RESTART WITH 10`}}},
	{"CreateType", []describeSample{{sql: `CREATE TYPE shapes_mood AS ENUM ('a', 'b')`}}},
	{"AlterType", []describeSample{{sql: `ALTER TYPE shapes_mood ADD VALUE 'c'`}}},
	{"CommentOn", []describeSample{{sql: `COMMENT ON TABLE shapes IS 'shapes'`}}},
	{"CreateExtension", []describeSample{{sql: `CREATE EXTENSION IF NOT EXISTS vector`}}},
	{"DropExtension", []describeSample{{sql: `DROP EXTENSION IF EXISTS "uuid-ossp"`}}},
	{"Analyze", []describeSample{{sql: `ANALYZE shapes`}}},
	{"Vacuum", []describeSample{{sql: `VACUUM shapes`}, {sql: `VACUUM (ANALYZE) shapes`}}},
	{"ShowStats", []describeSample{{sql: `SHOW STATS FOR shapes`, rows: true}}},
	{"ShowTables", []describeSample{{sql: `SHOW TABLES`, rows: true}}},
	{"ShowDatabases", []describeSample{{sql: `SHOW DATABASES`, rows: true}}},
	{"ShowPlacement", []describeSample{
		{sql: `SHOW PLACEMENT`, rows: true},
		{sql: `SHOW PLACEMENT FOR DATABASE shapes_other2`, rows: true},
	}},
	{"ShowRanges", []describeSample{
		{sql: `SHOW RANGES`, rows: true},
	}},
	{"ShowNodes", []describeSample{{sql: `SHOW NODES`, rows: true}}},
	{"ShowSequences", []describeSample{{sql: `SHOW SEQUENCES`, rows: true}}},
	{"ShowFunctions", []describeSample{{sql: `SHOW FUNCTIONS`, rows: true}}},
	{"Show", []describeSample{
		{sql: `SHOW COLUMNS FROM shapes`, rows: true},
		{sql: `SHOW INDEXES FROM shapes`, rows: true},
		{sql: `SHOW CREATE TABLE shapes`, rows: true},
		{sql: `SHOW CREATE VIEW shapes_view`, rows: true},
		{sql: `SHOW VIEWS`, rows: true},
		{sql: `SHOW USERS`, rows: true},
		{sql: `SHOW ROLES`, rows: true},
		{sql: `SHOW GRANTS ON TABLE shapes`},
		{sql: `SHOW GRANTS ON ROLE`},
		{sql: `SHOW ALL`, rows: true},
		{sql: `SHOW SESSIONS`, rows: true},
	}},
	{"SetVar", []describeSample{
		{sql: `SET application_name = 'shapes'`},
		{sql: `SHOW application_name`, rows: true},
		{sql: `SHOW TRANSACTION ISOLATION LEVEL`, rows: true},
		{sql: `RESET application_name`},
	}},
	{"Explain", []describeSample{
		{sql: `EXPLAIN SELECT * FROM shapes`, rows: true},
		{sql: `EXPLAIN ANALYZE SELECT * FROM shapes`, rows: true},
	}},
	{"SetClusterSetting", []describeSample{{sql: `SET CLUSTER SETTING key_service.rate_limit = 50`}}},
	{"ShowClusterSetting", []describeSample{{sql: `SHOW CLUSTER SETTING key_service.rate_limit`, rows: true}}},
	// No key service in a test cluster: refused, which is what is checked.
	{"CreateEncryptionKey", []describeSample{{sql: `CREATE ENCRYPTION KEY shapes_key`, fails: true}}},
	{"AlterEncryptionKey", []describeSample{{sql: `ALTER ENCRYPTION KEY shapes_key ROTATE`, fails: true}}},
	{"ShowEncryptionKeys", []describeSample{{sql: `SHOW ENCRYPTION KEYS`, fails: true}}},
	{"DropEncryptionKey", []describeSample{{sql: `DROP ENCRYPTION KEY IF EXISTS shapes_key`, fails: true}}},
	{"CreateRole", []describeSample{{sql: `CREATE ROLE shapes_role`}, {sql: `CREATE ROLE shapes_role2`}}},
	{"GrantRevoke", []describeSample{
		{sql: `GRANT SELECT ON shapes TO shapes_role`},
		{sql: `REVOKE SELECT ON shapes FROM shapes_role`},
	}},
	{"AlterDefaultPrivileges", []describeSample{{sql: `ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO shapes_role`}}},
	{"AlterOwner", []describeSample{{sql: `ALTER TABLE shapes OWNER TO shapes_role`}}},
	{"ReassignOwned", []describeSample{{sql: `REASSIGN OWNED BY shapes_role TO shapes_role2`}}},
	{"CreatePolicy", []describeSample{{sql: `CREATE POLICY shapes_p ON shapes USING (true)`}}},
	{"AlterPolicy", []describeSample{{sql: `ALTER POLICY shapes_p ON shapes RENAME TO shapes_q`}}},
	{"DropPolicy", []describeSample{{sql: `DROP POLICY shapes_q ON shapes`}}},
	{"DropOwned", []describeSample{{sql: `DROP OWNED BY shapes_role`}}},
	{"DropRole", []describeSample{{sql: `DROP ROLE shapes_role`}}},
	{"Begin", []describeSample{{sql: `BEGIN`}}},
	{"Savepoint", []describeSample{{sql: `SAVEPOINT shapes_sp`}}},
	{"RollbackToSavepoint", []describeSample{{sql: `ROLLBACK TO SAVEPOINT shapes_sp`}}},
	{"ReleaseSavepoint", []describeSample{{sql: `RELEASE SAVEPOINT shapes_sp`}}},
	{"LockTable", []describeSample{{sql: `LOCK TABLE shapes IN ACCESS SHARE MODE`}}},
	{"DeclareCursor", []describeSample{{sql: `DECLARE shapes_c CURSOR FOR SELECT 1 AS one`}}},
	{"Fetch", []describeSample{{sql: `FETCH ALL FROM shapes_c`, rows: true}}},
	{"CloseCursor", []describeSample{{sql: `CLOSE shapes_c`}}},
	{"Prepare", []describeSample{{sql: `PREPARE shapes_ps AS SELECT 1 AS one`}}},
	{"ExecutePrepared", []describeSample{{sql: `EXECUTE shapes_ps`, rows: true}}},
	{"Deallocate", []describeSample{{sql: `DEALLOCATE shapes_ps`}}},
	{"Commit", []describeSample{{sql: `COMMIT`}}},
	{"SetTransactionSnapshot", []describeSample{{sql: `SET TRANSACTION SNAPSHOT 'x'`, fails: true}}},
	{"SetTransactionAsOf", []describeSample{{sql: `SET TRANSACTION AS OF SYSTEM TIME '-1s'`, fails: true}}},
	{"Rollback", []describeSample{{sql: `ROLLBACK`}}},
	{"Listen", []describeSample{{sql: `LISTEN shapes_ch`}}},
	{"Notify", []describeSample{{sql: `NOTIFY shapes_ch`}}},
	{"Unlisten", []describeSample{{sql: `UNLISTEN shapes_ch`}}},
	{"Discard", []describeSample{{sql: `DISCARD PLANS`}, {sql: `DISCARD ALL`}}},
	{"CopyFrom", []describeSample{{sql: `COPY shapes FROM STDIN`, fails: true}}},
	{"CopyTo", []describeSample{
		{sql: `COPY shapes TO STDOUT`},
		{sql: `COPY (SELECT id FROM shapes ORDER BY id) TO STDOUT WITH (FORMAT csv)`},
	}},
	{"Truncate", []describeSample{{sql: `TRUNCATE shapes`}}},
	{"DropView", []describeSample{{sql: `DROP VIEW shapes_view`}}},
	{"DropIndex", []describeSample{{sql: `DROP INDEX shapes_v2`}}},
	{"DropSequence", []describeSample{{sql: `DROP SEQUENCE shapes_seq`}}},
	{"DropTable", []describeSample{{sql: `DROP TABLE shapes`}}},
	{"DropType", []describeSample{{sql: `DROP TYPE shapes_mood`}}},
	{"DropDatabase", []describeSample{{sql: `DROP DATABASE shapes_other2`}}},
	{"DropSchema", []describeSample{{sql: `DROP SCHEMA shapes_s2 CASCADE`}}},
	{"Use", []describeSample{{sql: `USE datax`}}},
}

// statementKinds returns the name of every type in the parser package
// with a stmt() method: every kind of statement there is.
func statementKinds(t *testing.T) []string {
	t.Helper()
	dir := filepath.Join("..", "..", "sql", "parser")
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var kinds []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := goparser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range af.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Name.Name != "stmt" || fd.Type.Params.NumFields() != 0 || fd.Recv == nil || len(fd.Recv.List) != 1 {
				continue
			}
			if star, ok := fd.Recv.List[0].Type.(*ast.StarExpr); ok {
				if id, ok := star.X.(*ast.Ident); ok {
					kinds = append(kinds, id.Name)
				}
			}
		}
	}
	sort.Strings(kinds)
	if len(kinds) < 40 {
		t.Fatalf("found only %d statement kinds in %s: %v", len(kinds), dir, kinds)
	}
	return kinds
}

// TestExtendedDescribeEveryStatementKind walks a statement of every kind
// through Parse / Describe (statement) / Bind / Describe (portal) /
// Execute and checks the extended protocol's promise: Describe answers
// RowDescription for a statement that returns rows and NoData for one
// that does not, both Describes agree, and Execute sends DataRows only
// after a RowDescription, each as wide as it. Issue #322: ALTER TABLE
// ... PURGE (and SPLIT AT, SHOW DATABASES, SHOW PLACEMENT, ...) were
// described as NoData and then sent rows. A statement kind added to the
// parser without a sample here fails the test.
func TestExtendedDescribeEveryStatementKind(t *testing.T) {
	tc := Start(t, 3)
	covered := map[string]bool{}
	for _, k := range describeSamples {
		covered[k.kind] = true
	}
	for _, kind := range statementKinds(t) {
		if !covered[kind] {
			t.Errorf("statement kind %s has no sample in describeSamples", kind)
		}
	}

	r := dialRaw(t, tc)
	for _, k := range describeSamples {
		for _, s := range k.samples {
			stmts, err := parser.Parse(s.sql)
			if err != nil || len(stmts) != 1 {
				t.Fatalf("%s: parse: %v", s.sql, err)
			}
			if got := reflect.TypeOf(stmts[0]).Elem().Name(); got != k.kind {
				t.Fatalf("%s parses as %s, listed under %s", s.sql, got, k.kind)
			}
			t.Logf("walking %s", s.sql)
			walkExtended(t, r, s)
		}
	}
}

// walkExtended runs one sample through the extended protocol and checks
// the message order.
func walkExtended(t *testing.T, r *rawFrontend, s describeSample) {
	t.Helper()
	r.send(
		&pgproto3.Parse{Query: s.sql},
		&pgproto3.Describe{ObjectType: 'S'},
		&pgproto3.Bind{},
		&pgproto3.Describe{ObjectType: 'P'},
		&pgproto3.Execute{},
		&pgproto3.Sync{},
	)
	var (
		stmtDesc, portalDesc *pgproto3.RowDescription
		describedS           bool
		describedP           bool
		rows                 int
		failed               string
		completed            bool
	)
	for done := false; !done; {
		switch m := r.recv().(type) {
		case *pgproto3.ParseComplete, *pgproto3.BindComplete, *pgproto3.ParameterDescription,
			*pgproto3.NoticeResponse, *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
		case *pgproto3.RowDescription:
			c := *m
			c.Fields = append([]pgproto3.FieldDescription(nil), m.Fields...)
			if !describedS {
				stmtDesc, describedS = &c, true
			} else if !describedP {
				portalDesc, describedP = &c, true
			} else {
				t.Errorf("%s: a third RowDescription", s.sql)
			}
		case *pgproto3.NoData:
			if !describedS {
				describedS = true
			} else if !describedP {
				describedP = true
			} else {
				t.Errorf("%s: a third NoData", s.sql)
			}
		case *pgproto3.DataRow:
			if !describedP {
				t.Errorf("%s: DataRow before both Describes answered", s.sql)
			} else if portalDesc == nil {
				t.Errorf("%s: DataRow after the portal was described as NoData", s.sql)
			} else if len(m.Values) != len(portalDesc.Fields) {
				t.Errorf("%s: DataRow of %d values, RowDescription of %d fields", s.sql, len(m.Values), len(portalDesc.Fields))
			}
			rows++
		case *pgproto3.CopyOutResponse, *pgproto3.CopyData, *pgproto3.CopyDone:
			// COPY TO: described as NoData, its rows go out in copy-out
			// mode.
			if !describedP || portalDesc != nil {
				t.Errorf("%s: %T from a statement not described as NoData", s.sql, m)
			}
		case *pgproto3.CommandComplete:
			completed = true
		case *pgproto3.ErrorResponse:
			failed = m.Code + " " + m.Message
		case *pgproto3.ReadyForQuery:
			done = true
		default:
			t.Errorf("%s: unexpected %T", s.sql, m)
		}
	}
	if s.fails {
		if failed == "" {
			t.Errorf("%s: expected a refusal, completed with %d rows", s.sql, rows)
		}
		return
	}
	if failed != "" || !completed {
		t.Errorf("%s: %s", s.sql, failed)
		return
	}
	if !describedS || !describedP {
		t.Errorf("%s: Describe not answered (statement %v, portal %v)", s.sql, describedS, describedP)
		return
	}
	if (stmtDesc == nil) != (portalDesc == nil) || (stmtDesc != nil && len(stmtDesc.Fields) != len(portalDesc.Fields)) {
		t.Errorf("%s: statement and portal Describe disagree: %v vs %v", s.sql, fieldNames(stmtDesc), fieldNames(portalDesc))
	}
	if s.rows && rows == 0 {
		t.Errorf("%s: no rows (described as %v)", s.sql, fieldNames(portalDesc))
	}
}

func fieldNames(rd *pgproto3.RowDescription) []string {
	if rd == nil {
		return nil
	}
	out := make([]string, len(rd.Fields))
	for i, f := range rd.Fields {
		out[i] = string(f.Name)
	}
	return out
}

// TestPurgeOverExtendedProtocol is the report as a client sees it: pgx
// in its default extended mode and in simple mode gets PURGE's report as
// rows of eight named columns, and so do SPLIT AT, SHOW DATABASES and
// SHOW PLACEMENT; the two modes describe the same columns.
func TestPurgeOverExtendedProtocol(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	setup, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close(ctx)
	for _, q := range []string{
		`CREATE TABLE p (id INT8 PRIMARY KEY, v TEXT)`,
		`INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c')`,
		`DELETE FROM p WHERE id = 2`,
	} {
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	queries := []string{
		`ALTER TABLE p PURGE`,
		`ALTER TABLE p SPLIT AT VALUES (2)`,
		`ALTER TABLE p UNSPLIT ALL`,
		`SHOW DATABASES`,
		`SHOW PLACEMENT`,
		`SHOW GRANTS ON ROLE`,
	}
	names := map[string][]string{}
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeDescribeExec, pgx.QueryExecModeSimpleProtocol} {
		cfg, err := pgx.ParseConfig(pgURL(tc, 0))
		if err != nil {
			t.Fatal(err)
		}
		cfg.DefaultQueryExecMode = mode
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queries {
			rows, err := conn.Query(ctx, q)
			if err != nil {
				t.Fatalf("%v %s: %v", mode, q, err)
			}
			var got []string
			for _, f := range rows.FieldDescriptions() {
				got = append(got, f.Name)
			}
			n := 0
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					t.Fatalf("%v %s: %v", mode, q, err)
				}
				if len(vals) != len(got) {
					t.Fatalf("%v %s: row of %d values, %d columns", mode, q, len(vals), len(got))
				}
				if q == `ALTER TABLE p PURGE` {
					if status, _ := vals[7].(string); status != "clean" {
						t.Errorf("%v purge row status %v", mode, vals)
					}
				}
				n++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("%v %s: %v", mode, q, err)
			}
			if q == `ALTER TABLE p PURGE` && (n == 0 || len(got) != 8 || got[0] != "range_id") {
				t.Errorf("%v purge: %d rows of %v", mode, n, got)
			}
			if prev, ok := names[q]; ok && fmt.Sprint(prev) != fmt.Sprint(got) {
				t.Errorf("%s: %v describes %v, an earlier mode %v", q, mode, got, prev)
			}
			names[q] = got
		}
		conn.Close(ctx)
	}
}
