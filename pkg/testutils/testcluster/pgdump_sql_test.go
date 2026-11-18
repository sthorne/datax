package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestPgDumpSQL covers, statement by statement, the SQL pg_dump and its
// restore send (issue #366), whether or not pg_dump is installed:
// set_config over pg_settings, acldefault() and the ACL columns, LOCK
// TABLE, PREPARE / EXECUTE, cursors, snapshot export and import, ALTER
// SEQUENCE ... OWNED BY, ALTER COLUMN ... ADD GENERATED AS IDENTITY,
// ALTER VIEW / SCHEMA / LARGE OBJECT ... OWNER TO, schema-qualified
// sequence names, and the catalog rows pg_dump orders its output by.
func TestPgDumpSQL(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	h := nopkHelpers{t, ctx, s}

	h.exec(`CREATE ROLE reader`)
	h.exec(`CREATE TABLE a (id INT8 PRIMARY KEY, v TEXT)`)
	h.exec(`CREATE TABLE b (k INT8 NOT NULL, v TEXT)`)
	h.exec(`CREATE TABLE z (n INT8)`)
	h.exec(`CREATE VIEW av AS SELECT id FROM a`)
	h.exec(`INSERT INTO a VALUES (1, 'x'), (2, 'y')`)
	h.exec(`INSERT INTO b VALUES (7, 'p'), (8, 'q')`)
	h.exec(`GRANT SELECT, INSERT ON a TO reader`)

	// set_config with its name read from the row: once per row, none
	// for a setting there is no row for.
	h.expect(`SELECT set_config(name, '7s', false) FROM pg_settings WHERE name = 'statement_timeout'`, "7s")
	h.expect(`SHOW statement_timeout`, "7000")
	h.expect(`SELECT count(*) FROM (SELECT set_config(name, 'x', false) FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind') q`, "0")
	h.exec(`SET statement_timeout = 0`)

	// acldefault and the ACLs it is compared with.
	h.expect(`SELECT acldefault('r', 10), acldefault('s', 10), acldefault('n', 10), acldefault('d', 10)`,
		"{root=arwdDxt/root}|{root=rwU/root}|{root=UC/root}|{=Tc/root,root=CTc/root}")
	h.expect(`SELECT relname, relacl, acldefault(CASE WHEN relkind = 'S' THEN 's' ELSE 'r' END, relowner) FROM pg_class WHERE relname IN ('a', 'b') ORDER BY relname`,
		"a|{root=arwdDxt/root,reader=ar/root}|{root=arwdDxt/root};b|NULL|{root=arwdDxt/root}")
	h.expect(`SELECT nspowner, nspacl FROM pg_namespace WHERE nspname = 'public'`, "10|{root=UC/root,=U/root}")
	h.expect(`SELECT description FROM pg_description WHERE objoid = 2200 AND classoid = 2615`, "standard public schema")
	h.exec(`ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO reader`)
	h.expect(`SELECT defaclrole, defaclnamespace, defaclobjtype, defaclacl FROM pg_default_acl`, "10|0|r|{root=arwdDxt/root,reader=r/root}")

	// pg_get_viewdef ends with the semicolon pg_dump strips.
	h.expect(`SELECT pg_get_viewdef('av'::regclass)`, "SELECT id FROM a;")
	// A table with only its primary key has an index (pg_dump reads the
	// keys of the tables with relhasindex).
	h.expect(`SELECT relhasindex FROM pg_class WHERE relname = 'a'`, "t")

	// LOCK TABLE: in a block; the weak modes only.
	h.code(`LOCK TABLE a`, sql.CodeNoActiveTransaction)
	h.exec(`BEGIN`)
	if r := h.exec(`LOCK TABLE public.a, b IN ACCESS SHARE MODE`); r.Tag != "LOCK TABLE" {
		t.Fatalf("LOCK tag %q", r.Tag)
	}
	h.exec(`LOCK a IN ROW EXCLUSIVE MODE NOWAIT`)
	h.code(`LOCK TABLE a IN EXCLUSIVE MODE`, sql.CodeFeatureNotSupported)
	h.exec(`ROLLBACK`)
	h.exec(`BEGIN`)
	h.code(`LOCK TABLE nope IN ACCESS SHARE MODE`, sql.CodeUndefinedTable)
	h.exec(`ROLLBACK`)

	// PREPARE / EXECUTE / DEALLOCATE.
	h.exec(`PREPARE q(pg_catalog.oid) AS SELECT relname FROM pg_class WHERE oid = $1`)
	h.code(`PREPARE q AS SELECT 1`, sql.CodeDuplicatePreparedStatement)
	h.expect(`EXECUTE q('`+h.exec(`SELECT 'a'::regclass::oid`).Rows[0][0].Text()+`')`, "a")
	h.code(`EXECUTE q`, sql.CodeSyntaxError)
	h.code(`EXECUTE nope(1)`, sql.CodeInvalidSQLStatementName)
	h.exec(`DEALLOCATE q`)
	h.code(`DEALLOCATE q`, sql.CodeInvalidSQLStatementName)
	h.exec(`PREPARE r AS SELECT 1`)
	h.exec(`DEALLOCATE ALL`)
	h.code(`EXECUTE r`, sql.CodeInvalidSQLStatementName)

	// Cursors.
	h.code(`DECLARE c CURSOR FOR SELECT id FROM a`, sql.CodeNoActiveTransaction)
	h.exec(`BEGIN`)
	h.exec(`DECLARE c CURSOR FOR SELECT id FROM a ORDER BY id`)
	h.code(`DECLARE c CURSOR FOR SELECT 1`, sql.CodeDuplicateCursor)
	h.expect(`FETCH 1 IN c`, "1")
	h.expect(`FETCH ALL FROM c`, "2")
	h.expect(`FETCH NEXT c`, "")
	h.exec(`CLOSE c`)
	h.code(`FETCH c`, sql.CodeInvalidCursorName)
	h.exec(`ROLLBACK`)
	h.exec(`BEGIN`)
	h.exec(`DECLARE d CURSOR FOR SELECT id FROM a`)
	h.exec(`COMMIT`)
	h.code(`FETCH d`, sql.CodeInvalidCursorName) // gone with its transaction

	// Snapshots: the importer reads what the exporter saw.
	h.exec(`BEGIN`)
	snap := h.exec(`SELECT pg_export_snapshot()`).Rows[0][0].Text()
	h.exec(`COMMIT`)
	h.exec(`INSERT INTO a VALUES (3, 'later')`)
	h.exec(`BEGIN`)
	h.exec(`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`)
	h.exec(`SET TRANSACTION SNAPSHOT '` + snap + `'`)
	h.expect(`SELECT count(*) FROM a`, "2")
	h.lastCode(`INSERT INTO a VALUES (4, 'no')`) // a snapshot's transaction reads only
	h.exec(`ROLLBACK`)
	h.exec(`BEGIN`)
	h.exec(`SELECT 1 FROM a`)
	h.code(`SET TRANSACTION SNAPSHOT '`+snap+`'`, sql.CodeActiveTransaction)
	h.exec(`ROLLBACK`)
	h.exec(`BEGIN`)
	h.code(`SET TRANSACTION SNAPSHOT 'not-a-snapshot'`, sql.CodeInvalidParameterValue)
	h.exec(`ROLLBACK`)
	h.code(`SET TRANSACTION SNAPSHOT '`+snap+`'`, sql.CodeNoActiveTransaction)

	// Identity added to an existing column, then OWNED BY.
	h.code(`ALTER TABLE z ALTER COLUMN n ADD GENERATED ALWAYS AS IDENTITY`, sql.CodeObjectNotInState) // nullable
	h.exec(`ALTER TABLE b ALTER COLUMN k ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.b_k_custom START WITH 100 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1)`)
	h.code(`ALTER TABLE b ALTER COLUMN k ADD GENERATED ALWAYS AS IDENTITY`, sql.CodeObjectNotInState)
	h.expect(`INSERT INTO b (v) VALUES ('r') RETURNING k`, "100")
	h.expect(`SELECT attidentity, atthasdef FROM pg_attribute WHERE attrelid = 'b'::regclass AND attname = 'k'`, "d|f")
	h.expect(`SELECT pg_get_serial_sequence('b', 'k')`, "public.b_k_custom")
	h.exec(`CREATE SEQUENCE z_seq`)
	h.exec(`ALTER SEQUENCE public.z_seq OWNED BY public.z.n`)
	h.expect(`SELECT pg_get_serial_sequence('z', 'n')`, "public.z_seq")
	h.exec(`ALTER TABLE ONLY z ALTER COLUMN n SET DEFAULT nextval('public.z_seq'::regclass)`)
	h.expect(`SELECT count(*) FROM pg_depend WHERE classid = 'pg_attrdef'::regclass AND refobjid = 'z_seq'::regclass AND deptype = 'n'`, "1")
	h.expect(`SELECT setval('public.z_seq', 41, true)`, "41")
	h.expect(`SELECT nextval('public.z_seq')`, "42")
	h.exec(`ALTER SEQUENCE z_seq OWNED BY NONE`)
	h.expect(`SELECT count(*) FROM pg_depend WHERE objid = 'z_seq'::regclass AND deptype = 'a'`, "0")
	h.expect(`SELECT count(*) FROM pg_depend WHERE classid = 'pg_class'::regclass AND objid = 'av'::regclass AND refobjid = 'a'::regclass`, "1")

	// Owners.
	h.exec(`CREATE USER alice PASSWORD 'pw'`)
	h.exec(`ALTER VIEW av OWNER TO alice`)
	h.expect(`SELECT pg_get_userbyid(relowner) IS NOT NULL, relowner = (SELECT oid FROM pg_roles WHERE rolname = 'alice') FROM pg_class WHERE relname = 'av'`, "t|t")
	h.exec(`ALTER SCHEMA public OWNER TO root`)
	h.exec(`ALTER SCHEMA public OWNER TO pg_database_owner`)
	h.code(`ALTER SCHEMA public OWNER TO alice`, sql.CodeFeatureNotSupported)
	h.code(`ALTER SCHEMA other OWNER TO root`, sql.CodeInvalidSchemaName)

	// Settings and spellings the dump's script uses.
	h.exec(`SET default_table_access_method = heap`)
	h.code(`SET default_table_access_method = columnar`, sql.CodeInvalidParameterValue)
	h.exec(`TRUNCATE TABLE ONLY z`)
	h.expect(`SELECT typname[0] = '_', typname FROM pg_type WHERE typname IN ('_text', 'text') ORDER BY typname`, "t|_text;f|text")
	h.expect(`SELECT quote_ident('plain'), quote_ident('Mixed'), quote_ident('select')`, `plain|"Mixed"|"select"`)
	h.expect(`SELECT option_name, option_value FROM pg_options_to_table('{a=1,b=x y}') ORDER BY 1`, "a|1;b|x y")
	h.expect(`SELECT format_type(t.oid, a.atttypmod) FROM pg_attribute a JOIN pg_type t ON a.atttypid = t.oid WHERE a.attrelid = 'a'::regclass AND a.attname = 'v'`, "text")
	if r := h.exec(`SELECT count(*) FROM pg_class WHERE relnamespace = 11 AND relname LIKE 'datax%' AND oid > 4294967295`); r.Rows[0][0].Text() != "0" {
		t.Fatalf("a system table's OID is past 32 bits")
	}
}

// lastCode runs q and returns its SQLSTATE ("" when it succeeds), for
// an error whose code is whatever the engine's refusal of it is.
func (h nopkHelpers) lastCode(q string) string {
	h.t.Helper()
	_, serr := trySQL(h.ctx, h.s, q)
	if serr == nil {
		h.t.Fatalf("%s: succeeded", q)
	}
	return serr.Code
}
