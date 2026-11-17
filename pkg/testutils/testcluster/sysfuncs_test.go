package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestSystemFunctions (issue #357): the system information and privilege
// functions and keywords Rails, Hibernate, pg_dump and drivers call —
// pg_get_serial_sequence, the to_reg* lookups, pg_is_in_recovery,
// current_schemas, pg_has_role, the has_*_privilege family answered
// from the role graph and the grants, and the user / current_role /
// current_catalog keywords — each with its NULL and error cases.
func TestSystemFunctions(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	root := sql.NewSession(db, catalog.NewAccessor())
	waitForDatabases(t, ctx, root)

	for _, stmt := range []string{
		`CREATE TABLE sf_t (id SERIAL PRIMARY KEY, v INT8, note TEXT)`,
		`CREATE TABLE "SF_Mixed" (id INT8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY, n TEXT)`,
		`CREATE TABLE sf_plain (id INT8 PRIMARY KEY)`,
		`CREATE SEQUENCE sf_owned OWNED BY sf_t.v`,
		`CREATE SEQUENCE sf_free`,
		`CREATE ROLE sf_staff`,
		`CREATE ROLE sf_lead`,
		`CREATE USER sf_alice PASSWORD 'alice-pw-1' IN ROLE sf_staff`,
		`CREATE USER sf_bob PASSWORD 'bob-pw-123' NOINHERIT`,
		`GRANT sf_staff TO sf_bob`,
		`GRANT sf_staff TO sf_lead WITH ADMIN OPTION`,
		`GRANT sf_lead TO sf_alice`,
		`GRANT SELECT ON sf_t TO sf_staff`,
		`GRANT INSERT ON sf_t TO sf_alice WITH GRANT OPTION`,
		`GRANT USAGE ON SEQUENCE sf_free TO sf_alice`,
		`CREATE ROLE sf_reader`,
		`GRANT read_all TO sf_reader`,
		`CREATE DATABASE sf_db`,
		`REVOKE CONNECT ON DATABASE sf_db FROM PUBLIC`,
	} {
		execSQL(t, ctx, root, stmt)
	}

	runExprCases(t, ctx, root, []exprCase{
		// pg_get_serial_sequence: the table read as SQL reads a name, the
		// column as written; SERIAL, identity and OWNED BY sequences.
		{expr: `pg_get_serial_sequence('sf_t', 'id')`, want: "public.sf_t_id_seq"},
		{expr: `pg_get_serial_sequence('public.sf_t', 'id')`, want: "public.sf_t_id_seq"},
		{expr: `pg_get_serial_sequence('"sf_t"', 'id')`, want: "public.sf_t_id_seq"},
		{expr: `pg_get_serial_sequence('SF_T', 'id')`, want: "public.sf_t_id_seq"},
		{expr: `pg_catalog.pg_get_serial_sequence('sf_t', 'v')`, want: "public.sf_owned"},
		{expr: `pg_get_serial_sequence('"SF_Mixed"', 'id')`, want: `public."SF_Mixed_id_seq"`},
		{expr: `pg_get_serial_sequence('sf_t', 'note')`, want: "NULL"},
		{expr: `pg_get_serial_sequence(NULL, 'id')`, want: "NULL"},
		{expr: `pg_get_serial_sequence('sf_t', NULL)`, want: "NULL"},
		{expr: `pg_get_serial_sequence('SF_Mixed', 'id')`, code: sql.CodeUndefinedTable},
		{expr: `pg_get_serial_sequence('nosuch', 'id')`, code: sql.CodeUndefinedTable},
		{expr: `pg_get_serial_sequence('sf_t', 'ID')`, code: sql.CodeUndefinedColumn},
		{expr: `pg_get_serial_sequence('sf_t')`, code: sql.CodeUndefinedFunction},
		// pg_sequences, pg_class and pg_depend agree with it.
		{expr: `(SELECT count(*) FROM pg_sequences WHERE sequencename = 'sf_t_id_seq')`, want: "1"},
		{expr: `(SELECT deptype FROM pg_depend WHERE objid = to_regclass('sf_t_id_seq') AND refobjid = 'sf_t'::regclass AND refobjsubid = 1)`, want: "a"},
		{expr: `(SELECT deptype FROM pg_depend WHERE objid = to_regclass('sf_owned') AND refobjsubid = 2)`, want: "a"},
		{expr: `(SELECT deptype FROM pg_depend WHERE refobjid = '"SF_Mixed"'::regclass)`, want: "i"},

		// The to_reg* lookups: NULL where the cast would fail.
		{expr: `to_regclass('sf_t') = 'sf_t'::regclass`, want: "t"},
		{expr: `to_regclass('public.sf_t') = 'public.sf_t'::regclass`, want: "t"},
		{expr: `to_regclass('"SF_Mixed"') = '"SF_Mixed"'::regclass`, want: "t"},
		{expr: `to_regclass('sf_free') = (SELECT oid FROM pg_class WHERE relname = 'sf_free')`, want: "t"},
		{expr: `to_regclass('pg_catalog.pg_class')`, want: "1259"},
		{expr: `to_regclass('pg_class') = 'pg_class'::regclass`, want: "t"},
		{expr: `to_regclass('SF_Mixed')`, want: "NULL"},
		{expr: `to_regclass('nosuch')`, want: "NULL"},
		{expr: `to_regclass('no.such.thing.here')`, want: "NULL"},
		{expr: `to_regclass('nosuchdb.sf_t')`, want: "NULL"},
		{expr: `to_regclass('sf_db.nosuch')`, want: "NULL"},
		{expr: `to_regclass('"unterminated')`, want: "NULL"},
		{expr: `to_regclass(NULL)`, want: "NULL"},
		{expr: `to_regproc('lower') = (SELECT oid FROM pg_proc WHERE proname = 'lower')`, want: "t"},
		{expr: `to_regproc('pg_catalog.lower') = to_regprocedure('lower(text)')`, want: "t"},
		{expr: `to_regproc('nosuch')`, want: "NULL"},
		{expr: `to_regprocedure('lower')`, want: "NULL"},
		{expr: `to_regprocedure('lower(text, text)')`, want: "NULL"},
		{expr: `to_regnamespace('public')`, want: "2200"},
		{expr: `to_regnamespace('pg_catalog')`, want: "11"},
		{expr: `to_regnamespace('nosuch')`, want: "NULL"},
		{expr: `to_regrole('sf_alice') = (SELECT oid FROM pg_roles WHERE rolname = 'sf_alice')`, want: "t"},
		{expr: `to_regrole('SF_ALICE') = to_regrole('sf_alice')`, want: "t"},
		{expr: `to_regrole('nosuch')`, want: "NULL"},
		{expr: `to_regrole('public')`, want: "NULL"},
		{expr: `to_regrole(NULL)`, want: "NULL"},

		{expr: `pg_is_in_recovery()`, want: "f"},
		{expr: `pg_catalog.pg_is_in_recovery()`, want: "f"},
		{expr: `current_schemas(false)`, want: "{public}"},
		{expr: `current_schemas(true)`, want: "{pg_catalog,public}"},
		{expr: `current_schemas(NULL)`, want: "NULL"},
		{expr: `current_schemas()`, code: sql.CodeUndefinedFunction},

		// The keywords.
		{expr: `user`, want: "root"},
		{expr: `current_role`, want: "root"},
		{expr: `session_user`, want: "root"},
		{expr: `current_catalog = current_database()`, want: "t"},
		{expr: `user || '/' || current_role`, want: "root/root"},

		// has_table_privilege and its kin, for a named user: grants
		// direct and through an inherited role, the grant option, and
		// NOINHERIT.
		{expr: `has_table_privilege('sf_alice', 'sf_t', 'SELECT')`, want: "t"},
		{expr: `has_table_privilege('sf_alice', 'sf_t', 'INSERT')`, want: "t"},
		{expr: `has_table_privilege('sf_alice', 'sf_t', 'UPDATE')`, want: "f"},
		{expr: `has_table_privilege('sf_alice', 'sf_t', 'update, Insert')`, want: "t"},
		{expr: `has_table_privilege('sf_alice', 'sf_t', 'INSERT WITH GRANT OPTION')`, want: "t"},
		{expr: `has_table_privilege('sf_alice', 'sf_t', 'SELECT WITH GRANT OPTION')`, want: "f"},
		{expr: `has_table_privilege('sf_alice', 'sf_t', 'REFERENCES')`, want: "f"},
		{expr: `has_table_privilege('sf_bob', 'sf_t', 'SELECT')`, want: "f"},
		{expr: `has_table_privilege('sf_alice', 'sf_plain', 'SELECT')`, want: "f"},
		{expr: `has_table_privilege('public', 'sf_t', 'SELECT')`, want: "f"},
		{expr: `has_table_privilege('sf_alice', 'pg_class', 'SELECT')`, want: "t"},
		{expr: `has_table_privilege('sf_alice', 'pg_catalog.pg_class', 'INSERT')`, want: "f"},
		{expr: `has_table_privilege(to_regrole('sf_alice'), 'sf_t'::regclass, 'SELECT')`, want: "t"},
		{expr: `has_table_privilege(to_regrole('sf_alice'), 'sf_t', 'SELECT')`, want: "t"},
		{expr: `has_table_privilege('sf_alice', to_regclass('sf_plain'), 'SELECT')`, want: "f"},
		{expr: `has_table_privilege('sf_t', 'SELECT')`, want: "t"},
		{expr: `has_table_privilege('sf_t', 'REFERENCES, TRIGGER')`, want: "t"},
		{expr: `has_table_privilege('sf_t', 'RULE')`, want: "f"},
		{expr: `has_table_privilege(current_user, 'sf_t', 'TRUNCATE')`, want: "t"},
		{expr: `has_table_privilege(999999999, 'SELECT')`, want: "NULL"},
		{expr: `has_table_privilege(NULL, 'SELECT')`, want: "NULL"},
		{expr: `has_table_privilege('sf_t', NULL)`, want: "NULL"},
		{expr: `has_table_privilege('sf_t', 'FOO')`, code: sql.CodeInvalidParameterValue},
		{expr: `has_table_privilege('sf_t', 'SELECT,')`, code: sql.CodeInvalidParameterValue},
		{expr: `has_table_privilege('sf_t', 'USAGE')`, code: sql.CodeInvalidParameterValue},
		{expr: `has_table_privilege('nosuch', 'SELECT')`, code: sql.CodeUndefinedTable},
		{expr: `has_table_privilege('nosuch_role', 'sf_t', 'SELECT')`, code: sql.CodeUndefinedObject},
		{expr: `has_table_privilege('sf_t')`, code: sql.CodeUndefinedFunction},
		{expr: `has_any_column_privilege('sf_alice', 'sf_t', 'SELECT')`, want: "t"},
		{expr: `has_any_column_privilege('sf_alice', 'sf_t', 'UPDATE')`, want: "f"},
		{expr: `has_any_column_privilege('sf_t', 'DELETE')`, code: sql.CodeInvalidParameterValue},
		{expr: `has_column_privilege('sf_alice', 'sf_t', 'note', 'SELECT')`, want: "t"},
		{expr: `has_column_privilege('sf_alice', 'sf_t', 'note', 'UPDATE')`, want: "f"},
		{expr: `has_column_privilege('sf_t', 3::int2, 'SELECT')`, want: "t"},
		{expr: `has_column_privilege('sf_t', 99::int2, 'SELECT')`, want: "NULL"},
		{expr: `has_column_privilege('sf_t', 'nosuch', 'SELECT')`, code: sql.CodeUndefinedColumn},
		{expr: `has_column_privilege('sf_t', 'note', 'DELETE')`, code: sql.CodeInvalidParameterValue},
		{expr: `has_sequence_privilege('sf_alice', 'sf_free', 'USAGE')`, want: "t"},
		{expr: `has_sequence_privilege('sf_alice', 'sf_free', 'UPDATE')`, want: "f"},
		{expr: `has_sequence_privilege('sf_bob', 'sf_free', 'USAGE')`, want: "f"},
		{expr: `has_sequence_privilege('sf_alice', 'sf_t_id_seq', 'USAGE')`, want: "t"}, // INSERT on the owning table
		{expr: `has_sequence_privilege('sf_t', 'USAGE')`, code: sql.CodeWrongObjectType},
		{expr: `has_sequence_privilege('nosuch', 'USAGE')`, code: sql.CodeUndefinedTable},
		{expr: `has_sequence_privilege('sf_free', 'DELETE')`, code: sql.CodeInvalidParameterValue},
		{expr: `has_schema_privilege('sf_alice', 'public', 'USAGE')`, want: "t"},
		{expr: `has_schema_privilege('sf_alice', 'public', 'CREATE')`, want: "f"},
		{expr: `has_schema_privilege('public', 'CREATE')`, want: "t"},
		{expr: `has_schema_privilege('sf_alice', 'pg_catalog', 'USAGE')`, want: "t"},
		{expr: `has_schema_privilege('sf_alice', 'pg_catalog', 'CREATE')`, want: "f"},
		{expr: `has_schema_privilege(2200, 'USAGE')`, want: "t"},
		{expr: `has_schema_privilege(4242, 'USAGE')`, want: "NULL"},
		{expr: `has_schema_privilege('nosuch', 'USAGE')`, code: sql.CodeInvalidSchemaName},
		{expr: `has_schema_privilege('public', 'SELECT')`, code: sql.CodeInvalidParameterValue},
		{expr: `has_database_privilege('sf_alice', current_database(), 'CONNECT')`, want: "t"},
		{expr: `has_database_privilege('sf_alice', current_database(), 'TEMP')`, want: "t"},
		{expr: `has_database_privilege('sf_alice', current_database(), 'CREATE')`, want: "f"},
		{expr: `has_database_privilege('sf_alice', 'sf_db', 'CONNECT')`, want: "f"},
		{expr: `has_database_privilege('sf_alice', 'sf_db', 'TEMPORARY')`, want: "f"},
		{expr: `has_database_privilege('sf_db', 'CONNECT, CREATE')`, want: "t"},
		{expr: `has_database_privilege('nosuch', 'CONNECT')`, code: sql.CodeInvalidCatalogName},
		{expr: `has_database_privilege('sf_db', 'USAGE')`, code: sql.CodeInvalidParameterValue},
		{expr: `has_function_privilege('sf_alice', 'lower(text)', 'EXECUTE')`, want: "t"},
		{expr: `has_function_privilege(to_regproc('lower'), 'execute')`, want: "t"},
		{expr: `has_function_privilege('nosuch(int)', 'EXECUTE')`, code: sql.CodeUndefinedFunction},
		{expr: `has_function_privilege('lower(text)', 'USAGE')`, code: sql.CodeInvalidParameterValue},
		{expr: `has_type_privilege('sf_alice', 'int4', 'USAGE')`, want: "t"},
		{expr: `has_type_privilege('nosuch', 'USAGE')`, code: sql.CodeUndefinedObject},
		{expr: `has_tablespace_privilege('pg_default', 'CREATE')`, want: "t"},
		{expr: `has_tablespace_privilege('sf_alice', 'pg_default', 'CREATE')`, want: "f"},
		{expr: `has_tablespace_privilege('nosuch', 'CREATE')`, code: sql.CodeUndefinedObject},
		{expr: `has_foreign_data_wrapper_privilege('nosuch', 'USAGE')`, code: sql.CodeUndefinedObject},
		{expr: `has_server_privilege('nosuch', 'USAGE')`, code: sql.CodeUndefinedObject},

		// pg_has_role: MEMBER regardless of INHERIT, USAGE through it,
		// SET, and the admin option held through another role.
		{expr: `pg_has_role('sf_alice', 'sf_staff', 'MEMBER')`, want: "t"},
		{expr: `pg_has_role('sf_alice', 'sf_staff', 'USAGE')`, want: "t"},
		{expr: `pg_has_role('sf_alice', 'sf_staff', 'SET')`, want: "t"},
		{expr: `pg_has_role('sf_alice', 'sf_staff', 'MEMBER WITH ADMIN OPTION')`, want: "t"},
		{expr: `pg_has_role('sf_bob', 'sf_staff', 'MEMBER')`, want: "t"},
		{expr: `pg_has_role('sf_bob', 'sf_staff', 'USAGE')`, want: "f"},
		{expr: `pg_has_role('sf_bob', 'sf_staff', 'SET')`, want: "t"},
		{expr: `pg_has_role('sf_bob', 'sf_staff', 'USAGE WITH GRANT OPTION')`, want: "f"},
		{expr: `pg_has_role('sf_alice', 'sf_bob', 'MEMBER')`, want: "f"},
		{expr: `pg_has_role('sf_alice', 'sf_alice', 'USAGE')`, want: "t"},
		{expr: `pg_has_role(to_regrole('sf_alice'), to_regrole('sf_lead'), 'MEMBER')`, want: "t"},
		{expr: `pg_has_role('sf_staff', 'MEMBER')`, want: "t"}, // root holds every role
		{expr: `pg_has_role('sf_alice', NULL, 'MEMBER')`, want: "NULL"},
		// PostgreSQL's predefined roles: datax's counterpart where it
		// has one, else a role only admins hold (psql's \l+ asks).
		{expr: `pg_has_role('sf_alice', 'pg_read_all_stats', 'USAGE')`, want: "f"},
		{expr: `pg_has_role('pg_read_all_stats', 'USAGE')`, want: "t"},
		{expr: `pg_has_role('sf_alice', 'pg_read_all_data', 'MEMBER')`, want: "f"},
		{expr: `pg_has_role('sf_reader', 'pg_read_all_data', 'USAGE')`, want: "t"},
		{expr: `has_table_privilege('sf_reader', 'sf_plain', 'SELECT')`, want: "t"},
		{expr: `has_table_privilege('sf_reader', 'sf_plain', 'INSERT')`, want: "f"},
		{expr: `pg_has_role('sf_alice', 'nosuch', 'MEMBER')`, code: sql.CodeUndefinedObject},
		{expr: `pg_has_role('sf_alice', 'public', 'MEMBER')`, code: sql.CodeUndefinedObject},
		{expr: `pg_has_role('sf_alice', 'sf_staff', 'SELECT')`, code: sql.CodeInvalidParameterValue},
	})

	// The forms without a user answer for the current role: a session of
	// sf_alice's, and root after SET ROLE.
	alice := sql.NewSessionForUser(db, catalog.NewAccessor(), "sf_alice")
	runExprCases(t, ctx, alice, []exprCase{
		{expr: `user`, want: "sf_alice"},
		{expr: `current_role`, want: "sf_alice"},
		{expr: `has_table_privilege('sf_t', 'SELECT')`, want: "t"},
		{expr: `has_table_privilege('sf_t', 'UPDATE')`, want: "f"},
		{expr: `has_table_privilege('sf_plain', 'SELECT')`, want: "f"},
		{expr: `has_sequence_privilege('sf_free', 'USAGE')`, want: "t"},
		{expr: `has_schema_privilege('public', 'CREATE')`, want: "f"},
		{expr: `has_database_privilege('sf_db', 'CONNECT')`, want: "f"},
		{expr: `pg_has_role('sf_staff', 'USAGE')`, want: "t"},
		{expr: `pg_has_role('sf_bob', 'MEMBER')`, want: "f"},
	})
	execSQL(t, ctx, root, `SET ROLE sf_bob`)
	runExprCases(t, ctx, root, []exprCase{
		{expr: `current_role || ' ' || session_user`, want: "sf_bob root"},
		{expr: `has_table_privilege('sf_t', 'SELECT')`, want: "f"},
		{expr: `pg_has_role('sf_staff', 'SET')`, want: "t"},
	})
	execSQL(t, ctx, root, `RESET ROLE`)

	// The object argument may name a column: the catalogs' rows.
	for q, want := range map[string]string{
		`SELECT relname FROM pg_class WHERE relname LIKE 'sf%' AND relkind = 'r' AND has_table_privilege(oid, 'SELECT') ORDER BY relname`: "sf_t",
		// sf_free by its USAGE grant, which covers currval's SELECT in
		// datax; the others through INSERT on the table that owns them.
		`SELECT relname FROM pg_class WHERE relname LIKE 'sf%' AND relkind = 'S' AND has_table_privilege(oid, 'SELECT') ORDER BY relname`: "sf_free;sf_owned;sf_t_id_seq",
		`SELECT relname FROM pg_class c WHERE relname LIKE 'sf%' AND has_table_privilege('sf_alice', c.relname, 'INSERT') ORDER BY 1`:     "sf_t",
		`SELECT rolname FROM pg_roles WHERE rolname LIKE 'sf%' AND pg_has_role(oid, 'MEMBER') ORDER BY rolname`:                           "sf_alice;sf_lead;sf_staff",
		`SELECT nspname FROM pg_namespace WHERE has_schema_privilege(oid, 'CREATE') ORDER BY nspname`:                                     "",
		`SELECT relname FROM pg_class WHERE relname LIKE 'sf%' AND relkind = 'S' AND has_sequence_privilege(oid, 'USAGE') ORDER BY 1`:     "sf_free;sf_owned;sf_t_id_seq",
	} {
		r, serr := trySQL(ctx, alice, q)
		if serr != nil {
			t.Errorf("%s: %v", q, serr)
			continue
		}
		var got []string
		for _, row := range r.Rows {
			got = append(got, row[0].Text())
		}
		if s := joinRows(got); s != want {
			t.Errorf("%s: %q, want %q", q, s, want)
		}
	}
	if _, serr := trySQL(ctx, root, `SELECT has_table_privilege(rolname, 'sf_t', 'SELECT') FROM pg_roles`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Errorf("a column user argument: %v, want 0A000", serr)
	}

	// Over the wire, with parameters, as Rails sends them.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, c := range []struct {
		q    string
		args []any
		want string
	}{
		// ActiveRecord's serial_sequence / default_sequence_name.
		{`SELECT pg_get_serial_sequence($1, $2)`, []any{"sf_t", "id"}, "public.sf_t_id_seq"},
		{`SELECT pg_get_serial_sequence('"SF_Mixed"', 'id')`, nil, `public."SF_Mixed_id_seq"`},
		// ActiveRecord's pk_and_sequence_for.
		{`SELECT attr.attname, nsp.nspname, seq.relname
		  FROM pg_class seq, pg_attribute attr, pg_depend dep, pg_constraint cons, pg_namespace nsp
		  WHERE seq.oid = dep.objid AND seq.relkind = 'S' AND attr.attrelid = dep.refobjid AND attr.attnum = dep.refobjsubid
		    AND attr.attrelid = cons.conrelid AND attr.attnum = cons.conkey[1] AND seq.relnamespace = nsp.oid
		    AND cons.contype = 'p' AND dep.classid = 'pg_class'::regclass AND dep.refobjid = '"sf_t"'::regclass`, nil, "id|public|sf_t_id_seq"},
		{`SELECT current_schemas(false)`, nil, "{public}"},
		{`SELECT has_table_privilege($1, $2, 'SELECT'), to_regclass($2) IS NOT NULL, pg_is_in_recovery()`, []any{"sf_alice", "sf_t"}, "t|t|f"},
		{`SELECT user, current_role, current_catalog`, nil, "root|root|datax"},
	} {
		got, err := queryCells(t, ctx, conn, c.q, append([]any{pgx.QueryExecModeSimpleProtocol}, c.args...)...)
		if err != nil || got != c.want {
			t.Errorf("%s: %q %v, want %q", c.q, got, err, c.want)
		}
	}
	// The keywords are column labels after AS, user a bare one too; in
	// double quotes they name columns.
	execSQL(t, ctx, root, `CREATE TABLE sf_kw (id INT8 PRIMARY KEY, "current_role" TEXT, "user" TEXT)`)
	execSQL(t, ctx, root, `INSERT INTO sf_kw VALUES (1, 'a role', 'a user')`)
	rows, err := conn.Query(ctx, `SELECT current_user AS user, 1 AS select, 2 user, "current_role", "user", current_role FROM sf_kw`, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range rows.FieldDescriptions() {
		names = append(names, f.Name)
	}
	var vals []string
	for rows.Next() {
		for _, v := range rows.RawValues() {
			vals = append(vals, string(v))
		}
	}
	rows.Close()
	if joinRows(names) != "user;select;user;current_role;user;?column?" || joinRows(vals) != "root;1;2;a role;a user;root" {
		t.Errorf("labels %q, values %q", names, vals)
	}
}

func joinRows(rows []string) string {
	out := ""
	for i, r := range rows {
		if i > 0 {
			out += ";"
		}
		out += r
	}
	return out
}
