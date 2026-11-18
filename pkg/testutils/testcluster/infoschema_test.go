package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// isColumns returns a query's result column names and wire type OIDs,
// as name:oid.
func isColumns(t *testing.T, ctx context.Context, c *pgx.Conn, query string) string {
	t.Helper()
	rows, err := c.Query(ctx, query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	var out []string
	for _, f := range rows.FieldDescriptions() {
		out = append(out, f.Name+":"+itoaOID(f.DataTypeOID))
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return strings.Join(out, " ")
}

func itoaOID(oid uint32) string {
	if oid == 0 {
		return "0"
	}
	var b []byte
	for oid > 0 {
		b = append([]byte{byte('0' + oid%10)}, b...)
		oid /= 10
	}
	return string(b)
}

// TestInformationSchemaViews covers gap PG-C-001: information_schema's
// views exist with PostgreSQL's columns, in PostgreSQL's order and
// types (sql_identifier and character_data as text, yes_or_no as
// varchar(3), cardinal_number as integer), their rows come from the
// catalog (sequences, grants, roles, constraints, views), and each view
// lists only what PostgreSQL's would show the reader: a role without
// grants does not see another role's tables in table_privileges.
func TestInformationSchemaViews(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := rlsConn(t, ctx, tc, 0, "root")
	for _, stmt := range []string{
		`CREATE USER alice`,
		`CREATE USER bob`,
		`CREATE ROLE staff`,
		`GRANT staff TO alice WITH ADMIN OPTION`,
		`CREATE TABLE parent (id INT PRIMARY KEY, code VARCHAR(10) NOT NULL UNIQUE, price NUMERIC(10,2) CHECK (price >= 0), tags TEXT[], created TIMESTAMP(3), d DATE)`,
		`CREATE TABLE child (cid INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, pid INT REFERENCES parent (id) ON DELETE CASCADE, note TEXT)`,
		`CREATE SEQUENCE s1 START 5 INCREMENT 2`,
		`CREATE TABLE ser (id SERIAL PRIMARY KEY, v INT)`,
		`CREATE VIEW pv AS SELECT id, code FROM parent WHERE id > 1`,
		`GRANT SELECT ON parent TO alice`,
		`GRANT INSERT, UPDATE ON child TO alice WITH GRANT OPTION`,
		`GRANT USAGE ON SEQUENCE s1 TO alice`,
		`GRANT SELECT ON pv TO PUBLIC`,
	} {
		rlsExec(t, ctx, root, stmt)
	}

	// ---- shapes: PostgreSQL 16's columns, order and types ----
	for q, want := range map[string]string{
		`SELECT * FROM information_schema.sequences`:         "sequence_catalog:25 sequence_schema:25 sequence_name:25 data_type:25 numeric_precision:23 numeric_precision_radix:23 numeric_scale:23 start_value:25 minimum_value:25 maximum_value:25 increment:25 cycle_option:1043",
		`SELECT * FROM information_schema.table_privileges`:  "grantor:25 grantee:25 table_catalog:25 table_schema:25 table_name:25 privilege_type:25 is_grantable:1043 with_hierarchy:1043",
		`SELECT * FROM information_schema.role_table_grants`: "grantor:25 grantee:25 table_catalog:25 table_schema:25 table_name:25 privilege_type:25 is_grantable:1043 with_hierarchy:1043",
		`SELECT * FROM information_schema.column_privileges`: "grantor:25 grantee:25 table_catalog:25 table_schema:25 table_name:25 column_name:25 privilege_type:25 is_grantable:1043",
		`SELECT * FROM information_schema.usage_privileges`:  "grantor:25 grantee:25 object_catalog:25 object_schema:25 object_name:25 object_type:25 privilege_type:25 is_grantable:1043",
		`SELECT * FROM information_schema.applicable_roles`:  "grantee:25 role_name:25 is_grantable:1043",
		`SELECT * FROM information_schema.enabled_roles`:     "role_name:25",
		`SELECT * FROM information_schema.check_constraints`: "constraint_catalog:25 constraint_schema:25 constraint_name:25 check_clause:25",
		`SELECT * FROM information_schema.view_table_usage`:  "view_catalog:25 view_schema:25 view_name:25 table_catalog:25 table_schema:25 table_name:25",
		`SELECT * FROM information_schema.triggers`:          "trigger_catalog:25 trigger_schema:25 trigger_name:25 event_manipulation:25 event_object_catalog:25 event_object_schema:25 event_object_table:25 action_order:23 action_condition:25 action_statement:25 action_orientation:25 action_timing:25 action_reference_old_table:25 action_reference_new_table:25 action_reference_old_row:25 action_reference_new_row:25 created:1184",
		`SELECT * FROM information_schema.parameters`:        "specific_catalog:25 specific_schema:25 specific_name:25 ordinal_position:23 parameter_mode:25 is_result:1043 as_locator:1043 parameter_name:25 data_type:25 character_maximum_length:23 character_octet_length:23 character_set_catalog:25 character_set_schema:25 character_set_name:25 collation_catalog:25 collation_schema:25 collation_name:25 numeric_precision:23 numeric_precision_radix:23 numeric_scale:23 datetime_precision:23 interval_type:25 interval_precision:23 udt_catalog:25 udt_schema:25 udt_name:25 scope_catalog:25 scope_schema:25 scope_name:25 maximum_cardinality:23 dtd_identifier:25 parameter_default:25",
		`SELECT * FROM information_schema.table_constraints`: "constraint_catalog:25 constraint_schema:25 constraint_name:25 table_catalog:25 table_schema:25 table_name:25 constraint_type:25 is_deferrable:1043 initially_deferred:1043 enforced:1043 nulls_distinct:1043",
	} {
		if got := isColumns(t, ctx, root, q); got != want {
			t.Fatalf("%s:\n got %s\nwant %s", q, got, want)
		}
	}
	// routines has PostgreSQL's 82 columns; it, and the views of what
	// datax has none of, are empty.
	if got := strings.Count(isColumns(t, ctx, root, `SELECT * FROM information_schema.routines`), ":"); got != 82 {
		t.Fatalf("routines has %d columns", got)
	}
	for _, v := range []string{"routines", "parameters", "triggers", "domains", "attributes", "user_defined_types", "routine_privileges", "udt_privileges", "foreign_tables", "transforms"} {
		rlsWant(t, ctx, root, []string{"0"}, `SELECT count(*) FROM information_schema.`+v)
	}

	// ---- rows ----
	rlsWant(t, ctx, root, []string{
		"datax|public|s1|bigint|64|2|0|5|1|9223372036854775807|2|NO",
		"datax|public|ser_id_seq|bigint|64|2|0|1|1|9223372036854775807|1|NO",
	}, `SELECT * FROM information_schema.sequences ORDER BY sequence_name`) // the identity column's sequence is not listed
	rlsWant(t, ctx, root, []string{
		"root|alice|child|INSERT|YES|NO", "root|alice|child|UPDATE|YES|NO",
		"root|root|child|DELETE|YES|NO", "root|root|child|INSERT|YES|NO", "root|root|child|REFERENCES|YES|NO", "root|root|child|SELECT|YES|YES",
		"root|root|child|TRIGGER|YES|NO", "root|root|child|TRUNCATE|YES|NO", "root|root|child|UPDATE|YES|NO",
		"root|alice|parent|SELECT|NO|YES",
		"root|PUBLIC|pv|SELECT|NO|YES",
	}, `SELECT grantor, grantee, table_name, privilege_type, is_grantable, with_hierarchy FROM information_schema.table_privileges
		WHERE table_name IN ('child', 'parent', 'pv') AND (grantee <> 'root' OR table_name = 'child') ORDER BY table_name, grantee, privilege_type`)
	rlsWant(t, ctx, root, []string{"alice|code|SELECT|NO", "alice|id|SELECT|NO"},
		`SELECT grantee, column_name, privilege_type, is_grantable FROM information_schema.column_privileges WHERE table_name = 'parent' AND grantee = 'alice' AND column_name IN ('id', 'code') ORDER BY column_name`)
	rlsWant(t, ctx, root, []string{"root|PUBLIC|C|COLLATION|NO", "root|alice|s1|SEQUENCE|NO", "root|root|s1|SEQUENCE|YES"},
		`SELECT grantor, grantee, object_name, object_type, is_grantable FROM information_schema.usage_privileges WHERE object_name IN ('C', 's1') ORDER BY object_name, grantee`)
	rlsWant(t, ctx, root, []string{"alice|staff|YES", "root|admin|YES"},
		`SELECT * FROM information_schema.applicable_roles ORDER BY grantee`)
	rlsWant(t, ctx, root, []string{"2200_" + oidOf(t, ctx, root, "child") + "_1_not_null|cid IS NOT NULL", "parent_price_check|(price >= 0)"},
		`SELECT constraint_name, check_clause FROM information_schema.check_constraints WHERE constraint_name LIKE '%_`+oidOf(t, ctx, root, "child")+`_%' OR constraint_name = 'parent_price_check' ORDER BY 1`)
	rlsWant(t, ctx, root, []string{"child_pid_fkey|parent|id", "child_pkey|child|cid", "parent_code_key|parent|code", "parent_pkey|parent|id", "parent_price_check|parent|price"},
		`SELECT constraint_name, table_name, column_name FROM information_schema.constraint_column_usage WHERE table_name IN ('parent', 'child') ORDER BY 1`)
	rlsWant(t, ctx, root, []string{"child_pid_fkey|pid|1|1", "child_pkey|cid|1|<nil>"},
		`SELECT constraint_name, column_name, ordinal_position, position_in_unique_constraint FROM information_schema.key_column_usage WHERE table_name = 'child' ORDER BY 1`)
	rlsWant(t, ctx, root, []string{"pv|parent"}, `SELECT view_name, table_name FROM information_schema.view_table_usage`)
	rlsWant(t, ctx, root, []string{"pv|parent|code", "pv|parent|id"}, `SELECT view_name, table_name, column_name FROM information_schema.view_column_usage ORDER BY column_name`)
	rlsWant(t, ctx, root, []string{"parent|4|text|text|a4"},
		`SELECT object_name, collection_type_identifier, data_type, udt_name, dtd_identifier FROM information_schema.element_types`)
	rlsWant(t, ctx, root, []string{"pv|SELECT id, code FROM parent WHERE id > 1;|NONE|NO"},
		`SELECT table_name, view_definition, check_option, is_updatable FROM information_schema.views WHERE table_schema = 'public'`)
	rlsWant(t, ctx, root, []string{
		"cid|<nil>|integer|32|2|0|<nil>|YES|ALWAYS|1|1",
		"code|10|character varying|<nil>|<nil>|<nil>|<nil>|NO|<nil>|<nil>|<nil>",
		"created|<nil>|timestamp without time zone|<nil>|<nil>|<nil>|3|NO|<nil>|<nil>|<nil>",
		"price|<nil>|numeric|10|10|2|<nil>|NO|<nil>|<nil>|<nil>",
	}, `SELECT column_name, character_maximum_length, data_type, numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision,
		is_identity, identity_generation, identity_start, identity_increment
		FROM information_schema.columns WHERE column_name IN ('cid', 'code', 'created', 'price') AND table_name IN ('child', 'parent') ORDER BY 1`)
	rlsWant(t, ctx, root, []string{"||UTF8|UCS|UTF8|datax|pg_catalog|C"}, `SELECT coalesce(character_set_catalog, ''), coalesce(character_set_schema, ''), character_set_name, character_repertoire, form_of_use, default_collate_catalog, default_collate_schema, default_collate_name FROM information_schema.character_sets`)

	// ---- what each reader sees ----
	alice := rlsConn(t, ctx, tc, 0, "alice")
	bob := rlsConn(t, ctx, tc, 0, "bob")
	rlsWant(t, ctx, alice, []string{
		"root|alice|child|INSERT|YES", "root|alice|child|UPDATE|YES", "root|alice|parent|SELECT|NO", "root|PUBLIC|pv|SELECT|NO",
	}, `SELECT grantor, grantee, table_name, privilege_type, is_grantable FROM information_schema.table_privileges WHERE table_schema = 'public' ORDER BY table_name, grantee, privilege_type`)
	// No grant: none of root's tables, only what was granted to PUBLIC.
	rlsWant(t, ctx, bob, []string{"root|PUBLIC|pv|SELECT"},
		`SELECT grantor, grantee, table_name, privilege_type FROM information_schema.table_privileges WHERE table_schema = 'public'`)
	rlsWant(t, ctx, bob, []string{}, `SELECT table_name FROM information_schema.role_table_grants`)
	rlsWant(t, ctx, bob, []string{}, `SELECT table_name FROM information_schema.column_privileges WHERE table_name <> 'pv'`)
	rlsWant(t, ctx, bob, []string{}, `SELECT sequence_name FROM information_schema.sequences`)
	rlsWant(t, ctx, bob, []string{"bob"}, `SELECT role_name FROM information_schema.enabled_roles`)
	rlsWant(t, ctx, bob, []string{}, `SELECT grantee FROM information_schema.applicable_roles`)
	rlsWant(t, ctx, alice, []string{"alice|INSERT", "alice|SELECT", "alice|UPDATE"},
		`SELECT DISTINCT grantee, privilege_type FROM information_schema.role_table_grants ORDER BY 2`)
	rlsWant(t, ctx, alice, []string{"s1"}, `SELECT sequence_name FROM information_schema.sequences`)
	rlsWant(t, ctx, alice, []string{"alice", "staff"}, `SELECT role_name FROM information_schema.enabled_roles ORDER BY 1`)
	rlsWant(t, ctx, alice, []string{"alice|staff|YES"}, `SELECT * FROM information_schema.administrable_role_authorizations`)
	// Constraints: table_constraints for a privilege other than SELECT,
	// check_constraints and the usage views for the owner only; a view's
	// definition for its owner.
	rlsWant(t, ctx, alice, []string{"child"}, `SELECT DISTINCT table_name FROM information_schema.table_constraints WHERE table_schema = 'public'`)
	rlsWant(t, ctx, alice, []string{}, `SELECT constraint_name FROM information_schema.check_constraints WHERE constraint_schema = 'public'`)
	rlsWant(t, ctx, alice, []string{}, `SELECT constraint_name FROM information_schema.constraint_column_usage`)
	rlsWant(t, ctx, alice, []string{"pv|<nil>"}, `SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = 'public'`)
	rlsWant(t, ctx, bob, []string{"pv"}, `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`)

	// The views are listed in pg_class under information_schema, by
	// their own names.
	rlsWant(t, ctx, root, []string{"information_schema|sequences"},
		`SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'sequences'`)
	// information_schema is not on the search path: its views are not
	// visible (Django lists the visible relations outside pg_catalog).
	rlsWant(t, ctx, root, []string{"false|true"}, `SELECT pg_table_is_visible('information_schema.tables'::regclass), pg_table_is_visible('parent'::regclass)`)
	rlsWant(t, ctx, root, []string{"child", "parent", "pv", "ser"},
		`SELECT c.relname FROM pg_class c WHERE c.relkind IN ('r', 'v') AND c.relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname <> 'pg_catalog') AND pg_table_is_visible(c.oid) ORDER BY 1`)
}

// oidOf is a table's pg_class OID as text.
func oidOf(t *testing.T, ctx context.Context, c *pgx.Conn, table string) string {
	t.Helper()
	return rlsRows(t, ctx, c, `SELECT oid FROM pg_class WHERE relname = $1`, table)[0]
}

// TestCatalogDefsOfLiteralOIDs covers gap PG-C-007: pg_get_indexdef and
// pg_get_constraintdef on an OID (a literal, a regclass, pg_class.oid),
// for a table keyed by the hidden rowid among others, render the real
// key columns — never an empty list — and the call is named as
// PostgreSQL names it.
func TestCatalogDefsOfLiteralOIDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	c := rlsConn(t, ctx, tc, 0, "root")
	for _, stmt := range []string{
		`CREATE TABLE src (a INT PRIMARY KEY, b TEXT)`,
		`CREATE TABLE ctas AS SELECT * FROM src`,
		`CREATE INDEX ctas_b ON ctas (b)`,
		`ALTER TABLE ctas ADD CONSTRAINT ctas_a_key UNIQUE (a)`,
		`ALTER TABLE ctas ADD CONSTRAINT ctas_a_check CHECK (a > 0)`,
	} {
		rlsExec(t, ctx, c, stmt)
	}
	rlsWant(t, ctx, c, []string{"CREATE UNIQUE INDEX src_pkey ON public.src USING btree (a)"}, `SELECT pg_get_indexdef('src_pkey'::regclass)`)
	rlsWant(t, ctx, c, []string{"CREATE INDEX ctas_b ON public.ctas USING btree (b)"}, `SELECT pg_get_indexdef('ctas_b'::regclass)`)
	rlsWant(t, ctx, c, []string{"b"}, `SELECT pg_get_indexdef('ctas_b'::regclass, 1, true)`)
	oid := rlsRows(t, ctx, c, `SELECT 'ctas_a_key'::regclass::oid`)[0]
	rlsWant(t, ctx, c, []string{"CREATE UNIQUE INDEX ctas_a_key ON public.ctas USING btree (a)"}, `SELECT pg_get_indexdef(`+oid+`)`)
	rlsWant(t, ctx, c, []string{"UNIQUE (a)"}, `SELECT pg_get_constraintdef(`+oid+`)`)
	rlsWant(t, ctx, c, []string{"<nil>"}, `SELECT pg_get_indexdef(12345)`)
	for _, r := range rlsRows(t, ctx, c, `SELECT oid FROM pg_constraint WHERE conrelid = 'ctas'::regclass`) {
		def := rlsRows(t, ctx, c, `SELECT pg_get_constraintdef(`+r+`)`)[0]
		if def != "UNIQUE (a)" && def != "CHECK (a > 0)" {
			t.Fatalf("pg_get_constraintdef(%s) = %q", r, def)
		}
	}
	// Over the catalogs: every index and constraint of the rowid table,
	// none with an empty column list, and no primary key at all.
	for _, q := range []string{
		`SELECT pg_get_indexdef(c.oid) FROM pg_class c WHERE c.relkind = 'i' AND c.relname LIKE 'ctas%'`,
		`SELECT pg_get_indexdef(i.indexrelid) FROM pg_index i WHERE i.indrelid = 'ctas'::regclass`,
		`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'ctas'::regclass`,
		`SELECT indexdef FROM pg_indexes WHERE tablename = 'ctas'`,
	} {
		got := rlsRows(t, ctx, c, q)
		if len(got) != 2 {
			t.Fatalf("%s: %v", q, got)
		}
		for _, def := range got {
			if strings.Contains(def, "()") || strings.Contains(def, "PRIMARY KEY") || strings.Contains(def, "pkey") {
				t.Fatalf("%s: %q", q, def)
			}
		}
	}
	// The output column carries the function's name.
	if got := isColumns(t, ctx, c, `SELECT pg_get_indexdef(i.indexrelid), pg_get_constraintdef(x.oid) FROM pg_index i, pg_constraint x LIMIT 1`); got != "pg_get_indexdef:25 pg_get_constraintdef:25" {
		t.Fatalf("columns: %s", got)
	}
}

// TestTimezoneCatalogs covers gap PG-C-008: pg_timezone_names and
// pg_timezone_abbrevs, from the IANA database Go embeds, with
// PostgreSQL's columns and types.
func TestTimezoneCatalogs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := privCluster(t)
	c := rlsConn(t, ctx, tc, 0, "root")
	if got := isColumns(t, ctx, c, `SELECT * FROM pg_timezone_names`); got != "name:25 abbrev:25 utc_offset:1186 is_dst:16" {
		t.Fatalf("pg_timezone_names: %s", got)
	}
	if got := isColumns(t, ctx, c, `SELECT * FROM pg_catalog.pg_timezone_abbrevs`); got != "abbrev:25 utc_offset:1186 is_dst:16" {
		t.Fatalf("pg_timezone_abbrevs: %s", got)
	}
	rlsWant(t, ctx, c, []string{"Asia/Kolkata|IST|05:30:00|false", "Etc/GMT+5|-05|-05:00:00|false", "UTC|UTC|00:00:00|false"},
		`SELECT name, abbrev, utc_offset::text, is_dst FROM pg_timezone_names WHERE name IN ('UTC', 'Asia/Kolkata', 'Etc/GMT+5') ORDER BY name`)
	rlsWant(t, ctx, c, []string{"true"}, `SELECT count(*) > 100 FROM pg_timezone_names WHERE name LIKE 'America/%'`)
	rlsWant(t, ctx, c, []string{"CEST|02:00:00|true", "EDT|-04:00:00|true", "EST|-05:00:00|false", "UTC|00:00:00|false"},
		`SELECT abbrev, utc_offset::text, is_dst FROM pg_timezone_abbrevs WHERE abbrev IN ('EST', 'EDT', 'CEST', 'UTC') ORDER BY abbrev`)
}

// TestReportedParameters covers gap PG-W-003: the parameters a
// connection is told of at startup are PostgreSQL 14's GUC_REPORT set
// with the session's values, a changed one is reported again, and the
// server's preset options are settings.
func TestReportedParameters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := rlsConn(t, ctx, tc, 0, "root")
	rlsExec(t, ctx, root, `CREATE USER carol`)
	cfg, err := pgx.ParseConfig("postgres://root@" + tc.Nodes[0].SQLAddr() + "/datax?sslmode=disable&application_name=probe")
	if err != nil {
		t.Fatal(err)
	}
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	for name, want := range map[string]string{
		"server_version": "14.0 datax", "server_encoding": "UTF8", "client_encoding": "UTF8", "DateStyle": "ISO, MDY",
		"IntervalStyle": "postgres", "integer_datetimes": "on", "standard_conforming_strings": "on", "TimeZone": "UTC",
		"application_name": "probe", "default_transaction_read_only": "off", "in_hot_standby": "off",
		"is_superuser": "on", "session_authorization": "root",
	} {
		if got := c.PgConn().ParameterStatus(name); got != want {
			t.Fatalf("ParameterStatus(%s) = %q, want %q", name, got, want)
		}
	}
	rlsExec(t, ctx, c, `SET default_transaction_read_only = on`)
	if got := c.PgConn().ParameterStatus("default_transaction_read_only"); got != "on" {
		t.Fatalf("default_transaction_read_only after SET: %q", got)
	}
	carol := rlsConn(t, ctx, tc, 0, "carol")
	if got := carol.PgConn().ParameterStatus("is_superuser"); got != "off" {
		t.Fatalf("carol is_superuser: %q", got)
	}
	if got := carol.PgConn().ParameterStatus("session_authorization"); got != "carol" {
		t.Fatalf("carol session_authorization: %q", got)
	}
	rlsWant(t, ctx, carol, []string{"off|carol"}, `SELECT current_setting('is_superuser'), current_setting('session_authorization')`)
	for name, want := range map[string]string{
		"server_version_num": "140000", "max_identifier_length": "63", "block_size": "8192", "max_index_keys": "32",
		"max_function_args": "100", "lc_collate": "C", "lc_ctype": "C", "in_hot_standby": "off", "data_checksums": "off", "segment_size": "1GB",
	} {
		rlsWant(t, ctx, root, []string{want}, `SHOW `+name)
	}
	rlsRefused(t, ctx, root, "55P02", `SET is_superuser = off`)
	rlsRefused(t, ctx, root, "55P02", `SET block_size = 16384`)
	// SHOW ALL leaves out is_superuser and session_authorization, as
	// PostgreSQL's does.
	rlsWant(t, ctx, root, []string{"0"}, `SELECT count(*) FROM pg_settings WHERE name IN ('is_superuser', 'session_authorization')`)
	rlsWant(t, ctx, root, []string{"1"}, `SELECT count(*) FROM pg_settings WHERE name = 'in_hot_standby'`)
}
