package sql

import (
	"reflect"
	"testing"

	"github.com/sthorne/datax/pkg/sql/parser"
)

// TestCastTypeNames (issue #343): the names a statement's casts write
// that are not built in, which the session must resolve as user-defined
// types or refuse with 42704. Every built-in spelling, alias, array form
// and qualified name is left out; every other name is found, wherever
// the cast stands.
func TestCastTypeNames(t *testing.T) {
	for _, c := range []struct {
		sql  string
		want []string
	}{
		// Built in: none to resolve.
		{`SELECT 1::int, 1::int2, 1::int4, 1::int8, 1::integer, 1::smallint, 1::bigint,
			1::float, 1::float4, 1::float8, 1::real, 1::double precision, CAST(1 AS double precision),
			1::numeric(10,2), 1::decimal, 1::dec, 1::numeric`, nil},
		{`SELECT 'a'::text, 'a'::varchar(3), 'a'::character varying, 'a'::character varying(3), 'a'::char(2),
			'a'::character, 'a'::bpchar, 'a'::name, 'a'::"char", 'a'::string`, nil},
		{`SELECT 'x'::timestamptz, 'x'::timestamp, 'x'::timestamp with time zone, 'x'::timestamp(3) without time zone,
			'x'::time, 'x'::time without time zone, 'x'::time with time zone, 'x'::timetz, 'x'::date, 'x'::interval,
			CAST('x' AS timestamp with time zone)`, nil},
		{`SELECT 'x'::bool, 'x'::boolean, 'x'::bytea, 'x'::uuid, 'x'::json, 'x'::jsonb, 'x'::tsvector, 'x'::tsquery,
			'x'::vector(3), 'x'::bytes, 'x'::blob`, nil},
		{`SELECT 'x'::int[], 'x'::_int4, 'x'::_text, 'x'::text[], 'x'::numeric(5,2)[], 'x'::_oid, 'x'::oid[]`, nil},
		{`SELECT 'x'::regclass, 'x'::pg_catalog.regclass, 'x'::regtype, 'x'::pg_catalog.regtype, 'x'::regproc,
			'x'::regprocedure, 'x'::regnamespace, 'x'::regrole, 'x'::regconfig, 'x'::oid, 'x'::xid,
			1::pg_catalog.text, 1::PG_CATALOG.INT4, 'x'::unknown`, nil},
		{`SELECT c.oid::pg_catalog.regclass::pg_catalog.text FROM pg_class c WHERE c.relname OPERATOR(pg_catalog.~) '^(t)$'`, nil},
		// Not built in: to resolve (a user-defined type, or 42704).
		{`SELECT 'a'::foo_unknown_type`, []string{"foo_unknown_type"}},
		{`SELECT 'A'::citext, 'a'::hstore, '1'::money, '<a/>'::xml, '1.2.3.4'::inet, '0/1'::pg_lsn, '{}'::int4multirange`,
			[]string{"citext", "hstore", "money", "xml", "inet", "pg_lsn", "int4multirange"}},
		{`SELECT CAST('a' AS citext), 'a'::pg_catalog.citext, 'a'::citext(3), '{a}'::citext[], '{a}'::_citext`,
			[]string{"citext", "citext[]", "_citext"}},
		{`SELECT NULL::mood, $1::mood`, []string{"mood"}},
		// Every position: WHERE, subqueries, CTEs, DML, DDL defaults and checks.
		{`SELECT 1 FROM t WHERE a::t1 = 'x' AND b IN (SELECT c::t2 FROM u) AND (SELECT 1)::t3 = 1`, []string{"t1", "t2", "t3"}},
		{`WITH w AS (SELECT 'a'::t1) SELECT * FROM (SELECT 'b'::t2) s`, []string{"t1", "t2"}},
		{`INSERT INTO t VALUES ('a'::t1) RETURNING b::t2`, []string{"t1", "t2"}},
		{`UPDATE t SET a = 'a'::t1 WHERE b::t2 = 'x'`, []string{"t1", "t2"}},
		{`CREATE TABLE t (a text DEFAULT 'a'::t1, b text CHECK (b::t2 = 'x'), CHECK (a::t3 <> 'y'))`, []string{"t1", "t2", "t3"}},
		{`ALTER TABLE t ADD COLUMN c text DEFAULT 'a'::t1`, []string{"t1"}},
		{`ALTER TABLE t ALTER COLUMN c SET DEFAULT 'a'::t1`, []string{"t1"}},
		{`CREATE VIEW v AS SELECT 'a'::t1`, []string{"t1"}},
		{`EXPLAIN SELECT 'a'::t1`, []string{"t1"}},
	} {
		stmts, err := parser.Parse(c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		if got := castTypeNames(stmts[0]); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n got %q\nwant %q", c.sql, got, c.want)
		}
	}
}
