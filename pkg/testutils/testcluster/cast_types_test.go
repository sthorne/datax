package testcluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestCastToUnknownType (issue #343): a cast to a type name datax does
// not have (x::t, CAST(x AS t)) fails with PostgreSQL's 42704 type "t"
// does not exist, wherever it stands and before any row is read, rather
// than passing the value through as text. Built-in names and their
// aliases, multi-word spellings, array forms, qualified names, the
// catalog types tools cast to and the database's enums still cast.
func TestCastToUnknownType(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	one := func(q string) string {
		t.Helper()
		return strings.Join(rowsText(t, ctx, s, q), "\n")
	}
	undefined := func(q, typ string) {
		t.Helper()
		_, serr := trySQL(ctx, s, q)
		if serr == nil || serr.Code != sql.CodeUndefinedObject || serr.Msg != `type "`+typ+`" does not exist` {
			t.Fatalf("%s: %v, want 42704 type %q does not exist", q, serr, typ)
		}
	}

	execSQL(t, ctx, s, `CREATE TYPE mood AS ENUM ('sad', 'happy')`)
	execSQL(t, ctx, s, `CREATE TABLE ct (id INT PRIMARY KEY, m mood, t TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE ct_empty (id INT PRIMARY KEY, t TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO ct VALUES (1, 'happy', 'sad')`)

	// The issue's examples, and every position a cast may take.
	undefined(`SELECT 'a'::foo_unknown_type`, "foo_unknown_type")
	for _, typ := range []string{"citext", "hstore", "money", "xml", "inet", "pg_lsn", "int4multirange"} {
		undefined(`SELECT 'x'::`+typ, typ)
		undefined(`SELECT CAST('x' AS `+typ+`)`, typ)
	}
	undefined(`SELECT 'x'::pg_catalog.citext`, "citext")
	undefined(`SELECT 'x'::citext(10)`, "citext")
	undefined(`SELECT '{x}'::citext[]`, "citext[]")
	undefined(`SELECT NULL::citext`, "citext")
	undefined(`SELECT t::citext FROM ct_empty`, "citext")
	undefined(`SELECT id FROM ct WHERE t::citext = 'SAD'`, "citext")
	undefined(`SELECT id FROM ct WHERE id IN (SELECT id FROM ct_empty WHERE t::citext = 'x')`, "citext")
	undefined(`WITH w AS (SELECT 'a'::citext AS v) SELECT v FROM w`, "citext")
	undefined(`SELECT pg_typeof('a'::citext)`, "citext")
	undefined(`INSERT INTO ct VALUES (2, 'sad', 'a'::citext)`, "citext")
	undefined(`UPDATE ct SET t = 'a'::citext`, "citext")
	undefined(`DELETE FROM ct WHERE t::citext = 'x'`, "citext")
	undefined(`EXPLAIN SELECT 'a'::citext`, "citext")
	undefined(`CREATE VIEW ctv AS SELECT 'a'::citext AS v`, "citext")
	undefined(`CREATE TABLE ct2 (id INT PRIMARY KEY, t TEXT DEFAULT 'a'::citext)`, "citext")
	undefined(`CREATE TABLE ct2 (id INT PRIMARY KEY, t TEXT CHECK (t::citext <> 'x'))`, "citext")
	undefined(`ALTER TABLE ct ALTER COLUMN t SET DEFAULT 'a'::citext`, "citext")
	undefined(`ALTER TABLE ct ADD COLUMN u TEXT DEFAULT 'a'::citext`, "citext")
	// Column types were already refused the same way.
	undefined(`CREATE TABLE ct2 (id INT PRIMARY KEY, t citext)`, "citext")
	undefined(`ALTER TABLE ct ADD COLUMN u foo_unknown_type`, "foo_unknown_type")
	undefined(`ALTER TABLE ct ALTER COLUMN t TYPE foo_unknown_type`, "foo_unknown_type")
	if got := one(`SELECT count(*) FROM ct WHERE t = 'sad'`); got != "1" {
		t.Fatalf("the refused statements changed ct: %q", got)
	}

	// Built-in names, aliases and spellings keep casting.
	for q, want := range map[string]string{
		`SELECT '7'::int, '7'::int2, '7'::int4, '7'::integer, '7'::smallint, '7'::bigint, '7'::int8`:                                                                         "7|7|7|7|7|7|7",
		`SELECT '1.5'::float8, '1.5'::float4, '1.5'::real, '1.5'::double precision, CAST(1.5 AS double precision)`:                                                           "1.5|1.5|1.5|1.5|1.5",
		`SELECT '1.5'::numeric(5,2), '1.5'::decimal, '1.5'::dec, '1.5'::numeric`:                                                                                             "1.50|1.5|1.5|1.5",
		`SELECT 'abc'::varchar(2), 'abc'::character varying(2), 'abc'::character varying, 'abc'::char(2), 'abc'::bpchar, 'abc'::name, 'abc'::"char", 'abc'::pg_catalog.text`: "ab|ab|abc|ab|abc|abc|abc|abc",
		`SELECT '2024-01-02 03:04:05+00'::timestamptz, '2024-01-02 03:04:05'::timestamp without time zone, CAST('2024-01-02' AS timestamp with time zone)`:                   "2024-01-02 03:04:05+00|2024-01-02 03:04:05|2024-01-02 00:00:00+00",
		`SELECT '2024-01-02'::date, '10:00'::time, '10:00'::time without time zone, '1 day'::interval`:                                                                       "2024-01-02|10:00:00|10:00:00|1 day",
		`SELECT 't'::bool, 'f'::boolean, '\x01'::bytea, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, '{"a":1}'::json, '{"a":1}'::jsonb`:                                     `t|f|\x01|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|{"a":1}|{"a":1}`,
		`SELECT '{1,2}'::int[], '{1,2}'::_int4, '{a}'::text[], '{a}'::_text`:                                                                                                 "{1,2}|{1,2}|{a}|{a}",
		`SELECT 'ct'::regclass = 'ct'::pg_catalog.regclass, 'int4'::regtype, 'pg_catalog'::pg_catalog.regnamespace, '5'::oid`:                                                "t|integer|pg_catalog|5",
		`SELECT 'happy'::mood, CAST('sad' AS mood), m::text, t::mood FROM ct`:                                                                                                "happy|sad|happy|sad",
		`SELECT count(*) FROM ct WHERE m = 'happy'::mood AND t::mood = 'sad'`:                                                                                                "1",
		`SELECT NULL::mood IS NULL, '{happy}'::mood[], '{happy}'::_mood`:                                                                                                     "t|{happy}|{happy}",
		`SELECT c.oid::pg_catalog.regclass::pg_catalog.text FROM pg_catalog.pg_class c WHERE c.relname = 'ct'`:                                                               "ct",
		`SELECT DATE '2024-01-02', INTERVAL '2 days', TIMESTAMPTZ '2024-01-02 00:00:00+00'`:                                                                                  "2024-01-02|2 days|2024-01-02 00:00:00+00",
	} {
		if got := one(q); got != want {
			t.Errorf("%s:\n got %q\nwant %q", q, got, want)
		}
	}

	// In a transaction block the refusal fails the block; a type created
	// in it is seen by the check.
	execSQL(t, ctx, s, `BEGIN`)
	execSQL(t, ctx, s, `CREATE TYPE size AS ENUM ('s', 'l')`)
	if got := one(`SELECT 'l'::size, NULL::size IS NULL`); got != "l|t" {
		t.Fatalf("a type of the open transaction: %q", got)
	}
	undefined(`SELECT 'x'::citext`, "citext")
	if _, serr := trySQL(ctx, s, `SELECT 1`); serr == nil || serr.Code != sql.CodeInFailedTransaction {
		t.Fatalf("after the refusal: %v, want 25P02", serr)
	}
	execSQL(t, ctx, s, `ROLLBACK`)
	undefined(`SELECT 'l'::size`, "size")

	// Over the wire, simple and extended protocol alike.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement} {
		var v string
		err := conn.QueryRow(ctx, `SELECT 'A'::citext`, mode).Scan(&v)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42704" || pgErr.Message != `type "citext" does not exist` {
			t.Fatalf("mode %v: %v (%q), want 42704", mode, err, v)
		}
		if err := conn.QueryRow(ctx, `SELECT $1::text::mood::text`, mode, "happy").Scan(&v); err != nil || v != "happy" {
			t.Fatalf("mode %v: %v %q", mode, err, v)
		}
	}
}
