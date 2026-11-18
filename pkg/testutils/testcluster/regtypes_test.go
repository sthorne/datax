package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestRegTypes (PG-T-012): the object identifier types on a literal.
// 'name'::regclass (a table's, an index's or a catalog's),
// regtype, regproc, regprocedure, regnamespace and regrole resolve to
// the object's OID — which ::oid yields and comparisons use — and
// print as the object's name, as PostgreSQL's do; an OID literal
// prints its object's name (or itself when it names nothing); a name
// that names nothing is PostgreSQL's error. The OIDs a client reads
// fit in 32 bits.
func TestRegTypes(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol // text results
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	for _, q := range []string{`CREATE TABLE rt (id INT PRIMARY KEY, v TEXT)`, `CREATE INDEX rt_v ON rt (v)`} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	var rtOID, idxOID string
	if err := conn.QueryRow(ctx, `SELECT oid::text FROM pg_class WHERE relname = 'rt'`).Scan(&rtOID); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT oid::text FROM pg_class WHERE relname = 'rt_v'`).Scan(&idxOID); err != nil {
		t.Fatal(err)
	}

	for q, want := range map[string]string{
		`SELECT 'pg_class'::regclass, 'pg_class'::regclass::oid, 'pg_catalog.pg_class'::regclass::oid`:                    "pg_class|1259|1259",
		`SELECT 'rt'::regclass, 'rt'::regclass::oid, 'rt'::regclass::text, 'public.rt'::regclass`:                         "rt|" + rtOID + "|rt|rt",
		`SELECT 'rt_v'::regclass, 'rt_v'::regclass::oid`:                                                                  "rt_v|" + idxOID,
		`SELECT relname FROM pg_class WHERE oid = 'rt'::regclass`:                                                         "rt",
		`SELECT relname FROM pg_class WHERE oid = 'rt_v'::regclass`:                                                       "rt_v",
		`SELECT c.oid::regclass FROM pg_class c WHERE c.relname = 'rt_v'`:                                                 "rt_v",
		`SELECT 1259::regclass, '1259'::regclass, 999999::regclass`:                                                       "pg_class|pg_class|999999",
		`SELECT 'int8'::regtype, 'int8'::regtype::oid, 'int4'::regtype::oid, 'integer'::regtype`:                          "bigint|20|23|integer",
		`SELECT 'text[]'::regtype, 'text[]'::regtype::oid, 'varchar'::regtype, 'timestamptz'::regtype`:                    "text[]|1009|character varying|timestamp with time zone",
		`SELECT 'vector'::regtype, 'vector'::regtype::oid = to_regtype('vector')::oid`:                                    "vector|t",
		`SELECT 20::regtype, 1043::regtype`:                                                                               "bigint|character varying",
		`SELECT typname FROM pg_type WHERE oid = 'int4'::regtype`:                                                         "int4",
		`SELECT 'lower'::regproc, 'lower'::regproc::oid = to_regproc('lower')::oid`:                                       "lower|t",
		`SELECT 'lower(text)'::regprocedure, 'lower(text)'::regprocedure::oid = to_regproc('lower')`:                      "lower(text)|t",
		`SELECT 'public'::regnamespace, 'public'::regnamespace::oid, 2200::regnamespace, 'pg_catalog'::regnamespace::oid`: "public|2200|public|11",
		`SELECT 'root'::regrole, 'root'::regrole::oid, 10::regrole`:                                                       "root|10|root",
		`SELECT 'rt'::regclass::oid::int8 + 0 = ` + rtOID:                                                                 "t",
	} {
		if got, err := queryCells(t, ctx, conn, q); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", q, got, err, want)
		}
	}
	for q, code := range map[string]string{
		`SELECT 'nosuch'::regclass`:     "42P01",
		`SELECT 'nosuch'::regtype`:      "42704",
		`SELECT 'nosuch'::regproc`:      "42883",
		`SELECT 'nosuch'::regnamespace`: "3F000",
		`SELECT 'nosuch'::regrole`:      "42704",
	} {
		if _, err := queryCells(t, ctx, conn, q); pgCode(err) != code {
			t.Errorf("%s: %v, want %s", q, err, code)
		}
	}

	// Every OID the catalogs expose fits an unsigned 32-bit OID.
	for _, q := range []string{
		`SELECT max(oid) FROM pg_class`, `SELECT max(oid) FROM pg_type`, `SELECT max(oid) FROM pg_proc`,
		`SELECT max(oid) FROM pg_constraint`, `SELECT max(indexrelid) FROM pg_index`, `SELECT max(attrelid) FROM pg_attribute`,
		`SELECT max(oid) FROM pg_roles`, `SELECT max(oid) FROM pg_namespace`, `SELECT max(oid) FROM pg_database`,
	} {
		var n *int64
		if err := conn.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if n != nil && (*n < 0 || *n > 1<<32-1) {
			t.Errorf("%s = %d: not a 32-bit OID", q, *n)
		}
	}
}
