package testcluster

import (
	"context"
	dbsql "database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestORMIntrospectionQueries: the schema-introspection queries the
// popular ORMs run (SQLAlchemy's and Django's PostgreSQL dialects, and
// the information_schema shape sqlc and friends use), through
// database/sql over the extended protocol with bound parameters.
func TestORMIntrospectionQueries(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE TABLE t (id INT8 PRIMARY KEY, name TEXT NOT NULL, qty INT8 DEFAULT 0)`)
	execSQL(t, ctx, root, `CREATE INDEX t_qty ON t (qty)`)

	db, err := dbsql.Open("pgx", "postgres://root@"+tc.Nodes[0].SQLAddr()+"/datax?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// SQLAlchemy: table names in a schema.
	rows, err := db.QueryContext(ctx, `SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relkind in ('r', 'p')`, "public")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	rows.Close()
	if len(names) != 1 || names[0] != "t" {
		t.Fatalf("table names: %v", names)
	}

	// SQLAlchemy: the table's OID, then its columns (a correlated default
	// lookup beside a LEFT JOIN to pg_description).
	var oid int64
	if err := db.QueryRowContext(ctx, `SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1 AND n.nspname = $2`, "t", "public").Scan(&oid); err != nil {
		t.Fatal(err)
	}
	rows, err = db.QueryContext(ctx, `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) AS DEFAULT,
  a.attnotnull, a.attrelid as table_oid, pgd.description as comment, a.attgenerated as generated, a.attidentity as identity_options
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_description pgd ON (pgd.objoid = a.attrelid AND pgd.objsubid = a.attnum)
WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`, oid)
	if err != nil {
		t.Fatal(err)
	}
	type col struct {
		name, typ string
		def       dbsql.NullString
		notnull   bool
	}
	var cols []col
	for rows.Next() {
		var c col
		var tableOID int64
		var comment, generated, identity dbsql.NullString
		if err := rows.Scan(&c.name, &c.typ, &c.def, &c.notnull, &tableOID, &comment, &generated, &identity); err != nil {
			t.Fatal(err)
		}
		if tableOID != oid {
			t.Fatalf("table_oid %d, want %d", tableOID, oid)
		}
		cols = append(cols, c)
	}
	rows.Close()
	if len(cols) != 3 || cols[0].name != "id" || cols[0].typ != "bigint" || !cols[0].notnull || cols[0].def.Valid ||
		cols[2].name != "qty" || cols[2].notnull || cols[2].def.String != "0" {
		t.Fatalf("sqlalchemy columns: %+v", cols)
	}

	// SQLAlchemy: primary key columns through indkey (an int2vector).
	rows, err = db.QueryContext(ctx, `SELECT a.attname FROM pg_catalog.pg_class t JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid JOIN pg_catalog.pg_attribute a ON t.oid = a.attrelid AND a.attnum = ANY(ix.indkey) WHERE t.oid = $1 AND ix.indisprimary ORDER BY a.attnum`, oid)
	if err != nil {
		t.Fatal(err)
	}
	var pk []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		pk = append(pk, n)
	}
	rows.Close()
	if len(pk) != 1 || pk[0] != "id" {
		t.Fatalf("primary key: %v", pk)
	}

	// Django: relations of the connected database.
	rows, err = db.QueryContext(ctx, `SELECT c.relname,
  CASE WHEN c.relispartition THEN 'p' WHEN c.relkind IN ('m', 'v') THEN 'v' ELSE 't' END
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_inherits i ON c.oid = i.inhrelid
LEFT JOIN pg_catalog.pg_class p ON i.inhparent = p.oid
WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v')
  AND c.relnamespace IN (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname NOT IN ('pg_catalog', 'pg_toast') AND nspname NOT LIKE 'pg\_%')
  AND pg_catalog.pg_table_is_visible(c.oid)`)
	if err != nil {
		t.Fatal(err)
	}
	var rels [][2]string
	for rows.Next() {
		var r [2]string
		if err := rows.Scan(&r[0], &r[1]); err != nil {
			t.Fatal(err)
		}
		rels = append(rels, r)
	}
	rows.Close()
	if len(rels) != 1 || rels[0] != [2]string{"t", "t"} {
		t.Fatalf("django relations: %v", rels)
	}

	// Django: the table description (boolean expressions as values, a
	// LEFT JOIN to pg_collation, a NOT LIKE with an escaped wildcard).
	rows, err = db.QueryContext(ctx, `SELECT a.attname AS column_name,
  NOT (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) AS is_nullable,
  pg_get_expr(ad.adbin, ad.adrelid) AS column_default,
  CASE WHEN collname = 'default' THEN NULL ELSE collname END AS collation,
  a.attidentity != '' AS is_autofield
FROM pg_attribute a
LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
LEFT JOIN pg_collation co ON a.attcollation = co.oid
JOIN pg_type t ON a.atttypid = t.oid
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v')
  AND c.relname = $1
  AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
  AND pg_catalog.pg_table_is_visible(c.oid)`, "t")
	if err != nil {
		t.Fatal(err)
	}
	type dcol struct {
		name      string
		nullable  bool
		def       dbsql.NullString
		collation dbsql.NullString
		autofield bool
	}
	var dcols []dcol
	for rows.Next() {
		var c dcol
		if err := rows.Scan(&c.name, &c.nullable, &c.def, &c.collation, &c.autofield); err != nil {
			t.Fatal(err)
		}
		dcols = append(dcols, c)
	}
	rows.Close()
	if len(dcols) != 3 || dcols[0].name != "id" || dcols[0].nullable || dcols[1].nullable || !dcols[2].nullable ||
		dcols[2].def.String != "0" || dcols[0].collation.Valid || dcols[0].autofield {
		t.Fatalf("django description: %+v", dcols)
	}

	// information_schema, as sqlc / migration tools read it, with a
	// bound parameter.
	rows, err = db.QueryContext(ctx, `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position`, "t")
	if err != nil {
		t.Fatal(err)
	}
	var infos [][3]string
	for rows.Next() {
		var r [3]string
		if err := rows.Scan(&r[0], &r[1], &r[2]); err != nil {
			t.Fatal(err)
		}
		infos = append(infos, r)
	}
	rows.Close()
	if len(infos) != 3 || infos[0] != [3]string{"id", "bigint", "NO"} || infos[2] != [3]string{"qty", "bigint", "YES"} {
		t.Fatalf("information_schema.columns: %v", infos)
	}

	// The driver-startup shape: the types a type map asks for.
	var typOID int64
	if err := db.QueryRowContext(ctx, `SELECT t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace ns ON typnamespace = ns.oid WHERE typname = $1`, "jsonb").Scan(&typOID); err != nil || typOID != 3802 {
		t.Fatalf("pg_type lookup: %d %v", typOID, err)
	}
}
