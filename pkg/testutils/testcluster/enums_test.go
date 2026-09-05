package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// TestEnums (issue #96, part four): CREATE TYPE ... AS ENUM, columns
// of the type (plain, keyed and indexed), labels in and out, ordering
// by declaration, casts, ALTER TYPE ADD VALUE seen by a second
// gateway, DROP TYPE and its refusal, the catalogs (pg_type, pg_enum,
// information_schema, \d, \dT), the wire through pgx, ALTER COLUMN TYPE
// from text.
func TestEnums(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// Leased gateways on two nodes: ADD VALUE's drain waits for the
	// other to adopt the refreshed descriptor.
	s := leasedSession(t, tc, 0, 2*time.Second)
	waitForDatabases(t, ctx, s)
	other := leasedSession(t, tc, 1, 2*time.Second)

	text := func(r *sql.Result) string {
		var b strings.Builder
		for _, row := range r.Rows {
			for i, d := range row {
				if i > 0 {
					b.WriteByte('|')
				}
				b.WriteString(d.Text())
			}
			b.WriteByte('\n')
		}
		return b.String()
	}
	one := func(q string) string {
		t.Helper()
		return strings.TrimSuffix(text(execSQL(t, ctx, s, q)), "\n")
	}
	refused := func(stmt, code string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, stmt); serr == nil || serr.Code != code {
			t.Fatalf("%s: %v, want %s", stmt, serr, code)
		}
	}

	execSQL(t, ctx, s, `CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`)
	execSQL(t, ctx, s, `CREATE TYPE IF NOT EXISTS mood AS ENUM ('x')`)
	refused(`CREATE TYPE mood AS ENUM ('x')`, sql.CodeDuplicateObject)
	refused(`CREATE TYPE int8 AS ENUM ('x')`, sql.CodeDuplicateObject)
	refused(`CREATE TABLE bad (m nosuch)`, sql.CodeUndefinedObject)
	execSQL(t, ctx, s, `CREATE TYPE size AS ENUM ('s', 'm', 'l')`)
	execSQL(t, ctx, s, `CREATE TABLE p (id INT8 PRIMARY KEY, m mood NOT NULL, sz size DEFAULT 'm')`)
	execSQL(t, ctx, s, `CREATE INDEX p_m ON p (m)`)
	execSQL(t, ctx, s, `INSERT INTO p (id, m) VALUES (1, 'happy'), (2, 'sad'), (3, 'ok')`)
	execSQL(t, ctx, s, `INSERT INTO p VALUES (4, 'happy'::mood, 'l')`)
	execSQL(t, ctx, s, `INSERT INTO p (id, m, sz) VALUES ($1, $2, $3)`, sqlString("5"), sqlString("sad"), sqlString("s"))
	refused(`INSERT INTO p (id, m) VALUES (9, 'angry')`, sql.CodeInvalidTextRepresentation)
	refused(`INSERT INTO p (id, m, sz) VALUES (9, 'ok', 'xl')`, sql.CodeInvalidTextRepresentation)
	refused(`UPDATE p SET m = 'furious' WHERE id = 1`, sql.CodeInvalidTextRepresentation)
	refused(`SELECT 'nope'::mood`, sql.CodeInvalidTextRepresentation)

	for _, c := range []struct{ q, want string }{
		{`SELECT id, m, sz FROM p ORDER BY id`, "1|happy|m\n2|sad|m\n3|ok|m\n4|happy|l\n5|sad|s"},
		{`SELECT id FROM p ORDER BY m, id`, "2\n5\n3\n1\n4"},
		{`SELECT id FROM p ORDER BY m DESC, id`, "1\n4\n3\n2\n5"},
		{`SELECT id FROM p WHERE m = 'happy' ORDER BY id`, "1\n4"},
		{`SELECT id FROM p WHERE m > 'ok'::mood ORDER BY id`, "1\n4"},
		{`SELECT id FROM p WHERE m IN ('sad', 'ok') ORDER BY id`, "2\n3\n5"},
		{`SELECT id FROM p WHERE m = $1 ORDER BY id`, ""},
		{`SELECT m, count(*) FROM p GROUP BY m ORDER BY m`, "sad|2\nok|1\nhappy|2"},
		{`SELECT min(m), max(m) FROM p`, "sad|happy"},
		{`SELECT 'ok'::mood, 'ok'::mood::text, m::text || '!' FROM p WHERE id = 3`, "ok|ok|ok!"},
		{`SELECT m = 'ok', m::text = 'ok', length(m::text) FROM p WHERE id = 3`, "t|t|2"},
		{`EXPLAIN SELECT id FROM p WHERE m = 'ok'`, ""},
	} {
		if strings.Contains(c.q, "$1") {
			if r := execSQL(t, ctx, s, c.q, sqlString("ok")); text(r) != "3\n" {
				t.Errorf("%s with 'ok': %q", c.q, text(r))
			}
			continue
		}
		got := one(c.q)
		switch {
		case strings.HasPrefix(c.q, "EXPLAIN"):
			if !strings.Contains(got, "p_m") {
				t.Errorf("%s: plan should use the enum index: %s", c.q, got)
			}
		case got != c.want:
			t.Errorf("%s: %q, want %q", c.q, got, c.want)
		}
	}

	// ADD VALUE: the label is usable at once, on this gateway and on
	// another one holding a lease, and sorts last.
	if _, serr := trySQL(ctx, other, `SELECT count(*) FROM p`); serr != nil {
		t.Fatal(serr)
	}
	execSQL(t, ctx, s, `ALTER TYPE mood ADD VALUE 'ecstatic'`)
	execSQL(t, ctx, s, `ALTER TYPE mood ADD VALUE IF NOT EXISTS 'ecstatic'`)
	refused(`ALTER TYPE mood ADD VALUE 'ecstatic'`, sql.CodeDuplicateObject)
	execSQL(t, ctx, s, `INSERT INTO p (id, m) VALUES (6, 'ecstatic')`)
	execSQL(t, ctx, other, `INSERT INTO p (id, m) VALUES (7, 'ecstatic')`)
	if got := text(execSQL(t, ctx, other, `SELECT id, m FROM p WHERE id >= 6 ORDER BY m, id`)); got != "6|ecstatic\n7|ecstatic\n" {
		t.Fatalf("after ADD VALUE on the other gateway: %q", got)
	}
	if got := one(`SELECT id FROM p ORDER BY m DESC, id LIMIT 2`); got != "6\n7" {
		t.Fatalf("new label sorts last: %q", got)
	}

	// Catalogs.
	create := execSQL(t, ctx, s, `SHOW CREATE TABLE p`).Rows[0][1].S
	if !strings.Contains(create, "m mood NOT NULL") || !strings.Contains(create, "sz size DEFAULT 'm'") {
		t.Fatalf("SHOW CREATE TABLE:\n%s", create)
	}
	if got := one(`SELECT typname, typtype, typcategory FROM pg_type WHERE typtype = 'e' ORDER BY typname`); got != "mood|e|E\nsize|e|E" {
		t.Fatalf("pg_type: %q", got)
	}
	if got := one(`SELECT e.enumlabel, e.enumsortorder FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid WHERE t.typname = 'mood' ORDER BY e.enumsortorder`); got != "sad|1\nok|2\nhappy|3\necstatic|4" {
		t.Fatalf("pg_enum: %q", got)
	}
	if got := one(`SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_name = 'p' AND column_name IN ('m', 'sz') ORDER BY ordinal_position`); got != "m|USER-DEFINED|mood\nsz|USER-DEFINED|size" {
		t.Fatalf("information_schema.columns: %q", got)
	}
	if got := one(`SELECT attname, format_type(atttypid, atttypmod), atttypid = (SELECT oid FROM pg_type WHERE typname = 'mood') FROM pg_attribute WHERE attrelid = 'p'::regclass AND attname = 'm'`); got != "m|mood|t" {
		t.Fatalf("pg_attribute: %q", got)
	}

	// DROP TYPE refuses while a column uses it; then the column goes.
	refused(`DROP TYPE mood`, sql.CodeDependentObjectsExist)
	refused(`DROP TYPE nosuch`, sql.CodeUndefinedObject)
	execSQL(t, ctx, s, `DROP TYPE IF EXISTS nosuch`)
	execSQL(t, ctx, s, `ALTER TABLE p DROP COLUMN sz`)
	execSQL(t, ctx, s, `DROP TYPE size`)
	refused(`CREATE TABLE q (s size)`, sql.CodeUndefinedObject)

	// ALTER COLUMN TYPE: text to the enum (every value must be a label),
	// and the enum back to text.
	execSQL(t, ctx, s, `CREATE TABLE txt (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO txt VALUES (1, 'ok'), (2, 'happy'), (3, 'meh')`)
	refused(`ALTER TABLE txt ALTER COLUMN v TYPE mood`, sql.CodeInvalidTextRepresentation)
	execSQL(t, ctx, s, `DELETE FROM txt WHERE id = 3`)
	execSQL(t, ctx, s, `ALTER TABLE txt ALTER COLUMN v TYPE mood`)
	if got := one(`SELECT id, v FROM txt ORDER BY v`); got != "1|ok\n2|happy" {
		t.Fatalf("after ALTER COLUMN TYPE mood: %q", got)
	}
	if c := execSQL(t, ctx, s, `SHOW CREATE TABLE txt`).Rows[0][1].S; !strings.Contains(c, "v mood") {
		t.Fatalf("SHOW CREATE TABLE after retype:\n%s", c)
	}
	execSQL(t, ctx, s, `ALTER TABLE txt ALTER COLUMN v TYPE TEXT`)
	execSQL(t, ctx, s, `INSERT INTO txt VALUES (3, 'meh')`)
	execSQL(t, ctx, s, `CREATE TABLE p2 (LIKE p)`)
	if c := execSQL(t, ctx, s, `SHOW CREATE TABLE p2`).Rows[0][1].S; !strings.Contains(c, "m mood NOT NULL") {
		t.Fatalf("LIKE:\n%s", c)
	}
	execSQL(t, ctx, s, `CREATE TABLE p3 AS SELECT id, m FROM p`)
	if c := execSQL(t, ctx, s, `SHOW CREATE TABLE p3`).Rows[0][1].S; !strings.Contains(c, "m mood") {
		t.Fatalf("CREATE TABLE AS:\n%s", c)
	}

	// The wire: the column describes with the type's OID (past the
	// builtin range), values travel as labels in both formats, and a
	// text parameter binds.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	sd, err := conn.Prepare(ctx, "d", `SELECT m, m::text FROM p`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if sd.Fields[0].DataTypeOID < 16384 || sd.Fields[1].DataTypeOID != 25 {
		t.Fatalf("Describe: %v", sd.Fields)
	}
	var label string
	if err := conn.QueryRow(ctx, `SELECT m FROM p WHERE id = 1`).Scan(&label); err != nil || label != "happy" {
		t.Fatalf("scan: %q %v", label, err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO p (id, m) VALUES ($1, $2)`, int64(20), "ok"); err != nil {
		t.Fatalf("text param: %v", err)
	}
	var n int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM p WHERE m = $1`, "ok").Scan(&n); err != nil || n != 2 {
		t.Fatalf("param comparison: %d %v", n, err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO p (id, m) VALUES ($1, $2)`, int64(21), "bogus"); err == nil || !strings.Contains(err.Error(), "invalid input value for enum mood") {
		t.Fatalf("bad label over the wire: %v", err)
	}
	execSQL(t, ctx, s, `DROP TABLE p`)
	execSQL(t, ctx, s, `DROP TABLE p2`)
	execSQL(t, ctx, s, `DROP TABLE p3`)
	execSQL(t, ctx, s, `DROP TYPE mood`)
}

func sqlString(s string) types.Datum { return types.NewString(s) }

// TestEnumsNeedV10: CREATE TYPE is refused until the cluster version
// is finalized.
func TestEnumsNeedV10(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V9 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	if _, serr := trySQL(ctx, s, `CREATE TYPE mood AS ENUM ('a')`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("CREATE TYPE at v9: %v, want 0A000", serr)
	}
}
