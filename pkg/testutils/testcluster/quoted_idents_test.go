package testcluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestQuotedPrismaSchema (issue #367): a Prisma-style schema — quoted
// mixed-case enum, table, column, index and constraint names — end to
// end over pgx: CREATE TYPE "Role", a column of type "Role" (which
// used to fold to role: 42704), inserts, casts in every spelling
// (::"Role", CAST($1::text AS "public"."Role")), ALTER TYPE ... ADD
// VALUE, ALTER COLUMN ... TYPE "Role", COPY FROM into the enum column,
// and the catalogs (pg_type, format_type, regtype, \d's queries),
// which print the names quoted the way PostgreSQL does.
func TestQuotedPrismaSchema(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	one := func(q string, args ...any) string {
		t.Helper()
		rows, err := conn.Query(ctx, q, args...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var out []string
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			var cells []string
			for _, v := range vals {
				if v == nil {
					cells = append(cells, "NULL")
					continue
				}
				cells = append(cells, strings.TrimSpace(strings.ReplaceAll(toText(v), "\n", " ")))
			}
			out = append(out, strings.Join(cells, "|"))
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return strings.Join(out, "\n")
	}
	refused := func(q, code string) {
		t.Helper()
		_, err := conn.Exec(ctx, q)
		var pe *pgconn.PgError
		if !asPgError(err, &pe) || pe.Code != code {
			t.Fatalf("%s: %v, want %s", q, err, code)
		}
	}

	// The migration Prisma writes for an enum and a model.
	exec(`CREATE TYPE "Role" AS ENUM ('USER', 'ADMIN')`)
	exec(`CREATE TYPE "Status" AS ENUM ('ACTIVE', 'BANNED')`)
	exec(`CREATE TABLE "User" (
		"id" SERIAL NOT NULL,
		"email" TEXT NOT NULL,
		"role" "Role" NOT NULL DEFAULT 'USER',
		"status" "public"."Status",
		"createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT "User_pkey" PRIMARY KEY ("id")
	)`)
	exec(`CREATE UNIQUE INDEX "User_email_key" ON "User"("email")`)
	// The unquoted spelling folds to role, which is no type.
	refused(`CREATE TABLE bad (r Role)`, "42704")
	refused(`CREATE TABLE bad (r "role")`, "42704")

	// Prisma's inserts: the enum value as a cast of a text parameter.
	exec(`INSERT INTO "public"."User" ("email","role") VALUES ($1,CAST($2::text AS "public"."Role"))`, "a@x", "ADMIN")
	exec(`INSERT INTO "User" ("email") VALUES ('b@x')`)
	exec(`INSERT INTO "User" ("email", "role", "status") VALUES ('c@x', 'ADMIN'::"Role", CAST('BANNED' AS "Status"))`)
	refused(`INSERT INTO "User" ("email", "role") VALUES ('d@x', 'admin')`, "22P02")
	refused(`SELECT 'USER'::"role"`, "42704")
	refused(`SELECT 'nope'::"Role"`, "22P02")

	for _, c := range []struct{ q, want string }{
		{`SELECT "id", "email", "role", "status" FROM "User" ORDER BY "id"`, "1|a@x|ADMIN|NULL\n2|b@x|USER|NULL\n3|c@x|ADMIN|BANNED"},
		{`SELECT "public"."User"."email" FROM "public"."User" WHERE "public"."User"."role" = CAST($1::text AS "public"."Role") ORDER BY "id"`, "a@x\nc@x"},
		{`SELECT "email" FROM "User" WHERE "role" = 'USER'::"Role"`, "b@x"},
		{`SELECT "email" FROM "User" WHERE "role" IN (CAST('ADMIN' AS "Role")) ORDER BY 1`, "a@x\nc@x"},
		{`SELECT 'ADMIN'::"Role"::text`, "ADMIN"},
		{`SELECT "role"::text FROM "User" WHERE "id" = 2`, "USER"},
	} {
		args := []any{}
		if strings.Contains(c.q, "$1") {
			args = append(args, "ADMIN")
		}
		if got := one(c.q, args...); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.q, got, c.want)
		}
	}

	// ALTER TYPE ... ADD VALUE, ALTER COLUMN ... TYPE "Role".
	exec(`ALTER TYPE "Role" ADD VALUE 'MODERATOR'`)
	exec(`ALTER TYPE "public"."Role" ADD VALUE IF NOT EXISTS 'MODERATOR'`)
	exec(`UPDATE "User" SET "role" = 'MODERATOR' WHERE "id" = 2`)
	exec(`CREATE TABLE "Post" ("id" INT8 PRIMARY KEY, "kind" TEXT)`)
	exec(`INSERT INTO "Post" VALUES (1, 'USER'), (2, 'ADMIN')`)
	exec(`ALTER TABLE "Post" ALTER COLUMN "kind" TYPE "Role"`)
	if got := one(`SELECT "kind" FROM "Post" WHERE "kind" > 'USER' ORDER BY "id"`); got != "ADMIN" {
		t.Errorf("after ALTER COLUMN TYPE: %q", got)
	}

	// COPY FROM into the enum column, text and binary (pgx CopyFrom).
	n, err := conn.CopyFrom(ctx, pgx.Identifier{"User"}, []string{"email", "role"}, pgx.CopyFromRows([][]any{{"e@x", "MODERATOR"}, {"f@x", "USER"}}))
	if err != nil || n != 2 {
		t.Fatalf("CopyFrom: %d %v", n, err)
	}
	if _, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("g@x\tADMIN\n"), `COPY "User" ("email", "role") FROM STDIN`); err != nil {
		t.Fatalf("COPY FROM STDIN: %v", err)
	}
	if _, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("h@x\tadmin\n"), `COPY "User" ("email", "role") FROM STDIN`); err == nil {
		t.Fatalf("COPY FROM with an unknown label: no error")
	}
	if got := one(`SELECT "email", "role" FROM "User" WHERE "id" > 3 ORDER BY "id"`); got != "e@x|MODERATOR\nf@x|USER\ng@x|ADMIN" {
		t.Errorf("after COPY: %q", got)
	}

	// The catalogs: pg_type keeps the name as is; format_type, regtype
	// and \d quote it as PostgreSQL does.
	for _, c := range []struct{ q, want string }{
		{`SELECT typname, typtype FROM pg_type WHERE typname IN ('Role', 'Status') ORDER BY 1`, "Role|e\nStatus|e"},
		{`SELECT count(*) FROM pg_type WHERE typname = 'role'`, "0"},
		{`SELECT format_type(oid, NULL) FROM pg_type WHERE typname = 'Role'`, `"Role"`},
		{`SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a WHERE a.attrelid = '"User"'::regclass AND a.attname = 'role'`, `"Role"`},
		{`SELECT '"Role"'::regtype::oid = (SELECT oid FROM pg_type WHERE typname = 'Role')`, "true"},
		{`SELECT to_regtype('"Role"') IS NOT NULL, to_regtype('Role') IS NULL`, "true|true"},
		{`SELECT string_agg(enumlabel, ',' ORDER BY enumsortorder) FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid WHERE t.typname = 'Role'`, "USER,ADMIN,MODERATOR"},
		{`SELECT udt_name, data_type FROM information_schema.columns WHERE table_name = 'User' AND column_name = 'role'`, "Role|USER-DEFINED"},
		{`SELECT indexdef FROM pg_indexes WHERE indexname = 'User_email_key'`, `CREATE UNIQUE INDEX "User_email_key" ON public."User" USING btree (email)`},
		{`SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c WHERE c.conname = 'User_pkey'`, `PRIMARY KEY (id)`},
	} {
		if got := one(c.q); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.q, got, c.want)
		}
	}

	// A quoted mixed-case name is never the builtin its folded spelling
	// names: "Date" is an enum beside date.
	exec(`CREATE TYPE "Date" AS ENUM ('today', 'tomorrow')`)
	exec(`CREATE TABLE "Plan" ("id" INT8 PRIMARY KEY, "when" "Date" DEFAULT 'tomorrow'::"Date", "on" date)`)
	exec(`INSERT INTO "Plan" ("id", "on") VALUES (1, '2024-01-02')`)
	exec(`INSERT INTO "Plan" VALUES (2, 'today', '2024-01-03')`)
	if got := one(`SELECT "when", "when"::"Date"::text, "on"::text, 'today'::"Date" < 'tomorrow'::"Date" FROM "Plan" ORDER BY "id"`); got != "tomorrow|tomorrow|2024-01-02|true\ntoday|today|2024-01-03|true" {
		t.Errorf(`"Date": %q`, got)
	}
	if got := one(`SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d WHERE d.adrelid = '"Plan"'::regclass`); got != `'tomorrow'::"Date"` {
		t.Errorf(`"Date" default: %q`, got)
	}

	// DROP TYPE: refused while a column uses it, then dropped.
	refused(`DROP TYPE "Role"`, "2BP01")
	refused(`DROP TYPE "role"`, "42704")
	exec(`DROP TABLE "Post"`)
	exec(`DROP TABLE "User"`)
	exec(`DROP TYPE "Role"`)
	exec(`DROP TYPE IF EXISTS "Role"`)
	if got := one(`SELECT count(*) FROM pg_type WHERE typname = 'Role'`); got != "0" {
		t.Errorf("after DROP TYPE: %q", got)
	}
}

// toText renders a pgx result value as text for comparison.
func toText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case fmt.Stringer:
		return x.String()
	}
	return fmt.Sprint(v)
}

func asPgError(err error, pe **pgconn.PgError) bool { return errors.As(err, pe) }

// TestQuotedIdentifiers (issue #367): "" in a quoted identifier is one
// " for every object — table, column, index, constraint, role, type,
// sequence — and the output paths quote such a name back as PostgreSQL
// does (quote_ident, format('%I'), pg_get_indexdef,
// pg_get_constraintdef, pg_get_expr, format_type, SHOW CREATE TABLE);
// quoted keywords are identifiers ("select", "order", "user",
// "concurrently"); E” strings take every escape PostgreSQL's do, with
// its errors.
func TestQuotedIdentifiers(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	one := func(q string) string {
		t.Helper()
		var rows []string
		for _, row := range execSQL(t, ctx, s, q).Rows {
			var cells []string
			for _, d := range row {
				cells = append(cells, d.Text())
			}
			rows = append(rows, strings.Join(cells, "|"))
		}
		return strings.Join(rows, "\n")
	}
	refused := func(q, code, msg string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != code || msg != "" && serr.Msg != msg {
			t.Fatalf("%s: %v, want %s %s", q, serr, code, msg)
		}
	}

	execSQL(t, ctx, s, `CREATE TYPE "k""t" AS ENUM ('p', 'q')`)
	execSQL(t, ctx, s, `CREATE TABLE "a""b" (
		"c""d" SERIAL PRIMARY KEY,
		"select" TEXT, "order" INT8, "user" TEXT, "concurrently" INT8, "Mixed" TEXT, "time" "k""t",
		CONSTRAINT "ck""1" CHECK ("order" > 0))`)
	execSQL(t, ctx, s, `CREATE INDEX "i""x" ON "a""b" ("select")`)
	execSQL(t, ctx, s, `CREATE INDEX "concurrently" ON "a""b" ("order")`)
	execSQL(t, ctx, s, `CREATE UNIQUE INDEX "user" ON "a""b" ("user")`)
	// An expression key over a mixed-case column: stored as text that
	// reads back as the same column.
	execSQL(t, ctx, s, `CREATE INDEX "lx" ON "a""b" (lower("Mixed"))`)
	execSQL(t, ctx, s, `INSERT INTO "a""b" ("select", "order", "user", "concurrently", "Mixed", "time") VALUES ('s', 2, 'u', 3, 'M', 'q'::"k""t")`)
	refused(`INSERT INTO "a""b" ("order") VALUES (0)`, sql.CodeCheckViolation, "")
	execSQL(t, ctx, s, `CREATE ROLE "r""1"`)
	execSQL(t, ctx, s, `GRANT SELECT ON "a""b" TO "r""1"`)

	for _, c := range []struct{ q, want string }{
		{`SELECT "c""d", "select", "order", "user", "concurrently", "Mixed", "time" FROM "a""b" AS "from" WHERE "from"."order" = 2 ORDER BY "order"`, "1|s|2|u|3|M|q"},
		{`SELECT relname FROM pg_class WHERE relname IN ('a"b', 'i"x', 'concurrently', 'user', 'a"b_c"d_seq') ORDER BY 1`, "a\"b\na\"b_c\"d_seq\nconcurrently\ni\"x\nuser"},
		{`SELECT attname FROM pg_attribute WHERE attrelid = '"a""b"'::regclass AND attnum > 0 ORDER BY attnum`, "c\"d\nselect\norder\nuser\nconcurrently\nMixed\ntime"},
		{`SELECT typname, format_type(oid, NULL) FROM pg_type WHERE typname = 'k"t'`, `k"t|"k""t"`},
		{`SELECT rolname FROM pg_roles WHERE rolname = 'r"1'`, `r"1`},
		{`SELECT indexdef FROM pg_indexes WHERE tablename = 'a"b' ORDER BY 1`, strings.Join([]string{
			`CREATE INDEX "concurrently" ON public."a""b" USING btree ("order")`,
			`CREATE INDEX "i""x" ON public."a""b" USING btree ("select")`,
			`CREATE INDEX lx ON public."a""b" USING btree (lower("Mixed"))`,
			`CREATE UNIQUE INDEX "a""b_pkey" ON public."a""b" USING btree ("c""d")`,
			`CREATE UNIQUE INDEX "user" ON public."a""b" USING btree ("user")`}, "\n")},
		{`SELECT "c""d" FROM "a""b" WHERE lower("Mixed") = 'm'`, "1"},
		{`SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = '"a""b"'::regclass ORDER BY 1`,
			"a\"b_pkey|PRIMARY KEY (\"c\"\"d\")\nck\"1|CHECK (\"order\" > 0)\nuser|UNIQUE (\"user\")"},
		{`SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d WHERE d.adrelid = '"a""b"'::regclass`, `nextval('"a""b_c""d_seq"')`},
		// psql's \d column query.
		{`SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a WHERE a.attrelid = '"a""b"'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`,
			"c\"d|integer\nselect|text\norder|bigint\nuser|text\nconcurrently|bigint\nMixed|text\ntime|\"k\"\"t\""},
		{`SELECT quote_ident('a"b'), quote_ident('select'), quote_ident('Mixed'), quote_ident('time'), quote_ident('ok'), format('%I.%I', 'a"b', 'user')`,
			`"a""b"|"select"|"Mixed"|"time"|ok|"a""b"."user"`},
		{`SELECT relacl FROM pg_class WHERE relname = 'a"b'`, `{root=arwdDxt/root,"\"r\"\"1\"=r/root"}`},
	} {
		if got := one(c.q); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.q, got, c.want)
		}
	}
	if got := one(`SHOW CREATE TABLE "a""b"`); !strings.Contains(got, `CREATE TABLE "a""b" (
  "c""d" INT4 NOT NULL DEFAULT nextval('"a""b_c""d_seq"'),
  "select" TEXT,
  "order" INT8,
  "user" TEXT,
  "concurrently" INT8,
  "Mixed" TEXT,
  "time" "k""t",
  PRIMARY KEY ("c""d"),
  INDEX "i""x" ("select"),
  INDEX "concurrently" ("order"),
  UNIQUE INDEX "user" ("user"),
  INDEX lx (lower("Mixed")),
  CONSTRAINT "ck""1" CHECK ("order" > 0)
)`) {
		t.Errorf("SHOW CREATE TABLE:\n%s", got)
	}

	// Renames, drops and the role, by their quoted names.
	execSQL(t, ctx, s, `ALTER TABLE "a""b" RENAME COLUMN "order" TO "o""rd"`)
	if got := one(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'ck"1'`); got != `CHECK ("o""rd" > 0)` {
		t.Errorf("CHECK after RENAME COLUMN: %q", got)
	}
	execSQL(t, ctx, s, `ALTER INDEX "i""x" RENAME TO "j""y"`)
	execSQL(t, ctx, s, `DROP INDEX "concurrently"`)
	execSQL(t, ctx, s, `ALTER TABLE "a""b" DROP CONSTRAINT "ck""1"`)
	execSQL(t, ctx, s, `ALTER TABLE "a""b" RENAME TO "e""f"`)
	if got := one(`SELECT "o""rd", nextval('"a""b_c""d_seq"') FROM "e""f"`); got != "2|3" { // the refused insert took 2
		t.Errorf("after the renames: %q", got)
	}
	if got := one(`SELECT relname FROM pg_class WHERE relname IN ('j"y', 'concurrently', 'e"f') ORDER BY 1`); got != "e\"f\nj\"y" {
		t.Errorf("relations after the renames: %q", got)
	}
	execSQL(t, ctx, s, `REVOKE SELECT ON "e""f" FROM "r""1"`)
	execSQL(t, ctx, s, `DROP ROLE "r""1"`)
	execSQL(t, ctx, s, `DROP TABLE "e""f"`)
	execSQL(t, ctx, s, `DROP TYPE "k""t"`)

	// Identifier errors.
	refused(`SELECT ""`, sql.CodeSyntaxError, `zero-length delimited identifier at or near """"`)
	refused(`SELECT "abc`, sql.CodeSyntaxError, `unterminated quoted identifier at or near ""abc"`)

	// E'' escapes, and PostgreSQL's errors for the invalid ones.
	if got := one(`SELECT E'\b\f\n\r\t' = chr(8) || chr(12) || chr(10) || chr(13) || chr(9), E'\x41\x4a\x7' = 'AJ' || chr(7), E'\101\60', E'\u00e9\U0001F600', E'\xC3\xA9', E'\q\\', E'\xZ', E'\uD83D\uDE00'`); got != `t|t|A0|é😀|é|q\|xZ|😀` {
		t.Errorf("E'' escapes: %q", got)
	}
	refused(`SELECT E'\0'`, "22021", `invalid byte sequence for encoding "UTF8": 0x00`)
	refused(`SELECT E'\x80'`, "22021", `invalid byte sequence for encoding "UTF8": 0x80`)
	refused(`SELECT E'\u12'`, "22025", `invalid Unicode escape`)
	refused(`SELECT E'\u0000'`, sql.CodeSyntaxError, `invalid Unicode escape value`)
	refused(`SELECT E'\uD83D'`, sql.CodeSyntaxError, `invalid Unicode surrogate pair`)
}
