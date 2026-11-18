package testcluster

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
)

// alterHarness is a session with the small helpers these tests share.
type alterHarness struct {
	t   *testing.T
	ctx context.Context
	s   *sql.Session
}

func newAlterHarness(t *testing.T, ctx context.Context, s *sql.Session) *alterHarness {
	return &alterHarness{t: t, ctx: ctx, s: s}
}

func (h *alterHarness) exec(q string) *sql.Result {
	h.t.Helper()
	return execSQL(h.t, h.ctx, h.s, q)
}

func (h *alterHarness) text(q string) string {
	h.t.Helper()
	r := h.exec(q)
	var out []string
	for _, row := range r.Rows {
		var cells []string
		for _, d := range row {
			if d.Null {
				cells = append(cells, "NULL")
			} else {
				cells = append(cells, d.Text())
			}
		}
		out = append(out, strings.Join(cells, "|"))
	}
	return strings.Join(out, ";")
}

func (h *alterHarness) expect(q, want string) {
	h.t.Helper()
	if got := h.text(q); got != want {
		h.t.Fatalf("%s:\n got %q\nwant %q", q, got, want)
	}
}

// fails runs q and wants SQLSTATE code with a message containing msg.
func (h *alterHarness) fails(q, code, msg string) {
	h.t.Helper()
	_, serr := trySQL(h.ctx, h.s, q)
	if serr == nil {
		h.t.Fatalf("%s: succeeded, want %s", q, code)
	}
	if serr.Code != code || !strings.Contains(serr.Msg, msg) {
		h.t.Fatalf("%s: got [%s] %s, want [%s] containing %q", q, serr.Code, serr.Msg, code, msg)
	}
}

func (h *alterHarness) fill(table string, n int, row func(i int) string) {
	h.t.Helper()
	var vals []string
	for i := 1; i <= n; i++ {
		vals = append(vals, row(i))
		if len(vals) == 100 || i == n {
			h.exec(fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(vals, ", ")))
			vals = nil
		}
	}
}

// TestAlterTableMultiAction covers ALTER TABLE with several actions
// (issue #364): the actions apply in order as one change — one
// descriptor version when they only change the descriptor, inside a
// transaction block too — and a failing action leaves nothing applied,
// whether it fails before anything is published or once the rows are
// read. ONLY, IF EXISTS and the refused combinations too.
func TestAlterTableMultiAction(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	h := newAlterHarness(t, ctx, s)
	version := func(table string) uint64 {
		return lookupDescriptor(t, ctx, tc.Nodes[0].DB(), table).Version
	}

	h.exec(`CREATE TABLE t (id INT8 PRIMARY KEY, a INT8, b TEXT, c INT8 DEFAULT 5, d INT8 NOT NULL DEFAULT 0)`)
	h.exec(`CREATE TABLE p (id INT8 PRIMARY KEY)`)
	h.exec(`INSERT INTO p VALUES (1), (2), (3)`)
	h.fill("t", 150, func(i int) string { return fmt.Sprintf("(%d, %d, 'b%d', %d, %d)", i, i, i, i, i) })

	// Descriptor-only actions: one version, in order — the column added
	// first gets the default set by a later action.
	v := version("t")
	h.exec(`ALTER TABLE t ADD COLUMN e INT8, ADD COLUMN f INT8 DEFAULT 7, DROP COLUMN c, ALTER COLUMN a SET DEFAULT 1, ALTER COLUMN e SET DEFAULT 9, ALTER COLUMN d DROP NOT NULL`)
	if got := version("t"); got != v+1 {
		t.Fatalf("descriptor version %d -> %d, want one step", v, got)
	}
	h.expect(`SELECT count(*), count(e), sum(f) FROM t`, "150|0|1050")
	h.expect(h.columnsQuery("t"), "id bigint;a bigint;b text;d bigint;e bigint;f bigint")
	h.exec(`INSERT INTO t (id, d) VALUES (1001, NULL)`)
	h.expect(`SELECT a, e, f, d FROM t WHERE id = 1001`, "1|9|7|NULL")

	// The same inside a transaction block, rolled back and committed.
	h.exec(`BEGIN`)
	h.exec(`ALTER TABLE t ADD COLUMN g TEXT DEFAULT 'x', DROP COLUMN e`)
	h.expect(`SELECT count(g) FROM t`, "151")
	h.exec(`ROLLBACK`)
	h.expect(h.columnsQuery("t"), "id bigint;a bigint;b text;d bigint;e bigint;f bigint")
	h.exec(`BEGIN; ALTER TABLE t ADD COLUMN g TEXT DEFAULT 'x', DROP COLUMN e; COMMIT`)
	h.expect(h.columnsQuery("t"), "id bigint;a bigint;b text;d bigint;f bigint;g text")
	// An action that reads the rows cannot run in a block: refused,
	// naming it, and the block's other work is not applied either.
	h.exec(`BEGIN`)
	h.fails(`ALTER TABLE t ADD COLUMN h INT8, ADD CONSTRAINT a_pos CHECK (a > 0)`, "25001", "ADD CONSTRAINT a_pos")
	h.exec(`ROLLBACK`)

	// A failure before anything is published: nothing applied, wherever
	// the bad action is.
	v = version("t")
	h.fails(`ALTER TABLE t ADD COLUMN h INT8, DROP COLUMN g, DROP COLUMN nope`, "42703", "nope")
	h.fails(`ALTER TABLE t DROP COLUMN g, ADD COLUMN h INT8, ADD CONSTRAINT h_pos CHECK (h > 0), ALTER COLUMN nope SET NOT NULL`, "42703", "nope")
	h.fails(`ALTER TABLE t ADD COLUMN h INT8, ADD COLUMN h TEXT`, "42701", "h")
	if got := version("t"); got != v {
		t.Fatalf("a failed statement changed the descriptor: version %d -> %d", v, got)
	}
	h.expect(h.columnsQuery("t"), "id bigint;a bigint;b text;d bigint;f bigint;g text")

	// A failure once the rows are read: the validation, the uniqueness
	// check, the NOT NULL check and the conversion each undo the whole
	// statement, the actions that waited (DROP COLUMN g) included.
	for _, c := range []struct{ q, code, msg string }{
		{`ALTER TABLE t DROP COLUMN g, ADD COLUMN h INT8 DEFAULT 1, ADD CONSTRAINT a_neg CHECK (a < 0)`, "23514", "a_neg"},
		{`ALTER TABLE t DROP COLUMN g, ADD COLUMN h INT8 DEFAULT 1 UNIQUE`, "23505", "t_h_key"},
		{`ALTER TABLE t DROP COLUMN g, ADD COLUMN h INT8, ALTER COLUMN h SET NOT NULL`, "23502", `column "h" of relation "t" contains null values`},
		{`ALTER TABLE t DROP COLUMN g, ALTER COLUMN d SET NOT NULL`, "23502", `column "d" of relation "t" contains null values`},
		{`ALTER TABLE t DROP COLUMN g, ALTER COLUMN b TYPE INT8`, "22P02", "b"},
		{`ALTER TABLE t DROP COLUMN g, ADD COLUMN h INT8, ADD CONSTRAINT h_p FOREIGN KEY (h) REFERENCES p (id), ALTER COLUMN h SET DEFAULT 99, ADD COLUMN k INT8 DEFAULT 99 REFERENCES p (id)`, "23503", ""},
	} {
		h.fails(c.q, c.code, c.msg)
		h.expect(h.columnsQuery("t"), "id bigint;a bigint;b text;d bigint;f bigint;g text")
		h.expect(`SELECT count(*) FROM pg_constraint WHERE conrelid = 't'::regclass AND contype <> 'p'`, "0")
		h.expect(`SELECT count(*) FROM pg_indexes WHERE tablename = 't'`, "1")
		if d := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "t"); d.Alter != nil || len(d.Columns) != 6 {
			t.Fatalf("%s: left %d columns, pending %v", c.q, len(d.Columns), d.Alter)
		}
		if d := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "p"); len(d.InboundFKs) != 0 {
			t.Fatalf("%s: left a reference on p: %v", c.q, d.InboundFKs)
		}
	}
	h.exec(`UPDATE t SET d = 0 WHERE d IS NULL`)

	// The same statements succeed once the data allows them, and the
	// actions that read the rows publish once and flip once.
	v = version("t")
	h.exec(`ALTER TABLE t DROP COLUMN g, ADD COLUMN h INT8 DEFAULT 1, ADD CONSTRAINT a_pos CHECK (a > 0), ALTER COLUMN d SET NOT NULL, ADD COLUMN k INT8 DEFAULT 2 REFERENCES p (id), ADD CONSTRAINT h_k CHECK (h < k)`)
	if got := version("t"); got != v+2 {
		t.Fatalf("online statement: descriptor version %d -> %d, want publish and flip", v, got)
	}
	h.expect(h.columnsQuery("t"), "id bigint;a bigint;b text;d bigint;f bigint;h bigint;k bigint")
	h.expect(`SELECT count(*), sum(h), sum(k) FROM t`, "151|151|302")
	h.expect(`SELECT conname, contype, convalidated FROM pg_constraint WHERE conrelid = 't'::regclass AND contype <> 'p' ORDER BY conname`, "a_pos|c|t;h_k|c|t;t_k_fkey|f|t")
	h.fails(`INSERT INTO t (id, a, d, h, k) VALUES (2000, 1, 0, 5, 2)`, "23514", "h_k")
	h.fails(`INSERT INTO t (id, a, d, k) VALUES (2000, 1, 0, 9)`, "23503", "")
	h.fails(`INSERT INTO t (id, a, d) VALUES (2000, 1, NULL)`, "23502", "")

	// ONLY, IF EXISTS and t *.
	h.exec(`ALTER TABLE ONLY t ADD COLUMN m INT8, DROP COLUMN m`)
	h.exec(`ALTER TABLE IF EXISTS ONLY t ALTER COLUMN h SET DEFAULT 2, ALTER COLUMN k DROP DEFAULT`)
	h.exec(`ALTER TABLE t * ALTER COLUMN h DROP DEFAULT`)
	if r := h.exec(`ALTER TABLE IF EXISTS nosuch ADD COLUMN x INT8, ADD CONSTRAINT y CHECK (x > 0)`); len(r.Notices) != 1 || r.Notices[0] != `relation "nosuch" does not exist, skipping` {
		t.Fatalf("IF EXISTS on a missing table: %v", r.Notices)
	}
	h.exec(`ALTER TABLE IF EXISTS nosuch ADD COLUMN x INT8, DROP COLUMN y`)
	h.fails(`ALTER TABLE nosuch ADD COLUMN x INT8, DROP COLUMN y`, "42P01", "nosuch")
	h.exec(`ALTER TABLE t ADD COLUMN IF NOT EXISTS h INT8, DROP COLUMN IF EXISTS nope, ADD COLUMN IF NOT EXISTS n INT8 DEFAULT 4`)
	h.expect(`SELECT count(*), sum(n) FROM t`, "151|604")

	// The combinations that cannot be one change.
	for _, c := range []struct{ q, code, msg string }{
		{`ALTER TABLE t ADD COLUMN x INT8, SET (gc_ttl = '1h')`, "0A000", "SET (...) cannot be combined"},
		{`ALTER TABLE t ADD COLUMN x INT8, SPLIT AT VALUES (5)`, "0A000", "SPLIT AT cannot be combined"},
		{`ALTER TABLE t ADD COLUMN x INT8, RENAME COLUMN a TO z`, "42601", "RENAME cannot be combined"},
		{`ALTER TABLE t ADD COLUMN x INT8, OWNER TO root`, "42601", "OWNER cannot be combined"},
		{`ALTER TABLE t ALTER COLUMN b TYPE VARCHAR(10), ALTER COLUMN b TYPE TEXT`, "42601", `cannot alter type of column "b" twice`},
		{`ALTER TABLE t DROP CONSTRAINT a_pos, ADD CONSTRAINT a_pos CHECK (a > 1)`, "0A000", "ADD CONSTRAINT a_pos cannot be combined"},
		{`ALTER TABLE t ALTER COLUMN b TYPE VARCHAR(10), ADD CONSTRAINT b_short CHECK (b <> 'x')`, "0A000", "ADD CONSTRAINT b_short cannot be combined"},
		{`ALTER TABLE t ADD COLUMN x TEXT, ALTER COLUMN x TYPE INT8`, "0A000", "ALTER COLUMN x TYPE cannot be combined"},
		{`ALTER TABLE t DROP COLUMN n, ADD COLUMN n INT8 NOT NULL DEFAULT random()::INT8`, "0A000", "ADD COLUMN n cannot be combined"},
	} {
		v = version("t")
		h.fails(c.q, c.code, c.msg)
		if got := version("t"); got != v {
			t.Fatalf("%s: refused but changed the descriptor", c.q)
		}
	}
	h.expect(h.columnsQuery("t"), "id bigint;a bigint;b text;d bigint;f bigint;h bigint;k bigint;n bigint")
	h.expect(`SELECT count(*) FROM pg_constraint WHERE conrelid = 't'::regclass AND conname = 'a_pos'`, "1")

	// A retype with the defaults and NOT NULL of the same column after it
	// (Rails' change_column), and a constraint dropped beside it.
	h.fails(`ALTER TABLE t ALTER COLUMN b TYPE VARCHAR(20), ALTER COLUMN b SET DEFAULT 'none', ALTER COLUMN b SET NOT NULL, DROP CONSTRAINT h_k`, "23502", `column "b" of relation "t" contains null values`)
	h.expect(`SELECT data_type FROM information_schema.columns WHERE table_name = 't' AND column_name = 'b'`, "text")
	h.exec(`UPDATE t SET b = 'x' WHERE b IS NULL`)
	h.exec(`ALTER TABLE t ALTER COLUMN b TYPE VARCHAR(20), ALTER COLUMN b SET DEFAULT 'none', ALTER COLUMN b SET NOT NULL, DROP CONSTRAINT h_k`)
	h.expect(`SELECT column_name, data_type, character_maximum_length, is_nullable, column_default FROM information_schema.columns WHERE table_name = 't' AND column_name = 'b'`, "b|character varying(20)|20|NO|'none'")
	h.exec(`INSERT INTO t (id, a, d) VALUES (3000, 1, 0)`)
	h.expect(`SELECT b, h, k FROM t WHERE id = 3000`, "none|NULL|NULL")
	h.expect(`SELECT count(*) FROM pg_constraint WHERE conrelid = 't'::regclass AND conname = 'h_k'`, "0")
}

// columnsQuery lists the table's visible columns and types in order.
func (h *alterHarness) columnsQuery(table string) string {
	return fmt.Sprintf(`SELECT column_name || ' ' || data_type FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position`, table)
}

// alterKind is one kind of action for the combination matrix: setup
// prepares the table for it, action is its text, check sees its effect,
// and bad, when set, is a variant that fails (with code).
type alterKind struct {
	name      string
	setup     string
	action    string
	check     func(h *alterHarness, table string)
	bad, code string
	// rows: the action reads the rows (and cannot run in a block).
	rows bool
}

// TestAlterTableMultiActionMatrix runs every pair of action kinds in one
// statement, in both orders: each applies, and when one of them fails
// the other is not applied either.
func TestAlterTableMultiActionMatrix(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	h := newAlterHarness(t, ctx, s)
	h.exec(`CREATE TABLE ref (id INT8 PRIMARY KEY)`)
	h.exec(`INSERT INTO ref VALUES (1), (2)`)

	col := func(table, name string) string {
		return fmt.Sprintf(`SELECT data_type || ' ' || is_nullable || ' ' || coalesce(column_default, '-') FROM information_schema.columns WHERE table_name = '%s' AND column_name = '%s'`, table, name)
	}
	kinds := []alterKind{
		{name: "add-null", action: "ADD COLUMN an INT8", bad: "ADD COLUMN an INT8, ADD COLUMN an INT8", code: "42701",
			check: func(h *alterHarness, tb string) { h.expect(fmt.Sprintf(`SELECT count(an) FROM %s`, tb), "0") }},
		{name: "add-const", action: "ADD COLUMN ac INT8 NOT NULL DEFAULT 3",
			check: func(h *alterHarness, tb string) { h.expect(fmt.Sprintf(`SELECT sum(ac) FROM %s`, tb), "60") }},
		{name: "add-stable", action: "ADD COLUMN at TIMESTAMPTZ DEFAULT now()",
			check: func(h *alterHarness, tb string) {
				h.expect(fmt.Sprintf(`SELECT count(at), count(DISTINCT at) FROM %s`, tb), "20|1")
			}},
		{name: "add-volatile", action: "ADD COLUMN av UUID NOT NULL DEFAULT gen_random_uuid()", rows: true,
			bad: "ADD COLUMN av FLOAT8 NOT NULL DEFAULT nullif(0 * random(), 0)", code: "23502",
			check: func(h *alterHarness, tb string) {
				h.expect(fmt.Sprintf(`SELECT count(av), count(DISTINCT av) FROM %s`, tb), "20|20")
			}},
		{name: "add-unique", action: "ADD COLUMN au UUID DEFAULT gen_random_uuid() UNIQUE", rows: true,
			bad: "ADD COLUMN au INT8 DEFAULT 1 UNIQUE", code: "23505",
			check: func(h *alterHarness, tb string) {
				h.expect(fmt.Sprintf(`SELECT count(*) FROM pg_indexes WHERE tablename = '%s' AND indexname = '%s_au_key'`, tb, tb), "1")
				h.expect(fmt.Sprintf(`SELECT count(DISTINCT au) FROM %s`, tb), "20")
			}},
		{name: "drop-col", action: "DROP COLUMN dc", setup: "ALTER TABLE %s ADD COLUMN dc INT8", bad: "DROP COLUMN nope", code: "42703",
			check: func(h *alterHarness, tb string) { h.expect(col(tb, "dc"), "") }},
		{name: "set-default", action: "ALTER COLUMN a SET DEFAULT 42", bad: "ALTER COLUMN nope SET DEFAULT 1", code: "42703",
			check: func(h *alterHarness, tb string) { h.expect(col(tb, "a"), "bigint YES 42") }},
		{name: "drop-not-null", action: "ALTER COLUMN nn DROP NOT NULL", setup: "ALTER TABLE %s ADD COLUMN nn INT8 NOT NULL DEFAULT 0",
			check: func(h *alterHarness, tb string) { h.expect(col(tb, "nn"), "bigint YES 0") }},
		{name: "set-not-null", action: "ALTER COLUMN b SET NOT NULL", rows: true, bad: "ALTER COLUMN z SET NOT NULL", code: "23502",
			check: func(h *alterHarness, tb string) { h.expect(col(tb, "b"), "text NO -") }},
		{name: "check", action: "ADD CONSTRAINT a_pos CHECK (a > 0)", rows: true, bad: "ADD CONSTRAINT a_neg CHECK (a < 0)", code: "23514",
			check: func(h *alterHarness, tb string) {
				h.expect(fmt.Sprintf(`SELECT convalidated FROM pg_constraint WHERE conrelid = '%s'::regclass AND conname = 'a_pos'`, tb), "t")
			}},
		{name: "fk", action: "ADD CONSTRAINT r_fk FOREIGN KEY (r) REFERENCES ref (id)", rows: true, bad: "ADD CONSTRAINT r_bad FOREIGN KEY (a) REFERENCES ref (id)", code: "23503",
			check: func(h *alterHarness, tb string) {
				h.expect(fmt.Sprintf(`SELECT convalidated FROM pg_constraint WHERE conrelid = '%s'::regclass AND conname = 'r_fk'`, tb), "t")
			}},
		{name: "unique", action: "ADD CONSTRAINT a_key UNIQUE (a)", rows: true, bad: "ADD CONSTRAINT r_key UNIQUE (r)", code: "23505",
			check: func(h *alterHarness, tb string) {
				h.expect(fmt.Sprintf(`SELECT count(*) FROM pg_indexes WHERE tablename = '%s' AND indexname = 'a_key'`, tb), "1")
			}},
		{name: "drop-constraint", action: "DROP CONSTRAINT old_ck", setup: "ALTER TABLE %s ADD CONSTRAINT old_ck CHECK (a > -100)", bad: "DROP CONSTRAINT nope", code: "42704",
			check: func(h *alterHarness, tb string) {
				h.expect(fmt.Sprintf(`SELECT count(*) FROM pg_constraint WHERE conrelid = '%s'::regclass AND conname = 'old_ck'`, tb), "0")
			}},
		{name: "retype", action: "ALTER COLUMN s TYPE INT8", rows: true, bad: "ALTER COLUMN b TYPE INT8", code: "22P02",
			check: func(h *alterHarness, tb string) {
				h.expect(fmt.Sprintf(`SELECT sum(s) FROM %s`, tb), "210")
				h.expect(col(tb, "s"), "bigint YES -")
			}},
		{name: "widen", action: "ALTER COLUMN w TYPE VARCHAR(50)", rows: true,
			check: func(h *alterHarness, tb string) { h.expect(col(tb, "w"), "character varying(50) YES -") }},
		{name: "validate", action: "VALIDATE CONSTRAINT nv", rows: true, setup: "ALTER TABLE %s ADD CONSTRAINT nv CHECK (a > 0) NOT VALID",
			check: func(h *alterHarness, tb string) {
				h.expect(fmt.Sprintf(`SELECT convalidated FROM pg_constraint WHERE conrelid = '%s'::regclass AND conname = 'nv'`, tb), "t")
			}},
	}
	n := 0
	run := func(k1, k2 alterKind, bad1, bad2 bool) {
		n++
		tb := fmt.Sprintf("m%d", n)
		h.exec(fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, a INT8, b TEXT, r INT8, s TEXT, w VARCHAR(10), z INT8)`, tb))
		h.fill(tb, 20, func(i int) string { return fmt.Sprintf("(%d, %d, 'b%d', %d, '%d', 'w', NULL)", i, i, i, 1+i%2, i) })
		for _, k := range []alterKind{k1, k2} {
			if k.setup != "" {
				h.exec(fmt.Sprintf(k.setup, tb))
			}
		}
		a1, a2 := k1.action, k2.action
		if bad1 {
			a1 = k1.bad
		}
		if bad2 {
			a2 = k2.bad
		}
		before := h.text(h.columnsQuery(tb)) + "/" + h.text(fmt.Sprintf(`SELECT conname FROM pg_constraint WHERE conrelid = '%s'::regclass ORDER BY conname`, tb)) + "/" + h.text(fmt.Sprintf(`SELECT indexname FROM pg_indexes WHERE tablename = '%s' ORDER BY indexname`, tb))
		q := fmt.Sprintf(`ALTER TABLE %s %s, %s`, tb, a1, a2)
		if bad1 || bad2 {
			code := k1.code
			if bad2 {
				code = k2.code
			}
			_, serr := trySQL(ctx, s, q)
			if serr == nil || serr.Code != code {
				t.Fatalf("%s: want %s, got %v", q, code, serr)
			}
			after := h.text(h.columnsQuery(tb)) + "/" + h.text(fmt.Sprintf(`SELECT conname FROM pg_constraint WHERE conrelid = '%s'::regclass ORDER BY conname`, tb)) + "/" + h.text(fmt.Sprintf(`SELECT indexname FROM pg_indexes WHERE tablename = '%s' ORDER BY indexname`, tb))
			if after != before {
				t.Fatalf("%s: failed but changed the table:\n before %s\n after  %s", q, before, after)
			}
			if d := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), tb); d.Alter != nil {
				t.Fatalf("%s: left a pending ALTER", q)
			}
		} else {
			if k1.rows || k2.rows {
				// Refused inside a block, naming the action, and nothing
				// applied; then run on its own.
				h.exec(`BEGIN`)
				if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != "25001" {
					t.Fatalf("%s in a block: %v", q, serr)
				}
				h.exec(`ROLLBACK`)
				execSQL(t, ctx, s, q)
			} else {
				// Descriptor-only: it runs in a block too.
				execSQL(t, ctx, s, `BEGIN; `+q+`; COMMIT`)
			}
			k1.check(h, tb)
			k2.check(h, tb)
			if d := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), tb); d.Alter != nil {
				t.Fatalf("%s: left a pending ALTER", q)
			}
		}
		h.exec(`DROP TABLE ` + tb)
	}
	for i, k1 := range kinds {
		for j, k2 := range kinds {
			if i == j {
				continue
			}
			run(k1, k2, false, false)
			if k2.bad != "" {
				run(k1, k2, false, true)
			}
		}
	}
	t.Logf("%d statements", n)
}

// TestAlterTableFrameworkStatements runs the ALTER TABLE statements Rails
// (change_table bulk: true, add_reference with a foreign key) and Prisma
// migrations generate, as they generate them.
func TestAlterTableFrameworkStatements(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	h := newAlterHarness(t, ctx, s)

	// Rails: create_table, then change_table :users, bulk: true.
	h.exec(`CREATE TABLE "users" ("id" bigserial primary key, "email" character varying NOT NULL, "legacy" character varying, "score" integer)`)
	h.exec(`CREATE TABLE "authors" ("id" bigserial primary key, "name" character varying)`)
	h.exec(`INSERT INTO "authors" ("name") VALUES ('a'), ('b')`)
	// t.timestamps on the empty table: NOT NULL with no default is
	// fine while there are no rows.
	h.exec(`ALTER TABLE "users" ADD "name" character varying, ADD "age" integer DEFAULT 0 NOT NULL, ADD "created_at" timestamp(6) NOT NULL, ADD "updated_at" timestamp(6) NOT NULL`)
	h.exec(`INSERT INTO "users" ("email", "score", "created_at", "updated_at") VALUES ('a@x', 1, now(), now()), ('b@x', 2, now(), now())`)
	// t.remove, t.change, t.change_default, t.change_null, t.rename is
	// a statement of its own.
	h.exec(`ALTER TABLE "users" DROP COLUMN "legacy", ALTER COLUMN "score" TYPE bigint, ALTER COLUMN "score" SET DEFAULT 10, ALTER COLUMN "score" SET NOT NULL, ALTER COLUMN "age" DROP DEFAULT`)
	h.expect(`SELECT column_name, data_type, is_nullable, coalesce(column_default, '-') FROM information_schema.columns WHERE table_name = 'users' AND column_name IN ('score', 'age') ORDER BY column_name`, "age|integer|NO|-;score|bigint|NO|10")
	// t.references :author, foreign_key: true, and t.timestamps over
	// rows: the NOT NULL columns with no default fail, as in PostgreSQL.
	h.exec(`ALTER TABLE "users" ADD "author_id" bigint, ADD CONSTRAINT "fk_rails_e1d8f4c1a8" FOREIGN KEY ("author_id") REFERENCES "authors" ("id")`)
	h.expect(`SELECT conname, confrelid::regclass::text FROM pg_constraint WHERE conrelid = 'users'::regclass AND contype = 'f'`, "fk_rails_e1d8f4c1a8|authors")
	h.exec(`UPDATE "users" SET "author_id" = 2`)
	h.fails(`DELETE FROM "authors" WHERE "id" = 2`, "23503", "")
	h.fails(`ALTER TABLE "users" ADD "published_at" timestamp(6) NOT NULL, ADD "slug" character varying`, "23502", `column "published_at" of relation "users" contains null values`)
	h.expect(`SELECT count(*) FROM information_schema.columns WHERE table_name = 'users' AND column_name IN ('published_at', 'slug')`, "0")
	// add_column with a default lambda.
	h.exec(`ALTER TABLE "users" ADD "token" uuid DEFAULT gen_random_uuid() NOT NULL, ADD "seen_at" timestamp(6) DEFAULT CURRENT_TIMESTAMP NOT NULL`)
	h.expect(`SELECT count(DISTINCT "token"), count(DISTINCT "seen_at") FROM "users"`, "2|1")

	// Prisma: an enum, then the migration's ALTER TABLE statements. (The
	// enum is named in lower case: a quoted mixed-case type name in a
	// column definition is folded, a separate issue.)
	h.exec(`CREATE TYPE "role" AS ENUM ('USER', 'ADMIN')`)
	h.exec(`CREATE TABLE "Post" ("id" SERIAL NOT NULL, "title" TEXT NOT NULL, "content" TEXT NOT NULL, "draft" BOOLEAN, "authorId" INTEGER, CONSTRAINT "Post_pkey" PRIMARY KEY ("id"))`)
	h.exec(`CREATE TABLE "User" ("id" SERIAL NOT NULL, "email" TEXT NOT NULL, CONSTRAINT "User_pkey" PRIMARY KEY ("id"))`)
	h.exec(`INSERT INTO "User" ("email") VALUES ('a'), ('b'), ('c')`)
	h.exec(`INSERT INTO "Post" ("title", "content", "authorId") VALUES ('t1', 'c1', 1), ('t2', 'c2', 2)`)
	h.exec(`ALTER TABLE "User" ADD COLUMN "bio" TEXT,
ADD COLUMN "role" "role" NOT NULL DEFAULT 'USER',
ADD COLUMN "uuid" UUID NOT NULL DEFAULT gen_random_uuid(),
ADD COLUMN "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP`)
	h.expect(`SELECT count(*), count("bio"), count(DISTINCT "uuid"), count(DISTINCT "updatedAt"), min("role"::text) FROM "User"`, "3|0|3|1|USER")
	h.exec(`ALTER TABLE "Post" DROP COLUMN "draft",
ADD COLUMN "views" INTEGER NOT NULL DEFAULT 0,
ALTER COLUMN "title" SET DATA TYPE VARCHAR(255),
ALTER COLUMN "content" DROP NOT NULL`)
	h.exec(`ALTER TABLE "Post" ADD CONSTRAINT "Post_authorId_fkey" FOREIGN KEY ("authorId") REFERENCES "User"("id") ON DELETE SET NULL ON UPDATE CASCADE`)
	h.exec(`ALTER TABLE "User" ALTER COLUMN "email" SET DATA TYPE VARCHAR(100), ALTER COLUMN "bio" SET DEFAULT ''`)
	h.expect(h.columnsQuery("Post"), "id integer;title character varying(255);content text;authorId integer;views integer")
	h.expect(h.columnsQuery("User"), "id integer;email character varying(100);bio text;role USER-DEFINED;uuid uuid;updatedAt timestamp(3) without time zone")

	// pg_dump: ALTER TABLE ONLY.
	h.exec(`ALTER TABLE ONLY public."Post" ALTER COLUMN "views" SET DEFAULT 1`)
	h.exec(`ALTER TABLE ONLY public."Post" ADD CONSTRAINT "Post_title_key" UNIQUE ("title")`)
}

// TestAlterAddColumnVolatileDefaults: ADD COLUMN with a default that is
// not a constant gives each existing row the value PostgreSQL gives it,
// checked row by row: a stable default (now()) once for the statement,
// the same for every row; a volatile one (gen_random_uuid(), random(),
// nextval) afresh for each row. NOT NULL and UNIQUE hold over them.
func TestAlterAddColumnVolatileDefaults(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	h := newAlterHarness(t, ctx, s)

	const n = 300 // several backfill chunks
	h.exec(`CREATE TABLE v (id INT8 PRIMARY KEY, a INT8)`)
	h.fill("v", n, func(i int) string { return fmt.Sprintf("(%d, %d)", i, i) })
	h.exec(`DELETE FROM v WHERE id % 10 = 0`) // gaps between the rows
	const left = n - n/10

	before := time.Now()
	h.exec(`ALTER TABLE v ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now()`)
	after := time.Now()
	// One value for every existing row, the ALTER's: no row rewritten.
	r := h.exec(`SELECT DISTINCT created_at FROM v`)
	if len(r.Rows) != 1 {
		t.Fatalf("now() default: %d distinct values over the existing rows", len(r.Rows))
	}
	at := time.Unix(0, r.Rows[0][0].I)
	if at.Before(before.Add(-time.Second)) || at.After(after.Add(time.Second)) {
		t.Fatalf("now() default %v outside the ALTER's time [%v, %v]", at, before, after)
	}
	// A later INSERT takes its own now().
	time.Sleep(10 * time.Millisecond)
	h.exec(`INSERT INTO v (id, a) VALUES (10000, 0)`)
	h.expect(`SELECT count(DISTINCT created_at) FROM v`, "2")

	// gen_random_uuid(): a fresh value per row, unique, checked as such
	// by the UNIQUE constraint's index.
	h.exec(`ALTER TABLE v ADD COLUMN id2 UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE`)
	h.expect(`SELECT count(*), count(id2), count(DISTINCT id2) FROM v`, fmt.Sprintf("%d|%d|%d", left+1, left+1, left+1))
	r = h.exec(`SELECT id, id2::TEXT FROM v ORDER BY id`)
	for _, row := range r.Rows {
		u := row[1].Text()
		if len(u) != 36 || u[14] != '4' {
			t.Fatalf("row %d: %q is not a version 4 UUID", row[0].I, u)
		}
		// The unique index finds the row by it.
		h.expect(fmt.Sprintf(`SELECT id FROM v WHERE id2 = '%s'`, u), fmt.Sprint(row[0].I))
	}
	h.expect(`SELECT indexname FROM pg_indexes WHERE tablename = 'v' AND indexname <> 'v_pkey'`, "v_id2_key")

	// random() and clock_timestamp(): per row.
	h.exec(`ALTER TABLE v ADD COLUMN r FLOAT8 DEFAULT random(), ADD COLUMN ct TIMESTAMPTZ DEFAULT clock_timestamp()`)
	h.expect(`SELECT count(r), count(DISTINCT r) > 200, min(r) >= 0, max(r) < 1, count(DISTINCT ct) > 1 FROM v`, fmt.Sprintf("%d|t|t|t|t", left+1))

	// now() inside a volatile expression is the statement's, for every
	// row.
	h.exec(`ALTER TABLE v ADD COLUMN later TIMESTAMPTZ DEFAULT now() + random() * INTERVAL '0 seconds'`)
	h.expect(`SELECT count(DISTINCT later), count(later) FROM v`, fmt.Sprintf("1|%d", left+1))

	// SERIAL: the sequence numbers every existing row.
	h.exec(`ALTER TABLE v ADD COLUMN seq SERIAL`)
	h.expect(`SELECT count(DISTINCT seq), min(seq), max(seq) FROM v`, fmt.Sprintf("%d|1|%d", left+1, left+1))
	h.exec(`INSERT INTO v (id, a) VALUES (10001, 0)`)
	h.expect(`SELECT seq FROM v WHERE id = 10001`, fmt.Sprint(left+2))
	h.exec(`ALTER TABLE v ADD COLUMN ident INT8 GENERATED ALWAYS AS IDENTITY`)
	h.expect(`SELECT count(DISTINCT ident), count(ident) FROM v`, fmt.Sprintf("%d|%d", left+2, left+2))

	// NOT NULL with a volatile default that gives NULL fails, and the
	// column is not added; UNIQUE over a constant over several rows fails.
	h.fails(`ALTER TABLE v ADD COLUMN bad FLOAT8 NOT NULL DEFAULT nullif(0 * random(), 0)`, "23502", `column "bad" of relation "v" contains null values`)
	h.fails(`ALTER TABLE v ADD COLUMN bad INT8 DEFAULT 7 UNIQUE`, "23505", "v_bad_key")
	h.fails(`ALTER TABLE v ADD COLUMN bad INT8 DEFAULT floor(random() * 2)::INT8 UNIQUE`, "23505", "v_bad_key")
	h.expect(`SELECT count(*) FROM information_schema.columns WHERE table_name = 'v' AND column_name = 'bad'`, "0")
	h.expect(`SELECT count(*) FROM pg_class WHERE relname = 'v_bad_seq' OR relname = 'v_bad_key'`, "0")
	if d := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "v"); d.Alter != nil {
		t.Fatal("a failed ADD COLUMN left its plan")
	}
	for _, c := range lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "v").Columns {
		if c.Hidden || c.Adding != "" {
			t.Fatalf("a failed ADD COLUMN left column %+v", c)
		}
	}

	// Inside a block, a volatile default is refused (the rows are filled
	// online); a stable one is fine.
	h.exec(`BEGIN`)
	h.fails(`ALTER TABLE v ADD COLUMN x UUID DEFAULT gen_random_uuid()`, "25001", "ADD COLUMN x")
	h.exec(`ROLLBACK`)
	h.exec(`BEGIN; ALTER TABLE v ADD COLUMN y TIMESTAMPTZ DEFAULT now(); COMMIT`)
	h.expect(`SELECT count(DISTINCT y), count(y) FROM v`, fmt.Sprintf("1|%d", left+2))
}

// TestAlterAddColumnBackfillConcurrentWrites runs ADD COLUMN backfills
// on one node of three while the other two insert, update and delete
// rows of the table: every row ends up with the column filled — a value
// of its own for a volatile default, the statement's for now() — and
// the unique index agrees with the rows.
func TestAlterAddColumnBackfillConcurrentWrites(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	// Gateways that lease their descriptors, as a server's do: the drain
	// waits for them.
	const ttl = 2 * time.Second
	s := leasedSession(t, tc, 0, ttl)
	waitForDatabases(t, ctx, s)
	h := newAlterHarness(t, ctx, s)

	const n = 1500
	h.exec(`CREATE TABLE w (id INT8 PRIMARY KEY, v INT8, note TEXT)`)
	h.fill("w", n, func(i int) string { return fmt.Sprintf("(%d, %d, 'n')", i, i) })

	// Slow the backfill a little so the writers overlap it.
	sql.TestingAlterBackfillChunk = func(string) error { time.Sleep(5 * time.Millisecond); return nil }
	defer func() { sql.TestingAlterBackfillChunk = nil }()

	var stop atomic.Bool
	var wg sync.WaitGroup
	var inserted, writes atomic.Int64
	writers := make([]*sql.Session, 5)
	for w := 1; w <= 4; w++ {
		writers[w] = leasedSession(t, tc, 1+w%2, ttl)
		// Each writer has the table's descriptor leased before the ALTER.
		execSQL(t, ctx, writers[w], `SELECT count(*) FROM w`)
	}
	for w := 1; w <= 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ws := writers[w]
			rng := rand.New(rand.NewSource(int64(w)))
			for i := 0; !stop.Load(); i++ {
				var q string
				switch i % 4 {
				case 0:
					q = fmt.Sprintf(`INSERT INTO w (id, v, note) VALUES (%d, %d, 'new')`, 100000*w+i, i)
				case 1:
					q = fmt.Sprintf(`UPDATE w SET v = v + 1, note = 'upd' WHERE id = %d`, 1+rng.Intn(n))
				case 2:
					q = fmt.Sprintf(`DELETE FROM w WHERE id = %d`, 1+rng.Intn(n))
				case 3:
					q = fmt.Sprintf(`UPDATE w SET note = 'upd2' WHERE id BETWEEN %d AND %d`, 1+rng.Intn(n), 1+rng.Intn(n))
				}
				if _, serr := trySQL(ctx, ws, q); serr == nil {
					writes.Add(1)
					if i%4 == 0 {
						inserted.Add(1)
					}
				}
			}
		}(w)
	}
	time.Sleep(50 * time.Millisecond)
	h.exec(`ALTER TABLE w ADD COLUMN u UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE, ADD COLUMN r FLOAT8 NOT NULL DEFAULT random(), ADD COLUMN at TIMESTAMPTZ NOT NULL DEFAULT now(), ADD COLUMN k INT8 NOT NULL DEFAULT 5`)
	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	if writes.Load() < 20 {
		t.Fatalf("only %d concurrent writes", writes.Load())
	}
	t.Logf("%d concurrent writes, %d inserts", writes.Load(), inserted.Load())

	for i := range tc.Nodes {
		hi := newAlterHarness(t, ctx, writers[1+i%4])
		total := hi.text(`SELECT count(*) FROM w`)
		hi.expect(`SELECT count(u), count(DISTINCT u), count(r), count(at), sum(k) / 5 FROM w`, strings.Repeat(total+"|", 4)+total)
		// The rows that predate the ALTER share its now().
		hi.expect(`SELECT count(DISTINCT at) FROM w WHERE id <= 1500`, "1")
		// The unique index has an entry for every row and none other.
		r := hi.exec(`SELECT id, u::TEXT FROM w WHERE id % 37 = 0 OR id > 100000`)
		for _, row := range r.Rows {
			hi.expect(fmt.Sprintf(`SELECT id FROM w WHERE u = '%s'`, row[1].Text()), fmt.Sprint(row[0].I))
		}
		_ = i
	}
	// New writes after the statement fill the columns too.
	h.exec(`INSERT INTO w (id) VALUES (999999)`)
	h.expect(`SELECT u IS NOT NULL, r IS NOT NULL, k FROM w WHERE id = 999999`, "t|t|5")
}

// TestAlterAddColumnBackfillRestart stops the gateway in the middle of an
// ADD COLUMN backfill: the table keeps working meanwhile — the column
// stays invisible, new rows fill it — another online ALTER TABLE is
// refused, and running the statement again, on another node, resumes
// it: every row gets its own value, the ones filled before the stop
// keep theirs.
func TestAlterAddColumnBackfillRestart(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	const ttl = 2 * time.Second
	s0 := leasedSession(t, tc, 0, ttl)
	waitForDatabases(t, ctx, s0)
	h0 := newAlterHarness(t, ctx, s0)
	const n = 640 // ten chunks
	h0.exec(`CREATE TABLE z (id INT8 PRIMARY KEY, v INT8)`)
	h0.fill("z", n, func(i int) string { return fmt.Sprintf("(%d, %d)", i, i) })

	const stmt = `ALTER TABLE z ADD COLUMN u UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE, ADD COLUMN at TIMESTAMPTZ DEFAULT now() + random() * INTERVAL '0 s', DROP COLUMN v`
	var chunks atomic.Int64
	reached, release := make(chan struct{}), make(chan struct{})
	sql.TestingAlterBackfillChunk = func(string) error {
		if chunks.Add(1) == 4 {
			close(reached)
			<-release // the gateway stops here
			return sql.ErrTestingAlterInterrupted
		}
		return nil
	}
	defer func() { sql.TestingAlterBackfillChunk = nil }()
	done := make(chan *sql.Error, 1)
	go func() {
		_, serr := trySQL(ctx, s0, stmt)
		done <- serr
	}()
	<-reached
	tc.StopNode(0)
	close(release)
	if serr := <-done; serr == nil || serr.Code != "57014" {
		t.Fatalf("the statement through the stopped gateway: %v", serr)
	}
	sql.TestingAlterBackfillChunk = nil

	s1 := leasedSession(t, tc, 1, ttl)
	h1 := newAlterHarness(t, ctx, s1)
	d := lookupDescriptor(t, ctx, tc.Nodes[1].DB(), "z")
	if d.Alter == nil {
		t.Fatal("the interrupted statement left no plan")
	}
	var pending catalog.Column
	for _, c := range d.Columns {
		if c.Adding == "u" {
			pending = c
		}
	}
	if !pending.Hidden {
		t.Fatalf("the column being added is not hidden: %+v", pending)
	}
	// Meanwhile the table works as it did, and writes fill the column.
	h1.expect(h1.columnsQuery("z"), "id bigint;v bigint")
	h1.exec(`INSERT INTO z VALUES (5000, 1)`)
	h1.exec(`UPDATE z SET v = 0 - v WHERE id <= 100`)
	h1.exec(`DELETE FROM z WHERE id = 3`)
	h1.fails(`INSERT INTO z (id, u) VALUES (5001, gen_random_uuid())`, "42703", "u")
	h1.fails(`ALTER TABLE z ADD COLUMN w INT8, ADD CONSTRAINT w_pos CHECK (w > 0)`, "55006", "run that statement again")
	h1.fails(`ALTER TABLE z ADD COLUMN u INT8`, "55006", "being added")
	filled := 0
	if err := tc.Nodes[1].DB().RunTxn(ctx, "count", func(ctx context.Context, txn *kvclient.Txn) error {
		filled = 0
		lo, hi := rowenc.PrimarySpanFor(d)
		kvs, err := txn.Scan(ctx, lo, hi, 0)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			row, err := rowenc.DecodeValue(d, kv.Value)
			if err != nil {
				return err
			}
			if _, ok := row[pending.ID]; ok {
				filled++
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if filled == 0 || filled >= n {
		t.Fatalf("%d of %d rows filled before the stop, want some", filled, n)
	}

	// The gateway comes back; the statement runs again elsewhere and
	// resumes.
	tc.RestartNode(0, engines[0])
	r := h1.exec(stmt)
	if len(r.Notices) == 0 || !strings.Contains(r.Notices[len(r.Notices)-1], "resumed") {
		t.Fatalf("re-run: notices %v", r.Notices)
	}
	h1.expect(h1.columnsQuery("z"), "id bigint;u uuid;at timestamp with time zone")
	h1.expect(`SELECT count(*), count(u), count(DISTINCT u), count(DISTINCT at) FROM z WHERE id < 5000`, fmt.Sprintf("%d|%d|%d|1", n-1, n-1, n-1))
	h1.expect(`SELECT count(u) FROM z WHERE id = 5000`, "1")
	if d := lookupDescriptor(t, ctx, tc.Nodes[1].DB(), "z"); d.Alter != nil {
		t.Fatal("the plan is still there")
	}
	// The table takes an online ALTER TABLE again.
	h1.exec(`ALTER TABLE z ADD COLUMN w INT8 DEFAULT 1, ADD CONSTRAINT w_pos CHECK (w > 0)`)
}

// TestAlterAddColumnBackupRestore backs up a table after ADD COLUMN with
// volatile and stable defaults and restores it into a new cluster: the
// values each row was given come back as they were.
func TestAlterAddColumnBackupRestore(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	h := newAlterHarness(t, ctx, s)
	h.exec(`CREATE TABLE b (id INT8 PRIMARY KEY, v INT8, gone TEXT)`)
	h.fill("b", 200, func(i int) string { return fmt.Sprintf("(%d, %d, 'g')", i, i) })
	h.exec(`ALTER TABLE b ADD COLUMN u UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE, ADD COLUMN at TIMESTAMPTZ DEFAULT now(), ADD COLUMN k INT8 NOT NULL DEFAULT 3, DROP COLUMN gone, ADD COLUMN s SERIAL`)
	h.exec(`INSERT INTO b (id, v) VALUES (1000, 0)`)
	const q = `SELECT id, v, u::TEXT, at::TEXT, k, s FROM b ORDER BY id`
	want := h.text(q)

	dir := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatalf("backup: %v", err)
	}
	tc2 := Start(t, 3)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	h2 := newAlterHarness(t, ctx, s2)
	h2.expect(q, want)
	h2.expect(h2.columnsQuery("b"), "id bigint;v bigint;u uuid;at timestamp with time zone;k bigint;s integer")
	// The restored table works: the unique index, the defaults.
	h2.exec(`INSERT INTO b (id, v) VALUES (1001, 0)`)
	h2.expect(`SELECT k, s > 0, u IS NOT NULL FROM b WHERE id = 1001`, "3|t|t")
	u := h2.text(`SELECT u::TEXT FROM b WHERE id = 7`)
	h2.expect(fmt.Sprintf(`SELECT id FROM b WHERE u = '%s'`, u), "7")
	h2.fails(fmt.Sprintf(`UPDATE b SET u = '%s' WHERE id = 8`, u), "23505", "")
}
