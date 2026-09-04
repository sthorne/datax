package testcluster

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// TestConstraints: CHECK, UNIQUE and FOREIGN KEY constraints — every
// form of CREATE TABLE and ALTER TABLE, the referential actions, the
// cascade cap, NOT VALID / VALIDATE, DROP CONSTRAINT, SET / DROP NOT
// NULL, DROP TABLE CASCADE, the catalogs, COPY, psql, and the error
// codes.
func TestConstraints(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	texts := func(r *sql.Result) []string {
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
		return out
	}
	expect := func(what string, r *sql.Result, rows ...string) {
		t.Helper()
		if strings.Join(texts(r), ";") != strings.Join(rows, ";") {
			t.Fatalf("%s: got %v, want %v", what, texts(r), rows)
		}
	}
	code := func(what, q, want string) {
		t.Helper()
		_, serr := trySQL(ctx, s, q)
		if serr == nil || serr.Code != want {
			t.Fatalf("%s: %v, want %s", what, serr, want)
		}
		if strings.Contains(what, "@") {
			if frag := strings.SplitN(what, "@", 2)[1]; !strings.Contains(serr.Error(), frag) {
				t.Fatalf("%s: message %q lacks %q", what, serr.Error(), frag)
			}
		}
	}

	// ---- CHECK -------------------------------------------------------
	execSQL(t, ctx, s, `CREATE TABLE items (id INT8 PRIMARY KEY, qty INT8 CHECK (qty > 0), price DECIMAL, name TEXT,
		CONSTRAINT price_ok CHECK (price >= 0 OR price IS NULL), CHECK (length(name) < 10))`)
	execSQL(t, ctx, s, `INSERT INTO items VALUES (1, 5, 1.5, 'anvil'), (2, 1, NULL, NULL), (3, NULL, 0, 'x')`)
	code("check on insert@items_qty_check", `INSERT INTO items VALUES (4, 0, 1, 'x')`, sql.CodeCheckViolation)
	code("named check@price_ok", `INSERT INTO items VALUES (4, 1, -1, 'x')`, sql.CodeCheckViolation)
	code("function check@items_name_check", `INSERT INTO items VALUES (4, 1, 1, 'a name too long')`, sql.CodeCheckViolation)
	code("check on update", `UPDATE items SET qty = -1 WHERE id = 1`, sql.CodeCheckViolation)
	expect("null passes, update ok", execSQL(t, ctx, s, `UPDATE items SET qty = NULL, price = NULL WHERE id = 1 RETURNING qty`), "NULL")
	code("check with subquery", `CREATE TABLE bad (id INT8 PRIMARY KEY, n INT8 CHECK (n IN (SELECT id FROM items)))`, sql.CodeFeatureNotSupported)
	code("check unknown column", `CREATE TABLE bad (id INT8 PRIMARY KEY, n INT8 CHECK (m > 0))`, sql.CodeUndefinedColumn)
	code("check unknown function", `CREATE TABLE bad (id INT8 PRIMARY KEY, n INT8 CHECK (nosuch(n) > 0))`, sql.CodeSyntaxError)
	code("check volatile", `CREATE TABLE bad (id INT8 PRIMARY KEY, n INT8 CHECK (n > unique_rowid()))`, sql.CodeFeatureNotSupported)
	// Stable session functions are allowed and resolved per statement.
	execSQL(t, ctx, s, `CREATE TABLE stamped (id INT8 PRIMARY KEY, at TIMESTAMPTZ CHECK (at <= now()), who TEXT CHECK (who = current_user))`)
	execSQL(t, ctx, s, `INSERT INTO stamped VALUES (1, '2020-01-01 00:00:00Z', 'root')`)
	code("check now()@stamped_at_check", `INSERT INTO stamped VALUES (2, '2100-01-01 00:00:00Z', 'root')`, sql.CodeCheckViolation)
	code("check current_user@stamped_who_check", `INSERT INTO stamped VALUES (2, '2020-01-01 00:00:00Z', 'bob')`, sql.CodeCheckViolation)
	execSQL(t, ctx, s, `CREATE TABLE stamped2 (id INT8 PRIMARY KEY, at TIMESTAMPTZ)`)
	execSQL(t, ctx, s, `INSERT INTO stamped2 VALUES (1, '2020-01-01 00:00:00Z'), (2, '2100-01-01 00:00:00Z')`)
	code("add check now() validates@fut", `ALTER TABLE stamped2 ADD CONSTRAINT fut CHECK (at <= now())`, sql.CodeCheckViolation)
	execSQL(t, ctx, s, `DELETE FROM stamped2 WHERE id = 2`)
	execSQL(t, ctx, s, `ALTER TABLE stamped2 ADD CONSTRAINT fut CHECK (at <= now())`)
	code("added check now() enforced@fut", `INSERT INTO stamped2 VALUES (2, '2100-01-01 00:00:00Z')`, sql.CodeCheckViolation)
	code("duplicate constraint name", `CREATE TABLE bad (id INT8 PRIMARY KEY, n INT8, CONSTRAINT c CHECK (n > 0), CONSTRAINT c CHECK (n < 9))`, sql.CodeDuplicateObject)
	expect("check constraints in the catalog",
		execSQL(t, ctx, s, `SELECT conname, contype, convalidated, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'items'::regclass AND contype = 'c' ORDER BY conname`),
		"items_name_check|c|t|CHECK (length(name) < 10)", "items_qty_check|c|t|CHECK (qty > 0)", "price_ok|c|t|CHECK (price >= 0 OR price IS NULL)")
	expect("information_schema.check_constraints", execSQL(t, ctx, s, `SELECT check_clause FROM information_schema.check_constraints WHERE constraint_name = 'price_ok'`), "price >= 0 OR price IS NULL")
	if r := execSQL(t, ctx, s, `SHOW CREATE TABLE items`); !strings.Contains(r.Rows[0][1].S, "CONSTRAINT price_ok CHECK (price >= 0 OR price IS NULL)") {
		t.Fatalf("SHOW CREATE TABLE: %s", r.Rows[0][1].S)
	}
	// ADD CONSTRAINT validates existing rows; NOT VALID defers that.
	execSQL(t, ctx, s, `INSERT INTO items VALUES (4, 5000, 1, 'big')`)
	code("add check fails on existing row", `ALTER TABLE items ADD CONSTRAINT qty_small CHECK (qty < 1000)`, sql.CodeCheckViolation)
	expect("failed constraint not left behind", execSQL(t, ctx, s, `SELECT count(*) FROM pg_constraint WHERE conname = 'qty_small'`), "0")
	execSQL(t, ctx, s, `ALTER TABLE items ADD CONSTRAINT qty_small CHECK (qty < 1000) NOT VALID`)
	expect("not valid", execSQL(t, ctx, s, `SELECT convalidated FROM pg_constraint WHERE conname = 'qty_small'`), "f")
	code("not valid still checks new rows", `INSERT INTO items VALUES (5, 2000, 1, 'x')`, sql.CodeCheckViolation)
	code("validate finds the old row", `ALTER TABLE items VALIDATE CONSTRAINT qty_small`, sql.CodeCheckViolation)
	execSQL(t, ctx, s, `DELETE FROM items WHERE id = 4`)
	execSQL(t, ctx, s, `ALTER TABLE items VALIDATE CONSTRAINT qty_small`)
	expect("validated", execSQL(t, ctx, s, `SELECT convalidated FROM pg_constraint WHERE conname = 'qty_small'`), "t")
	execSQL(t, ctx, s, `ALTER TABLE items DROP CONSTRAINT qty_small`)
	execSQL(t, ctx, s, `INSERT INTO items VALUES (5, 2000, 1, 'x')`)
	code("drop unknown constraint", `ALTER TABLE items DROP CONSTRAINT nosuch`, sql.CodeUndefinedObject)
	execSQL(t, ctx, s, `ALTER TABLE items DROP CONSTRAINT IF EXISTS nosuch`)
	code("validate unknown constraint", `ALTER TABLE items VALIDATE CONSTRAINT nosuch`, sql.CodeUndefinedObject)
	execSQL(t, ctx, s, `BEGIN`)
	code("add constraint in a transaction", `ALTER TABLE items ADD CHECK (qty > 0)`, sql.CodeActiveTransaction)
	execSQL(t, ctx, s, `ROLLBACK`)
	code("drop constrained column", `ALTER TABLE items DROP COLUMN qty`, sql.CodeDependentObjectsExist)

	// ---- UNIQUE ------------------------------------------------------
	execSQL(t, ctx, s, `CREATE TABLE u (id INT8 PRIMARY KEY, a INT8 UNIQUE, b TEXT, c TEXT, CONSTRAINT u_bc UNIQUE (b, c))`)
	execSQL(t, ctx, s, `INSERT INTO u VALUES (1, 10, 'x', 'y'), (2, 20, 'x', 'z')`)
	code("column unique@u_a_key", `INSERT INTO u VALUES (3, 10, 'q', 'q')`, sql.CodeUniqueViolation)
	code("table unique@u_bc", `INSERT INTO u VALUES (3, 30, 'x', 'y')`, sql.CodeUniqueViolation)
	expect("on conflict on constraint", execSQL(t, ctx, s, `INSERT INTO u VALUES (3, 30, 'x', 'y') ON CONFLICT ON CONSTRAINT u_bc DO UPDATE SET a = 31 RETURNING id, a`), "1|31")
	expect("unique constraints in the catalog", execSQL(t, ctx, s, `SELECT conname, contype FROM pg_constraint WHERE conrelid = 'u'::regclass ORDER BY conname`), "u_a_key|u", "u_bc|u", "u_pkey|p")
	execSQL(t, ctx, s, `INSERT INTO u VALUES (4, 40, 'dup', 'a'), (5, 50, 'dup2', 'a')`)
	code("add unique with duplicates", `ALTER TABLE u ADD CONSTRAINT u_c_key UNIQUE (c)`, sql.CodeUniqueViolation)
	expect("failed unique leaves no index", execSQL(t, ctx, s, `SELECT count(*) FROM pg_indexes WHERE indexname = 'u_c_key'`), "0")
	execSQL(t, ctx, s, `DELETE FROM u WHERE id = 5`)
	execSQL(t, ctx, s, `ALTER TABLE u ADD CONSTRAINT u_c_key UNIQUE (c)`)
	code("added unique enforced", `INSERT INTO u VALUES (6, 60, 'q', 'a')`, sql.CodeUniqueViolation)
	if pl := explainPlan(t, ctx, s, `SELECT id FROM u WHERE c = 'a'`); !strings.Contains(pl, `"u_c_key"`) {
		t.Fatalf("unique constraint index unused: %q", pl)
	}
	execSQL(t, ctx, s, `ALTER TABLE u DROP CONSTRAINT u_c_key`)
	execSQL(t, ctx, s, `INSERT INTO u VALUES (6, 60, 'q', 'a')`)
	expect("dropped unique's index gone", execSQL(t, ctx, s, `SELECT count(*) FROM pg_indexes WHERE indexname = 'u_c_key'`), "0")

	// ---- FOREIGN KEY -------------------------------------------------
	execSQL(t, ctx, s, `CREATE TABLE p (id INT8 PRIMARY KEY, code TEXT, CONSTRAINT p_code_key UNIQUE (code))`)
	execSQL(t, ctx, s, `CREATE TABLE c (id INT8 PRIMARY KEY, pid INT8 REFERENCES p ON DELETE CASCADE,
		code TEXT CONSTRAINT c_code_fkey REFERENCES p (code) ON DELETE SET NULL ON UPDATE CASCADE, n INT8)`)
	execSQL(t, ctx, s, `CREATE TABLE g (id INT8 PRIMARY KEY, cid INT8, CONSTRAINT g_cid_fkey FOREIGN KEY (cid) REFERENCES c (id) ON DELETE CASCADE)`)
	execSQL(t, ctx, s, `CREATE TABLE r (id INT8 PRIMARY KEY, pid INT8 REFERENCES p)`)
	code("no unique key on the referenced columns", `CREATE TABLE bad (id INT8 PRIMARY KEY, n INT8 REFERENCES c (n))`, sql.CodeInvalidColumnReference)
	code("type mismatch", `CREATE TABLE bad (id INT8 PRIMARY KEY, t TEXT REFERENCES p (id))`, sql.CodeInvalidColumnReference)
	code("unknown referenced table", `CREATE TABLE bad (id INT8 PRIMARY KEY, t INT8 REFERENCES nosuch)`, sql.CodeUndefinedTable)
	expect("automatic indexes on the referencing columns", execSQL(t, ctx, s, `SELECT indexname FROM pg_indexes WHERE tablename = 'c' ORDER BY 1`), "c_code_fkey_idx", "c_pid_fkey_idx", "c_pkey")
	if pl := explainPlan(t, ctx, s, `SELECT id FROM c WHERE pid = 1`); !strings.Contains(pl, `"c_pid_fkey_idx"`) {
		t.Fatalf("foreign key index unused: %q", pl)
	}
	execSQL(t, ctx, s, `INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	execSQL(t, ctx, s, `INSERT INTO c VALUES (1, 1, 'a', 0), (2, 1, NULL, 0), (3, 2, 'a', 0), (4, NULL, NULL, 0)`)
	code("insert violates fk@(pid)=(9) is not present in table \"p\"", `INSERT INTO c VALUES (5, 9, NULL, 0)`, sql.CodeForeignKeyViolation)
	code("insert violates fk through unique index", `INSERT INTO c VALUES (5, 1, 'zz', 0)`, sql.CodeForeignKeyViolation)
	code("update violates fk", `UPDATE c SET pid = 9 WHERE id = 1`, sql.CodeForeignKeyViolation)
	execSQL(t, ctx, s, `UPDATE c SET n = 1 WHERE id = 1`) // untouched key columns are not re-checked
	execSQL(t, ctx, s, `INSERT INTO g VALUES (1, 1), (2, 3)`)
	execSQL(t, ctx, s, `INSERT INTO r VALUES (1, 3)`)
	code("delete restrict@is still referenced from table \"r\"", `DELETE FROM p WHERE id = 3`, sql.CodeForeignKeyViolation)
	execSQL(t, ctx, s, `DELETE FROM r`)
	// ON UPDATE CASCADE through the unique key.
	expect("update cascade", execSQL(t, ctx, s, `UPDATE p SET code = 'z' WHERE id = 1 RETURNING code`), "z")
	expect("children re-keyed", execSQL(t, ctx, s, `SELECT id, code FROM c WHERE code = 'z' ORDER BY id`), "1|z", "3|z")
	// ON DELETE CASCADE and SET NULL, two levels deep.
	execSQL(t, ctx, s, `DELETE FROM p WHERE id = 1`)
	expect("cascade deleted the children", execSQL(t, ctx, s, `SELECT id, pid, code FROM c ORDER BY id`), "3|2|NULL", "4|NULL|NULL")
	expect("cascade reached the grandchildren", execSQL(t, ctx, s, `SELECT id FROM g ORDER BY id`), "2")
	// The cascade cap.
	execSQL(t, ctx, s, `INSERT INTO p VALUES (5, 'e')`)
	execSQL(t, ctx, s, `INSERT INTO c VALUES (10, 5, NULL, 0), (11, 5, NULL, 0), (12, 5, NULL, 0)`)
	execSQL(t, ctx, s, `SET foreign_key_cascade_limit = 2`)
	expect("setting shows", execSQL(t, ctx, s, `SHOW foreign_key_cascade_limit`), "2")
	code("cascade over the cap", `DELETE FROM p WHERE id = 5`, sql.CodeProgramLimitExceeded)
	expect("nothing cascaded", execSQL(t, ctx, s, `SELECT count(*) FROM c WHERE pid = 5`), "3")
	execSQL(t, ctx, s, `SET foreign_key_cascade_limit = 10000`)
	execSQL(t, ctx, s, `DELETE FROM p WHERE id = 5`)
	expect("cascaded under the cap", execSQL(t, ctx, s, `SELECT count(*) FROM c WHERE pid = 5`), "0")
	// The catalogs and SHOW CREATE TABLE.
	expect("foreign keys in the catalog",
		execSQL(t, ctx, s, `SELECT conname, contype, confrelid::regclass, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'c'::regclass AND contype = 'f' ORDER BY conname`),
		"c_code_fkey|f|p|FOREIGN KEY (code) REFERENCES p(code) ON UPDATE CASCADE ON DELETE SET NULL", "c_pid_fkey|f|p|FOREIGN KEY (pid) REFERENCES p(id) ON DELETE CASCADE")
	expect("referential_constraints", execSQL(t, ctx, s, `SELECT constraint_name, unique_constraint_name, update_rule, delete_rule FROM information_schema.referential_constraints ORDER BY 1`),
		"c_code_fkey|p_code_key|CASCADE|SET NULL", "c_pid_fkey|p_pkey|NO ACTION|CASCADE", "g_cid_fkey|c_pkey|NO ACTION|CASCADE", "r_pid_fkey|p_pkey|NO ACTION|NO ACTION")
	expect("key_column_usage", execSQL(t, ctx, s, `SELECT column_name FROM information_schema.key_column_usage WHERE constraint_name = 'c_code_fkey'`), "code")
	expect("table_constraints", execSQL(t, ctx, s, `SELECT constraint_type FROM information_schema.table_constraints WHERE constraint_name = 'g_cid_fkey'`), "FOREIGN KEY")
	if r := execSQL(t, ctx, s, `SHOW CREATE TABLE c`); !strings.Contains(r.Rows[0][1].S, "CONSTRAINT c_pid_fkey FOREIGN KEY (pid) REFERENCES p(id) ON DELETE CASCADE") || strings.Contains(r.Rows[0][1].S, "INDEX c_pid_fkey_idx") {
		t.Fatalf("SHOW CREATE TABLE: %s", r.Rows[0][1].S)
	}
	// DROP TABLE refuses a referenced table without CASCADE.
	code("drop referenced table", `DROP TABLE p`, sql.CodeDependentObjectsExist)
	code("drop referenced column", `ALTER TABLE p DROP COLUMN code`, sql.CodeDependentObjectsExist)
	execSQL(t, ctx, s, `DROP TABLE p CASCADE`)
	expect("children's keys dropped with the parent", execSQL(t, ctx, s, `SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND conrelid IN ('c'::regclass, 'r'::regclass)`), "0")
	execSQL(t, ctx, s, `INSERT INTO c VALUES (20, 999, 'nope', 0)`)
	// ADD FOREIGN KEY online: existing rows are validated.
	execSQL(t, ctx, s, `CREATE TABLE p2 (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s, `INSERT INTO p2 VALUES (2)`)
	code("add fk fails on existing row", `ALTER TABLE c ADD CONSTRAINT c_pid_fkey FOREIGN KEY (pid) REFERENCES p2 (id)`, sql.CodeForeignKeyViolation)
	expect("failed fk not left behind", execSQL(t, ctx, s, `SELECT count(*) FROM pg_constraint WHERE conname = 'c_pid_fkey'`), "0")
	execSQL(t, ctx, s, `ALTER TABLE c ADD CONSTRAINT c_pid_fkey FOREIGN KEY (pid) REFERENCES p2 (id) ON DELETE SET NULL NOT VALID`)
	code("not valid fk checks new rows", `INSERT INTO c VALUES (21, 7, NULL, 0)`, sql.CodeForeignKeyViolation)
	code("validate fk", `ALTER TABLE c VALIDATE CONSTRAINT c_pid_fkey`, sql.CodeForeignKeyViolation)
	execSQL(t, ctx, s, `DELETE FROM c WHERE id = 20`)
	execSQL(t, ctx, s, `ALTER TABLE c VALIDATE CONSTRAINT c_pid_fkey`)
	execSQL(t, ctx, s, `DELETE FROM p2 WHERE id = 2`)
	expect("set null", execSQL(t, ctx, s, `SELECT id, pid FROM c WHERE id = 3`), "3|NULL")
	execSQL(t, ctx, s, `ALTER TABLE c DROP CONSTRAINT c_pid_fkey`)
	execSQL(t, ctx, s, `INSERT INTO c VALUES (22, 7, NULL, 0)`)
	// Self-reference, with rows of one statement referencing each other.
	execSQL(t, ctx, s, `CREATE TABLE tree (id INT8 PRIMARY KEY, parent INT8 REFERENCES tree ON DELETE CASCADE)`)
	execSQL(t, ctx, s, `INSERT INTO tree VALUES (1, NULL), (2, 1), (3, 2), (4, 1)`)
	code("self-reference violation", `INSERT INTO tree VALUES (5, 42)`, sql.CodeForeignKeyViolation)
	execSQL(t, ctx, s, `DELETE FROM tree WHERE id = 1`)
	expect("self cascade", execSQL(t, ctx, s, `SELECT count(*) FROM tree`), "0")
	// Cross-database keys are refused.
	execSQL(t, ctx, s, `CREATE DATABASE other`)
	execSQL(t, ctx, s, `CREATE TABLE other.t (id INT8 PRIMARY KEY)`)
	code("cross-database fk", `CREATE TABLE bad (id INT8 PRIMARY KEY, t INT8 REFERENCES other.t)`, sql.CodeFeatureNotSupported)

	// ---- SET / DROP NOT NULL -----------------------------------------
	execSQL(t, ctx, s, `CREATE TABLE nn (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO nn VALUES (1, NULL), (2, 'x')`)
	code("set not null with nulls", `ALTER TABLE nn ALTER COLUMN v SET NOT NULL`, sql.CodeNotNullViolation)
	execSQL(t, ctx, s, `INSERT INTO nn VALUES (3, NULL)`) // the failed change was reverted
	execSQL(t, ctx, s, `UPDATE nn SET v = 'y' WHERE v IS NULL`)
	execSQL(t, ctx, s, `ALTER TABLE nn ALTER COLUMN v SET NOT NULL`)
	code("not null enforced", `INSERT INTO nn VALUES (4, NULL)`, sql.CodeNotNullViolation)
	expect("is_nullable", execSQL(t, ctx, s, `SELECT is_nullable FROM information_schema.columns WHERE table_name = 'nn' AND column_name = 'v'`), "NO")
	execSQL(t, ctx, s, `ALTER TABLE nn ALTER COLUMN v DROP NOT NULL`)
	execSQL(t, ctx, s, `INSERT INTO nn VALUES (4, NULL)`)
	code("drop not null on the key", `ALTER TABLE nn ALTER COLUMN id DROP NOT NULL`, sql.CodeInvalidColumnReference)

	// ---- COPY --------------------------------------------------------
	conn, err := pgx.Connect(ctx, "postgres://root@"+tc.Nodes[0].SQLAddr()+"/datax?sslmode=disable")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	_, err = conn.CopyFrom(ctx, pgx.Identifier{"items"}, []string{"id", "qty", "price", "name"},
		pgx.CopyFromRows([][]any{{100, int64(1), "1.0", "ok"}, {101, int64(-5), "1.0", "bad"}}))
	if err == nil || !strings.Contains(err.Error(), "23514") && !strings.Contains(err.Error(), "check constraint") {
		t.Fatalf("COPY ignored the CHECK: %v", err)
	}
	if !strings.Contains(err.Error(), "row 2") {
		t.Fatalf("COPY error lacks the row: %v", err)
	}
	expect("COPY chunk rolled back", execSQL(t, ctx, s, `SELECT count(*) FROM items WHERE id >= 100`), "0")

	// ---- psql --------------------------------------------------------
	if psql, err := lookPsql(); err == nil {
		url := "postgres://root@" + tc.Nodes[0].SQLAddr() + "/datax?sslmode=disable"
		execSQL(t, ctx, s, `CREATE TABLE p3 (id INT8 PRIMARY KEY)`)
		execSQL(t, ctx, s, `CREATE TABLE c3 (id INT8 PRIMARY KEY, pid INT8 REFERENCES p3 ON DELETE CASCADE, n INT8 CHECK (n > 0))`)
		out, err := runPsql(ctx, psql, url, `\d c3`)
		if err != nil || strings.Contains(out, "ERROR") {
			t.Fatalf("psql \\d c3: %v\n%s", err, out)
		}
		for _, want := range []string{`"c3_n_check" CHECK (n > 0)`, `"c3_pid_fkey" FOREIGN KEY (pid) REFERENCES p3(id) ON DELETE CASCADE`} {
			if !strings.Contains(out, want) {
				t.Fatalf("psql \\d c3 lacks %q:\n%s", want, out)
			}
		}
		out, err = runPsql(ctx, psql, url, `\d p3`)
		if err != nil || strings.Contains(out, "ERROR") || !strings.Contains(out, `TABLE "c3" CONSTRAINT "c3_pid_fkey" FOREIGN KEY (pid) REFERENCES p3(id) ON DELETE CASCADE`) {
			t.Fatalf("psql \\d p3 lacks the referencing key: %v\n%s", err, out)
		}
	}
}

// TestForeignKeyRace: a parent delete racing a child insert from another
// gateway — exactly one of them wins, never a dangling child.
func TestForeignKeyRace(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE TABLE p (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, root, `CREATE TABLE c (id INT8 PRIMARY KEY, pid INT8 REFERENCES p)`)
	for i := range tc.Nodes {
		s := sql.NewSession(tc.Nodes[i].DB(), catalog.NewAccessor())
		execSQL(t, ctx, s, `SELECT count(*) FROM c`)
	}
	retry := func(s *sql.Session, q string) *sql.Error {
		for attempt := 0; ; attempt++ {
			_, serr := trySQL(ctx, s, q)
			if serr == nil || serr.Code != sql.CodeSerializationFailure || attempt > 20 {
				return serr
			}
		}
	}
	for round := 0; round < 20; round++ {
		id := itoa(round)
		execSQL(t, ctx, root, `INSERT INTO p VALUES (`+id+`)`)
		var wg sync.WaitGroup
		var delErr, insErr *sql.Error
		wg.Add(2)
		go func() {
			defer wg.Done()
			delErr = retry(sql.NewSession(tc.Nodes[1].DB(), catalog.NewAccessor()), `DELETE FROM p WHERE id = `+id)
		}()
		go func() {
			defer wg.Done()
			insErr = retry(sql.NewSession(tc.Nodes[2].DB(), catalog.NewAccessor()), `INSERT INTO c VALUES (`+id+`, `+id+`)`)
		}()
		wg.Wait()
		if delErr != nil && delErr.Code != sql.CodeForeignKeyViolation {
			t.Fatalf("round %d: delete: %v", round, delErr)
		}
		if insErr != nil && insErr.Code != sql.CodeForeignKeyViolation {
			t.Fatalf("round %d: insert: %v", round, insErr)
		}
		parents := execSQL(t, ctx, root, `SELECT count(*) FROM p WHERE id = `+id).Rows[0][0].I
		children := execSQL(t, ctx, root, `SELECT count(*) FROM c WHERE pid = `+id).Rows[0][0].I
		if children > parents {
			t.Fatalf("round %d: dangling child (parents %d, children %d; delete %v, insert %v)", round, parents, children, delErr, insErr)
		}
		if delErr == nil && insErr == nil {
			t.Fatalf("round %d: both the delete and the insert succeeded", round)
		}
	}
}

// TestConstraintsNeedV8: a cluster still at v7 refuses constraint DDL
// while plain tables keep working.
func TestConstraintsNeedV8(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V7 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE plain (id INT8 PRIMARY KEY, n INT8)`)
	execSQL(t, ctx, s, `INSERT INTO plain VALUES (1, 1)`)
	for _, q := range []string{
		`CREATE TABLE t (id INT8 PRIMARY KEY, n INT8 CHECK (n > 0))`,
		`CREATE TABLE t (id INT8 PRIMARY KEY, n INT8 REFERENCES plain)`,
		`CREATE TABLE t (id INT8 PRIMARY KEY, n INT8 UNIQUE)`,
		`ALTER TABLE plain ADD CONSTRAINT c CHECK (n > 0)`,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
			t.Fatalf("%s: %v, want 0A000", q, serr)
		}
	}
}
