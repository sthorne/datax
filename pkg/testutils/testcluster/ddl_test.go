package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestDDLCompleteness covers the first DDL set of #95: DROP INDEX (with
// the post-commit keyspace reclaim), ALTER INDEX RENAME, ALTER TABLE
// RENAME TO / RENAME COLUMN / RENAME CONSTRAINT across two leased
// gateways, ALTER COLUMN SET / DROP DEFAULT (fill-on-read columns
// included), TRUNCATE (multi-range, indexes, AS OF below it, foreign
// keys, CASCADE, RESTART IDENTITY, inside a transaction), IF [NOT]
// EXISTS on every form, and the error codes.
func TestDDLCompleteness(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	const ttl = 3 * time.Second
	s := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 1, ttl)
	waitForDatabases(t, ctx, s)
	db := tc.Nodes[0].DB()

	count := func(sess *sql.Session, q string) int64 {
		t.Helper()
		r := execSQL(t, ctx, sess, q)
		if len(r.Rows) != 1 {
			t.Fatalf("%s: %+v", q, r.Rows)
		}
		return r.Rows[0][0].I
	}
	expectCode := func(q, code string) *sql.Error {
		t.Helper()
		_, serr := trySQL(ctx, s, q)
		if serr == nil || serr.Code != code {
			t.Fatalf("%s: want %s, got %v", q, code, serr)
		}
		return serr
	}
	rowsText := func(r *sql.Result) string {
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
	spanKeys := func(tableID, indexID uint64) int {
		t.Helper()
		lo, hi := keys.TableIndexSpan(tableID, indexID)
		kvs, err := db.Scan(ctx, lo, hi, 0)
		if err != nil {
			t.Fatal(err)
		}
		return len(kvs)
	}

	// ---- DROP INDEX ----
	execSQL(t, ctx, s, `CREATE TABLE users (id INT8 PRIMARY KEY, email TEXT, city TEXT, code TEXT, CONSTRAINT users_code_key UNIQUE (code))`)
	execSQL(t, ctx, s, `INSERT INTO users VALUES (1, 'a@x', 'oslo', 'c1'), (2, 'b@x', 'bergen', 'c2'), (3, 'c@x', 'oslo', 'c3')`)
	execSQL(t, ctx, s, `CREATE INDEX by_city ON users (city)`)
	desc := lookupDescriptor(t, ctx, db, "users")
	byCity, ok := desc.Index("by_city")
	if !ok || spanKeys(desc.ID, byCity.ID) != 3 {
		t.Fatalf("by_city before drop: %+v, %d keys", byCity, spanKeys(desc.ID, byCity.ID))
	}
	if p := explainPlan(t, ctx, s, `SELECT id FROM users WHERE city = 'oslo'`); !strings.Contains(p, `index "by_city"`) {
		t.Fatalf("plan before drop: %s", p)
	}
	execSQL(t, ctx, s, `CREATE INDEX IF NOT EXISTS by_city ON users (email)`) // no-op, not a rebuild
	if d := lookupDescriptor(t, ctx, db, "users"); len(d.Indexes) != 2 {
		t.Fatalf("IF NOT EXISTS created an index: %+v", d.Indexes)
	}
	expectCode(`CREATE INDEX by_city ON users (email)`, sql.CodeDuplicateObject)
	expectCode(`DROP INDEX users_code_key`, sql.CodeDependentObjectsExist)
	expectCode(`DROP INDEX nope`, sql.CodeUndefinedObject)
	execSQL(t, ctx, s, `DROP INDEX IF EXISTS nope`)
	// Inside a transaction: rolled back with it, committed with it, and
	// the entries are reclaimed only after the commit.
	execSQL(t, ctx, s, `BEGIN; DROP INDEX by_city; ROLLBACK`)
	if _, ok := lookupDescriptor(t, ctx, db, "users").Index("by_city"); !ok {
		t.Fatal("rolled-back DROP INDEX took effect")
	}
	execSQL(t, ctx, s, `BEGIN; DROP INDEX by_city`)
	if n := spanKeys(desc.ID, byCity.ID); n != 3 {
		t.Fatalf("index wiped before commit: %d keys", n)
	}
	execSQL(t, ctx, s, `COMMIT`)
	if _, ok := lookupDescriptor(t, ctx, db, "users").Index("by_city"); ok {
		t.Fatal("DROP INDEX did not drop")
	}
	if n := spanKeys(desc.ID, byCity.ID); n != 0 {
		t.Fatalf("index entries after DROP INDEX: %d", n)
	}
	if p := explainPlan(t, ctx, s, `SELECT id FROM users WHERE city = 'oslo'`); p != "full table scan" {
		t.Fatalf("plan after drop: %s", p)
	}
	if n := count(sB, `SELECT count(*) FROM users WHERE city = 'oslo'`); n != 2 {
		t.Fatalf("other gateway after drop: %d", n)
	}
	execSQL(t, ctx, s, `CREATE INDEX by_city ON users (city)`) // the name is free again

	// ---- ALTER INDEX RENAME ----
	execSQL(t, ctx, s, `ALTER INDEX by_city RENAME TO by_town`)
	if p := explainPlan(t, ctx, s, `SELECT id FROM users WHERE city = 'oslo'`); !strings.Contains(p, `index "by_town"`) {
		t.Fatalf("plan after rename: %s", p)
	}
	expectCode(`ALTER INDEX by_city RENAME TO x`, sql.CodeUndefinedObject)
	execSQL(t, ctx, s, `ALTER INDEX IF EXISTS by_city RENAME TO x`)
	expectCode(`ALTER INDEX by_town RENAME TO users_code_key`, sql.CodeDuplicateObject)
	expectCode(`ALTER INDEX by_town RENAME TO primary`, sql.CodeSyntaxError)
	// A constraint's index renames the constraint with it.
	execSQL(t, ctx, s, `ALTER INDEX users_code_key RENAME TO users_code_uniq`)
	if r := execSQL(t, ctx, s, `SHOW CREATE TABLE users`); !strings.Contains(r.Rows[0][1].S, "CONSTRAINT users_code_uniq UNIQUE (code)") {
		t.Fatalf("SHOW CREATE TABLE after index rename: %s", r.Rows[0][1].S)
	}
	expectCode(`INSERT INTO users VALUES (4, 'd@x', 'oslo', 'c1')`, sql.CodeUniqueViolation)

	// ---- RENAME TABLE across gateways ----
	execSQL(t, ctx, s, `CREATE TABLE orders (id SERIAL PRIMARY KEY, uid INT8 REFERENCES users ON DELETE CASCADE, qty INT8 CHECK (qty > 0), note TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO orders (uid, qty) VALUES (1, 5), (2, 6)`)
	if n := count(sB, `SELECT count(*) FROM users`); n != 3 { // B leases users
		t.Fatalf("B's read: %d", n)
	}
	expectCode(`ALTER TABLE users RENAME TO orders`, sql.CodeDuplicateTable)
	expectCode(`ALTER TABLE users RENAME TO other.people`, sql.CodeSyntaxError) // one identifier: no database move
	expectCode(`ALTER TABLE users RENAME TO pg_class`, sql.CodeInsufficientPriv)
	execSQL(t, ctx, s, `ALTER TABLE users RENAME TO people`)
	if _, serr := trySQL(ctx, sB, `SELECT count(*) FROM users`); serr == nil || serr.Code != sql.CodeUndefinedTable {
		t.Fatalf("B still resolves the old name: %v", serr)
	}
	if n := count(sB, `SELECT count(*) FROM people WHERE city = 'oslo'`); n != 2 {
		t.Fatalf("B's read of the new name: %d", n)
	}
	// Foreign keys, owned sequences and the unique constraint follow the ID.
	expectCode(`INSERT INTO orders (uid, qty) VALUES (99, 1)`, sql.CodeForeignKeyViolation)
	execSQL(t, ctx, s, `DELETE FROM people WHERE id = 2`)
	if n := count(s, `SELECT count(*) FROM orders`); n != 1 {
		t.Fatalf("cascade through renamed parent: %d", n)
	}
	execSQL(t, ctx, s, `INSERT INTO people VALUES (2, 'b@x', 'bergen', 'c2')`)
	// (The refused insert above drew a sequence value: gaps are normal.)
	if r := execSQL(t, ctx, s, `INSERT INTO orders (uid, qty) VALUES (2, 7) RETURNING id`); r.Rows[0][0].I < 3 {
		t.Fatalf("SERIAL after parent rename: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, s, `SHOW TABLES`); !strings.Contains(rowsText(r), "people") || strings.Contains(rowsText(r), "users") {
		t.Fatalf("SHOW TABLES after rename: %s", rowsText(r))
	}

	// ---- RENAME COLUMN ----
	execSQL(t, ctx, s, `CREATE INDEX orders_by_qty ON orders (qty)`)
	expectCode(`ALTER TABLE orders RENAME COLUMN nope TO x`, sql.CodeUndefinedColumn)
	expectCode(`ALTER TABLE orders RENAME COLUMN qty TO note`, sql.CodeDuplicateObject)
	execSQL(t, ctx, s, `ALTER TABLE orders RENAME COLUMN qty TO amount`)
	expectCode(`SELECT qty FROM orders`, sql.CodeUndefinedColumn)
	if r := execSQL(t, ctx, s, `SHOW CREATE TABLE orders`); !strings.Contains(r.Rows[0][1].S, "CHECK (amount > 0)") || !strings.Contains(r.Rows[0][1].S, "amount INT8") {
		t.Fatalf("SHOW CREATE TABLE after column rename: %s", r.Rows[0][1].S)
	}
	expectCode(`INSERT INTO orders (uid, amount) VALUES (1, 0)`, sql.CodeCheckViolation) // the CHECK followed
	if p := explainPlan(t, ctx, sB, `SELECT id FROM orders WHERE amount = 5`); !strings.Contains(p, `index "orders_by_qty"`) {
		t.Fatalf("index on renamed column: %s", p)
	}
	if n := count(sB, `SELECT count(*) FROM orders WHERE amount >= 5`); n != 2 {
		t.Fatalf("read through renamed column: %d", n)
	}
	execSQL(t, ctx, s, `ALTER TABLE people RENAME id TO pid`) // a primary-key column
	if n := count(s, `SELECT count(*) FROM people WHERE pid = 1`); n != 1 {
		t.Fatalf("renamed PK column: %d", n)
	}
	execSQL(t, ctx, s, `BEGIN; ALTER TABLE people RENAME COLUMN pid TO id; ROLLBACK`)
	if n := count(s, `SELECT count(*) FROM people WHERE pid = 1`); n != 1 {
		t.Fatal("rolled-back RENAME COLUMN took effect")
	}

	// ---- RENAME CONSTRAINT ----
	execSQL(t, ctx, s, `ALTER TABLE orders RENAME CONSTRAINT orders_qty_check TO amount_positive`)
	expectCode(`ALTER TABLE orders RENAME CONSTRAINT orders_qty_check TO x`, sql.CodeUndefinedObject)
	expectCode(`ALTER TABLE orders RENAME CONSTRAINT orders_pkey TO x`, sql.CodeFeatureNotSupported)
	if serr := expectCode(`INSERT INTO orders (uid, amount) VALUES (1, -1)`, sql.CodeCheckViolation); !strings.Contains(serr.Msg, "amount_positive") {
		t.Fatalf("violation names the renamed constraint: %s", serr.Msg)
	}
	execSQL(t, ctx, s, `ALTER TABLE people RENAME CONSTRAINT users_code_uniq TO people_code_key`)
	if d := lookupDescriptor(t, ctx, db, "people"); func() bool { _, ok := d.Index("people_code_key"); return !ok }() {
		t.Fatalf("unique index did not follow the constraint: %+v", d.Indexes)
	}
	execSQL(t, ctx, s, `ALTER TABLE orders DROP CONSTRAINT amount_positive`)
	execSQL(t, ctx, s, `INSERT INTO orders (uid, amount) VALUES (1, -1)`)

	// ---- SET / DROP DEFAULT ----
	execSQL(t, ctx, s, `CREATE TABLE cfg (id INT8 PRIMARY KEY, c TEXT DEFAULT 'a', k INT8 GENERATED ALWAYS AS IDENTITY)`)
	execSQL(t, ctx, s, `INSERT INTO cfg (id) VALUES (1)`)
	execSQL(t, ctx, s, `ALTER TABLE cfg ALTER COLUMN c SET DEFAULT 'b'`)
	execSQL(t, ctx, s, `INSERT INTO cfg (id) VALUES (2)`)
	execSQL(t, ctx, s, `ALTER TABLE cfg ALTER c SET DEFAULT lower('X') || 'y'`)
	execSQL(t, ctx, s, `INSERT INTO cfg (id) VALUES (3)`)
	execSQL(t, ctx, s, `ALTER TABLE cfg ALTER COLUMN c DROP DEFAULT`)
	execSQL(t, ctx, s, `INSERT INTO cfg (id) VALUES (4)`)
	execSQL(t, ctx, s, `INSERT INTO cfg (id, c) VALUES (5, DEFAULT)`)
	if r := execSQL(t, ctx, s, `SELECT c FROM cfg ORDER BY id`); rowsText(r) != "a;b;xy;NULL;NULL" {
		t.Fatalf("defaults over time: %s", rowsText(r))
	}
	expectCode(`ALTER TABLE cfg ALTER COLUMN k SET DEFAULT 1`, sql.CodeSyntaxError)
	expectCode(`ALTER TABLE cfg ALTER COLUMN id SET DEFAULT 'x'`, sql.CodeSyntaxError)
	expectCode(`ALTER TABLE cfg ALTER COLUMN nope SET DEFAULT 1`, sql.CodeUndefinedColumn)
	// A column added with a DEFAULT keeps filling its pre-existing rows
	// from the original constant, whatever the default becomes.
	execSQL(t, ctx, s, `ALTER TABLE cfg ADD COLUMN d INT8 DEFAULT 7`)
	execSQL(t, ctx, s, `ALTER TABLE cfg ALTER COLUMN d SET DEFAULT 9`)
	execSQL(t, ctx, s, `INSERT INTO cfg (id) VALUES (6)`)
	if r := execSQL(t, ctx, s, `SELECT column_default FROM information_schema.columns WHERE table_name = 'cfg' AND column_name = 'd'`); r.Rows[0][0].S != "9" {
		t.Fatalf("column_default after SET DEFAULT on a filled column: %+v", r.Rows)
	}
	execSQL(t, ctx, s, `ALTER TABLE cfg ALTER COLUMN d DROP DEFAULT`)
	execSQL(t, ctx, s, `INSERT INTO cfg (id) VALUES (7)`)
	execSQL(t, ctx, s, `UPDATE cfg SET d = DEFAULT WHERE id = 5`)
	if r := execSQL(t, ctx, sB, `SELECT d FROM cfg ORDER BY id`); rowsText(r) != "7;7;7;7;NULL;9;NULL" {
		t.Fatalf("fill value vs. insert default: %s", rowsText(r))
	}
	if r := execSQL(t, ctx, s, `SELECT column_default, is_nullable FROM information_schema.columns WHERE table_name = 'cfg' AND column_name = 'd'`); !r.Rows[0][0].Null {
		t.Fatalf("column_default after DROP DEFAULT on a filled column: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, s, `SHOW CREATE TABLE cfg`); strings.Contains(r.Rows[0][1].S, "DEFAULT") {
		t.Fatalf("SHOW CREATE TABLE after DROP DEFAULT: %s", r.Rows[0][1].S)
	}

	// ---- TRUNCATE ----
	execSQL(t, ctx, s, `CREATE TABLE big (id INT8 PRIMARY KEY, v INT8, k SERIAL)`)
	execSQL(t, ctx, s, `CREATE INDEX big_by_v ON big (v)`)
	for i := 0; i < 200; i += 50 {
		var vals []string
		for j := i; j < i+50; j++ {
			vals = append(vals, fmt.Sprintf("(%d, %d)", j, j%10))
		}
		execSQL(t, ctx, s, `INSERT INTO big (id, v) VALUES `+strings.Join(vals, ", "))
	}
	if _, err := db.AdminSplit(ctx, tablePKKey(t, ctx, tc, "big", 100)); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `CREATE TABLE lines (id INT8 PRIMARY KEY, big_id INT8 REFERENCES big)`)
	execSQL(t, ctx, s, `INSERT INTO lines VALUES (1, 1), (2, 2)`)
	before := lookupDescriptor(t, ctx, db, "big")
	beforeTS := tc.Nodes[0].Clock().Now().WallTime
	time.Sleep(10 * time.Millisecond)
	expectCode(`TRUNCATE big`, sql.CodeFeatureNotSupported)        // lines references it
	expectCode(`TRUNCATE datax_metrics`, sql.CodeInsufficientPriv) // the cluster's table
	execSQL(t, ctx, s, `TRUNCATE TABLE big, lines RESTART IDENTITY`)
	if n := count(sB, `SELECT count(*) FROM big`); n != 0 {
		t.Fatalf("rows after TRUNCATE: %d", n)
	}
	if n := count(s, `SELECT count(*) FROM big WHERE v = 3`); n != 0 {
		t.Fatalf("index entries after TRUNCATE: %d", n)
	}
	if n := count(s, `SELECT count(*) FROM lines`); n != 0 {
		t.Fatalf("second table after TRUNCATE: %d", n)
	}
	if n := count(s, fmt.Sprintf(`SELECT count(*) FROM big AS OF SYSTEM TIME '%d'`, beforeTS)); n != 200 {
		t.Fatalf("historical read below TRUNCATE: %d", n)
	}
	after := lookupDescriptor(t, ctx, db, "big")
	if after.LivePrimaryIndex() == before.LivePrimaryIndex() || len(after.RetiredLayouts) != 1 ||
		after.RetiredLayouts[0].PrimaryIndexID != before.LivePrimaryIndex() || len(after.RetiredLayouts[0].IndexIDs) != 1 ||
		after.Indexes[0].ID == before.Indexes[0].ID {
		t.Fatalf("layout after TRUNCATE: before %d/%v, after %d/%v retired %+v", before.LivePrimaryIndex(), before.Indexes, after.LivePrimaryIndex(), after.Indexes, after.RetiredLayouts)
	}
	if n := spanKeys(before.ID, before.LivePrimaryIndex()); n != 200 {
		t.Fatalf("retired layout reclaimed early: %d keys", n)
	}
	if r := execSQL(t, ctx, s, `INSERT INTO big (id, v) VALUES (1000, 1) RETURNING k`); r.Rows[0][0].I != 1 {
		t.Fatalf("RESTART IDENTITY: %+v", r.Rows)
	}
	if n := count(sB, `SELECT count(*) FROM big WHERE v = 1`); n != 1 {
		t.Fatalf("index after TRUNCATE + insert: %d", n)
	}
	// CASCADE truncates the referencing table too.
	execSQL(t, ctx, s, `INSERT INTO lines VALUES (1, 1000)`)
	execSQL(t, ctx, s, `TRUNCATE big CASCADE`)
	if n := count(s, `SELECT count(*) FROM lines`) + count(s, `SELECT count(*) FROM big`); n != 0 {
		t.Fatalf("CASCADE: %d rows left", n)
	}
	// Inside a transaction: rolled back with it, or committed with the
	// rest of it.
	execSQL(t, ctx, s, `INSERT INTO big (id, v) VALUES (1, 1), (2, 2)`)
	execSQL(t, ctx, s, `BEGIN; TRUNCATE big CASCADE`)
	if n := count(s, `SELECT count(*) FROM big`); n != 0 {
		t.Fatalf("TRUNCATE inside the transaction: %d", n)
	}
	execSQL(t, ctx, s, `ROLLBACK`)
	if n := count(sB, `SELECT count(*) FROM big`); n != 2 {
		t.Fatalf("rolled-back TRUNCATE: %d", n)
	}
	execSQL(t, ctx, s, `BEGIN; INSERT INTO big (id, v) VALUES (3, 3); TRUNCATE big CASCADE; INSERT INTO big (id, v) VALUES (4, 4); COMMIT`)
	if r := execSQL(t, ctx, sB, `SELECT id FROM big ORDER BY id`); rowsText(r) != "4" {
		t.Fatalf("committed TRUNCATE: %s", rowsText(r))
	}

	// ---- IF [NOT] EXISTS everywhere ----
	for _, q := range []string{
		`ALTER TABLE IF EXISTS nope RENAME TO x`,
		`ALTER TABLE IF EXISTS nope ADD CONSTRAINT c CHECK (v > 0)`,
		`ALTER TABLE IF EXISTS nope ALTER COLUMN v SET NOT NULL`,
		`ALTER TABLE big ADD COLUMN IF NOT EXISTS v INT8`,
		`ALTER TABLE big DROP COLUMN IF EXISTS nope`,
		`CREATE USER IF NOT EXISTS ann PASSWORD 'pw12345'`,
		`CREATE USER IF NOT EXISTS ann PASSWORD 'pw12345'`,
		`DROP USER IF EXISTS ann`,
		`DROP USER IF EXISTS ann`,
		`ALTER SEQUENCE IF EXISTS nope RESTART`,
		`DROP SEQUENCE IF EXISTS nope`,
	} {
		execSQL(t, ctx, s, q)
	}
	expectCode(`ALTER TABLE nope RENAME TO x`, sql.CodeUndefinedTable)
	expectCode(`ALTER TABLE big ADD COLUMN v INT8`, sql.CodeDuplicateObject)
	expectCode(`ALTER TABLE big DROP COLUMN nope`, sql.CodeUndefinedColumn)
	if d := lookupDescriptor(t, ctx, db, "big"); len(d.Columns) != 3 {
		t.Fatalf("IF NOT EXISTS changed the columns: %+v", d.Columns)
	}
}

// TestTruncateReclaim: the layout TRUNCATE retires stays on disk for
// historical reads and is reclaimed by the re-shard janitor once it ages
// past the keep window.
func TestTruncateReclaim(t *testing.T) {
	n, _ := startGCNodeCfg(t, func(c *server.Config) {
		c.ReshardRetireFor = 50 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE h (id INT8 PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, s, `CREATE INDEX h_by_v ON h (v)`)
	execSQL(t, ctx, s, `INSERT INTO h VALUES (1, 10), (2, 20), (3, 30)`)
	before := lookupDescriptor(t, ctx, n.DB(), "h")
	execSQL(t, ctx, s, `TRUNCATE h`)
	execSQL(t, ctx, s, `INSERT INTO h VALUES (4, 40)`)

	scan := func(indexID uint64) int {
		lo, hi := keys.TableIndexSpan(before.ID, indexID)
		kvs, err := n.DB().Scan(ctx, lo, hi, 0)
		if err != nil {
			t.Fatal(err)
		}
		return len(kvs)
	}
	if scan(before.LivePrimaryIndex()) != 3 || scan(before.Indexes[0].ID) != 3 {
		t.Fatalf("retired layout: %d primary, %d index keys", scan(before.LivePrimaryIndex()), scan(before.Indexes[0].ID))
	}
	time.Sleep(100 * time.Millisecond)
	n.RunReshardJanitorOnce(ctx)
	if d := lookupDescriptor(t, ctx, n.DB(), "h"); len(d.RetiredLayouts) != 0 {
		t.Fatalf("retired layouts after janitor: %+v", d.RetiredLayouts)
	}
	if scan(before.LivePrimaryIndex()) != 0 || scan(before.Indexes[0].ID) != 0 {
		t.Fatalf("layout not reclaimed: %d primary, %d index keys", scan(before.LivePrimaryIndex()), scan(before.Indexes[0].ID))
	}
	if r := execSQL(t, ctx, s, `SELECT id FROM h WHERE v = 40`); len(r.Rows) != 1 || r.Rows[0][0].I != 4 {
		t.Fatalf("live layout after janitor: %+v", r.Rows)
	}
}
