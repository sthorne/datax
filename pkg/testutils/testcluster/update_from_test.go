package testcluster

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// UPDATE ... FROM and DELETE ... USING (issue #355).

// TestUpdateFromDeleteUsing: the FROM (USING) list's forms, the
// at-most-once rule for a target row joining several rows, SET and
// RETURNING reading the list's columns, correlation, constraints, and
// the errors PostgreSQL gives.
func TestUpdateFromDeleteUsing(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	rows := func(res *sql.Result) []string {
		out := []string{}
		for _, r := range res.Rows {
			cells := make([]string, len(r))
			for i, d := range r {
				cells[i] = d.Text()
			}
			out = append(out, strings.Join(cells, "|"))
		}
		return out
	}
	want := func(q string, wantTag string, wantRows ...string) *sql.Result {
		t.Helper()
		res := execSQL(t, ctx, s, q)
		if wantTag != "" && res.Tag != wantTag {
			t.Fatalf("%s: tag %q, want %q", q, res.Tag, wantTag)
		}
		if wantRows == nil {
			wantRows = []string{}
		}
		if got := rows(res); !reflect.DeepEqual(got, wantRows) {
			t.Fatalf("%s:\n got %q\nwant %q", q, got, wantRows)
		}
		return res
	}
	refused := func(q, code string) {
		t.Helper()
		_, err := trySQL(ctx, s, q)
		if err == nil || err.Code != code {
			t.Fatalf("%s: got %v, want SQLSTATE %s", q, err, code)
		}
	}
	reset := func() {
		t.Helper()
		execSQL(t, ctx, s, `DELETE FROM orders`)
		execSQL(t, ctx, s, `DELETE FROM staging`)
		execSQL(t, ctx, s, `INSERT INTO orders VALUES (1, 'new', 1), (2, 'new', 2), (3, 'new', 3), (4, 'new', 4)`)
		execSQL(t, ctx, s, `INSERT INTO staging VALUES (1, 'paid'), (3, 'shipped'), (9, 'lost')`)
	}

	execSQL(t, ctx, s, `CREATE TABLE orders (id INT8 PRIMARY KEY, status TEXT, qty INT8)`)
	execSQL(t, ctx, s, `CREATE TABLE staging (id INT8 PRIMARY KEY, status TEXT)`)
	reset()

	// The common hand-written form: the target's rows take values from
	// the rows they join; the others are untouched.
	want(`UPDATE orders o SET status = s.status FROM staging s WHERE o.id = s.id`, "UPDATE 2")
	want(`SELECT * FROM orders ORDER BY id`, "", "1|paid|1", "2|new|2", "3|shipped|3", "4|new|4")

	// RETURNING * is the target's columns, then the FROM list's, as in
	// PostgreSQL; a target column returns its new value.
	reset()
	res := want(`UPDATE orders AS o SET status = s.status, qty = o.qty * 10 FROM staging s WHERE o.id = s.id RETURNING *`, "UPDATE 2",
		"1|paid|10|1|paid", "3|shipped|30|3|shipped")
	var names []string
	for _, c := range res.Columns {
		names = append(names, c.Name+":"+c.Type.String())
	}
	if got := strings.Join(names, ","); got != "id:INT8,status:TEXT,qty:INT8,id:INT8,status:TEXT" {
		t.Fatalf("RETURNING * columns: %s", got)
	}
	// Expressions over both sides; an unaliased target answers to its
	// name.
	reset()
	want(`UPDATE orders SET qty = orders.qty + staging.id FROM staging WHERE orders.id = staging.id AND staging.status <> 'lost' RETURNING orders.id, qty, staging.status || '/' || orders.status AS s`,
		"UPDATE 2", "1|2|paid/new", "3|6|shipped/new")

	// SET values read the row as it was: a swap through a FROM list.
	reset()
	want(`UPDATE orders o SET status = o.qty::text, qty = length(o.status) FROM staging s WHERE o.id = s.id RETURNING o.id, o.status, o.qty`,
		"UPDATE 2", "1|1|3", "3|3|3")

	// A target row joining several rows changes once, taking the first
	// joined row: the VALUES list's rows in written order.
	reset()
	want(`UPDATE orders o SET qty = o.qty + v.d FROM (VALUES (1, 10), (1, 20), (2, 5)) AS v(k, d) WHERE o.id = v.k RETURNING o.id, o.qty, v.d`,
		"UPDATE 2", "1|11|10", "2|7|5")
	reset()
	want(`UPDATE orders o SET qty = o.qty + v.d FROM (VALUES (1, 20), (1, 10)) AS v(k, d) WHERE o.id = v.k RETURNING o.qty`,
		"UPDATE 1", "21")
	// A cross join with a several-row list: every row once.
	reset()
	want(`UPDATE orders SET qty = 0 FROM staging`, "UPDATE 4")
	reset()
	want(`DELETE FROM orders USING (VALUES (1), (1), (1), (2)) AS v(k) WHERE orders.id = v.k`, "DELETE 2")
	want(`SELECT id FROM orders ORDER BY id`, "", "3", "4")

	// The FROM list's forms: a subquery, a WITH member, several members,
	// explicit joins (inner, LEFT, USING), a view.
	reset()
	want(`UPDATE orders o SET status = d.status FROM (SELECT id, upper(status) AS status FROM staging WHERE id < 5) d WHERE o.id = d.id RETURNING o.id, o.status`,
		"UPDATE 2", "1|PAID", "3|SHIPPED")
	reset()
	want(`WITH paid AS (SELECT id FROM staging WHERE status = 'paid') UPDATE orders SET status = 'done' FROM paid WHERE orders.id = paid.id RETURNING orders.id`,
		"UPDATE 1", "1")
	reset()
	want(`UPDATE orders o SET qty = a.id + b.id FROM staging a, staging b WHERE o.id = a.id AND b.id = 9 RETURNING o.id, o.qty`,
		"UPDATE 2", "1|10", "3|12")
	reset()
	want(`UPDATE orders o SET status = a.status || b.status FROM staging a JOIN staging b ON b.id = a.id WHERE o.id = a.id RETURNING o.id, o.status`,
		"UPDATE 2", "1|paidpaid", "3|shippedshipped")
	reset()
	want(`UPDATE orders o SET status = coalesce(x.status, 'none') FROM staging a LEFT JOIN orders x ON x.qty = a.id AND x.id > 1 WHERE o.id = a.id RETURNING o.id, o.status`,
		"UPDATE 2", "1|none", "3|new")
	reset()
	want(`UPDATE orders o SET qty = b.id FROM staging a JOIN staging b USING (id) WHERE o.id = a.id RETURNING o.id, o.qty`,
		"", "1|1", "3|3")
	refused(`UPDATE orders o SET qty = 1 FROM staging a JOIN staging b USING (id) WHERE o.id = id`, sql.CodeAmbiguousColumn)
	// A RIGHT JOIN inside the list keeps its unmatched rows: the target
	// is joined with the list's result, not inside it.
	reset()
	want(`UPDATE orders o SET status = coalesce(a.status, 'none') FROM staging a RIGHT JOIN orders x ON x.id = a.id WHERE o.id = x.id RETURNING o.id, o.status`,
		"UPDATE 4", "1|paid", "2|none", "3|shipped", "4|none")
	reset()
	execSQL(t, ctx, s, `CREATE VIEW paid_view AS SELECT id, status FROM staging WHERE status = 'paid'`)
	want(`UPDATE orders o SET status = p.status FROM paid_view p WHERE o.id = p.id RETURNING o.id, o.status`, "UPDATE 1", "1|paid")
	want(`DELETE FROM orders USING paid_view WHERE orders.id = paid_view.id RETURNING orders.id`, "DELETE 1", "1")

	reset()
	want(`UPDATE orders o SET qty = g.n * 7 FROM generate_series(1, 2) AS g(n) WHERE o.id = g.n RETURNING o.id, o.qty`, "UPDATE 2", "1|7", "2|14")

	// Correlation: a subquery in SET reading the target and the FROM
	// row, and one in WHERE reading the FROM row.
	reset()
	want(`UPDATE orders o SET qty = (SELECT count(*) FROM staging s2 WHERE s2.id <= s.id AND s2.id <> o.id) FROM staging s WHERE o.id = s.id RETURNING o.id, o.qty`,
		"UPDATE 2", "1|0", "3|1")
	reset()
	want(`UPDATE orders o SET status = 'seen' FROM staging s WHERE o.id = s.id AND EXISTS (SELECT 1 FROM orders o2 WHERE o2.id = s.id + 1) RETURNING o.id`,
		"UPDATE 2", "1", "3")
	reset()
	want(`UPDATE orders o SET qty = (SELECT max(id) FROM staging) FROM staging s WHERE o.id = s.id RETURNING o.qty`, "UPDATE 2", "9", "9")

	// Parameters.
	reset()
	if res := execSQL(t, ctx, s, `UPDATE orders o SET status = $2 FROM staging s WHERE o.id = s.id AND s.status = $1 RETURNING o.id, o.status`,
		types.NewString("shipped"), types.NewString("x")); !reflect.DeepEqual(rows(res), []string{"3|x"}) {
		t.Fatalf("parameters: %v", rows(res))
	}

	// The common DELETE ... USING form.
	execSQL(t, ctx, s, `CREATE TABLE users (id INT8 PRIMARY KEY, banned BOOL)`)
	execSQL(t, ctx, s, `CREATE TABLE posts (id INT8 PRIMARY KEY, uid INT8, body TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO users VALUES (1, false), (2, true), (3, true)`)
	execSQL(t, ctx, s, `INSERT INTO posts VALUES (10, 1, 'a'), (11, 2, 'b'), (12, 2, 'c'), (13, 3, 'd'), (14, NULL, 'e')`)
	want(`DELETE FROM posts t USING users u WHERE t.uid = u.id AND u.banned AND t.id < 13 RETURNING *`, "DELETE 2", "11|2|b|2|t", "12|2|c|2|t")
	want(`DELETE FROM posts USING users u WHERE posts.uid = u.id AND u.banned`, "DELETE 1")
	want(`SELECT id FROM posts ORDER BY id`, "", "10", "14")
	// No match: RETURNING still describes its columns.
	res = want(`DELETE FROM posts p USING users u WHERE p.uid = u.id AND u.id = 99 RETURNING p.id, u.banned`, "DELETE 0")
	if len(res.Columns) != 2 || res.Columns[1].Type != types.Bool {
		t.Fatalf("empty RETURNING columns: %+v", res.Columns)
	}
	// An alias on a single-table statement.
	want(`UPDATE posts AS p SET body = p.body || '!' WHERE p.id = 10 RETURNING p.body`, "UPDATE 1", "a!")
	want(`DELETE FROM posts p WHERE p.id = 14 RETURNING p.id`, "DELETE 1", "14")
	want(`UPDATE posts p SET body = 'z' WHERE EXISTS (SELECT 1 FROM users u WHERE u.id = p.uid AND NOT u.banned) RETURNING p.id`, "UPDATE 1", "10")

	// Primary keys, unique keys and foreign keys.
	execSQL(t, ctx, s, `CREATE TABLE parent (id INT8 PRIMARY KEY, code TEXT UNIQUE)`)
	execSQL(t, ctx, s, `CREATE TABLE child (id INT8 PRIMARY KEY, pid INT8 REFERENCES parent ON DELETE CASCADE ON UPDATE CASCADE, code TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE renames (old INT8 PRIMARY KEY, new INT8)`)
	execSQL(t, ctx, s, `INSERT INTO parent VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	execSQL(t, ctx, s, `INSERT INTO child VALUES (10, 1, NULL), (11, 2, NULL), (12, 2, NULL)`)
	execSQL(t, ctx, s, `INSERT INTO renames VALUES (1, 101), (2, 102)`)
	// Moving primary keys through a FROM list: the children follow.
	want(`UPDATE parent p SET id = r.new FROM renames r WHERE p.id = r.old RETURNING p.id`, "UPDATE 2", "101", "102")
	want(`SELECT id, pid FROM child ORDER BY id`, "", "10|101", "11|102", "12|102")
	// Two rows taking one key, or a key another row holds: 23505.
	refused(`UPDATE parent p SET code = 'same' FROM renames r WHERE p.id = r.new`, sql.CodeUniqueViolation)
	refused(`UPDATE parent p SET code = 'c' FROM renames r WHERE p.id = r.new AND r.old = 1`, sql.CodeUniqueViolation)
	// A child pointed at a missing parent: 23503.
	refused(`UPDATE child c SET pid = r.old FROM renames r WHERE c.pid = r.new`, sql.CodeForeignKeyViolation)
	execSQL(t, ctx, s, `UPDATE child c SET pid = 3 FROM renames r WHERE c.pid = r.new AND r.old = 1`)
	// DELETE ... USING cascades.
	want(`DELETE FROM parent p USING renames r WHERE p.id = r.new AND r.old = 2`, "DELETE 1")
	want(`SELECT id, pid FROM child ORDER BY id`, "", "10|3")
	want(`SELECT id FROM parent ORDER BY id`, "", "3", "101")

	// Errors, with PostgreSQL's SQLSTATEs.
	reset()
	refused(`UPDATE orders o SET status = id FROM staging s WHERE o.id = s.id`, sql.CodeAmbiguousColumn)
	refused(`UPDATE orders o SET status = 'x' FROM staging s WHERE o.id = id`, sql.CodeAmbiguousColumn)
	refused(`UPDATE orders o SET status = x.status FROM staging s WHERE o.id = s.id`, sql.CodeUndefinedTable)
	refused(`UPDATE orders o SET nosuch = s.status FROM staging s WHERE o.id = s.id`, sql.CodeUndefinedColumn)
	refused(`UPDATE orders o SET status = s.nosuch FROM staging s WHERE o.id = s.id`, sql.CodeUndefinedColumn)
	refused(`UPDATE orders SET status = 'x' FROM orders`, sql.CodeDuplicateTable)
	refused(`UPDATE orders o SET status = 'x' FROM nosuch n`, sql.CodeUndefinedTable)
	refused(`DELETE FROM orders o USING staging s WHERE o.id = s.id RETURNING (SELECT 1)`, sql.CodeFeatureNotSupported)
	refused(`DELETE FROM orders o USING staging s WHERE o.id = s.id RETURNING count(*)`, sql.CodeGrouping)
	refused(`UPDATE orders o SET status = 'x' FROM (VALUES (1)) WHERE true`, sql.CodeSyntaxError)
	refused(`UPDATE paid_view SET status = 'x' FROM staging`, sql.CodeWrongObjectType)
	// Nothing changed.
	want(`SELECT * FROM orders ORDER BY id`, "", "1|new|1", "2|new|2", "3|new|3", "4|new|4")

	// A self-join under another alias is allowed.
	want(`UPDATE orders o SET qty = o2.qty * 100 FROM orders o2 WHERE o2.id = o.id + 1 RETURNING o.id, o.qty`,
		"UPDATE 3", "1|200", "2|300", "3|400")
	// VALUES lists with column aliases now work in a SELECT's FROM too.
	want(`SELECT o.id, v.tag FROM orders o JOIN (VALUES (1, 'one'), (2, 'two')) AS v(id, tag) ON v.id = o.id ORDER BY o.id`, "", "1|one", "2|two")
	// A data-modifying WITH member.
	reset()
	want(`WITH moved AS (UPDATE orders o SET status = s.status FROM staging s WHERE o.id = s.id RETURNING o.id, s.status) SELECT id, status FROM moved ORDER BY id`,
		"", "1|paid", "3|shipped")
}

// TestUpdateFromDeleteUsingSecurity: the target needs UPDATE (DELETE),
// the FROM list's tables SELECT; row security filters the FROM list by
// its SELECT policies and the target by its SELECT and command policies,
// and the new rows must pass WITH CHECK.
func TestUpdateFromDeleteUsingSecurity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := StartWithOptions(t, 1, func(cfg *server.Config) { cfg.DescLeaseTTL = time.Second })
	root := rlsConn(t, ctx, tc, 0, "root")
	for _, stmt := range []string{
		`CREATE USER towner PASSWORD 'towner-pw-1'`,
		`CREATE USER alice PASSWORD 'alice-pw-1'`,
		`GRANT CREATE ON DATABASE datax TO towner`,
	} {
		rlsExec(t, ctx, root, stmt)
	}
	owner := rlsConn(t, ctx, tc, 0, "towner")
	alice := rlsConn(t, ctx, tc, 0, "alice")
	for _, stmt := range []string{
		`CREATE TABLE acct (id INT8 PRIMARY KEY, owner TEXT, v INT8)`,
		`CREATE TABLE src (id INT8 PRIMARY KEY, owner TEXT, v INT8)`,
		`INSERT INTO acct VALUES (1, 'alice', 0), (2, 'alice', 0), (3, 'bob', 0), (4, 'bob', 0)`,
		`INSERT INTO src VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'alice', 30), (4, 'bob', 40)`,
		`GRANT UPDATE, DELETE ON acct TO alice`,
	} {
		rlsExec(t, ctx, owner, stmt)
	}
	upd := `UPDATE acct a SET v = s.v FROM src s WHERE a.id = s.id`
	del := `DELETE FROM acct a USING src s WHERE a.id = s.id AND s.v > 100`

	// Privileges: SELECT on the FROM list's tables; the target needs
	// only the statement's own privilege, as a plain UPDATE ... WHERE.
	rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, upd)
	rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, del)
	rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `UPDATE acct a SET v = 1 FROM (SELECT id FROM src) s WHERE a.id = s.id`)
	rlsExec(t, ctx, owner, `GRANT SELECT ON src TO alice`)
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, err := alice.Exec(ctx, upd)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s after GRANT: %v", upd, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := rlsExec(t, ctx, alice, del); n != 0 {
		t.Fatalf("DELETE ... USING matching nothing: %d", n)
	}
	rlsWant(t, ctx, owner, []string{"1|10", "2|20", "3|30", "4|40"}, `SELECT id, v FROM acct ORDER BY id`)
	// Without UPDATE on the target: refused.
	rlsExec(t, ctx, owner, `GRANT SELECT ON acct TO alice`)
	rlsExec(t, ctx, owner, `REVOKE UPDATE ON acct FROM alice`)
	rlsSettle(t, ctx, alice, []string{"42501"}, `SELECT '42501' FROM acct WHERE id = 1`)
	refusedSoon := func(stmt string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for {
			_, err := alice.Exec(ctx, stmt)
			if rlsCode(err) == sql.CodeInsufficientPriv {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: got %v, want 42501", stmt, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	refusedSoon(upd)
	rlsExec(t, ctx, owner, `GRANT UPDATE ON acct TO alice`)

	// Row security on both tables: alice sees her src rows only, may
	// update her acct rows only, and may not give a row away.
	for _, stmt := range []string{
		`ALTER TABLE acct ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE src ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY sel ON acct FOR SELECT USING (true)`,
		`CREATE POLICY upd ON acct FOR UPDATE USING (owner = current_user)`,
		`CREATE POLICY del ON acct FOR DELETE USING (owner = current_user)`,
		`CREATE POLICY mine ON src FOR SELECT USING (owner = current_user)`,
		`UPDATE acct SET v = 0`,
	} {
		rlsExec(t, ctx, owner, stmt)
	}
	rlsSettle(t, ctx, alice, []string{"1", "3"}, `SELECT id FROM src ORDER BY id`)
	// acct 1 and 2 are hers; src 1 and 3 are visible to her: only acct 1
	// joins a visible row and may change.
	rlsWant(t, ctx, alice, []string{"1|10"}, upd+` RETURNING a.id, a.v`)
	rlsWant(t, ctx, owner, []string{"1|10", "2|0", "3|0", "4|0"}, `SELECT id, v FROM acct ORDER BY id`)
	// Every row of acct joined to a visible src row: still only hers.
	rlsWant(t, ctx, alice, []string{"1|1", "2|1"}, `UPDATE acct a SET v = s.id FROM src s WHERE s.id = 1 RETURNING a.id, a.v`)
	// WITH CHECK (the UPDATE policy's USING): a new row she could not
	// update is refused.
	rlsRefused(t, ctx, alice, sql.CodeInsufficientPriv, `UPDATE acct a SET owner = 'bob' FROM src s WHERE a.id = s.id`)
	// DELETE: its policy picks her rows.
	rlsWant(t, ctx, alice, []string{"1"}, `DELETE FROM acct a USING src s WHERE a.id = s.id RETURNING a.id`)
	rlsWant(t, ctx, owner, []string{"2", "3", "4"}, `SELECT id FROM acct ORDER BY id`)
}

// TestUpdateFromExtendedProtocol: parameters are typed from the joined
// columns, RETURNING is described before it runs, and the rows match
// the description.
func TestUpdateFromExtendedProtocol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := Start(t, 1)
	conn := rlsConn(t, ctx, tc, 0, "root")
	for _, stmt := range []string{
		`CREATE TABLE inv (id INT8 PRIMARY KEY, sku TEXT, n INT8, at TIMESTAMPTZ)`,
		`CREATE TABLE delta (sku TEXT PRIMARY KEY, d INT8, ok BOOL)`,
		`INSERT INTO inv VALUES (1, 'a', 10, now()), (2, 'b', 20, now()), (3, 'c', 30, now())`,
		`INSERT INTO delta VALUES ('a', 1, true), ('b', 2, false), ('c', 3, true)`,
	} {
		rlsExec(t, ctx, conn, stmt)
	}
	q := `UPDATE inv i SET n = i.n + d.d * $1 FROM delta d WHERE i.sku = d.sku AND d.ok = $2 RETURNING i.id, i.n, d.ok, d.sku || '!' AS tag`
	sd, err := conn.Prepare(ctx, "upd", q)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !reflect.DeepEqual(sd.ParamOIDs, []uint32{pgtype.Int8OID, pgtype.BoolOID}) {
		t.Fatalf("parameter types: %v", sd.ParamOIDs)
	}
	var got []string
	tm := pgtype.NewMap()
	for _, f := range sd.Fields {
		typ, _ := tm.TypeForOID(f.DataTypeOID)
		name := "?"
		if typ != nil {
			name = typ.Name
		}
		got = append(got, f.Name+":"+name)
	}
	if strings.Join(got, ",") != "id:int8,n:int8,ok:bool,tag:text" {
		t.Fatalf("described columns: %v", got)
	}
	rlsWant(t, ctx, conn, []string{"1|110|true|a!", "3|330|true|c!"}, q, int64(100), true)
	rlsWant(t, ctx, conn, []string{"1|210|true|a!", "3|630|true|c!"}, q, int64(100), true)
	rlsWant(t, ctx, conn, []string{"2|20"}, `DELETE FROM inv USING delta WHERE inv.sku = delta.sku AND NOT delta.ok AND delta.d = $1 RETURNING inv.id, inv.n`, int64(2))
	// Simple protocol too.
	rows, err := conn.Query(ctx, `UPDATE inv SET n = 0 FROM delta WHERE inv.sku = delta.sku RETURNING *`, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatalf("simple protocol: %v", err)
	}
	var names []string
	for _, f := range rows.FieldDescriptions() {
		names = append(names, f.Name)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil || n != 2 || strings.Join(names, ",") != "id,sku,n,at,sku,d,ok" {
		t.Fatalf("simple protocol: %d rows, columns %v, %v", n, names, err)
	}
}

// TestUpdateSetReadsOldRow: every SET of a plain UPDATE reads the row as
// it was before the statement, so SET a = b, b = a swaps and a later SET
// sees the old value of a column an earlier one changed, as in
// PostgreSQL.
func TestUpdateSetReadsOldRow(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	execSQL(t, ctx, s, `CREATE TABLE sw (id INT PRIMARY KEY, a INT, b INT)`)
	execSQL(t, ctx, s, `INSERT INTO sw VALUES (1, 10, 20), (2, 30, 40)`)
	execSQL(t, ctx, s, `UPDATE sw SET a = b, b = a`)
	execSQL(t, ctx, s, `UPDATE sw SET a = a + 1, b = a WHERE id = 2`)
	res := execSQL(t, ctx, s, `SELECT id, a, b FROM sw ORDER BY id`)
	var got []string
	for _, r := range res.Rows {
		got = append(got, r[0].Text()+"|"+r[1].Text()+"|"+r[2].Text())
	}
	if want := []string{"1|20|10", "2|41|40"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}
