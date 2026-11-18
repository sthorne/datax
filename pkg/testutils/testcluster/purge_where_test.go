package testcluster

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// asOf runs src (which holds AS OF SYSTEM TIME '<ts>') and returns the
// SQLSTATE it fails with, or "" and the result.
func asOf(ctx context.Context, s *sql.Session, src string) (string, *sql.Result) {
	res, serr := trySQL(ctx, s, src)
	if serr != nil {
		return serr.Code, nil
	}
	return "", res
}

// TestPurgeWhereTenant (issue #375) on three nodes: ALTER TABLE e PURGE
// WHERE tenant = 'a' removes tenant a's old versions from every
// replica's engines and refuses reads of them from before the purge
// (72000), while tenant b's history stays readable at that time, through
// the primary key and through an index that leads with the tenant. An
// index keyed by another column is purged whole, with a NOTICE.
func TestPurgeWhereTenant(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	const goneA, keptB = "TENANT-A-OLD-Kq3Zs6Wn1T", "TENANT-B-OLD-Hd5Js1Yf6G"
	execSQL(t, ctx, s, `CREATE TABLE e (tenant TEXT, id INT8, v TEXT, PRIMARY KEY (tenant, id)) WITH (gc_ttl = '1h')`)
	execSQL(t, ctx, s, `CREATE INDEX e_tenant_v ON e (tenant, v)`)
	execSQL(t, ctx, s, `CREATE INDEX e_v ON e (v)`)
	for i := 0; i < 10; i++ {
		execSQL(t, ctx, s, `INSERT INTO e VALUES ('a', $1, $2)`, intD(i), strD(goneA))
		execSQL(t, ctx, s, `INSERT INTO e VALUES ('b', $1, $2)`, intD(i), strD(keptB))
	}
	for i, eng := range engines {
		held := engineHolds(t, eng, []byte(goneA))
		for wait := time.Now().Add(30 * time.Second); !held && time.Now().Before(wait); {
			time.Sleep(50 * time.Millisecond)
			held = engineHolds(t, eng, []byte(goneA))
		}
		if !held {
			t.Fatalf("n%d: tenant a's value is not in the engine before the purge", i+1)
		}
	}
	before := strconv.FormatInt(tc.Nodes[0].Clock().Now().WallTime, 10)
	// Both tenants' first versions are superseded: each has history a
	// purge could collect.
	execSQL(t, ctx, s, `UPDATE e SET v = 'new-' || tenant`)
	execSQL(t, ctx, s, `DELETE FROM e WHERE id = 9`)

	res := execSQL(t, ctx, s, `ALTER TABLE e PURGE WHERE tenant = 'a'`)
	report := purgeRows(t, res)
	if !strings.HasPrefix(res.Tag, "PURGE ") || strings.Contains(res.Tag, "INCOMPLETE") || res.Tag == "PURGE 0" {
		t.Fatalf("purge of tenant a:\n%s", report)
	}
	nodes := map[int64]bool{}
	for _, r := range res.Rows {
		nodes[r[1].I] = true
		if r[7].S != "clean" {
			t.Fatalf("a replica is not clean:\n%s", report)
		}
	}
	if len(nodes) != 3 {
		t.Fatalf("the report names nodes %v, want all three:\n%s", nodes, report)
	}
	if len(res.Notices) != 1 || !strings.Contains(res.Notices[0], `"e_v"`) {
		t.Fatalf("notices: %q", res.Notices)
	}

	// Tenant a's history is gone, and reads of it are refused.
	for _, q := range []string{
		`SELECT v FROM e AS OF SYSTEM TIME '` + before + `' WHERE tenant = 'a'`,
		`SELECT v FROM e AS OF SYSTEM TIME '` + before + `' WHERE tenant = 'a' AND id = 3`,
	} {
		if code, res := asOf(ctx, s, q); code != sql.CodeSnapshotTooOld {
			t.Fatalf("%s: %s %v, want %s", q, code, res, sql.CodeSnapshotTooOld)
		}
	}
	// Tenant b's is not: the old values, the deleted row included.
	code, old := asOf(ctx, s, `SELECT v FROM e AS OF SYSTEM TIME '`+before+`' WHERE tenant = 'b'`)
	if code != "" || len(old.Rows) != 10 || old.Rows[0][0].S != keptB {
		t.Fatalf("tenant b's history: %s %v", code, old)
	}
	code, old = asOf(ctx, s, `SELECT count(*) FROM e AS OF SYSTEM TIME '`+before+`' WHERE tenant = 'b' AND v = '`+keptB+`'`)
	if code != "" || old.Rows[0][0].I != 10 {
		t.Fatalf("tenant b's history by (tenant, v): %s %v", code, old)
	}
	// The present reads as before for both.
	if n := countSQL(t, ctx, s, `SELECT count(*) FROM e WHERE tenant = 'a' AND v = 'new-a'`); n != 9 {
		t.Fatalf("tenant a now: %d rows", n)
	}
	if n := countSQL(t, ctx, s, `SELECT count(*) FROM e WHERE v = 'new-b'`); n != 9 {
		t.Fatalf("tenant b now: %d rows", n)
	}

	// Below the MVCC layer, on every replica: no trace of tenant a's old
	// value in either engine; tenant b's is still there.
	for i, eng := range engines {
		if engineHolds(t, eng, []byte(goneA)) || engineHolds(t, tc.RaftEngine(i), []byte(goneA)) {
			t.Errorf("n%d: an engine still holds tenant a's old value", i+1)
		}
		if !engineHolds(t, eng, []byte(keptB)) {
			t.Errorf("n%d: tenant b's history is gone from the engine", i+1)
		}
	}

	// A second purge of the same tenant has nothing left to collect.
	if res := execSQL(t, ctx, s, `ALTER TABLE e PURGE WHERE tenant = 'a'`); res.Tag != "PURGE 0" {
		t.Fatalf("second purge:\n%s", purgeRows(t, res))
	}
}

// TestPurgeWhereCompositeKey (issue #375): a prefix of a composite
// primary key, given as a row equality, and a range on the column after
// an equality prefix, each purge exactly the keys they name: the rows
// beside them, before and after, keep their history. Over pgwire the
// prefix value may be a bind parameter.
func TestPurgeWhereCompositeKey(t *testing.T) {
	tc := StartWithOptions(t, 1, noRowTTLLoop)
	n := tc.Nodes[0]
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE c (a INT8, b INT8, c INT8, v TEXT, PRIMARY KEY (a, b, c))`)
	for a := 1; a <= 3; a++ {
		for b := 1; b <= 8; b++ {
			for c := 1; c <= 2; c++ {
				execSQL(t, ctx, s, `INSERT INTO c VALUES ($1, $2, $3, 'old')`, intD(a), intD(b), intD(c))
			}
		}
	}
	before := strconv.FormatInt(n.Clock().Now().WallTime, 10)
	execSQL(t, ctx, s, `UPDATE c SET v = 'new'`)

	readable := func(a, b int) bool {
		t.Helper()
		q := `SELECT v FROM c AS OF SYSTEM TIME '` + before + `' WHERE a = ` + strconv.Itoa(a) + ` AND b = ` + strconv.Itoa(b)
		code, res := asOf(ctx, s, q)
		switch {
		case code == sql.CodeSnapshotTooOld:
			return false
		case code != "":
			t.Fatalf("%s: %s", q, code)
		case len(res.Rows) != 2 || res.Rows[0][0].S != "old":
			t.Fatalf("%s: %v", q, res.Rows)
		}
		return true
	}
	check := func(stage string, purged map[[2]int]bool) {
		t.Helper()
		for a := 1; a <= 3; a++ {
			for b := 1; b <= 8; b++ {
				if got := readable(a, b); got == purged[[2]int{a, b}] {
					t.Errorf("%s: (%d, %d) readable at the old time = %v, want %v", stage, a, b, got, !got)
				}
			}
		}
	}
	purged := map[[2]int]bool{}

	if res := execSQL(t, ctx, s, `ALTER TABLE c PURGE WHERE (a, b) = (1, 2)`); res.Tag != "PURGE 2" {
		t.Fatalf("prefix (1, 2):\n%s", purgeRows(t, res))
	}
	purged[[2]int{1, 2}] = true
	check("prefix (1, 2)", purged)

	if res := execSQL(t, ctx, s, `ALTER TABLE c PURGE WHERE a = 2 AND b BETWEEN 5 AND 6`); res.Tag != "PURGE 4" {
		t.Fatalf("a = 2, b between 5 and 6:\n%s", purgeRows(t, res))
	}
	purged[[2]int{2, 5}], purged[[2]int{2, 6}] = true, true
	check("BETWEEN", purged)

	conn := pgConn(t, ctx, tc, 0)
	tag, err := conn.Exec(ctx, `ALTER TABLE c PURGE WHERE a = $1 AND b >= $2 AND b < 3`, int64(3), int64(2))
	if err != nil || tag.String() != "PURGE 2" {
		t.Fatalf("a = $1 AND b >= $2 AND b < 3 over pgwire: %q %v", tag.String(), err)
	}
	purged[[2]int{3, 2}] = true
	check("parameters", purged)

	// The present is untouched.
	if n := countSQL(t, ctx, s, `SELECT count(*) FROM c WHERE v = 'new'`); n != 48 {
		t.Fatalf("%d current rows", n)
	}
}

// TestPurgeWhereRefusedAndPrivileges (issue #375): a predicate that does
// not name a primary-key prefix or range is refused with its SQLSTATE
// and a hint naming the key, and purges nothing; PURGE WHERE needs what
// ALTER TABLE ... PURGE needs, ownership of the table, so a role with
// every privilege on it but not owning it is refused (42501); and it
// cannot run in a transaction block.
func TestPurgeWhereRefusedAndPrivileges(t *testing.T) {
	tc := StartWithOptions(t, 1, noRowTTLLoop)
	n := tc.Nodes[0]
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE e (tenant TEXT, id INT8, v TEXT, PRIMARY KEY (tenant, id))`)
	execSQL(t, ctx, s, `INSERT INTO e VALUES ('a', 1, 'old'), ('b', 1, 'old')`)
	before := strconv.FormatInt(n.Clock().Now().WallTime, 10)
	execSQL(t, ctx, s, `UPDATE e SET v = 'new'`)

	for _, c := range []struct{ q, code string }{
		{`ALTER TABLE e PURGE WHERE v = 'old'`, sql.CodeInvalidColumnReference},
		{`ALTER TABLE e PURGE WHERE id = 1`, sql.CodeInvalidColumnReference},
		{`ALTER TABLE e PURGE WHERE tenant = 'a' OR id = 1`, sql.CodeFeatureNotSupported},
		{`ALTER TABLE e PURGE WHERE tenant = upper('a')`, sql.CodeFeatureNotSupported},
		{`ALTER TABLE e PURGE WHERE tenant <> 'a'`, sql.CodeFeatureNotSupported},
		{`ALTER TABLE e PURGE WHERE nope = 1`, sql.CodeUndefinedColumn},
	} {
		_, serr := trySQL(ctx, s, c.q)
		if serr == nil || serr.Code != c.code {
			t.Fatalf("%s: %v, want %s", c.q, serr, c.code)
		}
		if c.code != sql.CodeUndefinedColumn && !strings.Contains(serr.Hint, "(tenant, id)") {
			t.Errorf("%s: hint %q", c.q, serr.Hint)
		}
	}

	execSQL(t, ctx, s, `CREATE ROLE purger LOGIN`)
	execSQL(t, ctx, s, `GRANT ALL ON e TO purger`)
	purger := sql.NewSessionForUser(n.DB(), catalog.NewAccessor(), "purger")
	for _, q := range []string{`ALTER TABLE e PURGE WHERE tenant = 'a'`, `ALTER TABLE e PURGE`} {
		if _, serr := trySQL(ctx, purger, q); serr == nil || serr.Code != sql.CodeInsufficientPriv {
			t.Fatalf("%s by a role that does not own the table: %v, want %s", q, serr, sql.CodeInsufficientPriv)
		}
	}
	execSQL(t, ctx, s, `BEGIN`)
	if _, serr := trySQL(ctx, s, `ALTER TABLE e PURGE WHERE tenant = 'a'`); serr == nil || serr.Code != sql.CodeActiveTransaction {
		t.Fatalf("in a transaction block: %v", serr)
	}
	execSQL(t, ctx, s, `ROLLBACK`)
	if res := execSQL(t, ctx, s, `ALTER TABLE IF EXISTS no_such PURGE WHERE tenant = 'a'`); res.Tag != "ALTER TABLE" {
		t.Fatalf("IF EXISTS on a missing table: %q", res.Tag)
	}

	// None of it purged anything.
	for _, tenant := range []string{"a", "b"} {
		code, res := asOf(ctx, s, `SELECT v FROM e AS OF SYSTEM TIME '`+before+`' WHERE tenant = '`+tenant+`'`)
		if code != "" || len(res.Rows) != 1 || res.Rows[0][0].S != "old" {
			t.Fatalf("tenant %s's history after the refusals: %s %v", tenant, code, res)
		}
	}
	// The owner may.
	if res := execSQL(t, ctx, s, `ALTER TABLE e PURGE WHERE tenant = 'a'`); res.Tag != "PURGE 1" {
		t.Fatalf("the owner's purge:\n%s", purgeRows(t, res))
	}
}
