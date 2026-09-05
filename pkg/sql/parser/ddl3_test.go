package parser

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// TestParseCreateTableAsLikeCommentRetype: CREATE TABLE ... AS (bare,
// with names, with a PRIMARY KEY, WITH [NO] DATA), LIKE clauses and
// their options, SELECT INTO's refusal, COMMENT ON, and ALTER COLUMN
// TYPE.
func TestParseCreateTableAsLikeCommentRetype(t *testing.T) {
	ct := parseOne(t, `CREATE TABLE big AS SELECT id, qty * 2 AS q2 FROM orders WHERE qty > 10`).(*CreateTable)
	if ct.Name != "big" || ct.As == nil || ct.As.Table != "orders" || ct.NoData || ct.AsText != "SELECT id, qty * 2 AS q2 FROM orders WHERE qty > 10" {
		t.Fatalf("create table as: %+v %q", ct, ct.AsText)
	}
	ct = parseOne(t, `CREATE TABLE IF NOT EXISTS big (a, b, PRIMARY KEY (a)) AS SELECT id, qty FROM orders WITH NO DATA`).(*CreateTable)
	if !ct.IfNotExists || strings.Join(ct.AsColumns, ",") != "a,b" || strings.Join(ct.PrimaryKey, ",") != "a" || !ct.NoData || ct.As == nil {
		t.Fatalf("create table (names) as: %+v", ct)
	}
	ct = parseOne(t, `CREATE TABLE big AS SELECT 1 WITH DATA`).(*CreateTable)
	if ct.NoData || ct.As == nil {
		t.Fatalf("with data: %+v", ct)
	}
	for _, bad := range []string{
		`CREATE TABLE t AS INSERT INTO u VALUES (1)`,
		`SELECT id INTO t2 FROM t`,
		`CREATE TABLE t AS SELECT 1 WITH NOT DATA`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
	if _, err := Parse(`SELECT id INTO t2 FROM t`); err == nil || !strings.Contains(err.Error(), "CREATE TABLE ... AS") {
		t.Fatalf("select into hint: %v", err)
	}

	ct = parseOne(t, `CREATE TABLE t2 (x INT8, LIKE t1 INCLUDING DEFAULTS INCLUDING INDEXES, y TEXT, LIKE app.t3 INCLUDING ALL EXCLUDING COMMENTS)`).(*CreateTable)
	if len(ct.Columns) != 2 || len(ct.Like) != 2 {
		t.Fatalf("like: %+v", ct)
	}
	if l := ct.Like[0]; l.Table != "t1" || !l.Defaults || !l.Indexes || l.Constraints || l.Position != 1 {
		t.Fatalf("like 1: %+v", l)
	}
	if l := ct.Like[1]; l.Table != "app.t3" || !l.Defaults || !l.Indexes || !l.Constraints || l.Comments || l.Position != 2 {
		t.Fatalf("like 2: %+v", l)
	}
	if _, err := Parse(`CREATE TABLE t2 (LIKE t1 INCLUDING NOTHING)`); err == nil {
		t.Fatal("unknown LIKE option parsed")
	}

	co := parseOne(t, `COMMENT ON TABLE orders IS 'customer orders'`).(*CommentOn)
	if co.Kind != "table" || co.Name != "orders" || co.Text == nil || *co.Text != "customer orders" {
		t.Fatalf("comment on table: %+v", co)
	}
	co = parseOne(t, `COMMENT ON COLUMN app.orders.qty IS NULL`).(*CommentOn)
	if co.Kind != "column" || co.Name != "app.orders" || co.Column != "qty" || co.Text != nil {
		t.Fatalf("comment on column: %+v", co)
	}
	co = parseOne(t, `COMMENT ON INDEX by_city IS 'lookup'`).(*CommentOn)
	if co.Kind != "index" || co.Name != "by_city" {
		t.Fatalf("comment on index: %+v", co)
	}
	if co := parseOne(t, `COMMENT ON VIEW v IS 'x'`).(*CommentOn); co.Kind != "table" || co.Name != "v" {
		t.Fatalf("comment on view: %+v", co)
	}
	for _, bad := range []string{`COMMENT ON SEQUENCE s IS 'x'`, `COMMENT ON COLUMN qty IS 'x'`, `COMMENT ON TABLE t IS 1`} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}

	at := parseOne(t, `ALTER TABLE t ALTER COLUMN n TYPE DECIMAL(10, 2)`).(*AlterTable)
	if at.SetType == nil || at.SetType.Column != "n" || at.SetType.Type != types.Decimal || at.SetType.Precision != 10 || at.SetType.Scale != 2 {
		t.Fatalf("alter column type: %+v", at.SetType)
	}
	at = parseOne(t, `ALTER TABLE t ALTER n SET DATA TYPE TEXT`).(*AlterTable)
	if at.SetType == nil || at.SetType.Type != types.String {
		t.Fatalf("set data type: %+v", at.SetType)
	}
	for _, bad := range []string{`ALTER TABLE t ALTER COLUMN n TYPE SERIAL`, `ALTER TABLE t ALTER COLUMN n TYPE TEXT USING n::text`} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
}
