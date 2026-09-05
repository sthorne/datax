package parser

import (
	"strings"
	"testing"
)

// TestParseViews: CREATE [OR REPLACE] VIEW with and without a column
// list (the query text is kept as written), DROP VIEW forms, SHOW VIEWS
// and SHOW CREATE VIEW, and the refused forms.
func TestParseViews(t *testing.T) {
	cv := parseOne(t, `CREATE VIEW  big_orders AS SELECT id, qty * 2 AS q2 FROM orders WHERE qty > 10`).(*CreateView)
	if cv.Name != "big_orders" || cv.OrReplace || len(cv.Columns) != 0 || cv.Query == nil || cv.Query.Table != "orders" {
		t.Fatalf("create view: %+v", cv)
	}
	if cv.Text != "SELECT id, qty * 2 AS q2 FROM orders WHERE qty > 10" {
		t.Fatalf("view text: %q", cv.Text)
	}
	cv = parseOne(t, `CREATE OR REPLACE VIEW app.v (a, b) AS SELECT x, y FROM t;`).(*CreateView)
	if cv.Name != "app.v" || !cv.OrReplace || strings.Join(cv.Columns, ",") != "a,b" || cv.Text != "SELECT x, y FROM t" {
		t.Fatalf("or replace: %+v", cv)
	}
	cv = parseOne(t, `CREATE VIEW v AS WITH q AS (SELECT 1 AS n) SELECT n FROM q`).(*CreateView)
	if len(cv.Query.With) != 1 || !strings.HasPrefix(cv.Text, "WITH q") {
		t.Fatalf("view over WITH: %+v %q", cv, cv.Text)
	}
	for _, bad := range []string{
		`CREATE VIEW v AS INSERT INTO t VALUES (1)`,
		`CREATE OR REPLACE TABLE v AS SELECT 1`,
		`CREATE VIEW v SELECT 1`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
	dv := parseOne(t, `DROP VIEW IF EXISTS v1, app.v2 CASCADE`).(*DropView)
	if !dv.IfExists || !dv.Cascade || strings.Join(dv.Names, ",") != "v1,app.v2" {
		t.Fatalf("drop view: %+v", dv)
	}
	if dv := parseOne(t, `DROP VIEW v RESTRICT`).(*DropView); dv.IfExists || dv.Cascade || len(dv.Names) != 1 {
		t.Fatalf("drop view restrict: %+v", dv)
	}
	if sh := parseOne(t, `SHOW VIEWS`).(*Show); sh.Kind != "views" {
		t.Fatalf("show views: %+v", sh)
	}
	if sh := parseOne(t, `SHOW CREATE VIEW v`).(*Show); sh.Kind != "create" || sh.Table != "v" {
		t.Fatalf("show create view: %+v", sh)
	}
}
