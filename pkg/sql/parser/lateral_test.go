package parser

import "testing"

// TestParseLateral: LATERAL before a FROM-list subquery or table
// function, in every join form (comma, CROSS JOIN, JOIN ... ON, LEFT
// JOIN ... ON true), marks the member lateral; a lateral JOIN's ON may
// be a bare condition with no join-key equality; LATERAL heading the
// FROM list is accepted (it has nothing to its left to read).
func TestParseLateral(t *testing.T) {
	sel := parseOne(t, `SELECT * FROM t, LATERAL (SELECT t.a + 1) l`).(*Select)
	if len(sel.Joins) != 1 || !sel.Joins[0].Lateral || !sel.Joins[0].Cross || sel.Joins[0].Derived == nil || sel.Joins[0].Alias != "l" {
		t.Fatalf("comma LATERAL: %+v", sel.Joins)
	}
	sel = parseOne(t, `SELECT u.id, o.id FROM users u CROSS JOIN LATERAL (SELECT * FROM orders o WHERE o.user_id = u.id ORDER BY created DESC LIMIT 3) AS o`).(*Select)
	if j := sel.Joins[0]; !j.Lateral || !j.Cross || j.Derived == nil || j.Derived.Limit != 3 || j.Alias != "o" {
		t.Fatalf("CROSS JOIN LATERAL: %+v", j)
	}
	sel = parseOne(t, `SELECT * FROM users u JOIN LATERAL (SELECT user_id, count(*) AS n FROM orders WHERE user_id = u.id GROUP BY user_id) c ON c.user_id = u.id`).(*Select)
	if j := sel.Joins[0]; !j.Lateral || j.Cross || len(j.On) != 1 {
		t.Fatalf("JOIN LATERAL ON: %+v", j)
	}
	sel = parseOne(t, `SELECT * FROM users u LEFT JOIN LATERAL (SELECT o.id FROM orders o WHERE o.user_id = u.id LIMIT 1) o ON true`).(*Select)
	if j := sel.Joins[0]; !j.Lateral || !j.Left || len(j.On) != 0 || len(j.Filter) != 1 {
		t.Fatalf("LEFT JOIN LATERAL ON true: %+v", j)
	}
	sel = parseOne(t, `SELECT * FROM items i LEFT JOIN LATERAL unnest(i.tags) AS x(tag) ON x.tag <> 'a'`).(*Select)
	if j := sel.Joins[0]; !j.Lateral || !j.Left || j.FuncTable == nil || j.Alias != "x" || len(j.FuncCols) != 1 || len(j.Filter) != 1 {
		t.Fatalf("LEFT JOIN LATERAL function: %+v", j)
	}
	// A table function is implicitly lateral: no keyword needed.
	sel = parseOne(t, `SELECT * FROM items i, unnest(i.tags) AS tag`).(*Select)
	if j := sel.Joins[0]; j.Lateral || j.FuncTable == nil || j.Alias != "tag" {
		t.Fatalf("implicit lateral function: %+v", j)
	}
	sel = parseOne(t, `SELECT * FROM LATERAL (SELECT 1 AS a) x, t`).(*Select)
	if sel.Derived == nil || sel.Alias != "x" || len(sel.Joins) != 1 {
		t.Fatalf("leading LATERAL: %+v", sel)
	}
	// A nested lateral parses inside the lateral subquery.
	sel = parseOne(t, `SELECT * FROM a, LATERAL (SELECT * FROM b, LATERAL (SELECT b.x + a.y AS z) c WHERE b.k = a.k) d`).(*Select)
	if inner := sel.Joins[0].Derived; inner == nil || len(inner.Joins) != 1 || !inner.Joins[0].Lateral {
		t.Fatalf("nested lateral: %+v", sel.Joins[0])
	}
	// LATERAL ahead of anything but a subquery or a function call is a
	// table named lateral.
	if sel := parseOne(t, `SELECT * FROM t, lateral l`).(*Select); sel.Joins[0].Lateral || sel.Joins[0].Table != "lateral" || sel.Joins[0].Alias != "l" {
		t.Fatalf("table named lateral: %+v", sel.Joins[0])
	}
	// An ordinary JOIN still needs a join-key equality.
	if _, err := Parse(`SELECT * FROM t JOIN (SELECT 1 AS a) s ON true`); err == nil {
		t.Fatal("non-lateral JOIN ON true: no error")
	}
	// The keyword shows in the fingerprint: a lateral and a plain
	// derived member are different statements.
	a, err := Parse(`SELECT * FROM t, LATERAL (SELECT 1 AS a) s`)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Parse(`SELECT * FROM t, (SELECT 1 AS a) s`)
	if err != nil {
		t.Fatal(err)
	}
	if Fingerprint(a[0]).Text == Fingerprint(b[0]).Text {
		t.Fatalf("fingerprints equal: %s", Fingerprint(a[0]).Text)
	}
}
