package parser

import (
	"testing"
)

// TestParseUpdateFromDeleteUsing (issue #355): the target's alias, the
// FROM / USING list as join members, VALUES members with column
// aliases, and RETURNING never taken for an alias.
func TestParseUpdateFromDeleteUsing(t *testing.T) {
	up := parseOne(t, `UPDATE orders o SET status = s.status FROM staging s WHERE o.id = s.id RETURNING *`).(*Update)
	if up.Table != "orders" || up.Alias != "o" || len(up.From) != 1 || len(up.Where) != 1 || len(up.Returning) != 1 {
		t.Fatalf("%+v", up)
	}
	if f := up.From[0]; !f.Cross || f.Table != "staging" || f.Alias != "s" {
		t.Fatalf("FROM member %+v", f)
	}
	up = parseOne(t, `UPDATE t AS a SET v = 1 FROM u, (SELECT 1 AS k) d JOIN w ON w.k = d.k LEFT JOIN x ON x.k = w.k, (VALUES (1, 'a'), (2, 'b')) AS v(k, s)`).(*Update)
	if up.Alias != "a" || len(up.From) != 5 {
		t.Fatalf("%+v", up.From)
	}
	if f := up.From[1]; !f.Cross || f.Derived == nil || f.Table != "d" || f.Alias != "d" {
		t.Fatalf("derived member %+v", f)
	}
	if f := up.From[2]; f.Cross || f.Table != "w" || len(f.On) != 1 {
		t.Fatalf("join member %+v", f)
	}
	if f := up.From[3]; !f.Left || f.Table != "x" {
		t.Fatalf("left join member %+v", f)
	}
	v := up.From[4]
	if !v.Cross || v.Derived == nil || v.Alias != "v" || v.Derived.Exprs[0].Alias != "k" || v.Derived.Exprs[1].Alias != "s" || v.Derived.Union == nil {
		t.Fatalf("VALUES member %+v", v)
	}
	up = parseOne(t, `UPDATE t SET v = 1 RETURNING v`).(*Update)
	if up.Alias != "" || up.From != nil || len(up.Returning) != 1 {
		t.Fatalf("%+v", up)
	}

	del := parseOne(t, `DELETE FROM t USING u WHERE t.uid = u.id AND u.banned`).(*Delete)
	if del.Table != "t" || del.Alias != "" || len(del.Using) != 1 || del.Using[0].Table != "u" || !del.Using[0].Cross {
		t.Fatalf("%+v", del)
	}
	del = parseOne(t, `DELETE FROM t AS x USING u, v WHERE x.a = u.a RETURNING x.a`).(*Delete)
	if del.Alias != "x" || len(del.Using) != 2 || len(del.Returning) != 1 {
		t.Fatalf("%+v", del)
	}
	del = parseOne(t, `DELETE FROM t returning *`).(*Delete)
	if del.Alias != "" || len(del.Returning) != 1 {
		t.Fatalf("RETURNING read as an alias: %+v", del)
	}
	// WITH scopes over them.
	up = parseOne(t, `WITH s AS (SELECT 1 AS id) UPDATE t SET v = s.id FROM s WHERE t.id = s.id`).(*Update)
	if len(up.With) != 1 || len(up.From) != 1 {
		t.Fatalf("%+v", up)
	}

	for _, bad := range []string{
		`UPDATE t SET v = 1 FROM`,
		`UPDATE t SET v = 1 FROM (VALUES (1))`,
		`UPDATE t SET v = 1 FROM (SELECT 1)`,
		`UPDATE t SET v = 1 FROM (VALUES (1)) AS v(a, b)`,
		`UPDATE t SET v = 1 FROM (SELECT * FROM u) AS d(a)`,
		`DELETE FROM t USING`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%s: parsed", bad)
		}
	}

	// The members reach every walker (parameters inside them count).
	stmt := parseOne(t, `UPDATE t SET v = $1 FROM (SELECT $2::int8 AS k) d JOIN u ON u.k = d.k WHERE t.k = d.k AND u.x = $3`)
	if n := CountParams(stmt); n != 3 {
		t.Fatalf("CountParams: %d", n)
	}
	if n := CountParams(parseOne(t, `DELETE FROM t USING (VALUES ($1::int8)) AS v(k) WHERE t.k = v.k`)); n != 1 {
		t.Fatalf("CountParams USING: %d", n)
	}
	// The FROM list is part of the statement's shape.
	a := Fingerprint(parseOne(t, `UPDATE t SET v = 1 FROM u WHERE t.k = u.k`))
	b := Fingerprint(parseOne(t, `UPDATE t SET v = 1 FROM w WHERE t.k = w.k`))
	c := Fingerprint(parseOne(t, `UPDATE t SET v = 1 WHERE t.k = 1`))
	if a.Hash == b.Hash || a.Hash == c.Hash || len(a.Tables) != 2 {
		t.Fatalf("fingerprints: %+v %+v %+v", a, b, c)
	}
}
