package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestAggregateOrderBy covers issue #356: ORDER BY inside an aggregate
// call (string_agg(b, ',' ORDER BY id)), with ASC / DESC and NULLS FIRST
// / LAST and their defaults, over columns the arguments do not use;
// DISTINCT with and without it (the arguments sorted, 42P10 for an ORDER
// BY expression that is no argument); FILTER; mode() and the other
// ordered-set aggregates' WITHIN GROUP; in grouped (GROUP BY
// expressions, ROLLUP) and ungrouped queries, joins, HAVING, the outer
// ORDER BY and subqueries; refused on a window aggregate as in
// PostgreSQL; Describe and parameters over the extended protocol.
// Every expected result was taken from PostgreSQL 16 (JSON rendered
// compact, as datax stores it).
func TestAggregateOrderBy(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE ev (id INT8 PRIMARY KEY, grp TEXT, b TEXT, x INT8, y INT8, j JSONB)`)
	execSQL(t, ctx, s, `INSERT INTO ev VALUES
		(1, 'a', 'p', 3, 30, '{"k":1}'), (2, 'a', 'q', 1, NULL, '{"k":2}'), (3, 'a', 'r', 2, 10, NULL),
		(4, 'b', 's', NULL, 20, '[1]'), (5, 'b', 't', 5, 20, '"x"'), (6, 'b', NULL, 4, NULL, 'true')`)
	execSQL(t, ctx, s, `CREATE TABLE g (name TEXT PRIMARY KEY, label TEXT, rank INT8)`)
	execSQL(t, ctx, s, `INSERT INTO g VALUES ('a', 'alpha', 2), ('b', 'beta', 1)`)

	runGroupedCases(t, ctx, s, []groupedCase{
		// Ungrouped: the order, by a column the arguments do not use.
		{sql: `SELECT string_agg(b, ',' ORDER BY id DESC) FROM ev`, want: "t,s,r,q,p"},
		{sql: `SELECT string_agg(b, ',' ORDER BY abs(x - 3), id) FROM ev WHERE b IS NOT NULL`, want: "p,r,q,t,s"},
		{sql: `SELECT array_agg(b ORDER BY grp DESC, b) FROM ev`, want: "{s,t,NULL,p,q,r}"},
		// NULLs: last ascending, first descending, unless written.
		{sql: `SELECT array_agg(x ORDER BY y DESC, id) FROM ev`, want: "{1,4,3,NULL,5,2}"},
		{sql: `SELECT array_agg(x ORDER BY y NULLS FIRST, id) FROM ev`, want: "{1,4,2,NULL,5,3}"},
		{sql: `SELECT array_agg(x ORDER BY y DESC NULLS LAST, id) FROM ev`, want: "{3,NULL,5,2,1,4}"},
		{sql: `SELECT array_agg(y ORDER BY y) FROM ev`, want: "{10,20,20,30,NULL,NULL}"},
		{sql: `SELECT array_agg(b ORDER BY b NULLS FIRST), array_agg(b ORDER BY b DESC), array_agg(b ORDER BY b DESC NULLS LAST) FROM ev`,
			want: "{NULL,p,q,r,s,t}|{NULL,t,s,r,q,p}|{t,s,r,q,p,NULL}"},
		// The json aggregates: NULL inputs kept, in order; the object
		// aggregates' last value for a key wins.
		{sql: `SELECT json_agg(x ORDER BY id DESC) FROM ev`, want: "[4,5,null,2,1,3]"},
		{sql: `SELECT jsonb_agg(j ORDER BY id DESC) FROM ev`, want: `[true,"x",[1],null,{"k":2},{"k":1}]`},
		{sql: `SELECT jsonb_object_agg(grp, b ORDER BY id) FROM ev`, want: `{"a":"r","b":null}`},
		{sql: `SELECT jsonb_object_agg(grp, b ORDER BY id DESC) FROM ev`, want: `{"a":"p","b":"s"}`},
		// A number is a constant, not a position; a scalar subquery is
		// a constant too.
		{sql: `SELECT string_agg(b, ',' ORDER BY 1) FROM ev`, want: "p,q,r,s,t"},
		{sql: `SELECT array_agg(x ORDER BY (SELECT 1)) FROM ev`, want: "{3,1,2,NULL,5,4}"},
		// The order changes nothing an order-blind aggregate computes.
		{sql: `SELECT count(x ORDER BY y), sum(x ORDER BY y DESC), max(b ORDER BY id) FROM ev`, want: "5|15|t"},
		{sql: `SELECT string_agg(b, ',' ORDER BY id) FROM ev WHERE id > 100`, want: "NULL"},
		// DISTINCT sorts on the arguments; ORDER BY may only name them.
		{sql: `SELECT array_agg(DISTINCT y) FROM ev`, want: "{10,20,30,NULL}"},
		{sql: `SELECT json_agg(DISTINCT x) FROM ev`, want: "[1,2,3,4,5,null]"},
		{sql: `SELECT array_agg(DISTINCT y ORDER BY y DESC) FROM ev`, want: "{NULL,30,20,10}"},
		{sql: `SELECT array_agg(DISTINCT x ORDER BY x DESC) FROM ev`, want: "{NULL,5,4,3,2,1}"},
		{sql: `SELECT array_agg(DISTINCT ev.y ORDER BY y) FROM ev`, want: "{10,20,30,NULL}"},
		{sql: `SELECT string_agg(DISTINCT b, ',' ORDER BY b DESC), string_agg(DISTINCT b, ',' ORDER BY b) FROM ev`, want: "t,s,r,q,p|p,q,r,s,t"},
		{sql: `SELECT jsonb_object_agg(DISTINCT grp, grp ORDER BY grp DESC) FROM ev`, want: `{"a":"a","b":"b"}`},
		{sql: `SELECT array_agg(DISTINCT x ORDER BY y) FROM ev`, code: "42P10"},
		{sql: `SELECT sum(DISTINCT x ORDER BY y) FROM ev`, code: "42P10"},
		// FILTER with ORDER BY.
		{sql: `SELECT count(*) FILTER (WHERE x > 1), string_agg(b, ',' ORDER BY id DESC) FILTER (WHERE x > 1) FROM ev`, want: "4|t,r,p"},
		{sql: `SELECT string_agg(x::text, ';' ORDER BY x) FILTER (WHERE grp = 'a') FROM ev`, want: "1;2;3"},
		// Grouped: by a column, an expression, ROLLUP.
		{sql: `SELECT grp, string_agg(b, '-' ORDER BY y DESC NULLS LAST, id), array_agg(id ORDER BY x * -1) FROM ev GROUP BY grp ORDER BY grp`,
			want: "a|p-r-q|{1,3,2}; b|s-t|{5,6,4}"},
		{sql: `SELECT grp, json_agg(b ORDER BY b DESC) FROM ev GROUP BY grp ORDER BY grp`, want: `a|["r","q","p"]; b|[null,"t","s"]`},
		{sql: `SELECT upper(grp), string_agg(b, '' ORDER BY b DESC) FROM ev GROUP BY upper(grp) ORDER BY 1`, want: "A|rqp; B|ts"},
		{sql: `SELECT grp, string_agg(b, ',' ORDER BY id DESC) FROM ev GROUP BY ROLLUP (grp) ORDER BY grp NULLS LAST`,
			want: "a|r,q,p; b|t,s; NULL|t,s,r,q,p"},
		// Joins: grouped, and ordered by the other side's column.
		{sql: `SELECT g.label, string_agg(e.b, ',' ORDER BY e.id DESC) FROM ev e JOIN g ON g.name = e.grp GROUP BY g.label ORDER BY g.label`,
			want: "alpha|r,q,p; beta|t,s"},
		{sql: `SELECT string_agg(g.label || e.id, ',' ORDER BY g.rank, e.id DESC) FROM ev e JOIN g ON g.name = e.grp`,
			want: "beta6,beta5,beta4,alpha3,alpha2,alpha1"},
		// HAVING and the outer ORDER BY.
		{sql: `SELECT grp FROM ev GROUP BY grp HAVING string_agg(b, '' ORDER BY id) = 'pqr'`, want: "a"},
		{sql: `SELECT grp FROM ev GROUP BY grp ORDER BY string_agg(b, '' ORDER BY b DESC) DESC`, want: "b; a"},
		// Subqueries: correlated scalar, and a derived table.
		{sql: `SELECT name, (SELECT string_agg(b, ',' ORDER BY id DESC) FROM ev WHERE ev.grp = g.name) FROM g ORDER BY name`,
			want: "a|r,q,p; b|t,s"},
		{sql: `SELECT o.id FROM ev o WHERE (SELECT string_agg(e2.b, '' ORDER BY e2.id DESC) FROM ev e2 WHERE e2.grp = o.grp) = 'rqp' ORDER BY o.id`,
			want: "1; 2; 3"},
		{sql: `SELECT id, (SELECT array_agg(g.label ORDER BY g.rank) FROM g WHERE g.rank <= ev.id) FROM ev WHERE id <= 2 ORDER BY id`,
			want: "1|{beta}; 2|{beta,alpha}"},
		{sql: `SELECT * FROM (SELECT grp, array_agg(x ORDER BY x DESC NULLS LAST) AS xs FROM ev GROUP BY grp) d ORDER BY grp`,
			want: "a|{3,2,1}; b|{5,4,NULL}"},
		// Ordered-set aggregates.
		{sql: `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x), mode() WITHIN GROUP (ORDER BY y), mode() WITHIN GROUP (ORDER BY y DESC) FROM ev`,
			want: "3|20|20"},
		{sql: `SELECT grp, mode() WITHIN GROUP (ORDER BY b) FROM ev GROUP BY grp ORDER BY grp`, want: "a|p; b|s"},
		{sql: `SELECT mode() FROM ev`, code: "42883"},
		{sql: `SELECT percentile_cont(0.5) FROM ev`, code: "42883"},
		{sql: `SELECT percentile_cont(0.5 ORDER BY x) FROM ev`, code: "42883"},
		{sql: `SELECT string_agg(b, ',') WITHIN GROUP (ORDER BY b) FROM ev`, code: "42883"},
		{sql: `SELECT string_agg(b, ',' ORDER BY b) WITHIN GROUP (ORDER BY b) FROM ev`, code: "42601"},
		// Refused: a window aggregate's ORDER BY or DISTINCT, an
		// aggregate inside, an unknown column.
		{sql: `SELECT sum(x ORDER BY y) OVER () FROM ev`, code: "0A000"},
		{sql: `SELECT id, array_agg(x ORDER BY y) OVER (PARTITION BY grp) FROM ev`, code: "0A000"},
		{sql: `SELECT count(DISTINCT x) OVER () FROM ev`, code: "0A000"},
		{sql: `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) OVER () FROM ev`, code: "0A000"},
		{sql: `SELECT array_agg(x ORDER BY count(*)) FROM ev`, code: "42803"},
		{sql: `SELECT array_agg(x ORDER BY nope) FROM ev`, code: "42703"},
		// datax difference: jsonb has no order, so ordering by one is
		// refused and DISTINCT over one keeps the input order
		// (PostgreSQL sorts jsonb).
		{sql: `SELECT array_agg(x ORDER BY j) FROM ev`, code: "0A000"},
		{sql: `SELECT jsonb_agg(DISTINCT j) FROM ev`, want: `[{"k":1},{"k":2},[1],"x",true,null]`},
	})

	if _, serr := trySQL(ctx, s, `SELECT sum(x ORDER BY y) OVER () FROM ev`); serr == nil || serr.Msg != "aggregate ORDER BY is not implemented for window functions" {
		t.Errorf("window ORDER BY message: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT array_agg(DISTINCT x ORDER BY y) FROM ev`); serr == nil ||
		serr.Msg != "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list" {
		t.Errorf("DISTINCT ORDER BY message: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT mode() FROM ev`); serr == nil || serr.Msg != "function mode() does not exist" {
		t.Errorf("mode() message: %v", serr)
	}

	// Through pgx: Describe types the ordered aggregates as the
	// unordered ones, a parameter inside the ORDER BY binds, and the
	// text the wire carries is PostgreSQL's.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	const oidInt8Array, oidJsonb = 1016, 3802
	if got := describeOIDs(t, ctx, conn, "aggorder_shapes", `SELECT string_agg(b, ',' ORDER BY id DESC), array_agg(x ORDER BY y NULLS FIRST),
		jsonb_agg(j ORDER BY id), count(*), mode() WITHIN GROUP (ORDER BY b) FROM ev`); !sameOIDs(got, []uint32{oidText, oidInt8Array, oidJsonb, oidInt8, oidText}) {
		t.Fatalf("described: %v", got)
	}
	var agg string
	if err := conn.QueryRow(ctx, `SELECT string_agg(b, ',' ORDER BY abs(x - $1), id) FROM ev WHERE b IS NOT NULL`, 3).Scan(&agg); err != nil || agg != "p,r,q,t,s" {
		t.Fatalf("parameter in ORDER BY: %q %v", agg, err)
	}
	if err := conn.QueryRow(ctx, `SELECT string_agg(b, $1 ORDER BY id DESC) FROM ev WHERE grp = $2`, "/", "a").Scan(&agg); err != nil || agg != "r/q/p" {
		t.Fatalf("parameters beside ORDER BY: %q %v", agg, err)
	}
	var texts []string
	rows, err := conn.Query(ctx, `SELECT grp, array_agg(x ORDER BY y DESC, id)::text FROM ev GROUP BY grp ORDER BY grp`, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for rows.Next() {
		var g, a string
		if err := rows.Scan(&g, &a); err != nil {
			t.Fatalf("scan: %v", err)
		}
		texts = append(texts, g+"="+a)
	}
	rows.Close()
	if rows.Err() != nil || strings.Join(texts, " ") != "a={1,3,2} b={4,NULL,5}" {
		t.Fatalf("array_agg over the wire: %v %v", texts, rows.Err())
	}
	var xs []int64
	if err := conn.QueryRow(ctx, `SELECT array_agg(x ORDER BY y DESC NULLS LAST, id) FILTER (WHERE x IS NOT NULL) FROM ev`).Scan(&xs); err != nil ||
		len(xs) != 5 || xs[0] != 3 || xs[1] != 5 || xs[2] != 2 || xs[3] != 1 || xs[4] != 4 {
		t.Fatalf("array_agg scanned: %v %v", xs, err)
	}
}
