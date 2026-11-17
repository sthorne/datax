package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestJSONBConcatOperator (issue #342): jsonb || jsonb merges objects,
// concatenates arrays and wraps non-arrays as PostgreSQL does — over
// literals and columns, in an UPDATE that persists the result, with
// NULL operands — and describes as jsonb; json || json merges too (json is
// jsonb in datax, unlike PostgreSQL), and the other uses of || are unchanged.
func TestJSONBConcatOperator(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	runExprCases(t, ctx, s, []exprCase{
		// object || object: keys merge, the right side wins.
		{expr: `'{"a":1}'::jsonb || '{"b":2}'::jsonb`, want: `{"a":1,"b":2}`},
		{expr: `'{"a":1,"b":2}'::jsonb || '{"b":"x"}'::jsonb`, want: `{"a":1,"b":"x"}`},
		{expr: `'{"a":{"x":1}}'::jsonb || '{"a":{"y":2}}'`, want: `{"a":{"y":2}}`},
		// array || array concatenates; array || non-array appends;
		// non-array || array prepends.
		{expr: `'[1,2]'::jsonb || '[3,4]'::jsonb`, want: `[1,2,3,4]`},
		{expr: `'[1,2]'::jsonb || '3'::jsonb`, want: `[1,2,3]`},
		{expr: `'[1]'::jsonb || '{"a":1}'::jsonb`, want: `[1,{"a":1}]`},
		{expr: `'"x"'::jsonb || '[1]'::jsonb`, want: `["x",1]`},
		{expr: `'{"a":1}'::jsonb || '[1]'::jsonb`, want: `[{"a":1},1]`},
		// Two non-arrays (not both objects) make a two-element array.
		{expr: `'1'::jsonb || '"y"'::jsonb`, want: `[1,"y"]`},
		{expr: `'{"a":1}'::jsonb || '2'::jsonb`, want: `[{"a":1},2]`},
		// An untyped literal beside a jsonb is jsonb, on either side.
		{expr: `'{"a":1}' || '{"b":2}'::jsonb`, want: `{"a":1,"b":2}`},
		{expr: `'{"a":1}'::jsonb || 'nope'`, code: "22P02"},
		// A path result is jsonb too.
		{expr: `'{"a":{"x":1}}'::jsonb -> 'a' || '{"y":2}'`, want: `{"x":1,"y":2}`},
		// NULL operands: the result is NULL.
		{expr: `'{"a":1}'::jsonb || NULL`, want: "NULL"},
		{expr: `NULL || '{"a":1}'::jsonb`, want: "NULL"},
		{expr: `NULL::jsonb || '{"a":1}'::jsonb`, want: "NULL"},
		// json is jsonb in datax, so json || json merges (PostgreSQL has
		// no || on json: a deliberate difference).
		{expr: `'{"a":1}'::json || '{"b":2}'::json`, want: `{"a":1,"b":2}`},
		{expr: `'{"a":1}'::json || '{"b":2}'::jsonb`, want: `{"a":1,"b":2}`},
		{expr: `'{"a":1}'::jsonb || '{"b":2}'::json`, want: `{"a":1,"b":2}`},
		// jsonb || a non-text scalar has no operator either.
		{expr: `'{"a":1}'::jsonb || 5`, code: "42883"},
		// jsonb || text is anynonarray || text: the texts concatenate.
		{expr: `'{"a":1}'::jsonb || 'x'::text`, want: `{"a":1}x`},
		{expr: `'x'::text || '{"a":1}'::jsonb`, want: `x{"a":1}`},
		// The other uses of || are unchanged.
		{expr: `'ab' || 'cd'`, want: "abcd"},
		{expr: `'{"a":1}' || '{"b":2}'`, want: `{"a":1}{"b":2}`},
		{expr: `'a' || 1`, want: "a1"},
		{expr: `ARRAY[1,2] || 3`, want: "{1,2,3}"},
		{expr: `'a'::tsvector || 'b'::tsvector`, want: "'a' 'b'"},
		{expr: `'ab' || NULL`, want: "NULL"},
	})

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE docs (id INT8 PRIMARY KEY, j JSONB, s TEXT)`,
		`INSERT INTO docs VALUES (1, '{"a":1,"z":0}', 'x'), (2, '[1]', 'y'), (3, NULL, 'z')`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	cells := func(q string, args ...any) string {
		t.Helper()
		got, err := queryCells(t, ctx, conn, q, append([]any{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return got
	}
	// Column operands.
	if got := cells(`SELECT j || '{"z":1}' FROM docs ORDER BY id`); got != `{"a":1,"z":1};[1,{"z":1}];NULL` {
		t.Errorf("j || literal: %s", got)
	}
	if got := cells(`SELECT '{"b":2}' || j FROM docs ORDER BY id`); got != `{"a":1,"b":2,"z":0};[{"b":2},1];NULL` {
		t.Errorf("literal || j: %s", got)
	}
	if got := cells(`SELECT a.j || b.j FROM docs a, docs b WHERE a.id = 1 AND b.id = 2`); got != `[{"a":1,"z":0},1]` {
		t.Errorf("column || column: %s", got)
	}
	// A text column beside a jsonb concatenates the texts (anynonarray || text).
	if got := cells(`SELECT j || s FROM docs WHERE id = 1`); got != `{"a":1,"z":0}x` {
		t.Errorf("j || text column: %s", got)
	}
	// A parameter is untyped: it binds as jsonb.
	if got := cells(`SELECT j || $1 FROM docs WHERE id = 1`, `{"p":true}`); got != `{"a":1,"p":true,"z":0}` {
		t.Errorf("j || $1: %s", got)
	}

	// The result describes as jsonb (RowDescription), including with a
	// NULL or parameter operand; jsonb || text stays text.
	const oidText, oidJSONB = 25, 3802
	for i, c := range []struct {
		q    string
		want []uint32
	}{
		{`SELECT '{"a":1}'::jsonb || '{"b":2}'::jsonb, j || '{"z":1}', j || j, j || NULL, j || $1 FROM docs`, []uint32{oidJSONB, oidJSONB, oidJSONB, oidJSONB, oidJSONB}},
		{`SELECT j || s, s || j, 'a' || 'b' FROM docs`, []uint32{oidText, oidText, oidText}},
	} {
		got := describeOIDs(t, ctx, conn, "jc"+itoa(i), c.q)
		if len(got) != len(c.want) {
			t.Fatalf("%s: %d columns, want %d", c.q, len(got), len(c.want))
		}
		for j := range got {
			if got[j] != c.want[j] {
				t.Errorf("%s: column %d describes as OID %d, want %d", c.q, j+1, got[j], c.want[j])
			}
		}
	}
	// And scans into a map.
	var m map[string]any
	if err := conn.QueryRow(ctx, `SELECT j || '{"b":2}' FROM docs WHERE id = 1`).Scan(&m); err != nil || len(m) != 3 || m["b"] != float64(2) {
		t.Errorf("scan jsonb result: %v %v", m, err)
	}

	// UPDATE persists the merged value, from a literal and a parameter;
	// a NULL document stays NULL.
	if _, err := conn.Exec(ctx, `UPDATE docs SET j = j || '{"z":2,"n":[1]}'`); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE docs SET j = j || $1 WHERE id = 1`, `{"p":null}`); err != nil {
		t.Fatalf("update with a parameter: %v", err)
	}
	if got := cells(`SELECT j FROM docs ORDER BY id`); got != `{"a":1,"n":[1],"p":null,"z":2};[1,{"n":[1],"z":2}];NULL` {
		t.Errorf("after update: %s", got)
	}
	if got := cells(`SELECT id FROM docs WHERE j @> '{"z":2}'`); got != "1" {
		t.Errorf("merged document is queryable as jsonb: %s", got)
	}

	// json || json merges like jsonb: datax keeps no json/jsonb
	// distinction in values (a deliberate difference from PostgreSQL,
	// which has no || on json).
	if got := cells(`SELECT '{"a":1}'::json || '{"b":2}'::json`); got != `{"a":1,"b":2}` {
		t.Errorf("json || json: %s", got)
	}
}

// TestByteaLengthFunctions (issue #344): length, octet_length,
// substring/substr, position/strpos and md5 over a bytea read its
// bytes, not the hex text it renders as.
func TestByteaLengthFunctions(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	runExprCases(t, ctx, s, []exprCase{
		{expr: `length('\x0102'::bytea)`, want: "2"},
		{expr: `octet_length('\x0102'::bytea)`, want: "2"},
		{expr: `length(''::bytea)`, want: "0"},
		{expr: `length(NULL::bytea)`, want: "NULL"},
		{expr: `substring('\x41424344'::bytea, 2, 2)`, want: `\x4243`},
		{expr: `substring('\x41424344'::bytea FROM 3)`, want: `\x4344`},
		{expr: `substr('\x41424344'::bytea, 0, 2)`, want: `\x41`},
		{expr: `substring('\x41'::bytea, 1, -1)`, code: "22023"},
		{expr: `position('\x43'::bytea IN '\x41424344'::bytea)`, want: "3"},
		{expr: `position('\x4344' IN '\x41424344'::bytea)`, want: "3"},
		{expr: `position('\x45'::bytea IN '\x41424344'::bytea)`, want: "0"},
		{expr: `strpos('\x41424344'::bytea, '\x44'::bytea)`, want: "4"},
		{expr: `md5('\x01020304'::bytea)`, want: "08d6c05a21512a79a1dfeb9d2a8f262f"},
		// Text is unchanged.
		{expr: `length('héllo')`, want: "5"},
		{expr: `octet_length('héllo')`, want: "6"},
		{expr: `substring('héllo', 2, 2)`, want: "él"},
		{expr: `position('l' IN 'héllo')`, want: "3"},
		{expr: `md5('abc')`, want: "900150983cd24fb0d6963f7d28e17f72"},
	})

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE blobs (id INT8 PRIMARY KEY, b BYTEA)`,
		`INSERT INTO blobs VALUES (1, '\x01020304'), (2, ''), (3, NULL)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	got, err := queryCells(t, ctx, conn, `SELECT length(b), octet_length(b), substring(b FROM 2 FOR 2), substr(b, 3), position('\x03'::bytea IN b) FROM blobs ORDER BY id`, pgx.QueryResultFormats{pgx.TextFormatCode})
	if err != nil {
		t.Fatal(err)
	}
	if want := `4|4|\x0203|\x0304|3;0|0|\x|\x|0;NULL|NULL|NULL|NULL|NULL`; got != want {
		t.Errorf("bytea column functions:\n got %s\nwant %s", got, want)
	}
	// A bytea parameter in binary format.
	var n int64
	if err := conn.QueryRow(ctx, `SELECT length($1::bytea)`, []byte{0, 1, 2}).Scan(&n); err != nil || n != 3 {
		t.Errorf("length($1::bytea) = %d, %v", n, err)
	}
	// substring of a bytea describes and scans as bytea.
	const oidBytea, oidInt8, oidText = 17, 20, 25
	oids := describeOIDs(t, ctx, conn, "bl", `SELECT substring(b, 1, 2), length(b), substring('abc', 1, 2) FROM blobs`)
	if len(oids) != 3 || oids[0] != oidBytea || oids[1] != oidInt8 || oids[2] != oidText {
		t.Errorf("describe: %v, want [%d %d %d]", oids, oidBytea, oidInt8, oidText)
	}
	var sub []byte
	if err := conn.QueryRow(ctx, `SELECT substring(b, 2, 2) FROM blobs WHERE id = 1`).Scan(&sub); err != nil || string(sub) != "\x02\x03" {
		t.Errorf("scan substring(bytea): %x, %v", sub, err)
	}
}
