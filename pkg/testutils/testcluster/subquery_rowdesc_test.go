package testcluster

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestScalarSubqueryRowDescription: a scalar subquery in the select list
// is described with its own output column's type (PostgreSQL types a
// scalar sublink by its one select item), whether the query has a FROM
// or not, the subquery is correlated or not, and whatever path runs the
// outer query (a table, a join, a grouped select, a derived table). The
// values arrive in that type in every pgx mode: the extended protocol's
// binary results (Describe before Execute), text results, and the simple
// protocol, where the RowDescription comes from execution. Before #388
// the FROM paths described the column as text (OID 25), and pgx could
// not scan a count(*) into an int64.
func TestScalarSubqueryRowDescription(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE sp (a INT8 PRIMARY KEY, i4 INT4, f FLOAT8, n NUMERIC, b TEXT)`,
		`CREATE TABLE sq (id INT8 PRIMARY KEY, p INT8, v INT4, t TEXT, f FLOAT8)`,
		`INSERT INTO sp VALUES (1, 7, 1.5, 2.5, 'x'), (2, 8, 2.5, 3.5, 'y')`,
		`INSERT INTO sq VALUES (1, 1, 10, 'a', 0.5), (2, 1, 20, 'b', 1.5), (3, 2, 30, 'c', 2.5)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	const (
		oidBool    = 16
		oidInt8    = 20
		oidInt4    = 23
		oidText    = 25
		oidFloat8  = 701
		oidNumeric = 1700
	)
	i64 := func() any { return new(int64) }
	i32 := func() any { return new(int32) }
	str := func() any { return new(string) }
	f64 := func() any { return new(float64) }
	nf64 := func() any { return new(*float64) }
	ni32 := func() any { return new(*int32) }
	bl := func() any { return new(bool) }

	cases := []struct {
		sql  string
		oids []uint32
		dest []func() any
		want []string // one per row: the scanned values, space separated
	}{
		// The issue's shapes.
		{`SELECT (SELECT count(*) FROM sq WHERE sq.p = sp.a) FROM sp ORDER BY a`,
			[]uint32{oidInt8}, []func() any{i64}, []string{"2", "1"}},
		{`SELECT (SELECT count(*) FROM sq) FROM sp ORDER BY a`,
			[]uint32{oidInt8}, []func() any{i64}, []string{"3", "3"}},
		{`SELECT c.relname, (SELECT count(*) FROM pg_attribute a WHERE a.attrelid = c.oid) FROM pg_class c WHERE c.relname = 'sp'`,
			// relname is text, not name (19): PG-T-027.
			[]uint32{oidText, oidInt8}, []func() any{str, i64}, []string{"sp 5"}},
		{`SELECT (SELECT count(*) FROM pg_attribute) > 0 AS n`,
			[]uint32{oidBool}, []func() any{bl}, []string{"true"}},
		{`SELECT (SELECT count(*) FROM sq) AS n`,
			[]uint32{oidInt8}, []func() any{i64}, []string{"3"}},
		// Aggregates: count is int8; sum of int4 is int8, avg numeric,
		// max and min their argument's type.
		{`SELECT (SELECT sum(v) FROM sq WHERE sq.p = sp.a), (SELECT avg(v) FROM sq WHERE sq.p = sp.a) FROM sp ORDER BY a`,
			[]uint32{oidInt8, oidNumeric}, []func() any{i64, f64}, []string{"30 15", "30 30"}},
		{`SELECT (SELECT max(t) FROM sq WHERE sq.p = sp.a), (SELECT min(f) FROM sq), (SELECT max(id) FROM sq WHERE sq.p = sp.a) FROM sp ORDER BY a`,
			[]uint32{oidText, oidFloat8, oidInt8}, []func() any{str, f64, i64}, []string{"b 0.5 2", "c 0.5 3"}},
		// Plain columns keep their column's type (int4 stays int4).
		{`SELECT (SELECT i4 FROM sp WHERE sp.a = sq.id), (SELECT n FROM sp WHERE sp.a = sq.id), (SELECT b FROM sp WHERE sp.a = sq.id) FROM sq WHERE id < 3 ORDER BY id`,
			[]uint32{oidInt4, oidNumeric, oidText}, []func() any{i32, f64, str}, []string{"7 2.5 x", "8 3.5 y"}},
		// Expressions: inside the subquery and around it.
		{`SELECT (SELECT f * 2 FROM sq WHERE sq.id = sp.a), (SELECT count(*) FROM sq WHERE sq.p = sp.a) + 1, (SELECT max(f) FROM sq) * 2 FROM sp ORDER BY a`,
			[]uint32{oidFloat8, oidInt8, oidFloat8}, []func() any{f64, i64, f64}, []string{"1 3 5", "3 2 5"}},
		// No row: NULL, still typed.
		{`SELECT (SELECT f FROM sq WHERE sq.id = 99), (SELECT v FROM sq WHERE sq.p = sp.a + 100) FROM sp ORDER BY a`,
			[]uint32{oidFloat8, oidInt4}, []func() any{nf64, ni32}, []string{"<nil> <nil>", "<nil> <nil>"}},
		{`SELECT (SELECT f FROM sq WHERE sq.id = 99) AS n`,
			[]uint32{oidFloat8}, []func() any{nf64}, []string{"<nil>"}},
		// Joins, grouping, derived tables.
		{`SELECT sp.a, (SELECT count(*) FROM sq s2 WHERE s2.p = sp.a) FROM sp JOIN sq ON sq.id = sp.a ORDER BY sp.a`,
			[]uint32{oidInt8, oidInt8}, []func() any{i64, i64}, []string{"1 2", "2 1"}},
		{`SELECT a, (SELECT count(*) FROM sq WHERE sq.p = sp.a) FROM sp GROUP BY a ORDER BY a`,
			[]uint32{oidInt8, oidInt8}, []func() any{i64, i64}, []string{"1 2", "2 1"}},
		{`SELECT d.c FROM (SELECT a, (SELECT count(*) FROM sq WHERE sq.p = sp.a) AS c FROM sp) d ORDER BY d.a`,
			[]uint32{oidInt8}, []func() any{i64}, []string{"2", "1"}},
		// A condition over a subquery is a boolean.
		{`SELECT EXISTS (SELECT 1 FROM sq WHERE sq.p = sp.a + 1) FROM sp ORDER BY a`,
			[]uint32{oidBool}, []func() any{bl}, []string{"true", "false"}},
	}

	modes := []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement,
		pgx.QueryExecModeCacheDescribe,
		pgx.QueryExecModeDescribeExec,
		pgx.QueryExecModeExec,
		pgx.QueryExecModeSimpleProtocol,
	}
	for _, c := range cases {
		for _, mode := range modes {
			rows, err := conn.Query(ctx, c.sql, mode)
			if err != nil {
				t.Fatalf("%v %s: %v", mode, c.sql, err)
			}
			var oids []uint32
			for _, fd := range rows.FieldDescriptions() {
				oids = append(oids, fd.DataTypeOID)
			}
			var got []string
			for rows.Next() {
				dest := make([]any, len(c.dest))
				for i, mk := range c.dest {
					dest[i] = mk()
				}
				if err := rows.Scan(dest...); err != nil {
					rows.Close()
					t.Fatalf("%v %s: scan: %v", mode, c.sql, err)
				}
				vals := make([]string, len(dest))
				for i, d := range dest {
					vals[i] = derefString(d)
				}
				got = append(got, strings.Join(vals, " "))
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				t.Fatalf("%v %s: %v", mode, c.sql, err)
			}
			if !reflect.DeepEqual(oids, c.oids) {
				t.Errorf("%v %s: OIDs %v, want %v", mode, c.sql, oids, c.oids)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("%v %s: rows %q, want %q", mode, c.sql, got, c.want)
			}
		}
	}

	// Describe and the rows agree in either result format on the
	// extended protocol: a text-format int8 is its digits, a binary one
	// eight bytes.
	const corr = `SELECT (SELECT count(*) FROM sq WHERE sq.p = sp.a), (SELECT v FROM sq WHERE sq.id = sp.a) FROM sp ORDER BY a`
	for _, format := range []int16{0, 1} {
		rr := conn.PgConn().ExecParams(ctx, corr, nil, nil, nil, []int16{format}).Read()
		if rr.Err != nil {
			t.Fatalf("format %d: %v", format, rr.Err)
		}
		if len(rr.FieldDescriptions) != 2 || rr.FieldDescriptions[0].DataTypeOID != oidInt8 || rr.FieldDescriptions[1].DataTypeOID != oidInt4 {
			t.Fatalf("format %d: fields %+v", format, rr.FieldDescriptions)
		}
		if len(rr.Rows) != 2 {
			t.Fatalf("format %d: %d rows", format, len(rr.Rows))
		}
		count, v := rr.Rows[0][0], rr.Rows[0][1]
		switch format {
		case 0:
			if string(count) != "2" || string(v) != "10" {
				t.Fatalf("text values %q %q", count, v)
			}
		case 1:
			if len(count) != 8 || count[7] != 2 || len(v) != 4 || v[3] != 10 {
				t.Fatalf("binary values %x %x", count, v)
			}
		}
	}
}

// derefString renders the value a scan destination points at.
func derefString(p any) string {
	v := reflect.ValueOf(p).Elem()
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "<nil>"
		}
		v = v.Elem()
	}
	return fmt.Sprint(v.Interface())
}

// TestIntegerWidthThroughRelations: an int2 or int4 column read through
// a view, a CTE, a derived table, a join with a view or a set operation
// keeps its width in the RowDescription (int2 / int4 / int8, as in
// PostgreSQL), in every pgx mode, and its binary values arrive in that
// width; a set operation's column is its widest member's (int2 UNION
// int8 is int8). A view's catalog rows (pg_attribute,
// information_schema.columns) carry the width too. Before, every
// integer read through one of these was int8.
func TestIntegerWidthThroughRelations(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE iw (id INT8 PRIMARY KEY, a INT2, b INT4, c INT8)`,
		`INSERT INTO iw VALUES (1, 1, 2, 3), (2, 4, 5, 6000000000)`,
		`CREATE VIEW iwv AS SELECT a, b, c FROM iw`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	const (
		oidInt2 = 21
		oidInt4 = 23
		oidInt8 = 20
	)
	i16 := func() any { return new(int16) }
	i32 := func() any { return new(int32) }
	i64 := func() any { return new(int64) }
	abc := []uint32{oidInt2, oidInt4, oidInt8}
	abcDest := []func() any{i16, i32, i64}
	abcRows := []string{"1 2 3", "4 5 6000000000"}
	cases := []struct {
		sql  string
		oids []uint32
		dest []func() any
		want []string
	}{
		{`SELECT a, b, c FROM iwv ORDER BY a`, abc, abcDest, abcRows},
		{`WITH w AS (SELECT a, b, c FROM iw) SELECT a, b, c FROM w ORDER BY a`, abc, abcDest, abcRows},
		{`SELECT * FROM (SELECT a, b, c FROM iw) s ORDER BY a`, abc, abcDest, abcRows},
		{`SELECT s.a, s.b, s.c FROM (SELECT a, b, c FROM iwv) s ORDER BY 1`, abc, abcDest, abcRows},
		{`SELECT v.a, v.b, x.c FROM iw x JOIN iwv v ON v.a = x.a ORDER BY 1`, abc, abcDest, abcRows},
		{`SELECT a, b, c, row_number() OVER (ORDER BY a) FROM iwv ORDER BY a`,
			[]uint32{oidInt2, oidInt4, oidInt8, oidInt8}, []func() any{i16, i32, i64, i64}, []string{"1 2 3 1", "4 5 6000000000 2"}},
		{`SELECT a FROM iw UNION ALL SELECT a FROM iw ORDER BY 1`,
			[]uint32{oidInt2}, []func() any{i16}, []string{"1", "1", "4", "4"}},
		{`SELECT * FROM (SELECT a FROM iw UNION SELECT b FROM iw) u ORDER BY 1`,
			[]uint32{oidInt4}, []func() any{i32}, []string{"1", "2", "4", "5"}},
		{`SELECT * FROM (SELECT a FROM iw UNION SELECT c FROM iw) u ORDER BY 1`,
			[]uint32{oidInt8}, []func() any{i64}, []string{"1", "3", "4", "6000000000"}},
	}
	modes := []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement,
		pgx.QueryExecModeDescribeExec,
		pgx.QueryExecModeExec,
		pgx.QueryExecModeSimpleProtocol,
	}
	for _, c := range cases {
		for _, mode := range modes {
			rows, err := conn.Query(ctx, c.sql, mode)
			if err != nil {
				t.Fatalf("%v %s: %v", mode, c.sql, err)
			}
			var oids []uint32
			for _, fd := range rows.FieldDescriptions() {
				oids = append(oids, fd.DataTypeOID)
			}
			var got []string
			for rows.Next() {
				dest := make([]any, len(c.dest))
				for i, mk := range c.dest {
					dest[i] = mk()
				}
				if err := rows.Scan(dest...); err != nil {
					rows.Close()
					t.Fatalf("%v %s: scan: %v", mode, c.sql, err)
				}
				vals := make([]string, len(dest))
				for i, d := range dest {
					vals[i] = derefString(d)
				}
				got = append(got, strings.Join(vals, " "))
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				t.Fatalf("%v %s: %v", mode, c.sql, err)
			}
			if !reflect.DeepEqual(oids, c.oids) {
				t.Errorf("%v %s: OIDs %v, want %v", mode, c.sql, oids, c.oids)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("%v %s: rows %q, want %q", mode, c.sql, got, c.want)
			}
		}
	}

	var got []string
	rows, err := conn.Query(ctx, `SELECT a.attname || ':' || a.atttypid::text || ':' || c.data_type FROM pg_attribute a
		JOIN information_schema.columns c ON c.table_name = 'iwv' AND c.column_name = a.attname
		WHERE a.attrelid = 'iwv'::regclass AND a.attnum > 0 ORDER BY a.attnum`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		got = append(got, s)
	}
	rows.Close()
	if want := []string{"a:21:smallint", "b:23:integer", "c:20:bigint"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("the view's catalog rows: %q, want %q", got, want)
	}
}
