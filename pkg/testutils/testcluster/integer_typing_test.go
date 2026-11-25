package testcluster

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestIntegerTyping (PG-F-016): int2, int4 and int8 are types of their
// own, as in PostgreSQL. An integer literal is an int4 when it fits;
// integer operators are of their wider operand's type and overflow at
// it (22003 "smallint / integer / bigint out of range"); pg_typeof,
// to_regtype and the wire (RowDescription and ParameterDescription, in
// every pgx mode, with binary values in the described width) report
// the real width. sum over int2 / int4 stays bigint and over int8
// numeric. Before, every integer expression was int8:
// 2147483647::int4 + 1 was 2147483648 and SELECT 1 described as int8.
func TestIntegerTyping(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE it (id INT8 PRIMARY KEY, a INT2, b INT4, c INT8)`,
		`INSERT INTO it VALUES (1, 1, 2, 3), (2, 32767, 2147483647, 9223372036854775807)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	// Values: pg_typeof, to_regtype and format_type.
	for q, want := range map[string]string{
		`SELECT pg_typeof(1), pg_typeof(-2147483648), pg_typeof(2147483648), pg_typeof(1::int2), pg_typeof(1::int8)`:           "integer integer bigint smallint bigint",
		`SELECT pg_typeof(a), pg_typeof(b), pg_typeof(c) FROM it WHERE id = 1`:                                                 "smallint integer bigint",
		`SELECT pg_typeof(a + a), pg_typeof(a + b), pg_typeof(b + c), pg_typeof(b * 2), pg_typeof(a + 1) FROM it WHERE id = 1`: "smallint integer bigint integer integer",
		`SELECT pg_typeof(coalesce(a, 1)), pg_typeof(CASE WHEN a > 0 THEN a ELSE b END) FROM it WHERE id = 1`:                  "integer integer",
		`SELECT pg_typeof(x) FROM (SELECT b + 1 AS x FROM it WHERE id = 1) s`:                                                  "integer",
		`SELECT to_regtype('int4'), to_regtype('integer'), to_regtype('int2'), to_regtype('int8')`:                             "23 23 21 20",
		`SELECT format_type(to_regtype('int4'), -1), format_type(to_regtype('smallint'), -1)`:                                  "integer smallint",
		`SELECT b + 1::int8, b::int8 + 1, a + 1 FROM it WHERE id = 2`:                                                          "2147483648 2147483648 32768",
	} {
		var got []string
		rows, err := conn.Query(ctx, q, pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			for _, v := range vals {
				got = append(got, derefString(&v))
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if strings.Join(got, " ") != want {
			t.Errorf("%s = %q, want %q", q, strings.Join(got, " "), want)
		}
	}

	// Overflow is at the result type's width.
	for q, want := range map[string]string{
		`SELECT 2147483647::int4 + 1`:                      "integer out of range",
		`SELECT 2147483647 + 1`:                            "integer out of range",
		`SELECT 50000 * 50000`:                             "integer out of range",
		`SELECT (-2147483648) / -1`:                        "integer out of range",
		`SELECT 32767::int2 + 1::int2`:                     "smallint out of range",
		`SELECT 9223372036854775807 + 1`:                   "bigint out of range",
		`SELECT 70000::int2`:                               "smallint out of range",
		`SELECT b + 1 FROM it WHERE id = 2`:                "integer out of range",
		`SELECT a + a FROM it WHERE id = 2`:                "smallint out of range",
		`SELECT c + 1 FROM it WHERE id = 2`:                "bigint out of range",
		`SELECT id FROM it WHERE b * 2 > 0`:                "integer out of range",
		`SELECT max(b) + 1 FROM it`:                        "integer out of range",
		`UPDATE it SET c = b + 1 WHERE id = 2`:             "integer out of range",
		`WITH w AS (SELECT b FROM it) SELECT b + 1 FROM w`: "integer out of range",
	} {
		for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement} {
			_, err := conn.Exec(ctx, q, mode)
			var pe *pgconn.PgError
			if !errors.As(err, &pe) || pe.Code != "22003" || pe.Message != want {
				t.Errorf("%v %s: %v, want 22003 %q", mode, q, err, want)
			}
		}
	}

	// The wire: computed integer columns describe in their width and
	// their binary values arrive in it.
	const (
		oidInt2    = 21
		oidInt4    = 23
		oidInt8    = 20
		oidNumeric = 1700
	)
	i16 := func() any { return new(int16) }
	i32 := func() any { return new(int32) }
	i64 := func() any { return new(int64) }
	str := func() any { return new(string) }
	cases := []struct {
		sql  string
		oids []uint32
		dest []func() any
		want []string
	}{
		{`SELECT 1, 2147483648, 1::int2, -2147483648, 1 + 1`,
			[]uint32{oidInt4, oidInt8, oidInt2, oidInt4, oidInt4}, []func() any{i32, i64, i16, i32, i32}, []string{"1 2147483648 1 -2147483648 2"}},
		{`SELECT b + 1, a + a, a + 1, c + 1, a::int8 FROM it WHERE id = 1`,
			[]uint32{oidInt4, oidInt2, oidInt4, oidInt8, oidInt8}, []func() any{i32, i16, i32, i64, i64}, []string{"3 2 2 4 1"}},
		{`SELECT x.b + y.a, x.a * y.a FROM it x JOIN it y ON y.id = x.id WHERE x.id = 1`,
			[]uint32{oidInt4, oidInt2}, []func() any{i32, i16}, []string{"3 1"}},
		{`SELECT a, max(b), min(a) + 1, b + 1, count(*) FROM it WHERE id = 1 GROUP BY a, b`,
			[]uint32{oidInt2, oidInt4, oidInt4, oidInt4, oidInt8}, []func() any{i16, i32, i32, i32, i64}, []string{"1 2 2 3 1"}},
		{`SELECT sum(a), sum(b), sum(c) FROM it WHERE id = 1`,
			[]uint32{oidInt8, oidInt8, oidNumeric}, []func() any{i64, i64, str}, []string{"1 2 3"}},
		{`WITH w AS (SELECT b + 1 AS x, 7 AS y FROM it WHERE id = 1) SELECT x, y, x * y FROM w`,
			[]uint32{oidInt4, oidInt4, oidInt4}, []func() any{i32, i32, i32}, []string{"3 7 21"}},
		{`SELECT coalesce(a, 1), CASE WHEN a > 0 THEN a ELSE 0 END, 1 << 31 FROM it WHERE id = 1`,
			[]uint32{oidInt4, oidInt4, oidInt4}, []func() any{i32, i32, i32}, []string{"1 1 -2147483648"}},
		{`SELECT (SELECT b FROM it WHERE id = 1) + 1, (SELECT count(*) FROM it) + 1 FROM it WHERE id = 1`,
			[]uint32{oidInt4, oidInt8}, []func() any{i32, i64}, []string{"3 3"}},
		{`SELECT * FROM (VALUES (1::int2), (100000)) v(x)`,
			[]uint32{oidInt4}, []func() any{i32}, []string{"1", "100000"}},
		{`VALUES (1::int2, 1), (2::int2, 2)`,
			[]uint32{oidInt2, oidInt4}, []func() any{i16, i32}, []string{"1 1", "2 2"}},
		{`SELECT 1 UNION ALL SELECT 2147483648 ORDER BY 1`,
			[]uint32{oidInt8}, []func() any{i64}, []string{"1", "2147483648"}},
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

	// Parameters describe in the width of what they stand for.
	for q, want := range map[string][]uint32{
		`SELECT id FROM it WHERE a = $1 AND b = $2 AND c = $3`: {oidInt2, oidInt4, oidInt8},
		`INSERT INTO it (id, a, b, c) VALUES ($1, $2, $3, $4)`: {oidInt8, oidInt2, oidInt4, oidInt8},
		`SELECT $1::int4 + 1, b + $2 FROM it`:                  {oidInt4, oidInt4},
	} {
		sd, err := conn.Prepare(ctx, "", q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if !reflect.DeepEqual(sd.ParamOIDs, want) {
			t.Errorf("%s: parameter OIDs %v, want %v", q, sd.ParamOIDs, want)
		}
	}
	var n int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM it WHERE a = $1 AND b = $2`, int16(1), int32(2)).Scan(&n); err != nil || n != 1 {
		t.Fatalf("binary int2 / int4 parameters: %d, %v", n, err)
	}
}
