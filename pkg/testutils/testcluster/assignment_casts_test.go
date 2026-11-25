package testcluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestAssignmentCasts: a numeric or float8 value stores into an
// integer column as PostgreSQL's assignment casts (pg_cast castcontext
// 'a') convert it, on every path that stores a value: INSERT ... VALUES, INSERT ...
// SELECT, UPDATE (with a correlated subquery and with FROM) and ON
// CONFLICT DO UPDATE. numeric rounds half away from zero, float8 half
// to even (rint); a value out of the column's range is 22003
// "smallint / integer / bigint out of range", a numeric NaN 0A000. A
// comparison is not an assignment: WHERE c = 7.5 matches no integer.
// Before, every such store failed with 22P02 "cannot use DECIMAL value
// as INT8". The expected values are PostgreSQL 16's.
func TestAssignmentCasts(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	exec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q, pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	rowsOf := func(q string) string {
		t.Helper()
		res, err := conn.PgConn().Exec(ctx, q).ReadAll()
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var out []string
		for _, r := range res {
			for _, row := range r.Rows {
				var parts []string
				for _, v := range row {
					parts = append(parts, string(v))
				}
				out = append(out, strings.Join(parts, "|"))
			}
		}
		return strings.Join(out, "; ")
	}
	expect := func(q, want string) {
		t.Helper()
		if got := rowsOf(q); got != want {
			t.Fatalf("%s:\n got %q\nwant %q", q, got, want)
		}
	}
	refused := func(q, code, msg string) {
		t.Helper()
		_, err := conn.Exec(ctx, q, pgx.QueryExecModeSimpleProtocol)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != code || pe.Message != msg {
			t.Fatalf("%s: %v, want %s %q", q, err, code, msg)
		}
	}

	exec(`CREATE TABLE ac (id INT8 PRIMARY KEY, a INT2, b INT4, c INT8, n NUMERIC)`)
	// numeric: half away from zero; float8: half to even.
	exec(`INSERT INTO ac VALUES (1, 2.5, -2.5, 3.5, 1), (2, 1.5::float8, 2.5::float8, -2.5::float8, 1.5)`)
	expect(`SELECT id, a, b, c, n FROM ac ORDER BY id`, "1|3|-3|4|1; 2|2|2|-2|1.5")
	exec(`INSERT INTO ac VALUES (3, 0, 0, 9223372036854775807.4, 0)`)
	expect(`SELECT c FROM ac WHERE id = 3`, "9223372036854775807")

	// INSERT ... SELECT, UPDATE, ON CONFLICT DO UPDATE.
	exec(`INSERT INTO ac SELECT id + 10, n + 0.5, n * 3, n - 0.5, n FROM ac WHERE id = 2`)
	expect(`SELECT a, b, c FROM ac WHERE id = 12`, "2|5|1")
	exec(`UPDATE ac SET c = 7.5, b = 2.5::float8 WHERE id = 1`)
	expect(`SELECT b, c FROM ac WHERE id = 1`, "2|8")
	exec(`UPDATE ac SET a = (SELECT avg(c) FROM ac x WHERE x.id IN (1, 2)) WHERE id = 1`)
	expect(`SELECT a FROM ac WHERE id = 1`, "3")
	exec(`UPDATE ac SET b = s.n * 10 FROM ac s WHERE s.id = 2 AND ac.id = 3`)
	expect(`SELECT b FROM ac WHERE id = 3`, "15")
	exec(`INSERT INTO ac VALUES (1, 0, 0, 0, 4.5) ON CONFLICT (id) DO UPDATE SET c = excluded.n`)
	expect(`SELECT c FROM ac WHERE id = 1`, "5")

	// Out of range, NaN.
	refused(`INSERT INTO ac (id, a) VALUES (20, 32767.5)`, "22003", "smallint out of range")
	refused(`INSERT INTO ac (id, a) VALUES (20, 1e30)`, "22003", "smallint out of range")
	refused(`INSERT INTO ac (id, b) VALUES (20, 3e9::float8)`, "22003", "integer out of range")
	refused(`INSERT INTO ac (id, b) VALUES (20, 'NaN'::float8)`, "22003", "integer out of range")
	refused(`INSERT INTO ac (id, c) VALUES (20, 1e19)`, "22003", "bigint out of range")
	refused(`UPDATE ac SET c = 9.3e18::float8 WHERE id = 1`, "22003", "bigint out of range")
	refused(`INSERT INTO ac (id, b) VALUES (20, 'NaN'::numeric)`, "0A000", "cannot convert NaN to integer")
	expect(`SELECT count(*) FROM ac WHERE id = 20`, "0")

	// Comparisons do not round: no integer equals 7.5.
	expect(`SELECT count(*) FROM ac WHERE c = 7.5`, "0")
	expect(`SELECT count(*) FROM ac WHERE id = 1.5`, "0")
	expect(`SELECT count(*) FROM ac WHERE id = 1.0`, "1")
}
