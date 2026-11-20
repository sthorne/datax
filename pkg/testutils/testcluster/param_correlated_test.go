package testcluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestParamCorrelatedArithmetic (issue #381): an untyped parameter in
// arithmetic with an outer column inside a correlated subquery takes
// the column's type, as PostgreSQL infers it. Over the extended
// protocol the statement used to fail with XX000 "WHERE x: cannot use
// FLOAT8 value as INT8" whenever the parameter's value arrived in text
// format (pgx's exec mode, and the simple protocol's quoted literal):
// the string was read as a float beside the BIGINT. Each shape runs in
// pgx's default mode (Describe, then binary), its exec mode (text
// format, no Describe) and the simple protocol, and Describe reports $1
// as int8. The subquery stands in WHERE, in a join's ON, as IN and
// scalar subqueries, and two levels deep; the parameter stands on
// either side of +, - and *.
func TestParamCorrelatedArithmetic(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = cn.Close(ctx) }()

	for _, q := range []string{
		`CREATE TABLE a (id INT8 PRIMARY KEY)`,
		`CREATE TABLE b (id INT8 PRIMARY KEY, aid INT8)`,
		`CREATE TABLE c (x INT8 PRIMARY KEY, t TEXT)`,
		`INSERT INTO a VALUES (1), (2), (3)`,
		`INSERT INTO b VALUES (10, 1), (20, 2), (30, 3)`,
		`INSERT INTO c VALUES (2, 'two'), (4, 'four'), (6, 'six')`,
	} {
		if _, err := cn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	modes := []struct {
		name string
		mode pgx.QueryExecMode
	}{
		{"describe", pgx.QueryExecModeCacheStatement},
		{"exec", pgx.QueryExecModeExec},
		{"simple", pgx.QueryExecModeSimpleProtocol},
	}
	rows := func(mode pgx.QueryExecMode, q string, args ...any) (string, error) {
		rs, err := cn.Query(ctx, q, append([]any{mode}, args...)...)
		if err != nil {
			return "", err
		}
		defer rs.Close()
		var out []string
		for rs.Next() {
			vals, err := rs.Values()
			if err != nil {
				return "", err
			}
			cells := make([]string, len(vals))
			for i, v := range vals {
				cells[i] = fmt.Sprint(v)
			}
			out = append(out, strings.Join(cells, "|"))
		}
		return strings.Join(out, ";"), rs.Err()
	}

	for _, c := range []struct {
		name string
		sql  string
		arg  int64
		want string
	}{
		// The issue's two shapes.
		{"where exists", `SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.x = a.id + $1) ORDER BY a.id`, 1, "1;3"},
		{"join on exists", `SELECT a.id, b.id FROM a JOIN b ON b.aid = a.id AND EXISTS (SELECT 1 FROM c WHERE c.x = a.id + $1) ORDER BY a.id`, 1, "1|10;3|30"},
		// The parameter on the left, other operators, a comparison with
		// the outer column itself.
		{"param first", `SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.x = $1 + a.id) ORDER BY a.id`, 1, "1;3"},
		{"minus", `SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.x = a.id - $1) ORDER BY a.id`, -3, "1;3"},
		{"times", `SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.x = a.id * $1) ORDER BY a.id`, 2, "1;2;3"},
		{"outer compared", `SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.x > a.id AND a.id < $1) ORDER BY a.id`, 3, "1;2"},
		// IN, scalar and nested subqueries.
		{"in", `SELECT a.id FROM a WHERE a.id IN (SELECT c.x - $1 FROM c WHERE c.x > a.id) ORDER BY a.id`, 1, "1;3"},
		{"in correlated filter", `SELECT a.id FROM a WHERE a.id IN (SELECT b.aid FROM b WHERE EXISTS (SELECT 1 FROM c WHERE c.x = a.id + $1)) ORDER BY a.id`, 1, "1;3"},
		{"scalar", `SELECT a.id, (SELECT c.t FROM c WHERE c.x = a.id * $1) FROM a ORDER BY a.id`, 2, "1|two;2|four;3|six"},
		{"nested", `SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = a.id AND EXISTS (SELECT 1 FROM c WHERE c.x = a.id + $1)) ORDER BY a.id`, 1, "1;3"},
		// Uncorrelated, for comparison.
		{"uncorrelated", `SELECT x FROM c WHERE x = 1 + $1`, 1, "2"},
	} {
		for _, m := range modes {
			got, err := rows(m.mode, c.sql, c.arg)
			if err != nil {
				t.Errorf("%s (%s): %s: %v", c.name, m.name, c.sql, err)
				continue
			}
			if got != c.want {
				t.Errorf("%s (%s): got %q, want %q", c.name, m.name, got, c.want)
			}
		}
		sd, err := cn.Prepare(ctx, "", c.sql)
		if err != nil {
			t.Errorf("%s: prepare: %v", c.name, err)
			continue
		}
		if fmt.Sprint(sd.ParamOIDs) != "[20]" {
			t.Errorf("%s: ParameterDescription %v, want [20] (int8)", c.name, sd.ParamOIDs)
		}
	}

	// A float operand compares with the integer column as a number, as
	// in PostgreSQL (int8 = float8), instead of failing to convert.
	for _, m := range modes {
		got, err := rows(m.mode, `SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.x = a.id + $1::float8) ORDER BY a.id`, 1.0)
		if err != nil || got != "1;3" {
			t.Errorf("float8 operand (%s): %q, %v; want \"1;3\"", m.name, got, err)
		}
	}

	// A value that is not a number is a proper error, never XX000: text
	// beside a BIGINT does not parse (22P02), as in PostgreSQL.
	for _, q := range []string{
		`SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.x = a.id + $1)`,
		`SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.x = $1)`,
		`SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.x IN ($1))`,
	} {
		for _, arg := range []string{"1.5", "abc"} {
			_, err := rows(pgx.QueryExecModeExec, q, arg)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "22P02" {
				t.Errorf("%s with %q: %v, want 22P02", q, arg, err)
			}
		}
	}
}
