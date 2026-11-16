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

// TestParamPositionsOverPgwire (issue #322): a parameter is counted and
// typed wherever it stands — only inside an IN subquery, against a
// qualified TIMESTAMPTZ column of a join used twice, through a
// single-table alias, in LIMIT, a WITH member, INSERT ... SELECT, a
// joined subquery, a scalar subquery, HAVING, ON, RETURNING, ON
// CONFLICT, and under EXPLAIN [ANALYZE] — over the extended protocol
// (Parse / Describe / Bind / Execute, so ParameterDescription carries
// the types pgx encodes with) and the simple one. Also the clock
// keywords (current_timestamp, current_time, localtime, ... with an
// optional precision) everywhere, RETURNING included.
func TestParamPositionsOverPgwire(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ext, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = ext.Close(ctx) }()
	scfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	scfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	simple, err := pgx.ConnectConfig(ctx, scfg)
	if err != nil {
		t.Fatalf("simple connect: %v", err)
	}
	defer func() { _ = simple.Close(ctx) }()

	for _, q := range []string{
		`CREATE TABLE s (sid INT8 PRIMARY KEY, g TEXT)`,
		`CREATE TABLE sr (id INT8 PRIMARY KEY, sid INT8, w FLOAT8)`,
		`CREATE TABLE cl (cid INT8 PRIMARY KEY, name TEXT)`,
		`CREATE TABLE gr (gid INT8 PRIMARY KEY, cid INT8, at TIMESTAMPTZ)`,
		`CREATE TABLE r (id INT8 PRIMARY KEY, n INT8, v TIMESTAMPTZ)`,
		`CREATE TABLE sink (id INT8 PRIMARY KEY, at TIMESTAMPTZ)`,
		`INSERT INTO s VALUES (1, 'a'), (2, 'b'), (3, 'a')`,
		`INSERT INTO sr VALUES (1, 1, 0.5), (2, 2, 1.5), (3, 1, 2.5), (4, 3, 3.5)`,
		`INSERT INTO cl VALUES (1, 'x'), (2, 'y')`,
		`INSERT INTO gr VALUES (1, 1, '2020-01-01 00:00:00+00'), (2, 1, '2030-01-01 00:00:00+00'), (3, 2, '2020-01-01 00:00:00+00'), (4, 2, '2019-01-01 00:00:00+00')`,
		`INSERT INTO r VALUES (1, 0, '2020-01-01 00:00:00+00')`,
	} {
		if _, err := ext.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	cut := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	// rows renders every row, cells joined by "|" and rows by ";".
	rows := func(cn *pgx.Conn, q string, args ...any) (string, error) {
		rs, err := cn.Query(ctx, q, args...)
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

	for _, tc := range []struct {
		name string
		sql  string
		args []any
		want string
		// oids, when set, are the ParameterDescription the extended
		// protocol must report.
		oids []uint32
	}{
		// (9) A parameter only inside IN (subquery).
		{"in subquery", `SELECT count(*) FROM sr WHERE sid IN (SELECT sid FROM s WHERE g = $1)`, []any{"a"}, "3", []uint32{25}},
		{"delete in subquery", `DELETE FROM sr WHERE sid IN (SELECT sid FROM s WHERE g = $1)`, []any{"none"}, "", []uint32{25}},
		{"exists subquery", `SELECT count(*) FROM sr WHERE EXISTS (SELECT 1 FROM s WHERE s.sid = sr.sid AND s.g = $1)`, []any{"b"}, "1", nil},
		{"update in subquery", `UPDATE sr SET w = $2 WHERE sid IN (SELECT sid FROM s WHERE g = $1) RETURNING id`, []any{"none", 1.0}, "", []uint32{25, 701}},
		// (10) Twice against a qualified TIMESTAMPTZ column of a join.
		{"join timestamptz twice", `SELECT count(*) FROM gr AS g JOIN cl AS c ON c.cid = g.cid WHERE (g.at < $1 OR (g.at = $1 AND g.gid < $2))`,
			[]any{cut, int64(2)}, "2", []uint32{1184, 20}},
		{"one table twice", `SELECT count(*) FROM gr WHERE (at < $1 OR (at = $1 AND gid < $2))`, []any{cut, int64(2)}, "2", []uint32{1184, 20}},
		// (11) A single table's alias.
		{"alias", `SELECT count(*) FROM gr AS g WHERE g.at < $1`, []any{cut}, "1", []uint32{1184}},
		{"alias select list", `SELECT g.gid, g.at < $1 FROM gr g WHERE g.cid = $2 ORDER BY g.gid`, []any{cut, int64(2)}, "3|false;4|true", []uint32{1184, 20}},
		{"alias grouped", `SELECT g.cid, count(*) FROM gr AS g WHERE g.at >= $1 GROUP BY g.cid HAVING count(*) > $2 ORDER BY g.cid`, []any{cut, int64(1)}, "1|2", []uint32{1184, 20}},
		// Other positions.
		{"limit offset", `SELECT gid FROM gr ORDER BY gid LIMIT $1 OFFSET $2`, []any{int64(2), int64(1)}, "2;3", []uint32{20, 20}},
		{"cte", `WITH old AS (SELECT gid, cid FROM gr WHERE at < $1) SELECT count(*) FROM old WHERE cid = $2`, []any{cut, int64(2)}, "1", []uint32{1184, 20}},
		{"cte columns", `WITH c AS (SELECT cid AS k, name FROM cl) SELECT name FROM c WHERE k = $1`, []any{int64(2)}, "y", []uint32{20}},
		{"derived", `SELECT count(*) FROM (SELECT gid, at FROM gr WHERE cid = $1) AS d WHERE d.at < $2`, []any{int64(2), cut}, "1", []uint32{20, 1184}},
		{"joined derived", `SELECT c.name FROM cl AS c JOIN (SELECT cid FROM gr WHERE at > $1) AS d ON d.cid = c.cid`, []any{cut}, "x", []uint32{1184}},
		{"scalar subquery", `SELECT name, (SELECT count(*) FROM gr WHERE gr.cid = cl.cid AND gr.at <= $1) FROM cl ORDER BY name`, []any{cut}, "x|1;y|2", []uint32{1184}},
		{"having", `SELECT cid FROM gr GROUP BY cid HAVING count(*) > $1 ORDER BY cid`, []any{int64(1)}, "1;2", []uint32{20}},
		{"join on filter", `SELECT g.gid FROM gr AS g JOIN cl AS c ON c.cid = g.cid AND c.name = $1 ORDER BY g.gid`, []any{"y"}, "3;4", []uint32{25}},
		{"case", `SELECT sum(CASE WHEN at < $1 THEN 1 ELSE 0 END) FROM gr`, []any{cut}, "1", []uint32{1184}},
		{"insert select", `INSERT INTO sink (id, at) SELECT $1, $2 ON CONFLICT (id) DO UPDATE SET at = excluded.at RETURNING id`, []any{int64(1), cut}, "1", []uint32{20, 1184}},
		{"insert select from", `INSERT INTO sink (id, at) SELECT gid + 10, at FROM gr WHERE at < $1 ON CONFLICT (id) DO UPDATE SET at = excluded.at RETURNING id`, []any{cut}, "14", []uint32{1184}},
		{"insert select subquery", `INSERT INTO sink (id, at) SELECT gid + 20, at FROM gr WHERE cid IN (SELECT cid FROM cl WHERE name = $1) ON CONFLICT (id) DO NOTHING`, []any{"none"}, "", []uint32{25}},
		{"on conflict", `INSERT INTO sink (id, at) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET at = $2 WHERE sink.at < $3 RETURNING id`, []any{int64(1), cut, cut}, "", []uint32{20, 1184, 1184}},
		{"returning param", `UPDATE r SET n = n + 1 WHERE id = $1 RETURNING v < $2`, []any{int64(1), cut}, "false", []uint32{20, 1184}},
		// (5) EXPLAIN counts and types the statement's parameters.
		{"explain no from", `EXPLAIN SELECT $1::text`, []any{"x"}, "result: one row computed without reading a table", []uint32{25}},
		{"cast", `SELECT $1::int8 + 1, $2::date`, []any{int64(41), cut}, "42|" + fmt.Sprint(cut), []uint32{20, 1082}},
	} {
		for _, mode := range []struct {
			name string
			cn   *pgx.Conn
		}{{"extended", ext}, {"simple", simple}} {
			got, err := rows(mode.cn, tc.sql, tc.args...)
			if err != nil {
				t.Errorf("%s (%s): %s: %v", tc.name, mode.name, tc.sql, err)
				continue
			}
			if got != tc.want {
				t.Errorf("%s (%s): got %q, want %q", tc.name, mode.name, got, tc.want)
			}
		}
		if tc.oids != nil {
			sd, err := ext.Prepare(ctx, "", tc.sql)
			if err != nil {
				t.Errorf("%s: prepare: %v", tc.name, err)
				continue
			}
			if fmt.Sprint(sd.ParamOIDs) != fmt.Sprint(tc.oids) {
				t.Errorf("%s: ParameterDescription %v, want %v", tc.name, sd.ParamOIDs, tc.oids)
			}
		}
	}

	// EXPLAIN [ANALYZE] of a parameterized statement: the parameters are
	// described like the statement's own, and ANALYZE runs with them.
	for _, mode := range []struct {
		name string
		cn   *pgx.Conn
	}{{"extended", ext}, {"simple", simple}} {
		plan, err := rows(mode.cn, `EXPLAIN SELECT gid FROM gr AS g WHERE g.at < $1`, cut)
		if err != nil || plan == "" {
			t.Errorf("explain (%s): %q, %v", mode.name, plan, err)
		}
		plan, err = rows(mode.cn, `EXPLAIN ANALYZE SELECT gid FROM gr WHERE at <= $1 AND cid = $2`, cut, int64(2))
		if err != nil || !strings.Contains(plan, "output: 2 rows") {
			t.Errorf("explain analyze (%s): %q, %v", mode.name, plan, err)
		}
		plan, err = rows(mode.cn, `EXPLAIN ANALYZE SELECT $1::int8 + 1`, int64(41))
		if err != nil || !strings.Contains(plan, "output: 1 rows") {
			t.Errorf("explain analyze no from (%s): %q, %v", mode.name, plan, err)
		}
	}
	for _, q := range []string{
		`EXPLAIN SELECT gid FROM gr AS g WHERE g.at < $1`,
		`EXPLAIN ANALYZE SELECT gid FROM gr WHERE at <= $1`,
	} {
		sd, err := ext.Prepare(ctx, "", q)
		if err != nil || fmt.Sprint(sd.ParamOIDs) != "[1184]" {
			t.Errorf("%s: %+v, %v", q, sd, err)
		}
	}

	// One parameter fixed as two types by two columns is 42P08, as in
	// PostgreSQL.
	for _, q := range []string{
		`SELECT count(*) FROM gr AS g JOIN cl AS c ON c.cid = g.cid WHERE g.at = $1 OR c.name = $1`,
		`INSERT INTO cl (cid, name) VALUES ($1, $1)`,
	} {
		_, err := ext.Prepare(ctx, "", q)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42P08" {
			t.Errorf("%s: %v, want 42P08", q, err)
		}
	}

	// (2) now() and the clock keywords in RETURNING of every write, and
	// the keywords wherever an expression goes.
	for _, mode := range []struct {
		name string
		cn   *pgx.Conn
		id   int64
	}{{"extended", ext, 10}, {"simple", simple, 20}} {
		for _, c := range []struct{ sql, want string }{
			{`UPDATE r SET n = n + 1 WHERE id = 1 RETURNING now() > v, current_timestamp = now(), v < now()`, "true|true|true"},
			{`UPDATE r SET n = n + 1 WHERE id = 1 RETURNING current_timestamp > v, localtimestamp > v, current_date > '2020-01-01'::date`, "true|true|true"},
			{fmt.Sprintf(`INSERT INTO r VALUES (%d, 0, now()) RETURNING v = now(), now() = current_timestamp`, mode.id), "true|true"},
			{fmt.Sprintf(`UPSERT INTO r VALUES (%d, 1, current_timestamp) RETURNING v = now(), n`, mode.id), "true|1"},
			{fmt.Sprintf(`INSERT INTO r VALUES (%d, 0, '2020-01-01 00:00:00+00') ON CONFLICT (id) DO UPDATE SET n = r.n + 1, v = now() WHERE r.v <= now() RETURNING v = now(), n`, mode.id), "true|2"},
			{fmt.Sprintf(`DELETE FROM r WHERE id = %d RETURNING now() >= v`, mode.id), "true"},
			{`SELECT current_timestamp = now(), current_date = now()::date, localtime IS NOT NULL, current_time IS NOT NULL`, "true|true|true|true"},
			{`SELECT current_timestamp(0) <= now() + interval '1 second', current_timestamp(3) <= now() + interval '1 second', localtimestamp(2) IS NOT NULL, current_time(1) IS NOT NULL, localtime(6) IS NOT NULL`, "true|true|true|true|true"},
			{`SELECT count(*) FROM r WHERE v < current_timestamp AND id = 1`, "1"},
			{fmt.Sprintf(`INSERT INTO r VALUES (%d, 0, current_timestamp(0)) RETURNING v <= now() + interval '1 second', current_timestamp(1) <= now() + interval '1 second'`, mode.id+1), "true|true"},
		} {
			got, err := rows(mode.cn, c.sql)
			if err != nil || got != c.want {
				t.Errorf("%s: %s: got %q, %v; want %q", mode.name, c.sql, got, err, c.want)
			}
		}
	}
	// A precision is an integer constant.
	for _, q := range []string{`SELECT current_timestamp(-1)`, `SELECT current_timestamp('x')`, `SELECT current_date(1)`} {
		if _, err := rows(ext, q); err == nil {
			t.Errorf("%s: accepted", q)
		}
	}
}
