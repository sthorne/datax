package testcluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// TestScanExprWireOutputEqual: a streamed scan with computed select
// items or a computed filter (decoded into one reused row, only the
// columns the statement reads, the filter's first; the kept columns
// copied out a chunk of rows at a time and the select list evaluated
// over them when the client pulls each row) sends the same DataRow
// bytes as the same statement materialized (a sort, which evaluates
// over a row map per row), in text over both protocols and in binary,
// under the session settings that change how values render. The
// statements name columns everywhere an expression can: in function
// arguments, operators, CASE arms and conditions, a comparison used as
// a value, IN lists and OR, on either side of a filter.
func TestScanExprWireOutputEqual(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, s := range wireTableSetup {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	r := dialRaw(t, tc)
	for _, setting := range []string{
		"",
		"SET TimeZone = 'America/St_Johns'",
		"SET IntervalStyle = 'iso_8601'",
		"SET extra_float_digits = -2",
	} {
		if setting != "" {
			r.rawRowsOf(setting, nil)
		}
		for _, f := range []struct {
			name    string
			formats []int16
		}{{"simple", nil}, {"text", []int16{0}}, {"binary", []int16{1}}} {
			for _, q := range []struct{ streamed, materialized string }{
				{ // computed items over every kind of column
					`SELECT k + 1, lower(t), t || '!', coalesce(i4, -1) * 2::int8, n2 * 2, upper(vc), ch, length(bb), j -> 'a', ai,
						CASE WHEN b THEN 'y' WHEN b IS NULL THEN NULL ELSE 'n' END, tz, ts, iv, m, fill + 0, f * 1, u, d, tm, tt FROM wt`,
					`SELECT k + 1, lower(t), t || '!', coalesce(i4, -1) * 2::int8, n2 * 2, upper(vc), ch, length(bb), j -> 'a', ai,
						CASE WHEN b THEN 'y' WHEN b IS NULL THEN NULL ELSE 'n' END, tz, ts, iv, m, fill + 0, f * 1, u, d, tm, tt FROM wt ORDER BY k + 0`,
				},
				{ // columns named only inside CASE, a comparison value, coalesce
					`SELECT CASE i2 WHEN 7 THEN vc ELSE t END, CASE WHEN n > 0 THEN 'pos' END, (d = '2026-08-30'), coalesce(vc, ch, t) FROM wt`,
					`SELECT CASE i2 WHEN 7 THEN vc ELSE t END, CASE WHEN n > 0 THEN 'pos' END, (d = '2026-08-30'), coalesce(vc, ch, t) FROM wt ORDER BY k + 0`,
				},
				{ // a computed filter, bare columns kept
					`SELECT k, t, j, n2, ai FROM wt WHERE i4 % 2 = 0 OR lower(t) LIKE 'h%'`,
					`SELECT k, t, j, n2, ai FROM wt WHERE i4 % 2 = 0 OR lower(t) LIKE 'h%' ORDER BY k + 0`,
				},
				{ // the filter's columns, then the kept ones it does not read (stamped CHAR, NUMERIC(p,s), arrays, TIMESTAMP)
					`SELECT t, vc, ai, tt, n2, ch, ts, tz FROM wt WHERE coalesce(i2, 0) + g >= 1`,
					`SELECT t, vc, ai, tt, n2, ch, ts, tz FROM wt WHERE coalesce(i2, 0) + g >= 1 ORDER BY k + 0`,
				},
				{ // a filter reading a kept column, IN with a column in the list
					`SELECT t, k, ch FROM wt WHERE length(coalesce(t, '')) < 4 OR k IN (g, 5)`,
					`SELECT t, k, ch FROM wt WHERE length(coalesce(t, '')) < 4 OR k IN (g, 5) ORDER BY k + 0`,
				},
				{ // computed items and a computed filter together
					`SELECT k * 2, upper(t), n2 FROM wt WHERE coalesce(i4, 1) <> 0 AND g * 1 > 0`,
					`SELECT k * 2, upper(t), n2 FROM wt WHERE coalesce(i4, 1) <> 0 AND g * 1 > 0 ORDER BY k + 0`,
				},
				{ // a column compared with constants (evaluated without an environment)
					`SELECT k, f, 'c' FROM wt WHERE f > 0 OR f IS NULL OR i2 IN (7, 8)`,
					`SELECT k, f, 'c' FROM wt WHERE f > 0 OR f IS NULL OR i2 IN (7, 8) ORDER BY k + 0`,
				},
				{ // a reverse primary-key range
					`SELECT k * 2, t, n2 FROM wt WHERE k > -10 ORDER BY k DESC`,
					`SELECT k * 2, t, n2 FROM wt WHERE k > -10 ORDER BY k + 0 DESC`,
				},
				{ // an index scan, with a computed residual
					`SELECT n2 + 1, k, ch, iv FROM wt WHERE g = 1 AND k * 1 <> 3`,
					`SELECT n2 + 1, k, ch, iv FROM wt WHERE g = 1 AND k * 1 <> 3 ORDER BY k + 0`,
				},
			} {
				streamed := r.rawRowsOf(q.streamed, f.formats)
				mat := r.rawRowsOf(q.materialized, f.formats)
				if len(streamed) == 0 || len(streamed) != len(mat) {
					t.Fatalf("%q %s %s: %d / %d rows", setting, f.name, q.streamed, len(streamed), len(mat))
				}
				for i := range streamed {
					if len(streamed[i]) != len(mat[i]) {
						t.Fatalf("%s: row %d has %d / %d values", q.streamed, i, len(streamed[i]), len(mat[i]))
					}
					for c := range streamed[i] {
						s, m := streamed[i][c], mat[i][c]
						if !bytes.Equal(s, m) || (s == nil) != (m == nil) {
							t.Errorf("%q %s %s: row %d value %d: streamed %q, materialized %q", setting, f.name, q.streamed, i, c, s, m)
						}
					}
				}
			}
		}
	}
}

// TestScanExprEvaluatedWhenPulled: a streamed scan evaluates its select
// list for a row only when the client pulls that row, as it always has:
// a failing expression on the fourth row (10 / 0) sends the three rows
// before it and then 22012; a LIMIT, or a portal executed for two rows
// and closed, never reaches it.
func TestScanExprEvaluatedWhenPulled(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, s := range wireTableSetup {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	r := dialRaw(t, tc)
	// Rows in key order: -4, 1, 2, 3, 5.
	const q = `SELECT k, 10 / (k - 3) FROM wt`
	r.send(&pgproto3.Query{String: q})
	var got []string
	code := ""
	for done := false; !done; {
		switch m := r.recv().(type) {
		case *pgproto3.DataRow:
			got = append(got, string(m.Values[0]))
		case *pgproto3.ErrorResponse:
			code = m.Code
		case *pgproto3.ReadyForQuery:
			done = true
		}
	}
	if code != "22012" || len(got) != 3 || got[0] != "-4" || got[2] != "2" {
		t.Fatalf("%s: rows %v then %q, want rows -4, 1, 2 then 22012", q, got, code)
	}

	if rows := r.rawRowsOf(q+` LIMIT 3`, nil); len(rows) != 3 {
		t.Fatalf("LIMIT 3: %d rows", len(rows))
	}

	r.send(
		&pgproto3.Parse{Name: "p", Query: q},
		&pgproto3.Bind{DestinationPortal: "c", PreparedStatement: "p"},
		&pgproto3.Execute{Portal: "c", MaxRows: 2},
		&pgproto3.Close{ObjectType: 'P', Name: "c"},
		&pgproto3.Sync{},
	)
	rows, suspended := 0, false
	for done := false; !done; {
		switch m := r.recv().(type) {
		case *pgproto3.DataRow:
			rows++
		case *pgproto3.PortalSuspended:
			suspended = true
		case *pgproto3.ErrorResponse:
			t.Fatalf("portal of two rows: %s %s", m.Code, m.Message)
		case *pgproto3.ReadyForQuery:
			done = true
		}
	}
	if rows != 2 || !suspended {
		t.Fatalf("portal: %d rows, suspended %v; want 2 rows, suspended", rows, suspended)
	}
}

// TestScanShapesAllocsPerRow: streamed scans off the bare-column path
// allocate about what the bare-column path does per row sent, in the
// whole process. Before: computed items (k + 1, n * 2) 7.0 allocations
// a row, lower(pad) 8.0, a computed filter passing one row in three 9.1
// (3 per row scanned), a reverse scan 4.0, a text-search filter passing
// one row in four 12.2 (3 per row scanned). Now: about 0.1, 3 (lower's
// result, its argument list and pad decoded), 1.1 (pad, decoded only
// for the rows that pass), 1.1 (pad) and 0.2 (the text matched where it
// is stored).
func TestScanShapesAllocsPerRow(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful under the race detector")
	}
	const rows = 5000
	limits := map[string]float64{"exprs": 0.5, "lower": 3.5, "computed-where": 1.6, "reverse": 1.5, "tsearch": 0.6}
	for _, sh := range scanShapes(rows) {
		limit, ok := limits[sh.name]
		if !ok {
			continue
		}
		sb := startScanBenchQuery(t, rows, false, sh.q, sh.want)
		got, _, mallocs, _ := sb.measure(5)
		perRow := float64(mallocs) / float64(got)
		t.Logf("%s: %.2f allocs/row", sh.name, perRow)
		if perRow > limit {
			t.Errorf("%s (%s): %.2f allocations per row sent, want at most %.1f", sh.name, sh.q, perRow, limit)
		}
	}
}

// TestScanTextSearchWireOutputEqual: a streamed scan filtering on
// `to_tsvector(config, body) @@ query` (body decoded as a view of the
// stored row when nothing else reads it, copied when a matching row
// keeps it) sends the same DataRow bytes as the same statement
// materialized, in text and binary, read in full or by a primary-key
// range: body kept or not, in an expression, under NOT, read by another
// conjunct too, NULL, empty, non-ASCII and past the length the fused
// matcher reads directly. The same statements on a table with a GIN
// index (an inverted scan, which materializes) give the same rows.
func TestScanTextSearchWireOutputEqual(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, tbl := range []string{"docs", "docsgin"} {
		for _, s := range []string{
			`CREATE TABLE ` + tbl + ` (k INT8 PRIMARY KEY, body TEXT, title VARCHAR(60), ch CHAR(8))`,
			`INSERT INTO ` + tbl + ` VALUES
				(1, 'The quick brown fox jumps over the lazy dog', 'fox one', 'a'),
				(2, 'A slow green turtle', 'turtle', NULL),
				(3, NULL, 'no body', 'c'),
				(4, '', 'empty', 'd'),
				(5, 'Foxes, foxes everywhere: a fox den', NULL, 'e'),
				(6, 'héllo wörld, the fox says ünïcode', 'unicode', 'f'),
				(7, repeat('fox and hound ', 40000), 'long', 'g'),
				(8, 'quick thinking saves the lazy', 'quick', 'h')`,
		} {
			if _, err := conn.Exec(ctx, s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
	}
	if _, err := conn.Exec(ctx, `CREATE INDEX docsgin_fts ON docsgin USING GIN (to_tsvector('english', body))`); err != nil {
		t.Fatal(err)
	}
	r := dialRaw(t, tc)
	for _, tbl := range []string{"docs", "docsgin"} {
		for _, f := range []struct {
			name    string
			formats []int16
		}{{"simple", nil}, {"binary", []int16{1}}} {
			for _, q := range []string{
				`SELECT k FROM %s WHERE to_tsvector('english', body) @@ to_tsquery('english', 'fox')`,
				`SELECT k, body, title, ch FROM %s WHERE to_tsvector('english', body) @@ plainto_tsquery('english', 'quick fox')`,
				`SELECT k, upper(body), length(title) FROM %s WHERE to_tsvector('english', body) @@ websearch_to_tsquery('english', 'fox -lazy')`,
				`SELECT k, title FROM %s WHERE NOT (to_tsvector('english', body) @@ to_tsquery('english', 'fox'))`,
				`SELECT k, title FROM %s WHERE to_tsvector('english', body) @@ to_tsquery('english', 'fox | lazy') AND length(body) < 100`,
				`SELECT k, body FROM %s WHERE to_tsvector('simple', body) @@ to_tsquery('simple', 'the') AND k > 0`,
			} {
				streamed := r.rawRowsOf(fmt.Sprintf(q, tbl), f.formats)
				mat := r.rawRowsOf(fmt.Sprintf(q, tbl)+` ORDER BY k + 0`, f.formats)
				if len(streamed) == 0 || len(streamed) != len(mat) {
					t.Fatalf("%s %s: %d / %d rows", f.name, fmt.Sprintf(q, tbl), len(streamed), len(mat))
				}
				for i := range streamed {
					for c := range streamed[i] {
						s, m := streamed[i][c], mat[i][c]
						if !bytes.Equal(s, m) || (s == nil) != (m == nil) {
							t.Errorf("%s %s: row %d value %d: streamed %.60q, materialized %.60q", f.name, fmt.Sprintf(q, tbl), i, c, s, m)
						}
					}
				}
			}
		}
	}
}
