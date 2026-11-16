package sql

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestParamTypeInference: a parameter takes the type of the column it is
// compared with or assigned to, wherever it stands — qualified through an
// alias or a join side, inside OR groups, in subqueries (correlated ones
// resolving outer columns), HAVING, LIMIT, CASE, RETURNING, ON CONFLICT
// and under EXPLAIN — one type however often it is used, and 42P08 when
// two columns disagree.
func TestParamTypeInference(t *testing.T) {
	tables := map[string]*catalog.TableDescriptor{
		"gr": {Name: "gr", Columns: []catalog.Column{
			{ID: 1, Name: "gid", Type: types.Int}, {ID: 2, Name: "cid", Type: types.Int}, {ID: 3, Name: "at", Type: types.Timestamp},
		}},
		"cl": {Name: "cl", Columns: []catalog.Column{
			{ID: 1, Name: "cid", Type: types.Int}, {ID: 2, Name: "name", Type: types.String}, {ID: 3, Name: "born", Type: types.Date},
		}},
		"s": {Name: "s", Columns: []catalog.Column{
			{ID: 1, Name: "sid", Type: types.Int}, {ID: 2, Name: "g", Type: types.Uuid},
		}},
		"sr": {Name: "sr", Columns: []catalog.Column{
			{ID: 1, Name: "id", Type: types.Int}, {ID: 2, Name: "sid", Type: types.Int}, {ID: 3, Name: "w", Type: types.Float},
		}},
	}
	lookup := func(name string) (*catalog.TableDescriptor, error) {
		if d, ok := tables[name]; ok {
			return d, nil
		}
		return nil, &catalog.ErrTableNotFound{Name: name}
	}
	const (
		I  = types.Int
		F  = types.Float
		S  = types.String
		TS = types.Timestamp
		U  = types.Uuid
		X  = types.Unknown
	)
	for _, tc := range []struct {
		sql  string
		want []types.Family
		err  string
	}{
		// Issue #322 (10): qualified columns of a join, a parameter used
		// twice, inside an OR group.
		{`SELECT count(*) FROM gr AS g JOIN cl AS c ON c.cid = g.cid WHERE (g.at < $1 OR (g.at = $1 AND g.gid < $2))`, []types.Family{TS, I}, ""},
		{`SELECT count(*) FROM gr WHERE at < $1 OR (at = $1 AND gid < $2)`, []types.Family{TS, I}, ""},
		// (11): an alias of a single table.
		{`SELECT count(*) FROM gr AS g WHERE g.at < $1`, []types.Family{TS}, ""},
		{`SELECT count(*) FROM gr g WHERE gr.gid = $1`, []types.Family{I}, ""},
		// (9): only in a subquery.
		{`SELECT count(*) FROM sr WHERE sid IN (SELECT sid FROM s WHERE g = $1)`, []types.Family{U}, ""},
		{`DELETE FROM sr WHERE sid IN (SELECT sid FROM s WHERE g = $1)`, []types.Family{U}, ""},
		{`UPDATE sr SET w = $2 WHERE sid IN (SELECT sid FROM s WHERE g = $1) RETURNING id`, []types.Family{U, F}, ""},
		{`SELECT id FROM sr WHERE EXISTS (SELECT 1 FROM s WHERE s.sid = sr.sid AND s.g = $1)`, []types.Family{U}, ""},
		// A correlated subquery resolves the outer alias.
		{`SELECT count(*) FROM gr AS g WHERE EXISTS (SELECT 1 FROM cl WHERE cl.cid = g.cid AND g.at > $1)`, []types.Family{TS}, ""},
		// Scalar subquery in the select list, CASE, HAVING, LIMIT/OFFSET.
		{`SELECT (SELECT max(w) FROM sr WHERE sid = $1) FROM gr`, []types.Family{I}, ""},
		{`SELECT CASE WHEN at > $1 THEN 1 ELSE 0 END FROM gr`, []types.Family{TS}, ""},
		{`SELECT cid, count(*) FROM gr GROUP BY cid HAVING count(*) > $1`, []types.Family{I}, ""},
		{`SELECT gid FROM gr WHERE cid = $1 LIMIT $2 OFFSET $3`, []types.Family{I, I, I}, ""},
		{`SELECT gid FROM gr WHERE cid = 1 UNION SELECT cid FROM cl WHERE name = $1`, []types.Family{S}, ""},
		// Writes: SET, VALUES, RETURNING, ON CONFLICT, INSERT ... SELECT.
		{`INSERT INTO gr (gid, at) VALUES ($1, $2) ON CONFLICT (gid) DO UPDATE SET at = $3 WHERE gr.cid = $4 RETURNING gid`, []types.Family{I, TS, TS, I}, ""},
		{`INSERT INTO gr (gid, at) SELECT $1, $2`, []types.Family{I, TS}, ""},
		{`INSERT INTO gr (gid, cid) SELECT sid, sid FROM s WHERE g = $1`, []types.Family{U}, ""},
		{`UPDATE gr SET at = $1 WHERE gid = $2 RETURNING at > $1`, []types.Family{TS, I}, ""},
		// EXPLAIN types the statement under it.
		{`EXPLAIN SELECT count(*) FROM gr AS g WHERE g.at < $1`, []types.Family{TS}, ""},
		{`EXPLAIN ANALYZE SELECT gid FROM gr WHERE cid = $1`, []types.Family{I}, ""},
		{`EXPLAIN SELECT $1::text`, []types.Family{S}, ""},
		{`SELECT $1, $2::int8 + 1`, []types.Family{X, I}, ""},
		// A cast fixes the parameter's type whatever it is compared to.
		{`SELECT gid FROM gr WHERE cid::text = $1::text`, []types.Family{S}, ""},
		// A function argument only suggests a type; a column fixes it.
		{`SELECT gid FROM gr WHERE at < $1 AND length($1) > 0`, []types.Family{TS}, ""},
		// Numbers of different widths agree; text and a timestamp do not.
		{`SELECT id FROM sr WHERE sid = $1 OR w = $1`, []types.Family{I}, ""},
		{`SELECT count(*) FROM gr AS g JOIN cl AS c ON c.cid = g.cid WHERE g.at = $1 OR c.name = $1`, nil, "42P08"},
		{`INSERT INTO cl (cid, name) VALUES ($1, $1)`, nil, "42P08"},
		// Unknown tables fail as they will at execution.
		{`SELECT x FROM nope WHERE x = $1`, nil, "42P01"},
	} {
		stmts, err := parser.Parse(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		pt := newParamTyper(parser.CountParams(stmts[0]), lookup)
		err = pt.statement(stmts[0], nil, true)
		if tc.err != "" {
			if serr := ToSQLError(err); serr == nil || serr.Code != tc.err {
				t.Errorf("%s: error %v, want %s", tc.sql, err, tc.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.sql, err)
			continue
		}
		var got, want []string
		for _, f := range pt.fams {
			got = append(got, f.String())
		}
		for _, f := range tc.want {
			want = append(want, f.String())
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: %v, want %v", tc.sql, got, want)
		}
	}
}
