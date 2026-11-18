package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestStatisticalAndBitAggregates (PG-F-013): corr, covar_pop,
// covar_samp and the regr_* family over (Y, X) pairs (a pair with a
// NULL skipped), bit_and / bit_or / bit_xor, any_value and the JSON
// _strict / _unique aggregates — grouped, ungrouped, with FILTER, and
// as window functions — with PostgreSQL's NULL rules and SQLSTATEs.
func TestStatisticalAndBitAggregates(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE st (id INT PRIMARY KEY, g INT, y FLOAT8, x FLOAT8, b INT, j TEXT, n NUMERIC)`)
	execSQL(t, ctx, s, `INSERT INTO st VALUES
		(1, 1, 1, 1, 12, 'a', 1), (2, 1, 2, 2, 10, NULL, 2), (3, 1, 3, 4, 6, 'c', 3),
		(4, 2, NULL, 5, 3, 'd', 4), (5, 2, 4, 8, NULL, 'e', 5)`)

	for q, want := range map[string]string{
		// Pairs (1,1) (2,2) (3,4) (4,8): Sxx 28.75, Syy 5, Sxy 11.5.
		`SELECT regr_count(y, x), regr_avgx(y, x), regr_avgy(y, x), regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM st`:           "4|3.75|2.5|28.75|5|11.5",
		`SELECT covar_pop(y, x), round(covar_samp(y, x)::numeric, 12), regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM st`: "2.875|3.833333333333|0.4|1|0.92",
		`SELECT round(corr(y, x)::numeric, 12) FROM st`: "0.959166304663",
		// Integer and numeric arguments read as double precision.
		`SELECT regr_count(b, id), regr_sxx(n, id), covar_pop(id, id) FROM st`: "4|10|2",
		// No pairs: NULL, but regr_count is 0; covar_samp needs two.
		`SELECT corr(y, x), covar_pop(y, x), regr_count(y, x), regr_slope(y, x) FROM st WHERE false`: "NULL|NULL|0|NULL",
		`SELECT covar_samp(y, x), covar_pop(y, x) FROM st WHERE id = 1`:                              "NULL|0",
		// X constant: no slope, corr or r2; Y constant: r2 is 1.
		`SELECT regr_slope(y, 1), corr(y, 1), regr_r2(y, 1), regr_r2(1, x) FROM st`: "NULL|NULL|NULL|1",
		`SELECT g, regr_count(y, x), covar_pop(y, x) FROM st GROUP BY g ORDER BY g`: "1|3|1;2|1|0",
		`SELECT regr_count(y, x) FILTER (WHERE id < 3) FROM st`:                     "2",
		// Bitwise: NULLs are skipped; no rows is NULL.
		`SELECT bit_and(b), bit_or(b), bit_xor(b) FROM st`:                 "0|15|3",
		`SELECT bit_and(b), bit_or(b), bit_xor(b) FROM st WHERE g = 1`:     "0|14|0",
		`SELECT bit_and(b), bit_or(b), bit_xor(b) FROM st WHERE b IS NULL`: "NULL|NULL|NULL",
		`SELECT g, bit_or(b) FROM st GROUP BY g ORDER BY g`:                "1|14;2|3",
		// any_value: some non-NULL value of the group.
		`SELECT any_value(j) IN ('a', 'c', 'd', 'e') FROM st`:  "t",
		`SELECT any_value(j) FROM st WHERE id = 2`:             "NULL",
		`SELECT g, any_value(g) FROM st GROUP BY g ORDER BY g`: "1|1;2|2",
		// The JSON _strict forms skip NULLs; _unique refuses a key twice.
		`SELECT jsonb_agg_strict(j ORDER BY id), json_agg_strict(j ORDER BY id) FROM st`: `["a","c","d","e"]|["a","c","d","e"]`,
		`SELECT jsonb_object_agg_strict(id, j) FROM st`:                                  `{"1":"a","3":"c","4":"d","5":"e"}`,
		`SELECT jsonb_object_agg_unique(id, j) FROM st WHERE id < 3`:                     `{"1":"a","2":null}`,
		`SELECT jsonb_object_agg_unique_strict(id, j) FROM st WHERE id < 3`:              `{"1":"a"}`,
		`SELECT jsonb_agg_strict(j) FROM st WHERE j IS NULL`:                             "NULL",
	} {
		if got, err := queryCellsSession(ctx, s, q); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", q, got, err, want)
		}
	}

	// As window functions.
	for q, want := range map[string]string{
		`SELECT id, bit_or(b) OVER (ORDER BY id), regr_count(y, x) OVER (ORDER BY id) FROM st ORDER BY id`:      "1|12|1;2|14|2;3|14|3;4|15|3;5|15|4",
		`SELECT id, covar_pop(y, x) OVER (PARTITION BY g) FROM st ORDER BY id`:                                  "1|1;2|1;3|1;4|0;5|0",
		`SELECT id, bit_xor(b) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM st ORDER BY id`: "1|12;2|6;3|12;4|5;5|3",
		`SELECT id, any_value(g) OVER (PARTITION BY g) FROM st ORDER BY id`:                                     "1|1;2|1;3|1;4|2;5|2",
	} {
		if got, err := queryCellsSession(ctx, s, q); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", q, got, err, want)
		}
	}

	for q, code := range map[string]string{
		`SELECT corr(j, x) FROM st`:                           "42883",
		`SELECT bit_and(j) FROM st`:                           "42883",
		`SELECT bit_or(y) FROM st`:                            "42883",
		`SELECT corr(y) FROM st`:                              "42883",
		`SELECT jsonb_object_agg_unique(g, id) FROM st`:       "22030",
		`SELECT json_object_agg_unique_strict(g, id) FROM st`: "22030",
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != code {
			t.Errorf("%s: %v, want %s", q, serr, code)
		}
	}
}
