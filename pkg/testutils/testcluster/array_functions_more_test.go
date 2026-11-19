package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestArrayFunctionsMore (PG-F-012): array_dims, array_fill,
// array_replace, array_positions, array_sample, array_shuffle,
// trim_array, generate_subscripts, array_reverse and array_sort, with
// PostgreSQL's results, NULL handling and SQLSTATEs.
func TestArrayFunctionsMore(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	runExprCases(t, ctx, s, []exprCase{
		{expr: `array_dims(ARRAY[1,2,3])`, want: "[1:3]"},
		{expr: `array_dims('{a,b}'::text[])`, want: "[1:2]"},
		{expr: `array_dims('{}'::int[])`, want: "NULL"},
		{expr: `array_fill(7, ARRAY[3])`, want: "{7,7,7}"},
		{expr: `array_fill('x'::text, ARRAY[2], ARRAY[1])`, want: "{x,x}"},
		{expr: `array_fill(7, ARRAY[0])`, want: "{}"},
		{expr: `array_fill(NULL::int, ARRAY[2])`, want: "{NULL,NULL}"},
		{expr: `array_fill(1, NULL)`, code: "22004"},
		{expr: `array_fill(1, ARRAY[2,2])`, code: "0A000"},
		{expr: `array_replace(ARRAY[1,2,5,4], 5, 3)`, want: "{1,2,3,4}"},
		{expr: `array_replace(ARRAY[1,NULL,3,NULL], NULL, 0)`, want: "{1,0,3,0}"},
		{expr: `array_replace(ARRAY['a','b'], 'a', NULL)`, want: "{NULL,b}"},
		{expr: `array_replace(NULL::int[], 1, 2)`, want: "NULL"},
		{expr: `array_positions(ARRAY['A','A','B','A'], 'A')`, want: "{1,2,4}"},
		{expr: `array_positions(ARRAY[1,NULL,3,NULL], NULL)`, want: "{2,4}"},
		{expr: `array_positions(ARRAY[1,2], 9)`, want: "{}"},
		{expr: `array_positions(NULL::int[], 1)`, want: "NULL"},
		{expr: `trim_array(ARRAY[1,2,3,4,5,6], 2)`, want: "{1,2,3,4}"},
		{expr: `trim_array(ARRAY[1,2], 2)`, want: "{}"},
		{expr: `trim_array(ARRAY[1], 3)`, code: "2202E"},
		{expr: `trim_array(ARRAY[1], -1)`, code: "2202E"},
		{expr: `array_reverse(ARRAY[1,2,3,NULL])`, want: "{NULL,3,2,1}"},
		{expr: `array_sort(ARRAY[3,NULL,1,2])`, want: "{1,2,3,NULL}"},
		{expr: `array_sort(ARRAY[3,NULL,1,2], true)`, want: "{NULL,3,2,1}"},
		{expr: `array_sort(ARRAY[3,NULL,1,2], true, false)`, want: "{3,2,1,NULL}"},
		{expr: `array_sort(ARRAY[3,NULL,1,2], false, true)`, want: "{NULL,1,2,3}"},
		{expr: `array_sort(ARRAY['b','a','c'])`, want: "{a,b,c}"},
		{expr: `cardinality(array_sample(ARRAY[1,2,3,4,5], 3))`, want: "3"},
		{expr: `array_sort(array_sample(ARRAY[1,2,3], 3))`, want: "{1,2,3}"},
		{expr: `array_sample(ARRAY[1,2], 3)`, code: "22023"},
		{expr: `array_sample(ARRAY[1,2], -1)`, code: "22023"},
		{expr: `array_sort(array_shuffle(ARRAY[3,1,2]))`, want: "{1,2,3}"},
		{expr: `array_shuffle('{}'::int[])`, want: "{}"},
	})

	// The results are typed: an int8 array stays one.
	got, err := queryCellsSession(ctx, s, `SELECT pg_typeof(array_replace(ARRAY[1,2], 1, 3)), pg_typeof(array_fill(1.5, ARRAY[2])), pg_typeof(array_positions(ARRAY[1], 1))`)
	if err != nil || got != "_int8|_numeric|_int8" {
		t.Errorf("result types: %q, %v", got, err)
	}

	// generate_subscripts, in FROM and in a FROM-less select list.
	for q, want := range map[string]string{
		`SELECT * FROM generate_subscripts(ARRAY['a','b','c'], 1)`:                   "1;2;3",
		`SELECT generate_subscripts(ARRAY['a','b','c'], 1, true)`:                    "3;2;1",
		`SELECT s FROM generate_subscripts(ARRAY['a','b'], 1) AS s`:                  "1;2",
		`SELECT count(*) FROM generate_subscripts(ARRAY['a','b'], 2)`:                "0",
		`SELECT count(*) FROM generate_subscripts('{}'::text[], 1)`:                  "0",
		`SELECT (ARRAY[10,20,30])[i] FROM generate_subscripts(ARRAY[10,20,30], 1) i`: "10;20;30",
	} {
		if got, err := queryCellsSession(ctx, s, q); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", q, got, err, want)
		}
	}
}

// queryCellsSession runs a query on a session and renders every cell
// as text ("NULL" for NULL), cells joined by "|", rows by ";".
func queryCellsSession(ctx context.Context, s *sql.Session, q string) (string, error) {
	res, serr := trySQL(ctx, s, q)
	if serr != nil {
		return "", serr
	}
	rows := make([]string, len(res.Rows))
	for i, row := range res.Rows {
		cells := make([]string, len(row))
		for j, d := range row {
			cells[j] = "NULL"
			if !d.Null {
				cells[j] = d.Text()
			}
		}
		rows[i] = strings.Join(cells, "|")
	}
	return strings.Join(rows, ";"), nil
}
