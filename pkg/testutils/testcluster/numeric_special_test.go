package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestNumericNaNAndInfinity (PG-T-013): numeric's NaN, Infinity and
// -Infinity — input in PostgreSQL's spellings, output, comparison and
// ordering (NaN after everything, NULL after NaN), index scans,
// arithmetic, the aggregates, NUMERIC(p,s), casts to and from float8
// and integers, and the binary wire form (the NUMERIC_NAN / PINF /
// NINF sign words, both directions).
func TestNumericNaNAndInfinity(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol // text results
	text, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer text.Close(ctx)
	exec := func(q string) {
		t.Helper()
		if _, err := text.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`CREATE TABLE ns (id INT PRIMARY KEY, x NUMERIC, y NUMERIC(5,2))`)
	exec(`CREATE INDEX ns_x ON ns (x)`)
	exec(`INSERT INTO ns VALUES (1, 'NaN', 'NaN'), (2, 'Infinity', 1), (3, '-Infinity', 2), (4, 5, 3), (5, NULL, NULL), (6, '-inf', 4), (7, 'nan', 5)`)

	for q, want := range map[string]string{
		`SELECT 'NaN'::numeric, 'Infinity'::numeric, '-inf'::numeric, ' +INFINITY '::numeric`:                         "NaN|Infinity|-Infinity|Infinity",
		`SELECT 'NaN'::numeric = 'nan'::numeric, 'NaN'::numeric > 'Infinity'::numeric, '-Infinity'::numeric < -1e100`: "t|t|t",
		`SELECT 'Infinity'::numeric = 'Infinity'::float8, 'NaN'::numeric = 'NaN'::float8, 1::numeric < 'Infinity'`:    "t|t|t",
		`SELECT x FROM ns ORDER BY x, id`:                                      "-Infinity;-Infinity;5;Infinity;NaN;NaN;NULL",
		`SELECT x FROM ns ORDER BY x DESC, id`:                                 "NULL;NaN;NaN;Infinity;5;-Infinity;-Infinity",
		`SELECT id FROM ns WHERE x > 1000 ORDER BY id`:                         "1;2;7",
		`SELECT id FROM ns WHERE x < 0 ORDER BY id`:                            "3;6",
		`SELECT id FROM ns WHERE x = 'NaN' ORDER BY id`:                        "1;7",
		`SELECT id FROM ns WHERE x = 'Infinity'`:                               "2",
		`SELECT count(DISTINCT x), count(x) FROM ns`:                           "4|6",
		`SELECT x, count(*) FROM ns WHERE x IS NOT NULL GROUP BY x ORDER BY x`: "-Infinity|2;5|1;Infinity|1;NaN|2",
		`SELECT y FROM ns ORDER BY id`:                                         "NaN;1.00;2.00;3.00;NULL;4.00;5.00",
		// Arithmetic.
		`SELECT 'Infinity'::numeric + 1, 'Infinity'::numeric - 'Infinity'::numeric, 'Infinity'::numeric * 0, 1 / 'Infinity'::numeric`:    "Infinity|NaN|NaN|0",
		`SELECT 'NaN'::numeric / 0, 0 - 'Infinity'::numeric, 5 % 'Infinity'::numeric, 'Infinity'::numeric % 5, 'Infinity'::numeric * -2`: "NaN|-Infinity|5|NaN|-Infinity",
		`SELECT 'NaN'::numeric + 1, 2.5 * 'NaN'::numeric, 'Infinity'::numeric / 'Infinity'::numeric`:                                     "NaN|NaN|NaN",
		// Functions.
		`SELECT abs('-Infinity'::numeric), round('NaN'::numeric, 2), floor('Infinity'::numeric), ceil('-Infinity'::numeric), sign('-Infinity'::numeric)`: "Infinity|NaN|Infinity|-Infinity|-1",
		`SELECT to_jsonb('NaN'::numeric), to_jsonb('-Infinity'::numeric)`:                                                                                `"NaN"|"-Infinity"`,
		// Aggregates: NaN wins; Infinity and -Infinity together are NaN.
		`SELECT sum(x), avg(x), min(x), max(x) FROM ns`:    "NaN|NaN|-Infinity|NaN",
		`SELECT sum(x), avg(x) FROM ns WHERE id IN (2, 4)`: "Infinity|Infinity",
		`SELECT sum(x) FROM ns WHERE id IN (2, 3)`:         "NaN",
		`SELECT sum(x), avg(x) FROM ns WHERE id IN (3, 6)`: "-Infinity|-Infinity",
		`SELECT max(x) FROM ns WHERE id IN (2, 3, 4)`:      "Infinity",
		// Casts.
		`SELECT 'NaN'::numeric::float8, 'Infinity'::float8::numeric, '-Infinity'::numeric::float8, 'NaN'::float8::numeric`: "NaN|Infinity|-Infinity|NaN",
		`SELECT 'NaN'::numeric::text, 'Infinity'::text::numeric`:                                                           "NaN|Infinity",
	} {
		if got, err := queryCells(t, ctx, text, q); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", q, got, err, want)
		}
	}
	for q, code := range map[string]string{
		`SELECT 'Infinity'::numeric / 0`:                        "22012",
		`SELECT 'Infinity'::numeric % 0`:                        "22012",
		`SELECT 'NaN'::numeric::int8`:                           "0A000",
		`SELECT 'Infinity'::numeric::int4`:                      "0A000",
		`SELECT '-Infinity'::numeric::int2`:                     "0A000",
		`SELECT 'Infinity'::numeric(5,2)`:                       "22003",
		`INSERT INTO ns VALUES (8, 1, 'Infinity')`:              "22003",
		`SELECT 'nanx'::numeric`:                                "22P02",
		`SELECT * FROM generate_series(1, 'Infinity'::numeric)`: "22023",
		`SELECT * FROM generate_series('NaN'::numeric, 1)`:      "22023",
	} {
		if _, err := queryCells(t, ctx, text, q); pgCode(err) != code {
			t.Errorf("%s: %v, want %s", q, err, code)
		}
	}

	// The binary wire form, out and in.
	bin, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer bin.Close(ctx)
	var nan, pinf, ninf pgtype.Numeric
	if err := bin.QueryRow(ctx, `SELECT x FROM ns WHERE id = 1`).Scan(&nan); err != nil || !nan.NaN {
		t.Fatalf("binary NaN: %+v, %v", nan, err)
	}
	if err := bin.QueryRow(ctx, `SELECT 'Infinity'::numeric, '-Infinity'::numeric`).Scan(&pinf, &ninf); err != nil ||
		pinf.InfinityModifier != pgtype.Infinity || ninf.InfinityModifier != pgtype.NegativeInfinity {
		t.Fatalf("binary infinities: %+v %+v, %v", pinf, ninf, err)
	}
	var back string
	if err := bin.QueryRow(ctx, `SELECT $1::numeric::text || ',' || $2::numeric::text || ',' || $3::numeric::text`,
		pgtype.Numeric{NaN: true, Valid: true}, pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true},
		pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}).Scan(&back); err != nil || back != "NaN,Infinity,-Infinity" {
		t.Fatalf("binary parameters: %q, %v", back, err)
	}
}
