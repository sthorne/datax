package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestNumericDivisionScaleOverPgwire: numeric `/` and avg(numeric) use
// PostgreSQL's result scale (select_div_scale: at least 16 significant
// digits, at least either operand's scale), not a fixed 6 places that
// turned 0.0000005 / 1 into 0. The expected texts are PostgreSQL 16's,
// trailing zeros included; the binary form carries the same scale, and
// a filter on a small quotient keeps its rows.
func TestNumericDivisionScaleOverPgwire(t *testing.T) {
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
	bin, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer bin.Close(ctx)

	if _, err := text.Exec(ctx, `CREATE TABLE nd (id INT8 PRIMARY KEY, x NUMERIC, y NUMERIC(30,2))`); err != nil {
		t.Fatal(err)
	}
	if _, err := text.Exec(ctx, `INSERT INTO nd VALUES (1, 0.0000001, 12345678901234567890.1), (2, 0.0000002, 1), (3, 0.0000009, 2)`); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct{ q, want string }{
		{`SELECT 0.0000005::numeric / 1`, "0.000000500000000000000000"},
		{`SELECT 0.0000001::numeric / 3`, "0.000000033333333333333333"},
		{`SELECT 1::numeric / 3`, "0.33333333333333333333"},
		{`SELECT 7 / 2.0`, "3.5000000000000000"},
		{`SELECT avg(x) FROM (VALUES (0.0000001::numeric),(0.0000002::numeric)) v(x)`, "0.000000150000000000000000"},
		{`SELECT avg(x) FROM (VALUES (1::numeric),(2::numeric),(2::numeric)) v(x)`, "1.6666666666666667"},
		{`SELECT avg(x) FROM (VALUES (1),(2),(2)) v(x)`, "1.6666666666666667"},
		{`SELECT avg(x) FROM nd WHERE id < 3`, "0.000000150000000000000000"},
		{`SELECT y / 1 FROM nd WHERE id = 1`, "12345678901234567890.10"}, // the column's scale 2
		{`SELECT count(*) FROM nd WHERE x / 3 > 0.0000001`, "1"},
	} {
		var got string
		if err := text.QueryRow(ctx, c.q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		if got != c.want {
			t.Errorf("%s: got %s, want %s", c.q, got, c.want)
		}
	}

	// Binary NUMERIC: the dscale on the wire is the result scale.
	var n pgtype.Numeric
	if err := bin.QueryRow(ctx, `SELECT 1::numeric / 3`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if !n.Valid || n.Exp != -20 || n.Int.String() != "33333333333333333333" {
		t.Errorf("binary 1/3: got %s×10^%d, want 33333333333333333333×10^-20", n.Int, n.Exp)
	}

	expectCode(t, ctx, text, `SELECT 1::numeric / 0`, "22012")
	expectCode(t, ctx, text, `SELECT x / 0 FROM nd`, "22012")
}
