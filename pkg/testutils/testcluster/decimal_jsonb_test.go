package testcluster

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// numericText renders a pgtype.Numeric (coefficient × 10^Exp) as a plain
// decimal string, so binary NUMERIC results can be compared exactly.
func numericText(t *testing.T, n pgtype.Numeric) string {
	t.Helper()
	if !n.Valid {
		return "<null>"
	}
	if n.NaN || n.Int == nil {
		t.Fatalf("unexpected numeric %+v", n)
	}
	neg := n.Int.Sign() < 0
	digits := new(big.Int).Abs(n.Int).String()
	exp := int(n.Exp)
	var s string
	switch {
	case exp >= 0:
		s = digits + strings.Repeat("0", exp)
	case len(digits) > -exp:
		s = digits[:len(digits)+exp] + "." + digits[len(digits)+exp:]
	default:
		s = "0." + strings.Repeat("0", -exp-len(digits)) + digits
	}
	// Trim trailing fraction zeros to the canonical form the server sends.
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	if s == "" {
		s = "0"
	}
	if neg && s != "0" {
		s = "-" + s
	}
	return s
}

// TestDecimalJSONBOverPgwire: DECIMAL and JSONB round-trip through a stock
// pgx client in both wire directions — real base-10000 binary NUMERIC and
// versioned binary JSONB on the extended protocol, text on the simple
// protocol — and DECIMAL aggregation stays exact where float64 breaks.
func TestDecimalJSONBOverPgwire(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Bare DECIMAL: unconstrained, values survive with full fidelity.
	// (DECIMAL(p,s) rescaling/enforcement is TestDecimalTypmodOverPgwire.)
	if _, err := conn.Exec(ctx, `CREATE TABLE items (
		id INT8 PRIMARY KEY, price DECIMAL, attrs JSONB
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Binary parameters: pgx sends NUMERIC as base-10000 groups and JSONB
	// with the version byte; the server must decode both.
	price := pgtype.Numeric{Int: big.NewInt(1234567), Exp: -3, Valid: true} // 1234.567
	if _, err := conn.Exec(ctx, `INSERT INTO items VALUES ($1, $2, $3)`,
		int64(1), price, `{"z": 1, "a": {"nested": "yes", "n": 5}}`); err != nil {
		t.Fatalf("param insert: %v", err)
	}
	// Text literals: a bare decimal literal must survive exactly, and JSONB
	// text normalizes (sorted keys, compact) on ingest.
	if _, err := conn.Exec(ctx, `INSERT INTO items VALUES
		(2, 9.99, '{"b":2,  "a": "x"}'),
		(3, -0.00012, NULL),
		(4, NULL, '{"deep": {"k": "v"}}')`); err != nil {
		t.Fatalf("literal insert: %v", err)
	}

	// Binary results.
	var gotPrice pgtype.Numeric
	var gotAttrs []byte
	if err := conn.QueryRow(ctx, `SELECT price, attrs FROM items WHERE id = 1`).
		Scan(&gotPrice, &gotAttrs); err != nil {
		t.Fatalf("binary scan: %v", err)
	}
	if s := numericText(t, gotPrice); s != "1234.567" {
		t.Fatalf("price = %s, want 1234.567", s)
	}
	if want := `{"a":{"n":5,"nested":"yes"},"z":1}`; string(gotAttrs) != want {
		t.Fatalf("attrs = %s, want %s", gotAttrs, want)
	}

	// Text results on the simple protocol.
	scfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	scfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	sconn, err := pgx.ConnectConfig(ctx, scfg)
	if err != nil {
		t.Fatalf("simple connect: %v", err)
	}
	defer func() { _ = sconn.Close(ctx) }()
	var tPrice, tAttrs string
	if err := sconn.QueryRow(ctx, `SELECT price, attrs FROM items WHERE id = 2`).
		Scan(&tPrice, &tAttrs); err != nil {
		t.Fatalf("text scan: %v", err)
	}
	if tPrice != "9.99" || tAttrs != `{"a":"x","b":2}` {
		t.Fatalf("text results: %q %q", tPrice, tAttrs)
	}

	// Negative small decimal survives both directions.
	var neg pgtype.Numeric
	if err := conn.QueryRow(ctx, `SELECT price FROM items WHERE id = 3`).Scan(&neg); err != nil {
		t.Fatalf("neg scan: %v", err)
	}
	if s := numericText(t, neg); s != "-0.00012" {
		t.Fatalf("neg = %s", s)
	}

	// NULLs stay NULL.
	var nPrice pgtype.Numeric
	var nAttrs []byte
	if err := conn.QueryRow(ctx, `SELECT price, attrs FROM items WHERE id = 3`).
		Scan(&nPrice, &nAttrs); err == nil {
		if nAttrs != nil {
			t.Fatalf("attrs not NULL: %s", nAttrs)
		}
	}

	// ->/->> over the wire: -> stays jsonb (normalized), ->> renders text.
	var deep []byte
	var deepText string
	if err := conn.QueryRow(ctx, `SELECT attrs -> 'deep', attrs -> 'deep' ->> 'k' FROM items WHERE id = 4`).
		Scan(&deep, &deepText); err != nil {
		t.Fatalf("path scan: %v", err)
	}
	if string(deep) != `{"k":"v"}` || deepText != "v" {
		t.Fatalf("path results: %s %q", deep, deepText)
	}
	// Path in WHERE, with a binary text parameter on the RHS.
	var pathID int64
	if err := conn.QueryRow(ctx, `SELECT id FROM items WHERE attrs ->> 'b' = $1`, "2").
		Scan(&pathID); err != nil || pathID != 2 {
		t.Fatalf("path where: id=%d err=%v", pathID, err)
	}

	// DECIMAL aggregation is exact where float64 is not: 0.1 + 0.2.
	if _, err := conn.Exec(ctx, `CREATE TABLE ledger (id INT8 PRIMARY KEY, d DECIMAL, f FLOAT8)`); err != nil {
		t.Fatalf("create ledger: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO ledger VALUES (1, 0.1, 0.1), (2, 0.2, 0.2)`); err != nil {
		t.Fatalf("insert ledger: %v", err)
	}
	var sumD pgtype.Numeric
	var avgD pgtype.Numeric
	var sumF float64
	if err := conn.QueryRow(ctx, `SELECT SUM(d), AVG(d), SUM(f) FROM ledger`).
		Scan(&sumD, &avgD, &sumF); err != nil {
		t.Fatalf("agg scan: %v", err)
	}
	if s := numericText(t, sumD); s != "0.3" {
		t.Fatalf("SUM(decimal) = %s, want 0.3", s)
	}
	if s := numericText(t, avgD); s != "0.15" {
		t.Fatalf("AVG(decimal) = %s, want 0.15", s)
	}
	if sumF == 0.3 {
		t.Fatalf("float SUM unexpectedly exact — the control lost its point")
	}
}

// TestDecimalKeysAndJSONBGuards: DECIMAL is fully indexable via the
// order-preserving key encoding (PK order, point plans, secondary
// indexes); JSONB is not, and says so with 0A000; path operators evaluate
// with PostgreSQL NULL semantics and stay out of key planning.
func TestDecimalKeysAndJSONBGuards(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	// DECIMAL primary key: the KV scan order IS numeric order.
	execSQL(t, ctx, s, `CREATE TABLE p (price DECIMAL PRIMARY KEY, name TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO p VALUES (10, 'ten'), (9.99, 'sub'), (-5, 'neg'),
		(0.5, 'half'), (100.25, 'big'), (0, 'zero')`)
	res := execSQL(t, ctx, s, `SELECT name FROM p`)
	var order []string
	for _, r := range res.Rows {
		order = append(order, r[0].S)
	}
	want := []string{"neg", "zero", "half", "sub", "ten", "big"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("PK scan order %v, want %v", order, want)
		}
	}
	if pl := explainPlan(t, ctx, s, `SELECT name FROM p WHERE price = 9.99`); pl != "point lookup on primary key" {
		t.Fatalf("plan: %q", pl)
	}
	res = execSQL(t, ctx, s, `SELECT name FROM p WHERE price = 9.99`)
	if len(res.Rows) != 1 || res.Rows[0][0].S != "sub" {
		t.Fatalf("point: %+v", res.Rows)
	}
	// Range predicate over the decimal PK.
	res = execSQL(t, ctx, s, `SELECT name FROM p WHERE price > 0.5 AND price <= 10`)
	if len(res.Rows) != 2 || res.Rows[0][0].S != "sub" || res.Rows[1][0].S != "ten" {
		t.Fatalf("range: %+v", res.Rows)
	}

	// Secondary index on a DECIMAL column.
	execSQL(t, ctx, s, `CREATE TABLE tx (id INT8 PRIMARY KEY, amt DECIMAL)`)
	execSQL(t, ctx, s, `INSERT INTO tx VALUES (1, 5.5), (2, -1.25), (3, 5.5)`)
	execSQL(t, ctx, s, `CREATE INDEX by_amt ON tx (amt)`)
	if pl := explainPlan(t, ctx, s, `SELECT id FROM tx WHERE amt = 5.5`); !strings.Contains(pl, `index "by_amt"`) {
		t.Fatalf("plan: %q", pl)
	}
	if res := execSQL(t, ctx, s, `SELECT id FROM tx WHERE amt = 5.5`); len(res.Rows) != 2 {
		t.Fatalf("index rows: %+v", res.Rows)
	}

	// JSONB has no ordered key encoding: PK and CREATE INDEX refuse.
	if _, serr := trySQL(ctx, s, `CREATE TABLE bad (j JSONB PRIMARY KEY)`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("jsonb PK: %+v", serr)
	}
	execSQL(t, ctx, s, `CREATE TABLE docs (id INT8 PRIMARY KEY, j JSONB)`)
	if _, serr := trySQL(ctx, s, `CREATE INDEX by_j ON docs (j)`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("jsonb index: %+v", serr)
	}

	// Path evaluation: missing keys and non-objects yield NULL; a present
	// JSON null is jsonb 'null' under -> but SQL NULL under ->>.
	execSQL(t, ctx, s, `INSERT INTO docs VALUES
		(1, '{"a": {"b": 7}, "s": "txt", "z": null}'),
		(2, '[1, 2, 3]'),
		(3, NULL)`)
	res = execSQL(t, ctx, s, `SELECT j -> 'a' ->> 'b', j ->> 'missing', j -> 'z', j ->> 'z', j ->> 's' FROM docs WHERE id = 1`)
	r := res.Rows[0]
	if r[0].S != "7" || !r[1].Null || r[2].S != "null" || r[2].Null || !r[3].Null || r[4].S != "txt" {
		t.Fatalf("paths: %+v", r)
	}
	res = execSQL(t, ctx, s, `SELECT j ->> 'a' FROM docs WHERE id = 2`)
	if !res.Rows[0][0].Null {
		t.Fatalf("array extraction: %+v", res.Rows)
	}
	// WHERE on paths: NULL results never match; IS NULL sees them.
	res = execSQL(t, ctx, s, `SELECT id FROM docs WHERE j -> 'a' ->> 'b' = '7'`)
	if len(res.Rows) != 1 || res.Rows[0][0].I != 1 {
		t.Fatalf("path where: %+v", res.Rows)
	}
	res = execSQL(t, ctx, s, `SELECT id FROM docs WHERE j ->> 'missing' IS NULL AND j IS NOT NULL`)
	if len(res.Rows) != 2 {
		t.Fatalf("path IS NULL: %+v", res.Rows)
	}

	// Paths demand jsonb, and stay out of join queries (documented v1 cut).
	if _, serr := trySQL(ctx, s, `SELECT id ->> 'k' FROM docs`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("path on int: %+v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT d.id FROM docs AS d JOIN tx AS x ON x.id = d.id WHERE d.j ->> 'k' = 'v'`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("path in join: %+v", serr)
	}

	// Exact decimal arithmetic in expressions: 0.1 + 0.2 = 0.3.
	res = execSQL(t, ctx, s, `SELECT amt + 0.1 FROM tx WHERE id = 2`)
	if d := res.Rows[0][0]; d.Fam != types.Decimal || d.S != "-1.15" {
		t.Fatalf("arith: %+v", d)
	}
}

// benchSum builds a 4k-row table with the given column type and measures
// whole-table SUM per iteration — the decimal-vs-int aggregation overhead,
// with identical scan cost on both sides.
func benchSum(b *testing.B, colType, lit string) {
	tc := Start(b, 1)
	ctx := context.Background()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	if _, serr := trySQL(ctx, s, `CREATE TABLE bs (id INT8 PRIMARY KEY, v `+colType+`)`); serr != nil {
		b.Fatalf("create: %v", serr)
	}
	for i := 0; i < 4096; i += 256 {
		var sb strings.Builder
		sb.WriteString(`INSERT INTO bs VALUES `)
		for j := 0; j < 256; j++ {
			if j > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "(%d, %s)", i+j, lit)
		}
		if _, serr := trySQL(ctx, s, sb.String()); serr != nil {
			b.Fatalf("insert: %v", serr)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, serr := trySQL(ctx, s, `SELECT SUM(v) FROM bs`); serr != nil {
			b.Fatal(serr)
		}
	}
}

func BenchmarkSumDecimal(b *testing.B) { benchSum(b, "DECIMAL", "123.456789") }
func BenchmarkSumInt(b *testing.B)     { benchSum(b, "INT8", "123456789") }
