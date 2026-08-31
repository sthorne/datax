package testcluster

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// expectCode runs q and asserts it fails with the given SQLSTATE.
func expectCode(t *testing.T, ctx context.Context, conn *pgx.Conn, q, code string) {
	t.Helper()
	_, err := conn.Exec(ctx, q)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		t.Fatalf("%s: got %v, want SQLSTATE %s", q, err, code)
	}
}

// TestDecimalTypmodOverPgwire: DECIMAL(p,s) is enforced — values rescale
// to the declared scale (round-half-even) on every ingest path, integer
// overflow is SQLSTATE 22003, and stored values render with the fixed
// declared scale in both wire formats. Issue #39.
func TestDecimalTypmodOverPgwire(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE prices (
		id INT8 PRIMARY KEY, amt DECIMAL(10,2), note TEXT DEFAULT 'x'
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Rescale on insert: literals and a binary NUMERIC parameter.
	if _, err := conn.Exec(ctx, `INSERT INTO prices (id, amt) VALUES
		(1, 9.9), (2, 1.005), (3, 1.015), (4, -2.5), (5, NULL)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	binParam := pgtype.Numeric{Int: big.NewInt(1234567), Exp: -3, Valid: true} // 1234.567
	if _, err := conn.Exec(ctx, `INSERT INTO prices (id, amt) VALUES ($1, $2)`,
		int64(6), binParam); err != nil {
		t.Fatalf("binary param insert: %v", err)
	}

	// Fixed-scale rendering, text format (simple protocol).
	scfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	scfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	sconn, err := pgx.ConnectConfig(ctx, scfg)
	if err != nil {
		t.Fatalf("simple connect: %v", err)
	}
	defer func() { _ = sconn.Close(ctx) }()
	for _, c := range []struct {
		id   int64
		want string
	}{
		{1, "9.90"},  // padded
		{2, "1.00"},  // 1.005 ties to even
		{3, "1.02"},  // 1.015 ties to even
		{4, "-2.50"}, // negative padded
		{6, "1234.57"} /* binary param rescaled */} {
		var got string
		if err := sconn.QueryRow(ctx, `SELECT amt FROM prices WHERE id = $1`, c.id).Scan(&got); err != nil {
			t.Fatalf("id %d: %v", c.id, err)
		}
		if got != c.want {
			t.Fatalf("id %d: rendered %q, want %q", c.id, got, c.want)
		}
	}

	// Binary format: the wire dscale is the declared scale, and the value
	// round-trips exactly.
	var n pgtype.Numeric
	if err := conn.QueryRow(ctx, `SELECT amt FROM prices WHERE id = 1`).Scan(&n); err != nil {
		t.Fatalf("binary scan: %v", err)
	}
	if s := numericText(t, n); s != "9.9" {
		t.Fatalf("binary value = %s, want 9.9", s)
	}

	// RowDescription carries the typmod ((p<<16)|(s+4)).
	rows, err := conn.Query(ctx, `SELECT amt FROM prices WHERE id = 1`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	fd := rows.FieldDescriptions()
	rows.Close()
	if want := int32(10<<16 | (2 + 4)); len(fd) != 1 || fd[0].TypeModifier != want {
		t.Fatalf("typmod = %d, want %d", fd[0].TypeModifier, want)
	}

	// Overflow: 22003 on INSERT and UPDATE; rounding that overflows counts.
	expectCode(t, ctx, conn, `INSERT INTO prices (id, amt) VALUES (100, 123456789.5)`, "22003")
	expectCode(t, ctx, conn, `UPDATE prices SET amt = 99999999999.99 WHERE id = 1`, "22003")

	// UPDATE rescales too.
	if _, err := conn.Exec(ctx, `UPDATE prices SET amt = 3.999 WHERE id = 1`); err != nil {
		t.Fatalf("update: %v", err)
	}
	var upd string
	if err := sconn.QueryRow(ctx, `SELECT amt FROM prices WHERE id = 1`).Scan(&upd); err != nil || upd != "4.00" {
		t.Fatalf("update rescale: %q, %v", upd, err)
	}

	// NULL passes through.
	var nn pgtype.Numeric
	if err := conn.QueryRow(ctx, `SELECT amt FROM prices WHERE id = 5`).Scan(&nn); err != nil {
		t.Fatalf("null scan: %v", err)
	}
	if nn.Valid {
		t.Fatalf("null amt came back non-null: %+v", nn)
	}

	// Bare DECIMAL stays unconstrained and canonical.
	if _, err := conn.Exec(ctx, `CREATE TABLE freeform (id INT8 PRIMARY KEY, d DECIMAL)`); err != nil {
		t.Fatalf("create freeform: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO freeform VALUES (1, 1234.56789)`); err != nil {
		t.Fatalf("insert freeform: %v", err)
	}
	var free string
	if err := sconn.QueryRow(ctx, `SELECT d FROM freeform WHERE id = 1`).Scan(&free); err != nil || free != "1234.56789" {
		t.Fatalf("bare decimal: %q, %v", free, err)
	}

	// DEFAULT is validated at DDL time and rescaled on use.
	if _, err := conn.Exec(ctx, `ALTER TABLE prices ADD COLUMN fee DECIMAL(6,2) DEFAULT 0.125`); err != nil {
		t.Fatalf("add column: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO prices (id, amt) VALUES (7, 1)`); err != nil {
		t.Fatalf("insert with default: %v", err)
	}
	var fee string
	if err := sconn.QueryRow(ctx, `SELECT fee FROM prices WHERE id = 7`).Scan(&fee); err != nil || fee != "0.12" {
		t.Fatalf("default fee: %q, %v", fee, err)
	}
	expectCode(t, ctx, conn, `CREATE TABLE bad (id INT8 PRIMARY KEY, d DECIMAL(3,2) DEFAULT 99)`, "22003")
}

// TestDecimalTypmodKeysAndCopy: typmod enforcement runs before key
// encoding — a DECIMAL(p,s) primary key stores and looks up the quantized
// value (rounding collisions are 23505) — and COPY enforces it per row.
func TestDecimalTypmodKeysAndCopy(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE rates (
		r DECIMAL(6,2) PRIMARY KEY, label TEXT
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO rates VALUES (1.5, 'a'), (2.75, 'b')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Point lookup finds the quantized key by any spelling of the value.
	var label string
	if err := conn.QueryRow(ctx, `SELECT label FROM rates WHERE r = 1.50`).Scan(&label); err != nil || label != "a" {
		t.Fatalf("point lookup: %q, %v", label, err)
	}
	// Rounding collision: 1.501 quantizes to 1.50 which exists → 23505.
	expectCode(t, ctx, conn, `INSERT INTO rates VALUES (1.501, 'dup')`, "23505")
	// ORDER BY over quantized keys, fixed-scale rendering.
	scfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	scfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	sconn, err := pgx.ConnectConfig(ctx, scfg)
	if err != nil {
		t.Fatalf("simple connect: %v", err)
	}
	defer func() { _ = sconn.Close(ctx) }()
	srows, err := sconn.Query(ctx, `SELECT r FROM rates ORDER BY r DESC`)
	if err != nil {
		t.Fatalf("order by: %v", err)
	}
	var got []string
	for srows.Next() {
		var s string
		if err := srows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		got = append(got, s)
	}
	srows.Close()
	if strings.Join(got, ",") != "2.75,1.50" {
		t.Fatalf("order by: %v", got)
	}

	// Secondary index on a typmod column indexes the quantized value.
	if _, err := conn.Exec(ctx, `CREATE TABLE fees (
		id INT8 PRIMARY KEY, f DECIMAL(8,3)
	)`); err != nil {
		t.Fatalf("create fees: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE INDEX by_f ON fees (f)`); err != nil {
		t.Fatalf("index: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO fees VALUES (1, 0.0004), (2, 12.3456)`); err != nil {
		t.Fatalf("insert fees: %v", err)
	}
	var id int64
	if err := conn.QueryRow(ctx, `SELECT id FROM fees WHERE f = 12.346`).Scan(&id); err != nil || id != 2 {
		t.Fatalf("index lookup: %d, %v", id, err)
	}

	// COPY: rescale applies per row; an overflowing row fails with 22003.
	n, err := conn.CopyFrom(ctx, pgx.Identifier{"fees"}, []string{"id", "f"},
		pgx.CopyFromRows([][]any{
			{int64(10), pgtype.Numeric{Int: big.NewInt(15), Exp: -1, Valid: true}}, // 1.5 → 1.500
		}))
	if err != nil || n != 1 {
		t.Fatalf("copy: %d, %v", n, err)
	}
	var copied string
	if err := sconn.QueryRow(ctx, `SELECT f FROM fees WHERE id = 10`).Scan(&copied); err != nil || copied != "1.500" {
		t.Fatalf("copied: %q, %v", copied, err)
	}
	_, err = conn.CopyFrom(ctx, pgx.Identifier{"fees"}, []string{"id", "f"},
		pgx.CopyFromRows([][]any{
			{int64(11), pgtype.Numeric{Int: big.NewInt(999999), Exp: 3, Valid: true}}, // 999999000 overflows (8,3)
		}))
	if err == nil || !strings.Contains(err.Error(), "22003") && !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("copy overflow: %v", err)
	}
}
