package testcluster

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestFloatAndLiteralSemanticsOverPgwire: PostgreSQL 16's answers, with
// their wire types, for round(double precision) (rint: halves to even),
// a negated zero cast to float8 (-0), avg over int8 beyond int8's range
// (numeric, no error) and integer literals beyond int8 (numeric).
func TestFloatAndLiteralSemanticsOverPgwire(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE big (id INT8 PRIMARY KEY, v INT8)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO big VALUES (1, 9223372036854775807), (2, 9223372036854775807), (3, 1)`); err != nil {
		t.Fatal(err)
	}

	const (
		oidInt8    = 20
		oidFloat8  = 701
		oidText    = 25
		oidNumeric = 1700
	)
	// Text results, as the simple protocol returns them.
	for _, c := range []struct {
		q    string
		oids []uint32
		want string
	}{
		{`SELECT round(2.5::float8), round(-0.5::float8), round(3.5::float8), round(-2.5::float8)`,
			[]uint32{oidFloat8, oidFloat8, oidFloat8, oidFloat8}, "2|-0|4|-2"},
		{`SELECT -0.0::float8, -0::float8, (-0.0::float8)::text`, []uint32{oidFloat8, oidFloat8, oidText}, "-0|-0|-0"},
		{`SELECT avg(v) FROM big`, []uint32{oidNumeric}, "6148914691236517205"},
		{`SELECT 99999999999999999999, -9223372036854775808, 9223372036854775808`,
			[]uint32{oidNumeric, oidInt8, oidNumeric}, "99999999999999999999|-9223372036854775808|9223372036854775808"},
	} {
		res, err := conn.PgConn().Exec(ctx, c.q).ReadAll()
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		var oids []uint32
		for _, f := range res[0].FieldDescriptions {
			oids = append(oids, f.DataTypeOID)
		}
		var vals []string
		for _, v := range res[0].Rows[0] {
			vals = append(vals, string(v))
		}
		if got := strings.Join(vals, "|"); got != c.want || len(oids) != len(c.oids) {
			t.Errorf("%s: got %q (types %v), want %q (types %v)", c.q, got, oids, c.want, c.oids)
			continue
		}
		for i := range oids {
			if oids[i] != c.oids[i] {
				t.Errorf("%s: column %d has type %d, want %d", c.q, i+1, oids[i], c.oids[i])
			}
		}
	}

	// Binary float8 results keep the sign of zero too.
	var r, z float64
	if err := conn.QueryRow(ctx, `SELECT round(-0.5::float8), -0.0::float8`).Scan(&r, &z); err != nil {
		t.Fatal(err)
	}
	if r != 0 || !math.Signbit(r) || z != 0 || !math.Signbit(z) {
		t.Errorf("binary: round(-0.5::float8) = %v, -0.0::float8 = %v; want -0, -0", r, z)
	}
}
