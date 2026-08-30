package testcluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestColumnTypesOverPgwire: TIMESTAMPTZ, DATE, BYTEA, and UUID round-trip
// through a stock pgx client in both wire directions — binary parameters
// and results on the extended protocol, text rendering on the simple
// protocol — and compose with indexes, range predicates, and ORDER BY.
func TestColumnTypesOverPgwire(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE events (
		id INT8 PRIMARY KEY, at TIMESTAMPTZ, day DATE, payload BYTEA, tag UUID
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	at := time.Date(2026, 8, 30, 1, 2, 3, 123456000, time.UTC) // micro precision
	day := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	payload := []byte{0x00, 0xde, 0xad, 0x00, 0xff}
	const tag = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"

	// Binary parameters (pgx encodes against our described OIDs).
	if _, err := conn.Exec(ctx, `INSERT INTO events VALUES ($1, $2, $3, $4, $5)`,
		int64(1), at, day, payload, tag); err != nil {
		t.Fatalf("param insert: %v", err)
	}
	// Text literals through SQL.
	if _, err := conn.Exec(ctx, `INSERT INTO events VALUES
		(2, '2026-08-30 02:00:00Z', '2026-08-31', '\xbeef', 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
		(3, NULL, NULL, NULL, NULL)`); err != nil {
		t.Fatalf("literal insert: %v", err)
	}

	// Binary results (extended protocol defaults).
	var gotAt, gotDay time.Time
	var gotPayload []byte
	var gotTag [16]byte
	if err := conn.QueryRow(ctx, `SELECT at, day, payload, tag FROM events WHERE id = 1`).
		Scan(&gotAt, &gotDay, &gotPayload, &gotTag); err != nil {
		t.Fatalf("binary scan: %v", err)
	}
	if !gotAt.Equal(at) {
		t.Fatalf("at = %v, want %v", gotAt, at)
	}
	if !gotDay.Equal(day) {
		t.Fatalf("day = %v, want %v", gotDay, day)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload = %x", gotPayload)
	}
	wantTag, _ := parseUUIDString(tag)
	if gotTag != wantTag {
		t.Fatalf("tag = %x", gotTag)
	}

	// Text results (simple protocol exercises Datum.Text rendering against
	// pgx's PostgreSQL-format parsers).
	scfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	scfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	sconn, err := pgx.ConnectConfig(ctx, scfg)
	if err != nil {
		t.Fatalf("simple connect: %v", err)
	}
	defer func() { _ = sconn.Close(ctx) }()
	var tAt, tDay time.Time
	var tPayload []byte
	var tTag string
	if err := sconn.QueryRow(ctx, `SELECT at, day, payload, tag FROM events WHERE id = 1`).
		Scan(&tAt, &tDay, &tPayload, &tTag); err != nil {
		t.Fatalf("text scan: %v", err)
	}
	if !tAt.Equal(at) || !tDay.Equal(day) || !bytes.Equal(tPayload, payload) || tTag != tag {
		t.Fatalf("text results: %v %v %x %s", tAt, tDay, tPayload, tTag)
	}

	// NULLs stay NULL for every new type.
	var nAt, nDay *time.Time
	var nPayload []byte
	var nTag *string
	if err := conn.QueryRow(ctx, `SELECT at, day, payload, tag FROM events WHERE id = 3`).
		Scan(&nAt, &nDay, &nPayload, &nTag); err != nil {
		t.Fatalf("null scan: %v", err)
	}
	if nAt != nil || nDay != nil || nPayload != nil || nTag != nil {
		t.Fatalf("nulls: %v %v %v %v", nAt, nDay, nPayload, nTag)
	}

	// Timestamp ORDER BY and a binary timestamptz parameter in WHERE.
	rows, err := conn.Query(ctx, `SELECT id FROM events WHERE at >= $1 ORDER BY at DESC`,
		time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("range query: %v", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 1 {
		t.Fatalf("ordered ids: %v", ids)
	}
}

func parseUUIDString(s string) ([16]byte, error) {
	var out [16]byte
	n := 0
	for i := 0; i < len(s) && n < 32; i++ {
		c := s[i]
		if c == '-' {
			continue
		}
		var v byte
		switch {
		case c >= '0' && c <= '9':
			v = c - '0'
		case c >= 'a' && c <= 'f':
			v = c - 'a' + 10
		}
		if n%2 == 0 {
			out[n/2] = v << 4
		} else {
			out[n/2] |= v
		}
		n++
	}
	return out, nil
}

// TestTimestampPlanning: string literals coerce into timestamp key bounds,
// on both the primary key and a secondary index.
func TestTimestampPlanning(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE m (series INT, at TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (series, at))`)
	for h := 0; h < 6; h++ {
		execSQL(t, ctx, s, `INSERT INTO m VALUES (1, $1, 0.5)`,
			mustTS(t, 2026, 8, 30, h))
		execSQL(t, ctx, s, `INSERT INTO m VALUES (2, $1, 1.5)`,
			mustTS(t, 2026, 8, 30, h))
	}

	// (series, time-range) is a bounded PK scan — the time-series shape.
	q := `SELECT v FROM m WHERE series = 1 AND at >= '2026-08-30 02:00:00Z' AND at < '2026-08-30 04:00:00Z'`
	p := explainPlan(t, ctx, s, q)
	want := "range scan of primary key (series = 1, at >= 2026-08-30 02:00:00+00, at < 2026-08-30 04:00:00+00)"
	if p != want {
		t.Fatalf("plan: %q", p)
	}
	if res := execSQL(t, ctx, s, q); len(res.Rows) != 2 {
		t.Fatalf("rows: %+v", res.Rows)
	}

	// Aggregates over the bounded window.
	res := execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM m WHERE series = 2 AND at >= '2026-08-30 03:00:00Z'`)
	if res.Rows[0][0].I != 3 {
		t.Fatalf("count: %+v", res.Rows)
	}
}

func mustTS(t *testing.T, y int, mo time.Month, d, h int) types.Datum {
	t.Helper()
	return types.NewTimestamp(time.Date(y, mo, d, h, 0, 0, 0, time.UTC).UnixNano())
}

// TestColumnDefaults: CREATE TABLE defaults, ADD COLUMN ... DEFAULT
// [NOT NULL] with fill-on-read for pre-existing rows, and the
// NULL-vs-missing distinction for rows written after the ADD.
func TestColumnDefaults(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE cfg (id INT PRIMARY KEY, retries INT DEFAULT 3, note TEXT DEFAULT 'none')`)
	execSQL(t, ctx, s, `INSERT INTO cfg (id) VALUES (1)`)
	execSQL(t, ctx, s, `INSERT INTO cfg VALUES (2, NULL, NULL)`)
	execSQL(t, ctx, s, `INSERT INTO cfg VALUES (3, 9, 'x')`)

	res := execSQL(t, ctx, s, `SELECT retries, note FROM cfg ORDER BY id`)
	if res.Rows[0][0].I != 3 || res.Rows[0][1].S != "none" {
		t.Fatalf("defaults: %+v", res.Rows[0])
	}
	if !res.Rows[1][0].Null || !res.Rows[1][1].Null {
		t.Fatalf("explicit NULL overridden: %+v", res.Rows[1])
	}
	if res.Rows[2][0].I != 9 {
		t.Fatalf("explicit value: %+v", res.Rows[2])
	}

	// ADD COLUMN with DEFAULT NOT NULL: existing rows fill on read.
	execSQL(t, ctx, s, `ALTER TABLE cfg ADD COLUMN level INT DEFAULT 5 NOT NULL`)
	res = execSQL(t, ctx, s, `SELECT level FROM cfg ORDER BY id`)
	for i, r := range res.Rows {
		if r[0].Null || r[0].I != 5 {
			t.Fatalf("row %d level = %+v, want filled 5", i, r[0])
		}
	}
	// The filled column participates in WHERE.
	if res := execSQL(t, ctx, s, `SELECT id FROM cfg WHERE level = 5`); len(res.Rows) != 3 {
		t.Fatalf("filled where: %+v", res.Rows)
	}
	// New inserts: omitted → default; explicit NULL → 23502 (NOT NULL).
	execSQL(t, ctx, s, `INSERT INTO cfg (id) VALUES (4)`)
	if res := execSQL(t, ctx, s, `SELECT level FROM cfg WHERE id = 4`); res.Rows[0][0].I != 5 {
		t.Fatalf("new row level: %+v", res.Rows)
	}
	if _, serr := trySQL(ctx, s, `INSERT INTO cfg VALUES (5, 1, 'y', NULL)`); serr == nil || serr.Code != sql.CodeNotNullViolation {
		t.Fatalf("null into NOT NULL: %v", serr)
	}

	// A nullable ADD ... DEFAULT keeps NULL and default distinguishable.
	execSQL(t, ctx, s, `ALTER TABLE cfg ADD COLUMN opt INT DEFAULT 7`)
	execSQL(t, ctx, s, `INSERT INTO cfg (id, opt) VALUES (6, NULL)`)
	res = execSQL(t, ctx, s, `SELECT opt FROM cfg WHERE id = 1`)
	if res.Rows[0][0].Null || res.Rows[0][0].I != 7 {
		t.Fatalf("old row opt = %+v, want 7", res.Rows[0][0])
	}
	res = execSQL(t, ctx, s, `SELECT opt FROM cfg WHERE id = 6`)
	if !res.Rows[0][0].Null {
		t.Fatalf("explicit NULL opt = %+v", res.Rows[0][0])
	}

	// NOT NULL without a DEFAULT still has no value for existing rows.
	if _, serr := trySQL(ctx, s, `ALTER TABLE cfg ADD COLUMN bad INT NOT NULL`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("NOT NULL without DEFAULT: %v", serr)
	}
	// Typed defaults coerce (and bad ones are rejected).
	execSQL(t, ctx, s, `ALTER TABLE cfg ADD COLUMN since TIMESTAMPTZ DEFAULT '2026-01-01 00:00:00Z'`)
	res = execSQL(t, ctx, s, `SELECT since FROM cfg WHERE id = 1`)
	if res.Rows[0][0].Fam != types.Timestamp {
		t.Fatalf("typed default: %+v", res.Rows[0][0])
	}
	if _, serr := trySQL(ctx, s, `ALTER TABLE cfg ADD COLUMN oops INT DEFAULT 'zzz'`); serr == nil {
		t.Fatal("un-coercible default accepted")
	}
}
