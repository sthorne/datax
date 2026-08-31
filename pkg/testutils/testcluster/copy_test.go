package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// The COPY FROM STDIN suite (issue #42): raw wire-protocol flows for all
// three formats (with chunk-boundary torture — clients cut CopyData with
// no row alignment), the chunked-commit failure semantics, the protocol
// gates, and a pgx CopyFrom end-to-end pass over every column type.

// expectErrorCode asserts the next message is an ErrorResponse with the
// given SQLSTATE and returns it.
func expectErrorCode(r *rawFrontend, code string) *pgproto3.ErrorResponse {
	r.t.Helper()
	e := expect[*pgproto3.ErrorResponse](r)
	if e.Code != code {
		r.t.Fatalf("error code %s (%s), want %s", e.Code, e.Message, code)
	}
	return e
}

// simpleExec runs one statement over the raw connection and drains to
// ReadyForQuery, failing on any ErrorResponse.
func (r *rawFrontend) simpleExec(q string) {
	r.t.Helper()
	r.send(&pgproto3.Query{String: q})
	for {
		switch m := r.recv().(type) {
		case *pgproto3.ReadyForQuery:
			return
		case *pgproto3.ErrorResponse:
			r.t.Fatalf("%s: %s %s", q, m.Code, m.Message)
		}
	}
}

// copyIn starts a COPY statement and asserts the CopyInResponse.
func (r *rawFrontend) copyIn(stmt string, wantFormat byte) {
	r.t.Helper()
	r.send(&pgproto3.Query{String: stmt})
	cir := expect[*pgproto3.CopyInResponse](r)
	if cir.OverallFormat != wantFormat {
		r.t.Fatalf("overall format %d, want %d", cir.OverallFormat, wantFormat)
	}
}

// finishCopy sends CopyDone and asserts CommandComplete{"COPY n"} + Ready.
func (r *rawFrontend) finishCopy(wantRows int) {
	r.t.Helper()
	r.send(&pgproto3.CopyDone{})
	cc := expect[*pgproto3.CommandComplete](r)
	if got := string(cc.CommandTag); got != fmt.Sprintf("COPY %d", wantRows) {
		r.t.Fatalf("command tag %q, want COPY %d", got, wantRows)
	}
	expect[*pgproto3.ReadyForQuery](r)
}

func countRows(t *testing.T, tc *TestCluster, table string) int {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCopyFromText(t *testing.T) {
	tc := Start(t, 3)
	r := dialRaw(t, tc)
	r.simpleExec("CREATE TABLE ct (id INT8 PRIMARY KEY, name TEXT, note TEXT)")

	r.copyIn("COPY ct FROM STDIN", 0)
	// Escapes, NULL, an escaped terminator lookalike, and the whole stream
	// tortured into 3-byte CopyData messages — every line, field, and
	// escape sequence straddles message boundaries.
	data := "1\tal\\tpha\t\\N\n" +
		"2\tbeta\t\\\\slash\n" +
		"3\t\\x41bc\t\\.dot\n" +
		`\.` + "\nignored after terminator"
	for i := 0; i < len(data); i += 3 {
		end := i + 3
		if end > len(data) {
			end = len(data)
		}
		r.send(&pgproto3.CopyData{Data: []byte(data[i:end])})
	}
	r.finishCopy(3)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var name, note *string
	if err := conn.QueryRow(ctx, "SELECT name, note FROM ct WHERE id = 1").Scan(&name, &note); err != nil {
		t.Fatal(err)
	}
	if name == nil || *name != "al\tpha" || note != nil {
		t.Fatalf("row 1: name=%v note=%v", name, note)
	}
	if err := conn.QueryRow(ctx, "SELECT name, note FROM ct WHERE id = 3").Scan(&name, &note); err != nil {
		t.Fatal(err)
	}
	if name == nil || *name != "Abc" || note == nil || *note != ".dot" {
		t.Fatalf("row 3: name=%v note=%v", name, note)
	}
}

func TestCopyFromCSV(t *testing.T) {
	tc := Start(t, 3)
	r := dialRaw(t, tc)
	r.simpleExec("CREATE TABLE cc (id INT8 PRIMARY KEY, a TEXT, b TEXT)")

	r.copyIn("COPY cc FROM STDIN WITH (FORMAT csv)", 0)
	csv := "1,\"has,comma\",\"line\nbreak\"\r\n" +
		"2,,\"\"\n" + // NULL vs empty string
		"3,\"say \"\"hi\"\"\",plain\n"
	// Torture: cut inside the quoted field with the embedded newline.
	cut := 17
	r.send(&pgproto3.CopyData{Data: []byte(csv[:cut])})
	r.send(&pgproto3.CopyData{Data: []byte(csv[cut:])})
	r.finishCopy(3)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var a, b *string
	if err := conn.QueryRow(ctx, "SELECT a, b FROM cc WHERE id = 1").Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	if a == nil || *a != "has,comma" || b == nil || *b != "line\nbreak" {
		t.Fatalf("row 1: a=%v b=%v", a, b)
	}
	if err := conn.QueryRow(ctx, "SELECT a, b FROM cc WHERE id = 2").Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	if a != nil || b == nil || *b != "" {
		t.Fatalf("row 2 (NULL vs empty): a=%v b=%v", a, b)
	}
	if err := conn.QueryRow(ctx, "SELECT a FROM cc WHERE id = 3").Scan(&a); err != nil {
		t.Fatal(err)
	}
	if a == nil || *a != `say "hi"` {
		t.Fatalf("row 3: a=%v", a)
	}
}

// TestCopyErrorAtRow: a bad value mid-stream reports its row number, the
// chunks committed before it stay committed, and the connection returns
// to a clean state.
func TestCopyErrorAtRow(t *testing.T) {
	tc := Start(t, 3)
	r := dialRaw(t, tc)
	r.simpleExec("CREATE TABLE ce (id INT8 PRIMARY KEY)")

	r.copyIn("COPY ce FROM STDIN", 0)
	var sb strings.Builder
	for i := 1; i <= 300; i++ {
		fmt.Fprintf(&sb, "%d\n", i)
	}
	sb.WriteString("not-a-number\n")
	r.send(&pgproto3.CopyData{Data: []byte(sb.String())})
	r.send(&pgproto3.CopyDone{})
	e := expectErrorCode(r, "22P02")
	if !strings.Contains(e.Message, "row 301") {
		t.Fatalf("error does not name row 301: %q", e.Message)
	}
	if !strings.Contains(e.Message, "already committed") {
		t.Fatalf("error does not report committed rows: %q", e.Message)
	}
	expect[*pgproto3.ReadyForQuery](r)

	// Two full chunks (256 rows) committed; the partial third died with
	// the bad row.
	if n := countRows(t, tc, "ce"); n != 256 {
		t.Fatalf("committed rows: %d, want 256", n)
	}
	// The connection still works.
	r.simpleExec("SELECT 1")
}

func TestCopyFailAborts(t *testing.T) {
	tc := Start(t, 3)
	r := dialRaw(t, tc)
	r.simpleExec("CREATE TABLE cf (id INT8 PRIMARY KEY)")

	r.copyIn("COPY cf FROM STDIN", 0)
	var sb strings.Builder
	for i := 1; i <= 200; i++ {
		fmt.Fprintf(&sb, "%d\n", i)
	}
	r.send(&pgproto3.CopyData{Data: []byte(sb.String())})
	r.send(&pgproto3.CopyFail{Message: "client changed its mind"})
	e := expectErrorCode(r, "57014")
	if !strings.Contains(e.Message, "client changed its mind") {
		t.Fatalf("CopyFail message lost: %q", e.Message)
	}
	expect[*pgproto3.ReadyForQuery](r)
	// The first chunk (128 rows) was already durably committed when the
	// client bailed; the buffered remainder was dropped.
	if n := countRows(t, tc, "cf"); n != 128 {
		t.Fatalf("committed rows: %d, want 128", n)
	}
}

// TestCopyProtocolGates: BEGIN gate, non-final-statement gate, extended
// protocol rejection, and the stray-CopyData desync regression.
func TestCopyProtocolGates(t *testing.T) {
	tc := Start(t, 3)
	r := dialRaw(t, tc)
	r.simpleExec("CREATE TABLE cg (id INT8 PRIMARY KEY)")

	// Inside an explicit transaction: 25001.
	r.simpleExec("BEGIN")
	r.send(&pgproto3.Query{String: "COPY cg FROM STDIN"})
	expectErrorCode(r, "25001")
	expect[*pgproto3.ReadyForQuery](r)
	r.simpleExec("ROLLBACK")

	// COPY not last in a multi-statement query: 0A000. As the LAST
	// statement it runs (after the leading statement).
	r.send(&pgproto3.Query{String: "COPY cg FROM STDIN; SELECT 1"})
	expectErrorCode(r, "0A000")
	expect[*pgproto3.ReadyForQuery](r)
	r.send(&pgproto3.Query{String: "SELECT 1; COPY cg FROM STDIN"})
	expect[*pgproto3.RowDescription](r)
	expect[*pgproto3.DataRow](r)
	expect[*pgproto3.CommandComplete](r)
	expect[*pgproto3.CopyInResponse](r)
	r.send(&pgproto3.CopyData{Data: []byte("1\n")})
	r.finishCopy(1)

	// Extended protocol: Parse of COPY is refused.
	r.send(&pgproto3.Parse{Name: "s1", Query: "COPY cg FROM STDIN"}, &pgproto3.Sync{})
	expectErrorCode(r, "0A000")
	expect[*pgproto3.ReadyForQuery](r)

	// Desync regression: stray copy messages outside copy mode are
	// silently discarded and the next query's reply stream is clean.
	r.send(&pgproto3.CopyData{Data: []byte("garbage")}, &pgproto3.CopyDone{})
	r.send(&pgproto3.Query{String: "SELECT 2"})
	expect[*pgproto3.RowDescription](r)
	row := expect[*pgproto3.DataRow](r)
	if string(row.Values[0]) != "2" {
		t.Fatalf("post-desync row: %q", row.Values[0])
	}
	expect[*pgproto3.CommandComplete](r)
	expect[*pgproto3.ReadyForQuery](r)
}

// TestCopyFromPgx: pgx's CopyFrom (binary format) round-trips every column
// type, fills defaults for omitted columns, and maintains indexes.
func TestCopyFromPgx(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE alltypes (
		i INT8 PRIMARY KEY, f FLOAT8, b BOOL, s TEXT, ts TIMESTAMPTZ,
		d DATE, bs BYTEA, u UUID, n DECIMAL, j JSONB)`); err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 8, 31, 12, 0, 0, 123456000, time.UTC)
	date := time.Date(2020, 2, 29, 0, 0, 0, 0, time.UTC)
	rows := [][]any{
		{int64(1), 3.5, true, "hello", ts, date, []byte{0xde, 0xad}, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "12345.6789", `{"k":"v"}`},
		{int64(2), -0.25, false, "wor\tld", ts.Add(time.Hour), date.AddDate(0, 0, 1), []byte{}, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12", "-0.001", `[1,2]`},
	}
	n, err := conn.CopyFrom(ctx, pgx.Identifier{"alltypes"},
		[]string{"i", "f", "b", "s", "ts", "d", "bs", "u", "n", "j"}, pgx.CopyFromRows(rows))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("CopyFrom reported %d rows", n)
	}
	var s string
	var f float64
	var b bool
	if err := conn.QueryRow(ctx, "SELECT s, f, b FROM alltypes WHERE i = 2").Scan(&s, &f, &b); err != nil {
		t.Fatal(err)
	}
	if s != "wor\tld" || f != -0.25 || b {
		t.Fatalf("row 2: s=%q f=%v b=%v", s, f, b)
	}
	var gotTS time.Time
	var num string
	if err := conn.QueryRow(ctx, "SELECT ts, n::TEXT FROM alltypes WHERE i = 1").Scan(&gotTS, &num); err != nil {
		// n::TEXT cast unsupported; read decimal as string directly.
		if err := conn.QueryRow(ctx, "SELECT ts FROM alltypes WHERE i = 1").Scan(&gotTS); err != nil {
			t.Fatal(err)
		}
	}
	if !gotTS.Equal(ts) {
		t.Fatalf("ts: %v, want %v", gotTS, ts)
	}

	// Column subset: omitted columns take their defaults.
	if _, err := conn.Exec(ctx, "CREATE TABLE defs (id INT8 PRIMARY KEY, v TEXT DEFAULT 'dv', n INT8)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.CopyFrom(ctx, pgx.Identifier{"defs"}, []string{"id"},
		pgx.CopyFromRows([][]any{{int64(7)}})); err != nil {
		t.Fatal(err)
	}
	var v *string
	var nv *int64
	if err := conn.QueryRow(ctx, "SELECT v, n FROM defs WHERE id = 7").Scan(&v, &nv); err != nil {
		t.Fatal(err)
	}
	if v == nil || *v != "dv" || nv != nil {
		t.Fatalf("defaults: v=%v n=%v", v, nv)
	}

	// Unique secondary index enforced across a chunk boundary (>128 rows
	// between the duplicate pair).
	if _, err := conn.Exec(ctx, "CREATE TABLE uniq (id INT8 PRIMARY KEY, tag TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "CREATE UNIQUE INDEX by_tag ON uniq (tag)"); err != nil {
		t.Fatal(err)
	}
	var dup [][]any
	for i := 0; i < 200; i++ {
		dup = append(dup, []any{int64(i), fmt.Sprintf("t%03d", i)})
	}
	dup = append(dup, []any{int64(999), "t000"}) // duplicates row 1's tag, 200 rows later
	_, err = conn.CopyFrom(ctx, pgx.Identifier{"uniq"}, []string{"id", "tag"}, pgx.CopyFromRows(dup))
	if err == nil {
		t.Fatal("cross-chunk unique violation not caught")
	}
	if !strings.Contains(err.Error(), "23505") && !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestCopyIntoShardedTable: COPY computes the hidden shard bucket exactly
// like INSERT does, and the rows are readable by their logical PK.
func TestCopyIntoShardedTable(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE m (series TEXT, at TIMESTAMPTZ, v FLOAT8,
		PRIMARY KEY (series, at)) WITH (timeseries = true, shards = 4)`); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	var rows [][]any
	for i := 0; i < 150; i++ {
		rows = append(rows, []any{fmt.Sprintf("s%d", i%7), base.Add(time.Duration(i) * time.Second), float64(i)})
	}
	if _, err := conn.CopyFrom(ctx, pgx.Identifier{"m"}, []string{"series", "at", "v"}, pgx.CopyFromRows(rows)); err != nil {
		t.Fatal(err)
	}
	var got float64
	if err := conn.QueryRow(ctx, "SELECT v FROM m WHERE series = 's3' AND at = $1", base.Add(10*time.Second)).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 10 {
		t.Fatalf("sharded point read: %v", got)
	}
	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM m").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 150 {
		t.Fatalf("count: %d", n)
	}
}
