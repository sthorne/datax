package testcluster

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
)

// The COPY ... TO STDOUT suite (issue #365): every format, option and
// type against PostgreSQL's own output, the session settings that
// change a value's text, the copy-out message flow over both protocols,
// the round trip through COPY FROM, a large table streamed in bounded
// memory, cancellation and disconnects, privileges and row-level
// security, psql's \copy, and the errors PostgreSQL gives.

// copyOut runs a COPY TO over pgx (the simple protocol, as pgx's CopyTo
// does) and returns its output and command tag.
func copyOut(ctx context.Context, conn *pgx.Conn, stmt string) (string, string, error) {
	var buf bytes.Buffer
	tag, err := conn.PgConn().CopyTo(ctx, &buf, stmt)
	return buf.String(), tag.String(), err
}

func copyToConn(t *testing.T, ctx context.Context, tc *TestCluster) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// TestCopyToPostgresOutput: every format and option over every type,
// and the settings PostgreSQL applies to a value's text (IntervalStyle,
// bytea_output, extra_float_digits, TimeZone, DateStyle), byte for byte
// as PostgreSQL 16 writes them.
func TestCopyToPostgresOutput(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn := copyToConn(t, ctx, tc)
	for _, stmt := range copyToFixture {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if _, err := conn.Exec(ctx, `UPDATE ty SET t = t || $1 WHERE id = 3`, copyToFixtureTail); err != nil {
		t.Fatal(err)
	}
	for _, c := range copyToPGOutput {
		// A fresh session per case, so that one case's settings do not
		// leak into the next.
		sess := copyToConn(t, ctx, tc)
		for _, set := range c.settings {
			if _, err := sess.Exec(ctx, set); err != nil {
				t.Fatalf("%s: %v", set, err)
			}
		}
		got, tag, err := copyOut(ctx, sess, c.copy)
		if err != nil {
			t.Fatalf("%v %s: %v", c.settings, c.copy, err)
		}
		if c.hex {
			got = hex.EncodeToString([]byte(got))
		}
		if got != c.want {
			t.Errorf("%v %s:\n got %q\nwant %q", c.settings, c.copy, got, c.want)
		}
		if !strings.HasPrefix(tag, "COPY ") {
			t.Errorf("%s: tag %q", c.copy, tag)
		}
		_ = sess.Close(ctx)
	}
}

// TestCopyToErrors: PostgreSQL's errors for COPY TO — the option checks
// of ProcessCopyOptions, the relation and column checks, COPY (query)
// with parameters or without RETURNING, and the server-file privilege
// errors — with their SQLSTATEs and messages (PostgreSQL 16's wording).
func TestCopyToErrors(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn := copyToConn(t, ctx, tc)
	for _, stmt := range []string{
		`CREATE TABLE t (a INT8 PRIMARY KEY, b TEXT)`,
		`INSERT INTO t VALUES (1, 'x')`,
		`CREATE VIEW v AS SELECT * FROM t`,
		`CREATE SEQUENCE sq`,
		`CREATE USER bob`,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	type wantErr struct{ stmt, code, msg string }
	check := func(conn *pgx.Conn, c wantErr) *pgconn.PgError {
		t.Helper()
		_, _, err := copyOut(ctx, conn, c.stmt)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) {
			t.Fatalf("%s: %v, want %s", c.stmt, err, c.code)
		}
		if pe.Code != c.code || (c.msg != "" && pe.Message != c.msg) {
			t.Fatalf("%s: %s %q, want %s %q", c.stmt, pe.Code, pe.Message, c.code, c.msg)
		}
		return pe
	}
	for _, c := range []wantErr{
		{`COPY t TO STDOUT WITH (foo 1)`, "42601", `option "foo" not recognized`},
		{`COPY t TO STDOUT WITH (oids)`, "42601", `option "oids" not recognized`},
		{`COPY t TO STDOUT WITH (on_error 'stop')`, "42601", `option "on_error" not recognized`},
		{`COPY t TO STDOUT WITH (format)`, "42601", `format requires a parameter`},
		{`COPY t TO STDOUT WITH (delimiter)`, "42601", `delimiter requires a parameter`},
		{`COPY t TO STDOUT WITH (format xml)`, "22023", `COPY format "xml" not recognized`},
		{`COPY t TO STDOUT WITH (format 'CSV')`, "22023", `COPY format "CSV" not recognized`},
		{`COPY t TO STDOUT WITH (format csv, format text)`, "42601", `conflicting or redundant options`},
		{`COPY t TO STDOUT (format csv, header false, header true)`, "42601", `conflicting or redundant options`},
		{`COPY t TO STDOUT WITH (delimiter 'ab')`, "0A000", `COPY delimiter must be a single one-byte character`},
		{`COPY t TO STDOUT WITH (delimiter '')`, "0A000", `COPY delimiter must be a single one-byte character`},
		{`COPY t TO STDOUT WITH (delimiter E'\n')`, "22023", `COPY delimiter cannot be newline or carriage return`},
		{`COPY t TO STDOUT WITH (null E'\r')`, "22023", `COPY null representation cannot use newline or carriage return`},
		{`COPY t TO STDOUT WITH (delimiter 'a')`, "22023", `COPY delimiter cannot be "a"`},
		{`COPY t TO STDOUT WITH (delimiter '\')`, "22023", `COPY delimiter cannot be "\"`},
		{`COPY t TO STDOUT WITH (delimiter '.')`, "22023", `COPY delimiter cannot be "."`},
		{`COPY t TO STDOUT WITH (format binary, delimiter ',')`, "42601", `cannot specify DELIMITER in BINARY mode`},
		{`COPY t TO STDOUT WITH (format binary, null 'x')`, "42601", `cannot specify NULL in BINARY mode`},
		{`COPY t TO STDOUT WITH (format binary, header)`, "0A000", `cannot specify HEADER in BINARY mode`},
		{`COPY t TO STDOUT WITH (quote '"')`, "0A000", `COPY quote available only in CSV mode`},
		{`COPY t TO STDOUT WITH (format csv, quote 'ab')`, "0A000", `COPY quote must be a single one-byte character`},
		{`COPY t TO STDOUT WITH (format csv, delimiter ',', quote ',')`, "22023", `COPY delimiter and quote must be different`},
		{`COPY t TO STDOUT WITH (escape '"')`, "0A000", `COPY escape available only in CSV mode`},
		{`COPY t TO STDOUT WITH (format csv, escape 'ab')`, "0A000", `COPY escape must be a single one-byte character`},
		{`COPY t TO STDOUT WITH (force_quote (a))`, "0A000", `COPY force quote available only in CSV mode`},
		{`COPY t TO STDOUT WITH (format csv, force_quote a)`, "22023", `argument to option "force_quote" must be a list of column names`},
		{`COPY t TO STDOUT WITH (format csv, force_quote (zz))`, "42703", `column "zz" of relation "t" does not exist`},
		{`COPY t (a) TO STDOUT WITH (format csv, force_quote (b))`, "42P10", `FORCE_QUOTE column "b" not referenced by COPY`},
		{`COPY (SELECT a FROM t) TO STDOUT WITH (format csv, force_quote (b))`, "42703", `column "b" does not exist`},
		{`COPY t TO STDOUT WITH (format csv, force_not_null (a))`, "0A000", `COPY force not null only available using COPY FROM`},
		{`COPY t TO STDOUT CSV FORCE NOT NULL a`, "0A000", `COPY force not null only available using COPY FROM`},
		{`COPY t TO STDOUT WITH (format csv, force_null (a))`, "0A000", `COPY force null only available using COPY FROM`},
		{`COPY t TO STDOUT WITH (force_not_null (a))`, "0A000", `COPY force not null available only in CSV mode`},
		{`COPY t TO STDOUT WITH (format csv, header match)`, "0A000", `cannot use "match" with HEADER in COPY TO`},
		{`COPY t TO STDOUT WITH (header 2)`, "42601", `header requires a Boolean value or "match"`},
		{`COPY t TO STDOUT WITH (header foo)`, "42601", `header requires a Boolean value or "match"`},
		{`COPY t TO STDOUT WITH (null ',', delimiter ',')`, "0A000", `COPY delimiter must not appear in the NULL specification`},
		{`COPY t TO STDOUT WITH (format csv, null '"')`, "0A000", `CSV quote character must not appear in the NULL specification`},
		{`COPY t TO STDOUT WITH (encoding 'nope')`, "22023", `argument to option "encoding" must be a valid encoding name`},
		{`COPY t TO STDOUT WITH (default 'x')`, "0A000", `COPY DEFAULT only available using COPY FROM`},
		{`COPY sq TO STDOUT`, "42809", `cannot copy from sequence "sq"`},
		{`COPY t (a, a) TO STDOUT`, "42701", `column "a" specified more than once`},
		{`COPY t (zz) TO STDOUT`, "42703", `column "zz" of relation "t" does not exist`},
		{`COPY nosuch TO STDOUT`, "42P01", ""},
		{`COPY (SELECT $1) TO STDOUT`, "42P02", `there is no parameter $1`},
		{`COPY (SELECT a FROM t WHERE a = $2 OR a = $1) TO STDOUT`, "42P02", `there is no parameter $2`},
		{`COPY (SELECT 1) FROM STDIN`, "42601", ""},
		{`COPY (CREATE TABLE x (a INT8)) TO STDOUT`, "42601", ""},
		{`COPY t TO STDOUT WITH OIDS`, "42601", ""},
		{`COPY (INSERT INTO t VALUES (9, 'x')) TO STDOUT`, "0A000", `COPY query must have a RETURNING clause`},
		{`COPY (UPDATE t SET b = 'y') TO STDOUT`, "0A000", `COPY query must have a RETURNING clause`},
		{`COPY (DELETE FROM t) TO STDOUT`, "0A000", `COPY query must have a RETURNING clause`},
		// datax writes only UTF8: an encoding PostgreSQL would convert
		// to is refused rather than ignored.
		{`COPY t TO STDOUT WITH (encoding 'latin1')`, "0A000", `COPY ENCODING "latin1" is not supported: datax writes UTF8 only`},
		// root and the admins hold pg_write_server_files in PostgreSQL;
		// datax has no server files to write.
		{`COPY t TO '/tmp/x'`, "0A000", `COPY to a file is not supported`},
		{`COPY t TO PROGRAM 'cat'`, "0A000", `COPY TO PROGRAM is not supported`},
	} {
		check(conn, c)
	}
	pe := check(conn, wantErr{`COPY v TO STDOUT`, "42809", `cannot copy from view "v"`})
	if pe.Hint != "Try the COPY (SELECT ...) TO variant." {
		t.Fatalf("view hint: %q", pe.Hint)
	}
	// Nothing ran: the refused COPY (INSERT ...) inserted nothing.
	if out, _, err := copyOut(ctx, conn, `COPY t TO STDOUT`); err != nil || out != "1\tx\n" {
		t.Fatalf("after the refusals: %q %v", out, err)
	}
	// The options PostgreSQL accepts and that change nothing here.
	for _, stmt := range []string{
		`COPY t TO STDOUT WITH (encoding 'UTF8')`,
		`COPY t TO STDOUT WITH (encoding 'utf-8')`,
		`COPY t TO STDOUT WITH (encoding 'SQL_ASCII')`,
		`COPY t TO STDOUT ENCODING 'unicode'`,
		`COPY t TO STDOUT WITH (freeze)`,
		`COPY t TO STDOUT WITH (freeze false, header off, format text)`,
		`COPY t TO STDIN`,
	} {
		if out, tag, err := copyOut(ctx, conn, stmt); err != nil || out != "1\tx\n" || tag != "COPY 1" {
			t.Fatalf("%s: %q %q %v", stmt, out, tag, err)
		}
	}

	// A role without pg_write_server_files gets PostgreSQL's privilege
	// error, before its missing privilege on the table is looked at.
	bob, err := pgx.Connect(ctx, strings.Replace(pgURL(tc, 0), "root@", "bob@", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bob.Close(ctx) }()
	const hint = `Anyone can COPY to stdout or from stdin. psql's \copy command also works for anyone.`
	for _, c := range []struct{ stmt, msg, detail string }{
		{`COPY t TO '/tmp/x'`, `permission denied to COPY to a file`, `Only roles with privileges of the "pg_write_server_files" role may COPY to a file.`},
		{`COPY (SELECT 1) TO '/tmp/x' WITH (FORMAT csv)`, `permission denied to COPY to a file`, `Only roles with privileges of the "pg_write_server_files" role may COPY to a file.`},
		{`COPY t TO PROGRAM 'cat'`, `permission denied to COPY to or from an external program`, `Only roles with privileges of the "pg_execute_server_program" role may COPY to or from an external program.`},
	} {
		pe := check(bob, wantErr{c.stmt, "42501", c.msg})
		if pe.Detail != c.detail || pe.Hint != hint {
			t.Fatalf("%s: detail %q hint %q", c.stmt, pe.Detail, pe.Hint)
		}
	}
	check(bob, wantErr{`COPY t TO STDOUT`, "42501", ""})
	check(bob, wantErr{`COPY (SELECT * FROM t) TO STDOUT`, "42501", ""})

	// In a transaction block an error fails the block.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := copyOut(ctx, conn, `COPY t TO STDOUT WITH (format xml)`); pgErrCode(err) != "22023" {
		t.Fatalf("in a block: %v", err)
	}
	if _, _, err := copyOut(ctx, conn, `COPY t TO STDOUT`); pgErrCode(err) != "25P02" {
		t.Fatalf("after the error in the block: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestCopyToProtocol: the copy-out message flow — CopyOutResponse with
// the format codes, one CopyData per row (the binary file header in the
// first row's, the trailer in its own), CopyDone, COPY n — over the
// simple protocol, in the middle of a multi-statement query and over
// the extended protocol (described as NoData, any row limit, no
// parameters); an error after rows; and a COPY in a transaction block
// reading the block's own writes.
func TestCopyToProtocol(t *testing.T) {
	tc := Start(t, 1)
	r := dialRaw(t, tc)
	r.simpleExec(`CREATE TABLE cp (a INT8 PRIMARY KEY, b TEXT)`)
	r.simpleExec(`CREATE TABLE cp_empty (a INT4 PRIMARY KEY)`)
	r.simpleExec(`INSERT INTO cp VALUES (1, 'x'), (2, NULL)`)

	copyOutFlow := func(wantFormat byte, wantCols int, wantData []string, wantTag string) {
		t.Helper()
		resp := expect[*pgproto3.CopyOutResponse](r)
		if resp.OverallFormat != wantFormat || len(resp.ColumnFormatCodes) != wantCols {
			t.Fatalf("CopyOutResponse %+v, want format %d over %d columns", resp, wantFormat, wantCols)
		}
		for _, code := range resp.ColumnFormatCodes {
			if code != uint16(wantFormat) {
				t.Fatalf("column format codes %v", resp.ColumnFormatCodes)
			}
		}
		for i, want := range wantData {
			if got := string(expect[*pgproto3.CopyData](r).Data); got != want {
				t.Fatalf("CopyData %d: %q, want %q", i, got, want)
			}
		}
		expect[*pgproto3.CopyDone](r)
		if cc := expect[*pgproto3.CommandComplete](r); string(cc.CommandTag) != wantTag {
			t.Fatalf("tag %q, want %q", cc.CommandTag, wantTag)
		}
	}
	const sig = "PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00"

	r.send(&pgproto3.Query{String: `COPY cp TO STDOUT`})
	copyOutFlow(0, 2, []string{"1\tx\n", "2\t\\N\n"}, "COPY 2")
	expect[*pgproto3.ReadyForQuery](r)

	r.send(&pgproto3.Query{String: `COPY cp (b) TO STDOUT WITH (FORMAT csv, HEADER)`})
	copyOutFlow(0, 1, []string{"b\n", "x\n", "\n"}, "COPY 2")
	expect[*pgproto3.ReadyForQuery](r)

	r.send(&pgproto3.Query{String: `COPY cp TO STDOUT WITH (FORMAT binary)`})
	copyOutFlow(1, 2, []string{
		sig + "\x00\x02\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x01x",
		"\x00\x02\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x02\xff\xff\xff\xff",
		"\xff\xff",
	}, "COPY 2")
	expect[*pgproto3.ReadyForQuery](r)

	// No rows: the header and the trailer share the one CopyData.
	r.send(&pgproto3.Query{String: `COPY cp_empty TO STDOUT (FORMAT binary)`})
	copyOutFlow(1, 1, []string{sig + "\xff\xff"}, "COPY 0")
	expect[*pgproto3.ReadyForQuery](r)
	r.send(&pgproto3.Query{String: `COPY cp_empty TO STDOUT (FORMAT csv, HEADER)`})
	copyOutFlow(0, 1, []string{"a\n"}, "COPY 0")
	expect[*pgproto3.ReadyForQuery](r)

	// COPY TO reads nothing from the client, so it may stand anywhere in
	// a multi-statement query.
	r.send(&pgproto3.Query{String: `SELECT 1; COPY (SELECT a FROM cp ORDER BY a DESC) TO STDOUT; SELECT 2`})
	expect[*pgproto3.RowDescription](r)
	expect[*pgproto3.DataRow](r)
	expect[*pgproto3.CommandComplete](r)
	copyOutFlow(0, 1, []string{"2\n", "1\n"}, "COPY 2")
	expect[*pgproto3.RowDescription](r)
	if row := expect[*pgproto3.DataRow](r); string(row.Values[0]) != "2" {
		t.Fatalf("after the COPY: %q", row.Values[0])
	}
	expect[*pgproto3.CommandComplete](r)
	expect[*pgproto3.ReadyForQuery](r)

	// The extended protocol: Describe says NoData, the row limit does not
	// suspend it, and a second Execute of the completed portal completes
	// again with no rows.
	r.send(&pgproto3.Parse{Name: "c1", Query: `COPY cp TO STDOUT WITH (FORMAT csv)`},
		&pgproto3.Describe{ObjectType: 'S', Name: "c1"},
		&pgproto3.Bind{PreparedStatement: "c1"},
		&pgproto3.Describe{ObjectType: 'P'},
		&pgproto3.Execute{MaxRows: 1},
		&pgproto3.Execute{},
		&pgproto3.Sync{})
	expect[*pgproto3.ParseComplete](r)
	if pd := expect[*pgproto3.ParameterDescription](r); len(pd.ParameterOIDs) != 0 {
		t.Fatalf("parameters: %v", pd.ParameterOIDs)
	}
	expect[*pgproto3.NoData](r)
	expect[*pgproto3.BindComplete](r)
	expect[*pgproto3.NoData](r)
	copyOutFlow(0, 2, []string{"1,x\n", "2,\n"}, "COPY 2")
	if cc := expect[*pgproto3.CommandComplete](r); string(cc.CommandTag) != "COPY 2" {
		t.Fatalf("second Execute: %q", cc.CommandTag)
	}
	expect[*pgproto3.ReadyForQuery](r)

	// COPY (query) takes no parameters: a Bind that supplies one is
	// refused, and executing it reports the $n (PostgreSQL's 42P02).
	r.send(&pgproto3.Parse{Query: `COPY (SELECT a FROM cp WHERE a = $1) TO STDOUT`},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("1")}},
		&pgproto3.Sync{})
	expect[*pgproto3.ParseComplete](r)
	expectErrorCode(r, "08P01")
	expect[*pgproto3.ReadyForQuery](r)
	r.send(&pgproto3.Parse{Query: `COPY (SELECT a FROM cp WHERE a = $1) TO STDOUT`},
		&pgproto3.Bind{},
		&pgproto3.Execute{},
		&pgproto3.Sync{})
	expect[*pgproto3.ParseComplete](r)
	expect[*pgproto3.BindComplete](r)
	if e := expectErrorCode(r, "42P02"); e.Message != "there is no parameter $1" {
		t.Fatalf("message %q", e.Message)
	}
	expect[*pgproto3.ReadyForQuery](r)

	// An error after rows were produced: the rows, then the error, with
	// no CopyDone (PostgreSQL's failed copy-out).
	r.send(&pgproto3.Query{String: `COPY (SELECT a, 10 / (a - 2) FROM cp) TO STDOUT`})
	expect[*pgproto3.CopyOutResponse](r)
	if d := expect[*pgproto3.CopyData](r); string(d.Data) != "1\t-10\n" {
		t.Fatalf("row before the error: %q", d.Data)
	}
	expectErrorCode(r, "22012")
	expect[*pgproto3.ReadyForQuery](r)

	// In a transaction block, COPY reads the block's writes, including a
	// table the block created; the block stays open.
	r.simpleExec(`BEGIN`)
	r.simpleExec(`INSERT INTO cp VALUES (3, 'z')`)
	r.simpleExec(`CREATE TABLE cp_new (k INT8 PRIMARY KEY)`)
	r.simpleExec(`INSERT INTO cp_new VALUES (7)`)
	r.send(&pgproto3.Query{String: `COPY cp TO STDOUT; COPY cp_new TO STDOUT`})
	copyOutFlow(0, 2, []string{"1\tx\n", "2\t\\N\n", "3\tz\n"}, "COPY 3")
	copyOutFlow(0, 1, []string{"7\n"}, "COPY 1")
	if rq := expect[*pgproto3.ReadyForQuery](r); rq.TxStatus != 'T' {
		t.Fatalf("status %c", rq.TxStatus)
	}
	r.simpleExec(`ROLLBACK`)
	r.send(&pgproto3.Query{String: `COPY cp (a) TO STDOUT`})
	copyOutFlow(0, 1, []string{"1\n", "2\n"}, "COPY 2")
	expect[*pgproto3.ReadyForQuery](r)
}

// TestCopyToRoundTrip: what COPY TO writes, COPY FROM reads back to the
// same rows, in each format: copied into an empty twin, the twin's COPY
// TO is the same bytes.
func TestCopyToRoundTrip(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn := copyToConn(t, ctx, tc)
	for _, stmt := range copyToFixture {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if _, err := conn.Exec(ctx, `UPDATE ty SET t = t || $1 WHERE id = 3`, copyToFixtureTail); err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"text", "csv", "binary"} {
		twin := "ty_" + format
		if _, err := conn.Exec(ctx, `CREATE TABLE `+twin+` (LIKE ty INCLUDING ALL)`); err != nil {
			t.Fatal(err)
		}
		opts := " WITH (FORMAT " + format + ")"
		out, tag, err := copyOut(ctx, conn, `COPY ty TO STDOUT`+opts)
		if err != nil || tag != "COPY 7" {
			t.Fatalf("%s: %s %v", format, tag, err)
		}
		in, err := conn.PgConn().CopyFrom(ctx, strings.NewReader(out), `COPY `+twin+` FROM STDIN`+opts)
		if err != nil || in.RowsAffected() != 7 {
			t.Fatalf("%s: COPY FROM: %v %v", format, in, err)
		}
		back, _, err := copyOut(ctx, conn, `COPY `+twin+` TO STDOUT`+opts)
		if err != nil {
			t.Fatal(err)
		}
		if back != out {
			t.Fatalf("%s: the copy's COPY TO differs:\n got %q\nwant %q", format, back, out)
		}
		// And the rows compare equal, value by value.
		var diff int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM (SELECT id, i2, f8, num2, t, ch, tstz, ts, d, tm, iv, bt, u, ai, at, m FROM ty
			EXCEPT SELECT id, i2, f8, num2, t, ch, tstz, ts, d, tm, iv, bt, u, ai, at, m FROM `+twin+`) d`).Scan(&diff); err != nil || diff != 0 {
			t.Fatalf("%s: %d rows differ (%v)", format, diff, err)
		}
	}
}

// TestCopyToLargeTable: COPY of a table larger than the statement
// memory limit streams: the first CopyData reaches the client long
// before the table has been read, the rows go through the stream, and
// the limit is never hit. A retryable error before anything was flushed
// starts the output over invisibly.
func TestCopyToLargeTable(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	conn := copyToConn(t, ctx, tc)
	const rows = 30000
	fillBig(t, ctx, conn, rows, 300) // ~9 MiB of rows
	if _, err := conn.Exec(ctx, `SET statement_memory_limit = '1MB'`); err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"text", "csv", "binary"} {
		scannedBefore := testutil.ToFloat64(metrics.SQLRowsScanned)
		streamedBefore := testutil.ToFloat64(metrics.SQLStreamedRows)
		w := &firstWrite{at: func() float64 { return testutil.ToFloat64(metrics.SQLRowsScanned) - scannedBefore }}
		tag, err := conn.PgConn().CopyTo(ctx, w, `COPY big (k, pad) TO STDOUT WITH (FORMAT `+format+`)`)
		if err != nil || tag.String() != "COPY 30000" {
			t.Fatalf("%s: %s %v", format, tag, err)
		}
		if w.scannedAtFirst >= rows {
			t.Fatalf("%s: the server had scanned %.0f rows when the first data arrived: the COPY was materialized", format, w.scannedAtFirst)
		}
		if d := testutil.ToFloat64(metrics.SQLStreamedRows) - streamedBefore; d != rows {
			t.Fatalf("%s: streamed rows %.0f, want %d", format, d, rows)
		}
		if format == "text" {
			lines := strings.Split(strings.TrimSuffix(w.buf.String(), "\n"), "\n")
			if len(lines) != rows || lines[12345] != "12345\t"+strings.Repeat("x", 300) {
				t.Fatalf("text: %d lines", len(lines))
			}
		}
		t.Logf("%s: first data after %.0f of %d rows scanned, %d bytes", format, w.scannedAtFirst, rows, w.buf.Len())
	}

	// A retryable error at row 10, before the first flush: the statement
	// re-runs and the output is whole and in order.
	want, _, err := copyOut(ctx, conn, `COPY big (k) TO STDOUT WITH (FORMAT csv, HEADER)`)
	if err != nil {
		t.Fatal(err)
	}
	fired := false
	sql.TestingStreamHook = func(ctx context.Context, n int64) error {
		if n == 10 && !fired {
			fired = true
			return &kvclient.RetryableError{Cause: kvpb.NewErrorf("injected")}
		}
		return nil
	}
	defer func() { sql.TestingStreamHook = nil }()
	got, tag, err := copyOut(ctx, conn, `COPY big (k) TO STDOUT WITH (FORMAT csv, HEADER)`)
	sql.TestingStreamHook = nil
	if err != nil || !fired || got != want || tag != "COPY 30000" {
		t.Fatalf("restarted COPY: fired %v, %s, %v, same output %v", fired, tag, err, got == want)
	}
}

// firstWrite collects a COPY's output and records the server's scanned
// rows when the first data arrived.
type firstWrite struct {
	buf            bytes.Buffer
	at             func() float64
	scannedAtFirst float64
}

func (w *firstWrite) Write(p []byte) (int, error) {
	if w.buf.Len() == 0 {
		w.scannedAtFirst = w.at()
	}
	return w.buf.Write(p)
}

// TestCopyToCancel: a COPY is cancelled mid-stream (CancelRequest, as
// psql and pools send) and the connection is usable after; the
// statement timeout applies to it; a client that disconnects mid-COPY
// leaves no session behind and stops the read.
func TestCopyToCancel(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	cfg.BuildContextWatcherHandler = func(pc *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: pc, CancelRequestDelay: 50 * time.Millisecond, DeadlineDelay: 10 * time.Second}
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	const rows = 3000
	fillBig(t, ctx, conn, rows, 300)
	usable := func() {
		t.Helper()
		if out, _, err := copyOut(ctx, conn, `COPY (SELECT k FROM big WHERE k < 3) TO STDOUT`); err != nil || out != "0\n1\n2\n" {
			t.Fatalf("connection after the error: %q %v", out, err)
		}
	}

	// The hook parks the stream at row 100 until the statement is
	// cancelled.
	sql.TestingStreamHook = func(ctx context.Context, n int64) error {
		if n == 100 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	defer func() { sql.TestingStreamHook = nil }()
	cctx, ccancel := context.WithTimeout(ctx, 500*time.Millisecond)
	start := time.Now()
	_, _, err = copyOut(cctx, conn, `COPY big TO STDOUT`)
	ccancel()
	if pgErrCode(err) != sql.CodeQueryCanceled || time.Since(start) > 10*time.Second {
		t.Fatalf("cancelled COPY: %v after %s", err, time.Since(start))
	}
	usable()

	if _, err := conn.Exec(ctx, `SET statement_timeout = '300ms'`); err != nil {
		t.Fatal(err)
	}
	_, _, err = copyOut(ctx, conn, `COPY big TO STDOUT WITH (FORMAT csv)`)
	if pgErrCode(err) != sql.CodeQueryCanceled || !strings.Contains(err.Error(), "statement timeout") {
		t.Fatalf("timed-out COPY: %v", err)
	}
	if _, err := conn.Exec(ctx, `RESET statement_timeout`); err != nil {
		t.Fatal(err)
	}
	sql.TestingStreamHook = nil
	usable()

	// A client that goes away mid-COPY: the server notices at its next
	// write, ends the stream and the session.
	sessions := func() int {
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := sessions()
	var pulled atomic.Int64
	sql.TestingStreamHook = func(ctx context.Context, n int64) error {
		pulled.Add(1)
		if n > 200 {
			time.Sleep(time.Millisecond) // slow enough for the client to leave first
		}
		return nil
	}
	r := dialRaw(t, tc)
	r.send(&pgproto3.Query{String: `COPY big TO STDOUT`})
	expect[*pgproto3.CopyOutResponse](r)
	expect[*pgproto3.CopyData](r)
	if n := sessions(); n != before+1 {
		t.Fatalf("sessions during the COPY: %d, want %d", n, before+1)
	}
	_ = r.nc.Close()
	deadline := time.Now().Add(20 * time.Second)
	for sessions() != before {
		if time.Now().After(deadline) {
			t.Fatalf("the disconnected COPY's session is still there")
		}
		time.Sleep(50 * time.Millisecond)
	}
	stopped := pulled.Load()
	time.Sleep(300 * time.Millisecond)
	if p := pulled.Load(); p != stopped || p >= rows {
		t.Fatalf("the stream went on after the disconnect: %d then %d rows pulled", stopped, p)
	}
	sql.TestingStreamHook = nil
	usable()
}

// TestCopyToPrivilegesAndRowSecurity: COPY TO reads as SELECT does —
// the SELECT privilege, and row-level security: a role subject to a
// policy copies only the rows it may see (COPY t TO and COPY (query)
// TO alike), while the owner and a BYPASSRLS role copy them all.
func TestCopyToPrivilegesAndRowSecurity(t *testing.T) {
	tc := StartWithOptions(t, 1, func(cfg *server.Config) { cfg.DescLeaseTTL = time.Second })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := rlsConn(t, ctx, tc, 0, "root")
	for _, stmt := range []string{
		`CREATE USER owner_u`,
		`CREATE USER tenant_u`,
		`CREATE USER auditor_u BYPASSRLS`,
		`CREATE USER stranger`,
		`GRANT CREATE ON DATABASE datax TO owner_u`,
	} {
		rlsExec(t, ctx, root, stmt)
	}
	owner := rlsConn(t, ctx, tc, 0, "owner_u")
	for _, stmt := range []string{
		`CREATE TABLE docs (id INT8 PRIMARY KEY, tenant TEXT, body TEXT)`,
		`INSERT INTO docs VALUES (1, 'a', 'a1'), (2, 'b', 'b2'), (3, 'a', 'a3'), (4, 'b', 'b4')`,
		`GRANT SELECT ON docs TO tenant_u`,
		`GRANT SELECT ON docs TO auditor_u`,
		`ALTER TABLE docs ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY by_tenant ON docs USING (tenant = current_setting('app.tenant'))`,
	} {
		rlsExec(t, ctx, owner, stmt)
	}
	all := "1\ta\ta1\n2\tb\tb2\n3\ta\ta3\n4\tb\tb4\n"
	tenant := rlsConn(t, ctx, tc, 0, "tenant_u")
	rlsExec(t, ctx, tenant, `SET app.tenant = 'a'`)
	rlsSettle(t, ctx, tenant, []string{"1", "3"}, `SELECT id FROM docs ORDER BY id`)
	for _, c := range []struct {
		conn *pgx.Conn
		stmt string
		want string
	}{
		{tenant, `COPY docs TO STDOUT`, "1\ta\ta1\n3\ta\ta3\n"},
		{tenant, `COPY docs (body) TO STDOUT WITH (FORMAT csv, HEADER)`, "body\na1\na3\n"},
		{tenant, `COPY (SELECT id FROM docs ORDER BY id DESC) TO STDOUT`, "3\n1\n"},
		{tenant, `COPY docs TO STDOUT WITH (FORMAT binary)`, ""},
		{owner, `COPY docs TO STDOUT`, all},
		{rlsConn(t, ctx, tc, 0, "auditor_u"), `COPY docs TO STDOUT`, all},
		{root, `COPY docs TO STDOUT`, all},
	} {
		got, tag, err := copyOut(ctx, c.conn, c.stmt)
		if err != nil {
			t.Fatalf("%s: %v", c.stmt, err)
		}
		if c.want != "" && got != c.want {
			t.Fatalf("%s as %s: %q, want %q", c.stmt, c.conn.Config().User, got, c.want)
		}
		if c.conn == tenant && tag != "COPY 2" {
			t.Fatalf("%s: tag %q", c.stmt, tag)
		}
	}
	// No SELECT privilege, no COPY, in either form.
	stranger := rlsConn(t, ctx, tc, 0, "stranger")
	for _, stmt := range []string{`COPY docs TO STDOUT`, `COPY docs (id) TO STDOUT`, `COPY (SELECT id FROM docs) TO STDOUT`} {
		if _, _, err := copyOut(ctx, stranger, stmt); pgErrCode(err) != "42501" {
			t.Fatalf("%s as stranger: %v", stmt, err)
		}
	}
	// The named keys' material is not readable through COPY either.
	if _, _, err := copyOut(ctx, root, `COPY datax_keys (material) TO STDOUT`); err == nil {
		t.Fatal("COPY of datax_keys material was accepted")
	}
}

// TestCopyToPsql: psql's \copy ... to (which sends COPY ... TO STDOUT
// and writes the data itself) and COPY ... TO STDOUT run through psql.
func TestCopyToPsql(t *testing.T) {
	psql, err := lookPsql()
	if err != nil {
		t.Skip("psql not installed")
	}
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn := copyToConn(t, ctx, tc)
	for _, stmt := range []string{
		`CREATE TABLE pt (id INT8 PRIMARY KEY, name TEXT, note TEXT)`,
		`INSERT INTO pt VALUES (1, 'a b', NULL), (2, 'c,"d"', E'tab\there')`,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	url := pgURL(tc, 0)
	file := filepath.Join(t.TempDir(), "pt.csv")
	for _, c := range []struct{ cmd, want string }{
		{`\copy pt to stdout`, "1\ta b\t\\N\n2\tc,\"d\"\ttab\\there\n"},
		{`\copy pt to stdout with csv header`, "id,name,note\n1,a b,\n2,\"c,\"\"d\"\"\",tab\there\n"},
		{`\copy (select id, name from pt order by id desc) to stdout with (format csv)`, "2,\"c,\"\"d\"\"\"\n1,a b\n"},
		{`\copy pt (name) to stdout`, "a b\nc,\"d\"\n"},
		{`COPY pt TO STDOUT WITH (FORMAT csv, FORCE_QUOTE *)`, "\"1\",\"a b\",\n\"2\",\"c,\"\"d\"\"\",\"tab\there\"\n"},
		{`\copy pt to '` + file + `' with (format csv)`, ""},
	} {
		out, err := exec.CommandContext(ctx, psql, url, "-X", "-q", "-c", c.cmd).CombinedOutput()
		if err != nil || string(out) != c.want {
			t.Fatalf("psql %s: %v\n%q, want %q", c.cmd, err, out, c.want)
		}
	}
	data, err := os.ReadFile(file)
	if err != nil || string(data) != "1,a b,\n2,\"c,\"\"d\"\"\",tab\there\n" {
		t.Fatalf("\\copy to a client file: %q %v", data, err)
	}
	// And psql loads it back with \copy ... from.
	if _, err := conn.Exec(ctx, `CREATE TABLE pt2 (LIKE pt)`); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.CommandContext(ctx, psql, url, "-X", "-q", "-c", `\copy pt2 from '`+file+`' with (format csv)`).CombinedOutput(); err != nil {
		t.Fatalf("psql \\copy from: %v %s", err, out)
	}
	if out, _, err := copyOut(ctx, conn, `COPY pt2 TO STDOUT WITH (FORMAT csv)`); err != nil || out != string(data) {
		t.Fatalf("round trip through psql: %q %v", out, err)
	}
}
