package testcluster

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// rawRowsOf runs q on the raw connection and returns each DataRow's
// values, copied: through a simple Query when formats is nil, else
// through Parse/Bind/Execute with those result formats.
func (r *rawFrontend) rawRowsOf(q string, formats []int16) [][][]byte {
	r.t.Helper()
	if formats == nil {
		r.send(&pgproto3.Query{String: q})
	} else {
		r.send(
			&pgproto3.Parse{Query: q},
			&pgproto3.Bind{ResultFormatCodes: formats},
			&pgproto3.Execute{},
			&pgproto3.Sync{},
		)
	}
	var rows [][][]byte
	for {
		switch m := r.recv().(type) {
		case *pgproto3.DataRow:
			row := make([][]byte, len(m.Values))
			for i, v := range m.Values {
				if v != nil {
					row[i] = append([]byte{}, v...)
				}
			}
			rows = append(rows, row)
		case *pgproto3.ErrorResponse:
			r.t.Fatalf("%s: %s %s", q, m.Code, m.Message)
		case *pgproto3.ReadyForQuery:
			return rows
		}
	}
}

// wireTableSetup creates wt: a column of every value type, with NULLs,
// a dropped column, a column added later with a default, and an index
// on g.
var wireTableSetup = []string{
	`CREATE TYPE wmood AS ENUM ('sad', 'ok', 'happy')`,
	`CREATE TABLE wt (
		k INT8 PRIMARY KEY, g INT8, i2 INT2, i4 INT4, t TEXT, vc VARCHAR(10), ch CHAR(5),
		bb BYTEA, b BOOL, f FLOAT8, n NUMERIC, n2 NUMERIC(10,2), ts TIMESTAMP, tz TIMESTAMPTZ,
		d DATE, tm TIME, iv INTERVAL, u UUID, j JSONB, ai INT8[], tt TEXT[], m wmood, gone INT8)`,
	`CREATE INDEX wt_g ON wt (g)`,
	`INSERT INTO wt VALUES
		(1, 1, -32768, 2147483647, 'héllo', 'abc', 'ab', '\x00ff5c61', true, 3.141592653589793, -12345.678, 9.9,
		 '2026-08-30 01:02:03.456789', '2026-08-30 01:02:03.456789+00', '2026-08-30', '13:14:15.5', '1 year 2 mons 3 days 04:05:06.007',
		 '12345678-9abc-def0-0102-030405060708', '{"a": [1, 2.5, "x"], "b": null}', '{1,NULL,-3}', '{"a b","",NULL,"q\"x"}', 'happy', 5),
		(2, 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
		(3, 1, 0, 0, '', '', '', '', false, 'NaN', 'NaN', 0, '1999-12-31 23:59:59', '1970-01-01 00:00:00+00', '1999-12-31', '00:00:00', '-1 days',
		 '00000000-0000-0000-0000-000000000000', '"s"', '{}', '{}', 'sad', NULL),
		(-4, 2, 7, -7, 'x', 'y', 'abcde', '\x', false, '-Infinity', 0.000001, -99999999.99, '2000-01-01 00:00:00', '2000-01-01 00:00:00+00', '2000-01-01', '23:59:59.999999', '00:00:00.000001',
		 'ffffffff-ffff-ffff-ffff-ffffffffffff', '[]', '{9223372036854775807}', '{"z"}', 'ok', 1)`,
	`ALTER TABLE wt DROP COLUMN gone`,
	`ALTER TABLE wt ADD COLUMN fill INT8 DEFAULT 7`,
	`INSERT INTO wt (k, g, t, fill) VALUES (5, 1, 'later', NULL)`,
}

// TestScanWireOutputEqual: a streamed scan of bare columns (decoded
// into one reused map and projected at once, encoded straight into the
// wire buffer) sends the same DataRow bytes as the same rows through
// the per-row-map projection (an expression in the select list) and
// through a materialized result (a sort), in text over both protocols
// and in binary, for every value type with NULLs, under the session
// settings that change how values render.
func TestScanWireOutputEqual(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, s := range wireTableSetup {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	const ncols = 23
	r := dialRaw(t, tc)
	for _, setting := range []string{
		"",
		"SET TimeZone = 'America/St_Johns'",
		"SET IntervalStyle = 'iso_8601'",
		"SET bytea_output = 'escape'",
		"SET extra_float_digits = -2",
	} {
		if setting != "" {
			r.rawRowsOf(setting, nil)
		}
		for _, f := range []struct {
			name    string
			formats []int16
		}{{"simple", nil}, {"text", []int16{0}}, {"binary", []int16{1}}} {
			for _, q := range []struct{ plain, mapped, materialized string }{
				{ // the full scan
					`SELECT * FROM wt`,
					`SELECT *, k + 0 FROM wt`,
					`SELECT * FROM wt ORDER BY k + 0`,
				},
				{ // a residual filter
					`SELECT k, t, j, ai, tz FROM wt WHERE i4 <> 0 OR i4 IS NULL`,
					`SELECT k, t, j, ai, tz, 1 FROM wt WHERE i4 <> 0 OR i4 IS NULL`,
					`SELECT k, t, j, ai, tz FROM wt WHERE i4 <> 0 OR i4 IS NULL ORDER BY k + 0`,
				},
				{ // an index scan, columns reordered and repeated
					`SELECT n2, k, ch, k, iv FROM wt WHERE g = 1`,
					`SELECT n2, k, ch, k, iv, g * 2 FROM wt WHERE g = 1`,
					`SELECT n2, k, ch, k, iv FROM wt WHERE g = 1 ORDER BY k + 0`,
				},
				{ // a reverse primary-key range
					`SELECT * FROM wt WHERE k > -10 ORDER BY k DESC`,
					`SELECT *, 'x' FROM wt WHERE k > -10 ORDER BY k DESC`,
					`SELECT * FROM wt WHERE k > -10 ORDER BY k + 0 DESC`,
				},
			} {
				plain := r.rawRowsOf(q.plain, f.formats)
				mapped := r.rawRowsOf(q.mapped, f.formats)
				mat := r.rawRowsOf(q.materialized, f.formats)
				if len(plain) == 0 || len(plain) != len(mapped) || len(plain) != len(mat) {
					t.Fatalf("%q %s %s: %d / %d / %d rows", setting, f.name, q.plain, len(plain), len(mapped), len(mat))
				}
				for i := range plain {
					if q.plain == `SELECT * FROM wt` && len(plain[i]) != ncols {
						t.Fatalf("row of %d values, want %d", len(plain[i]), ncols)
					}
					for c := range plain[i] {
						p, m, x := plain[i][c], mapped[i][c], mat[i][c]
						if !bytes.Equal(p, m) || !bytes.Equal(p, x) || (p == nil) != (m == nil) || (p == nil) != (x == nil) {
							t.Errorf("%q %s %s: row %d value %d: plain %q, mapped %q, materialized %q", setting, f.name, q.plain, i, c, p, m, x)
						}
					}
				}
			}
		}
	}
	// Anchors, in text: the values the encoders write directly.
	r.rawRowsOf("SET bytea_output = 'hex'", nil)
	rows := r.rawRowsOf(`SELECT k, i2, i4, t, vc, ch, bb, u FROM wt WHERE k = 3 OR k = -4`, nil)
	want := [][]string{
		{"-4", "7", "-7", "x", "y", "abcde", `\x`, "ffffffff-ffff-ffff-ffff-ffffffffffff"},
		{"3", "0", "0", "", "", "     ", `\x`, "00000000-0000-0000-0000-000000000000"},
	}
	for i := range want {
		for c, w := range want[i] {
			if got := rows[i][c]; got == nil || string(got) != w {
				t.Errorf("row %d value %d: %q, want %q", i, c, got, w)
			}
		}
	}
}

// scanBench is a hijacked connection that runs a query over scanb
// (SELECT k, pad FROM scanb unless startScanBenchQuery says otherwise)
// and reads every frame, allocating nothing per row.
type scanBench struct {
	tb  testing.TB
	nc  net.Conn
	rd  *bufio.Reader
	msg []byte
	in  []byte
}

// startScanBench loads scanb (k INT8 PRIMARY KEY, pad TEXT) with rows
// rows of 256-byte padding on a fresh one-node cluster and returns a
// connection that reads it in text (a simple Query) or binary (a
// prepared statement's Bind/Execute with binary results).
func startScanBench(tb testing.TB, rows int, binaryResults bool) *scanBench {
	return startScanBenchQuery(tb, rows, binaryResults, "SELECT k, pad FROM scanb", rows)
}

// scanBenchBodies are scanb's body texts, row k taking the k%4th: one
// row in four mentions a fox.
var scanBenchBodies = []string{
	"The quick brown fox jumps over the lazy dog near the riverbank",
	"A slow green turtle walks under the busy bridge every morning",
	"Several tired horses were resting quietly in the shaded meadow",
	"Bright yellow lanterns lit the narrow streets of the old harbour",
}

// startScanBenchQuery is startScanBench for query q, which must return
// want rows; scanb also has n INT8 (= k) and body TEXT (scanBenchBodies).
func startScanBenchQuery(tb testing.TB, rows int, binaryResults bool, q string, want int) *scanBench {
	tb.Helper()
	tc := Start(tb, 1)
	ctx := context.Background()
	setup, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := setup.Exec(ctx, "CREATE TABLE scanb (k INT8 PRIMARY KEY, pad TEXT, n INT8, body TEXT)"); err != nil {
		tb.Fatal(err)
	}
	pad := strings.Repeat("p", 256)
	for i := 0; i < rows; i += 1000 {
		var sb strings.Builder
		sb.WriteString("INSERT INTO scanb VALUES ")
		for j := i; j < min(i+1000, rows); j++ {
			if j > i {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "(%d, '%s', %d, '%s')", j, pad, j, scanBenchBodies[j%len(scanBenchBodies)])
		}
		if _, err := setup.Exec(ctx, sb.String()); err != nil {
			tb.Fatal(err)
		}
	}
	_ = setup.Close(ctx)

	pc, err := pgconn.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := pc.Prepare(ctx, "s", q, nil); err != nil {
		tb.Fatal(err)
	}
	hj, err := pc.Hijack()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = hj.Conn.Close() })
	sb := &scanBench{tb: tb, nc: hj.Conn, rd: bufio.NewReaderSize(hj.Conn, 64<<10)}
	if !binaryResults {
		sb.msg = binary.BigEndian.AppendUint32(append(sb.msg, 'Q'), uint32(4+len(q)+1))
		sb.msg = append(append(sb.msg, q...), 0)
	} else {
		sb.msg = append(sb.msg, 'B', 0, 0, 0, 0, 0, 's', 0, 0, 0, 0, 0, 0, 1, 0, 1)
		binary.BigEndian.PutUint32(sb.msg[1:5], uint32(len(sb.msg)-1))
		sb.msg = append(sb.msg, 'E', 0, 0, 0, 9, 0, 0, 0, 0, 0, 'S', 0, 0, 0, 4)
	}
	if got := sb.run(); got != want {
		tb.Fatalf("%d rows, want %d", got, want)
	}
	return sb
}

// run sends the query and reads to ReadyForQuery, returning the rows.
func (sb *scanBench) run() int {
	if _, err := sb.nc.Write(sb.msg); err != nil {
		sb.tb.Fatal(err)
	}
	rows := 0
	var hdr [5]byte
	for {
		if _, err := io.ReadFull(sb.rd, hdr[:]); err != nil {
			sb.tb.Fatal(err)
		}
		n := int(binary.BigEndian.Uint32(hdr[1:])) - 4
		if cap(sb.in) < n {
			sb.in = make([]byte, n)
		}
		if _, err := io.ReadFull(sb.rd, sb.in[:n]); err != nil {
			sb.tb.Fatal(err)
		}
		switch hdr[0] {
		case 'D':
			rows++
		case 'E':
			sb.tb.Fatalf("server error: %q", sb.in[:n])
		case 'Z':
			return rows
		}
	}
}

// measure runs the query times times and returns the rows read, the
// time taken and the process's allocations meanwhile.
func (sb *scanBench) measure(times int) (rows int, elapsed time.Duration, mallocs, bytes uint64) {
	rows, elapsed, mallocs, bytes, _ = sb.measureCPU(times)
	return rows, elapsed, mallocs, bytes
}

// measureCPU is measure, also returning the processor time the whole
// process (the node, its background work and the reading client) used
// meanwhile: steadier than the elapsed time on a shared machine.
func (sb *scanBench) measureCPU(times int) (rows int, elapsed time.Duration, mallocs, bytes uint64, cpu time.Duration) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	cpuBefore := processCPU()
	start := time.Now()
	for i := 0; i < times; i++ {
		rows += sb.run()
	}
	elapsed = time.Since(start)
	cpu = processCPU() - cpuBefore
	runtime.ReadMemStats(&after)
	return rows, elapsed, after.Mallocs - before.Mallocs, after.TotalAlloc - before.TotalAlloc, cpu
}

// BenchmarkScanStream measures a large result set end to end on one
// in-process node: SELECT k, pad FROM a table of 20,000 rows of
// 256-byte padding, streamed through pgwire to a client that reads
// every frame and allocates nothing per row. It reports rows/s, ns/row,
// allocs/row and B/row; the allocations are the whole process's (the
// node, plus its background work) divided by the rows sent.
//
//	go test ./pkg/testutils/testcluster -run - -bench BenchmarkScanStream -benchtime 20x
func BenchmarkScanStream(b *testing.B) {
	for _, mode := range []string{"text", "binary"} {
		b.Run(mode, func(b *testing.B) {
			sb := startScanBench(b, 20000, mode == "binary")
			b.ResetTimer()
			rows, elapsed, mallocs, bytes := sb.measure(b.N)
			b.StopTimer()
			b.ReportMetric(float64(rows)/elapsed.Seconds(), "rows/s")
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(rows), "ns/row")
			b.ReportMetric(float64(mallocs)/float64(rows), "allocs/row")
			b.ReportMetric(float64(bytes)/float64(rows), "B/row")
		})
	}
}

// scanShapes are streamed scans off the bare-column path: computed
// projections, a computed filter, a reverse scan and a text-search
// filter, over startScanBenchQuery's table of rows rows.
func scanShapes(rows int) []struct {
	name, q string
	want    int
} {
	return []struct {
		name, q string
		want    int
	}{
		{"exprs", "SELECT k + 1, n * 2 FROM scanb", rows},
		{"lower", "SELECT k, lower(pad) FROM scanb", rows},
		{"computed-where", "SELECT k, pad FROM scanb WHERE n % 3 = 0", (rows + 2) / 3},
		{"reverse", "SELECT k, pad FROM scanb WHERE k >= 0 ORDER BY k DESC", rows},
		{"tsearch", "SELECT k FROM scanb WHERE to_tsvector('english', body) @@ to_tsquery('english', 'fox')", (rows + 3) / 4},
	}
}

// BenchmarkScanShapes is BenchmarkScanStream for scanShapes, in text.
//
//	go test ./pkg/testutils/testcluster -run - -bench BenchmarkScanShapes -benchtime 10x
func BenchmarkScanShapes(b *testing.B) {
	const rows = 20000
	for _, sh := range scanShapes(rows) {
		b.Run(sh.name, func(b *testing.B) {
			sb := startScanBenchQuery(b, rows, false, sh.q, sh.want)
			b.ResetTimer()
			got, elapsed, mallocs, bytes, cpu := sb.measureCPU(b.N)
			b.StopTimer()
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(got), "ns/row")
			b.ReportMetric(float64(cpu.Nanoseconds())/float64(got), "cpu-ns/row")
			b.ReportMetric(float64(mallocs)/float64(got), "allocs/row")
			b.ReportMetric(float64(bytes)/float64(got), "B/row")
		})
	}
}

// processCPU is the processor time, user and system, the process has
// used so far, as the kernel accounts it.
func processCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
