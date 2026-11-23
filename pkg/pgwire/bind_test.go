package pgwire

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// Bind-time checks on what a client sends, driven as raw protocol
// messages against a server with no KV underneath: Parse and Bind need
// no storage, so a Bind the server refuses is answered before anything
// reaches a transaction (Execute, which would, is not sent).

// trustConn connects to s in trust mode and completes the startup.
func trustConn(t *testing.T, s *Server) (net.Conn, *pgproto3.Frontend) {
	t.Helper()
	nc, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	_ = nc.SetDeadline(time.Now().Add(10 * time.Second))
	fe := pgproto3.NewFrontend(nc, nc)
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "root"}})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatalf("startup: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return nc, fe
		}
	}
}

// exchange sends msgs followed by a Sync and collects the server's
// answers up to its ReadyForQuery.
func exchange(t *testing.T, fe *pgproto3.Frontend, msgs ...pgproto3.FrontendMessage) []pgproto3.BackendMessage {
	t.Helper()
	for _, m := range msgs {
		fe.Send(m)
	}
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	var out []pgproto3.BackendMessage
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatalf("receive after %d message(s) %v: %v", len(out), out, err)
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			cp := *m
			out = append(out, &cp)
		case *pgproto3.ReadyForQuery:
			return out
		default:
			out = append(out, msg)
		}
	}
}

// bindOutcome runs Parse(query) and Bind(b) and returns the Bind's
// error code, or "" for a BindComplete.
func bindOutcome(t *testing.T, fe *pgproto3.Frontend, query string, b *pgproto3.Bind) string {
	t.Helper()
	got := exchange(t, fe, &pgproto3.Parse{Query: query}, b)
	if len(got) == 0 {
		t.Fatalf("no answer to Parse/Bind")
	}
	if e, ok := got[0].(*pgproto3.ErrorResponse); ok {
		t.Fatalf("Parse %q refused: %s %s", query, e.Code, e.Message)
	}
	if len(got) < 2 {
		t.Fatalf("no answer to Bind: %v", got)
	}
	switch m := got[1].(type) {
	case *pgproto3.BindComplete:
		return ""
	case *pgproto3.ErrorResponse:
		return m.Code
	default:
		t.Fatalf("Bind answered with %T", m)
	}
	return ""
}

// TestBindFormatCodeCounts: a Bind's parameter and result format-code
// lists hold 0, 1 or one code per item (protocol rule); any other count
// is a protocol violation (08P01). A short parameter list used to index
// past its end and take the node down with it.
func TestBindFormatCodeCounts(t *testing.T) {
	s := startServer(t, ServerOptions{})
	_, fe := trustConn(t, s)
	const q = "SELECT $1::text, $2::text, $3::text"
	params := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for _, c := range []struct {
		name          string
		param, result []int16
		want          string
	}{
		{"no codes", nil, nil, ""},
		{"one code", []int16{0}, []int16{0}, ""},
		{"a code each", []int16{0, 0, 0}, []int16{0, 1, 0}, ""},
		{"short parameter codes", []int16{0, 0}, nil, "08P01"},
		{"long parameter codes", []int16{0, 0, 0, 0}, nil, "08P01"},
		{"short result codes", nil, []int16{0, 0}, "08P01"},
		{"long result codes", nil, []int16{0, 0, 0, 0}, "08P01"},
	} {
		got := bindOutcome(t, fe, q, &pgproto3.Bind{ParameterFormatCodes: c.param, Parameters: params, ResultFormatCodes: c.result})
		if got != c.want {
			t.Errorf("%s: Bind answered %q, want %q", c.name, got, c.want)
		}
	}
	// The connection, and the server, are still there.
	if got := bindOutcome(t, fe, q, &pgproto3.Bind{Parameters: params}); got != "" {
		t.Fatalf("a valid Bind after the refusals answered %q", got)
	}
}

// TestBindUnsupportedFormatCode: a format code is 0 (text) or 1
// (binary); a Bind carrying any other, for a parameter or a result
// column, is refused with "unsupported format code: N", as PostgreSQL
// refuses it, where it used to be read as text (a parameter) or binary
// (a result column).
func TestBindUnsupportedFormatCode(t *testing.T) {
	s := startServer(t, ServerOptions{})
	_, fe := trustConn(t, s)
	const q = "SELECT $1::text, $2::text"
	params := [][]byte{[]byte("a"), []byte("b")}
	for _, c := range []struct {
		name          string
		param, result []int16
		bad           int16
	}{
		{"parameter code 2 for all", []int16{2}, nil, 2},
		{"parameter code 2 for one", []int16{0, 2}, nil, 2},
		{"negative parameter code", []int16{-1, 0}, nil, -1},
		{"result code 2 for all", nil, []int16{2}, 2},
		{"result code 7 for one", nil, []int16{1, 7}, 7},
	} {
		got := exchange(t, fe, &pgproto3.Parse{Query: q}, &pgproto3.Bind{ParameterFormatCodes: c.param, Parameters: params, ResultFormatCodes: c.result})
		if len(got) < 2 {
			t.Fatalf("%s: no answer to Bind: %v", c.name, got)
		}
		e, ok := got[1].(*pgproto3.ErrorResponse)
		if !ok {
			t.Errorf("%s: Bind answered %T, want an error", c.name, got[1])
			continue
		}
		if want := fmt.Sprintf("unsupported format code: %d", c.bad); e.Code != "08P01" || e.Message != want {
			t.Errorf("%s: Bind answered %s %q, want 08P01 %q", c.name, e.Code, e.Message, want)
		}
	}
	if got := bindOutcome(t, fe, q, &pgproto3.Bind{ParameterFormatCodes: []int16{0, 0}, Parameters: params, ResultFormatCodes: []int16{1}}); got != "" {
		t.Fatalf("a valid Bind after the refusals answered %q", got)
	}
}

// TestFunctionCallUnsupportedFormatCode: pgproto3 refuses to decode a
// fast-path call whose argument or result format code is not 0 or 1, so
// the call never runs; the connection ends with a FATAL 08P01 saying
// why, where it used to close with no word at all.
func TestFunctionCallUnsupportedFormatCode(t *testing.T) {
	s := startServer(t, ServerOptions{})
	oid, ok := vtable.ProcOID("lo_close")
	if !ok {
		t.Fatal("lo_close has no OID")
	}
	fd := binary.BigEndian.AppendUint32(nil, 0)
	for _, c := range []struct {
		name   string
		args   []uint16
		result uint16
	}{
		{"argument code 2", []uint16{2}, 1},
		{"result code 3", []uint16{1}, 3},
	} {
		nc, fe := trustConn(t, s)
		fe.Send(&pgproto3.FunctionCall{Function: uint32(oid), ArgFormatCodes: c.args, Arguments: [][]byte{fd}, ResultFormatCode: c.result})
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
		msg, err := fe.Receive()
		if err != nil {
			t.Fatalf("%s: no error before the close: %v", c.name, err)
		}
		if e, ok := msg.(*pgproto3.ErrorResponse); !ok || e.Code != "08P01" || e.Severity != "FATAL" || !strings.Contains(e.Message, "FunctionCall") {
			t.Fatalf("%s: got %#v, want a FATAL 08P01 naming the FunctionCall", c.name, msg)
		}
		expectClosed(t, nc, 5*time.Second)
	}
}

// TestPanicInConnectionClosesOnlyIt: a panic on one connection's
// goroutine ends that connection with an error, and the server goes on
// serving others.
func TestPanicInConnectionClosesOnlyIt(t *testing.T) {
	// Set before the server starts, and cleared after it stops (cleanups
	// run last-in first-out), so no connection goroutine races the write.
	testingPanicOnMessage = func(m pgproto3.FrontendMessage) {
		if p, ok := m.(*pgproto3.Parse); ok && p.Query == "SELECT 'boom'" {
			panic("test panic")
		}
	}
	t.Cleanup(func() { testingPanicOnMessage = nil })
	s := startServer(t, ServerOptions{})
	nc, fe := trustConn(t, s)
	_, other := trustConn(t, s)
	fe.Send(&pgproto3.Parse{Query: "SELECT 'boom'"})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	msg, err := fe.Receive()
	if err != nil {
		t.Fatalf("no error before the close: %v", err)
	}
	if e, ok := msg.(*pgproto3.ErrorResponse); !ok || e.Code != "XX000" || e.Severity != "FATAL" {
		t.Fatalf("got %#v, want a FATAL XX000", msg)
	}
	expectClosed(t, nc, 5*time.Second)
	if got := bindOutcome(t, other, "SELECT $1::text", &pgproto3.Bind{Parameters: [][]byte{[]byte("x")}}); got != "" {
		t.Fatalf("the other connection's Bind answered %q", got)
	}
}

// binaryArray builds a one-dimensional binary array value declaring n
// elements of type oid, followed by body.
func binaryArray(oid, n uint32, body []byte) []byte {
	raw := binary.BigEndian.AppendUint32(nil, 1) // ndim
	raw = binary.BigEndian.AppendUint32(raw, 0)  // has nulls
	raw = binary.BigEndian.AppendUint32(raw, oid)
	raw = binary.BigEndian.AppendUint32(raw, n) // dimension length
	raw = binary.BigEndian.AppendUint32(raw, 1) // lower bound
	return append(raw, body...)
}

// TestBindBinaryArrayElementCount: a binary array's element count is
// the client's word; one its bytes cannot hold is refused with 22P03
// before anything is sized by it. A 20-byte int8[] declaring
// 0xF0000000 elements used to have the runtime map hundreds of
// gigabytes, a fatal out-of-memory error for the whole node.
func TestBindBinaryArrayElementCount(t *testing.T) {
	s := startServer(t, ServerOptions{})
	_, fe := trustConn(t, s)
	two := binary.BigEndian.AppendUint32(nil, 8)
	two = binary.BigEndian.AppendUint64(two, 7)
	two = binary.BigEndian.AppendUint32(two, 0xffffffff) // NULL
	for _, c := range []struct {
		name string
		raw  []byte
		want string
	}{
		{"two elements", binaryArray(oidInt8, 2, two), ""},
		{"empty", binaryArray(oidInt8, 0, nil), ""},
		{"huge count, no elements", binaryArray(oidInt8, 0xF0000000, nil), "22P03"},
		{"count past the bytes", binaryArray(oidInt8, 4, two), "22P03"},
	} {
		got := bindOutcome(t, fe, "SELECT $1::int8[]", &pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: [][]byte{c.raw}})
		if got != c.want {
			t.Errorf("%s: Bind answered %q, want %q", c.name, got, c.want)
		}
	}
}

// pgMicros is t as a binary timestamp: microseconds since 2000-01-01.
func pgMicros(t time.Time) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(t.UnixMicro()-pgEpochOffsetMicros))
}

func pgDays(t time.Time) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(int32(t.Unix()/86400-pgEpochOffsetDays)))
}

func i64(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

func pgInterval(micros int64, days, months int32) []byte {
	out := i64(micros)
	out = binary.BigEndian.AppendUint32(out, uint32(days))
	return binary.BigEndian.AppendUint32(out, uint32(months))
}

// TestBinaryDatetimeRange: binary timestamp, date, interval and time
// parameters outside what the text input accepts are refused with
// 22008, PostgreSQL's infinity encodings among them, where they used to
// wrap silently: the year 2400 was stored as 1815, timestamptz infinity
// as 1999-12-31, date infinity as 5881610-07-11, an interval of 2^62 µs
// as 00:00:00. The same decoder serves binary COPY.
func TestBinaryDatetimeRange(t *testing.T) {
	utc := func(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }
	for _, c := range []struct {
		name string
		fam  types.Family
		raw  []byte
		want int64 // the stored value when accepted
		code string
	}{
		{"timestamp 2024", types.Timestamp, pgMicros(utc(2024, 7, 1)), utc(2024, 7, 1).UnixNano(), ""},
		{"timestamp first", types.Timestamp, pgMicros(utc(1678, 1, 1)), utc(1678, 1, 1).UnixNano(), ""},
		{"timestamp last", types.Timestamp, pgMicros(utc(2262, 1, 1).Add(-time.Microsecond)), utc(2262, 1, 1).Add(-time.Microsecond).UnixNano(), ""},
		{"timestamp 2262", types.Timestamp, pgMicros(utc(2262, 1, 1)), 0, "22008"},
		{"timestamp 2400", types.Timestamp, pgMicros(utc(2400, 1, 1)), 0, "22008"},
		{"timestamp 1677", types.Timestamp, pgMicros(utc(1677, 12, 31)), 0, "22008"},
		{"timestamp infinity", types.Timestamp, i64(math.MaxInt64), 0, "22008"},
		{"timestamp -infinity", types.Timestamp, i64(math.MinInt64), 0, "22008"},
		{"date 2400", types.Date, pgDays(utc(2400, 1, 1)), utc(2400, 1, 1).Unix() / 86400, ""},
		{"date 9999-12-31", types.Date, pgDays(utc(9999, 12, 31)), utc(9999, 12, 31).Unix() / 86400, ""},
		{"date 0000-01-01", types.Date, pgDays(utc(0, 1, 1)), utc(0, 1, 1).Unix() / 86400, ""},
		{"date 10000", types.Date, pgDays(utc(10000, 1, 1)), 0, "22008"},
		{"date before year 0", types.Date, pgDays(utc(-1, 12, 31)), 0, "22008"},
		{"date infinity", types.Date, binary.BigEndian.AppendUint32(nil, math.MaxInt32), 0, "22008"},
		{"date -infinity", types.Date, binary.BigEndian.AppendUint32(nil, 1<<31), 0, "22008"},
		{"interval 1 h", types.IntervalFam, pgInterval(3600e6, 2, 3), int64(time.Hour), ""},
		{"interval largest", types.IntervalFam, pgInterval(math.MaxInt64/1000, 0, 0), math.MaxInt64 / 1000 * 1000, ""},
		{"interval 2^62 us", types.IntervalFam, pgInterval(1<<62, 0, 0), 0, "22008"},
		{"interval -2^62 us", types.IntervalFam, pgInterval(-1<<62, 0, 0), 0, "22008"},
		{"interval infinity", types.IntervalFam, pgInterval(math.MaxInt64, math.MaxInt32, math.MaxInt32), 0, "22008"},
		{"time noon", types.Time, i64(12 * 3600e6), int64(12 * time.Hour), ""},
		{"time 24:00", types.Time, i64(24 * 3600e6), int64(24 * time.Hour), ""},
		{"time past 24:00", types.Time, i64(24*3600e6 + 1), 0, "22008"},
		{"time negative", types.Time, i64(-1), 0, "22008"},
		{"time 2^62 us", types.Time, i64(1 << 62), 0, "22008"},
	} {
		d, err := decodeBinaryParam(c.raw, c.fam)
		switch {
		case c.code == "" && err != nil:
			t.Errorf("%s: refused: %v", c.name, err)
		case c.code == "" && d.I != c.want:
			t.Errorf("%s: decoded %d, want %d", c.name, d.I, c.want)
		case c.code != "" && binaryErrCode(err, "") != c.code:
			t.Errorf("%s: decoded %+v, %v; want %s", c.name, d, err, c.code)
		}
	}
}

// TestBindBinaryDatetimeRange: the 22008 reaches the client as the
// Bind's error.
func TestBindBinaryDatetimeRange(t *testing.T) {
	s := startServer(t, ServerOptions{})
	_, fe := trustConn(t, s)
	y2400 := pgMicros(time.Date(2400, 1, 1, 0, 0, 0, 0, time.UTC))
	for _, c := range []struct {
		query string
		raw   []byte
		want  string
	}{
		{"SELECT $1::timestamptz", pgMicros(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)), ""},
		{"SELECT $1::timestamptz", y2400, "22008"},
		{"SELECT $1::timestamptz", i64(math.MaxInt64), "22008"},
		{"SELECT $1::date", binary.BigEndian.AppendUint32(nil, math.MaxInt32), "22008"},
		{"SELECT $1::interval", pgInterval(1<<62, 0, 0), "22008"},
	} {
		got := bindOutcome(t, fe, c.query, &pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: [][]byte{c.raw}})
		if got != c.want {
			t.Errorf("%s with %x: Bind answered %q, want %q", c.query, c.raw, got, c.want)
		}
	}
}
