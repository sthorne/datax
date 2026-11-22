package pgwire

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
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
