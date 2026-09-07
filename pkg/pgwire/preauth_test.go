package pgwire

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/testutils/scramclient"
	"github.com/sthorne/datax/pkg/util/stop"
)

// The pre-authentication bounds on the SQL listener (issue #212): a
// deadline on startup, a cap on connections that have not finished it,
// and the password-guessing budget consulted before the verifier
// lookup. These run against a server with no database — nothing here
// gets as far as needing one.

// startServer serves opts on a loopback listener with no KV underneath.
func startServer(t *testing.T, opts ServerOptions) *Server {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stopper := stop.NewStopper()
	t.Cleanup(stopper.Stop)
	return Serve(lis, nil, nil, stopper, opts)
}

// connCount is how many connections the server holds, pending or not.
func (s *Server) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// registered reports whether any connection is in the cancel registry.
func (s *Server) registered() int {
	s.cancel.mu.Lock()
	defer s.cancel.mu.Unlock()
	return len(s.cancel.byPID)
}

// waitFor polls cond for up to d.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s", what, d)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// expectClosed reads nc until the server closes it, failing if that
// takes longer than d. It returns whatever the server sent first.
func expectClosed(t *testing.T, nc net.Conn, d time.Duration) []byte {
	t.Helper()
	_ = nc.SetReadDeadline(time.Now().Add(d))
	var got []byte
	buf := make([]byte, 512)
	for {
		n, err := nc.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			return got
		}
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("the server held the connection past %s", d)
			}
			return got // a reset is a close too
		}
	}
}

// TestStartupDeadlineClosesASilentConnection: a connection that sends
// nothing is closed at the deadline, and everything the accept loop
// allocated for it — its conns entry, its cancel-registry entry, its
// pending slot — is released. Before #212 it was held indefinitely.
func TestStartupDeadlineClosesASilentConnection(t *testing.T) {
	s := startServer(t, ServerOptions{AuthTimeout: 300 * time.Millisecond})
	nc, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	waitFor(t, 5*time.Second, "the connection to be accepted", func() bool { return s.connCount() == 1 })
	if got := s.registered(); got != 1 {
		t.Fatalf("%d cancel-registry entries for one pending connection", got)
	}
	start := time.Now()
	expectClosed(t, nc, 5*time.Second)
	if since := time.Since(start); since > 3*time.Second {
		t.Fatalf("closed after %s; the deadline was 300ms", since)
	}
	waitFor(t, 5*time.Second, "the conns entry to be released", func() bool { return s.connCount() == 0 })
	waitFor(t, 5*time.Second, "the cancel-registry entry to be released", func() bool { return s.registered() == 0 })
	waitFor(t, 5*time.Second, "the pending slot to be released", func() bool { return s.pending.Load() == 0 })
}

// verifierFor is an Authenticator that knows one user, and counts how
// often it was asked — the point of the budget is that a refused
// attempt never reaches it.
type verifierFor struct {
	user string
	v    *security.ScramVerifier
	mu   sync.Mutex
	asks int
}

func newVerifierFor(t *testing.T, user, password string) *verifierFor {
	t.Helper()
	v, err := security.MakeScramVerifier(password)
	if err != nil {
		t.Fatal(err)
	}
	return &verifierFor{user: user, v: v}
}

func (a *verifierFor) lookup(_ context.Context, user string) (*security.ScramVerifier, error) {
	a.mu.Lock()
	a.asks++
	a.mu.Unlock()
	if user == a.user {
		return a.v, nil
	}
	return nil, nil
}

func (a *verifierFor) asked() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.asks
}

// scramOpts is a listener that authenticates with SCRAM over cleartext
// (no TLS, so a test client needs no certificates) against auth.
func scramOpts(auth *verifierFor) ServerOptions {
	return ServerOptions{
		Auth:       auth.lookup,
		MockSecret: func(context.Context) []byte { return []byte("test cluster secret") },
	}
}

// startSCRAM sends a StartupMessage for user and reads the server's
// answer, which is either the SASL advertisement or an error.
func startSCRAM(t *testing.T, nc net.Conn, user string) (*pgproto3.Frontend, pgproto3.BackendMessage) {
	t.Helper()
	fe := pgproto3.NewFrontend(nc, nc)
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": user}})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	msg, err := fe.Receive()
	if err != nil {
		t.Fatalf("after StartupMessage: %v", err)
	}
	return fe, msg
}

// TestStartupDeadlineClosesAStalledSCRAMExchange: a client that sends its
// SASLInitialResponse and then nothing is holding the server between
// server-first and client-final; it is closed at the deadline like a
// silent one. This is the stall the 30-second lookup context never
// bounded — it covered the KV read, not the socket.
func TestStartupDeadlineClosesAStalledSCRAMExchange(t *testing.T) {
	auth := newVerifierFor(t, "alice", "hunter2")
	opts := scramOpts(auth)
	opts.AuthTimeout = 300 * time.Millisecond
	s := startServer(t, opts)
	nc, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	fe, msg := startSCRAM(t, nc, "alice")
	if _, ok := msg.(*pgproto3.AuthenticationSASL); !ok {
		t.Fatalf("expected the SASL advertisement, got %T", msg)
	}
	fe.Send(&pgproto3.SASLInitialResponse{AuthMechanism: security.MechScram, Data: []byte("n,,n=alice,r=clientnonce0000000000")})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	// Server-first arrives; the client then stalls.
	if _, err := fe.Receive(); err != nil {
		t.Fatalf("after client-first: %v", err)
	}
	start := time.Now()
	expectClosed(t, nc, 5*time.Second)
	if since := time.Since(start); since > 3*time.Second {
		t.Fatalf("closed after %s; the deadline was 300ms", since)
	}
	waitFor(t, 5*time.Second, "the connection to be released", func() bool { return s.connCount() == 0 && s.pending.Load() == 0 })
}

// TestPendingCapRefusesRatherThanHolds: past MaxPendingAuth a new
// connection gets a FATAL 53300 and a close, with nothing allocated for
// it — and the cap is on the pending state, not on connections: once a
// pending one ends, the next is admitted.
func TestPendingCapRefusesRatherThanHolds(t *testing.T) {
	s := startServer(t, ServerOptions{MaxPendingAuth: 2, AuthTimeout: 10 * time.Second})
	var held []net.Conn
	for i := 0; i < 2; i++ {
		nc, err := net.Dial("tcp", s.Addr())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, nc)
	}
	defer func() {
		for _, nc := range held {
			_ = nc.Close()
		}
	}()
	waitFor(t, 5*time.Second, "two pending connections", func() bool { return s.pending.Load() == 2 })

	third, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	got := expectClosed(t, third, 5*time.Second)
	if len(got) == 0 || got[0] != 'E' {
		t.Fatalf("a refused connection got %q, want an ErrorResponse", got)
	}
	msg, err := pgproto3.NewFrontend(&byteReader{got}, nil).Receive()
	if err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	e, ok := msg.(*pgproto3.ErrorResponse)
	if !ok || e.Severity != "FATAL" || e.Code != "53300" {
		t.Fatalf("refusal is %+v, want FATAL 53300", msg)
	}
	if n := s.connCount(); n != 2 {
		t.Fatalf("%d connections held after a refusal, want the 2 admitted", n)
	}
	if n := s.pending.Load(); n != 2 {
		t.Fatalf("pending count %d after a refusal, want 2: the refused connection must not take a slot", n)
	}

	// Free a slot and the next connection is admitted.
	_ = held[0].Close()
	waitFor(t, 5*time.Second, "the closed connection's slot to be released", func() bool { return s.pending.Load() == 1 })
	fourth, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer fourth.Close()
	waitFor(t, 5*time.Second, "the next connection to be admitted", func() bool { return s.pending.Load() == 2 })
	_ = fourth.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if n, err := fourth.Read(make([]byte, 1)); err == nil || n != 0 {
		t.Fatal("an admitted connection was answered before it sent anything")
	}
}

// byteReader feeds a captured refusal to a Frontend for decoding.
type byteReader struct{ b []byte }

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

// recordingLimiter is an AuthLimiter that answers as told and records
// what it was asked.
type recordingLimiter struct {
	mu                       sync.Mutex
	budget                   bool
	asked, failed, succeeded int
	lastSource, lastUser     string
}

func (l *recordingLimiter) Budget(source, user string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asked++
	l.lastSource, l.lastUser = source, user
	return l.budget
}
func (l *recordingLimiter) Failed(string, string)    { l.mu.Lock(); l.failed++; l.mu.Unlock() }
func (l *recordingLimiter) Succeeded(string, string) { l.mu.Lock(); l.succeeded++; l.mu.Unlock() }

// TestBudgetIsAskedBeforeTheLookup: with no budget, the attempt is
// refused before the verifier is looked up, with a FATAL that names no
// user — and the refusal is the same for a user that exists and one
// that does not.
func TestBudgetIsAskedBeforeTheLookup(t *testing.T) {
	auth := newVerifierFor(t, "alice", "hunter2")
	lim := &recordingLimiter{budget: false}
	opts := scramOpts(auth)
	opts.AuthLimiter = lim
	s := startServer(t, opts)

	var refusals []*pgproto3.ErrorResponse
	for _, user := range []string{"alice", "nosuchuser"} {
		nc, err := net.Dial("tcp", s.Addr())
		if err != nil {
			t.Fatal(err)
		}
		_, msg := startSCRAM(t, nc, user)
		e, ok := msg.(*pgproto3.ErrorResponse)
		if !ok {
			t.Fatalf("%s: with no budget the answer must be a refusal, got %T", user, msg)
		}
		refusals = append(refusals, e)
		expectClosed(t, nc, 5*time.Second)
		_ = nc.Close()
	}
	if auth.asked() != 0 {
		t.Fatalf("the verifier was looked up %d times for attempts the budget refused", auth.asked())
	}
	if lim.failed != 0 {
		t.Fatal("a refused attempt was charged as a failure: it never ran")
	}
	a, b := refusals[0], refusals[1]
	if a.Severity != "FATAL" || a.Code != b.Code || a.Message != b.Message || a.Severity != b.Severity {
		t.Fatalf("a known and an unknown user are refused differently: %+v vs %+v", a, b)
	}
	if lim.lastUser != "nosuchuser" || lim.lastSource == "" {
		t.Fatalf("the budget was asked about %q from %q", lim.lastUser, lim.lastSource)
	}
}

// TestBudgetIsChargedOnFailureAndRefundedOnSuccess: the exchange runs
// when there is budget; a wrong password charges it, a right one
// refunds it. Charging after the outcome rather than before it is what
// lets a pool's simultaneous correct sign-ins all through (tested at
// the cluster level, against the real limiter).
func TestBudgetIsChargedOnFailureAndRefundedOnSuccess(t *testing.T) {
	auth := newVerifierFor(t, "alice", "hunter2")
	lim := &recordingLimiter{budget: true}
	opts := scramOpts(auth)
	opts.AuthLimiter = lim
	s := startServer(t, opts)

	for i, pw := range []string{"wrong", "hunter2"} {
		nc, err := net.Dial("tcp", s.Addr())
		if err != nil {
			t.Fatal(err)
		}
		ok := scramLogin(t, nc, "alice", pw)
		_ = nc.Close()
		if ok != (i == 1) {
			t.Fatalf("password %q: authenticated=%v", pw, ok)
		}
	}
	if lim.asked != 2 || lim.failed != 1 || lim.succeeded != 1 {
		t.Fatalf("asked %d, failed %d, succeeded %d; want 2, 1, 1", lim.asked, lim.failed, lim.succeeded)
	}
	if auth.asked() != 2 {
		t.Fatalf("the verifier was looked up %d times for two attempts with budget", auth.asked())
	}
}

// scramLogin runs the client side of SCRAM-SHA-256 over nc and reports
// whether the server accepted the password.
func scramLogin(t *testing.T, nc net.Conn, user, password string) bool {
	t.Helper()
	fe, msg := startSCRAM(t, nc, user)
	if _, ok := msg.(*pgproto3.AuthenticationSASL); !ok {
		t.Fatalf("expected the SASL advertisement, got %T", msg)
	}
	client := &scramclient.Client{User: user, Password: password, Nonce: "clientnonce0000000000"}
	fe.Send(&pgproto3.SASLInitialResponse{AuthMechanism: security.MechScram, Data: []byte(client.First())})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	msg, err := fe.Receive()
	if err != nil {
		t.Fatal(err)
	}
	cont, ok := msg.(*pgproto3.AuthenticationSASLContinue)
	if !ok {
		t.Fatalf("expected server-first, got %T", msg)
	}
	final, err := client.Final(string(cont.Data))
	if err != nil {
		t.Fatal(err)
	}
	fe.Send(&pgproto3.SASLResponse{Data: []byte(final)})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	for {
		msg, err = fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch msg.(type) {
		case *pgproto3.AuthenticationOk:
			return true
		case *pgproto3.ErrorResponse:
			return false
		}
	}
}

// TestCancelConnectionIsNotParked: the connection that carried a
// CancelRequest carries nothing else and never authenticates, so the
// server ends it rather than leaving it in the message loop — where,
// its startup deadline cleared, it would sit for as long as the client
// cared to hold it. The deadline here is long so that it is the server
// ending the connection, not the deadline.
func TestCancelConnectionIsNotParked(t *testing.T) {
	s := startServer(t, ServerOptions{AuthTimeout: 30 * time.Second})
	nc, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	fe := pgproto3.NewFrontend(nc, nc)
	fe.Send(&pgproto3.CancelRequest{ProcessID: 1, SecretKey: []byte{1, 2, 3, 4}})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	expectClosed(t, nc, 5*time.Second)
	waitFor(t, 5*time.Second, "the cancel connection to be released", func() bool { return s.connCount() == 0 && s.pending.Load() == 0 })
}
