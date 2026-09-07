package testcluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/testutils/scramclient"
	"github.com/sthorne/datax/pkg/util/log"
)

// The SQL port's pre-authentication bounds (issue #212), end to end
// against the real limiter and real clients. The wire-level mechanics
// — the deadline, the pending cap, the order of budget and lookup — are
// pinned in pkg/pgwire; what is pinned here is that the SQL door and
// the HTTP doors spend one budget, and the property the order of
// charging exists for.

// connectSecureFrom is connectSecure with the client's own address
// chosen, so a guesser and an operator can hold separate budgets on
// loopback (httpsClientFrom does the same for the HTTP doors).
func connectSecureFrom(ctx context.Context, url, localIP string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	ip := net.ParseIP(localIP)
	if ip == nil {
		return nil, errors.New("bad local address " + localIP)
	}
	cfg.DialFunc = (&net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}, Timeout: 10 * time.Second}).DialContext
	return pgx.ConnectConfig(ctx, cfg)
}

// waitForRoot retries until root's seeded credential authenticates.
func waitForRoot(t *testing.T, ctx context.Context, tc *TestCluster, certsDir string) *pgx.Conn {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// pgCode is the SQLSTATE of a connection failure ("" if it was not the
// server's error).
func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// TestSQLPasswordGuessingIsBounded: guessing at one account over the SQL
// port is refused after the same burst /api/login allows, before the
// verifier is looked up; a sign-in from elsewhere and another account
// from the guessing address are unaffected; and the refusal is the same
// budget the HTTP doors spend, so switching ports gains nothing.
func TestSQLPasswordGuessingIsBounded(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := waitForRoot(t, ctx, tc, certsDir)
	defer root.Close(ctx)
	if _, err := root.Exec(ctx, `CREATE USER target WITH PASSWORD 'rightpw'`); err != nil {
		t.Fatal(err)
	}
	// Recorded from here: root's own retries above fail until the
	// credential is seeded, and those are not the guesses being counted.
	rec := &auditRecorder{}
	log.SetAuditSink(rec.record)
	defer log.SetAuditSink(nil)

	// The guesser has its own address; the operator keeps 127.0.0.1.
	const guesser = "127.0.0.2"
	url := secureURL(tc, certsDir, "target", "wrongpw")
	var ran, refused int
	start := time.Now()
	for i := 0; i < 20; i++ {
		_, err := connectSecureFrom(ctx, url, guesser)
		switch code := pgCode(err); code {
		case "28P01": // the exchange ran and the password was wrong
			ran++
		case "28000": // refused before the exchange
			refused++
		default:
			t.Fatalf("guess %d: %v (SQLSTATE %q)", i, err, code)
		}
	}
	// The account burst (5, server.authAccountBurst), plus one for each
	// second the twenty handshakes took (the refill), plus one for the
	// fraction: the bound is on the rate, so it is measured against the
	// time taken rather than fixed.
	elapsed := time.Since(start)
	if limit := 5 + int(elapsed.Seconds()) + 1; ran > limit || refused == 0 {
		t.Fatalf("%d guesses ran in %s and %d were refused (limit %d): the SQL door is not bounded", ran, elapsed, refused, limit)
	}
	failures := rec.count("sql-auth-failure")
	if failures != ran {
		t.Fatalf("%d sql-auth-failure records for %d guesses that ran: a refused guess must not reach the verifier", failures, ran)
	}
	if n := rec.count("auth-throttled"); n < refused {
		t.Fatalf("%d auth-throttled records for %d refusals", n, refused)
	}
	// The record names the door and the cause, not the account.
	if rec.has("auth-throttled", "target") {
		t.Fatal("an auth-throttled record carries the username: the refusal must say nothing about who was asked for")
	}
	if !rec.has("auth-throttled", "sql", "rate-limit") {
		t.Fatal("the SQL refusal is not recorded as path sql, cause rate-limit")
	}

	// Two things the bound must not do. The account is not locked out:
	// the right password from the operator's address signs in while the
	// guesser is refused — the budget is per (source, account), never
	// per account. And the guesser's address is not locked out of other
	// accounts: root from the same address, inside the source burst,
	// still signs in.
	if c, err := connectSecure(ctx, secureURL(tc, certsDir, "target", "rightpw")); err != nil {
		t.Fatalf("the account's own sign-in from another address was refused while it was being guessed at: %v", err)
	} else {
		_ = c.Close(ctx)
	}
	if c, err := connectSecureFrom(ctx, secureURL(tc, certsDir, "root", "topsecret"), guesser); err != nil {
		t.Fatalf("a different account from the guessing address was refused: %v", err)
	} else {
		_ = c.Close(ctx)
	}

	// One budget for every door: the account the guesser exhausted over
	// SQL is refused to it over /api/login as well — without its budget
	// having been spent there at all.
	attacker := httpsClientFrom(t, certsDir, "", guesser)
	req, err := http.NewRequest(http.MethodPost, "https://"+tc.Nodes[0].HTTPAddr()+"/api/login",
		strings.NewReader(`{"user":"target","password":"wrongpw"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := attacker.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("/api/login answered %d to the address that exhausted the account's budget over SQL; want 429: the doors are not sharing a budget", resp.StatusCode)
	}

	// The refusals reach the security document as the cause they are.
	operator := httpsClient(t, certsDir, "")
	code, body, _ := authedGet(t, operator, "https://"+tc.Nodes[0].HTTPAddr()+"/api/security", "root", "topsecret")
	if code != http.StatusOK {
		t.Fatalf("/api/security: %d", code)
	}
	var sec server.SecurityStatus
	if err := jsonUnmarshal([]byte(body), &sec); err != nil {
		t.Fatal(err)
	}
	if sec.ThrottledRateLimit < float64(refused) {
		t.Fatalf("/api/security reports %v rate-limit refusals, %d were observed over SQL", sec.ThrottledRateLimit, refused)
	}
}

// staged is a connection that has sent its StartupMessage and been
// advertised SASL: the budget was asked, nothing has been charged, and
// the exchange has not run. Holding many of these at once is what a
// driver cannot do, and it is exactly the state that separates asking
// before the exchange from charging on the attempt.
type staged struct {
	nc net.Conn
	fe *pgproto3.Frontend
}

// startupAll opens n TLS connections to the first node from localIP and
// sends a StartupMessage for user on each, before any exchange runs. It
// returns the connections the server advertised SASL to and how many it
// refused with a FATAL instead.
func startupAll(t *testing.T, tc *TestCluster, certsDir, localIP, user string, n int) (admitted []*staged, refused int) {
	t.Helper()
	caPEM, err := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	addr := tc.Nodes[0].SQLAddr()
	host, _, _ := net.SplitHostPort(addr)
	ip := net.ParseIP(localIP)
	if ip == nil {
		t.Fatalf("bad local address %q", localIP)
	}
	d := &net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}, Timeout: 10 * time.Second}
	for i := 0; i < n; i++ {
		raw, err := d.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = raw.Close() })
		fe := pgproto3.NewFrontend(raw, raw)
		fe.Send(&pgproto3.SSLRequest{})
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
		var reply [1]byte
		if _, err := io.ReadFull(raw, reply[:]); err != nil || reply[0] != 'S' {
			t.Fatalf("SSLRequest: %v %q", err, reply)
		}
		nc := tls.Client(raw, &tls.Config{RootCAs: pool, ServerName: host})
		if err := nc.Handshake(); err != nil {
			t.Fatal(err)
		}
		fe = pgproto3.NewFrontend(nc, nc)
		fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": user}})
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
		msg, err := fe.Receive()
		if err != nil {
			t.Fatalf("after StartupMessage %d: %v", i, err)
		}
		switch m := msg.(type) {
		case *pgproto3.AuthenticationSASL:
			admitted = append(admitted, &staged{nc: nc, fe: fe})
		case *pgproto3.ErrorResponse:
			if m.Code != "28000" {
				t.Fatalf("connection %d refused with %s %q, want 28000", i, m.Code, m.Message)
			}
			refused++
		default:
			t.Fatalf("connection %d: %T after StartupMessage", i, msg)
		}
	}
	return admitted, refused
}

// finishAll runs the SCRAM exchange to completion on every staged
// connection, reporting how many authenticated and how many failed.
func finishAll(t *testing.T, conns []*staged, user, password string) (ok, failed int) {
	t.Helper()
	for i, c := range conns {
		client := &scramclient.Client{User: user, Password: password, Nonce: fmt.Sprintf("nonce%020d", i)}
		c.fe.Send(&pgproto3.SASLInitialResponse{AuthMechanism: security.MechScram, Data: []byte(client.First())})
		if err := c.fe.Flush(); err != nil {
			t.Fatal(err)
		}
		msg, err := c.fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		cont, isCont := msg.(*pgproto3.AuthenticationSASLContinue)
		if !isCont {
			t.Fatalf("connection %d: %T after client-first", i, msg)
		}
		final, err := client.Final(string(cont.Data))
		if err != nil {
			t.Fatal(err)
		}
		c.fe.Send(&pgproto3.SASLResponse{Data: []byte(final)})
		if err := c.fe.Flush(); err != nil {
			t.Fatal(err)
		}
	outcome:
		for {
			msg, err := c.fe.Receive()
			if err != nil {
				t.Fatalf("connection %d: %v awaiting the outcome", i, err)
			}
			switch msg.(type) {
			case *pgproto3.AuthenticationOk:
				ok++
				break outcome
			case *pgproto3.ErrorResponse:
				failed++
				break outcome
			}
		}
		_ = c.nc.Close()
	}
	return ok, failed
}

// TestConcurrentCorrectSignInsAreNotThrottled is the property the SQL
// door's order of charging exists for: a pool opening more connections
// at once than the source burst, every one with the right password, is
// not refused. The handshakes are held open together — all have sent
// their StartupMessage before any exchange has run — because that is
// the moment that separates the two designs: charging on the attempt
// refuses the eleventh here, since no success has happened yet to
// refund; asking before the exchange admits them all, and none of them
// will fail.
func TestConcurrentCorrectSignInsAreNotThrottled(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := waitForRoot(t, ctx, tc, certsDir)
	_ = root.Close(ctx)

	// A fresh address, so the budget is exactly one burst — the retries
	// above spent and refunded 127.0.0.1's.
	const pool = 24 // more than the source burst (10) and the account burst (5)
	admitted, refused := startupAll(t, tc, certsDir, "127.0.0.3", "root", pool)
	if refused != 0 {
		t.Fatalf("%d of %d connections with the right password were refused before any had failed", refused, pool)
	}
	if ok, failed := finishAll(t, admitted, "root", "topsecret"); ok != pool || failed != 0 {
		t.Fatalf("%d authenticated and %d failed of %d with the right password", ok, failed, pool)
	}
}

// TestAGuessingWaveIsChargedInFull is the other side of that order,
// written from the attack: with asking free, every guess that asks
// before the first failure lands will run — so a guesser fires a wave.
// What the wave must not be is cheaper than the same guesses one at a
// time. Every one is charged when it lands, into debt, and the source
// waits a second for each; a second wave, sent when a floor at the
// burst would already have refilled, is refused entirely.
func TestAGuessingWaveIsChargedInFull(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := waitForRoot(t, ctx, tc, certsDir)
	defer root.Close(ctx)
	if _, err := root.Exec(ctx, `CREATE USER target WITH PASSWORD 'rightpw'`); err != nil {
		t.Fatal(err)
	}

	// Wide enough that its debt (wave - 5 seconds) outlasts the twelve
	// seconds below by a margin even when landing the wave itself takes
	// seconds (under the race detector it does); no wider, since the
	// test waits the debt out.
	const wave = 25
	const guesser = "127.0.0.4"
	first, refused := startupAll(t, tc, certsDir, guesser, "target", wave)
	if refused != 0 {
		t.Fatalf("%d of the first wave refused before any guess had landed", refused)
	}
	began := time.Now()
	if ok, failed := finishAll(t, first, "target", "wrongpw"); ok != 0 || failed != wave {
		t.Fatalf("first wave: %d authenticated, %d failed, want 0 and %d", ok, failed, wave)
	}
	landed := time.Now() // the last guess of the wave

	// The wave put the source (wave - 5) seconds into debt, less the
	// refill while it was landing. Twelve seconds on — past the eleven
	// a floor at the burst would need to refill — nothing may run.
	time.Sleep(12*time.Second - time.Since(began))
	if _, ran := startupAll(t, tc, certsDir, guesser, "target", wave); ran != wave {
		t.Fatalf("%d of a second wave ran 12s after a wave of %d: the wave was charged as a burst, not in full", wave-ran, wave)
	}
	// Once the debt is paid — a second for every guess beyond the burst,
	// counted from the last one landing — one may.
	deadline := landed.Add(time.Duration(wave-5+1) * time.Second)
	time.Sleep(time.Until(deadline))
	if admitted, _ := startupAll(t, tc, certsDir, guesser, "target", 1); len(admitted) != 1 {
		t.Fatalf("no budget %s after the last of a wave of %d landed: the source is held for longer than the guesses cost", time.Since(landed), wave)
	}
}

// TestAuthTimeoutIsPlumbedToBothDoors: the configured deadline reaches
// the SQL listener and the HTTP server. A connection to either that
// sends nothing is closed at it, and ordinary clients — whose handshakes
// take milliseconds — are unaffected by one set well below the default.
func TestAuthTimeoutIsPlumbedToBothDoors(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		cfg.AuthTimeout = 500 * time.Millisecond
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := waitForRoot(t, ctx, tc, certsDir)
	defer root.Close(ctx)

	closedWithin := func(what string, nc net.Conn, limit time.Duration) {
		t.Helper()
		defer nc.Close()
		start := time.Now()
		_ = nc.SetReadDeadline(time.Now().Add(limit))
		buf := make([]byte, 64)
		for {
			_, err := nc.Read(buf)
			if err == nil {
				continue
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("%s: a silent connection was held past %s (deadline 500ms)", what, limit)
			}
			if since := time.Since(start); since > 3*time.Second {
				t.Fatalf("%s: closed only after %s", what, since)
			}
			return
		}
	}

	// SQL: raw TCP, nothing sent — not even the SSLRequest.
	sqlConn, err := net.Dial("tcp", tc.Nodes[0].SQLAddr())
	if err != nil {
		t.Fatal(err)
	}
	closedWithin("sql", sqlConn, 5*time.Second)

	// HTTP: a TLS session established and then no request line.
	caPEM, err := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	host, _, _ := net.SplitHostPort(tc.Nodes[0].HTTPAddr())
	httpConn, err := tls.Dial("tcp", tc.Nodes[0].HTTPAddr(), &tls.Config{RootCAs: pool, ServerName: host})
	if err != nil {
		t.Fatal(err)
	}
	closedWithin("http", httpConn, 5*time.Second)

	// And a client that does its handshake is untouched by a short
	// deadline: the connection above, and one more now.
	if _, err := root.Exec(ctx, `SELECT 1`); err != nil {
		t.Fatalf("an authenticated connection was affected by the deadline: %v", err)
	}
	if c, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret")); err != nil {
		t.Fatalf("a normal sign-in under a 500ms deadline: %v", err)
	} else {
		_ = c.Close(ctx)
	}
}
