package testcluster

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crypto/tls"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// TestCancelAuthorization (issue #211): a query cancel is authorized on
// the node that acts on it, by exactly one of two authorities that are
// never inferred from each other's absence.
//
// The secret issued in BackendKeyData is a wire client's only identity,
// so it authorizes a plain cancel of the one connection it was issued
// to — and it has to match: process IDs are nodeID<<20|seq with the
// sequence starting at 1, a few thousand small integers per node, so a
// cancel that skipped the check on a zero secret could be swept for
// cluster-wide. Terminating a session, and cancelling without the
// secret, is authority the SQL layer reserves to the admin role, and is
// checked here rather than on whichever node the caller reached first.
// A cleartext CancelRequest to a TLS listener is refused, because the
// secret is the whole authorization and in the clear it is readable and
// replayable by anyone on the path.
func TestCancelAuthorization(t *testing.T) {
	rec := &auditRecorder{}
	log.SetAuditSink(rec.record)
	defer log.SetAuditSink(nil)

	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// A SQL URL for any node of the cluster, not just the first.
	urlFor := func(node int, user, password string) string {
		return fmt.Sprintf("postgres://%s:%s@%s/datax?sslmode=verify-ca&sslrootcert=%s",
			user, password, tc.Nodes[node].SQLAddr(), filepath.Join(certsDir, "ca.crt"))
	}

	// Root's credential is seeded asynchronously; bob is an ordinary SQL
	// user with no roles and no grants.
	root := waitForSecureSQL(t, ctx, urlFor(0, "root", "topsecret"))
	defer func() { _ = root.Close(ctx) }()
	if _, err := root.Exec(ctx, `CREATE USER bob PASSWORD 'bob-pw'`); err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{"root", "bob"} {
		if err := security.CreateClientCert(certsDir, u); err != nil {
			t.Fatal(err)
		}
	}

	// callNode makes an admin RPC to one node under a client
	// certificate. rpc.Server.Admin deliberately does not requireNode,
	// so any CA-signed client certificate reaches this surface — which
	// is the point: the op has to authorize itself.
	callNode := func(node int, user string, req cluster.AdminRequest) cluster.AdminResponse {
		t.Helper()
		tlsCfg, err := security.LoadClientTLS(certsDir, user)
		if err != nil {
			t.Fatal(err)
		}
		trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
		trans.SetTLS(tlsCfg)
		cctx, ccancel := context.WithTimeout(ctx, 20*time.Second)
		defer ccancel()
		var resp cluster.AdminResponse
		if err := trans.Call(cctx, tc.Nodes[node].Addr(), "admin", req, &resp); err != nil {
			t.Fatalf("admin call as %q: %v", user, err)
		}
		return resp
	}

	// The victim runs on node 2, and every call up to the last one is
	// made against node 2 directly — its internode port for the admin
	// RPC, its SQL port for the wire packets. That is vector B of the
	// issue as filed, and it is the harder case: there is no forwarding
	// hop to lean on, so the node that owns the connection has to
	// authorize the request itself. The cross-node hop appears in the
	// last case here, and for the wire path in TestCancelAndTimeouts.
	victim, err := connectSecure(ctx, urlFor(1, "bob", "bob-pw"))
	if err != nil {
		t.Fatalf("bob connecting to node 2: %v", err)
	}
	defer func() { _ = victim.Close(ctx) }()
	pid, secret := victim.PgConn().PID(), victim.PgConn().SecretKey()
	if int32(pid)>>20 != 2 {
		t.Fatalf("pid %d should carry node 2 in its high bits", pid)
	}
	// probe watches node 2's own sessions, which is where every victim
	// here lives; pg_stat_activity reports the node it is asked on.
	probe, err := connectSecure(ctx, urlFor(1, "root", "topsecret"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = probe.Close(ctx) }()

	done := make(chan error, 1)
	go func() {
		_, err := victim.Exec(ctx, `SELECT pg_sleep(60)`)
		done <- err
	}()
	waitForRunningStatement(t, ctx, probe, pid)

	// A refused cancel answers nothing, so every negative assertion here
	// is about the target: the statement is still running.
	stillRunning := func(why string) {
		t.Helper()
		select {
		case err := <-done:
			t.Fatalf("%s: pg_sleep stopped (%v)", why, err)
		case <-time.After(time.Second):
		}
	}

	// Terminating is admin authority. bob holds a CA-signed certificate
	// and nothing else, so the node that owns the connection refuses,
	// audits the refusal with the principal, and counts it.
	deniedBefore := testutil.ToFloat64(metrics.AdminDenied)
	resp := callNode(1, "bob", cluster.AdminRequest{Op: "cancel-query", PID: int32(pid), Terminate: true})
	if !strings.Contains(resp.Error, "requires the admin role") {
		t.Fatalf("terminate as bob: %q, want an admin-role denial", resp.Error)
	}
	if d := testutil.ToFloat64(metrics.AdminDenied) - deniedBefore; d < 1 {
		t.Fatalf("datax_admin_denied_total advanced by %v, want >= 1", d)
	}
	if !rec.has("admin-denied", "cancel-query", "bob") {
		t.Fatalf("no admin-denied audit record for bob's terminate; got %v", rec.events)
	}
	stillRunning("a terminate without the admin role")

	// Holding the secret does not buy a terminate. bob owns this
	// connection and so knows its secret legitimately; anyone who read
	// one off the wire holds it just as well. The secret authorizes
	// cancelling that one statement and nothing more — turning it into
	// a session kill is the escalation the admin role stands in front
	// of.
	resp = callNode(1, "bob", cluster.AdminRequest{
		Op: "cancel-query", PID: int32(pid), Secret: secretKeyOf(secret), Terminate: true,
	})
	if !strings.Contains(resp.Error, "requires the admin role") {
		t.Fatalf("terminate as bob with the right secret: %q, want an admin-role denial", resp.Error)
	}
	stillRunning("a terminate carrying the connection's own secret")

	// Cancelling without the secret is the same authority (it is what
	// pg_cancel_backend spends), so it is refused the same way.
	resp = callNode(1, "bob", cluster.AdminRequest{Op: "cancel-query", PID: int32(pid)})
	if !strings.Contains(resp.Error, "requires the admin role") {
		t.Fatalf("secretless cancel as bob: %q, want an admin-role denial", resp.Error)
	}
	stillRunning("a cancel with no secret")

	// With a secret the op needs no role — it is the wire client's own
	// authority — but the secret has to be the connection's.
	wrong := secretKeyOf(secret) ^ 0xff
	resp = callNode(1, "bob", cluster.AdminRequest{Op: "cancel-query", PID: int32(pid), Secret: wrong})
	if resp.Error != "" {
		t.Fatalf("cancel with a wrong secret: %q, want no error and no cancel", resp.Error)
	}
	stillRunning("a wrong secret")

	// A cleartext CancelRequest to the TLS listener is refused, even
	// carrying the right secret.
	sendCancel(t, dialSQL(t, tc.Nodes[1].SQLAddr(), nil), pid, secret)
	stillRunning("a cleartext cancel against a TLS listener")

	// The same packet inside TLS — which is what pgx v5.10 and libpq
	// from PG17 send — cancels, so the guard costs the real path
	// nothing.
	cancelTLS, err := security.LoadClientTLS(certsDir, "bob")
	if err != nil {
		t.Fatal(err)
	}
	cancelTLS.Certificates = nil // a cancel connection presents no certificate
	sendCancel(t, dialSQL(t, tc.Nodes[1].SQLAddr(), cancelTLS), pid, secret)
	select {
	case err := <-done:
		if pgErrCode(err) != sql.CodeQueryCanceled {
			t.Fatalf("cancel over TLS: %v, want 57014", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a CancelRequest inside TLS did not stop pg_sleep")
	}

	// A terminate that carries a secret must still terminate.
	// adminRoleRequired and the dispatch under it are complements — the
	// caller who does not need the admin role is exactly the caller
	// whose request goes to CancelBySecret — and if they drift apart, a
	// terminate carrying a secret is routed to the method that only ever
	// cancels. No escalation: the admin role is still required. But the
	// session an operator asked to end survives, and nothing tells them.
	ender, err := connectSecure(ctx, urlFor(1, "bob", "bob-pw"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ender.Close(ctx) }()
	epid, esecret := ender.PgConn().PID(), ender.PgConn().SecretKey()
	edone := make(chan error, 1)
	go func() {
		_, err := ender.Exec(ctx, `SELECT pg_sleep(60)`)
		edone <- err
	}()
	waitForRunningStatement(t, ctx, probe, epid)
	if resp := callNode(1, "root", cluster.AdminRequest{
		Op: "cancel-query", PID: int32(epid), Secret: secretKeyOf(esecret), Terminate: true,
	}); resp.Error != "" {
		t.Fatalf("root's terminate carrying a secret: %q", resp.Error)
	}
	select {
	case <-edone:
	case <-time.After(15 * time.Second):
		t.Fatal("a terminate carrying a secret did not stop pg_sleep")
	}
	deadline := time.Now().Add(10 * time.Second)
	for !ender.IsClosed() && time.Now().Before(deadline) {
		_, _ = ender.Exec(ctx, `SELECT 1`)
		time.Sleep(50 * time.Millisecond)
	}
	if !ender.IsClosed() {
		t.Fatal("a terminate carrying a secret only cancelled: the connection is still open")
	}

	// An admin's pg_terminate_backend still crosses nodes: root runs it
	// on node 1 against a connection on node 2, and node 2 authorizes it
	// by the forwarding node's certificate (CN "node"), which carries
	// admin authority.
	victim2, err := connectSecure(ctx, urlFor(1, "bob", "bob-pw"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = victim2.Close(ctx) }()
	pid2 := victim2.PgConn().PID()
	var ok bool
	if err := root.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, int32(pid2)).Scan(&ok); err != nil || !ok {
		t.Fatalf("root's cross-node pg_terminate_backend: %v %v", ok, err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for !victim2.IsClosed() && time.Now().Before(deadline) {
		_, _ = victim2.Exec(ctx, `SELECT 1`)
		time.Sleep(50 * time.Millisecond)
	}
	if !victim2.IsClosed() {
		t.Fatal("cross-node pg_terminate_backend did not end the connection")
	}
}

// waitForRunningStatement blocks until pid is reported running a
// statement. Sleeping a fixed interval instead would leave every
// negative assertion below proving only "nothing cancelled it", which
// is the same claim as "the refusal held" only if there was something
// to cancel; it is also the construct that made #215's test flake on a
// loaded machine. probe must be connected to the node that owns pid,
// since pg_stat_activity reports the sessions of the node it is asked
// on.
func waitForRunningStatement(t *testing.T, ctx context.Context, probe *pgx.Conn, pid uint32) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var n int
		if err := probe.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE pid = $1 AND state = 'active'`,
			int32(pid)).Scan(&n); err != nil {
			t.Fatalf("polling pg_stat_activity for pid %d: %v", pid, err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d never showed a running statement", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForSecureSQL connects once the node has finished seeding root.
func waitForSecureSQL(t *testing.T, ctx context.Context, url string) *pgx.Conn {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for {
		c, err := connectSecure(ctx, url)
		if err == nil {
			return c
		}
		last = err
		if time.Now().After(deadline) {
			t.Fatalf("secure SQL never came up: %v", last)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// secretKeyOf reads a BackendKeyData secret as the uint32 the admin RPC
// carries.
func secretKeyOf(key []byte) uint32 {
	var v uint32
	for _, b := range key[:4] {
		v = v<<8 | uint32(b)
	}
	return v
}

// dialSQL opens a connection to a SQL listener, negotiating TLS with an
// SSLRequest when cfg is set — the sequence pgx uses for the connection
// it sends a cancel on.
func dialSQL(t *testing.T, addr string, cfg *tls.Config) net.Conn {
	t.Helper()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		return raw
	}
	if cfg.ServerName == "" {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ServerName = host
	}
	fe := pgproto3.NewFrontend(raw, raw)
	fe.Send(&pgproto3.SSLRequest{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	var reply [1]byte
	if _, err := io.ReadFull(raw, reply[:]); err != nil {
		t.Fatalf("reading the SSLRequest reply: %v", err)
	}
	if reply[0] != 'S' {
		t.Fatalf("SSLRequest answered %q, want 'S'", reply[0])
	}
	tlsConn := tls.Client(raw, cfg)
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("TLS handshake on the cancel connection: %v", err)
	}
	return tlsConn
}

// sendCancel writes one CancelRequest and hangs up, which is all a
// cancelling client does.
func sendCancel(t *testing.T, nc net.Conn, pid uint32, key []byte) {
	t.Helper()
	fe := pgproto3.NewFrontend(nc, nc)
	fe.Send(&pgproto3.CancelRequest{ProcessID: pid, SecretKey: key})
	if err := fe.Flush(); err != nil {
		t.Fatalf("sending CancelRequest: %v", err)
	}
	_ = nc.Close()
}
