package testcluster

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cli"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestCLISecureConnect: the plumbing behind `datax sql --certs-dir` and the
// admin clients' connect phase, against a real secure cluster — a client
// certificate authenticates a SQL session as its CommonName with no
// password, a TLS probe of the RPC port succeeds with the right CA and
// names the failure with the wrong one, and an unreachable address fails
// within the connect timeout with the address in the error.
func TestCLISecureConnect(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Root's credential is seeded asynchronously; wait for the cluster to
	// serve SQL before exercising the CLI paths.
	var root *pgx.Conn
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		root, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("secure cluster never served SQL: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() { _ = root.Close(ctx) }()
	if _, err := root.Exec(ctx, "CREATE USER ops PASSWORD 'unused'"); err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{"root", "ops"} {
		if err := security.CreateClientCert(certsDir, u); err != nil {
			t.Fatal(err)
		}
	}

	// `datax sql --certs-dir certs --user ops`: the URL still says
	// sslmode=disable and names root; the certs directory and --user win.
	url := "postgres://root@" + tc.Nodes[0].SQLAddr() + "/datax?sslmode=disable"
	cfg, err := cli.SQLConfig(url, certsDir, "ops")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	var conn *pgx.Conn
	err = cli.Connect(ctx, &cli.Progress{Out: &strings.Builder{}}, cli.SQLTarget(cfg), cli.SQLKind(cfg, certsDir), 10*time.Second,
		func(ctx context.Context) error {
			var err error
			conn, err = pgx.ConnectConfig(ctx, cfg)
			return err
		})
	if err != nil {
		t.Fatalf("certificate-authenticated connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	// ops holds no grants: a read of a root-only table proves the session
	// runs as ops (the certificate's CN), not as the URL's root.
	if _, err := root.Exec(ctx, "CREATE TABLE secret (id INT8 PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "SELECT * FROM secret"); err == nil {
		t.Fatal("ops should be denied on a table it holds no grant on")
	} else if !strings.Contains(err.Error(), "ops") {
		t.Fatalf("denial should name the session user ops: %v", err)
	}

	// The admin RPC probe: right CA and client certificate succeed.
	newTransport := func(dir, user string) *rpc.Transport {
		trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
		tlsCfg, err := security.LoadClientTLS(dir, user)
		if err != nil {
			t.Fatal(err)
		}
		trans.SetTLS(tlsCfg)
		return trans
	}
	addr := tc.Nodes[0].Addr()
	if err := newTransport(certsDir, "root").Probe(ctx, addr); err != nil {
		t.Fatalf("probe with the cluster CA: %v", err)
	}
	// A different CA: the handshake fails with a verification error, not a
	// timeout, and the message says so.
	otherDir := t.TempDir()
	if err := security.CreateCA(otherDir); err != nil {
		t.Fatal(err)
	}
	if err := security.CreateClientCert(otherDir, "root"); err != nil {
		t.Fatal(err)
	}
	err = cli.Connect(ctx, &cli.Progress{Out: &strings.Builder{}}, addr, "admin rpc", 10*time.Second, func(ctx context.Context) error {
		return newTransport(otherDir, "root").Probe(ctx, addr)
	})
	if err == nil || !strings.Contains(err.Error(), "could not connect to "+addr+" (admin rpc): ") || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("expected a verification failure naming the address, got %v", err)
	}

	// A listener that accepts and never answers: the connect timeout
	// fires with the address and elapsed time in the error.
	black, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = black.Close() }()
	var progress strings.Builder
	start := time.Now()
	err = cli.Connect(ctx, &cli.Progress{Out: &progress, After: 100 * time.Millisecond, Every: 100 * time.Millisecond},
		black.Addr().String(), "admin rpc", 500*time.Millisecond, func(ctx context.Context) error {
			return newTransport(certsDir, "root").Probe(ctx, black.Addr().String())
		})
	if err == nil || !strings.Contains(err.Error(), "could not connect to "+black.Addr().String()+" (admin rpc) after") {
		t.Fatalf("expected a timeout naming the address, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("connect timeout did not bound the wait: %s", elapsed)
	}
	if !strings.Contains(progress.String(), "still connecting to "+black.Addr().String()) {
		t.Fatalf("expected progress output, got %q", progress.String())
	}
}
