package testcluster

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
)

// startSecureCluster starts a 3-node cluster with mutual internode TLS and
// SQL TLS + SCRAM, seeding root's password on node 1. Optional hooks tweak
// each node's config before start (i is the zero-based node index).
func startSecureCluster(t *testing.T, rootPassword string, hooks ...func(i int, cfg *server.Config)) (*TestCluster, string) {
	t.Helper()
	certsDir := t.TempDir()
	if err := security.CreateCA(certsDir); err != nil {
		t.Fatal(err)
	}
	if err := security.CreateNodeCert(certsDir, []string{"localhost", "127.0.0.1"}); err != nil {
		t.Fatal(err)
	}

	clusterID := uuid.New()
	listeners := make([]net.Listener, 3)
	pgListeners := make([]net.Listener, 3)
	nodeIDs := make([]base.NodeID, 3)
	var nodeDescs []kvpb.NodeDescriptor
	for i := 0; i < 3; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
		pglis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		pgListeners[i] = pglis
		nodeIDs[i] = base.NodeID(i + 1)
		nodeDescs = append(nodeDescs, kvpb.NodeDescriptor{
			NodeID: nodeIDs[i], Address: lis.Addr().String(), LivenessTime: time.Now().UnixNano(),
		})
	}
	range1 := cluster.Range1Descriptor(nodeIDs)
	tc := &TestCluster{T: t}
	for i := 0; i < 3; i++ {
		cfg := server.Config{
			Listener:   listeners[i],
			PGListener: pgListeners[i],
			CertsDir:   certsDir,
			StaticBootstrap: &server.StaticBootstrap{
				ClusterID: clusterID, NodeID: nodeIDs[i], Range1: range1, Nodes: nodeDescs,
			},
		}
		if i == 0 {
			cfg.RootPassword = rootPassword
		}
		for _, hook := range hooks {
			hook(i, &cfg)
		}
		n, err := server.Start(cfg)
		if err != nil {
			t.Fatalf("starting secure node %d: %v", i+1, err)
		}
		tc.Nodes = append(tc.Nodes, n)
	}
	t.Cleanup(tc.StopAll)
	return tc, certsDir
}

func secureURL(tc *TestCluster, certsDir, user, password string) string {
	return fmt.Sprintf("postgres://%s:%s@%s/datax?sslmode=verify-ca&sslrootcert=%s",
		user, password, tc.Nodes[0].SQLAddr(), filepath.Join(certsDir, "ca.crt"))
}

func connectSecure(ctx context.Context, url string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return pgx.ConnectConfig(ctx, cfg)
}

// TestSecureClusterEndToEnd: a TLS + SCRAM cluster serves real pgx clients
// over sslmode=verify-ca, rejects wrong passwords and unknown users
// uniformly, and supports SQL user management.
func TestSecureClusterEndToEnd(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Root's credential is seeded asynchronously; retry until auth works.
	var conn *pgx.Conn
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		conn, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Real SQL over the TLS + SCRAM session.
	if _, err := conn.Exec(ctx, `CREATE TABLE secrets (id INT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO secrets VALUES (1, 'classified')`); err != nil {
		t.Fatal(err)
	}
	var v string
	if err := conn.QueryRow(ctx, `SELECT v FROM secrets WHERE id = 1`).Scan(&v); err != nil || v != "classified" {
		t.Fatalf("round trip: %q, %v", v, err)
	}

	// Wrong password and unknown user fail the same way.
	if _, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "wrong")); err == nil || !strings.Contains(err.Error(), "password authentication failed") {
		t.Fatalf("wrong password: %v", err)
	}
	if _, err := connectSecure(ctx, secureURL(tc, certsDir, "nobody", "x")); err == nil || !strings.Contains(err.Error(), "password authentication failed") {
		t.Fatalf("unknown user: %v", err)
	}

	// User management via SQL. Authenticating is not authorization: alice
	// needs a grant before she can read the table.
	if _, err := conn.Exec(ctx, `CREATE USER alice PASSWORD 'wonderland'`); err != nil {
		t.Fatal(err)
	}
	aliceConn, err := connectSecure(ctx, secureURL(tc, certsDir, "alice", "wonderland"))
	if err != nil {
		t.Fatalf("alice cannot log in: %v", err)
	}
	if err := aliceConn.QueryRow(ctx, `SELECT v FROM secrets WHERE id = 1`).Scan(&v); err == nil || !strings.Contains(err.Error(), "42501") {
		t.Fatalf("ungranted read succeeded or failed oddly: %q, %v", v, err)
	}
	if _, err := conn.Exec(ctx, `GRANT SELECT ON secrets TO alice`); err != nil {
		t.Fatal(err)
	}
	if err := aliceConn.QueryRow(ctx, `SELECT v FROM secrets WHERE id = 1`).Scan(&v); err != nil || v != "classified" {
		t.Fatalf("alice's granted query: %q, %v", v, err)
	}
	_ = aliceConn.Close(ctx)

	if _, err := conn.Exec(ctx, `ALTER USER alice PASSWORD 'rabbit'`); err != nil {
		t.Fatal(err)
	}
	if _, err := connectSecure(ctx, secureURL(tc, certsDir, "alice", "wonderland")); err == nil {
		t.Fatal("old password still accepted after ALTER USER")
	}
	if c, err := connectSecure(ctx, secureURL(tc, certsDir, "alice", "rabbit")); err != nil {
		t.Fatalf("new password rejected: %v", err)
	} else {
		_ = c.Close(ctx)
	}

	if _, err := conn.Exec(ctx, `DROP USER alice`); err != nil {
		t.Fatal(err)
	}
	if _, err := connectSecure(ctx, secureURL(tc, certsDir, "alice", "rabbit")); err == nil {
		t.Fatal("dropped user still accepted")
	}
	if _, err := conn.Exec(ctx, `DROP USER root`); err == nil {
		t.Fatal("dropping root was allowed")
	}

	// A cleartext client is refused at the SQL listener in secure mode:
	// datax answers the SSLRequest with 'S', so a client that insists on
	// cleartext cannot proceed.
	if _, err := connectSecure(ctx, fmt.Sprintf("postgres://root:topsecret@%s/datax?sslmode=disable", tc.Nodes[0].SQLAddr())); err == nil {
		t.Fatal("cleartext SQL connection succeeded in secure mode")
	}
}
