package testcluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/security"
)

// TestClientCertAuth: a CA-signed client certificate whose CommonName is
// the SQL user authenticates without a password; a mismatched CN falls
// back to SCRAM (and fails without one).
func TestClientCertAuth(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Wait for root's seeded credential, then create alice.
	var root *pgx.Conn
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		root, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() { _ = root.Close(ctx) }()
	if _, err := root.Exec(ctx, `CREATE USER alice PASSWORD 'irrelevant'`); err != nil {
		t.Fatal(err)
	}

	if err := security.CreateClientCert(certsDir, "alice"); err != nil {
		t.Fatal(err)
	}
	certURL := func(user, certUser string) string {
		return fmt.Sprintf("postgres://%s@%s/datax?sslmode=verify-ca&sslrootcert=%s&sslcert=%s&sslkey=%s",
			user, tc.Nodes[0].SQLAddr(), filepath.Join(certsDir, "ca.crt"),
			filepath.Join(certsDir, "client."+certUser+".crt"),
			filepath.Join(certsDir, "client."+certUser+".key"))
	}

	// No password anywhere: the certificate is the credential.
	conn, err := connectSecure(ctx, certURL("alice", "alice"))
	if err != nil {
		t.Fatalf("cert auth failed: %v", err)
	}
	var one int
	if err := conn.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("cert-authed query: %v", err)
	}
	_ = conn.Close(ctx)

	// A certificate for a different CN does not authenticate this user:
	// the server falls back to SCRAM, and there is no password.
	if err := security.CreateClientCert(certsDir, "mallory"); err != nil {
		t.Fatal(err)
	}
	if _, err := connectSecure(ctx, certURL("alice", "mallory")); err == nil {
		t.Fatal("mismatched-CN certificate authenticated")
	} else if !strings.Contains(err.Error(), "password authentication failed") &&
		!strings.Contains(err.Error(), "SASL") {
		t.Fatalf("unexpected mismatch error: %v", err)
	}
}

// TestSASLprepPasswords: a password stored with a non-ASCII space
// authenticates from a client sending the SASLprep-normalized form — the
// stored verifier is derived from the normalized password.
func TestSASLprepPasswords(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var root *pgx.Conn
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		root, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() { _ = root.Close(ctx) }()

	// U+00A0 (no-break space) normalizes to a plain space under SASLprep.
	if _, err := root.Exec(ctx, "CREATE USER carol PASSWORD 'pa\u00a0word'"); err != nil {
		t.Fatal(err)
	}
	login := func(password string) error {
		cfg, err := pgx.ParseConfig(secureURL(tc, certsDir, "carol", "x"))
		if err != nil {
			return err
		}
		cfg.Password = password
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			return err
		}
		_ = conn.Close(ctx)
		return nil
	}
	if err := login("pa word"); err != nil {
		t.Fatalf("normalized-password login failed: %v", err)
	}
	// The raw (pre-normalization) form works too: pgx normalizes it
	// client-side before proving.
	if err := login("pa\u00a0word"); err != nil {
		t.Fatalf("raw-password login failed: %v", err)
	}
	// A wrong password still fails.
	if err := login("pa-word"); err == nil {
		t.Fatal("wrong password accepted")
	}
}

// TestBinaryParameters: Bind with binary-format parameters decodes every
// supported family (driven through pgconn's ExecParams, which lets the
// test force the format codes the way JDBC-style clients do).
func TestBinaryParameters(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE b (id INT8 PRIMARY KEY, f FLOAT8, ok BOOL, s TEXT, at TIMESTAMPTZ)`); err != nil {
		t.Fatal(err)
	}

	be64 := func(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
	at := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	pgMicros := at.UnixMicro() - 946684800*1e6

	pc := conn.PgConn()
	res := pc.ExecParams(ctx,
		`INSERT INTO b VALUES ($1, $2, $3, $4, $5)`,
		[][]byte{
			be64(7),
			be64(math.Float64bits(2.5)),
			{1},
			[]byte("hello"),
			be64(uint64(pgMicros)),
		},
		nil,                    // param OIDs: inferred by the server
		[]int16{1, 1, 1, 1, 1}, // every parameter in binary format
		[]int16{1},             // binary results
	).Read()
	if res.Err != nil {
		t.Fatalf("binary-param insert: %v", res.Err)
	}

	var id int64
	var f float64
	var ok bool
	var s string
	var got time.Time
	if err := conn.QueryRow(ctx, `SELECT id, f, ok, s, at FROM b WHERE id = $1`, int64(7)).
		Scan(&id, &f, &ok, &s, &got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if id != 7 || f != 2.5 || !ok || s != "hello" || !got.Equal(at) {
		t.Fatalf("row: %d %v %v %q %v", id, f, ok, s, got)
	}
}
