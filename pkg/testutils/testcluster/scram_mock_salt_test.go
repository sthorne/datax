package testcluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/security"
)

// scramServerFirst opens a TLS connection to node i, starts SCRAM as
// user and returns the server-first message (the salt the server shows
// before anything is proven), without completing the exchange.
func scramServerFirst(t *testing.T, tc *TestCluster, certsDir string, i int, user string) string {
	t.Helper()
	nc, err := net.DialTimeout("tcp", tc.Nodes[i].SQLAddr(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	_ = nc.SetDeadline(time.Now().Add(20 * time.Second))
	fe := pgproto3.NewFrontend(nc, nc)
	fe.Send(&pgproto3.SSLRequest{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	var answer [1]byte
	if _, err := nc.Read(answer[:]); err != nil || answer[0] != 'S' {
		t.Fatalf("SSLRequest answered %q, %v", answer[:], err)
	}
	pem, err := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)
	tc2 := tls.Client(nc, &tls.Config{RootCAs: roots, ServerName: "localhost"})
	if err := tc2.Handshake(); err != nil {
		t.Fatal(err)
	}
	fe = pgproto3.NewFrontend(tc2, tc2)
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{"user": user, "database": "datax"}})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	msg, err := fe.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := msg.(*pgproto3.AuthenticationSASL); !ok {
		t.Fatalf("after startup: %T %+v", msg, msg)
	}
	fe.Send(&pgproto3.SASLInitialResponse{AuthMechanism: security.MechScram, Data: []byte("n,,n=" + user + ",r=fyko+d2lbbFgONRv9qkxdawL")})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	msg, err = fe.Receive()
	if err != nil {
		t.Fatal(err)
	}
	cont, ok := msg.(*pgproto3.AuthenticationSASLContinue)
	if !ok {
		t.Fatalf("after client-first: %T %+v", msg, msg)
	}
	return string(cont.Data)
}

// saltOf extracts the s= attribute of a server-first message.
func saltOf(t *testing.T, serverFirst string) string {
	t.Helper()
	for _, attr := range strings.Split(serverFirst, ",") {
		if v, ok := strings.CutPrefix(attr, "s="); ok {
			return v
		}
	}
	t.Fatalf("no salt in %q", serverFirst)
	return ""
}

// TestSCRAMStandInSaltPerUser (issue #137): the salt the SCRAM
// server-first shows for a user that does not exist is derived per name
// under a cluster-wide secret — the same for one name on every node and
// across handshakes, different between names, and shaped like a real
// user's — so the handshake alone no longer tells which names are
// users. Authentication still fails uniformly.
func TestSCRAMStandInSaltPerUser(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	deadline := time.Now().Add(30 * time.Second)
	for {
		root, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			_ = root.Close(ctx)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	nobodyA := saltOf(t, scramServerFirst(t, tc, certsDir, 0, "nobody-a"))
	nobodyB := saltOf(t, scramServerFirst(t, tc, certsDir, 0, "nobody-b"))
	if nobodyA == nobodyB {
		t.Fatalf("two names that do not exist share a salt: %s", nobodyA)
	}
	for i := range tc.Nodes {
		for attempt := 0; attempt < 2; attempt++ {
			if got := saltOf(t, scramServerFirst(t, tc, certsDir, i, "nobody-a")); got != nobodyA {
				t.Fatalf("node %d, handshake %d: salt %s for nobody-a, node 1 showed %s", i+1, attempt, got, nobodyA)
			}
		}
	}
	// The shape a real user's salt has.
	rootSalt := saltOf(t, scramServerFirst(t, tc, certsDir, 0, "root"))
	if len(rootSalt) != len(nobodyA) || rootSalt == nobodyA {
		t.Fatalf("real salt %s, stand-in %s", rootSalt, nobodyA)
	}
	// And the exchange still fails the same way for both.
	for _, user := range []string{"nobody-a", "root"} {
		_, err := connectSecure(ctx, secureURL(tc, certsDir, user, "wrong"))
		if err == nil || !strings.Contains(err.Error(), "password authentication failed") {
			t.Fatalf("%s with a wrong password: %v", user, err)
		}
	}
}
