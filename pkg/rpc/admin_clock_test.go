package rpc

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sthorne/datax/pkg/rpc/rpcpb"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// adminClockChildEnv selects the in-child half of the Admin clock tests: a
// regression calls log.Fatalf, which exits the process, so the scenario
// runs in a child and the parent asserts that it survived.
const adminClockChildEnv = "DATAX_TEST_ADMIN_CLOCK"

// TestAdminFutureClockDoesNotKillNode: an Admin call carrying a timestamp
// far past --max-offset used to reach log.Fatalf before any authorization,
// so any client certificate (or anyone, on an insecure cluster) could stop
// a node with one RPC. A non-node caller's timestamp must now leave the
// clock alone, a node peer's must fail only that call, and the server must
// go on serving.
func TestAdminFutureClockDoesNotKillNode(t *testing.T) {
	for _, mode := range []string{"secure", "insecure"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestAdminClockChild$", "-test.v", "-test.count=1")
			cmd.Env = append(os.Environ(), adminClockChildEnv+"="+mode)
			out, err := cmd.CombinedOutput()
			if err != nil || !strings.Contains(string(out), "--- PASS: TestAdminClockChild") {
				t.Fatalf("server process did not survive (err=%v):\n%s", err, out)
			}
		})
	}
}

func TestAdminClockChild(t *testing.T) {
	mode := os.Getenv(adminClockChildEnv)
	if mode == "" {
		t.Skip("run by TestAdminFutureClockDoesNotKillNode")
	}
	var serverTLS, nodeTLS, aliceTLS *tls.Config
	if mode == "secure" {
		dir := t.TempDir()
		if err := security.CreateCA(dir); err != nil {
			t.Fatal(err)
		}
		if err := security.CreateNodeCert(dir, []string{"127.0.0.1", "localhost"}); err != nil {
			t.Fatal(err)
		}
		if err := security.CreateClientCert(dir, "alice"); err != nil {
			t.Fatal(err)
		}
		cfgs, err := security.LoadNodeTLS(dir)
		if err != nil {
			t.Fatal(err)
		}
		serverTLS, nodeTLS = cfgs.Server, cfgs.Client.Clone()
		nodeTLS.ServerName = "127.0.0.1"
		if aliceTLS, err = security.LoadClientTLS(dir, "alice"); err != nil {
			t.Fatal(err)
		}
		aliceTLS.ServerName = "127.0.0.1"
	}

	clock := hlc.NewClock(nil, 500*time.Millisecond)
	admin := func(ctx context.Context, in []byte) ([]byte, error) { return []byte(`{}`), nil }
	gs := NewServer(clock, ServerHandlers{Admin: admin}, serverTLS)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	call := func(cfg *tls.Config, wall int64) (*rpcpb.Payload, error) {
		t.Helper()
		cc, err := DialAddr(lis.Addr().String(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cc.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return rpcpb.NewInternodeClient(cc).Admin(ctx, &rpcpb.Payload{Json: []byte(`{}`), Now: &rpcpb.Hlc{WallTime: wall}})
	}
	future := time.Now().Add(time.Hour).UnixNano()

	if mode == "secure" {
		// An operator's client certificate: its timestamp is ignored, and
		// the call reaches the handler, which authorizes it per op.
		if _, err := call(aliceTLS, future); err != nil {
			t.Fatalf("client-cert Admin with a future clock: %v", err)
		}
		if got := clock.Now().WallTime; got >= future {
			t.Fatalf("a client certificate moved the node's clock to %d", got)
		}
		// Within the offset, a client certificate still does not move it.
		ahead := clock.PhysicalNow() + int64(200*time.Millisecond)
		if _, err := call(aliceTLS, ahead); err != nil {
			t.Fatal(err)
		}
		if got := clock.Now().WallTime; got >= ahead {
			t.Fatalf("a client certificate moved the node's clock to %d", got)
		}
	}

	// A node peer (everyone, in insecure mode) past the offset: that call
	// fails, and the clock is not moved.
	peer := nodeTLS
	if _, err := call(peer, future); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("node Admin with a future clock: got %v, want FailedPrecondition", err)
	}
	if got := clock.Now().WallTime; got >= future {
		t.Fatalf("a rejected timestamp moved the node's clock to %d", got)
	}

	// The server is still up, and a node peer within the offset still
	// ratchets the clock.
	ahead := clock.PhysicalNow() + int64(200*time.Millisecond)
	if _, err := call(peer, ahead); err != nil {
		t.Fatalf("normal call after the future-clock calls: %v", err)
	}
	if got := clock.Now().WallTime; got < ahead {
		t.Fatalf("a node peer's timestamp did not ratchet the clock: %d < %d", got, ahead)
	}
	if mode == "secure" {
		if _, err := call(aliceTLS, time.Now().UnixNano()); err != nil {
			t.Fatalf("client-cert call after the future-clock calls: %v", err)
		}
	}
}
