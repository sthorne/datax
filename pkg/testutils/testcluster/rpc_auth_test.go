package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/rpc/rpcpb"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// TestInternodeRPCRequiresNodeCert: on a secure cluster a client
// certificate issued to a SQL user — even root — authenticates only to
// the admin RPC. The internode surfaces on the same port (Batch, Join,
// RaftMessages, Snapshot) require the node certificate; otherwise any
// user with a certificate could scan or write every key span, bypassing
// SQL privileges, or inject Raft traffic. The "node" identity cannot be
// minted as a client certificate or created as a SQL user.
func TestInternodeRPCRequiresNodeCert(t *testing.T) {
	rec := &auditRecorder{}
	log.SetAuditSink(rec.record)
	defer log.SetAuditSink(nil)

	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := security.CreateClientCert(certsDir, "root"); err != nil {
		t.Fatal(err)
	}
	tlsCfg, err := security.LoadClientTLS(certsDir, "root")
	if err != nil {
		t.Fatal(err)
	}
	cc, err := grpc.NewClient(tc.Nodes[0].Addr(), grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	client := rpcpb.NewInternodeClient(cc)

	wantDenied := func(method string, err error) {
		t.Helper()
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("%s as root's client cert: got %v, want PermissionDenied", method, err)
		}
		if !strings.Contains(err.Error(), "node certificate") {
			t.Fatalf("%s denial does not name the node certificate: %v", method, err)
		}
	}

	// The admin RPC still serves the same certificate (root is an admin).
	var resp cluster.AdminResponse
	trans := rpc.NewTransport(hlc.NewClock(nil, 500*time.Millisecond), nil, nil)
	trans.SetTLS(tlsCfg)
	if err := trans.Call(ctx, tc.Nodes[0].Addr(), "admin", cluster.AdminRequest{Op: "nodes"}, &resp); err != nil || resp.Error != "" {
		t.Fatalf("admin nodes as root: err=%v resp.Error=%q", err, resp.Error)
	}

	// Batch: an inconsistent scan of the meta span, which root can read
	// through SQL but must not read through the raw KV surface.
	start, end := keys.MetaSpan()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{
		Timestamp: hlc.Timestamp{WallTime: time.Now().UnixNano()}, ReadInconsistent: true}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}})
	data, err := kvpb.MarshalBatchRequest(ba)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Batch(ctx, &rpcpb.Payload{Proto: data})
	wantDenied("Batch", err)

	_, err = client.Join(ctx, &rpcpb.Payload{Json: []byte(`{}`)})
	wantDenied("Join", err)

	rs, err := client.RaftMessages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The server rejects before reading the stream; Send may or may not
	// observe the reset first, so the verdict is read from CloseAndRecv.
	_ = rs.Send(&rpcpb.RaftEnvelope{RangeId: 1, FromNode: 99, FromAddr: "attacker:1"})
	_, err = rs.CloseAndRecv()
	wantDenied("RaftMessages", err)

	ss, err := client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = ss.Send(&rpcpb.SnapshotChunk{HeaderJson: []byte(`{}`)})
	_, err = ss.CloseAndRecv()
	wantDenied("Snapshot", err)

	if !rec.has("rpc-denied", "Batch", "root") {
		t.Fatalf("no rpc-denied audit record for the Batch call; got %v", rec.events)
	}

	// The registry must not have learned the forged address.
	resp = cluster.AdminResponse{}
	if err := trans.Call(ctx, tc.Nodes[0].Addr(), "admin", cluster.AdminRequest{Op: "nodes"}, &resp); err != nil {
		t.Fatal(err)
	}
	for _, nd := range resp.Nodes {
		if nd.NodeID == 99 || strings.Contains(nd.Address, "attacker") {
			t.Fatalf("forged Raft envelope poisoned the node registry: %+v", nd)
		}
	}

	// "node" is reserved on both credential paths.
	if err := security.CreateClientCert(certsDir, "node"); err == nil {
		t.Fatal("client certificate for CN node was issued")
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			_, err = c.Exec(ctx, `CREATE USER node PASSWORD 'x'`)
			_ = c.Close(ctx)
			if err == nil || !strings.Contains(err.Error(), "reserved") {
				t.Fatalf("CREATE USER node: got %v, want reserved-name error", err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("secure SQL never came up: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
