package testcluster

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// auditRecorder captures audit events for assertions.
type auditRecorder struct {
	mu     sync.Mutex
	events []string
}

func (a *auditRecorder) record(event string, kv []any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	parts := []string{event}
	for _, v := range kv {
		if s, ok := v.(string); ok {
			parts = append(parts, s)
		}
	}
	a.events = append(a.events, strings.Join(parts, " "))
}

// count returns how many recorded events carry one of the given names.
func (a *auditRecorder) count(names ...string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, e := range a.events {
		for _, name := range names {
			if e == name || strings.HasPrefix(e, name+" ") {
				n++
				break
			}
		}
	}
	return n
}

func (a *auditRecorder) has(substrs ...string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
outer:
	for _, e := range a.events {
		for _, s := range substrs {
			if !strings.Contains(e, s) {
				continue outer
			}
		}
		return true
	}
	return false
}

// TestAdminRPCAuthorization: on a secure cluster the admin RPC surface
// authenticates callers by their client certificate's CommonName.
// Read-only ops serve any authenticated principal; state-changing ops
// require the admin role (root, an admin-role member, or the cluster's
// own "node" identity), are audited with the principal, and a denial
// increments datax_admin_denied_total. GRANT ADMIN takes effect live.
// Issue #46.
func TestAdminRPCAuthorization(t *testing.T) {
	rec := &auditRecorder{}
	log.SetAuditSink(rec.record)
	defer log.SetAuditSink(nil)

	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Root's credential is seeded asynchronously; wait for SQL to come up.
	deadline := time.Now().Add(30 * time.Second)
	var sqlErr error
	for {
		c, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			if _, err := c.Exec(ctx, `CREATE USER bob PASSWORD 'bob-pw'`); err != nil {
				t.Fatal(err)
			}
			_ = c.Close(ctx)
			break
		}
		sqlErr = err
		if time.Now().After(deadline) {
			t.Fatalf("secure SQL never came up: %v", sqlErr)
		}
		time.Sleep(200 * time.Millisecond)
	}

	for _, u := range []string{"root", "bob"} {
		if err := security.CreateClientCert(certsDir, u); err != nil {
			t.Fatal(err)
		}
	}
	call := func(user string, req cluster.AdminRequest) cluster.AdminResponse {
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
		if err := trans.Call(cctx, tc.Nodes[0].Addr(), "admin", req, &resp); err != nil {
			t.Fatalf("admin call as %q: %v", user, err)
		}
		return resp
	}

	// Read-only op: any authenticated principal.
	if resp := call("bob", cluster.AdminRequest{Op: "nodes"}); resp.Error != "" || len(resp.Nodes) == 0 {
		t.Fatalf("nodes as bob: error=%q nodes=%d", resp.Error, len(resp.Nodes))
	}

	// State-changing op as a non-admin: denied, audited, counted.
	deniedBefore := testutil.ToFloat64(metrics.AdminDenied)
	resp := call("bob", cluster.AdminRequest{Op: "split", Key: keys.TableDataPrefix(970)})
	if !strings.Contains(resp.Error, "requires the admin role") {
		t.Fatalf("split as bob: %q, want admin-role denial", resp.Error)
	}
	if d := testutil.ToFloat64(metrics.AdminDenied) - deniedBefore; d < 1 {
		t.Fatalf("datax_admin_denied_total advanced by %v, want >= 1", d)
	}
	if !rec.has("admin-denied", "split", "bob") {
		t.Fatalf("no admin-denied audit record for bob; got %v", rec.events)
	}

	// Same op as root: allowed and audited with the principal.
	if resp := call("root", cluster.AdminRequest{Op: "split", Key: keys.TableDataPrefix(970)}); resp.Error != "" {
		t.Fatalf("split as root: %v", resp.Error)
	}
	if !rec.has("admin-op", "split", "root") {
		t.Fatalf("no admin-op audit record for root's split; got %v", rec.events)
	}

	// GRANT ADMIN takes effect live: bob can now run state-changing ops.
	c, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, `GRANT ADMIN TO bob`); err != nil {
		t.Fatal(err)
	}
	_ = c.Close(ctx)
	if !rec.has("privilege-ddl", "GRANT", "ADMIN", "bob") {
		t.Fatalf("no privilege-ddl audit record for GRANT ADMIN; got %v", rec.events)
	}
	if resp := call("bob", cluster.AdminRequest{Op: "split", Key: keys.TableDataPrefix(971)}); resp.Error != "" {
		t.Fatalf("split as bob after GRANT ADMIN: %v", resp.Error)
	}

	// A cleartext caller cannot reach the admin surface at all (mutual TLS
	// is mandatory on the RPC port in secure mode). The rejection must be
	// the transport's: gRPC surfaces a failed handshake as Unavailable
	// before any RPC is dispatched — never a handler verdict such as
	// PermissionDenied, and never the client's own deadline — and the
	// admin handler must not have seen the call, so no admin-op or
	// admin-denied audit record may appear (issue #71).
	auditedBefore := rec.count("admin-op", "admin-denied")
	bare := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	bctx, bcancel := context.WithTimeout(ctx, 10*time.Second)
	defer bcancel()
	var bresp cluster.AdminResponse
	err = bare.Call(bctx, tc.Nodes[0].Addr(), "admin", cluster.AdminRequest{Op: "nodes"}, &bresp)
	if err == nil {
		t.Fatal("cleartext admin call against a secure cluster succeeded")
	}
	if c := status.Code(err); c != codes.Unavailable {
		t.Fatalf("cleartext admin call failed with %v (%v), want Unavailable from the failed TLS handshake", c, err)
	}
	if n := rec.count("admin-op", "admin-denied") - auditedBefore; n != 0 {
		t.Fatalf("cleartext call reached the admin handler: %d new audit records; all: %v", n, rec.events)
	}
}
