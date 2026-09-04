package testcluster

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// The rolling-upgrade suite (issue #49): a cluster of "old" binaries is
// upgraded node by node under transactional load, finalized only once every
// node runs the new binary, and every gate (finalize stragglers, join
// rejection, restart downgrade refusal, reannounce advisory) fires.

// waitForAdvertisedVersion polls the registry via the given node until
// every listed node advertises at least want.
func waitForAdvertisedVersion(t *testing.T, ctx context.Context, addr string, ids []base.NodeID, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp := adminCall(t, ctx, addr, cluster.AdminRequest{Op: "nodes"})
		got := map[base.NodeID]int{}
		for _, nd := range resp.Nodes {
			got[nd.NodeID] = nd.BinaryVersion
		}
		ok := true
		for _, id := range ids {
			bv := got[id]
			if bv == 0 {
				bv = 1
			}
			if bv < want {
				ok = false
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nodes %v never advertised version %d: %v", ids, want, got)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitForStoreVersionMirror polls a store's local cluster-version mirror.
func waitForStoreVersionMirror(t *testing.T, eng *storage.Engine, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if raw, err := eng.Get(keys.StoreClusterVersionKey()); err == nil && string(raw) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("store cluster-version mirror never reached %q", want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestRollingUpgradeUnderLoad(t *testing.T) {
	ctx := context.Background()
	asV2 := func(c *server.Config) { c.BinaryVersionOverride = version.V2 }
	asV3 := func(c *server.Config) { c.BinaryVersionOverride = version.V3 }
	tc, engines := StartWithEngines(t, 3, asV2)
	tc.LeaderIndex(1)
	chaosSeedAccounts(t, ctx, tc.Nodes[0].DB())

	var stop atomic.Bool
	var wg sync.WaitGroup
	chaosWorkers(tc, &stop, &wg, []int{0, 1, 2})
	defer func() { stop.Store(true); wg.Wait() }()

	// A v2 cluster cannot finalize v3: the serving binary is too old.
	resp := adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: 3})
	if !strings.Contains(resp.Error, "supports at most v2") {
		t.Fatalf("finalize on v2 binary: %q", resp.Error)
	}

	// Rolling restart onto the "new binary", one node at a time, under load.
	for i := 0; i < 3; i++ {
		tc.StopNode(i)
		tc.RestartNode(i, engines[i], asV3)
		tc.LeaderIndex(1) // wait for the cluster to be serviceable again
		if i < 2 {
			// Mid-upgrade: finalize must fail naming the v2 stragglers.
			waitForAdvertisedVersion(t, ctx, tc.Nodes[i].Addr(), []base.NodeID{base.NodeID(i + 1)}, 3)
			resp := adminCall(t, ctx, tc.Nodes[i].Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: 3})
			if !strings.Contains(resp.Error, "older binaries") {
				t.Fatalf("mid-upgrade finalize after node %d: %q", i+1, resp.Error)
			}
		}
	}

	// All nodes upgraded (and advertising it): finalize succeeds.
	ids := []base.NodeID{1, 2, 3}
	waitForAdvertisedVersion(t, ctx, tc.Nodes[0].Addr(), ids, 3)
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp = adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: 3})
		if resp.Error == "" {
			break
		}
		// A just-restarted node's row can lag; retry within the deadline.
		if time.Now().After(deadline) {
			t.Fatalf("finalize failed: %q", resp.Error)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if resp.ClusterVersion != 3 {
		t.Fatalf("finalized version %d, want 3", resp.ClusterVersion)
	}
	if resp := adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "nodes"}); resp.ClusterVersion != 3 {
		t.Fatalf("nodes op reports cluster version %d, want 3", resp.ClusterVersion)
	}
	// Idempotent.
	if resp := adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: 3}); resp.Error != "" || resp.ClusterVersion != 3 {
		t.Fatalf("re-finalize: %+v", resp)
	}

	// The load survived the whole dance with the invariant intact.
	stop.Store(true)
	wg.Wait()
	chaosVerify(t, ctx, tc)

	// A v2 binary can no longer join the finalized cluster.
	if _, err := tc.AddNodeErr(asV2); err == nil || !strings.Contains(err.Error(), "join rejected") {
		t.Fatalf("v2 join into finalized v3 cluster: %v", err)
	}
}

func TestBinaryTooOldRestart(t *testing.T) {
	// Default binaries bootstrap the cluster already finalized at Current.
	tc, engines := StartWithEngines(t, 3)
	tc.LeaderIndex(1)
	// The first heartbeat mirrors the replicated version into each store.
	waitForStoreVersionMirror(t, engines[2], strconv.Itoa(int(version.Current)))
	tc.StopNode(2)
	_, err := tc.RestartNodeErr(2, engines[2], func(c *server.Config) {
		c.BinaryVersionOverride = version.V2
	})
	if err == nil || !strings.Contains(err.Error(), "downgrading a node") {
		t.Fatalf("v2 restart into finalized cluster: %v", err)
	}
	// The same engine restarts fine on the current binary.
	tc.RestartNode(2, engines[2])
}

func TestReannounceVersionWarning(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	tc.LeaderIndex(1)
	ident, ok, err := cluster.ReadStoreIdent(engines[1])
	if err != nil || !ok {
		t.Fatalf("reading store ident: %v %v", err, ok)
	}
	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	defer trans.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A re-announce advertising a window entirely above this binary's is
	// rejected with the advisory error (KV-free path).
	var resp cluster.JoinResponse
	req := cluster.JoinRequest{
		Address: "203.0.113.9:1", NodeID: 2, ClusterID: ident.ClusterID,
		BinaryVersion: int(version.Current) + 5, MinSupported: int(version.Current) + 5,
	}
	if err := trans.Call(ctx, tc.Nodes[0].Addr(), "join", req, &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Error, "disjoint") {
		t.Fatalf("disjoint reannounce: %q", resp.Error)
	}
	// An overlapping window passes the advisory check.
	req.BinaryVersion, req.MinSupported = int(version.Current), int(version.MinSupported)
	req.Address = tc.Nodes[1].Addr()
	resp = cluster.JoinResponse{}
	if err := trans.Call(ctx, tc.Nodes[0].Addr(), "join", req, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("overlapping reannounce rejected: %q", resp.Error)
	}
}

// TestUnknownRequestUnionErrors covers the version-skew degradation rule: a
// request type this binary does not know (an all-nil union, which is what
// the JSON fallback encoding decodes an unknown request to) must yield an
// error, never a panic.
func TestUnknownRequestUnionErrors(t *testing.T) {
	tc, _ := StartWithEngines(t, 1)
	tc.LeaderIndex(1)
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: hlc.Timestamp{WallTime: 1}}}
	ba.Requests = append(ba.Requests, kvpb.RequestUnion{}) // unknown request type
	_, kerr := tc.Nodes[0].Store().ExecuteBatch(context.Background(), ba)
	if kerr == nil || !strings.Contains(kerr.Error(), "unsupported request") {
		t.Fatalf("all-nil union: %v", kerr)
	}
}

// TestUpgradeV6Databases: a v5 cluster (every node's binary overridden
// to v5) keeps creating tables in the flat namespace, which is what a v5
// node reads; database DDL is refused until finalize. A rolling restart
// onto the v6 binary changes nothing before finalize (the tables stay
// visible from every node). Finalizing v6 migrates the catalog: the flat
// namespace empties, every table sits under datax, qualified names work,
// and CREATE DATABASE starts working.
func TestUpgradeV6Databases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	asV5 := func(c *server.Config) { c.BinaryVersionOverride = version.V5 }
	tc, engines := StartWithEngines(t, 3, asV5)
	tc.LeaderIndex(1)

	sess := func(i int) *sql.Session { return sql.NewSession(tc.Nodes[i].DB(), catalog.NewAccessor()) }
	execSQL(t, ctx, sess(0), `CREATE TABLE old1 (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, sess(0), `INSERT INTO old1 VALUES (1, 'a')`)
	if _, serr := trySQL(ctx, sess(1), `CREATE DATABASE app`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("CREATE DATABASE before v6: %v", serr)
	}
	flatEntries := func() int {
		lo, hi := keys.NamespaceSpan()
		rows, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
		if err != nil {
			t.Fatal(err)
		}
		return len(rows)
	}
	if n := flatEntries(); n != 1 {
		t.Fatalf("flat namespace before v6 holds %d entries, want 1", n)
	}

	// Rolling restart onto the v6 binary; nothing moves before finalize,
	// and a table created mid-way still lands in the flat layout.
	for i := 0; i < 3; i++ {
		tc.StopNode(i)
		tc.RestartNode(i, engines[i])
		tc.LeaderIndex(1)
		if i == 1 {
			execSQL(t, ctx, sess(2), `CREATE TABLE old2 (id INT8 PRIMARY KEY)`)
		}
	}
	waitForAdvertisedVersion(t, ctx, tc.Nodes[0].Addr(), []base.NodeID{1, 2, 3}, int(version.V6))
	if n := flatEntries(); n != 2 {
		t.Fatalf("flat namespace before finalize holds %d entries, want 2", n)
	}
	for i := range tc.Nodes {
		if r := execSQL(t, ctx, sess(i), `SELECT v FROM old1`); len(r.Rows) != 1 {
			t.Fatalf("n%d before finalize: %+v", i+1, r.Rows)
		}
	}
	if _, serr := trySQL(ctx, sess(0), `CREATE DATABASE app`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("CREATE DATABASE before finalize: %v", serr)
	}

	// Finalize: the migration runs inside the upgrade op.
	resp := adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: int(version.V6)})
	if resp.Error != "" || resp.ClusterVersion != int(version.V6) {
		t.Fatalf("finalize v6: %+v", resp)
	}
	if n := flatEntries(); n != 0 {
		t.Fatalf("flat namespace after finalize holds %d entries", n)
	}
	for i := range tc.Nodes {
		s := sess(i)
		if r := execSQL(t, ctx, s, `SELECT v FROM datax.old1`); len(r.Rows) != 1 {
			t.Fatalf("n%d datax.old1 after finalize: %+v", i+1, r.Rows)
		}
		if r := execSQL(t, ctx, s, `SHOW TABLES`); len(r.Rows) != 2 {
			t.Fatalf("n%d SHOW TABLES after finalize: %+v", i+1, r.Rows)
		}
	}
	execSQL(t, ctx, sess(1), `CREATE DATABASE app`)
	execSQL(t, ctx, sess(1), `CREATE TABLE app.t (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, sess(2), `INSERT INTO app.t VALUES (7)`)
	if r := execSQL(t, ctx, sess(0), `SHOW DATABASES`); len(r.Rows) != 3 {
		t.Fatalf("SHOW DATABASES: %+v", r.Rows)
	}
	// A node restarted after finalize finds nothing left to migrate and
	// serves both layouts' tables by their new names.
	tc.StopNode(0)
	tc.RestartNode(0, engines[0])
	tc.LeaderIndex(1)
	if r := execSQL(t, ctx, sess(0), `SELECT id FROM app.t`); len(r.Rows) != 1 {
		t.Fatalf("app.t after restart: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, sess(0), `SELECT id FROM old2`); len(r.Rows) != 0 {
		t.Fatalf("old2 after restart: %+v", r.Rows)
	}
}
