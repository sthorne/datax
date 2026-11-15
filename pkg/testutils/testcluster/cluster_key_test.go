package testcluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/jackc/pgx/v5"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// ckCluster is a secure 3-node cluster whose nodes keep their store keys
// under file roots, with admin transports as root and as a node.
type ckCluster struct {
	t        *testing.T
	ctx      context.Context
	tc       *TestCluster
	certsDir string
	dirs     []string
	roots    []string
	asRoot   *rpc.Transport
	asNode   *rpc.Transport
	cfgHook  func(i int, cfg *server.Config)
}

func writeRootKey(t *testing.T, dir, name string) string {
	t.Helper()
	k := make([]byte, enc.KeyLen)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, k, 0o600); err != nil {
		t.Fatal(err)
	}
	return "file:" + p
}

// startCKCluster starts the cluster. hook may change a node's config
// (it runs after the defaults, at the first start and at every restart).
func startCKCluster(t *testing.T, ctx context.Context, hook func(i int, cfg *server.Config)) *ckCluster {
	c := &ckCluster{t: t, ctx: ctx, cfgHook: hook}
	keyDir := t.TempDir()
	for i := 0; i < 3; i++ {
		c.dirs = append(c.dirs, t.TempDir())
		c.roots = append(c.roots, writeRootKey(t, keyDir, "root"+string(rune('a'+i))+".key"))
	}
	c.tc, c.certsDir = startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			if err := security.CreateClientCert(cfg.CertsDir, "root"); err != nil {
				t.Fatal(err)
			}
			rootTLS, err := security.LoadClientTLS(cfg.CertsDir, "root")
			if err != nil {
				t.Fatal(err)
			}
			nodeTLS, err := security.LoadNodeTLS(cfg.CertsDir)
			if err != nil {
				t.Fatal(err)
			}
			c.asRoot = rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
			c.asRoot.SetTLS(rootTLS)
			c.asNode = rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
			c.asNode.SetTLS(nodeTLS.Client)
		}
		c.config(i, cfg)
	})
	for _, n := range c.tc.Nodes {
		c.tc.addrs = append(c.tc.addrs, n.Addr())
	}
	t.Cleanup(func() { c.asRoot.Close(); c.asNode.Close() })
	return c
}

func (c *ckCluster) config(i int, cfg *server.Config) {
	cfg.Dir = c.dirs[i]
	cfg.EncRoot = c.roots[i]
	cfg.GCInterval = -1
	cfg.LivenessGrace = 4 * time.Second
	if c.cfgHook != nil {
		c.cfgHook(i, cfg)
	}
}

func (c *ckCluster) restart(i int) *server.Node {
	c.t.Helper()
	n, err := c.tc.RestartNodeErr(i, nil, func(cfg *server.Config) {
		cfg.CertsDir = c.certsDir
		c.config(i, cfg)
	})
	if err != nil {
		c.t.Fatalf("restarting n%d: %v", i+1, err)
	}
	return n
}

// call runs an admin op on node i, as root or as a node.
func (c *ckCluster) call(i int, asNode bool, req cluster.AdminRequest) cluster.AdminResponse {
	c.t.Helper()
	tr := c.asRoot
	if asNode {
		tr = c.asNode
	}
	cctx, cancel := context.WithTimeout(c.ctx, 60*time.Second)
	defer cancel()
	var resp cluster.AdminResponse
	if err := tr.Call(cctx, c.tc.addrs[i], "admin", req, &resp); err != nil {
		resp.Error = "transport: " + err.Error()
	}
	return resp
}

// op runs a cluster-key op as root on node i and returns its error text.
func (c *ckCluster) op(i int, name string) (string, *cluster.ClusterKeyAnswer) {
	c.t.Helper()
	resp := c.call(i, false, cluster.AdminRequest{Op: "cluster-key-" + name})
	return resp.Error, resp.ClusterKey
}

func (c *ckCluster) status(i int) *cluster.ClusterKeyStatus {
	c.t.Helper()
	errText, ans := c.op(i, "status")
	if errText != "" || ans == nil || ans.Status == nil {
		c.t.Fatalf("status: %s", errText)
	}
	return ans.Status
}

// waitFor polls cond until it holds.
func (c *ckCluster) waitFor(what string, d time.Duration, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitReported waits until every running node has written its holder
// record.
func (c *ckCluster) waitReported(n int) {
	c.t.Helper()
	c.waitFor("every node to report its root", 30*time.Second, func() bool {
		st := c.status(0)
		got := 0
		for _, nd := range st.Nodes {
			if nd.Reported && nd.Root != "" {
				got++
			}
		}
		return got >= n
	})
}

// holdsCurrent reports, from the cluster's status, whether node id holds
// the current key.
func holdsCurrent(st *cluster.ClusterKeyStatus, id base.NodeID) bool {
	for _, nd := range st.Nodes {
		if nd.NodeID == id {
			return nd.HoldsCurrent
		}
	}
	return false
}

// fileRefs lists the cluster keys in node i's CLUSTER-KEY file.
func (c *ckCluster) fileRefs(i int) []string {
	c.t.Helper()
	entries, err := enc.ReadClusterKeyFile(vfs.Default, c.dirs[i])
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		c.t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.ClusterKeyRef.String())
	}
	return out
}

func (c *ckCluster) keyStore(i int) *keystore.Store {
	c.t.Helper()
	ks, err := c.tc.Nodes[i].KeyStore()
	if err != nil {
		c.t.Fatal(err)
	}
	return ks
}

func wantErr(t *testing.T, what, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Fatalf("%s: %q, want an error containing %q", what, got, want)
	}
}

// TestClusterKeyRefusals (issue #310): enabling the cluster key is
// refused, naming the reason, before the cluster is at v22, while a node
// keeps its store key in a key file (with the pointer to enc-root
// adopt), and while a member is down; nothing is created by a refusal.
func TestClusterKeyRefusals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	t.Run("before-v22", func(t *testing.T) {
		c := startCKCluster(t, ctx, func(i int, cfg *server.Config) { cfg.BinaryVersionOverride = version.V21 })
		errText, _ := c.op(0, "init")
		wantErr(t, "init at v21", errText, "needs cluster version v22")
		if st := c.status(0); st.Enabled || st.Pending != "" {
			t.Fatalf("a refused init left %+v", st)
		}
		if _, err := c.keyStore(0).Create(ctx, keystore.KindWrapping, "k"); err == nil || !strings.Contains(err.Error(), "v22") {
			t.Fatalf("a named key at v21: %v", err)
		}
	})

	t.Run("key-file node then down node", func(t *testing.T) {
		keyFile := filepath.Join(t.TempDir(), "store.key")
		k := make([]byte, enc.KeyLen)
		_, _ = rand.Read(k)
		if err := os.WriteFile(keyFile, k, 0o600); err != nil {
			t.Fatal(err)
		}
		c := startCKCluster(t, ctx, func(i int, cfg *server.Config) {
			if i == 2 {
				cfg.EncRoot, cfg.EncKeyPath = "", keyFile
			}
		})
		c.waitFor("every node to report", 30*time.Second, func() bool {
			got := 0
			for _, nd := range c.status(0).Nodes {
				if nd.Reported {
					got++
				}
			}
			return got == 3
		})
		errText, _ := c.op(0, "init")
		wantErr(t, "init with a key-file node", errText, "n3 keeps its store key in a key file")
		wantErr(t, "init with a key-file node", errText, "datax debug enc-root adopt")
		if c.fileRefs(0) != nil || c.status(0).Pending != "" {
			t.Fatal("a refused init made a key")
		}
		c.tc.StopNode(2)
		time.Sleep(5 * time.Second) // past the liveness grace
		errText, _ = c.op(0, "init")
		wantErr(t, "init with a node down", errText, "n3 is not live")
		if c.status(0).Enabled {
			t.Fatal("a refused init enabled the key")
		}
	})
}

// TestClusterKeyLifecycle (issue #310): the cluster key is enabled on
// every node; named keys are made, read on another node, and are not
// readable through SQL even by root; a node whose copy is lost, and a
// restarted node, get it back from a peer; a rotation rewraps every
// named key and every node's copy and destroys the old version;
// destroying a key purges its row's history; and a node being
// decommissioned is refused a hand-over.
func TestClusterKeyLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	c := startCKCluster(t, ctx, nil)
	c.waitReported(3)

	errText, ans := c.op(0, "init")
	if errText != "" {
		t.Fatalf("init: %s", errText)
	}
	st := ans.Status
	if !st.Enabled || st.Version != 1 || st.ID == "" {
		t.Fatalf("after init: %+v", st)
	}
	v1 := enc.ClusterKeyRef{ID: st.ID, Version: 1}
	for i := 0; i < 3; i++ {
		if refs := c.fileRefs(i); len(refs) != 1 || refs[0] != v1.String() {
			t.Fatalf("n%d's file after init: %v", i+1, refs)
		}
	}
	c.waitFor("every node to report holding the key", 20*time.Second, func() bool {
		st := c.status(1)
		return holdsCurrent(st, 1) && holdsCurrent(st, 2) && holdsCurrent(st, 3)
	})
	// Enabling again changes nothing.
	if errText, ans := c.op(1, "init"); errText != "" || ans.Status.ID != v1.ID || ans.Status.Version != 1 {
		t.Fatalf("a second init: %s %+v", errText, ans)
	}

	// Named keys: made on one node, the same material on another.
	ks1, ks2 := c.keyStore(0), c.keyStore(1)
	billing, err := ks1.Create(ctx, keystore.KindWrapping, "billing")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ks2.Create(ctx, keystore.KindWrapping, "billing"); !errors.Is(err, keystore.ErrExists) {
		t.Fatalf("a second key of the same name: %v", err)
	}
	tenant, err := ks2.Create(ctx, keystore.KindTenant, "items/4f6c")
	if err != nil {
		t.Fatal(err)
	}
	m1, err := ks1.Unwrap(ctx, billing.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m1.Release()
	m2, err := ks2.Unwrap(ctx, billing.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m1.Key, m2.Key) {
		t.Fatal("two nodes unwrap the same key to different material")
	}
	firstMaterial := append([]byte(nil), m1.Key...)
	m2.Release()
	if got, err := ks2.Get(ctx, "billing"); err != nil || got.ID != billing.ID || got.ClusterKey != v1 {
		t.Fatalf("Get by name: %+v %v", got, err)
	}

	// SQL: the metadata, never the material, whoever asks.
	conn := connectRoot(t, ctx, c)
	defer func() { _ = conn.Close(ctx) }()
	var name, kind, state string
	var gen int64
	if err := conn.QueryRow(ctx, `SELECT name, kind, state, generation FROM datax_keys WHERE id = $1`, billing.ID).Scan(&name, &kind, &state, &gen); err != nil {
		t.Fatalf("reading the keys' metadata as root: %v", err)
	}
	if name != "billing" || kind != "wrapping" || state != "enabled" || gen != 1 {
		t.Fatalf("metadata: %s %s %s %d", name, kind, state, gen)
	}
	rows, err := conn.Query(ctx, `SELECT * FROM datax_keys`)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range rows.FieldDescriptions() {
		if f.Name == "material" {
			t.Fatal("SELECT * FROM datax_keys has a material column")
		}
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if n != 2 {
		t.Fatalf("datax_keys lists %d keys, want 2", n)
	}
	for _, q := range []string{
		`SELECT material FROM datax_keys`,
		`SELECT material FROM datax.datax_keys`,
		`SELECT material FROM pg_catalog.datax_keys`,
		`DELETE FROM datax_keys`,
		`UPDATE datax_keys SET state = 'enabled'`,
		`INSERT INTO datax_keys (id) VALUES (1)`,
		`DROP TABLE datax_keys`,
		`ALTER TABLE datax_keys ADD COLUMN x INT`,
		`GRANT SELECT ON datax_keys TO root`,
		`COPY datax_keys (material) TO STDOUT`,
	} {
		if _, err := conn.Exec(ctx, q); err == nil {
			t.Fatalf("%s: accepted", q)
		}
	}
	// A copy of the table is a copy of its metadata: there is no material
	// to copy.
	if _, err := conn.Exec(ctx, `CREATE TABLE copied AS SELECT * FROM datax_keys`); err != nil {
		t.Fatalf("copying the keys' metadata: %v", err)
	}
	var cols int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'copied' AND column_name = 'material'`).Scan(&cols); err != nil || cols != 0 {
		t.Fatalf("a copy of datax_keys has a material column: %d %v", cols, err)
	}

	// A node whose copy is gone gets it back from a peer, and so does a
	// node that restarts with its copy.
	c.tc.StopNode(2)
	if err := enc.RemoveClusterKeyFile(vfs.Default, c.dirs[2]); err != nil {
		t.Fatal(err)
	}
	c.restart(2)
	c.waitFor("n3 to be handed the key back", 30*time.Second, func() bool {
		refs := c.fileRefs(2)
		return len(refs) == 1 && refs[0] == v1.String() && holdsCurrent(c.status(0), 3)
	})
	// The hand-over is on the record of the node that answered.
	handedOver := false
	for _, n := range c.tc.Nodes[:2] {
		for _, ev := range n.Events().Recent(0, 1000, true) {
			handedOver = handedOver || (ev.Kind == "cluster-key" && strings.Contains(ev.Summary, "to n3"))
		}
	}
	if !handedOver {
		t.Fatal("no event records the hand-over to n3")
	}
	ks3 := c.keyStore(2)
	m3, err := ks3.Unwrap(ctx, billing.ID, 0)
	if err != nil || !bytes.Equal(m3.Key, firstMaterial) {
		t.Fatalf("n3 after the hand-over: %v", err)
	}
	m3.Release()
	c.tc.StopNode(1)
	c.restart(1)
	ks2 = c.keyStore(1)
	m2b, err := ks2.Unwrap(ctx, billing.ID, 0)
	if err != nil || !bytes.Equal(m2b.Key, firstMaterial) {
		t.Fatalf("n2 after a restart: %v", err)
	}
	m2b.Release()

	// A rotation of the named key keeps the old version opening.
	rot, err := ks1.Rotate(ctx, billing.ID)
	if err != nil || rot.Version != 2 || rot.Generation != 2 {
		t.Fatalf("rotate: %+v %v", rot, err)
	}

	// Rotating the cluster key: every node holds v2 alone, every named
	// key is under it, and the material is what it was.
	errText, ans = c.op(2, "rotate")
	if errText != "" {
		t.Fatalf("cluster key rotate: %s", errText)
	}
	v2 := enc.ClusterKeyRef{ID: v1.ID, Version: 2}
	if ans.Status.Version != 2 || len(ans.Status.Previous) != 0 {
		t.Fatalf("after rotate: %+v (%v)", ans.Status, ans.Notes)
	}
	c.waitFor("every node to drop v1", 20*time.Second, func() bool {
		for i := 0; i < 3; i++ {
			if refs := c.fileRefs(i); len(refs) != 1 || refs[0] != v2.String() {
				return false
			}
		}
		return true
	})
	for _, k := range []keystore.Key{billing, tenant} {
		got, err := ks2.GetByID(ctx, k.ID)
		if err != nil || got.ClusterKey != v2 {
			t.Fatalf("key %s after the cluster key's rotation: %+v %v", k.Name, got, err)
		}
	}
	old, err := c.keyStore(2).Unwrap(ctx, billing.ID, 1)
	if err != nil || !bytes.Equal(old.Key, firstMaterial) {
		t.Fatalf("version 1 of billing after the cluster key's rotation: %v", err)
	}
	old.Release()
	// Running it again with nothing left to do rotates again; that is
	// fine — but a rotation's partial state is what it must finish, which
	// the partial-init test covers for the shared machinery.

	// Disable and enable move the generation; Unwrap follows the state.
	if k, err := ks1.Disable(ctx, tenant.ID); err != nil || k.State != keystore.StateDisabled {
		t.Fatalf("disable: %+v %v", k, err)
	}
	if _, err := ks2.Unwrap(ctx, tenant.ID, 0); !errors.Is(err, keystore.ErrDisabled) {
		t.Fatalf("unwrap of a disabled key: %v", err)
	}
	if k, err := ks1.Enable(ctx, tenant.ID); err != nil || k.State != keystore.StateEnabled {
		t.Fatalf("enable: %+v %v", k, err)
	}

	// Destroy: the material is gone and so is the row's history.
	k, rep, err := ks1.Destroy(ctx, tenant.ID)
	if err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if k.State != keystore.StateDestroyed || !rep.Complete || rep.VersionsRemoved == 0 {
		t.Fatalf("destroy: %+v, report %+v", k, rep)
	}
	if _, err := ks2.Unwrap(ctx, tenant.ID, 0); !errors.Is(err, keystore.ErrDestroyed) {
		t.Fatalf("unwrap of a destroyed key: %v", err)
	}
	if _, err := ks2.Create(ctx, keystore.KindTenant, "items/4f6c"); !errors.Is(err, keystore.ErrExists) {
		t.Fatalf("a destroyed key's name was given again: %v", err)
	}

	// A node being decommissioned is refused a hand-over.
	if resp := c.call(0, false, cluster.AdminRequest{Op: "decommission", NodeID: 3}); resp.Error != "" {
		t.Fatalf("decommission n3: %s", resp.Error)
	}
	x, _ := enc.NewPeerExchange()
	ask := cluster.AdminRequest{Op: "cluster-key-handover", ClusterKey: &cluster.ClusterKeyRequest{
		ClusterID: c.tc.Nodes[0].ClusterID().String(), NodeID: 3, Exchange: x.Public()}}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(700 * time.Millisecond) {
		got := c.call(0, true, ask).Error
		if strings.Contains(got, "being decommissioned") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a hand-over to the decommissioning n3: %q", got)
		}
	}
	// And the root user's certificate is not a node's.
	ask.ClusterKey.NodeID = 2
	time.Sleep(600 * time.Millisecond)
	wantErr(t, "a hand-over asked with root's certificate", c.call(0, false, ask).Error, "only the cluster's node certificate")
}

// connectRoot opens a SQL connection to node 1 as root.
func connectRoot(t *testing.T, ctx context.Context, c *ckCluster) *pgx.Conn {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := connectSecure(ctx, secureURL(c.tc, c.certsDir, "root", "topsecret"))
		if err == nil {
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestClusterKeyPartialFailureConverges (issue #310): an enabling that
// could not reach every node leaves the key unrecorded and unusable, and
// running it again — from another node that already holds the key —
// finishes handing out the same key rather than making a second. A
// rotation that stops halfway is finished the same way, with one new
// version, not two.
func TestClusterKeyPartialFailureConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	c := startCKCluster(t, ctx, nil)
	c.waitReported(3)

	cut := func(node base.NodeID) func(base.NodeID) error {
		return func(to base.NodeID) error {
			if to == node {
				return errors.New("injected: unreachable")
			}
			return nil
		}
	}
	c.tc.Nodes[0].SetClusterKeyPushFilterForTesting(cut(3))
	errText, _ := c.op(0, "init")
	wantErr(t, "an init that cannot reach n3", errText, "incomplete")
	st := c.status(0)
	if st.Enabled || st.Pending == "" {
		t.Fatalf("after a partial init: %+v", st)
	}
	r1, r2, r3 := c.fileRefs(0), c.fileRefs(1), c.fileRefs(2)
	if len(r1) != 1 || len(r2) != 1 || r1[0] != r2[0] || len(r3) != 0 {
		t.Fatalf("after a partial init the files hold %v %v %v", r1, r2, r3)
	}
	if _, err := c.keyStore(1).Create(ctx, keystore.KindWrapping, "early"); err == nil || !strings.Contains(err.Error(), "not enabled") {
		t.Fatalf("a named key before the cluster key is recorded: %v", err)
	}
	c.tc.Nodes[0].SetClusterKeyPushFilterForTesting(nil)
	errText, ans := c.op(1, "init")
	if errText != "" {
		t.Fatalf("the retried init: %s", errText)
	}
	if got := (enc.ClusterKeyRef{ID: ans.Status.ID, Version: ans.Status.Version}).String(); got != r1[0] || ans.Status.Pending != "" {
		t.Fatalf("the retried init enabled %s (pending %q), want the key the first attempt handed out, %s", got, ans.Status.Pending, r1[0])
	}
	for i := 0; i < 3; i++ {
		if refs := c.fileRefs(i); len(refs) != 1 || refs[0] != r1[0] {
			t.Fatalf("n%d after the retry: %v", i+1, refs)
		}
	}

	k, err := c.keyStore(0).Create(ctx, keystore.KindWrapping, "k")
	if err != nil {
		t.Fatal(err)
	}
	c.tc.Nodes[0].SetClusterKeyPushFilterForTesting(cut(2))
	errText, _ = c.op(0, "rotate")
	wantErr(t, "a rotation that cannot reach n2", errText, "incomplete")
	if st := c.status(0); st.Version != 1 || st.Pending == "" {
		t.Fatalf("after a partial rotation: %+v", st)
	}
	if got, _ := c.keyStore(1).GetByID(ctx, k.ID); got.ClusterKey.Version != 1 {
		t.Fatalf("a partial rotation moved a key to %s", got.ClusterKey)
	}
	c.tc.Nodes[0].SetClusterKeyPushFilterForTesting(nil)
	errText, ans = c.op(0, "rotate")
	if errText != "" {
		t.Fatalf("the retried rotation: %s", errText)
	}
	if ans.Status.Version != 2 || ans.Status.Pending != "" || len(ans.Status.Previous) != 0 {
		t.Fatalf("after the retried rotation: %+v", ans.Status)
	}
	if got, err := c.keyStore(2).GetByID(ctx, k.ID); err != nil || got.ClusterKey.Version != 2 {
		t.Fatalf("the key after the retried rotation: %+v %v", got, err)
	}
}

// TestClusterKeyExportImportRestore (issue #310): a backup carries
// datax_keys as stored; a fresh cluster refuses to restore it until it
// has its own cluster key and the source's, imported from an export
// wrapped under the operator's root; after that the whole-cluster
// restore brings every named key back, rewrapped under the target's key,
// with the same material.
func TestClusterKeyExportImportRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	src := startCKCluster(t, ctx, nil)
	src.waitReported(3)
	if errText, _ := src.op(0, "init"); errText != "" {
		t.Fatalf("init: %s", errText)
	}
	ks := src.keyStore(0)
	a, err := ks.Create(ctx, keystore.KindWrapping, "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := ks.Create(ctx, keystore.KindTable, "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ks.Rotate(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	material := map[[2]int64][]byte{}
	for _, kv := range [][2]int64{{a.ID, 1}, {a.ID, 2}, {b.ID, 1}} {
		m, err := ks.Unwrap(ctx, kv[0], int(kv[1]))
		if err != nil {
			t.Fatal(err)
		}
		material[kv] = append([]byte(nil), m.Key...)
		m.Release()
	}
	backupDir := filepath.Join(t.TempDir(), "bk")
	if _, err := src.tc.Nodes[0].RunBackupWith(ctx, server.BackupOptions{Dest: backupDir, AllowPlaintext: true}); err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Export, the way the CLI does: sealed for this exchange, then
	// wrapped under the operator's root here.
	x, _ := enc.NewPeerExchange()
	resp := src.call(0, false, cluster.AdminRequest{Op: "cluster-key-export", ClusterKey: &cluster.ClusterKeyRequest{Exchange: x.Public()}})
	if resp.Error != "" {
		t.Fatalf("export: %s", resp.Error)
	}
	if resp.ClusterKey.Status.Enabled != true {
		t.Fatal("export carries no status")
	}
	srcID := resp.ClusterKey.Status.ClusterID
	exported, err := x.OpenClusterKeys(resp.ClusterKey.Exchange, resp.ClusterKey.Sealed, []byte("datax cluster key export|"+srcID))
	if err != nil {
		t.Fatal(err)
	}
	exportRoot := writeRootKey(t, t.TempDir(), "export.key")
	root, err := enc.ParseRoot(exportRoot)
	if err != nil {
		t.Fatal(err)
	}
	file, err := enc.ExportClusterKeys(ctx, root, srcID, enc.ClusterKeyRef{ID: resp.ClusterKey.Status.ID, Version: resp.ClusterKey.Status.Version}, exported)
	if err != nil {
		t.Fatal(err)
	}
	enc.ZeroTransportKeys(exported)
	src.tc.StopAll()

	dst := startCKCluster(t, ctx, nil)
	dst.waitReported(3)
	n1 := dst.tc.Nodes[0]
	if _, err := n1.RunRestore(ctx, []string{backupDir}); err == nil || !strings.Contains(err.Error(), "not enabled") {
		t.Fatalf("a restore of named keys before the cluster key: %v", err)
	}
	if errText, _ := dst.op(0, "init"); errText != "" {
		t.Fatalf("init on the target: %s", errText)
	}
	if _, err := n1.RunRestore(ctx, []string{backupDir}); err == nil || !strings.Contains(err.Error(), "does not hold") {
		t.Fatalf("a restore of named keys before the import: %v", err)
	}
	opened, err := file.Open(ctx, []enc.Root{root})
	if err != nil {
		t.Fatal(err)
	}
	resp = dst.call(1, false, cluster.AdminRequest{Op: "cluster-key-import", ClusterKey: &cluster.ClusterKeyRequest{Keys: opened}})
	if resp.Error != "" {
		t.Fatalf("import: %s", resp.Error)
	}
	if got := resp.ClusterKey.Status.Imported; len(got) != 1 {
		t.Fatalf("after import: %v", got)
	}
	sum, err := n1.RunRestore(ctx, []string{backupDir})
	if err != nil {
		t.Fatalf("restore after import: %v", err)
	}
	if len(sum.Notes) == 0 || !strings.Contains(sum.Notes[0], "2 named key(s) rewrapped") {
		t.Fatalf("restore notes: %v", sum.Notes)
	}
	dstRef := enc.ClusterKeyRef{ID: dst.status(0).ID, Version: 1}
	dks := dst.keyStore(2)
	for kv, want := range material {
		k, err := dks.GetByID(ctx, kv[0])
		if err != nil || k.ClusterKey != dstRef {
			t.Fatalf("restored key %d: %+v %v (want under %s)", kv[0], k, err, dstRef)
		}
		m, err := dks.Unwrap(ctx, kv[0], int(kv[1]))
		if err != nil || !bytes.Equal(m.Key, want) {
			t.Fatalf("restored key %d version %d: %v", kv[0], kv[1], err)
		}
		m.Release()
	}
}

// addNode starts a new node that joins the cluster through node 1, under
// a file root of its own, and returns its index.
func (c *ckCluster) addNode() int {
	c.t.Helper()
	i := len(c.dirs)
	c.dirs = append(c.dirs, c.t.TempDir())
	c.roots = append(c.roots, writeRootKey(c.t, c.t.TempDir(), "joiner.key"))
	n, err := c.tc.AddNodeErr(func(cfg *server.Config) {
		cfg.CertsDir = c.certsDir
		cfg.Join = c.tc.addrs[0]
		c.config(i, cfg)
	})
	if err != nil {
		c.t.Fatalf("joining a node: %v", err)
	}
	c.tc.addrs = append(c.tc.addrs, n.Addr())
	return i
}

// TestClusterKeyJoiningNode (issue #310): a node that joins after the
// cluster key was enabled has no copy and is given none at join; it asks
// a running peer and is handed the key (it is live, asking from the host
// it is registered at), and unwraps a named key made before it existed.
// A node that joins while a rotation is half done — the new version
// recorded, the named keys not yet rewrapped — is handed both versions,
// unwraps a key still under the old one, and is left with the new
// version alone once the rotation is finished.
func TestClusterKeyJoiningNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	c := startCKCluster(t, ctx, nil)
	c.waitReported(3)
	errText, ans := c.op(0, "init")
	if errText != "" {
		t.Fatalf("init: %s", errText)
	}
	v1 := enc.ClusterKeyRef{ID: ans.Status.ID, Version: 1}
	early, err := c.keyStore(0).Create(ctx, keystore.KindWrapping, "early")
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.keyStore(0).Unwrap(ctx, early.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), m.Key...)
	m.Release()

	// A node joins after init.
	j := c.addNode()
	joinerID := c.tc.Nodes[j].NodeID()
	c.waitFor("the joining node to be handed the key", 40*time.Second, func() bool {
		refs := c.fileRefs(j)
		return len(refs) == 1 && refs[0] == v1.String() && holdsCurrent(c.status(0), joinerID)
	})
	got, err := c.keyStore(j).Unwrap(ctx, early.ID, 0)
	if err != nil || !bytes.Equal(got.Key, want) {
		t.Fatalf("the joined node unwrapping a key made before it joined: %v", err)
	}
	got.Release()

	// A rotation stops with v2 recorded and nothing rewrapped yet.
	c.tc.Nodes[0].StopClusterKeyRotationBeforeRewrapForTesting(true)
	errText, _ = c.op(0, "rotate")
	wantErr(t, "a rotation stopped before the rewrap", errText, "stopped before the rewrap")
	c.tc.Nodes[0].StopClusterKeyRotationBeforeRewrapForTesting(false)
	v2 := enc.ClusterKeyRef{ID: v1.ID, Version: 2}
	if st := c.status(0); st.Version != 2 || len(st.Previous) != 1 || st.Previous[0] != 1 {
		t.Fatalf("after the stopped rotation: %+v", st)
	}
	if k, err := c.keyStore(1).GetByID(ctx, early.ID); err != nil || k.ClusterKey != v1 {
		t.Fatalf("a key after the stopped rotation: %+v %v (want still under %s)", k, err, v1)
	}

	// A second node joins now: it needs both versions.
	j2 := c.addNode()
	joiner2 := c.tc.Nodes[j2].NodeID()
	c.waitFor("the second joining node to be handed both versions", 40*time.Second, func() bool {
		refs := c.fileRefs(j2)
		return len(refs) == 2 && refs[0] == v1.String() && refs[1] == v2.String() && holdsCurrent(c.status(0), joiner2)
	})
	got, err = c.keyStore(j2).Unwrap(ctx, early.ID, 0)
	if err != nil || !bytes.Equal(got.Key, want) {
		t.Fatalf("the second joined node unwrapping a key still under v1: %v", err)
	}
	got.Release()
	later, err := c.keyStore(j2).Create(ctx, keystore.KindWrapping, "later")
	if err != nil || later.ClusterKey != v2 {
		t.Fatalf("a key made on the joined node mid-rotation: %+v %v", later, err)
	}

	// Finishing the rotation leaves every node, the joiners included,
	// with v2 alone, and both keys under it.
	errText, ans = c.op(j2, "rotate")
	if errText != "" {
		t.Fatalf("finishing the rotation: %s", errText)
	}
	if ans.Status.Version != 2 || len(ans.Status.Previous) != 0 {
		t.Fatalf("after finishing the rotation: %+v (%v)", ans.Status, ans.Notes)
	}
	c.waitFor("every node to drop v1", 20*time.Second, func() bool {
		for i := range c.dirs {
			if refs := c.fileRefs(i); len(refs) != 1 || refs[0] != v2.String() {
				return false
			}
		}
		return true
	})
	for _, id := range []int64{early.ID, later.ID} {
		k, err := c.keyStore(j).GetByID(ctx, id)
		if err != nil || k.ClusterKey != v2 {
			t.Fatalf("key %d after the rotation: %+v %v", id, k, err)
		}
		got, err := c.keyStore(j).Unwrap(ctx, id, 0)
		if err != nil {
			t.Fatalf("key %d on the first joiner after the rotation: %v", id, err)
		}
		if id == early.ID && !bytes.Equal(got.Key, want) {
			t.Fatal("the early key's material changed")
		}
		got.Release()
	}
}
