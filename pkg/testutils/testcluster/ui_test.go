package testcluster

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/server/ui"
)

// startWithHTTP is StartWithEngines plus an HTTP listener per node.
func startWithHTTP(t *testing.T, numNodes int) *TestCluster {
	t.Helper()
	listeners := make([]net.Listener, numNodes)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
	}
	i := 0
	tc, _ := StartWithEngines(t, numNodes, func(c *server.Config) {
		c.HTTPListener = listeners[i]
		i++
	})
	return tc
}

// httpDo performs one request with the given headers and returns the
// response (its body drained and closed).
func httpDo(t *testing.T, method, url string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

func httpGet(t *testing.T, url string) (int, string, string) {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(body)
}

// TestClusterAPI: every node serves a cluster-wide document — all nodes
// listed live, cluster ranges deduplicated — plus its own local detail.
func TestClusterAPI(t *testing.T) {
	tc := startWithHTTP(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_ = ctx

	for i, n := range tc.Nodes {
		code, ctype, body := httpGet(t, "http://"+n.HTTPAddr()+"/api/cluster")
		if code != 200 || !strings.Contains(ctype, "application/json") {
			t.Fatalf("node %d /api/cluster: %d %s", i+1, code, ctype)
		}
		var doc server.ClusterStatus
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatalf("node %d: %v", i+1, err)
		}
		if doc.NodeID != i+1 {
			t.Fatalf("node %d reports node_id %d", i+1, doc.NodeID)
		}
		// Published JSON names (issue #146): the machine sample's load
		// averages and the storage snapshot are snake_case like the rest
		// of the document — asserted on the raw body so a dropped tag
		// fails rather than silently renaming a field.
		for _, key := range []string{`"load1"`, `"load5"`, `"load15"`, `"l0_files"`, `"compaction_debt_bytes"`, `"block_cache_hits"`, `"filter_hits"`, `"console_version"`} {
			if !strings.Contains(body, key) {
				t.Fatalf("node %d /api/cluster lacks %s", i+1, key)
			}
		}
		for _, key := range []string{`"Load1"`, `"L0Files"`, `"CompactionDebtBytes"`} {
			if strings.Contains(body, key) {
				t.Fatalf("node %d /api/cluster still carries the Go-cased %s", i+1, key)
			}
		}
		if doc.ConsoleVersion == "" {
			t.Fatalf("node %d: no console_version", i+1)
		}
		if len(doc.Nodes) != 3 {
			t.Fatalf("node %d sees %d nodes", i+1, len(doc.Nodes))
		}
		for _, cn := range doc.Nodes {
			if !cn.Live {
				t.Fatalf("node %d sees n%d dead: %+v", i+1, cn.NodeID, cn)
			}
		}
		if doc.Error != "" {
			t.Fatalf("node %d: %s", i+1, doc.Error)
		}
		if len(doc.Ranges) == 0 {
			t.Fatalf("node %d sees no cluster ranges", i+1)
		}
		seen := map[int64]bool{}
		for _, r := range doc.Ranges {
			if seen[r.RangeID] {
				t.Fatalf("node %d: duplicate range %d in cluster listing", i+1, r.RangeID)
			}
			seen[r.RangeID] = true
			if len(r.Replicas) == 0 {
				t.Fatalf("node %d: range %d has no replicas", i+1, r.RangeID)
			}
		}
		if doc.Storage == nil {
			t.Fatalf("node %d: no storage metrics", i+1)
		}
		if len(doc.Local.Ranges) == 0 {
			t.Fatalf("node %d: no local ranges", i+1)
		}
		// Every node's row carries the host summary its heartbeat
		// advertised (the very first beat may predate range 1's leader,
		// so allow a beat or two), and the serving node's own full sample
		// rides along; the in-memory test store has no disk to report.
		deadline := time.Now().Add(20 * time.Second)
		for {
			missing := 0
			for _, cn := range doc.Nodes {
				if cn.Machine == nil {
					missing++
					continue
				}
				if cn.Machine.Cores <= 0 || cn.Machine.UptimeSeconds < 0 {
					t.Fatalf("node %d sees n%d with a bad machine summary: %+v", i+1, cn.NodeID, cn.Machine)
				}
				if runtime.GOOS == "linux" && (cn.Machine.MemTotal == 0 || cn.Machine.FDLimit == 0) {
					t.Fatalf("node %d sees n%d without host memory or fd figures: %+v", i+1, cn.NodeID, cn.Machine)
				}
			}
			if missing == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d: %d nodes never advertised a machine summary", i+1, missing)
			}
			time.Sleep(500 * time.Millisecond)
			_, _, body = httpGet(t, "http://"+n.HTTPAddr()+"/api/cluster")
			if err := json.Unmarshal([]byte(body), &doc); err != nil {
				t.Fatal(err)
			}
		}
		m := doc.Local.Machine
		if m == nil || m.Goroutines == 0 || m.Cores == 0 {
			t.Fatalf("node %d: no local machine sample: %+v", i+1, m)
		}
		if m.DiskTotal != 0 {
			t.Fatalf("node %d: an in-memory store reported a disk: %+v", i+1, m)
		}
	}

	// /metrics exports the host figures next to the standard Go and
	// process collectors.
	_, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/metrics")
	for _, want := range []string{"datax_node_cpu_percent{scope=\"host\"}", "datax_node_memory_bytes{kind=\"available\"}", "datax_node_load1", "datax_node_load5", "datax_node_load15", "datax_process_fd_limit", "go_goroutines", "process_open_fds"} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics lacks %s", want)
		}
	}
}

// TestUIServed: the dashboard is served at exactly /, is self-contained
// (no external URLs — nodes may be airgapped), and unknown paths 404.
func TestUIServed(t *testing.T) {
	tc := startWithHTTP(t, 1)
	addr := tc.Nodes[0].HTTPAddr()

	code, ctype, body := httpGet(t, "http://"+addr+"/")
	if code != 200 || !strings.Contains(ctype, "text/html") {
		t.Fatalf("/: %d %s", code, ctype)
	}
	if !strings.Contains(body, "/api/cluster") {
		t.Fatal("dashboard does not poll /api/cluster")
	}
	// Airgap check: no external references of any kind. The only URLs the
	// page may contain are same-origin absolute paths.
	if re := regexp.MustCompile(`(https?:)?//[a-zA-Z0-9.-]+\.[a-z]{2,}`); re.MatchString(body) {
		t.Fatalf("dashboard references an external host: %q", re.FindString(body))
	}
	// The one <link> is the favicon slot, which the page paints itself on
	// a canvas (issue #150): it carries no href in the served page.
	stripped := strings.Replace(body, `<link rel="icon" id="favicon">`, "", 1)
	for _, tag := range []string{"<script src=", "<link ", "@import", "url("} {
		if strings.Contains(stripped, tag) {
			t.Fatalf("dashboard loads an external asset via %q", tag)
		}
	}

	if strings.Contains(body, "__CONSOLE_VERSION__") {
		t.Fatal("the served page still carries the console version placeholder")
	}
	// The console is assembled from its script files at startup (issue
	// #151): every one of them is in the page the node serves, and the
	// seam they were spliced into is gone.
	if strings.Contains(body, "__CONSOLE_SCRIPTS__") {
		t.Fatal("the served page still carries the console script placeholder")
	}
	names, err := ui.ScriptFiles()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if !strings.Contains(body, "// ==== "+name+" ====") {
			t.Fatalf("the served console is missing %s", name)
		}
	}
	// Every route the nav offers resolves to a container in the page.
	for _, view := range []string{"overview", "nodes", "node", "data", "sql", "schema", "metrics", "ops", "security"} {
		if n := strings.Count(body, `<main id="view-`+view+`"`); n != 1 {
			t.Fatalf("the served console has %d containers for the %s view, want 1", n, view)
		}
	}
	// The page is served with its digest as ETag (issue #146): a reload
	// with the same version is a 304, the version the page carries is
	// the one /api/cluster reports, and the tab can tell an upgrade.
	resp := httpDo(t, "GET", "http://"+addr+"/", nil)
	etag := resp.Header.Get("ETag")
	if etag == "" || resp.Header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("/: ETag %q, Cache-Control %q", etag, resp.Header.Get("Cache-Control"))
	}
	if !strings.Contains(body, `const CONSOLE_VERSION = `+etag) {
		t.Fatalf("the page does not carry its ETag %s as CONSOLE_VERSION", etag)
	}
	_, _, cbody := httpGet(t, "http://"+addr+"/api/cluster")
	if !strings.Contains(cbody, `"console_version": `+etag) {
		t.Fatalf("/api/cluster does not report console_version %s", etag)
	}
	if resp := httpDo(t, "GET", "http://"+addr+"/", map[string]string{"If-None-Match": etag}); resp.StatusCode != 304 {
		t.Fatalf("/ with If-None-Match %s: %d, want 304", etag, resp.StatusCode)
	}

	if code, _, _ := httpGet(t, "http://"+addr+"/nope"); code != 404 {
		t.Fatalf("/nope: %d, want 404", code)
	}
	if code, _, _ := httpGet(t, "http://"+addr+"/status"); code != 200 {
		t.Fatalf("/status: %d", code)
	}
	if code, _, _ := httpGet(t, "http://"+addr+"/metrics"); code != 200 {
		t.Fatalf("/metrics: %d", code)
	}
}

// TestClusterRollup (issue #145): every node's /api/cluster carries the
// same cluster rollup — the figures are summed over the live nodes'
// heartbeats, not taken from the node that served the page — and once a
// node falls outside the liveness grace window its figures leave the
// totals and the contributing-node count drops with it.
func TestClusterRollup(t *testing.T) {
	// Two heartbeat intervals: a peer's row is as old as the last tick
	// that carried it, so a tighter window flaps.
	const grace = 6 * time.Second
	listeners := make([]net.Listener, 3)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
	}
	i := 0
	tc, _ := StartWithEngines(t, 3, func(c *server.Config) {
		c.HTTPListener = listeners[i]
		c.LivenessGrace = grace
		c.DeadNodeThreshold = 2 * grace
		i++
	})
	rollupOf := func(n *server.Node) server.ClusterRollup {
		t.Helper()
		_, _, body := httpGet(t, "http://"+n.HTTPAddr()+"/api/cluster")
		var doc server.ClusterStatus
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		return doc.Rollup
	}
	// Convergence: every node's registry has every heartbeat, and every
	// range has a leader (leases == ranges).
	deadline := time.Now().Add(30 * time.Second)
	for {
		rs := []server.ClusterRollup{rollupOf(tc.Nodes[0]), rollupOf(tc.Nodes[1]), rollupOf(tc.Nodes[2])}
		ok := true
		for _, r := range rs {
			// (These nodes run no SQL server, so no heartbeat carries a
			// SQL summary and the SQL sums cover no node.)
			if r.Nodes != 3 || r.LiveNodes != 3 || r.Contributing != 3 || r.SQLContributing != 0 || r.Ranges == 0 || r.Leases != r.Ranges || r.Replicas != 3*r.Ranges || r.DataBytes <= 0 {
				ok = false
			}
		}
		// The structural figures agree exactly; the byte and QPS sums
		// are each node's view of the others' latest heartbeats, which
		// differ by a heartbeat's growth, so they are only required to
		// cover every node (Contributing above).
		for _, r := range rs[1:] {
			if r.Ranges != rs[0].Ranges || r.Replicas != rs[0].Replicas || r.Leases != rs[0].Leases {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rollups did not converge: %+v", rs)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// n3 goes away: after the grace window the survivors' totals cover
	// two contributing nodes and say so.
	tc.Nodes[2].Stop()
	deadline = time.Now().Add(30 * time.Second)
	for {
		r0, r1 := rollupOf(tc.Nodes[0]), rollupOf(tc.Nodes[1])
		if r0.Nodes == 3 && r0.LiveNodes == 2 && r0.Contributing == 2 &&
			r1.LiveNodes == 2 && r1.Contributing == 2 && r0.DataBytes > 0 && r1.DataBytes > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rollups after n3 stopped: %+v / %+v", r0, r1)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// TestOverviewAPI (issue #147): /api/overview carries what the overview
// draws — the cluster document, the health problems and the event tail
// — with the same figures the individual endpoints report, a
// per-section error map (empty here), and the events tail bounded by
// ?limit.
func TestOverviewAPI(t *testing.T) {
	tc := startWithHTTP(t, 3)
	addr := tc.Nodes[0].HTTPAddr()
	code, ctype, body := httpGet(t, "http://"+addr+"/api/overview?limit=5")
	if code != 200 || !strings.Contains(ctype, "application/json") {
		t.Fatalf("/api/overview: %d %s", code, ctype)
	}
	var ov server.OverviewStatus
	if err := json.Unmarshal([]byte(body), &ov); err != nil {
		t.Fatal(err)
	}
	if ov.Errors == nil || len(ov.Errors) != 0 {
		t.Fatalf("errors: %v", ov.Errors)
	}
	if ov.Health == nil || ov.Events == nil {
		t.Fatalf("missing sections: %s", body)
	}
	var cl server.ClusterStatus
	_, _, cbody := httpGet(t, "http://"+addr+"/api/cluster")
	if err := json.Unmarshal([]byte(cbody), &cl); err != nil {
		t.Fatal(err)
	}
	if ov.Cluster.NodeID != cl.NodeID || len(ov.Cluster.Nodes) != len(cl.Nodes) || ov.Cluster.Rollup.Ranges != cl.Rollup.Ranges || ov.Cluster.Rollup.Replicas != cl.Rollup.Replicas || ov.Cluster.ConsoleVersion != cl.ConsoleVersion {
		t.Fatalf("overview cluster section differs from /api/cluster:\n%+v\n%+v", ov.Cluster.Rollup, cl.Rollup)
	}
	var h server.HealthStatus
	_, _, hbody := httpGet(t, "http://"+addr+"/api/health")
	if err := json.Unmarshal([]byte(hbody), &h); err != nil {
		t.Fatal(err)
	}
	if ov.Health.Checks != h.Checks || ov.Health.NodeID != h.NodeID {
		t.Fatalf("overview health section differs from /api/health: %d checks vs %d", ov.Health.Checks, h.Checks)
	}
	var ev server.EventsStatus
	_, _, ebody := httpGet(t, "http://"+addr+"/api/events")
	if err := json.Unmarshal([]byte(ebody), &ev); err != nil {
		t.Fatal(err)
	}
	if ov.Events.Latest > ev.Latest || len(ov.Events.Events) > 5 {
		t.Fatalf("overview events: latest %d (endpoint %d), %d events with limit 5", ov.Events.Latest, ev.Latest, len(ov.Events.Events))
	}
}
