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
	for _, want := range []string{"datax_node_cpu_percent{scope=\"host\"}", "datax_node_memory_bytes{kind=\"available\"}", "datax_node_load1", "datax_process_fd_limit", "go_goroutines", "process_open_fds"} {
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
	for _, tag := range []string{"<script src=", "<link ", "@import", "url("} {
		if strings.Contains(body, tag) {
			t.Fatalf("dashboard loads an external asset via %q", tag)
		}
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
