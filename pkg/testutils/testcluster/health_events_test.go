package testcluster

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// healthDoc fetches and decodes /api/health from node i.
func healthDoc(t *testing.T, tc *TestCluster, i int) server.HealthStatus {
	t.Helper()
	code, _, body := httpGet(t, "http://"+tc.Nodes[i].HTTPAddr()+"/api/health")
	if code != 200 {
		t.Fatalf("n%d /api/health: HTTP %d: %s", i+1, code, body)
	}
	var doc server.HealthStatus
	if err := jsonUnmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func eventsDoc(t *testing.T, tc *TestCluster, i int, since uint64) server.EventsStatus {
	t.Helper()
	code, _, body := httpGet(t, fmt.Sprintf("http://%s/api/events?since=%d", tc.Nodes[i].HTTPAddr(), since))
	if code != 200 {
		t.Fatalf("n%d /api/events: HTTP %d: %s", i+1, code, body)
	}
	var doc server.EventsStatus
	if err := jsonUnmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// TestHealthAndEvents: a healthy cluster reports no problems (but a
// non-zero check count); a split shows up in the leader's event feed and
// tails by sequence; a stopped node surfaces as node-unresponsive and
// then node-down as its heartbeat ages past the grace and dead
// thresholds; and the panel is mirrored into datax_health_problems.
func TestHealthAndEvents(t *testing.T) {
	const (
		grace = 3 * time.Second
		dead  = 6 * time.Second
	)
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
		c.DeadNodeThreshold = dead
		i++
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Healthy: no warnings or criticals once every node has heartbeated
	// (the first checks may run before every heartbeat lands and before
	// the sampler has a second CPU reading, so allow a few polls).
	deadline := time.Now().Add(30 * time.Second)
	for {
		doc := healthDoc(t, tc, 0)
		if doc.Checks == 0 {
			t.Fatalf("no checks ran: %+v", doc)
		}
		var bad []string
		for _, p := range doc.Problems {
			if p.Severity != server.SeverityInfo {
				bad = append(bad, p.Check+": "+p.Summary)
			}
		}
		if len(bad) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("healthy cluster reports problems: %s", strings.Join(bad, "; "))
		}
		time.Sleep(500 * time.Millisecond)
	}

	// A split is an event on the range leader; tailing from the latest
	// sequence returns only what happened since.
	before := eventsDoc(t, tc, 0, 0)
	key := append(keys.TableDataPrefix(760), "m"...)
	if _, err := tc.Nodes[0].DB().AdminSplit(ctx, key); err != nil {
		t.Fatalf("split: %v", err)
	}
	found := false
	deadline = time.Now().Add(20 * time.Second)
	for !found && time.Now().Before(deadline) {
		for j := range tc.Nodes {
			doc := eventsDoc(t, tc, j, 0)
			for _, e := range doc.Events {
				if e.Kind == "split" && e.Seq > 0 && !e.At.IsZero() && e.Summary != "" {
					found = true
				}
			}
		}
		if !found {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if !found {
		t.Fatal("no split event recorded on any node")
	}
	tail := eventsDoc(t, tc, 0, before.Latest)
	for _, e := range tail.Events {
		if e.Seq <= before.Latest {
			t.Fatalf("since=%d returned seq %d", before.Latest, e.Seq)
		}
	}
	if tail.Latest < before.Latest {
		t.Fatalf("latest went backwards: %d -> %d", before.Latest, tail.Latest)
	}
	// Audit records reach the ring through a subscriber that sits beside
	// any sink a test installs, and in insecure mode every caller is
	// admin, so a CREATE USER shows up on the events feed marked audit.
	sess := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, sess, `CREATE USER auditee PASSWORD 'pw'`)
	audited := false
	for _, e := range eventsDoc(t, tc, 0, before.Latest).Events {
		if e.Audit && e.Kind == "role-ddl" && strings.Contains(e.Summary, "auditee") {
			audited = true
		}
	}
	if !audited {
		t.Fatal("CREATE USER produced no audit event on the feed")
	}

	// Stop n3: its heartbeat ages through the grace window (warning) and
	// then the dead threshold (critical), and /metrics carries the gauge.
	tc.StopNode(2)
	sawWarning, sawCritical := false, false
	deadline = time.Now().Add(dead + 30*time.Second)
	for time.Now().Before(deadline) {
		doc := healthDoc(t, tc, 0)
		for _, p := range doc.Problems {
			if p.Node != 3 {
				continue
			}
			switch p.Check {
			case "node-unresponsive":
				if p.Severity != server.SeverityWarning || p.Section != "nodes" {
					t.Fatalf("bad row: %+v", p)
				}
				sawWarning = true
			case "node-down":
				if p.Severity != server.SeverityCritical {
					t.Fatalf("bad row: %+v", p)
				}
				sawCritical = true
			}
		}
		if sawCritical {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !sawWarning || !sawCritical {
		t.Fatalf("stopped node: warning=%v critical=%v", sawWarning, sawCritical)
	}
	// Problems are sorted by severity, critical first.
	doc := healthDoc(t, tc, 0)
	rank := func(s string) int {
		switch s {
		case server.SeverityCritical:
			return 0
		case server.SeverityWarning:
			return 1
		}
		return 2
	}
	for j := 1; j < len(doc.Problems); j++ {
		if rank(doc.Problems[j-1].Severity) > rank(doc.Problems[j].Severity) {
			t.Fatalf("problems not sorted by severity: %+v", doc.Problems)
		}
	}
	_, _, metricsBody := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/metrics")
	if !strings.Contains(metricsBody, `datax_health_problems{check="node-down",severity="critical"} 1`) {
		t.Fatalf("/metrics lacks the node-down gauge:\n%s", grepLines(metricsBody, "datax_health_problems"))
	}
}

func grepLines(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
