package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// TestNodeAPI (issue #86): /api/node serves the serving node's own
// detail to anyone and another node's detail through the internode
// fan-out (admin only in secure mode), with identity from the registry
// and status, storage, settings, latency and events from the node itself.
func TestNodeAPI(t *testing.T) {
	tc := startWithHTTP(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_ = ctx

	code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/node")
	var self server.NodeDetail
	if code != 200 {
		t.Fatalf("/api/node: %d %s", code, body)
	}
	if err := jsonUnmarshal([]byte(body), &self); err != nil {
		t.Fatal(err)
	}
	if self.NodeID != 1 || !self.Live || self.Status == nil || len(self.Status.Ranges) == 0 || self.Storage == nil ||
		self.Settings["storage profile"] == "" || self.Release == "" || self.BinaryVersion == 0 || self.Events == nil {
		t.Fatalf("self detail incomplete: %s", body)
	}

	// Another node's detail arrives through the fan-out, with that node's
	// own figures (its ID, ranges, settings) and registry identity.
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, _, body = httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/node?id=3")
		if code != 200 {
			t.Fatalf("/api/node?id=3: %d %s", code, body)
		}
		var d server.NodeDetail
		if err := jsonUnmarshal([]byte(body), &d); err != nil {
			t.Fatal(err)
		}
		if d.NodeID != 3 {
			t.Fatalf("asked for n3, got n%d: %s", d.NodeID, body)
		}
		if d.Error == "" && d.Live && d.Status != nil && len(d.Status.Ranges) > 0 && d.Address == tc.Nodes[2].Addr() && len(d.Latency) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("n3 detail never complete: %s", body)
		}
		time.Sleep(300 * time.Millisecond)
	}
	if code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/node?id=9"); code != 404 || !strings.Contains(body, "not a member") {
		t.Fatalf("/api/node?id=9: %d %s", code, body)
	}
	if code, _, _ := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/node?id=x"); code != 400 {
		t.Fatalf("/api/node?id=x: %d", code)
	}
}
