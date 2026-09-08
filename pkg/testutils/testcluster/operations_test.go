package testcluster

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

func operationsDoc(t *testing.T, tc *TestCluster, i int) server.OperationsStatus {
	t.Helper()
	code, _, body := httpGet(t, "http://"+tc.Nodes[i].HTTPAddr()+"/api/operations")
	if code != 200 {
		t.Fatalf("n%d /api/operations: HTTP %d: %s", i+1, code, body)
	}
	var doc server.OperationsStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// The cluster's operations from any node's console (issue #210): an
// operation running on n2 and one that finished on n3 both appear in
// n1's /api/operations with their node named; a node that stops is
// named as not answering beside the rows that arrived, rather than
// failing the document.
func TestClusterOperationsAreSeenFromAnyNode(t *testing.T) {
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
		i++
	})

	// Operations are recorded by the node doing the work, on its own
	// ring: a backup still running on n2, a drain that finished on n3.
	tc.Nodes[1].Events().RecordStart("backup", "b1", "backup to /backups/nightly started")
	tc.Nodes[2].Events().RecordStart("decommission", "d1", "n3 is draining: moving its replicas off")
	tc.Nodes[2].Events().RecordEnd("decommission", "d1", "ok", "n3 drained: no replicas remain on it")

	doc := operationsDoc(t, tc, 0)
	if doc.NodeID != 1 || doc.Nodes != 3 || doc.NodesAsked != 3 || len(doc.Errors) != 0 {
		t.Fatalf("n1's document: node %d, %d of %d answered, errors %v", doc.NodeID, doc.Nodes, doc.NodesAsked, doc.Errors)
	}
	find := func(doc server.OperationsStatus, kind, op string) *server.ClusterOperation {
		for i := range doc.Operations {
			if doc.Operations[i].Kind == kind && doc.Operations[i].Op == op {
				return &doc.Operations[i]
			}
		}
		return nil
	}
	backup := find(doc, "backup", "b1")
	if backup == nil || backup.NodeID != 2 || !backup.Running || backup.ElapsedMs < 0 {
		t.Fatalf("n2's running backup from n1's console: %+v", backup)
	}
	drain := find(doc, "decommission", "d1")
	if drain == nil || drain.NodeID != 3 || drain.Running || drain.Outcome != "ok" {
		t.Fatalf("n3's finished drain from n1's console: %+v", drain)
	}
	// Running first, whatever node they are on.
	seenDone := false
	for _, o := range doc.Operations {
		if !o.Running {
			seenDone = true
		} else if seenDone {
			t.Fatalf("a running operation after a completed one: %+v", doc.Operations)
		}
	}
	// And the same from n3's console: the document does not depend on
	// which node was asked.
	if from3 := find(operationsDoc(t, tc, 2), "backup", "b1"); from3 == nil || from3.NodeID != 2 {
		t.Fatalf("n2's backup from n3's console: %+v", from3)
	}

	// n3 stops. Its operations are gone with its ring; the document
	// still arrives, says n3 did not answer and how long ago it was
	// last heard from, and n2's backup is still in it.
	tc.StopNode(2)
	time.Sleep(500 * time.Millisecond)
	doc = operationsDoc(t, tc, 0)
	if doc.Nodes != 2 || doc.NodesAsked != 3 || len(doc.Errors) != 1 {
		t.Fatalf("with n3 stopped: %d of %d answered, errors %v", doc.Nodes, doc.NodesAsked, doc.Errors)
	}
	if e := doc.Errors[0]; !strings.HasPrefix(e, "n3 did not answer: ") || !strings.Contains(e, "last heartbeat ") {
		t.Fatalf("the missing node is not named with its heartbeat age: %q", e)
	}
	if backup = find(doc, "backup", "b1"); backup == nil || backup.NodeID != 2 || !backup.Running {
		t.Fatalf("n2's backup with n3 stopped: %+v", backup)
	}
	if drain = find(doc, "decommission", "d1"); drain != nil {
		t.Fatalf("a stopped node's operation is still reported: %+v", drain)
	}
}
