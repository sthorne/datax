package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
)

// /api/operations (issue #210).
//
// The event ring is per node, so the operations view described whichever
// node served the page, and the operation an operator most wants to
// watch — a decommission driven by the draining node, a backup running
// where it was started, a re-encryption sweep of one store — was, by
// construction, usually running somewhere else. The per-node label was
// honest and did not stop anyone concluding the cluster was idle.
//
// This is the cluster's answer: every live node's paired operations
// (what its ring holds, plus what it knows is open whose start has aged
// out, per #190), merged, each row keeping the node it came from. It
// fans out over the same internode admin RPC /api/statements uses,
// under the same admin gate, and it is polled slowly for the same
// reason: a fan-out asks every node.
//
// The rings themselves stay where they are. Merging five hundred-entry
// rings into one feed would bury each node's record under the others';
// the operations are what generalise, the raw records are what stay
// local, and the view already keeps the two apart.

// OperationsStatus is the /api/operations document.
type OperationsStatus struct {
	Now    int64 `json:"now_unix_ms"`
	NodeID int   `json:"node_id"`
	// Operations are every answering node's, running first (newest start
	// first), then completed (newest end first) — the order each node's
	// own list has.
	Operations []ClusterOperation `json:"operations"`
	// Nodes is how many answered and NodesAsked how many were asked. A
	// node that did not answer is named in Errors with the reason and
	// its heartbeat age: partial is the normal case, not a failure — a
	// fan-out that failed whole because one node is unreachable would be
	// useless precisely when a node is unreachable.
	Nodes      int      `json:"nodes"`
	NodesAsked int      `json:"nodes_asked"`
	Errors     []string `json:"errors,omitempty"`
	// Truncated is how many completed operations were dropped to keep
	// the document bounded; running ones are never dropped.
	Truncated int `json:"truncated,omitempty"`
}

// ClusterOperation is one node's operation with the node it is on.
type ClusterOperation struct {
	Operation
	NodeID int `json:"node_id"`
}

// clusterOperationLimit bounds the completed operations the document
// carries after the merge. Each node's ring bounds its own list; the
// cluster's is that times the node count.
const clusterOperationLimit = 200

// peerAnswerTimeout is how long one node has to answer the fan-out.
const peerAnswerTimeout = 3 * time.Second

func (n *Node) serveOperationsAPI(w http.ResponseWriter, req *http.Request) {
	doc := n.operationsDoc(req.Context())
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

// nodeOperations is one node's answer to the fan-out.
type nodeOperations struct {
	id  base.NodeID
	ops []Operation
	err string
}

func (n *Node) operationsDoc(ctx context.Context) OperationsStatus {
	now := n.clock.Now().WallTime
	nowMs := now / int64(time.Millisecond)
	doc := OperationsStatus{Now: nowMs, NodeID: int(n.ident.NodeID), Operations: []ClusterOperation{}}

	// This node's own, then every peer's — together, so the document
	// takes one answer's time and not the sum of them: a node that does
	// not answer costs the whole timeout, and the cluster's operations
	// are wanted most when a node is not answering.
	answers := []nodeOperations{{id: n.ident.NodeID, ops: operationsFrom(n.events.Recent(0, 0, true), n.events.Open(), nowMs)}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, nd := range n.registry.All() {
		if nd.NodeID == n.ident.NodeID {
			continue
		}
		wg.Add(1)
		go func(id base.NodeID, livenessTime int64) {
			defer wg.Done()
			ans := nodeOperations{id: id}
			detail, err := n.peerNodeDetail(ctx, id)
			if err != nil {
				// Named with its heartbeat age, so "did not answer"
				// reads as the node being gone or as the node being
				// slow, which call for different responses.
				ans.err = fmt.Sprintf("n%d did not answer: %s (last heartbeat %s ago)", id, err.Error(),
					time.Duration(now-livenessTime).Truncate(time.Second))
			} else {
				ans.ops = detail.Operations
			}
			mu.Lock()
			answers = append(answers, ans)
			mu.Unlock()
		}(nd.NodeID, nd.LivenessTime)
	}
	wg.Wait()
	sort.Slice(answers, func(i, j int) bool { return answers[i].id < answers[j].id })

	doc.NodesAsked = len(answers)
	var running, done []ClusterOperation
	for _, a := range answers {
		if a.err != "" {
			doc.Errors = append(doc.Errors, a.err)
			continue
		}
		doc.Nodes++
		for _, op := range a.ops {
			row := ClusterOperation{Operation: op, NodeID: int(a.id)}
			if op.Running {
				running = append(running, row)
			} else {
				done = append(done, row)
			}
		}
	}
	doc.Operations, doc.Truncated = mergeOperations(running, done, clusterOperationLimit)
	return doc
}

// mergeOperations orders the rows as one node's own list is ordered —
// running first, newest start first; then completed, newest end first,
// the node id breaking ties so the order is stable across polls — and
// bounds the completed ones.
func mergeOperations(running, done []ClusterOperation, limit int) ([]ClusterOperation, int) {
	sort.SliceStable(running, func(i, j int) bool {
		if running[i].StartedMs != running[j].StartedMs {
			return running[i].StartedMs > running[j].StartedMs
		}
		return running[i].NodeID < running[j].NodeID
	})
	sort.SliceStable(done, func(i, j int) bool {
		if done[i].EndedMs != done[j].EndedMs {
			return done[i].EndedMs > done[j].EndedMs
		}
		return done[i].NodeID < done[j].NodeID
	})
	truncated := 0
	if len(done) > limit {
		truncated = len(done) - limit
		done = done[:limit]
	}
	out := make([]ClusterOperation, 0, len(running)+len(done))
	out = append(out, running...)
	out = append(out, done...)
	return out, truncated
}

// peerNodeDetail asks one node for its /api/node document over the
// internode admin RPC, within peerAnswerTimeout.
func (n *Node) peerNodeDetail(ctx context.Context, id base.NodeID) (NodeDetail, error) {
	addr, err := n.registry.Resolve(id)
	if err != nil {
		return NodeDetail{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, peerAnswerTimeout)
	defer cancel()
	var resp cluster.AdminResponse
	if err := n.trans.Call(ctx, addr, "admin", cluster.AdminRequest{Op: "node-detail"}, &resp); err != nil {
		return NodeDetail{}, err
	}
	if resp.Error != "" {
		return NodeDetail{}, fmt.Errorf("%s", resp.Error)
	}
	var detail NodeDetail
	if err := json.Unmarshal(resp.Status, &detail); err != nil {
		return NodeDetail{}, fmt.Errorf("undecodable detail: %w", err)
	}
	return detail, nil
}
