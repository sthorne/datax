package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/pgwire"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// /api/statements (issue #157).
//
// The console could show what is running now and what was slow recently,
// per node. Neither answers which statement shape costs this cluster the
// most — and a statement that takes 8ms and runs forty thousand times an
// hour, never slow enough for a ring, is usually the thing worth fixing.
//
// This is the cluster's answer: each node's fingerprint accounting,
// unioned and re-ranked by total time. It fans out over the same
// internode admin RPC /api/node already uses, under the same admin gate,
// because a representative statement carries data.

// StatementsStatus is the /api/statements document.
type StatementsStatus struct {
	Now    int64 `json:"now_unix_ms"`
	NodeID int   `json:"node_id"`
	// Statements are the heaviest shapes cluster-wide, by total time.
	Statements []ClusterStatement `json:"statements"`
	// Evicted is how many shapes the nodes dropped to stay within their
	// bounds, summed. Non-zero means this list is a window over the
	// busiest shapes rather than every shape that ran.
	Evicted uint64 `json:"evicted,omitempty"`
	// Nodes is how many nodes answered and NodesAsked how many were
	// asked: a partial answer says so rather than under-reporting
	// silently.
	Nodes      int      `json:"nodes"`
	NodesAsked int      `json:"nodes_asked"`
	Errors     []string `json:"errors,omitempty"`
}

// ClusterStatement is one shape's cluster-wide accounting.
//
// Only the summable figures are summed. There is no p99 of two p99s, so
// the percentiles stay in PerNode, where each belongs to the node that
// measured it.
type ClusterStatement struct {
	Fingerprint string   `json:"fingerprint"`
	Shape       string   `json:"shape"`
	Kind        string   `json:"kind"`
	Tables      []string `json:"tables,omitempty"`
	// Representative is one text this shape was built from, from
	// whichever node reported it last.
	Representative string `json:"representative,omitempty"`

	Count        uint64    `json:"count"`
	Errors       uint64    `json:"errors,omitempty"`
	Retries      uint64    `json:"retries,omitempty"`
	TotalMicros  int64     `json:"total_us"`
	MeanMicros   int64     `json:"mean_us"`
	MaxMicros    int64     `json:"max_us"`
	RowsReturned uint64    `json:"rows_returned"`
	RowsScanned  uint64    `json:"rows_scanned"`
	LastAt       time.Time `json:"last_at"`
	// PerNode carries each node's own row, percentiles included.
	PerNode []pgwire.StatementStat `json:"per_node,omitempty"`
}

// clusterStatementLimit bounds what the document carries after the
// union is re-ranked.
const clusterStatementLimit = 100

func (n *Node) serveStatementsAPI(w http.ResponseWriter, req *http.Request) {
	doc := n.statementsDoc(req)
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

func (n *Node) statementsDoc(req *http.Request) StatementsStatus {
	doc := StatementsStatus{
		Now:        n.clock.Now().WallTime / int64(time.Millisecond),
		NodeID:     int(n.ident.NodeID),
		Statements: []ClusterStatement{},
	}
	merged := map[string]*ClusterStatement{}
	add := func(nodeID int, stats []pgwire.StatementStat) {
		for _, st := range stats {
			m := merged[st.Fingerprint]
			if m == nil {
				m = &ClusterStatement{
					Fingerprint: st.Fingerprint, Shape: st.Shape, Kind: st.Kind,
					Tables: st.Tables, Representative: st.Representative,
				}
				merged[st.Fingerprint] = m
			}
			m.Count += st.Count
			m.Errors += st.Errors
			m.Retries += st.Retries
			m.TotalMicros += st.TotalMicros
			m.RowsReturned += st.RowsReturned
			m.RowsScanned += st.RowsScanned
			if st.MaxMicros > m.MaxMicros {
				m.MaxMicros = st.MaxMicros
			}
			if st.LastAt.After(m.LastAt) {
				m.LastAt = st.LastAt
				if st.Representative != "" {
					m.Representative = st.Representative
				}
			}
			row := st
			row.NodeID = nodeID
			m.PerNode = append(m.PerNode, row)
		}
	}

	// This node's own, then every peer's over the admin RPC.
	local := n.activityStatus()
	add(int(n.ident.NodeID), local.Statements)
	doc.Evicted += local.StatementsEvicted
	doc.Nodes, doc.NodesAsked = 1, 1

	for _, nd := range n.registry.All() {
		if nd.NodeID == n.ident.NodeID {
			continue
		}
		doc.NodesAsked++
		stats, evicted, err := n.peerStatements(req.Context(), nd.NodeID)
		if err != nil {
			// A node that cannot answer is named rather than quietly
			// missing: the totals below are then a partial sum, and the
			// console says which nodes are in them.
			doc.Errors = append(doc.Errors, fmt.Sprintf("n%d: %s", nd.NodeID, err.Error()))
			continue
		}
		doc.Nodes++
		doc.Evicted += evicted
		add(int(nd.NodeID), stats)
	}

	for _, m := range merged {
		if m.Count > 0 {
			m.MeanMicros = m.TotalMicros / int64(m.Count)
		}
		sort.Slice(m.PerNode, func(i, j int) bool { return m.PerNode[i].NodeID < m.PerNode[j].NodeID })
		doc.Statements = append(doc.Statements, *m)
	}
	sort.Slice(doc.Statements, func(i, j int) bool {
		if doc.Statements[i].TotalMicros != doc.Statements[j].TotalMicros {
			return doc.Statements[i].TotalMicros > doc.Statements[j].TotalMicros
		}
		return doc.Statements[i].Fingerprint < doc.Statements[j].Fingerprint
	})
	if len(doc.Statements) > clusterStatementLimit {
		doc.Statements = doc.Statements[:clusterStatementLimit]
	}
	return doc
}

// peerStatements asks one node for its fingerprint accounting, over the
// node-detail RPC the node page already uses.
func (n *Node) peerStatements(ctx context.Context, id base.NodeID) ([]pgwire.StatementStat, uint64, error) {
	addr, err := n.registry.Resolve(id)
	if err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var resp cluster.AdminResponse
	if err := n.trans.Call(ctx, addr, "admin", cluster.AdminRequest{Op: "node-detail"}, &resp); err != nil {
		return nil, 0, err
	}
	if resp.Error != "" {
		return nil, 0, fmt.Errorf("%s", resp.Error)
	}
	var detail NodeDetail
	if err := json.Unmarshal(resp.Status, &detail); err != nil {
		return nil, 0, fmt.Errorf("undecodable detail: %w", err)
	}
	if detail.Activity == nil {
		return nil, 0, nil
	}
	return detail.Activity.Statements, detail.Activity.StatementsEvicted, nil
}

// ExplainStatus is the /api/explain document: the plan for a
// fingerprint's representative statement.
type ExplainStatus struct {
	Fingerprint string `json:"fingerprint"`
	// Statement is the representative text the plan is for, so the
	// reader can see what was explained rather than trusting that it was
	// the right thing.
	Statement string   `json:"statement,omitempty"`
	Plan      []string `json:"plan,omitempty"`
	Error     string   `json:"error,omitempty"`
}

// serveExplainAPI closes the loop from "this shape is expensive" to
// "here is why" (issue #157).
//
// It is read-only in the strongest sense available: the text is run only
// after being parsed and checked to be an EXPLAIN-able statement, it is
// wrapped in EXPLAIN (never ANALYZE, which would execute it), and the
// text comes from this node's own accounting rather than from the
// request — so the endpoint cannot be used to run a statement of the
// caller's choosing.
func (n *Node) serveExplainAPI(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	fp := req.URL.Query().Get("fingerprint")
	doc := ExplainStatus{Fingerprint: fp}
	if fp == "" {
		w.WriteHeader(http.StatusBadRequest)
		doc.Error = "a fingerprint is required: /api/explain?fingerprint=..."
		_ = enc.Encode(doc)
		return
	}
	if n.sqlServer() == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		doc.Error = "this node serves no SQL"
		_ = enc.Encode(doc)
		return
	}
	text := n.sqlServer().Activity().StatementText(fp)
	if text == "" {
		w.WriteHeader(http.StatusNotFound)
		doc.Error = "n" + itoaNode(int(n.ident.NodeID)) +
			" no longer holds a statement for that shape: the accounting is bounded and forgets the least recently run"
		_ = enc.Encode(doc)
		return
	}
	doc.Statement = text
	plan, err := n.explainStatement(req.Context(), text)
	if err != nil {
		doc.Error = err.Error()
		_ = enc.Encode(doc)
		return
	}
	doc.Plan = plan
	_ = enc.Encode(doc)
}

// explainStatement plans one statement without running it.
func (n *Node) explainStatement(ctx context.Context, text string) ([]string, error) {
	// A representative may have been truncated to fit the accounting;
	// an unparseable tail is a "cannot explain", not a failure to hide.
	stmts, err := parser.Parse(strings.TrimSuffix(text, "…"))
	if err != nil {
		return nil, fmt.Errorf("this shape's representative statement cannot be re-parsed (it is stored truncated): %w", err)
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("the representative is %d statements; only a single statement can be explained", len(stmts))
	}
	if _, isExplain := stmts[0].(*parser.Explain); isExplain {
		return nil, fmt.Errorf("the representative is itself an EXPLAIN")
	}
	sess, err := n.systemSession()
	if err != nil {
		return nil, err
	}
	// Analyze stays false: EXPLAIN describes the plan, EXPLAIN ANALYZE
	// runs the statement. A console button must never be the second.
	res, serr := sess.Execute(ctx, &parser.Explain{Stmt: stmts[0], Analyze: false}, nil)
	if serr != nil {
		return nil, serr
	}
	out := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		var parts []string
		for _, d := range row {
			if d.Null {
				continue
			}
			parts = append(parts, d.Text())
		}
		out = append(out, strings.Join(parts, " "))
	}
	return out, nil
}

func itoaNode(n int) string { return fmt.Sprintf("%d", n) }
