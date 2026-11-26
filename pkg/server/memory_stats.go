package server

// TestingMemoryStats reports the sizes of the node's per-range and
// per-table in-memory maps — the store's, the KV client's, the SQL
// catalog's and the node's own (issue #391). A create-and-drop cycle,
// once its tables are reclaimed and their ranges merged away, must
// leave each where it started.
func (n *Node) TestingMemoryStats() map[string]int {
	out := n.store.TestingMemoryStats()
	for k, v := range n.db.TestingMemoryStats() {
		out[k] = v
	}
	if n.cat != nil {
		for k, v := range n.cat.TestingMemoryStats() {
			out[k] = v
		}
	}
	n.loadCooldownMu.Lock()
	out["node.loadcooldown"] = len(n.loadCooldown)
	n.loadCooldownMu.Unlock()
	if p := n.retention; p != nil {
		p.mu.Lock()
		out["node.retention.spans"] = len(p.spans)
		p.mu.Unlock()
	}
	n.tableDescs.mu.Lock()
	out["node.tabledescs"] = len(n.tableDescs.entries)
	n.tableDescs.mu.Unlock()
	n.placement.mu.Lock()
	out["node.placement"] = len(n.placement.policy)
	n.placement.mu.Unlock()
	n.statsRefresh.mu.Lock()
	if f := n.statsRefresh.follow; f != nil {
		if st, ok := f.state.(*statsState); ok {
			out["node.statsrefresh.tables"] = len(st.tables)
			out["node.statsrefresh.collected"] = len(st.collected)
		}
	}
	n.statsRefresh.mu.Unlock()
	return out
}
