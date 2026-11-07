package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// /api/index-usage (issue #208).
//
// The table detail view lists every index on a table and could not say
// whether anything used it. The executor now counts, per (table,
// index), the scans it opens and the rows it reads through each
// (pkg/sql/indexusage.go), and the metrics recorder writes the counts
// into datax_metrics every interval as index.scans{index=table.name}
// and index.rows{…}. This document sums them over the metrics table's
// retention window for one table, across every node that recorded —
// the same table every node writes into, so no fan-out is needed and a
// restart changes nothing the reader sees: the window is the retention,
// and the console names it.
//
// Admin-gated like /api/statements: which indexes a workload uses is
// a fact about the workload, not about the schema.
//
// The words are chosen with care, because someone will drop an index
// on the strength of this column. A count is "scans recorded in the
// last 7 days on the nodes that answered", never "unused": a node whose
// recorder is off, a window shorter than the workload's cycle, or a
// cluster younger than the window all leave a used index at zero.

// IndexUsageStatus is the /api/index-usage document for one table.
type IndexUsageStatus struct {
	NodeID int    `json:"node_id"`
	Table  string `json:"table"`
	// WindowSeconds is the metrics table's retention: the window the
	// counts cover.
	WindowSeconds int64 `json:"window_seconds"`
	// NodesAsked is every node the cluster knows; Nodes are those with
	// at least one sample of a series of this table in the window. A
	// node absent from Nodes recorded nothing — its recorder is off, or
	// it never read the table — and the reader should know the sum is
	// over the rest.
	NodesAsked int   `json:"nodes_asked"`
	Nodes      []int `json:"nodes"`
	// Recording is whether this node's recorder is on; off, the sums
	// come from the other nodes only.
	Recording bool `json:"recording"`
	// Indexes lists the primary index first, then the secondaries in
	// catalog order; an index never read is present with zeros.
	Indexes []IndexUsageRow `json:"indexes"`
	// Error names a series the table could not be read for; the rows
	// are then partial, not absent.
	Error string `json:"error,omitempty"`
}

// IndexUsageRow is one index's sums over the window.
type IndexUsageRow struct {
	Name    string `json:"name"`
	Primary bool   `json:"primary,omitempty"`
	// Scans and Rows are the counters' increase over the window, summed
	// across the nodes that recorded; a node restart inside the window
	// is a counter reset and is handled as one.
	Scans uint64 `json:"scans"`
	Rows  uint64 `json:"rows"`
	// LastUsedUnixMs is the newest recorder interval in which any node's
	// scan count rose; 0 when none did in the window.
	LastUsedUnixMs int64 `json:"last_used_unix_ms,omitempty"`
}

// indexUsageCacheFor bounds how often one table's document is recomputed:
// it reads a window of raw samples per (node, index, series), and the
// table detail polls every few seconds.
const indexUsageCacheFor = 30 * time.Second

type indexUsageCache struct {
	mu   sync.Mutex
	docs map[string]indexUsageCached
}

type indexUsageCached struct {
	at  time.Time
	doc *IndexUsageStatus
}

func (n *Node) serveIndexUsageAPI(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	table := req.URL.Query().Get("table")
	if table == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = enc.Encode(map[string]string{"error": "table: name the table"})
		return
	}
	doc, err := n.indexUsageDoc(req.Context(), table)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		_ = enc.Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = enc.Encode(doc)
}

// indexUsageDoc computes, or serves from the cache, one table's document.
func (n *Node) indexUsageDoc(ctx context.Context, table string) (*IndexUsageStatus, error) {
	n.indexUsage.mu.Lock()
	if n.indexUsage.docs == nil {
		n.indexUsage.docs = map[string]indexUsageCached{}
	}
	if c, ok := n.indexUsage.docs[table]; ok && time.Since(c.at) < indexUsageCacheFor {
		n.indexUsage.mu.Unlock()
		return c.doc, nil
	}
	n.indexUsage.mu.Unlock()

	// The caller's request waits for a catalog scan the way its own
	// /api/schema does, and a table the cache does not know is looked
	// for once more in a fresh scan before it is called absent: the
	// detail view opens on a table just created.
	find := func(sd *SchemaStatus) *SchemaTable {
		if sd == nil {
			return nil
		}
		for i := range sd.Tables {
			if sd.Tables[i].Name == table {
				return &sd.Tables[i]
			}
		}
		return nil
	}
	sd := n.schemaDoc(ctx)
	st := find(sd)
	if st == nil {
		sd = n.rebuildSchema(ctx)
		st = find(sd)
	}
	if sd == nil {
		return nil, fmt.Errorf("the schema is not known yet")
	}
	if st == nil {
		return nil, fmt.Errorf("no table called %q", table)
	}
	window := 7 * 24 * time.Hour
	for _, t := range sd.Tables {
		if t.Name == catalog.MetricsTableName && t.RetentionSeconds > 0 {
			window = time.Duration(t.RetentionSeconds) * time.Second
		}
	}
	now := time.Unix(0, n.clock.Now().WallTime)
	doc := &IndexUsageStatus{
		NodeID:        int(n.ident.NodeID),
		Table:         table,
		WindowSeconds: int64(window.Seconds()),
		Recording:     n.metricsRecordInterval() > 0,
		Nodes:         []int{},
	}
	names := []string{indexUsagePrimary}
	for _, ix := range st.Indexes {
		names = append(names, ix.Name)
	}
	nodes := n.registry.All()
	doc.NodesAsked = len(nodes)
	answered := map[int]bool{}
	for _, name := range names {
		row := IndexUsageRow{Name: name, Primary: name == indexUsagePrimary}
		for _, nd := range nodes {
			id := int64(nd.NodeID)
			scans, err := n.rawSeries(ctx, labelledSeries("index.scans", "index", indexUsageLabel(table, name)), id, now.Add(-window), now)
			if err != nil {
				doc.Error = fmt.Sprintf("n%d: %s", id, err.Error())
				continue
			}
			rows, err := n.rawSeries(ctx, labelledSeries("index.rows", "index", indexUsageLabel(table, name)), id, now.Add(-window), now)
			if err != nil {
				doc.Error = fmt.Sprintf("n%d: %s", id, err.Error())
				continue
			}
			if len(scans) > 0 || len(rows) > 0 {
				answered[int(id)] = true
			}
			inc, last := counterIncrease(scans)
			row.Scans += uint64(inc)
			if last > row.LastUsedUnixMs {
				row.LastUsedUnixMs = last
			}
			rinc, _ := counterIncrease(rows)
			row.Rows += uint64(rinc)
		}
		doc.Indexes = append(doc.Indexes, row)
	}
	for id := range answered {
		doc.Nodes = append(doc.Nodes, id)
	}
	sort.Ints(doc.Nodes)

	n.indexUsage.mu.Lock()
	n.indexUsage.docs[table] = indexUsageCached{at: time.Now(), doc: doc}
	n.indexUsage.mu.Unlock()
	return doc, nil
}

// indexUsagePrimary is the label value the primary index records under.
const indexUsagePrimary = "primary"

// indexUsageLabel is the index label value: the table's name and the
// index's, joined, which is how the series is keyed in datax_metrics
// (index.scans{index=orders.by_customer}).
func indexUsageLabel(table, index string) string { return table + "." + index }

// counterIncrease sums a cumulative counter's increase over its samples
// — the positive deltas, with a fall read as a reset (a node restart:
// the new value is the count since) — and reports the newest sample at
// which the counter rose, in unix milliseconds. The first sample's own
// value is not counted: it is the count before the window, or before the
// series was recorded, and nothing says which.
func counterIncrease(pts []tsample) (inc float64, lastRise int64) {
	for i := 1; i < len(pts); i++ {
		d := pts[i].v - pts[i-1].v
		if d < 0 {
			d = pts[i].v
		}
		if d > 0 {
			inc += d
			lastRise = pts[i].t / int64(time.Millisecond)
		}
	}
	return inc, lastRise
}

// indexUsageRows is the recorder's sample of this node's index counters
// (issue #208): one index.scans and one index.rows row per index the
// node has read through, labelled table.index, with the primary under
// "primary". Resolved through the schema cache, so an index of a table
// the cache does not know yet (just created) waits a tick.
func (n *Node) indexUsageRows(add func(name string, v float64)) {
	// Every node resolves names through its own cache; the leader's is
	// refreshed by the table gauges, the others' only by what asks, so
	// ask (a background rebuild past the cache's age, never a wait).
	n.refreshSchema()
	descs := n.tableDescs()
	for _, u := range sql.IndexUsageSnapshot() {
		d := descs[u.TableID]
		if d == nil || catalog.IsSystemTable(d.Name) {
			continue
		}
		name := ""
		if u.IndexID == d.LivePrimaryIndex() {
			name = indexUsagePrimary
		} else {
			for _, ix := range d.Indexes {
				if ix.ID == u.IndexID {
					name = ix.Name
					break
				}
			}
		}
		if name == "" {
			continue // an index dropped since, or a retired layout's
		}
		add(labelledSeries("index.scans", "index", indexUsageLabel(d.Name, name)), float64(u.Scans))
		add(labelledSeries("index.rows", "index", indexUsageLabel(d.Name, name)), float64(u.Rows))
	}
}
