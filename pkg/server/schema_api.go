package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// /api/schema — the schema browser behind the dashboard: every table
// with its columns, primary key, indexes, time-series options, grants,
// statistics (row count and age) and footprint (ranges, and the bytes
// this node's replicas hold), plus the users for admins. datax has one
// namespace, so this is a table list rather than a database picker.
//
// The document is assembled node-locally from range 1's catalog and the
// node's own replicas, cached for schemaCacheFor so a reload storm does
// not turn into a catalog scan storm, and filtered per request: root and
// admin-role members see everything; another user sees the tables it
// holds a privilege on and no user list.

// schemaCacheFor bounds how often the catalog is re-scanned for the
// browser and the table gauges.
const schemaCacheFor = 5 * time.Second

// schemaBuildTimeout bounds one catalog scan: a node cut off from range
// 1's leader reports the catalog unavailable rather than hanging the
// request that asked.
const schemaBuildTimeout = 5 * time.Second

// statsStaleAfter is when statistics count as stale in the browser (the
// background sampler's refresh threshold).
const statsStaleAfter = 10 * time.Minute

// SchemaColumn is one column of a table.
type SchemaColumn struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	NotNull   bool   `json:"not_null,omitempty"`
	Hidden    bool   `json:"hidden,omitempty"`
	Precision int32  `json:"precision,omitempty"`
	Scale     int32  `json:"scale,omitempty"`
}

// SchemaIndex is one secondary index.
type SchemaIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique,omitempty"`
	// State is "" or "public" for a readable index, "write-only" while a
	// CREATE INDEX backfill is in progress.
	State string `json:"state,omitempty"`
}

// SchemaStats is a table's statistics as the planner sees them.
type SchemaStats struct {
	RowCount    int64 `json:"row_count"`
	CollectedAt int64 `json:"collected_at_unix_ms"`
	AgeSeconds  int64 `json:"age_seconds"`
	// Stale is set past statsStaleAfter, when the sampler would refresh.
	Stale bool `json:"stale"`
}

// SchemaTable is one table in the browser.
type SchemaTable struct {
	ID uint64 `json:"id"`
	// Database is the table's database (the default one for tables that
	// predate the v6 migration).
	Database   string         `json:"database"`
	Name       string         `json:"name"`
	Version    uint64         `json:"version"`
	Columns    []SchemaColumn `json:"columns"`
	PrimaryKey []string       `json:"primary_key"`
	Indexes    []SchemaIndex  `json:"indexes,omitempty"`
	// View marks a view; Definition is its query.
	View       bool   `json:"view,omitempty"`
	Definition string `json:"definition,omitempty"`
	// Time-series options, when the table was created WITH (timeseries).
	Timeseries       bool  `json:"timeseries,omitempty"`
	RetentionSeconds int64 `json:"retention_seconds,omitempty"`
	Shards           int32 `json:"shards,omitempty"`
	// Privileges maps a user to its grants on the table (root and admins
	// bypass grants and are not listed).
	Privileges map[string][]string `json:"privileges,omitempty"`
	// Stats is nil when the table has never been analyzed.
	Stats *SchemaStats `json:"stats,omitempty"`
	// Ranges is how many ranges cover the table's key space cluster-wide;
	// LocalReplicas how many of them this node holds a replica of,
	// LeadersHere how many it leads, and LocalBytes the size of those
	// local replicas (the only bytes a node can measure without a
	// fan-out — an approximation of the table's size that is exact on a
	// node holding every replica).
	Ranges        int   `json:"ranges"`
	LocalReplicas int   `json:"local_replicas"`
	LeadersHere   int   `json:"leaders_here"`
	LocalBytes    int64 `json:"local_bytes"`
}

// SchemaUser is one SQL user (admins only see this list).
type SchemaUser struct {
	Name  string `json:"name"`
	Admin bool   `json:"admin"`
}

// SchemaStatus is the /api/schema document.
type SchemaStatus struct {
	Now       int64            `json:"now_unix_ms"`
	NodeID    int              `json:"node_id"`
	Principal ClusterPrincipal `json:"principal"`
	Tables    []SchemaTable    `json:"tables"`
	// Users is present for admins only.
	Users []SchemaUser `json:"users,omitempty"`
	// Error notes a partial document (the catalog scan failed); what is
	// present is still valid.
	Error string `json:"error,omitempty"`
}

// schemaCache holds the last full (unfiltered) document.
type schemaCache struct {
	mu   sync.Mutex
	at   time.Time
	doc  *SchemaStatus
	name map[uint64]string // table ID → name, for range labeling
	// descs keeps the descriptors too, so range spans render with table,
	// index and typed key values (rowenc.PrettyKey).
	descs map[uint64]*catalog.TableDescriptor
	// refreshing marks a background rebuild in flight (see refreshSchema).
	refreshing bool
}

func (n *Node) serveSchemaAPI(w http.ResponseWriter, req *http.Request) {
	full := n.schemaDoc(req.Context())
	p := n.clusterPrincipal(req)
	doc := *full
	doc.Now = n.clock.Now().WallTime / int64(time.Millisecond)
	doc.Principal = p
	if !p.Admin {
		// A user sees the tables it holds a grant on, and not who else
		// exists.
		var visible []SchemaTable
		for _, t := range full.Tables {
			if len(t.Privileges[p.User]) > 0 {
				visible = append(visible, t)
			}
		}
		doc.Tables = visible
		doc.Users = nil
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

// schemaDoc returns the cached full document, rebuilding it past
// schemaCacheFor. The rebuild is bounded by schemaBuildTimeout; the
// browser's own request waits for it, the other endpoints do not (see
// refreshSchema).
func (n *Node) schemaDoc(ctx context.Context) *SchemaStatus {
	n.schema.mu.Lock()
	if n.schema.doc != nil && time.Since(n.schema.at) < schemaCacheFor {
		doc := n.schema.doc
		n.schema.mu.Unlock()
		return doc
	}
	n.schema.mu.Unlock()
	return n.rebuildSchema(ctx)
}

// rebuildSchema scans the catalog and installs the result. Built without
// the lock: the build reads this node's range views, which label
// themselves through tableNameOf and the same lock. Two concurrent
// rebuilds are a wasted scan, not a hazard. A build that could not read
// the catalog keeps the previous name map, so range labels survive a
// partition instead of blinking out.
func (n *Node) rebuildSchema(ctx context.Context) *SchemaStatus {
	ctx, cancel := context.WithTimeout(ctx, schemaBuildTimeout)
	defer cancel()
	doc, names, descs := n.buildSchemaDoc(ctx)
	n.schema.mu.Lock()
	n.schema.doc, n.schema.at = doc, time.Now()
	if doc.Error == "" || n.schema.name == nil {
		n.schema.name = names
		n.schema.descs = descs
		// Lend the names to the key printer, so this process's log lines
		// name tables too (every node of a cluster knows the same names).
		keys.SetTableNamer(func(id uint64) (string, bool) {
			n.schema.mu.Lock()
			defer n.schema.mu.Unlock()
			name, ok := n.schema.name[id]
			return name, ok
		})
	}
	n.schema.mu.Unlock()
	return doc
}

// refreshSchema keeps the name map fresh for the endpoints that only
// label ranges (/api/cluster, /status): past schemaCacheFor it starts one
// background rebuild and returns at once. Those endpoints must answer
// while the catalog is unreachable — a partitioned node's dashboard is
// exactly when an operator looks — so they never wait on a catalog scan.
func (n *Node) refreshSchema() {
	n.schema.mu.Lock()
	stale := n.schema.doc == nil || time.Since(n.schema.at) >= schemaCacheFor
	if !stale || n.schema.refreshing {
		n.schema.mu.Unlock()
		return
	}
	n.schema.refreshing = true
	n.schema.mu.Unlock()
	if err := n.stopper.RunWorker(func(ctx context.Context) {
		defer func() {
			n.schema.mu.Lock()
			n.schema.refreshing = false
			n.schema.mu.Unlock()
		}()
		n.rebuildSchema(ctx)
	}); err != nil {
		n.schema.mu.Lock()
		n.schema.refreshing = false
		n.schema.mu.Unlock()
	}
}

// cachedSchemaDoc returns the last built document without rebuilding
// (nil before the first build); pair with refreshSchema.
func (n *Node) cachedSchemaDoc() *SchemaStatus {
	n.schema.mu.Lock()
	defer n.schema.mu.Unlock()
	return n.schema.doc
}

// tableNameOf labels a range with the table its start key belongs to
// ("" for system and meta ranges, or before the first catalog scan).
// prettyKey renders a key for the APIs and the dashboard: with the
// table, index and typed values when the key belongs to a table this
// node's schema cache knows, by shape otherwise.
func (n *Node) prettyKey(k keys.Key) string {
	if id, ok := tableIDOfKey(k); ok {
		n.schema.mu.Lock()
		d := n.schema.descs[id]
		n.schema.mu.Unlock()
		if d != nil {
			return rowenc.PrettyKey(d, k)
		}
	}
	return keys.Pretty(k)
}

// tableNames is the current table ID → name map, rebuilt now so a table
// created a moment ago is named too, for admin responses that print
// spans (admin operations are rare; the catalog scan is cheap).
func (n *Node) tableNames(ctx context.Context) map[uint64]string {
	n.rebuildSchema(ctx)
	n.schema.mu.Lock()
	defer n.schema.mu.Unlock()
	out := make(map[uint64]string, len(n.schema.name))
	for id, name := range n.schema.name {
		out[id] = name
	}
	return out
}

func (n *Node) tableNameOf(start keys.Key) string {
	id, ok := tableIDOfKey(start)
	if !ok {
		return ""
	}
	n.schema.mu.Lock()
	defer n.schema.mu.Unlock()
	return n.schema.name[id]
}

// tableIDOfKey decodes the table ID a user-data key belongs to.
func tableIDOfKey(k keys.Key) (uint64, bool) {
	p := keys.TablePrefix
	if len(k) <= len(p) || string(k[:len(p)]) != string(p) {
		return 0, false
	}
	_, id, err := encoding.DecodeUint64(k[len(p):])
	if err != nil {
		return 0, false
	}
	return id, true
}

func (n *Node) buildSchemaDoc(ctx context.Context) (*SchemaStatus, map[uint64]string, map[uint64]*catalog.TableDescriptor) {
	doc := &SchemaStatus{NodeID: int(n.ident.NodeID)}
	names := map[uint64]string{}
	byID := map[uint64]*catalog.TableDescriptor{}

	var descs []*catalog.TableDescriptor
	dbNames := map[uint64]string{0: catalog.DefaultDatabase}
	err := n.db.RunTxn(ctx, "schema-api", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		descs, err = catalog.NewAccessor().List(ctx, txn)
		if err != nil {
			return err
		}
		dbs, err := catalog.ListDatabases(ctx, txn)
		if err != nil {
			return err
		}
		for _, db := range dbs {
			dbNames[db.ID] = db.Name
		}
		return nil
	})
	if err != nil {
		doc.Error = "catalog listing unavailable: " + err.Error()
		return doc, names, byID
	}
	sort.Slice(descs, func(i, j int) bool { return descs[i].Name < descs[j].Name })

	// Range footprint: every range cluster-wide (from /meta) and this
	// node's replica views.
	ranges, _, rerr := n.clusterRanges(ctx)
	if rerr != nil {
		doc.Error = "range listing unavailable: " + rerr.Error()
	}
	local := map[int64]RangeStatus{}
	for _, rs := range n.rangeStatuses() {
		local[rs.RangeID] = rs
	}

	now := n.clock.Now().WallTime
	for _, d := range descs {
		t := SchemaTable{
			ID: d.ID, Name: d.Name, Version: d.Version,
			Database:   dbNames[d.DatabaseID],
			Timeseries: d.Timeseries, RetentionSeconds: d.RetentionSeconds, Shards: d.ShardBuckets,
			Privileges: d.Privileges,
			View:       d.IsView(), Definition: d.ViewQuery,
		}
		names[d.ID] = d.Name
		byID[d.ID] = d
		colName := map[catalog.ColumnID]string{}
		for _, c := range d.Columns {
			colName[c.ID] = c.Name
			t.Columns = append(t.Columns, SchemaColumn{
				Name: c.Name, Type: c.Type.String(), NotNull: c.NotNull, Hidden: c.Hidden,
				Precision: c.Precision, Scale: c.Scale,
			})
		}
		for _, id := range d.PrimaryKey {
			t.PrimaryKey = append(t.PrimaryKey, colName[id])
		}
		for _, idx := range d.Indexes {
			si := SchemaIndex{Name: idx.Name, Unique: idx.Unique, State: idx.State}
			for _, id := range idx.ColumnIDs {
				si.Columns = append(si.Columns, colName[id])
			}
			t.Indexes = append(t.Indexes, si)
		}
		if d.IsView() {
			doc.Tables = append(doc.Tables, t) // no rows, no ranges
			continue
		}
		if raw, err := n.db.Get(ctx, keys.TableStatsKey(d.ID)); err == nil && raw != nil {
			var st catalog.TableStatistics
			if json.Unmarshal(raw, &st) == nil {
				age := (now - st.CollectedAt) / int64(time.Second)
				t.Stats = &SchemaStats{
					RowCount: st.RowCount, CollectedAt: st.CollectedAt / int64(time.Millisecond),
					AgeSeconds: age, Stale: time.Duration(now-st.CollectedAt) > statsStaleAfter,
				}
			}
		}
		start, end := keys.TableDataSpan(d.ID)
		for _, r := range ranges {
			if r.StartKey.Compare(end) >= 0 || r.EndKey.Compare(start) <= 0 {
				continue
			}
			t.Ranges++
			if rs, ok := local[int64(r.RangeID)]; ok {
				t.LocalReplicas++
				t.LocalBytes += rs.SizeBytes
				if rs.Leader {
					t.LeadersHere++
				}
			}
		}
		metrics.TableRanges.WithLabelValues(d.Name).Set(float64(t.Ranges))
		if t.Stats != nil {
			metrics.TableRows.WithLabelValues(d.Name).Set(float64(t.Stats.RowCount))
			metrics.TableStatsAge.WithLabelValues(d.Name).Set(float64(t.Stats.AgeSeconds))
		}
		doc.Tables = append(doc.Tables, t)
	}

	// Users: the SCRAM records and the admin-role markers; root is an
	// implicit admin with no marker.
	admins := map[string]bool{"root": true}
	if aStart, aEnd := keys.AdminUserSpan(); true {
		if rows, err := n.db.Scan(ctx, aStart, aEnd, 0); err == nil {
			for _, kv := range rows {
				if name, ok := nameOfKey(kv.Key, aStart); ok {
					admins[name] = true
				}
			}
		}
	}
	seen := map[string]bool{}
	if uStart, uEnd := keys.UserSpan(); true {
		if rows, err := n.db.Scan(ctx, uStart, uEnd, 0); err == nil {
			for _, kv := range rows {
				if name, ok := nameOfKey(kv.Key, uStart); ok {
					seen[name] = true
					doc.Users = append(doc.Users, SchemaUser{Name: name, Admin: admins[name]})
				}
			}
		}
	}
	if !seen["root"] {
		// Insecure mode (no verifier seeded), or a secure cluster before
		// the seed lands: root exists regardless.
		doc.Users = append(doc.Users, SchemaUser{Name: "root", Admin: true})
	}
	sort.Slice(doc.Users, func(i, j int) bool { return doc.Users[i].Name < doc.Users[j].Name })
	return doc, names, byID
}

// nameOfKey decodes the user name from a users or admins record key.
func nameOfKey(k keys.Key, prefix keys.Key) (string, bool) {
	if len(k) <= len(prefix) {
		return "", false
	}
	_, name, err := encoding.DecodeString(k[len(prefix):])
	if err != nil {
		return "", false
	}
	return name, true
}
