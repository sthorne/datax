package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// /api/schema — the schema browser behind the dashboard: every table
// with its columns, primary key, indexes, time-series options, grants,
// statistics (row count and age) and footprint (ranges, and the bytes
// this node's replicas hold), plus the users for admins. datax has one
// namespace, so this is a table list rather than a database picker.
//
// The document is assembled node-locally from range 1's catalog and the
// node's own replicas, when somebody asks for it: a request that finds
// it older than schemaCacheFor builds it, concurrent requests share that
// one build, and a node nobody is browsing builds nothing (issue #311 —
// it used to be rebuilt on a timer on every node, because range labels,
// placement and the table gauges were read off its by-products; each of
// those now reads what it needs, see table_cache.go, placement.go and
// table_gauges.go). It is filtered per request: root and admin-role
// members see everything; another user sees the tables it holds a
// privilege on and no user list.

// schemaCacheFor bounds how often the catalog is re-scanned for the
// browser: the console polls every few seconds while the page is open,
// and a table created a moment ago shows within this.
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
	// Default is the column's DEFAULT as SQL text, literal or
	// expression. The table detail view reconstructs the DDL from this
	// document (issue #194), which it cannot do without it.
	Default string `json:"default,omitempty"`
}

// SchemaIndex is one secondary index.
type SchemaIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique,omitempty"`
	// State is "" or "public" for a readable index, "write-only" while a
	// CREATE INDEX backfill is in progress.
	State string `json:"state,omitempty"`
	// Method is the access method of an index that is not an ordinary
	// (btree) one: "gin" for an inverted index.
	Method string `json:"method,omitempty"`
}

// SchemaConstraint is one table constraint beyond the primary key and
// NOT NULL: a CHECK expression, a foreign key, or a named UNIQUE. The
// detail view reconstructs the table's DDL from this document (issue
// #194), and DDL that silently omits a foreign key is worse than none.
type SchemaConstraint struct {
	Name string `json:"name"`
	// Kind is "check", "foreign" or "unique".
	Kind    string   `json:"kind"`
	Columns []string `json:"columns,omitempty"`
	// Expr is a CHECK's expression as SQL text.
	Expr string `json:"expr,omitempty"`
	// RefTable and RefColumns are a foreign key's referenced table and
	// columns; OnDelete and OnUpdate its referential actions ("" =
	// restrict).
	RefTable   string   `json:"ref_table,omitempty"`
	RefColumns []string `json:"ref_columns,omitempty"`
	OnDelete   string   `json:"on_delete,omitempty"`
	OnUpdate   string   `json:"on_update,omitempty"`
	// Validated is false for a constraint added NOT VALID, or one still
	// being validated: new writes are checked, existing rows were not.
	Validated bool `json:"validated"`
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
	// Constraints are the table's CHECK, FOREIGN KEY and named UNIQUE
	// constraints, with their columns and referenced table resolved to
	// names.
	Constraints []SchemaConstraint `json:"constraints,omitempty"`
	// Comment is COMMENT ON TABLE's text ("" = none).
	Comment string `json:"comment,omitempty"`
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
	mu  sync.Mutex
	at  time.Time
	doc *SchemaStatus
	// descs keeps the descriptors the document was built from, for the
	// questions its shape cannot answer on its own (ownership, and so
	// what a caller who is not an admin may see).
	descs map[uint64]*catalog.TableDescriptor
	// building is non-nil while a build is in flight and closed when its
	// result is installed: the requests that arrive meanwhile wait for
	// it instead of scanning the catalog again. started and finished
	// number the builds, so a caller that must see the catalog as it is
	// now (rebuildSchema) can tell a build begun after it asked from one
	// that was already running.
	building          chan struct{}
	started, finished uint64
	// builds counts catalog scans for the document (TestingSchemaBuilds).
	builds atomic.Int64
}

// TestingSchemaBuilds is how many times this node has built the schema
// document since it started.
func (n *Node) TestingSchemaBuilds() int64 { return n.schema.builds.Load() }

// TestingBuildSchemaDoc builds the schema document once under ctx, so a
// test can attribute the build's KV requests to it; it returns the
// number of tables in the document.
func (n *Node) TestingBuildSchemaDoc(ctx context.Context) int {
	return len(n.rebuildSchema(ctx).Tables)
}

func (n *Node) serveSchemaAPI(w http.ResponseWriter, req *http.Request) {
	full := n.schemaDoc(req.Context())
	p := n.clusterPrincipal(req)
	doc := *full
	doc.Now = n.clock.Now().WallTime / int64(time.Millisecond)
	doc.Principal = p
	if !p.Admin {
		// A user sees the tables it may read, and not who else exists.
		//
		// "May read" is privilege resolution, not a direct-grant lookup:
		// a grant to `public`, a grant to a role the user is a member of,
		// ownership (which is not a row in Privileges at all), and the
		// read_all / write_all roles all count. Asking the narrower
		// question made the console disagree with SHOW TABLES about the
		// same user's schema — failing closed, so not a disclosure, but
		// in the common deployment where access is granted through roles
		// most non-admins saw nothing at all (issue #197).
		set, err := catalog.LazyRoleGraph(req.Context(), n.db).Effective(p.User)
		if err != nil {
			// Fail closed and say so, rather than showing a list built
			// from a predicate that did not run.
			doc.Tables, doc.Users = nil, nil
			doc.Error = "cannot resolve roles for " + p.User + ": " + err.Error()
			writeSchemaDoc(w, &doc)
			return
		}
		// And the schema half of the question: a table whose schema's
		// USAGE the user lacks is not listed, as SHOW TABLES does not
		// list it either — both ask catalog.SchemaUsageDenied (issue
		// #259).
		denied, err := n.schemaUsageDenied(req.Context(), set)
		if err != nil {
			doc.Tables, doc.Users = nil, nil
			doc.Error = "cannot resolve schema usage for " + p.User + ": " + err.Error()
			writeSchemaDoc(w, &doc)
			return
		}
		descs := n.schemaDescs()
		visible := make([]SchemaTable, 0, len(full.Tables))
		for _, t := range full.Tables {
			d := descs[t.ID]
			if d == nil || denied[d.DatabaseID] || !catalog.CanSeeTable(set, d) {
				continue
			}
			// The table is visible; its full grantee list is not. Naming
			// every other role granted on a table is the kind of thing
			// /api/security filters rather than gates, and the same
			// applies here: the caller sees its own effective grants.
			t.Privileges = ownGrants(set, d.Privileges)
			visible = append(visible, t)
		}
		doc.Tables = visible
		doc.Users = nil
	}
	writeSchemaDoc(w, &doc)
}

func writeSchemaDoc(w http.ResponseWriter, doc *SchemaStatus) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

// ownGrants narrows a table's grantee map to the roles the caller
// actually holds. The result is a fresh map: the one it filters belongs
// to the cached document every request shares.
func ownGrants(set catalog.RoleSet, privs map[string][]string) map[string][]string {
	var own map[string][]string
	for g, list := range privs {
		if !set.Has(g) {
			continue
		}
		if own == nil {
			own = map[string][]string{}
		}
		own[g] = list
	}
	return own
}

// schemaDescs is the descriptor map the last build of the document
// produced.
func (n *Node) schemaDescs() map[uint64]*catalog.TableDescriptor {
	n.schema.mu.Lock()
	defer n.schema.mu.Unlock()
	return n.schema.descs
}

// viewerDescs is the descriptor of every table, no older than
// schemaCacheFor when the catalog can be read: what decides which tables
// and keys a caller who is not an admin is shown. It waits for a build
// rather than answer from an old one, since a grant revoked an hour ago
// must not show a table for one more poll; resolving such a caller's
// roles already reads the catalog, so this adds no new way to wait.
func (n *Node) viewerDescs(ctx context.Context) map[uint64]*catalog.TableDescriptor {
	n.schemaDoc(ctx)
	return n.schemaDescs()
}

// schemaDoc returns the full document, building it when the cached one
// is older than schemaCacheFor. The build is bounded by
// schemaBuildTimeout and shared by every request waiting for it.
func (n *Node) schemaDoc(ctx context.Context) *SchemaStatus {
	return n.buildSchema(ctx, false)
}

// rebuildSchema returns a document built from a catalog scan that began
// after the call did, for the caller that has just been told of a table
// the cached document does not list.
func (n *Node) rebuildSchema(ctx context.Context) *SchemaStatus {
	return n.buildSchema(ctx, true)
}

func (n *Node) buildSchema(ctx context.Context, fresh bool) *SchemaStatus {
	c := &n.schema
	c.mu.Lock()
	want := c.started + 1 // the first build to begin from here on
	for {
		if c.doc != nil && ((fresh && c.finished >= want) || (!fresh && time.Since(c.at) < schemaCacheFor)) {
			doc := c.doc
			c.mu.Unlock()
			return doc
		}
		if building := c.building; building != nil {
			c.mu.Unlock()
			select {
			case <-building:
			case <-ctx.Done():
				// The caller gave up waiting; the build goes on for the
				// others. It gets the last document, if there is one.
				if doc := n.cachedSchemaDoc(); doc != nil {
					return doc
				}
				return &SchemaStatus{NodeID: int(n.ident.NodeID), Error: "catalog listing unavailable: " + ctx.Err().Error()}
			}
			c.mu.Lock()
			continue
		}
		building := make(chan struct{})
		c.building = building
		c.started++
		seq := c.started
		c.mu.Unlock()

		// Built without the lock, and detached from the caller's
		// cancellation: the requests that joined this build must not lose
		// it because the one that happened to start it hung up. A build
		// that could not read the catalog keeps the previous descriptors,
		// so what a caller may see does not blink out with a partition.
		bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), schemaBuildTimeout)
		doc, descs := n.buildSchemaDoc(bctx)
		cancel()

		c.mu.Lock()
		c.doc, c.at, c.finished = doc, time.Now(), seq
		if doc.Error == "" || c.descs == nil {
			c.descs = descs
		}
		c.building = nil
		close(building)
	}
}

// cachedSchemaDoc returns the last built document without rebuilding
// (nil before the first build).
func (n *Node) cachedSchemaDoc() *SchemaStatus {
	n.schema.mu.Lock()
	defer n.schema.mu.Unlock()
	return n.schema.doc
}

// prettyKey renders a key for the APIs and the dashboard: with the
// table, index and typed values when the key belongs to a table this
// node knows (tableDesc), by shape otherwise.
func (n *Node) prettyKey(k keys.Key) string {
	if id, ok := tableIDOfKey(k); ok {
		if d := n.tableDesc(id); d != nil {
			return rowenc.PrettyKey(d, k)
		}
		// No descriptor: a dropped table, or a reserved one not created
		// yet. The second kind has a name all the same, and the console
		// showed "/table/1099511627777" beside its named siblings (the UI
		// review of #285).
		if name := catalog.SystemTableName(id); name != "" {
			raw := "/table/" + strconv.FormatUint(id, 10)
			if p := keys.Pretty(k); strings.HasPrefix(p, raw) {
				return "/table/" + name + p[len(raw):]
			}
		}
	}
	return keys.Pretty(k)
}

// tableNameOf labels a range with the table its start key belongs to
// ("" for system and meta ranges, or a table this node has not read
// yet).
func (n *Node) tableNameOf(start keys.Key) string {
	id, ok := tableIDOfKey(start)
	if !ok {
		return ""
	}
	if d := n.tableDesc(id); d != nil {
		return d.Name
	}
	return ""
}

// tableIDOfKey decodes the table ID a user-data key belongs to.
func tableIDOfKey(k keys.Key) (uint64, bool) { return keys.TableIDOf(k) }

// columnDefault renders a column's DEFAULT as the SQL that produced it:
// an expression as written, a literal in its literal form.
func columnDefault(c catalog.Column) string {
	if c.DefaultExpr != "" {
		return c.DefaultExpr
	}
	if c.Default == nil {
		return ""
	}
	if c.Default.Null {
		return "NULL"
	}
	switch c.Default.Fam {
	case types.String, types.Bytes, types.Timestamp, types.Date, types.Time, types.IntervalFam, types.Uuid, types.Jsonb, types.Enum:
		return "'" + strings.ReplaceAll(c.Default.Text(), "'", "''") + "'"
	default:
		return c.Default.Text()
	}
}

// schemaConstraint resolves one constraint's column IDs, and a foreign
// key's referenced table and columns, to the names the browser shows.
func schemaConstraint(
	c catalog.Constraint,
	own map[catalog.ColumnID]string,
	tableNames map[uint64]string,
	colNames map[uint64]map[catalog.ColumnID]string,
) SchemaConstraint {
	sc := SchemaConstraint{
		Name: c.Name, Kind: c.Kind, Expr: c.Expr,
		OnDelete: c.OnDelete, OnUpdate: c.OnUpdate, Validated: c.Validated,
	}
	for _, id := range c.Columns {
		sc.Columns = append(sc.Columns, own[id])
	}
	if c.Kind == catalog.ConstraintForeign {
		sc.RefTable = tableNames[c.RefTable]
		ref := colNames[c.RefTable]
		for _, id := range c.RefColumns {
			sc.RefColumns = append(sc.RefColumns, ref[id])
		}
	}
	return sc
}

// buildSchemaDoc scans the catalog once: every descriptor, the
// databases, every table's statistics and the range list, a handful of
// requests whatever the number of tables.
func (n *Node) buildSchemaDoc(ctx context.Context) (*SchemaStatus, map[uint64]*catalog.TableDescriptor) {
	n.schema.builds.Add(1)
	doc := &SchemaStatus{NodeID: int(n.ident.NodeID)}
	names := map[uint64]string{}
	byID := map[uint64]*catalog.TableDescriptor{}

	var descs []*catalog.TableDescriptor
	var stats map[uint64]*catalog.TableStatistics
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
		stats, err = readAllTableStats(ctx, txn)
		return err
	})
	if err != nil {
		doc.Error = "catalog listing unavailable: " + err.Error()
		return doc, byID
	}
	sort.Slice(descs, func(i, j int) bool { return descs[i].Name < descs[j].Name })

	// Column names for every descriptor up front: a foreign key names a
	// table that may sort after the one referencing it, and the detail
	// view needs "REFERENCES orders (id)", not two integers.
	colNames := make(map[uint64]map[catalog.ColumnID]string, len(descs))
	for _, d := range descs {
		m := make(map[catalog.ColumnID]string, len(d.Columns))
		for _, c := range d.Columns {
			m[c.ID] = c.Name
		}
		colNames[d.ID] = m
		names[d.ID] = d.Name
	}

	// Range footprint: every range cluster-wide (from /meta) and this
	// node's replica views.
	ranges, _, rerr := n.clusterRanges(ctx)
	if rerr != nil {
		doc.Error = "range listing unavailable: " + rerr.Error()
	}
	rangeIdx := newRangeIndex(ranges)
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
			Comment: d.Comment,
		}
		byID[d.ID] = d
		colName := map[catalog.ColumnID]string{}
		for _, c := range d.Columns {
			colName[c.ID] = c.Name
			t.Columns = append(t.Columns, SchemaColumn{
				Name: c.Name, Type: c.TypeSQL(), NotNull: c.NotNull, Hidden: c.Hidden,
				Precision: c.Precision, Scale: c.Scale, Default: columnDefault(c),
			})
		}
		for _, id := range d.PrimaryKey {
			t.PrimaryKey = append(t.PrimaryKey, colName[id])
		}
		for _, idx := range d.Indexes {
			si := SchemaIndex{Name: idx.Name, Unique: idx.Unique, State: idx.State}
			if idx.IsInverted() {
				si.Method = vtable.IndexMethod(&idx)
			}
			for _, id := range idx.ColumnIDs {
				si.Columns = append(si.Columns, colName[id])
			}
			t.Indexes = append(t.Indexes, si)
		}
		for _, c := range d.Constraints {
			t.Constraints = append(t.Constraints, schemaConstraint(c, colName, names, colNames))
		}
		if d.IsView() {
			doc.Tables = append(doc.Tables, t) // no rows, no ranges
			continue
		}
		t.Stats = schemaStats(stats[d.ID], now)
		start, end := keys.TableDataSpan(d.ID)
		rangeIdx.overlapping(start, end, func(r kvpb.RangeDescriptor) {
			t.Ranges++
			if rs, ok := local[int64(r.RangeID)]; ok {
				t.LocalReplicas++
				t.LocalBytes += rs.SizeBytes
				if rs.Leader {
					t.LeadersHere++
				}
			}
		})
		doc.Tables = append(doc.Tables, t)
	}

	// Users: the login roles, with admin authority resolved through
	// membership (root is an implicit admin). Insecure mode has no
	// verifier for root; the role still exists.
	if roles, err := catalog.ListRoles(ctx, n.db); err == nil {
		graph := catalog.NewRoleGraph(roles)
		seen := false
		for _, r := range roles {
			if !r.Login && r.Name != catalog.RootRole {
				continue
			}
			set, gerr := graph.Effective(r.Name)
			if gerr != nil {
				continue
			}
			seen = seen || r.Name == catalog.RootRole
			doc.Users = append(doc.Users, SchemaUser{Name: r.Name, Admin: set.IsAdmin()})
		}
		if !seen {
			doc.Users = append(doc.Users, SchemaUser{Name: catalog.RootRole, Admin: true})
		}
	}
	sort.Slice(doc.Users, func(i, j int) bool { return doc.Users[i].Name < doc.Users[j].Name })
	return doc, byID
}
