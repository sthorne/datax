package sql

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/placement"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Cluster topology over SQL (issue #405): SHOW NODES, SHOW RANGES FROM
// DATABASE, and SHOW PLACEMENT's compliance columns. The answers come
// from the node the session is connected to: its registry of nodes and
// its listing of the range descriptors, the same sources the HTTP
// status API reads, so none of this needs that API enabled.

// Node states, as SHOW NODES reports them.
const (
	NodeLive         = "live"
	NodeUnresponsive = "unresponsive"
	NodeDead         = "dead"
)

// ClusterNode is one node of the registry.
type ClusterNode struct {
	NodeID   base.NodeID
	Address  string
	Locality base.Locality
	// State is NodeLive, NodeUnresponsive (heartbeat late, not yet
	// declared dead) or NodeDead.
	State         string
	Draining      bool
	ShuttingDown  bool
	LastHeartbeat time.Time
}

// ClusterInfo is the node's view of the cluster, set by the server.
type ClusterInfo interface {
	// Nodes lists every node in the registry, dead ones included until
	// they leave it.
	Nodes(ctx context.Context) ([]ClusterNode, error)
	// Ranges lists every range descriptor. It fails rather than answer
	// from a stale listing: a compliance answer must be current.
	Ranges(ctx context.Context) ([]kvpb.RangeDescriptor, error)
}

// Range statuses, as SHOW RANGES reports them; the order is the
// precedence when several apply.
const (
	rangeUnsatisfiable   = "unsatisfiable"
	rangeUnattributed    = "unattributed"
	rangeUnknownLocality = "unknown-locality"
	rangeMisplaced       = "misplaced"
	rangeOK              = "ok"
)

// rangeStatus is one range of a database and its standing against the
// database's policy.
type rangeStatus struct {
	desc kvpb.RangeDescriptor
	// table is the one table the range lies wholly inside; nil when it
	// is shared (unattributed).
	table     *catalog.TableDescriptor
	status    string
	misplaced []base.NodeID
	unknown   []base.NodeID
}

// placementReport is a database's compliance.
type placementReport struct {
	ranges                                 []rangeStatus
	misplaced, unsatisfiable, unattributed int
	unknownReplicas                        int
	satisfied                              bool
	nodes                                  map[base.NodeID]placement.NodeState
	nodeLocality                           map[base.NodeID]base.Locality
	constrained                            bool
	eligible                               int
}

// errClusterUnavailable is returned when the session has no cluster
// view (an internal session).
func errClusterUnavailable(what string) error {
	return newErrf(CodeFeatureNotSupported, "%s is not available on this session: it needs a node's SQL server", what)
}

// nodeStates projects the registry onto what compliance needs.
func nodeStates(nodes []ClusterNode) map[base.NodeID]placement.NodeState {
	out := make(map[base.NodeID]placement.NodeState, len(nodes))
	for _, n := range nodes {
		out[n.NodeID] = placement.NodeState{Locality: n.Locality, Usable: n.State == NodeLive && !n.Draining}
	}
	return out
}

// dataSpan is one table's key span.
type dataSpan struct {
	lo, hi keys.Key
	table  *catalog.TableDescriptor
}

// assessPlacement finds every range that holds any of db's table data
// and judges it against db's policy. A range is the database's if it
// overlaps any of its tables' key spans: it is attributed when it lies
// wholly inside one table's span (the only ranges the allocator applies
// a policy to; see pkg/server's placementFor), unattributed otherwise.
// An unattributed range is governed by no policy, so its data may sit
// anywhere: it counts against compliance.
func (s *Session) assessPlacement(ctx context.Context, txn *kvclient.Txn, db *catalog.DatabaseDescriptor) (*placementReport, error) {
	if s.Cluster == nil {
		return nil, errClusterUnavailable("placement compliance")
	}
	tables, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	var spans []dataSpan
	for _, t := range tables {
		if t.Virtual != "" || t.IsView() {
			continue // no data of its own
		}
		lo, hi := keys.TableDataSpan(t.ID)
		spans = append(spans, dataSpan{lo: lo, hi: hi, table: t})
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].lo.Compare(spans[j].lo) < 0 })
	nodes, err := s.Cluster.Nodes(ctx)
	if err != nil {
		return nil, err
	}
	descs, err := s.Cluster.Ranges(ctx)
	if err != nil {
		return nil, err
	}
	return judgePlacement(db.Placement, spans, descs, nodes), nil
}

// judgePlacement judges every range that overlaps one of spans (sorted
// by start key, non-overlapping) against policy.
func judgePlacement(policy base.PlacementPolicy, spans []dataSpan, descs []kvpb.RangeDescriptor, nodes []ClusterNode) *placementReport {
	rep := &placementReport{nodes: nodeStates(nodes), constrained: len(policy.Constraints) > 0}
	rep.nodeLocality = placement.Localities(rep.nodes)
	rep.eligible = placement.Eligible(policy, rep.nodes)
	unsat := placement.Unsatisfiable(policy, rep.eligible)
	for _, d := range descs {
		var hit []*catalog.TableDescriptor
		inside := false
		for _, sp := range spans {
			if len(d.EndKey) > 0 && d.EndKey.Compare(sp.lo) <= 0 {
				break // spans are sorted: the rest start later still
			}
			if d.StartKey.Compare(sp.hi) >= 0 {
				continue
			}
			hit = append(hit, sp.table)
			if d.StartKey.Compare(sp.lo) >= 0 && len(d.EndKey) > 0 && d.EndKey.Compare(sp.hi) <= 0 {
				inside = true
			}
		}
		if len(hit) == 0 {
			continue
		}
		rs := rangeStatus{desc: d}
		if len(hit) == 1 && inside {
			rs.table = hit[0]
		}
		var ids []base.NodeID
		for _, r := range d.Replicas {
			ids = append(ids, r.NodeID)
		}
		if rep.constrained {
			rs.misplaced = placement.Misplaced(policy, ids, rep.nodeLocality)
			rs.unknown = placement.UnknownLocality(ids, rep.nodes)
		}
		switch {
		case !rep.constrained:
			// Any node will do: nothing can be out of place. A shared
			// range is still reported, for what it is.
			rs.status = rangeOK
			if rs.table == nil {
				rs.status = rangeUnattributed
			}
		case unsat:
			rs.status = rangeUnsatisfiable
		case rs.table == nil:
			rs.status = rangeUnattributed
		case len(rs.unknown) > 0:
			rs.status = rangeUnknownLocality
		case len(rs.misplaced) > 0:
			rs.status = rangeMisplaced
		default:
			rs.status = rangeOK
		}
		if len(rs.misplaced) > 0 {
			rep.misplaced++
		}
		if rep.constrained && unsat {
			rep.unsatisfiable++
		}
		if rs.table == nil {
			rep.unattributed++
		}
		rep.unknownReplicas += len(rs.unknown)
		rep.ranges = append(rep.ranges, rs)
	}
	rep.satisfied = !rep.constrained ||
		(rep.misplaced == 0 && rep.unsatisfiable == 0 && rep.unattributed == 0 && rep.unknownReplicas == 0 && !unsat)
	return rep
}

// execShowRanges is SHOW RANGES [FROM DATABASE name]: one row per range
// holding the database's table data. The caller must be able to
// connect to the database.
func (s *Session) execShowRanges(ctx context.Context, txn *kvclient.Txn, t *parser.ShowRanges) (*Result, error) {
	name := t.Database
	if name == "" {
		name = s.database
	}
	if name == "" {
		name = catalog.DefaultDatabase
	}
	db, err := catalog.LookupDatabase(ctx, txn, name)
	if err != nil {
		return nil, ToSQLError(err)
	}
	ok, err := s.databasePrivAllowed(ctx, txn, db, catalog.PrivConnect)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, newErrf(CodeInsufficientPriv, "permission denied for database %q: SHOW RANGES needs CONNECT", db.Name)
	}
	rep, err := s.assessPlacement(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: showRangesColumns()}
	for _, rs := range rep.ranges {
		// A boundary key can carry a row's primary key, and renders the
		// table's name: both only for a role that may see the table.
		// Anyone else, and any shared range, gets the key's structure
		// alone (keys.PrettyPrefix, as the status API serves it; issue
		// #213).
		table, pretty := types.DNull, keys.PrettyPrefix
		if rs.table != nil && canSeeTable(set, rs.table) {
			table, pretty = types.NewString(rs.table.Name), keys.Pretty
		}
		var ids, locs []types.Datum
		for _, r := range rs.desc.Replicas {
			ids = append(ids, types.NewInt(int64(r.NodeID)))
			if l, ok := rep.nodeLocality[r.NodeID]; ok {
				locs = append(locs, types.NewString(l.String()))
			} else {
				locs = append(locs, types.DNull)
			}
		}
		res.Rows = append(res.Rows, []types.Datum{
			types.NewInt(int64(rs.desc.RangeID)),
			types.NewString(pretty(rs.desc.StartKey)),
			types.NewString(pretty(rs.desc.EndKey)),
			table,
			types.NewArray(types.Int, ids),
			types.NewArray(types.String, locs),
			types.NewString(rs.status),
		})
	}
	res.Tag = fmt.Sprintf("SHOW RANGES %d", len(res.Rows))
	return res, nil
}

// canViewCluster reports whether the actor may list the cluster's
// nodes: an admin, or a role with the VIEWCLUSTER attribute — its own,
// not inherited, as CREATEDB.
func (s *Session) canViewCluster(ctx context.Context, txn *kvclient.Txn) (bool, error) {
	if ok, err := s.isAdmin(ctx, txn); err != nil || ok {
		return ok, err
	}
	r, err := catalog.LookupRole(ctx, txn, s.actor())
	if err != nil || r == nil {
		return false, err
	}
	return r.ViewCluster, nil
}

// execShowNodes is SHOW NODES: every node in the registry, for the
// admin role and roles with VIEWCLUSTER. It shows the cluster's whole
// layout, which an ordinary role has no need of.
func (s *Session) execShowNodes(ctx context.Context, txn *kvclient.Txn) (*Result, error) {
	ok, err := s.canViewCluster(ctx, txn)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, newErrf(CodeInsufficientPriv, "permission denied: SHOW NODES needs the admin role or the VIEWCLUSTER attribute")
	}
	if s.Cluster == nil {
		return nil, errClusterUnavailable("SHOW NODES")
	}
	nodes, err := s.Cluster.Nodes(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	res := &Result{Columns: showNodesColumns()}
	for _, n := range nodes {
		hb := types.DNull
		if !n.LastHeartbeat.IsZero() {
			hb = types.NewTimestamp(n.LastHeartbeat.UnixNano())
		}
		res.Rows = append(res.Rows, []types.Datum{
			types.NewInt(int64(n.NodeID)),
			types.NewString(n.Address),
			types.NewString(n.Locality.String()),
			types.NewString(n.State),
			types.NewBool(n.Draining),
			types.NewBool(n.ShuttingDown),
			hb,
		})
	}
	res.Tag = fmt.Sprintf("SHOW NODES %d", len(res.Rows))
	return res, nil
}

// placementComplianceRow renders SHOW PLACEMENT's compliance columns:
// all NULL when the answer is unavailable (an internal session, or the
// range listing failed), never a guess. A start-up check reading
// satisfied must treat anything but true as a failure.
func (s *Session) placementComplianceRow(ctx context.Context, txn *kvclient.Txn, db *catalog.DatabaseDescriptor) []types.Datum {
	rep, err := s.assessPlacement(ctx, txn, db)
	if err != nil {
		return []types.Datum{types.DNull, types.DNull, types.DNull, types.DNull, types.DNull, types.DNull}
	}
	return []types.Datum{
		types.NewBool(rep.satisfied),
		types.NewInt(int64(len(rep.ranges))),
		types.NewInt(int64(rep.misplaced)),
		types.NewInt(int64(rep.unsatisfiable)),
		types.NewInt(int64(rep.unattributed)),
		types.NewInt(int64(rep.unknownReplicas)),
	}
}
