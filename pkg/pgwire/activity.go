package pgwire

import (
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sthorne/datax/pkg/kvpb"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// Activity is what the SQL server knows about its clients: every
// connection's state (idle, running a statement, or idle inside an open
// transaction — the state that holds write intents against everyone
// else), statement counts by kind, statement latency, serialization
// failures, and a ring of the slowest recent statements. The dashboard's
// SQL section, /status, the heartbeat summary and /metrics all read it;
// before it the server spawned a goroutine per connection and tracked
// nothing.
type Activity struct {
	slowThreshold time.Duration

	mu    sync.Mutex
	conns map[*conn]*connActivity
	// active and idleTxn count the connections in those states (kept by
	// setState; gauges publishes them).
	active, idleTxn int
	// counts by statement kind, cumulative since start.
	counts    map[string]uint64
	serFails  uint64
	copyRows  uint64
	latencies latencyRing
	slow      []SlowStatement // newest last, at most slowRingSize
	// Serialization failures attributed to what produced them (issue
	// #154): 40001s by statement shape and by user, cumulative since
	// the node started. retryOther counts the failures whose shape
	// arrived after retryShapeMax distinct ones were already known.
	retryShapes   map[string]*retryShape
	retryOther    uint64
	retriesByUser map[string]uint64
}

// retryShape is one statement shape's running count of 40001s.
type retryShape struct {
	count  uint64
	lastAt time.Time
	users  map[string]uint64
}

// connActivity is one connection's live state.
type connActivity struct {
	pid    int32
	user   string
	db     string
	app    string
	remote string
	state  string // idle | active | idle_in_txn
	since  time.Time
	opened time.Time
	// txnSince is when the open transaction block began (zero outside).
	txnSince time.Time
	stmt     string // the statement in flight (truncated), while active
	last     string // the last statement (pg_stat_activity's query when idle)
	kind     string
}

const (
	stateIdle      = "idle"
	stateActive    = "active"
	stateIdleInTxn = "idle_in_txn"

	// slowRingSize bounds the slow-statement ring; stmtTextLimit the
	// text kept per statement (it can carry data, hence admin-only).
	slowRingSize  = 50
	stmtTextLimit = 200
	// latencyRingSize bounds the durations kept for the percentiles.
	latencyRingSize = 1024
	// retryShapeMax bounds the distinct statement shapes counted for the
	// retry hot list. Past it, further shapes are counted in one
	// overflow total rather than growing the map without limit — a
	// console panel is not worth an unbounded allocation on the
	// statement path.
	retryShapeMax = 200
	// DefaultSlowStatementThreshold is the duration past which a
	// statement is recorded in the slow ring.
	DefaultSlowStatementThreshold = 500 * time.Millisecond
)

// SlowStatement is one recorded statement over the threshold.
type SlowStatement struct {
	At       time.Time `json:"at"`
	User     string    `json:"user"`
	Kind     string    `json:"kind"`
	Text     string    `json:"text"`
	Duration int64     `json:"duration_us"`
	Rows     int       `json:"rows"`
	Error    string    `json:"error,omitempty"`
	Retry    bool      `json:"retry,omitempty"` // ended in a 40001
}

// ActiveStatement is one statement in flight right now.
type ActiveStatement struct {
	User    string `json:"user"`
	Remote  string `json:"remote"`
	Kind    string `json:"kind"`
	Text    string `json:"text"`
	Elapsed int64  `json:"elapsed_us"`
}

// ConnectionInfo is one connection for the admin view.
type ConnectionInfo struct {
	User   string `json:"user"`
	Remote string `json:"remote"`
	State  string `json:"state"`
	Since  int64  `json:"since_ms"` // how long in this state
	// Where it came from, for telling one application apart from
	// another when contention is usually one of them (issue #154).
	Database    string `json:"database,omitempty"`
	Application string `json:"application,omitempty"`
}

// IdleTxn is one connection sitting inside an open transaction: the
// state whose write intents block every other writer to those keys.
// A duration alone does not say who to talk to, so this carries the
// user, the client, the application, and the last statement the
// session ran (issue #154). Statement text, so admin-gated.
type IdleTxn struct {
	PID         int32  `json:"pid"`
	User        string `json:"user"`
	Remote      string `json:"remote"`
	Database    string `json:"database,omitempty"`
	Application string `json:"application,omitempty"`
	// IdleMillis is how long it has been idle in this state; TxnMillis
	// how long the whole transaction block has been open. They differ
	// when the session did some work and then stopped.
	IdleMillis int64  `json:"idle_ms"`
	TxnMillis  int64  `json:"txn_ms,omitempty"`
	Last       string `json:"last,omitempty"`
}

// RetryShape is one statement shape's share of this node's
// serialization failures, cumulative since the node started.
type RetryShape struct {
	Shape  string            `json:"shape"`
	Count  uint64            `json:"count"`
	LastAt time.Time         `json:"last_at"`
	Users  map[string]uint64 `json:"users,omitempty"`
}

func newActivity(slow time.Duration) *Activity {
	if slow <= 0 {
		slow = DefaultSlowStatementThreshold
	}
	return &Activity{slowThreshold: slow, conns: make(map[*conn]*connActivity), counts: make(map[string]uint64),
		retryShapes: make(map[string]*retryShape), retriesByUser: make(map[string]uint64)}
}

func (a *Activity) connOpened(c *conn, remote string, pid int32) {
	a.mu.Lock()
	now := time.Now()
	a.conns[c] = &connActivity{pid: pid, remote: remote, state: stateIdle, since: now, opened: now}
	a.mu.Unlock()
	a.gauges()
}

// setSession records the connection's current database and
// application_name after a statement.
func (a *Activity) setSession(c *conn, db, app string) {
	a.mu.Lock()
	if ca, ok := a.conns[c]; ok {
		ca.db, ca.app = db, app
	}
	a.mu.Unlock()
}

// Sessions lists this node's sessions for SHOW SESSIONS and
// pg_stat_activity.
func (a *Activity) Sessions() []sql.SessionInfo {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]sql.SessionInfo, 0, len(a.conns))
	for _, ca := range a.conns {
		si := sql.SessionInfo{PID: ca.pid, User: ca.user, Database: ca.db, Application: ca.app, ClientAddr: ca.remote, BackendStart: ca.opened, XactStart: ca.txnSince}
		switch ca.state {
		case stateActive:
			si.State, si.Query, si.QueryStart = "active", ca.stmt, ca.since
		case stateIdleInTxn:
			si.State, si.Query = "idle in transaction", ca.last
		default:
			si.State, si.Query = "idle", ca.last
		}
		out = append(out, si)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PID < out[j].PID })
	return out
}

func (a *Activity) connClosed(c *conn) {
	a.mu.Lock()
	if ca, ok := a.conns[c]; ok {
		a.countState(ca.state, -1)
		delete(a.conns, c)
	}
	a.mu.Unlock()
	a.gauges()
}

func (a *Activity) setUser(c *conn, user string) {
	a.mu.Lock()
	if ca, ok := a.conns[c]; ok {
		ca.user = user
	}
	a.mu.Unlock()
}

// statementKind classifies a parsed statement for counting.
func statementKind(stmt parser.Statement) string {
	switch stmt.(type) {
	case *parser.Select, *parser.Explain, *parser.ShowTables, *parser.ShowStats, *parser.Show, *parser.ShowDatabases, *parser.ShowSequences, *parser.ShowFunctions, *parser.ShowPlacement:
		return "select"
	case *parser.Insert:
		return "insert"
	case *parser.Update:
		return "update"
	case *parser.Delete:
		return "delete"
	case *parser.CopyFrom:
		return "copy"
	case *parser.Begin, *parser.Commit, *parser.Rollback, *parser.Savepoint, *parser.ReleaseSavepoint, *parser.RollbackToSavepoint:
		return "txn"
	case *parser.CreateTable, *parser.CreateIndex, *parser.DropTable, *parser.AlterTable, *parser.CreateRole, *parser.DropRole, *parser.GrantRevoke, *parser.AlterOwner, *parser.ReassignOwned, *parser.DropOwned, *parser.AlterDefaultPrivileges, *parser.Analyze,
		*parser.CreateSequence, *parser.AlterSequence, *parser.DropSequence, *parser.CreateDatabase, *parser.DropDatabase, *parser.AlterDatabase:
		return "ddl"
	default:
		return "other"
	}
}

func truncateStmt(text string) string {
	if len(text) > stmtTextLimit {
		return text[:stmtTextLimit] + "…"
	}
	return text
}

// begin marks c as running stmt; the returned token ends it.
func (a *Activity) begin(c *conn, stmt parser.Statement, text string) *stmtToken {
	kind := statementKind(stmt)
	now := time.Now()
	a.mu.Lock()
	if ca, ok := a.conns[c]; ok {
		a.setState(ca, stateActive)
		ca.since, ca.stmt, ca.kind = now, truncateStmt(text), kind
	}
	a.mu.Unlock()
	a.gauges()
	return &stmtToken{a: a, c: c, kind: kind, text: text, start: now}
}

type stmtToken struct {
	a     *Activity
	c     *conn
	kind  string
	text  string
	start time.Time
}

// end records the statement's outcome and the connection's new state.
func (t *stmtToken) end(rows int64, serr *sql.Error, inTxn bool) {
	d := time.Since(t.start)
	a := t.a
	a.mu.Lock()
	if ca, ok := a.conns[t.c]; ok {
		ca.last, ca.stmt, ca.kind = ca.stmt, "", ""
		ca.since = time.Now()
		if inTxn {
			if ca.state != stateIdleInTxn && ca.txnSince.IsZero() {
				ca.txnSince = t.start
			}
			a.setState(ca, stateIdleInTxn)
		} else {
			a.setState(ca, stateIdle)
			ca.txnSince = time.Time{}
		}
	}
	a.counts[t.kind]++
	a.latencies.add(d)
	retry := serr != nil && serr.Code == sql.CodeSerializationFailure
	if retry {
		a.serFails++
		a.recordRetryLocked(t)
	}
	if d >= a.slowThreshold {
		ss := SlowStatement{At: t.start, Kind: t.kind, Text: truncateStmt(t.text), Duration: d.Microseconds(), Rows: int(rows), Retry: retry}
		if ca, ok := a.conns[t.c]; ok {
			ss.User = ca.user
		}
		if serr != nil {
			ss.Error = "[" + serr.Code + "] " + serr.Msg
		}
		a.slow = append(a.slow, ss)
		if len(a.slow) > slowRingSize {
			a.slow = a.slow[len(a.slow)-slowRingSize:]
		}
	}
	a.mu.Unlock()
	metrics.SQLStatements.WithLabelValues(t.kind).Inc()
	metrics.SQLStatementLatency.Observe(d.Seconds())
	if retry {
		metrics.SQLSerializationFailures.Inc()
	}
	a.gauges()
}

func (a *Activity) copied(rows int64) {
	a.mu.Lock()
	a.copyRows += uint64(rows)
	a.mu.Unlock()
	metrics.SQLCopyRows.Add(float64(rows))
}

// gauges refreshes the connection-state gauges.
// gauges publishes the connection counts. They are kept as the
// connections change state (setState) rather than counted here: this
// runs at every statement's start and end, and a walk of every
// connection under the lock was 3 % of a busy gateway's CPU.
func (a *Activity) gauges() {
	a.mu.Lock()
	open, active, idleTxn := len(a.conns), a.active, a.idleTxn
	a.mu.Unlock()
	connGauges.open.Set(float64(open))
	connGauges.active.Set(float64(active))
	connGauges.idleTxn.Set(float64(idleTxn))
}

// connGauges are the connection gauges' label handles, resolved once
// (WithLabelValues hashes the label per call).
var connGauges = struct{ open, active, idleTxn prometheus.Gauge }{
	open:    metrics.SQLConnections.WithLabelValues("open"),
	active:  metrics.SQLConnections.WithLabelValues("active"),
	idleTxn: metrics.SQLConnections.WithLabelValues("idle_in_txn"),
}

// setState moves a connection to state, keeping the counts (under a.mu).
func (a *Activity) setState(ca *connActivity, state string) {
	a.countState(ca.state, -1)
	ca.state = state
	a.countState(state, 1)
}

func (a *Activity) countState(state string, d int) {
	switch state {
	case stateActive:
		a.active += d
	case stateIdleInTxn:
		a.idleTxn += d
	}
}

// SlowThreshold is the duration past which statements are recorded.
func (a *Activity) SlowThreshold() time.Duration { return a.slowThreshold }

// Summary condenses the activity for the heartbeat and the cluster view.
func (a *Activity) Summary() *kvpb.SQLSummary {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := &kvpb.SQLSummary{Statements: make(map[string]uint64, len(a.counts)), ByUser: map[string]int{},
		PlanCacheHits: uint64(metrics.CounterValue(metrics.SQLPlanCacheHits)), PlanCacheMisses: uint64(metrics.CounterValue(metrics.SQLPlanCacheMisses))}
	now := time.Now()
	for _, ca := range a.conns {
		s.Open++
		s.ByUser[ca.user]++
		switch ca.state {
		case stateActive:
			s.Active++
		case stateIdleInTxn:
			s.IdleInTxn++
			if age := now.Sub(ca.since).Milliseconds(); age > s.OldestIdleTxnMillis {
				s.OldestIdleTxnMillis = age
			}
		}
	}
	for k, v := range a.counts {
		s.Statements[k] = v
	}
	s.SerializationFailures = a.serFails
	s.CopyRows = a.copyRows
	s.P50Micros, s.P99Micros = a.latencies.percentiles()
	return s
}

// Connections lists every connection (admin view).
func (a *Activity) Connections() []ConnectionInfo {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	out := make([]ConnectionInfo, 0, len(a.conns))
	for _, ca := range a.conns {
		out = append(out, ConnectionInfo{User: ca.user, Remote: ca.remote, State: ca.state,
			Since: now.Sub(ca.since).Milliseconds(), Database: ca.db, Application: ca.app})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Since > out[j].Since })
	return out
}

// Active lists the statements in flight (admin view).
func (a *Activity) Active() []ActiveStatement {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	var out []ActiveStatement
	for _, ca := range a.conns {
		if ca.state == stateActive {
			out = append(out, ActiveStatement{User: ca.user, Remote: ca.remote, Kind: ca.kind, Text: ca.stmt, Elapsed: now.Sub(ca.since).Microseconds()})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Elapsed > out[j].Elapsed })
	return out
}

// Slow returns the slow-statement ring, newest first (admin view).
func (a *Activity) Slow() []SlowStatement {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]SlowStatement, len(a.slow))
	for i, s := range a.slow {
		out[len(a.slow)-1-i] = s
	}
	return out
}

// latencyRing keeps the last latencyRingSize statement durations.
type latencyRing struct {
	buf  [latencyRingSize]time.Duration
	n    int
	next int
}

func (r *latencyRing) add(d time.Duration) {
	r.buf[r.next] = d
	r.next = (r.next + 1) % latencyRingSize
	if r.n < latencyRingSize {
		r.n++
	}
}

func (r *latencyRing) percentiles() (p50, p99 int64) {
	if r.n == 0 {
		return 0, 0
	}
	vals := make([]time.Duration, r.n)
	copy(vals, r.buf[:r.n])
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	at := func(p int) int64 {
		idx := (len(vals)*p + 99) / 100
		if idx >= len(vals) {
			idx = len(vals) - 1
		}
		return vals[idx].Microseconds()
	}
	return at(50), at(99)
}

// recordRetryLocked attributes one serialization failure to the shape
// and the user that produced it (issue #154). Called with a.mu held,
// from the statement path, so it does no allocation past the first
// sighting of a shape and stops growing at retryShapeMax.
func (a *Activity) recordRetryLocked(t *stmtToken) {
	user := ""
	if ca, ok := a.conns[t.c]; ok {
		user = ca.user
	}
	a.retriesByUser[user]++
	shape := Fingerprint(t.text)
	rs := a.retryShapes[shape]
	if rs == nil {
		// Past the bound, the failure is still counted — in one
		// overflow total, so the hot list says "and N more" rather
		// than quietly dropping them.
		if len(a.retryShapes) >= retryShapeMax {
			a.retryOther++
			return
		}
		rs = &retryShape{users: map[string]uint64{}}
		a.retryShapes[shape] = rs
	}
	rs.count++
	rs.lastAt = t.start
	rs.users[user]++
}

// RetryShapes is the retry hot list, heaviest first, with the number of
// failures whose shape did not fit the bounded table (admin view).
func (a *Activity) RetryShapes(limit int) (out []RetryShape, other uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out = make([]RetryShape, 0, len(a.retryShapes))
	for shape, rs := range a.retryShapes {
		users := make(map[string]uint64, len(rs.users))
		for u, c := range rs.users {
			users[u] = c
		}
		out = append(out, RetryShape{Shape: shape, Count: rs.count, LastAt: rs.lastAt, Users: users})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Shape < out[j].Shape
	})
	other = a.retryOther
	if limit > 0 && len(out) > limit {
		// What falls off the end is still counted, for the same reason
		// the overflow total exists.
		for _, rs := range out[limit:] {
			other += rs.Count
		}
		out = out[:limit]
	}
	return out, other
}

// RetriesByUser is this node's serialization failures by the user whose
// statement hit them; contention is usually one application, so this is
// most of the diagnosis. Not statement text, but it names users, so it
// travels with the rest of the admin view.
func (a *Activity) RetriesByUser() map[string]uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]uint64, len(a.retriesByUser))
	for u, c := range a.retriesByUser {
		out[u] = c
	}
	return out
}

// IdleTxns lists the connections idle inside an open transaction,
// longest-open first (admin view).
func (a *Activity) IdleTxns() []IdleTxn {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	out := make([]IdleTxn, 0, a.idleTxn)
	for _, ca := range a.conns {
		if ca.state != stateIdleInTxn {
			continue
		}
		it := IdleTxn{PID: ca.pid, User: ca.user, Remote: ca.remote, Database: ca.db, Application: ca.app,
			IdleMillis: now.Sub(ca.since).Milliseconds(), Last: ca.last}
		if !ca.txnSince.IsZero() {
			it.TxnMillis = now.Sub(ca.txnSince).Milliseconds()
		}
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IdleMillis > out[j].IdleMillis })
	return out
}
