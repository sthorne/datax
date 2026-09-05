package pgwire

import (
	"sort"
	"sync"
	"time"

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
	// counts by statement kind, cumulative since start.
	counts    map[string]uint64
	serFails  uint64
	copyRows  uint64
	latencies latencyRing
	slow      []SlowStatement // newest last, at most slowRingSize
	seq       uint64
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
}

func newActivity(slow time.Duration) *Activity {
	if slow <= 0 {
		slow = DefaultSlowStatementThreshold
	}
	return &Activity{slowThreshold: slow, conns: make(map[*conn]*connActivity), counts: make(map[string]uint64)}
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
	delete(a.conns, c)
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
	case *parser.Select, *parser.Explain, *parser.ShowTables, *parser.ShowStats, *parser.Show, *parser.ShowDatabases, *parser.ShowSequences, *parser.ShowFunctions:
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
	case *parser.CreateTable, *parser.CreateIndex, *parser.DropTable, *parser.AlterTable, *parser.CreateUser, *parser.DropUser, *parser.GrantRevoke, *parser.Analyze,
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
		ca.state, ca.since, ca.stmt, ca.kind = stateActive, now, truncateStmt(text), kind
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
func (t *stmtToken) end(res *sql.Result, serr *sql.Error, inTxn bool) {
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
			ca.state = stateIdleInTxn
		} else {
			ca.state = stateIdle
			ca.txnSince = time.Time{}
		}
	}
	a.counts[t.kind]++
	a.latencies.add(d)
	rows := 0
	if res != nil {
		rows = len(res.Rows)
	}
	retry := serr != nil && serr.Code == sql.CodeSerializationFailure
	if retry {
		a.serFails++
	}
	if d >= a.slowThreshold {
		ss := SlowStatement{At: t.start, Kind: t.kind, Text: truncateStmt(t.text), Duration: d.Microseconds(), Rows: rows, Retry: retry}
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
func (a *Activity) gauges() {
	open, active, idleTxn := 0, 0, 0
	a.mu.Lock()
	for _, ca := range a.conns {
		open++
		switch ca.state {
		case stateActive:
			active++
		case stateIdleInTxn:
			idleTxn++
		}
	}
	a.mu.Unlock()
	metrics.SQLConnections.WithLabelValues("open").Set(float64(open))
	metrics.SQLConnections.WithLabelValues("active").Set(float64(active))
	metrics.SQLConnections.WithLabelValues("idle_in_txn").Set(float64(idleTxn))
}

// SlowThreshold is the duration past which statements are recorded.
func (a *Activity) SlowThreshold() time.Duration { return a.slowThreshold }

// Summary condenses the activity for the heartbeat and the cluster view.
func (a *Activity) Summary() *kvpb.SQLSummary {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := &kvpb.SQLSummary{Statements: make(map[string]uint64, len(a.counts)), ByUser: map[string]int{}}
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
		out = append(out, ConnectionInfo{User: ca.user, Remote: ca.remote, State: ca.state, Since: now.Sub(ca.since).Milliseconds()})
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
