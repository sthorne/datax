package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/audit"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// The audit table (issue #293). Audit records have always been lines in
// the node's log; this makes a durable copy of them in the cluster, in a
// system table (datax_audit) that SQL can read and nothing but the nodes
// can write, with each node's records hash-chained so that removing or
// changing one shows (pkg/audit has the format and the check).
//
// The rule everything here follows: recording an event must never be
// able to fail or delay the thing being recorded. A login, a DDL
// statement, a refused admin call all happen whether or not the cluster
// can take a write at that moment — and the moments it cannot (no
// quorum, a partition) are among the ones worth auditing. So log.Audit
// only appends to a bounded buffer in memory, and a background loop
// moves the buffer into the table when the table can be written. While
// it cannot, the buffer holds what it can; past its bound events are
// counted and dropped, and the count becomes a record of its own
// (audit-gap) when writing resumes. The log line is written first and
// unconditionally, so it remains the record of last resort: the table
// can be behind the log, never ahead of it.

// AuditTableDDL creates the audit table. IF NOT EXISTS makes the nodes'
// concurrent attempts idempotent.
//
// It is deliberately not a timeseries table, although it looks like
// one. Storage-level retention removes rows silently and range by
// range; a chain cut that way cannot say where it was cut, and Verify
// could not tell ageing from tampering. The nodes age their own chains
// out instead (retainOnce), recording each cut in the chain first.
const AuditTableDDL = `CREATE TABLE IF NOT EXISTS ` + catalog.AuditTableName + ` (
  node INT8 NOT NULL,
  seq INT8 NOT NULL,
  at TIMESTAMPTZ NOT NULL,
  event TEXT NOT NULL,
  principal TEXT NOT NULL,
  target TEXT NOT NULL,
  outcome TEXT NOT NULL,
  detail TEXT NOT NULL,
  prev_hash TEXT NOT NULL,
  hash TEXT NOT NULL,
  PRIMARY KEY (node, seq)
)`

const (
	defaultAuditFlushInterval = time.Second
	// DefaultAuditRetention is how long records are kept when
	// ALTER TABLE datax_audit SET (retention = ...) has never been run.
	DefaultAuditRetention = 90 * 24 * time.Hour
	// defaultAuditHousekeepingInterval paces retention and anchoring.
	defaultAuditHousekeepingInterval = 10 * time.Minute

	// auditBufferSize bounds the events held while the table cannot be
	// written. An event is a few hundred bytes, so this is a megabyte or
	// two at worst, and several hours of an ordinary cluster's events.
	auditBufferSize = 4096
	// auditBatchSize is the most records one INSERT carries.
	auditBatchSize = 200
	// Refusals are the one kind of event an outsider can produce at
	// will: a failed login costs the caller nothing. They are recorded
	// at no more than auditRefusalRate a second after a burst of
	// auditRefusalBurst, repeats of the same refusal within a flush are
	// folded into one record with a count, and what goes past the rate
	// is counted into the next audit-gap record. The log and the
	// counters (datax_auth_failures_total and friends) still see every
	// one.
	auditRefusalRate  = 20
	auditRefusalBurst = 200
	// auditValueLimit bounds one attribute of a record.
	auditValueLimit = 512
	// auditRetainPage and auditRetainPages bound one retention pass over
	// one chain: rows read and deleted per statement, and statements per
	// pass. A backlog larger than that is worked off over later passes.
	auditRetainPage  = 500
	auditRetainPages = 20
)

// auditEntry is one event waiting to be given its place in the chain.
// Sequence numbers and hashes are assigned when a batch is written, not
// when the event happens, so an event dropped from the buffer never
// leaves a hole in the chain.
type auditEntry struct {
	atMicros  int64
	event     string
	principal string
	target    string
	outcome   string
	// attrs are the remaining attributes in the order they were given.
	attrs []auditAttr
	// refusal marks the events auditRefusalRate applies to; count is how
	// many identical ones this entry stands for.
	refusal bool
	count   int64
}

type auditAttr struct {
	key   string
	value any
}

// auditRefusals are the events an unauthenticated or unprivileged
// caller can cause without limit.
var auditRefusals = map[string]bool{
	"sql-auth-failure":   true,
	"http-auth-failure":  true,
	"auth-throttled":     true,
	"sql-preauth-closed": true,
	"admin-denied":       true,
	"rpc-denied":         true,
}

// auditSecretKey reports whether an attribute's value must not be
// stored. Audit call sites are written not to pass secrets at all — a
// role's password, a share, a backup key never reach log.Audit — so
// this is a second line, for the call site written later that forgets.
func auditSecretKey(key string) bool {
	k := strings.ToLower(key)
	for _, word := range []string{"password", "passphrase", "secret", "token", "credential"} {
		if strings.Contains(k, word) {
			return true
		}
	}
	return k == "share" || k == "key_material"
}

// newAuditEntry turns one log.Audit call into a table entry.
func newAuditEntry(atMicros int64, event string, kv []any) auditEntry {
	e := auditEntry{atMicros: atMicros, event: event, refusal: auditRefusals[event], count: 1}
	var targetKey string
	for i := 0; i+1 < len(kv); i += 2 {
		key := fmt.Sprint(kv[i])
		switch key {
		case "principal":
			e.principal = fmt.Sprint(kv[i+1])
			continue
		case "outcome":
			e.outcome = fmt.Sprint(kv[i+1])
			continue
		case "target", "targets", "op", "path", "dir":
			// The first of these a record carries is what it acted on.
			if targetKey == "" {
				targetKey, e.target = key, fmt.Sprint(kv[i+1])
				continue
			}
		}
		var value any
		switch v := kv[i+1].(type) {
		case bool, int, int64, uint64, float64:
			value = v
		default:
			s := fmt.Sprint(v)
			if len(s) > auditValueLimit {
				s = strings.ToValidUTF8(s[:auditValueLimit], "") + "..."
			}
			value = s
		}
		switch {
		case auditSecretKey(key):
			value = "[redacted]"
		case key == "key":
			// A range boundary given to an admin operation is a row's
			// primary key, pretty-printed: a row value. The log line
			// keeps it; the table, which is read by more people for
			// longer, does not.
			value = "[not recorded]"
		case key == "remote" && e.refusal:
			// Without the ephemeral port, repeated refusals of one
			// caller are the same record and fold into one.
			if host, _, err := net.SplitHostPort(fmt.Sprint(value)); err == nil {
				value = host
			}
		}
		e.attrs = append(e.attrs, auditAttr{key, value})
	}
	if e.outcome == "" {
		switch {
		case strings.Contains(event, "failure"):
			e.outcome = "failed"
		case e.refusal:
			e.outcome = "denied"
		default:
			e.outcome = "ok"
		}
	}
	return e
}

// detail renders the entry's attributes as a JSON object, keys in the
// order given (encoding/json would sort a map, and the order the call
// site chose reads better).
func (e *auditEntry) detail() string {
	var b strings.Builder
	b.WriteByte('{')
	first := true
	add := func(key string, value any) {
		kb, _ := json.Marshal(key)
		vb, err := json.Marshal(value)
		if err != nil {
			vb, _ = json.Marshal(fmt.Sprint(value))
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.Write(kb)
		b.WriteByte(':')
		b.Write(vb)
	}
	for _, a := range e.attrs {
		add(a.key, a.value)
	}
	if e.count > 1 {
		add("count", e.count)
	}
	b.WriteByte('}')
	return b.String()
}

// foldKey identifies refusals that are the same record but for when
// they happened.
func (e *auditEntry) foldKey() string {
	c := *e
	c.count = 1
	return e.event + "\x00" + e.principal + "\x00" + e.target + "\x00" + e.outcome + "\x00" + c.detail()
}

// auditTable is a node's writer for the audit table.
type auditTable struct {
	node *Node

	// mu guards the buffer; it is taken by log.Audit's caller and so is
	// never held across anything that can block.
	mu    sync.Mutex
	queue []auditEntry
	// folds maps a refusal's foldKey to its index in queue.
	folds map[string]int
	// dropped and suppressed are events not recorded since the last
	// audit-gap record: the buffer was full, or refusals outran their
	// rate.
	dropped, suppressed int64
	// refusalTokens is the token bucket behind auditRefusalRate.
	refusalTokens float64
	refusalAt     time.Time
	// status mirrors of the chain state, for /api/security.
	headSeq   int64
	headHash  string
	ready     bool
	lastError string
	// lastWarn rate-limits the write-failure warning.
	lastWarn time.Time

	// wake nudges the flush loop when the buffer goes from empty.
	wake chan struct{}

	// flushMu serializes everything that appends to this node's chain
	// (the flush loop, retention, anchoring) and guards the fields
	// below. Unlike mu it is held across SQL.
	flushMu sync.Mutex
	// loaded is whether seq and hash are this node's head as the table
	// last confirmed it; known is whether they were ever loaded, which
	// is what tells a restart-of-chain from a first load.
	loaded, known bool
	seq           int64
	hash          string
	// restart is the chain restart this node has noticed and not yet
	// recorded.
	restart *audit.Restart
	// paused holds the writer off while a restore replaces the table.
	paused bool
	// anchored is the heads the last anchor record carried.
	anchored map[int64]int64
}

// One process can hold several nodes (tests, `datax demo`), and
// log.Audit is process-wide: it cannot say which of them an event
// belongs to, and every node's sink hears every event. If each wrote
// what it heard, a three-node demo would record everything three times.
// So exactly one node per process records — the longest-running one —
// and the next takes over when it stops. A real deployment has one node
// per process and never notices.
var auditWriters struct {
	mu    sync.Mutex
	nodes []*auditTable
}

func (a *auditTable) register() {
	auditWriters.mu.Lock()
	auditWriters.nodes = append(auditWriters.nodes, a)
	auditWriters.mu.Unlock()
}

func (a *auditTable) unregister() {
	auditWriters.mu.Lock()
	defer auditWriters.mu.Unlock()
	for i, w := range auditWriters.nodes {
		if w == a {
			auditWriters.nodes = append(auditWriters.nodes[:i], auditWriters.nodes[i+1:]...)
			return
		}
	}
}

func (a *auditTable) recording() bool {
	auditWriters.mu.Lock()
	defer auditWriters.mu.Unlock()
	return len(auditWriters.nodes) > 0 && auditWriters.nodes[0] == a
}

func (n *Node) auditFlushInterval() time.Duration {
	switch {
	case n.cfg.AuditFlushInterval < 0:
		return 0
	case n.cfg.AuditFlushInterval == 0:
		return defaultAuditFlushInterval
	}
	return n.cfg.AuditFlushInterval
}

// startAuditTable subscribes the node to the audit stream. It runs
// before the store is open — the first events of a start (the store key
// being unwrapped, an unseal) happen before there is anything to write
// to, and wait in the buffer for the flush loop like any others.
func (n *Node) startAuditTable() {
	if n.auditFlushInterval() == 0 {
		return
	}
	a := &auditTable{node: n, wake: make(chan struct{}, 1), folds: map[string]int{}, refusalTokens: auditRefusalBurst}
	n.audit = a
	a.register()
	remove := log.AddAuditSink(a.enqueue)
	n.stopper.AddCloser(func() {
		remove()
		a.unregister()
	})
}

// enqueue is the audit sink. It runs on the goroutine doing the thing
// being audited, so it takes one short lock and returns.
func (a *auditTable) enqueue(event string, kv []any) {
	if !a.recording() {
		return
	}
	now := time.Now()
	e := newAuditEntry(now.UnixMicro(), event, kv)
	a.mu.Lock()
	wasEmpty := len(a.queue) == 0
	a.push(e, now)
	a.mu.Unlock()
	if wasEmpty {
		select {
		case a.wake <- struct{}{}:
		default:
		}
	}
}

// push adds e to the buffer; a.mu is held.
func (a *auditTable) push(e auditEntry, now time.Time) {
	if e.refusal {
		key := e.foldKey()
		if i, ok := a.folds[key]; ok {
			a.queue[i].count += e.count
			return
		}
		if !a.refusalAt.IsZero() {
			a.refusalTokens += now.Sub(a.refusalAt).Seconds() * auditRefusalRate
			if a.refusalTokens > auditRefusalBurst {
				a.refusalTokens = auditRefusalBurst
			}
		}
		a.refusalAt = now
		if a.refusalTokens < 1 {
			a.suppressed += e.count
			metrics.AuditDropped.WithLabelValues("rate").Add(float64(e.count))
			return
		}
		a.refusalTokens--
		if len(a.queue) < auditBufferSize {
			a.folds[key] = len(a.queue)
		}
	}
	if len(a.queue) >= auditBufferSize {
		a.dropped += e.count
		metrics.AuditDropped.WithLabelValues("buffer-full").Add(float64(e.count))
		return
	}
	a.queue = append(a.queue, e)
	metrics.AuditBuffered.Set(float64(len(a.queue)))
}

// take removes up to max entries from the front of the buffer, with the
// counts an audit-gap record should carry.
func (a *auditTable) take(max int) (batch []auditEntry, dropped, suppressed int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	k := len(a.queue)
	if k > max {
		k = max
	}
	batch = append(batch, a.queue[:k]...)
	a.queue = append(a.queue[:0], a.queue[k:]...)
	// The indexes moved; a refusal repeated after this starts a new
	// record, which is the fold's intended extent anyway (one flush).
	a.folds = map[string]int{}
	dropped, suppressed = a.dropped, a.suppressed
	metrics.AuditBuffered.Set(float64(len(a.queue)))
	return batch, dropped, suppressed
}

// putBack returns a batch that could not be written to the front of the
// buffer. Events that arrived meanwhile and no longer fit are the ones
// dropped: the older record is the one harder to reconstruct.
func (a *auditTable) putBack(batch []auditEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	q := append(batch, a.queue...)
	if over := len(q) - auditBufferSize; over > 0 {
		for _, e := range q[auditBufferSize:] {
			a.dropped += e.count
			metrics.AuditDropped.WithLabelValues("buffer-full").Add(float64(e.count))
		}
		q = q[:auditBufferSize]
	}
	a.queue = q
	a.folds = map[string]int{}
	metrics.AuditBuffered.Set(float64(len(a.queue)))
}

// auditLoop moves the buffer into the table for the node's lifetime and
// runs the chain's housekeeping.
func (n *Node) auditLoop(ctx context.Context) {
	a := n.audit
	if a == nil {
		return
	}
	interval := n.auditFlushInterval()
	housekeeping := n.cfg.AuditHousekeepingInterval
	if housekeeping <= 0 {
		housekeeping = defaultAuditHousekeepingInterval
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	nextHousekeeping := time.Now().Add(housekeeping)
	for {
		select {
		case <-ctx.Done():
			// One last attempt for what the stop itself produced. The
			// node is going away, so it is brief and its failure is
			// only what a crash would have cost anyway.
			fctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			a.flush(fctx)
			cancel()
			return
		case <-tick.C:
		case <-a.wake:
			// Let a burst gather into one statement.
			select {
			case <-ctx.Done():
			case <-time.After(interval / 10):
			}
		}
		a.flush(ctx)
		if time.Now().After(nextHousekeeping) {
			nextHousekeeping = time.Now().Add(housekeeping)
			a.housekeep(ctx)
		}
	}
}

// flush writes what the buffer holds, a batch at a time, until it is
// empty or a write fails.
func (a *auditTable) flush(ctx context.Context) {
	if !a.recording() {
		return
	}
	a.flushMu.Lock()
	defer a.flushMu.Unlock()
	for ctx.Err() == nil {
		a.mu.Lock()
		pending := len(a.queue) > 0 || a.dropped > 0 || a.suppressed > 0
		a.mu.Unlock()
		if !pending || a.paused {
			return
		}
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := a.flushBatch(wctx)
		cancel()
		if err != nil {
			a.noteError(err)
			return
		}
	}
}

func (a *auditTable) noteError(err error) {
	metrics.AuditWriteErrors.Inc()
	a.mu.Lock()
	a.lastError = err.Error()
	warn := time.Since(a.lastWarn) > time.Minute
	if warn {
		a.lastWarn = time.Now()
	}
	a.mu.Unlock()
	if warn {
		log.Warnf("audit table: write failed; events stay buffered (and in this log) and are retried: %v", err)
	}
}

// flushBatch writes one batch. a.flushMu is held.
func (a *auditTable) flushBatch(ctx context.Context) error {
	n := a.node
	if n.db == nil {
		return errors.New("the store is not open yet")
	}
	if n.readClusterVersion(ctx) < version.V21 {
		// Rule 4: nothing creates the table before finalize, and until
		// then the log is the audit record, as it always was. The
		// buffer is emptied rather than kept, or the first records of
		// the table would be a stale few thousand and a gap.
		a.mu.Lock()
		a.queue, a.dropped, a.suppressed = nil, 0, 0
		a.folds = map[string]int{}
		metrics.AuditBuffered.Set(0)
		a.mu.Unlock()
		return nil
	}
	if err := a.ensure(ctx); err != nil {
		return err
	}
	batch, dropped, suppressed := a.take(auditBatchSize)
	entries := batch
	if dropped > 0 || suppressed > 0 {
		gap := auditEntry{atMicros: time.Now().UnixMicro(), event: audit.EventGap, principal: a.principal(), target: catalog.AuditTableName, outcome: "ok", count: 1}
		gap.attrs = []auditAttr{{"dropped", dropped}, {"suppressed", suppressed}}
		entries = append([]auditEntry{gap}, batch...)
	}
	if err := a.appendEntries(ctx, entries); err != nil {
		a.putBack(batch)
		return err
	}
	a.mu.Lock()
	a.dropped -= dropped
	a.suppressed -= suppressed
	a.mu.Unlock()
	return nil
}

// principal is who the table's records about itself are attributed to.
func (a *auditTable) principal() string {
	return fmt.Sprintf("node:%d", a.node.ident.NodeID)
}

// ensure creates the table if needed and makes seq and hash this node's
// head. a.flushMu is held.
func (a *auditTable) ensure(ctx context.Context) error {
	a.mu.Lock()
	ready := a.ready
	a.mu.Unlock()
	if !ready {
		if _, err := a.exec(ctx, AuditTableDDL); err != nil {
			return err
		}
		a.mu.Lock()
		a.ready = true
		a.mu.Unlock()
	}
	self := int64(a.node.ident.NodeID)
	if a.loaded {
		// The head this node remembers must still be the table's. One
		// point read per flush is what notices a restore, or records
		// removed underneath the node, while the node is the only one
		// who still knows what was there.
		if a.seq == 0 {
			return nil // nothing to compare; a duplicate key on insert reloads
		}
		res, err := a.exec(ctx, "SELECT hash FROM "+catalog.AuditTableName+" WHERE node = $1 AND seq = $2", types.NewInt(self), types.NewInt(a.seq))
		if err != nil {
			return err
		}
		if len(res.Rows) == 1 && res.Rows[0][0].S == a.hash {
			return nil
		}
		a.loaded = false
	}
	head, ok, err := a.readHead(ctx, self)
	if err != nil {
		return err
	}
	if !ok {
		head = audit.Head{Node: self, Seq: 0, Hash: audit.GenesisHash}
	}
	if a.known && a.restart == nil && (head.Seq != a.seq || head.Hash != a.hash) {
		a.restart = &audit.Restart{Reason: "the head this node last wrote is not in the table (a restore, or records removed)", LostSeq: a.seq, LostHash: a.hash}
	}
	a.setHead(head.Seq, head.Hash)
	a.loaded, a.known = true, true
	if a.restart == nil {
		return nil
	}
	// The chain goes on from what the table holds, and says so first:
	// this record is why the head this node (or anyone who exported
	// it) saw is no longer there. It is kept until it is written, so a
	// failed attempt does not lose the explanation.
	d, _ := json.Marshal(a.restart)
	e := auditEntry{atMicros: time.Now().UnixMicro(), event: audit.EventRestart, principal: a.principal(), target: catalog.AuditTableName, outcome: "ok", count: 1}
	if err := a.appendRecords(ctx, []audit.Record{a.record(&e, string(d))}); err != nil {
		return err
	}
	a.restart = nil
	return nil
}

func (a *auditTable) setHead(seq int64, hash string) {
	a.seq, a.hash = seq, hash
	a.mu.Lock()
	a.headSeq, a.headHash, a.lastError = seq, hash, ""
	a.mu.Unlock()
}

// record gives e its place after the current head, without advancing
// it. detail overrides the entry's own attributes when non-empty.
func (a *auditTable) record(e *auditEntry, detail string) audit.Record {
	if detail == "" {
		detail = e.detail()
	}
	return audit.Record{
		Node: int64(a.node.ident.NodeID), AtMicros: e.atMicros, Event: e.event,
		Principal: e.principal, Target: e.target, Outcome: e.outcome, Detail: detail,
	}
}

// appendEntries chains entries onto the head and writes them.
func (a *auditTable) appendEntries(ctx context.Context, entries []auditEntry) error {
	recs := make([]audit.Record, len(entries))
	for i := range entries {
		recs[i] = a.record(&entries[i], "")
	}
	return a.appendRecords(ctx, recs)
}

// appendRecords assigns each record its sequence number and hashes and
// writes them in one statement — one transaction, so the chain in the
// table never ends in half a batch. a.flushMu is held and the head is
// loaded.
func (a *auditTable) appendRecords(ctx context.Context, recs []audit.Record) error {
	if len(recs) == 0 {
		return nil
	}
	seq, prev := a.seq, a.hash
	var b strings.Builder
	b.WriteString("INSERT INTO " + catalog.AuditTableName + " (node, seq, at, event, principal, target, outcome, detail, prev_hash, hash) VALUES ")
	params := make([]types.Datum, 0, 10*len(recs))
	for i := range recs {
		r := &recs[i]
		seq++
		r.Seq, r.PrevHash = seq, prev
		r.Hash = r.ComputeHash()
		prev = r.Hash
		if i > 0 {
			b.WriteString(", ")
		}
		p := len(params)
		fmt.Fprintf(&b, "($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)", p+1, p+2, p+3, p+4, p+5, p+6, p+7, p+8, p+9, p+10)
		params = append(params,
			types.NewInt(r.Node), types.NewInt(r.Seq), types.NewTimestamp(r.AtMicros*1000),
			types.NewString(r.Event), types.NewString(r.Principal), types.NewString(r.Target), types.NewString(r.Outcome),
			types.NewString(r.Detail), types.NewString(r.PrevHash), types.NewString(r.Hash))
	}
	if _, err := a.exec(ctx, b.String(), params...); err != nil {
		// Whatever went wrong — the table dropped by a restore, a row
		// already at this sequence number — the next attempt starts by
		// finding out what the table really holds.
		a.loaded = false
		if isMissingTable(err) {
			a.mu.Lock()
			a.ready = false
			a.mu.Unlock()
		}
		return err
	}
	a.setHead(seq, prev)
	metrics.AuditRecords.Add(float64(len(recs)))
	return nil
}

// exec runs one statement as the node's own session — the only session
// the audit table accepts a write from.
func (a *auditTable) exec(ctx context.Context, stmt string, params ...types.Datum) (*sql.Result, error) {
	sess, err := a.node.systemSession()
	if err != nil {
		return nil, err
	}
	stmts, err := parser.Parse(stmt)
	if err != nil {
		return nil, err
	}
	res, serr := sess.Execute(ctx, stmts[0], params)
	if serr != nil {
		return nil, serr
	}
	return res, nil
}

// readHead returns the last record of node's chain.
func (a *auditTable) readHead(ctx context.Context, node int64) (audit.Head, bool, error) {
	res, err := a.exec(ctx, "SELECT seq, hash FROM "+catalog.AuditTableName+" WHERE node = $1 ORDER BY seq DESC LIMIT 1", types.NewInt(node))
	if err != nil || len(res.Rows) == 0 {
		return audit.Head{}, false, err
	}
	return audit.Head{Node: node, Seq: res.Rows[0][0].I, Hash: res.Rows[0][1].S}, true, nil
}

// chainNodes lists the nodes that have a chain in the table, by seeking
// from one node's rows to the next rather than reading them all.
func (a *auditTable) chainNodes(ctx context.Context) ([]int64, error) {
	var out []int64
	after := int64(-1 << 62)
	for {
		res, err := a.exec(ctx, "SELECT node FROM "+catalog.AuditTableName+" WHERE node > $1 ORDER BY node, seq LIMIT 1", types.NewInt(after))
		if err != nil {
			return nil, err
		}
		if len(res.Rows) == 0 {
			return out, nil
		}
		after = res.Rows[0][0].I
		out = append(out, after)
	}
}

// housekeep ages records out and, on the node leading range 1, anchors
// the chains.
func (a *auditTable) housekeep(ctx context.Context) {
	if !a.recording() {
		return
	}
	n := a.node
	if n.db == nil || n.readClusterVersion(ctx) < version.V21 {
		return
	}
	a.mu.Lock()
	ready := a.ready
	a.mu.Unlock()
	if !ready {
		return // no event has needed the table yet
	}
	hctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	self := int64(n.ident.NodeID)
	chains := []int64{self}
	r1, _ := n.store.GetReplica(1)
	leader := r1 != nil && r1.IsLeader()
	if leader {
		// A node that has left the cluster cannot age its own chain
		// out, so the node leading range 1 does it for every chain
		// whose node the registry no longer knows as a member.
		member := map[int64]bool{}
		for _, nd := range n.registry.All() {
			if !nd.Draining {
				member[int64(nd.NodeID)] = true
			}
		}
		all, err := a.chainNodes(hctx)
		if err != nil {
			a.noteError(err)
			return
		}
		for _, node := range all {
			if node != self && !member[node] {
				chains = append(chains, node)
			}
		}
	}
	cutoff := time.Now().Add(-a.retention(hctx)).UnixMicro()
	for _, node := range chains {
		if err := a.retainOnce(hctx, node, cutoff); err != nil {
			a.noteError(err)
			return
		}
	}
	if leader {
		if err := a.anchor(hctx); err != nil {
			a.noteError(err)
		}
	}
}

// retention is how long records are kept: what ALTER TABLE datax_audit
// SET (retention = ...) stored in the table's descriptor, the default
// if that was never run.
func (a *auditTable) retention(ctx context.Context) time.Duration {
	if d := a.node.cfg.AuditRetention; d > 0 {
		return d
	}
	if raw, err := a.node.db.Get(ctx, keys.TableDescKey(catalog.AuditTableID)); err == nil && raw != nil {
		var d catalog.TableDescriptor
		if json.Unmarshal(raw, &d) == nil && d.RetentionSeconds > 0 {
			return time.Duration(d.RetentionSeconds) * time.Second
		}
	}
	return DefaultAuditRetention
}

// retainOnce removes the records at the start of node's chain that are
// older than cutoff, after recording in this node's own chain where the
// cut falls.
//
// Only a prefix of the chain goes: the scan stops at the first record
// young enough to keep, whatever the ages of those after it, so a clock
// that stepped backwards cannot make retention punch a hole. And the
// newest record always stays, so a chain that has gone quiet keeps its
// head and the next record still has something to follow.
func (a *auditTable) retainOnce(ctx context.Context, node, cutoff int64) error {
	var through audit.Head
	var first int64
	after := int64(0)
	for page := 0; page < auditRetainPages; page++ {
		res, err := a.exec(ctx, "SELECT seq, at, hash FROM "+catalog.AuditTableName+" WHERE node = $1 AND seq > $2 ORDER BY seq LIMIT $3",
			types.NewInt(node), types.NewInt(after), types.NewInt(auditRetainPage+1))
		if err != nil {
			return err
		}
		rows := res.Rows
		more := len(rows) > auditRetainPage
		stop := !more
		for i, row := range rows {
			if i == len(rows)-1 && !more {
				break // the chain's newest record
			}
			if i == auditRetainPage {
				break // the peeked row belongs to the next page
			}
			if row[1].I/1000 >= cutoff {
				stop = true
				break
			}
			if first == 0 {
				first = row[0].I
			}
			through = audit.Head{Node: node, Seq: row[0].I, Hash: row[2].S}
			after = row[0].I
		}
		if stop {
			break
		}
	}
	if through.Seq == 0 {
		return nil
	}
	a.flushMu.Lock()
	err := a.ensure(ctx)
	if err == nil {
		d, _ := json.Marshal(audit.Cut{Node: node, ThroughSeq: through.Seq, ThroughHash: through.Hash})
		cut := auditEntry{atMicros: time.Now().UnixMicro(), event: audit.EventRetention, principal: a.principal(), target: fmt.Sprintf("node %d", node), outcome: "ok", count: 1}
		err = a.appendRecords(ctx, []audit.Record{a.record(&cut, string(d))})
	}
	a.flushMu.Unlock()
	if err != nil {
		return err // nothing is deleted that the chain does not account for
	}
	for lo := first; lo <= through.Seq; lo += auditRetainPage {
		hi := lo + auditRetainPage - 1
		if hi > through.Seq {
			hi = through.Seq
		}
		if _, err := a.exec(ctx, "DELETE FROM "+catalog.AuditTableName+" WHERE node = $1 AND seq >= $2 AND seq <= $3",
			types.NewInt(node), types.NewInt(lo), types.NewInt(hi)); err != nil {
			// The cut is recorded and the records are still there; the
			// next pass removes them under a cut of its own.
			return err
		}
	}
	metrics.AuditRetained.Add(float64(through.Seq - first + 1))
	return nil
}

// heads returns the head of every chain in the table.
func (a *auditTable) heads(ctx context.Context) ([]audit.Head, error) {
	nodes, err := a.chainNodes(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]audit.Head, 0, len(nodes))
	for _, node := range nodes {
		h, ok, err := a.readHead(ctx, node)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, h)
		}
	}
	return out, nil
}

// anchor writes every chain's head into this node's chain, when any of
// them has moved since the last anchor. The chains are separate so that
// no node waits on another to record an event; this is what ties them
// back together, so that a chain rewritten on its own disagrees with
// the others' account of it.
func (a *auditTable) anchor(ctx context.Context) error {
	heads, err := a.heads(ctx)
	if err != nil {
		return err
	}
	a.flushMu.Lock()
	defer a.flushMu.Unlock()
	moved := false
	for _, h := range heads {
		if a.anchored[h.Node] != h.Seq {
			moved = true
		}
	}
	if !moved {
		return nil
	}
	if err := a.ensure(ctx); err != nil {
		return err
	}
	sort.Slice(heads, func(i, j int) bool { return heads[i].Node < heads[j].Node })
	d, _ := json.Marshal(audit.Anchor{Heads: heads})
	e := auditEntry{atMicros: time.Now().UnixMicro(), event: audit.EventAnchor, principal: a.principal(), target: catalog.AuditTableName, outcome: "ok", count: 1}
	if err := a.appendRecords(ctx, []audit.Record{a.record(&e, string(d))}); err != nil {
		return err
	}
	a.anchored = map[int64]int64{}
	for _, h := range heads {
		a.anchored[h.Node] = h.Seq
	}
	// The anchor is itself this chain's new head; without this the next
	// pass would see its own record as movement and anchor forever.
	a.anchored[int64(a.node.ident.NodeID)] = a.seq
	return nil
}

// pauseForRestore holds the writer off and, when the backup being
// restored carries an audit table, empties the one this cluster has.
// A restore lays the backup's rows over whatever is there, key by key;
// two different histories interleaved at the same sequence numbers are
// a chain that verifies nowhere. The few records a cluster fresh enough
// to restore into has are still in its nodes' logs. Each node then
// finds its head gone and says so in its chain (audit-chain-restart)
// before carrying on from the restored one.
func (n *Node) pauseAuditForRestore(ctx context.Context, replacing bool) (resume func()) {
	a := n.audit
	if a == nil {
		return func() {}
	}
	a.flushMu.Lock()
	a.paused = true
	a.flushMu.Unlock()
	if replacing {
		if err := a.clear(ctx); err != nil && !isMissingTable(err) {
			log.Warnf("audit table: clearing it ahead of the restore: %v", err)
		}
	}
	return func() {
		a.flushMu.Lock()
		a.paused, a.loaded = false, false
		a.flushMu.Unlock()
	}
}

// clear removes every record, a bounded statement at a time.
func (a *auditTable) clear(ctx context.Context) error {
	nodes, err := a.chainNodes(ctx)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		for {
			res, err := a.exec(ctx, "SELECT seq FROM "+catalog.AuditTableName+" WHERE node = $1 ORDER BY seq LIMIT 1", types.NewInt(node))
			if err != nil {
				return err
			}
			if len(res.Rows) == 0 {
				break
			}
			lo := res.Rows[0][0].I
			if _, err := a.exec(ctx, "DELETE FROM "+catalog.AuditTableName+" WHERE node = $1 AND seq >= $2 AND seq < $3",
				types.NewInt(node), types.NewInt(lo), types.NewInt(lo+auditRetainPage)); err != nil {
				return err
			}
		}
	}
	return nil
}

// SecurityAudit is the audit table's state on the serving node, in
// /api/security.
type SecurityAudit struct {
	// Enabled: this node writes audit records to the table. Ready: the
	// table exists and this node has written to it or read its head.
	Enabled bool   `json:"enabled"`
	Table   string `json:"table"`
	Ready   bool   `json:"ready"`
	// Head is the last record of this node's chain. Kept somewhere the
	// cluster cannot write, it is what `datax debug audit verify
	// --heads` later proves the chain against.
	Head *audit.Head `json:"head,omitempty"`
	// Buffered is the events waiting to be written; Dropped and
	// Suppressed count, since the node started, those that never will
	// be (the buffer filled; refusals outran their rate). They are in
	// the node's log regardless.
	Buffered   int     `json:"buffered"`
	Dropped    float64 `json:"dropped"`
	Suppressed float64 `json:"suppressed"`
	// WriteErrors counts failed writes since the node started;
	// LastError is the most recent, cleared by the next success.
	WriteErrors float64 `json:"write_errors"`
	LastError   string  `json:"last_error,omitempty"`
}

// AuditStatus is this node's audit table state.
func (n *Node) AuditStatus() SecurityAudit {
	st := SecurityAudit{Table: catalog.AuditTableName}
	a := n.audit
	if a == nil {
		return st
	}
	st.Enabled = a.recording()
	a.mu.Lock()
	st.Ready, st.Buffered, st.LastError = a.ready, len(a.queue), a.lastError
	if a.headSeq > 0 {
		st.Head = &audit.Head{Node: int64(n.ident.NodeID), Seq: a.headSeq, Hash: a.headHash}
	}
	a.mu.Unlock()
	st.Dropped = counterValue(metrics.AuditDropped.WithLabelValues("buffer-full"))
	st.Suppressed = counterValue(metrics.AuditDropped.WithLabelValues("rate"))
	st.WriteErrors = counterValue(metrics.AuditWriteErrors)
	return st
}

// AuditHousekeep runs one retention and anchoring pass now (tests).
func (n *Node) AuditHousekeep(ctx context.Context) {
	if n.audit != nil {
		n.audit.housekeep(ctx)
	}
}

// AuditFlush writes what the buffer holds now (tests).
func (n *Node) AuditFlush(ctx context.Context) {
	if n.audit != nil {
		n.audit.flush(ctx)
	}
}
