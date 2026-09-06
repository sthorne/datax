package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/placement"
	"github.com/sthorne/datax/pkg/util/events"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// /api/health — the problems panel: a list of things an operator should
// look at, each computed from data the serving node already holds (the
// registry, the /meta range list, its own store and engine, the pinger,
// the schema cache), so the panel costs no new collectors. Empty means
// green. /api/events — the node's recent operational events (see
// pkg/util/events), audit records included for admins.

// Problem severities.
const (
	SeverityCritical = "critical"
	SeverityWarning  = "warning"
	SeverityInfo     = "info"
)

// Problem is one row of the panel.
type Problem struct {
	Severity string `json:"severity"`
	// Check names the rule (stable, for alerting: datax_health_problems).
	Check   string `json:"check"`
	Summary string `json:"summary"`
	// Node or Range name what the problem concerns, when one thing does.
	Node  int   `json:"node,omitempty"`
	Range int64 `json:"range,omitempty"`
	// Section is the dashboard section the row links to.
	Section string `json:"section,omitempty"`
}

// HealthStatus is the /api/health document.
type HealthStatus struct {
	CheckedAt int64     `json:"checked_at_unix_ms"`
	NodeID    int       `json:"node_id"`
	Problems  []Problem `json:"problems"`
	// Checks is how many rules ran, so an empty list reads as "checked
	// and fine" rather than "nothing ran".
	Checks int `json:"checks"`
}

// healthCacheFor bounds how often the checks run.
const healthCacheFor = 3 * time.Second

// diskFreeWarn / diskFreeCritical are the store-disk thresholds.
const (
	diskFreeWarn     = 0.15
	diskFreeCritical = 0.05
	fdWarn           = 0.8
	authFailureRate  = 1.0 // per second over the last five minutes
)

type healthCache struct {
	mu   sync.Mutex
	at   time.Time
	doc  *HealthStatus
	prev counterSamples
}

// counterSamples remembers earlier readings of cumulative counters so a
// rate over the last window can be judged.
type counterSamples struct {
	stalls   []sample
	auth     []sample
	bgErrors int64
}

type sample struct {
	at time.Time
	v  float64
}

const rateWindow = 5 * time.Minute

// rateOver appends v and returns the increase over the window.
func rateOver(ss []sample, v float64, now time.Time) ([]sample, float64) {
	ss = append(ss, sample{at: now, v: v})
	i := 0
	for i < len(ss)-1 && now.Sub(ss[i].at) > rateWindow {
		i++
	}
	ss = ss[i:]
	return ss, v - ss[0].v
}

func (n *Node) serveHealthAPI(w http.ResponseWriter, req *http.Request) {
	doc := n.healthDoc(req)
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

func (n *Node) healthDoc(req *http.Request) *HealthStatus {
	n.health.mu.Lock()
	defer n.health.mu.Unlock()
	if n.health.doc != nil && time.Since(n.health.at) < healthCacheFor {
		return n.health.doc
	}
	doc := n.runHealthChecks(req)
	n.health.doc, n.health.at = doc, time.Now()
	return doc
}

// runHealthChecks evaluates every rule. Caller holds n.health.mu (for the
// counter samples).
func (n *Node) runHealthChecks(req *http.Request) *HealthStatus {
	now := time.Now()
	nowWall := n.clock.Now().WallTime
	doc := &HealthStatus{CheckedAt: nowWall / int64(time.Millisecond), NodeID: int(n.ident.NodeID), Problems: []Problem{}}
	add := func(p Problem) { doc.Problems = append(doc.Problems, p) }

	// Nodes: liveness, draining, binary versions, unfinalized upgrade.
	doc.Checks++
	nodes := n.registry.All()
	grace := n.livenessGrace().Nanoseconds()
	dead := n.deadNodeThreshold().Nanoseconds()
	live := map[base.NodeID]bool{}
	byID := map[base.NodeID]kvpb.NodeDescriptor{}
	cv := n.readClusterVersion(req.Context())
	minBinary, maxBinary := version.Version(0), version.Version(0)
	for _, nd := range nodes {
		byID[nd.NodeID] = nd
		age := nowWall - nd.LivenessTime
		switch {
		case age > dead:
			add(Problem{Severity: SeverityCritical, Check: "node-down", Node: int(nd.NodeID), Section: "nodes",
				Summary: fmt.Sprintf("n%d has not heartbeated for %s: its replicas are being repaired away", nd.NodeID, time.Duration(age).Truncate(time.Second))})
		case age > grace:
			add(Problem{Severity: SeverityWarning, Check: "node-unresponsive", Node: int(nd.NodeID), Section: "nodes",
				Summary: fmt.Sprintf("n%d has not heartbeated for %s (declared dead after %s)", nd.NodeID, time.Duration(age).Truncate(time.Second), time.Duration(dead))})
		default:
			live[nd.NodeID] = true
		}
		if nd.Draining {
			add(Problem{Severity: SeverityInfo, Check: "node-draining", Node: int(nd.NodeID), Section: "nodes",
				Summary: fmt.Sprintf("n%d is draining (decommission in progress)", nd.NodeID)})
		}
		if nd.ShuttingDown {
			add(Problem{Severity: SeverityInfo, Check: "node-stopping", Node: int(nd.NodeID), Section: "nodes",
				Summary: fmt.Sprintf("n%d is shutting down (shedding its leases and SQL connections)", nd.NodeID)})
		}
		bv := version.Version(nd.BinaryVersion)
		if bv == 0 {
			bv = version.V1
		}
		if minBinary == 0 || bv < minBinary {
			minBinary = bv
		}
		if bv > maxBinary {
			maxBinary = bv
		}
	}
	if len(nodes) > 0 && minBinary != maxBinary {
		add(Problem{Severity: SeverityWarning, Check: "mixed-binaries", Section: "nodes",
			Summary: fmt.Sprintf("nodes run binaries at protocol v%d..v%d: a rolling upgrade is in progress, or a node was missed", minBinary, maxBinary)})
	} else if len(nodes) > 0 && minBinary > cv {
		add(Problem{Severity: SeverityInfo, Check: "upgrade-unfinalized", Section: "nodes",
			Summary: fmt.Sprintf("every node runs protocol v%d but the cluster version is v%d: run `datax debug upgrade` to finalize (until then binaries can roll back)", minBinary, cv)})
	}

	// Ranges: replication, quorum, diversity.
	doc.Checks++
	if descs, _, err := n.clusterRanges(req.Context()); err == nil {
		localities := map[string]bool{}
		for _, nd := range nodes {
			localities[nd.Locality.String()] = true
		}
		under, noQuorum, undiverse := 0, 0, 0
		var underEx, quorumEx, divEx base.RangeID
		// Placement (issue #176): a range whose replicas sit outside the
		// policy its database names, and a policy no live node can
		// satisfy — the allocator leaves that data where it is rather
		// than widening the policy, so it has to be said out loud.
		misplaced, unsatisfiable := 0, 0
		var misEx, unsatEx base.RangeID
		var unsatPolicy string
		nodeLocalities := map[base.NodeID]base.Locality{}
		for _, nd := range nodes {
			nodeLocalities[nd.NodeID] = nd.Locality
		}
		for _, d := range descs {
			policy := n.placementOf(d)
			if len(policy.Constraints) > 0 {
				var ids []base.NodeID
				for _, rep := range d.Replicas {
					ids = append(ids, rep.NodeID)
				}
				if len(placement.Misplaced(policy, ids, nodeLocalities)) > 0 {
					anyHome := false
					for _, nd := range nodes {
						if live[nd.NodeID] && policy.Satisfies(nd.Locality) {
							anyHome = true
							break
						}
					}
					if anyHome {
						misplaced++
						misEx = d.RangeID
					} else {
						unsatisfiable++
						unsatEx, unsatPolicy = d.RangeID, policy.String()
					}
				}
			}
			liveReps, seen := 0, map[string]bool{}
			dup := false
			for _, rep := range d.Replicas {
				if live[rep.NodeID] {
					liveReps++
				}
				if nd, ok := byID[rep.NodeID]; ok {
					loc := nd.Locality.String()
					if seen[loc] {
						dup = true
					}
					seen[loc] = true
				}
			}
			want := policy.ReplicasOr(base.DefaultReplicationFactor)
			if liveReps*2 <= len(d.Replicas) {
				noQuorum++
				quorumEx = d.RangeID
			} else if len(d.Replicas) < want || liveReps < want {
				under++
				underEx = d.RangeID
			}
			if dup && len(localities) >= want && len(d.Replicas) >= want {
				undiverse++
				divEx = d.RangeID
			}
		}
		if noQuorum > 0 {
			add(Problem{Severity: SeverityCritical, Check: "quorum-lost", Range: int64(quorumEx), Section: "cluster-ranges",
				Summary: fmt.Sprintf("%d range(s) have lost a majority of live replicas (e.g. %s): unavailable until nodes return or `datax debug unsafe-recover`", noQuorum, quorumEx)})
		}
		if under > 0 {
			add(Problem{Severity: SeverityWarning, Check: "under-replicated", Range: int64(underEx), Section: "cluster-ranges",
				Summary: fmt.Sprintf("%d range(s) have fewer than %d live replicas (e.g. %s); the allocator repairs them as nodes allow", under, base.DefaultReplicationFactor, underEx)})
		}
		if undiverse > 0 {
			add(Problem{Severity: SeverityWarning, Check: "diversity", Range: int64(divEx), Section: "cluster-ranges",
				Summary: fmt.Sprintf("%d range(s) keep two replicas in one locality although %d localities exist (e.g. %s); a rack failure could cost two", undiverse, len(localities), divEx)})
		}
		if unsatisfiable > 0 {
			add(Problem{Severity: SeverityCritical, Check: "placement-unsatisfiable", Range: int64(unsatEx), Section: "cluster-ranges",
				Summary: fmt.Sprintf("%d range(s) name a placement no live node satisfies (e.g. %s wants %s): the data stays where it is until such a node joins", unsatisfiable, unsatEx, unsatPolicy)})
		}
		if misplaced > 0 {
			add(Problem{Severity: SeverityWarning, Check: "placement-misplaced", Range: int64(misEx), Section: "cluster-ranges",
				Summary: fmt.Sprintf("%d range(s) hold a replica outside their database's placement policy (e.g. %s); the allocator moves them one per pass", misplaced, misEx)})
		}
	} else {
		add(Problem{Severity: SeverityWarning, Check: "meta-unavailable", Section: "cluster-ranges",
			Summary: "the range listing is unavailable: " + err.Error()})
	}

	// Storage on this node: backpressure, stalls, background errors, the
	// debt gate; overload verdicts held about peers.
	doc.Checks++
	if eng := n.engine; eng != nil {
		m := eng.StorageMetrics()
		if ov, cause, detail := eng.OverloadedCause(); ov {
			add(Problem{Severity: SeverityWarning, Check: "backpressure", Node: int(n.ident.NodeID), Section: "storage",
				Summary: fmt.Sprintf("this node is shedding writes (%s): %s", cause, detail)})
		} else if eng.DebtGated() {
			add(Problem{Severity: SeverityWarning, Check: "debt-gate", Node: int(n.ident.NodeID), Section: "storage",
				Summary: fmt.Sprintf("the compaction-debt gate is latched (%s of pending compaction); table writes are shed until it halves", fmtBytesGo(m.CompactionDebtBytes))})
		}
		var stallDelta float64
		n.health.prev.stalls, stallDelta = rateOver(n.health.prev.stalls, float64(m.WriteStalls), now)
		if stallDelta > 0 {
			add(Problem{Severity: SeverityCritical, Check: "write-stalls", Node: int(n.ident.NodeID), Section: "storage",
				Summary: fmt.Sprintf("Pebble hard-stalled writes %d time(s) in the last %s: the store is past backpressure", int(stallDelta), rateWindow)})
		}
		if m.BackgroundErrors > n.health.prev.bgErrors {
			add(Problem{Severity: SeverityCritical, Check: "storage-errors", Node: int(n.ident.NodeID), Section: "storage",
				Summary: fmt.Sprintf("the storage engine reported %d background error(s) (compaction or flush); check the disk and the log", m.BackgroundErrors)})
		}
		n.health.prev.bgErrors = m.BackgroundErrors
	}
	if n.store != nil {
		for _, nd := range nodes {
			if nd.NodeID == n.ident.NodeID {
				continue
			}
			if ov, reason := n.store.NodeOverloaded(nd.NodeID); ov {
				add(Problem{Severity: SeverityWarning, Check: "follower-overloaded", Node: int(nd.NodeID), Section: "nodes",
					Summary: fmt.Sprintf("n%d reports itself overloaded (%s); writes to ranges it replicates are being shed", nd.NodeID, reason)})
			}
		}
	}

	// Machines: disk, file descriptors, memory, from every live node's
	// heartbeat (a node that has stopped heartbeating has its own row; its
	// last figures are stale).
	doc.Checks++
	for _, nd := range nodes {
		m := nd.Machine
		if m == nil || !live[nd.NodeID] {
			continue
		}
		if m.DiskTotal > 0 {
			frac := float64(m.DiskFree) / float64(m.DiskTotal)
			if frac < diskFreeCritical {
				add(Problem{Severity: SeverityCritical, Check: "disk-full", Node: int(nd.NodeID), Section: "nodes",
					Summary: fmt.Sprintf("n%d's store disk has %s free (%.0f%%): compaction needs headroom and a full disk is a hard stall", nd.NodeID, fmtBytesGo(m.DiskFree), 100*frac)})
			} else if frac < diskFreeWarn {
				add(Problem{Severity: SeverityWarning, Check: "disk-low", Node: int(nd.NodeID), Section: "nodes",
					Summary: fmt.Sprintf("n%d's store disk has %s free (%.0f%%)", nd.NodeID, fmtBytesGo(m.DiskFree), 100*frac)})
			}
		}
		if m.FDLimit > 0 && float64(m.OpenFDs)/float64(m.FDLimit) > fdWarn {
			add(Problem{Severity: SeverityWarning, Check: "fd-limit", Node: int(nd.NodeID), Section: "nodes",
				Summary: fmt.Sprintf("n%d holds %d of %d file descriptors; raise the limit (ulimit -n) before Pebble runs out", nd.NodeID, m.OpenFDs, m.FDLimit)})
		}
		if m.MemTotal > 0 && float64(m.MemAvailable)/float64(m.MemTotal) < 0.1 {
			add(Problem{Severity: SeverityWarning, Check: "memory-low", Node: int(nd.NodeID), Section: "nodes",
				Summary: fmt.Sprintf("n%d has %s of memory available (%.0f%%)", nd.NodeID, fmtBytesGo(m.MemAvailable), 100*float64(m.MemAvailable)/float64(m.MemTotal))})
		}
	}

	// Network: this node's pings.
	doc.Checks++
	if n.pinger != nil {
		max := n.clock.MaxOffset()
		for _, pl := range n.pinger.Snapshot() {
			if !pl.Reachable {
				if pl.AgeMillis < 0 {
					add(Problem{Severity: SeverityWarning, Check: "peer-unreachable", Node: int(pl.Peer), Section: "network",
						Summary: fmt.Sprintf("n%d has never answered this node's pings", pl.Peer)})
				} else {
					add(Problem{Severity: SeverityWarning, Check: "peer-unreachable", Node: int(pl.Peer), Section: "network",
						Summary: fmt.Sprintf("n%d stopped answering this node's pings %s ago", pl.Peer, (time.Duration(pl.AgeMillis) * time.Millisecond).Truncate(time.Second))})
				}
				continue
			}
			off := time.Duration(pl.OffsetMicros) * time.Microsecond
			if off < 0 {
				off = -off
			}
			if max > 0 && off >= max {
				add(Problem{Severity: SeverityCritical, Check: "clock-offset", Node: int(pl.Peer), Section: "network",
					Summary: fmt.Sprintf("n%d's clock is %s off this node's, past the tolerated %s: nodes refuse each other's timestamps at this point — fix NTP now", pl.Peer, off, max)})
			} else if max > 0 && off >= max/2 {
				add(Problem{Severity: SeverityWarning, Check: "clock-offset", Node: int(pl.Peer), Section: "network",
					Summary: fmt.Sprintf("n%d's clock is %s off this node's (tolerated: %s)", pl.Peer, off, max)})
			}
		}
	}

	// Consistency and audit.
	doc.Checks++
	if f := n.consistencyFailures.Load(); f > 0 {
		add(Problem{Severity: SeverityCritical, Check: "consistency-failure", Node: int(n.ident.NodeID), Section: "events",
			Summary: fmt.Sprintf("%d replica checksum mismatch(es) found by this node's sweeps since it started: replicated-state divergence; see the events", f)})
	}
	var authDelta float64
	n.health.prev.auth, authDelta = rateOver(n.health.prev.auth, counterValue(metrics.AuthFailures)+counterValue(metrics.AdminDenied), now)
	if window := now.Sub(n.health.prev.auth[0].at); window > 0 && authDelta/window.Seconds() > authFailureRate {
		add(Problem{Severity: SeverityWarning, Check: "auth-failures", Node: int(n.ident.NodeID), Section: "events",
			Summary: fmt.Sprintf("%d authentication failures or denied admin operations in the last %s on this node", int(authDelta), window.Truncate(time.Second))})
	}

	// Capacity: a store on course to fill, from the recorded free-space
	// series (issue #156). The forecasts are cached and refreshed in the
	// background, so this check reads whatever the last fit produced
	// rather than querying a day of samples on every poll.
	doc.Checks++
	for _, p := range capacityProblems(n.capacityForecasts()) {
		add(p)
	}

	// Certificates: an expiring one is a scheduled outage, so it is
	// reported before it happens (issue #156). Insecure clusters have
	// no certificates and the check finds nothing rather than being
	// skipped, so the count still says it ran.
	doc.Checks++
	certs, _ := n.certs.all()
	for _, p := range certProblems(certs, now) {
		p.Node = int(n.ident.NodeID)
		add(p)
	}

	// Statistics: tables with stale or missing statistics (info).
	doc.Checks++
	// From the cached schema document, refreshed in the background: the
	// panel must answer while the catalog is unreachable.
	n.refreshSchema()
	stale := 0
	if schema := n.cachedSchemaDoc(); schema != nil {
		for _, t := range schema.Tables {
			if t.Stats != nil && t.Stats.Stale && t.Stats.RowCount > 1000 {
				stale++
			}
		}
	}
	if stale > 0 {
		add(Problem{Severity: SeverityInfo, Check: "stale-statistics", Section: "schema",
			Summary: fmt.Sprintf("%d table(s) with over a thousand rows have statistics older than %s; the planner is estimating from stale counts", stale, statsStaleAfter)})
	}

	sort.SliceStable(doc.Problems, func(i, j int) bool {
		return severityRank(doc.Problems[i].Severity) < severityRank(doc.Problems[j].Severity)
	})
	// Gauges: one per (severity, check) pair, zeroed for the pairs that
	// cleared so alerts resolve.
	metrics.HealthProblems.Reset()
	for _, p := range doc.Problems {
		metrics.HealthProblems.WithLabelValues(p.Severity, p.Check).Inc()
	}
	return doc
}

func severityRank(s string) int {
	switch s {
	case SeverityCritical:
		return 0
	case SeverityWarning:
		return 1
	default:
		return 2
	}
}

// counterValue reads a Prometheus counter (the client offers no getter).
func counterValue(c interface{ Write(*dto.Metric) error }) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil || m.Counter == nil {
		return 0
	}
	return m.Counter.GetValue()
}

func fmtBytesGo(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for v := b / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// EventsStatus is the /api/events document.
type EventsStatus struct {
	NodeID int            `json:"node_id"`
	Latest uint64         `json:"latest_seq"`
	Events []events.Event `json:"events"`
	// Operations pairs the ring's start/end records into the operations
	// the cluster is running on itself, in flight first (issue #153).
	Operations []Operation `json:"operations,omitempty"`
	// OldestMs is the timestamp of the oldest record the ring still
	// holds. A caller asking for a window that reaches further back
	// than this is seeing the ring's limit, not a quiet cluster, and
	// the console says so rather than implying nothing happened
	// (issue #155).
	OldestMs int64 `json:"oldest_unix_ms,omitempty"`
}

// Operation is one long-running thing the cluster is doing to itself,
// derived from the ring's paired records (issue #153). The ring stays
// the audit trail; this is a reading of it, not a job store — an
// operation whose start has aged out of the ring simply stops being
// reported, which is why StartedMs is always the record's own time.
type Operation struct {
	Kind    string `json:"kind"`
	Op      string `json:"op"`
	Summary string `json:"summary"`
	// StartedMs and, once it ends, EndedMs and Outcome. Running is the
	// question the flat log could not answer.
	StartedMs int64  `json:"started_unix_ms"`
	EndedMs   int64  `json:"ended_unix_ms,omitempty"`
	Outcome   string `json:"outcome,omitempty"`
	Running   bool   `json:"running,omitempty"`
	// ElapsedMs is how long it ran, or has been running so far. Progress
	// that cannot be known is shown as elapsed time, never as a bar with
	// a number nobody measured.
	ElapsedMs int64 `json:"elapsed_ms"`
}

// operationsFrom pairs the ring's start/end records by (kind, op).
// Running operations come first, newest start first; then the completed
// ones, newest end first.
func operationsFrom(evs []events.Event, nowMs int64) []Operation {
	type key struct{ kind, op string }
	idx := map[key]int{}
	var out []Operation
	for _, ev := range evs {
		if ev.Op == "" {
			continue
		}
		k := key{ev.Kind, ev.Op}
		switch ev.Phase {
		case events.PhaseStart:
			if _, seen := idx[k]; seen {
				continue
			}
			idx[k] = len(out)
			out = append(out, Operation{
				Kind: ev.Kind, Op: ev.Op, Summary: ev.Summary,
				StartedMs: ev.At.UnixMilli(), Running: true,
			})
		case events.PhaseEnd:
			i, seen := idx[k]
			if !seen {
				// The start aged out of the ring: report the end alone
				// rather than dropping it, with no elapsed time to
				// claim.
				out = append(out, Operation{
					Kind: ev.Kind, Op: ev.Op, Summary: ev.Summary,
					EndedMs: ev.At.UnixMilli(), Outcome: ev.Outcome,
				})
				continue
			}
			out[i].EndedMs = ev.At.UnixMilli()
			out[i].Outcome = ev.Outcome
			out[i].Summary = ev.Summary
			out[i].Running = false
		}
	}
	for i := range out {
		switch {
		case out[i].Running:
			out[i].ElapsedMs = nowMs - out[i].StartedMs
		case out[i].StartedMs > 0:
			out[i].ElapsedMs = out[i].EndedMs - out[i].StartedMs
		}
	}
	sort.SliceStable(out, func(a, b int) bool {
		if out[a].Running != out[b].Running {
			return out[a].Running
		}
		if out[a].Running {
			return out[a].StartedMs > out[b].StartedMs
		}
		return out[a].EndedMs > out[b].EndedMs
	})
	return out
}

func (n *Node) serveEventsAPI(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	since, _ := strconv.ParseUint(q.Get("since"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > events.RingSize {
		limit = 200
	}
	p := n.clusterPrincipal(req)
	doc := EventsStatus{NodeID: int(n.ident.NodeID), Latest: n.events.Seq()}
	// A time window (?from=unix_ms) instead of a tail: what the metrics
	// charts need to annotate the range they are drawing (issue #155).
	// The ring is a ring, so the answer also carries how far back it
	// reaches.
	if fromMs, err := strconv.ParseInt(q.Get("from"), 10, 64); err == nil && fromMs > 0 {
		evs, oldest := n.events.Since(time.UnixMilli(fromMs), limit, p.Admin)
		doc.Events = evs
		if !oldest.IsZero() {
			doc.OldestMs = oldest.UnixMilli()
		}
	} else {
		doc.Events = n.events.Recent(since, limit, p.Admin)
	}
	if doc.Events == nil {
		doc.Events = []events.Event{}
	}
	doc.Operations = operationsFrom(n.events.Recent(0, 0, p.Admin), n.clock.Now().WallTime/int64(time.Millisecond))
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

// installAuditSink subscribes the event ring to the audit stream for the
// node's lifetime (alongside, not instead of, any sink a test installs).
func (n *Node) installAuditSink() {
	ring := n.events
	remove := log.AddAuditSink(func(event string, kv []any) {
		var b []byte
		for i := 0; i+1 < len(kv); i += 2 {
			if len(b) > 0 {
				b = append(b, ' ')
			}
			b = append(b, fmt.Sprintf("%v=%v", kv[i], kv[i+1])...)
		}
		ring.RecordAudit(event, string(b))
	})
	n.stopper.AddCloser(remove)
}
