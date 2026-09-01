package server

import (
	"context"
	"encoding/json"
	"math"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server/ui"
	"github.com/sthorne/datax/pkg/util/log"
)

// startHTTP brings up the observability endpoint (--http-listen):
// Prometheus metrics at /metrics, a JSON node overview at /status. TLS'd
// in secure mode.
func (n *Node) startHTTP() error {
	lis := n.cfg.HTTPListener
	if lis == nil {
		if n.cfg.HTTPListen == "" {
			return nil
		}
		var err error
		lis, err = net.Listen("tcp", n.cfg.HTTPListen)
		if err != nil {
			return err
		}
	}
	n.httpAddr = lis.Addr().String()

	// Per-node gauges (closures over this node's store) plus the shared
	// process-wide series.
	nodeReg := prometheus.NewRegistry()
	nodeReg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "datax_ranges", Help: "Replicas hosted by this node.",
		}, func() float64 { return float64(len(n.rangeStatuses())) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "datax_range_leaders", Help: "Ranges this node currently leads.",
		}, func() float64 {
			leaders := 0
			for _, rs := range n.rangeStatuses() {
				if rs.Leader {
					leaders++
				}
			}
			return float64(leaders)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "datax_raft_log_entries", Help: "Raft log entries retained across this node's replicas.",
		}, func() float64 {
			var total uint64
			for _, rs := range n.rangeStatuses() {
				if rs.AppliedIndex > rs.TruncatedIndex {
					total += rs.AppliedIndex - rs.TruncatedIndex
				}
			}
			return float64(total)
		}),
	)
	if eng := n.engine; eng != nil {
		nodeReg.MustRegister(
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_storage_l0_files", Help: "Pebble L0 sstable count.",
			}, func() float64 { return float64(eng.StorageMetrics().L0Files) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_storage_l0_sublevels", Help: "Pebble L0 sublevel count (read amplification of L0).",
			}, func() float64 { return float64(eng.StorageMetrics().L0Sublevels) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_storage_compaction_debt_bytes", Help: "Estimated bytes of pending compaction work.",
			}, func() float64 { return float64(eng.StorageMetrics().CompactionDebtBytes) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_storage_memtable_count", Help: "Memtables held (mutable + queued for flush).",
			}, func() float64 { return float64(eng.StorageMetrics().MemtableCount) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "datax_storage_write_stalls_total", Help: "Pebble hard write-stall events on this store.",
			}, func() float64 { return float64(eng.StorageMetrics().WriteStalls) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "datax_storage_disk_slow_total", Help: "Slow-disk events reported by Pebble.",
			}, func() float64 { return float64(eng.StorageMetrics().DiskSlowEvents) }),
		)
	}
	gatherers := prometheus.Gatherers{metrics.Registry, nodeReg}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{}))
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(n.statusSummary())
	})
	mux.HandleFunc("/api/cluster", n.serveClusterAPI)
	// Cross-node drill-down: fans out over the internode RPC, so it is
	// admin-gated (the read-only endpoints above stay open to any
	// authenticated user).
	mux.Handle("/api/range", n.requireAdmin(http.HandlerFunc(n.serveRangeAPI)))
	// The dashboard, exact path only — anything else 404s rather than
	// serving the page for every typo. Self-contained and read-only.
	page, uerr := ui.FS.ReadFile("index.html")
	if uerr != nil {
		return uerr
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/" {
			http.NotFound(w, req)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(page)
	})

	srv := &http.Server{Handler: n.httpAuth(mux)}
	if n.tlsCfgs != nil {
		srv.TLSConfig = n.tlsCfgs.PGServer.Clone()
		go func() {
			if err := srv.ServeTLS(lis, "", ""); err != http.ErrServerClosed {
				log.Debugf("http server stopped: %v", err)
			}
		}()
	} else {
		go func() {
			if err := srv.Serve(lis); err != http.ErrServerClosed {
				log.Debugf("http server stopped: %v", err)
			}
		}()
	}
	n.stopper.AddCloser(func() { _ = srv.Close() })
	log.Infof("node %s serving the dashboard, /metrics, /status and /api/cluster at %s", n.ident.NodeID, n.httpAddr)
	return nil
}

// httpPrincipal identifies the authenticated caller of an HTTP request:
// the client-certificate CommonName or the HTTP Basic username. The zero
// value means insecure mode (trust, like pgwire).
type httpPrincipal struct {
	User string
	Via  string // "cert" or "basic"; "" in insecure mode
}

type principalCtxKey struct{}

// principalFrom returns the request's authenticated principal (zero in
// insecure mode).
func principalFrom(req *http.Request) httpPrincipal {
	p, _ := req.Context().Value(principalCtxKey{}).(httpPrincipal)
	return p
}

func serveAs(next http.Handler, w http.ResponseWriter, req *http.Request, p httpPrincipal) {
	next.ServeHTTP(w, req.WithContext(context.WithValue(req.Context(), principalCtxKey{}, p)))
}

// httpAuth wraps the WHOLE mux — a route added later is secure by default.
// In secure mode every request must present either HTTP Basic credentials
// matching a stored user's SCRAM verifier, or a CA-verified client
// certificate whose CommonName is the user (the same two doors pgwire
// offers). Any valid user reaches the read-only endpoints; the resolved
// principal rides the request context for per-endpoint authorization
// (requireAdmin) and audit records. Insecure mode passes everything
// through, matching pgwire's trust semantics.
func (n *Node) httpAuth(next http.Handler) http.Handler {
	if n.tlsCfgs == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// A CA-verified client certificate authenticates by CommonName.
		// The TLS config uses VerifyClientCertIfGiven, so VerifiedChains —
		// not the mere presence of TLS state — is the test.
		if req.TLS != nil && len(req.TLS.VerifiedChains) > 0 &&
			len(req.TLS.VerifiedChains[0]) > 0 &&
			req.TLS.VerifiedChains[0][0].Subject.CommonName != "" {
			serveAs(next, w, req, httpPrincipal{User: req.TLS.VerifiedChains[0][0].Subject.CommonName, Via: "cert"})
			return
		}
		user, pass, ok := req.BasicAuth()
		if ok {
			ctx := req.Context()
			verifier, err := n.lookupVerifier(ctx, user)
			if err != nil || verifier == nil {
				// Unknown user (or lookup failure): burn the same work as a
				// real check so the response timing does not reveal whether
				// the user exists.
				verifier = security.DummyVerifier()
			}
			if security.VerifyPassword(verifier, pass) {
				serveAs(next, w, req, httpPrincipal{User: user, Via: "basic"})
				return
			}
			// Credentials were presented and rejected (a bare challenge
			// round-trip is normal browser flow, not an auth failure).
			metrics.AuthFailures.Inc()
			log.Audit("http-auth-failure", "principal", user, "remote", req.RemoteAddr, "path", req.URL.Path)
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="datax"`)
		http.Error(w, "authentication required", http.StatusUnauthorized)
	})
}

// requireAdmin gates an endpoint on the admin role: the authenticated
// principal must be root, an admin-role member, or the cluster's own
// certificate identity ("node"). Insecure mode passes through (trust,
// like everything else). Denials are audited.
func (n *Node) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if n.tlsCfgs == nil {
			next.ServeHTTP(w, req)
			return
		}
		p := principalFrom(req)
		if !n.isAdminPrincipal(req.Context(), p.User) {
			metrics.AdminDenied.Inc()
			log.Audit("admin-denied", "principal", p.User, "via", p.Via, "path", req.URL.Path, "remote", req.RemoteAddr)
			http.Error(w, "admin role required", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, req)
	})
}

// RangeStatus is one replica's view in /status.
type RangeStatus struct {
	RangeID        int64  `json:"range_id"`
	StartKey       string `json:"start_key"`
	EndKey         string `json:"end_key"`
	Replicas       []int  `json:"replicas"`
	Leader         bool   `json:"leader"`
	AppliedIndex   uint64 `json:"applied_index"`
	TruncatedIndex uint64 `json:"truncated_index"`
	SizeBytes      int64  `json:"size_bytes"`
	// QPS is the leader-local measured request rate (~0 on followers).
	QPS         float64 `json:"qps"`
	GCThreshold string  `json:"gc_threshold,omitempty"`
	// ClosedTimestamp is this replica's applied closed timestamp — the
	// newest fixed timestamp it can serve follower reads at. Empty until
	// the first publication applies.
	ClosedTimestamp string `json:"closed_timestamp,omitempty"`
}

// NodeStatus is the /status document.
type NodeStatus struct {
	NodeID    int           `json:"node_id"`
	Address   string        `json:"address"`
	SQLAddr   string        `json:"sql_address,omitempty"`
	Locality  string        `json:"locality,omitempty"`
	Ranges    []RangeStatus `json:"ranges"`
	LeaderOf  int           `json:"leader_of"`
	LiveNodes int           `json:"live_nodes"`
}

func (n *Node) rangeStatuses() []RangeStatus {
	var out []RangeStatus
	n.store.VisitReplicas(func(r *kvserver.Replica) bool {
		desc := r.Desc()
		rs := RangeStatus{
			RangeID:        int64(desc.RangeID),
			StartKey:       desc.StartKey.String(),
			EndKey:         desc.EndKey.String(),
			Leader:         r.IsLeader(),
			AppliedIndex:   r.AppliedIndex(),
			TruncatedIndex: r.TruncatedIndex(),
			SizeBytes:      r.SizeBytes(),
			QPS:            math.Round(r.QPS()*10) / 10,
		}
		if thr := r.GCThreshold(); !thr.IsEmpty() {
			rs.GCThreshold = thr.String()
		}
		if ct := r.ClosedTimestamp(); !ct.IsEmpty() {
			rs.ClosedTimestamp = ct.String()
		}
		for _, rep := range desc.Replicas {
			rs.Replicas = append(rs.Replicas, int(rep.NodeID))
		}
		out = append(out, rs)
		return true
	})
	return out
}

func (n *Node) statusSummary() NodeStatus {
	ranges := n.rangeStatuses()
	st := NodeStatus{
		NodeID:    int(n.ident.NodeID),
		Address:   n.addr,
		SQLAddr:   n.SQLAddr(),
		Locality:  n.cfg.Locality.String(),
		Ranges:    ranges,
		LiveNodes: len(n.registry.All()),
	}
	for _, rs := range ranges {
		if rs.Leader {
			st.LeaderOf++
		}
	}
	return st
}
