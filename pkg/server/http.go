package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/pgwire"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server/ui"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/util/sysstats"
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
			Name: "datax_quiescent_ranges", Help: "Replicas on this node that are quiescent: idle, not ticking or heartbeating.",
		}, func() float64 {
			q := 0
			for _, rs := range n.rangeStatuses() {
				if rs.Quiescent {
					q++
				}
			}
			return float64(q)
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
	// The host: the standard Go and process collectors (goroutines, heap,
	// GC, process CPU seconds, RSS, open fds) plus datax gauges for what
	// only the host can tell — host CPU, load, memory, the store disk and
	// the network — all read from the node's sampler.
	nodeReg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if sys := n.sys; sys != nil {
		latest := func() sysstats.Sample { return sys.Latest() }
		cpu := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "datax_node_cpu_percent", Help: "Host CPU busy over the last sampling interval, percent of all cores (scope=host), and this process's share of one core (scope=process).",
		}, []string{"scope"})
		mem := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "datax_node_memory_bytes", Help: "Host memory: kind=total, available (as the kernel defines it), process_rss.",
		}, []string{"kind"})
		disk := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "datax_store_disk_bytes", Help: "The store directory's filesystem: kind=total, free.",
		}, []string{"kind"})
		diskIO := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "datax_store_disk_io_bytes_per_second", Help: "Throughput of the block device backing the store over the last sampling interval: op=read, write.",
		}, []string{"op"})
		net := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "datax_node_net_bytes_per_second", Help: "Network throughput over every non-loopback interface: dir=rx, tx.",
		}, []string{"dir"})
		nodeReg.MustRegister(cpu, mem, disk, diskIO, net,
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_node_load1", Help: "One-minute load average.",
			}, func() float64 { return latest().Load1 }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_node_load5", Help: "Five-minute load average.",
			}, func() float64 { return latest().Load5 }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_node_load15", Help: "Fifteen-minute load average.",
			}, func() float64 { return latest().Load15 }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_node_cores", Help: "Logical CPUs on the host.",
			}, func() float64 { return float64(latest().Cores) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_store_disk_busy_percent", Help: "Share of the last sampling interval the store's block device had I/O in flight.",
			}, func() float64 { return latest().DiskBusyPercent }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_process_fd_limit", Help: "Soft limit on open file descriptors (Pebble holds one per open sstable).",
			}, func() float64 { return float64(latest().FDLimit) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_process_uptime_seconds", Help: "Seconds since the node started.",
			}, func() float64 { return float64(latest().ProcessUp) }),
		)
		// Vectors have no Func form; refresh them from each sample, and
		// seed them now so the first scrape already carries every series.
		update := func(m sysstats.Sample) {
			cpu.WithLabelValues("host").Set(m.CPUPercent)
			cpu.WithLabelValues("process").Set(m.ProcessCPUPercent)
			mem.WithLabelValues("total").Set(float64(m.MemTotal))
			mem.WithLabelValues("available").Set(float64(m.MemAvailable))
			mem.WithLabelValues("process_rss").Set(float64(m.RSS))
			disk.WithLabelValues("total").Set(float64(m.DiskTotal))
			disk.WithLabelValues("free").Set(float64(m.DiskFree))
			diskIO.WithLabelValues("read").Set(m.DiskReadBytesPS)
			diskIO.WithLabelValues("write").Set(m.DiskWriteBytesPS)
			net.WithLabelValues("rx").Set(m.NetRxBytesPS)
			net.WithLabelValues("tx").Set(m.NetTxBytesPS)
		}
		sys.OnSample(update)
		update(latest())
	}
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
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_storage_block_cache_bytes", Help: "Bytes held by the block cache (shared by the process's engines).",
			}, func() float64 { return float64(eng.StorageMetrics().BlockCacheBytes) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_storage_block_cache_size_bytes", Help: "The block cache's configured size (--cache-size or the profile's share of memory).",
			}, func() float64 { return float64(eng.CacheSize()) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "datax_storage_block_cache_hits_total", Help: "Block cache hits; with misses, the hit rate that sizes --cache-size.",
			}, func() float64 { return float64(eng.StorageMetrics().BlockCacheHits) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "datax_storage_block_cache_misses_total", Help: "Block cache misses.",
			}, func() float64 { return float64(eng.StorageMetrics().BlockCacheMisses) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "datax_storage_bloom_hits_total", Help: "Point reads a bloom filter answered without reading a data block.",
			}, func() float64 { return float64(eng.StorageMetrics().FilterHits) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "datax_storage_bloom_misses_total", Help: "Point reads the bloom filters could not answer (the key may be present).",
			}, func() float64 { return float64(eng.StorageMetrics().FilterMisses) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "datax_storage_disk_slow_total", Help: "Slow-disk events reported by Pebble.",
			}, func() float64 { return float64(eng.StorageMetrics().DiskSlowEvents) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_storage_debt_gate", Help: "1 while the compaction-debt backpressure gate is latched (hysteresis: enters above the profile's high-water debt, exits below the low).",
			}, func() float64 {
				// The latch is re-evaluated on snapshot refresh, and refreshes
				// otherwise ride write/raft traffic — an idle node would report
				// its last latched value forever. StorageMetrics refreshes at
				// most once per second, so scraping is rate-limited by design.
				_ = eng.StorageMetrics()
				if eng.DebtGated() {
					return 1
				}
				return 0
			}),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "datax_storage_debt_gate_entered_total", Help: "Times the compaction-debt gate latched.",
			}, func() float64 { return float64(eng.DebtGateEntries()) }),
		)
		// Bytes written by each engine since it opened: WAL, memtable
		// flushes and compactions — the write amplification a split store
		// (issue #105) is about. engine=state is the state machine (no
		// WAL bytes once split), engine=raft the raft log.
		written := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "datax_storage_bytes_written_total", Help: "Bytes an engine wrote since it opened: kind=wal, flush, compaction; engine=state (the state machine), raft (the raft log, split stores only).",
		}, []string{"engine", "kind"})
		nodeReg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "datax_storage_split", Help: "1 when the raft log lives on its own engine and the state engine runs without a WAL (issue #105).",
		}, func() float64 {
			if n.raftEngine != nil {
				return 1
			}
			return 0
		}))
		nodeReg.MustRegister(&writtenCollector{vec: written, node: n})
		if eng.Encrypted() {
			nodeReg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_reencryption_remaining_bytes", Help: "Live sstable bytes still encrypted under retired data keys (0 = every live sstable rides the active key).",
			}, func() float64 {
				remaining, _, _ := eng.ReencryptionStatus()
				return float64(remaining)
			}))
		}
		if eng.PrefixBloom() {
			nodeReg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "datax_prefix_bloom_remaining_bytes", Help: "Live sstable bytes still carrying whole-key bloom filters after the store entered prefix mode (0 = every point read can be answered by the filters; cluster version v15, issue #161).",
			}, func() float64 {
				remaining, _, _ := eng.FilterRewriteStatus()
				return float64(remaining)
			}))
		}
	}
	// Certificate expiry (issue #156): registered only in secure mode,
	// so an insecure cluster reports no series rather than a series
	// asserting nothing expires.
	if n.tlsCfgs != nil {
		nodeReg.MustRegister(newCertExpiryCollector(n))
	}
	gatherers := prometheus.Gatherers{metrics.Registry, nodeReg}

	mux := http.NewServeMux()
	mux.Handle("/metrics", n.requireMetrics(promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})))
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		n.refreshSchema() // range labels, without waiting on the catalog
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
	mux.HandleFunc("/api/schema", n.serveSchemaAPI)
	mux.Handle("/api/activity", n.requireAdmin(http.HandlerFunc(n.serveActivityAPI)))
	// Profiles (issue #100): net/http/pprof under /debug/pprof/, admin-gated
	// like the drill-downs — a profile exposes statement text and key
	// bytes. `datax debug profile` fetches one; `datax bench
	// --server-profile` pulls a CPU profile for a run.
	mux.Handle("/debug/pprof/", n.requireAdmin(http.HandlerFunc(pprof.Index)))
	mux.Handle("/debug/pprof/cmdline", n.requireAdmin(http.HandlerFunc(pprof.Cmdline)))
	mux.Handle("/debug/pprof/profile", n.requireAdmin(http.HandlerFunc(pprof.Profile)))
	mux.Handle("/debug/pprof/symbol", n.requireAdmin(http.HandlerFunc(pprof.Symbol)))
	mux.Handle("/debug/pprof/trace", n.requireAdmin(http.HandlerFunc(pprof.Trace)))
	mux.HandleFunc("/api/health", n.serveHealthAPI)
	// /api/security is not gated as a whole (issue #156): certificate
	// expiry and role membership are operational and belong to any
	// authenticated user. What names people — the per-user connection
	// breakdown and the client certificates observed — is filtered out
	// of the document for a non-admin, the way the event ring already
	// filters audit records.
	mux.HandleFunc("/api/security", n.serveSecurityAPI)
	mux.HandleFunc("/api/overview", n.serveOverviewAPI)
	// The console's front door (issue #158). Both are reached before
	// authentication (httpAuth exempts them): signing in is how a
	// principal is established, and signing out of a session that has
	// already lapsed must still clear the cookie.
	mux.HandleFunc("/api/login", n.serveLogin)
	mux.HandleFunc("/api/logout", n.serveLogout)
	mux.HandleFunc("/api/metrics", n.serveMetricsAPI)
	mux.HandleFunc("/api/node", n.serveNodeAPI)
	mux.HandleFunc("/api/events", n.serveEventsAPI)
	// The dashboard, exact path only — anything else 404s rather than
	// serving the page for every typo. Self-contained and read-only.
	page, uerr := renderConsolePage()
	if uerr != nil {
		return uerr
	}
	// The console's version is a digest of the page as this binary
	// embeds it: the page carries it, every /api/cluster document
	// carries it, and the page offers a reload when they differ — a tab
	// left open across a rolling upgrade otherwise keeps running the old
	// console against the new API (issue #146). It is also the page's
	// ETag, so a reload after an upgrade fetches the new page and one
	// before it is a 304.
	if err := n.renderLoginPage(); err != nil {
		return err
	}
	n.consoleVersion = consoleVersionOf(page)
	page = bytes.ReplaceAll(page, []byte(consoleVersionPlaceholder), []byte(n.consoleVersion))
	etag := `"` + n.consoleVersion + `"`
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/" {
			http.NotFound(w, req)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Cache-Control", "no-cache")
		if strings.Contains(req.Header.Get("If-None-Match"), etag) {
			w.WriteHeader(http.StatusNotModified)
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
		// The sign-in endpoints are the way a principal is established,
		// so they run ahead of the check (issue #158). They authenticate
		// their own callers: /api/login verifies the password it is
		// given, and /api/logout only clears the caller's own cookie.
		if req.URL.Path == "/api/login" || req.URL.Path == "/api/logout" {
			next.ServeHTTP(w, req)
			return
		}
		// A CA-verified client certificate authenticates by CommonName.
		// The TLS config uses VerifyClientCertIfGiven, so VerifiedChains —
		// not the mere presence of TLS state — is the test.
		if req.TLS != nil && len(req.TLS.VerifiedChains) > 0 &&
			len(req.TLS.VerifiedChains[0]) > 0 &&
			req.TLS.VerifiedChains[0][0].Subject.CommonName != "" {
			cn := req.TLS.VerifiedChains[0][0].Subject.CommonName
			// What reached this node, expiry included, is worth
			// reporting whether or not the role check below admits it.
			n.certs.observeClient(req.TLS.VerifiedChains[0][0])
			// The certificate names the role; the role must exist and
			// hold LOGIN, as on pgwire (issue #138) — so NOLOGIN or DROP
			// ROLE closes this door too, years before the certificate
			// expires. The cluster's own identity ("node", a reserved
			// name with no descriptor) is admitted as it is everywhere.
			if cn == security.NodePrincipal {
				serveAs(next, w, req, httpPrincipal{User: cn, Via: "cert"})
				return
			}
			if ok, err := n.canLogin(req.Context(), cn); err == nil && ok {
				serveAs(next, w, req, httpPrincipal{User: cn, Via: "cert"})
				return
			}
			// Refused: Basic credentials, if any, still get their turn.
			metrics.AuthFailures.Inc()
			log.Audit("http-auth-failure", "principal", cn, "via", "cert", "remote", req.RemoteAddr, "path", req.URL.Path)
		}
		// A session cookie, which a person gets from the login page. It
		// sits behind the certificate (a certificate is the stronger
		// claim and the one machines use) and ahead of Basic, so a
		// signed-in browser stops replaying a password on every poll.
		if p, present := n.sessionPrincipal(req); present {
			if p.User != "" {
				serveAs(next, w, req, p)
				return
			}
			// Present but not valid: expired, forged, or a role that may
			// no longer sign in (sessionPrincipal audited which). Clear
			// it so the browser stops sending it, then fall through —
			// Basic credentials or a certificate may still carry this
			// request.
			clearSessionCookie(w)
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
		// A browser navigation gets the login page; anything scripted
		// gets the challenge it has always got, so Prometheus, curl and
		// `datax debug` are unaffected (issue #158). The test is the
		// Accept header, never the user agent.
		if wantsHTML(req) {
			n.serveLoginPage(w, req)
			return
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

// requireMetrics gates /metrics on the metrics role (or admin): a scrape
// account is a member of metrics and needs nothing else. Insecure mode
// passes through; denials are audited.
func (n *Node) requireMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if n.tlsCfgs == nil {
			next.ServeHTTP(w, req)
			return
		}
		p := principalFrom(req)
		if !n.isMetricsPrincipal(req.Context(), p.User) {
			metrics.AdminDenied.Inc()
			log.Audit("admin-denied", "principal", p.User, "via", p.Via, "path", req.URL.Path, "remote", req.RemoteAddr, "needs", catalog.MetricsRole)
			http.Error(w, "metrics role required", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, req)
	})
}

// RangeStatus is one replica's view in /status.
type RangeStatus struct {
	RangeID  int64  `json:"range_id"`
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
	Table    string `json:"table,omitempty"`
	Replicas []int  `json:"replicas"`
	Leader   bool   `json:"leader"`
	// Quiescent: the replica is asleep (no ticks, no heartbeats) until a
	// message or request wakes it.
	Quiescent      bool   `json:"quiescent,omitempty"`
	AppliedIndex   uint64 `json:"applied_index"`
	LastIndex      uint64 `json:"last_index"`
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
	Quiescent int           `json:"quiescent_ranges"`
	LiveNodes int           `json:"live_nodes"`
	// Machine is the node's latest host sample (CPU, memory, the store
	// disk, network, file descriptors, Go runtime); nil before the first
	// sample.
	Machine *sysstats.Sample `json:"machine,omitempty"`
}

func (n *Node) rangeStatuses() []RangeStatus {
	var out []RangeStatus
	n.store.VisitReplicas(func(r *kvserver.Replica) bool {
		desc := r.Desc()
		rs := RangeStatus{
			RangeID:        int64(desc.RangeID),
			StartKey:       n.prettyKey(desc.StartKey),
			EndKey:         n.prettyKey(desc.EndKey),
			Table:          n.tableNameOf(desc.StartKey),
			Leader:         r.IsLeader(),
			Quiescent:      r.Quiescent(),
			AppliedIndex:   r.AppliedIndex(),
			LastIndex:      r.LastIndex(),
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
	if n.sys != nil {
		if m := n.sys.Latest(); !m.At.IsZero() {
			st.Machine = &m
		}
	}
	for _, rs := range ranges {
		if rs.Leader {
			st.LeaderOf++
		}
		if rs.Quiescent {
			st.Quiescent++
		}
	}
	return st
}

// ActivityStatus is the /api/activity document: this node's SQL
// connections and the statements in flight and recently slow. Statement
// text can carry data, so the route is admin-only, and it covers the
// serving node (each node keeps its own).
type ActivityStatus struct {
	NodeID      int                      `json:"node_id"`
	Summary     *kvpb.SQLSummary         `json:"summary,omitempty"`
	Connections []pgwire.ConnectionInfo  `json:"connections"`
	Active      []pgwire.ActiveStatement `json:"active"`
	Slow        []pgwire.SlowStatement   `json:"slow"`
	// SlowThresholdMillis is the duration past which statements land in
	// Slow.
	SlowThresholdMillis int64 `json:"slow_threshold_ms"`

	// Contention (issue #154). A serializable database lives and dies
	// on retries, and a 40001 rate on its own says nothing about what
	// to change. RetryShapes attributes this node's serialization
	// failures to the statement shapes that produced them, heaviest
	// first, with RetryShapesOther counting those past the bound;
	// RetriesByUser breaks them down by user, because contention is
	// usually one application. IdleTxns lists the sessions sitting
	// inside an open transaction, whose intents block other writers.
	//
	// All of it names statement text or users, so it stays behind the
	// same admin gate as the rest of this document. The rate figures
	// the console draws beside it come from /api/metrics, which is not
	// gated: a rate is not sensitive, a statement is.
	RetryShapes      []pgwire.RetryShape `json:"retry_shapes"`
	RetryShapesOther uint64              `json:"retry_shapes_other,omitempty"`
	RetriesByUser    map[string]uint64   `json:"retries_by_user,omitempty"`
	IdleTxns         []pgwire.IdleTxn    `json:"idle_txns"`
}

// retryShapeLimit bounds the hot list this document carries. What falls
// off the end is added to RetryShapesOther rather than dropped.
const retryShapeLimit = 20

func (n *Node) serveActivityAPI(w http.ResponseWriter, req *http.Request) {
	doc := n.activityStatus()
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

// writtenCollector reports the engines' cumulative write counters on each
// scrape (a counter vector whose values come from Pebble, not from
// increments).
type writtenCollector struct {
	vec  *prometheus.CounterVec
	node *Node
}

func (c *writtenCollector) Describe(ch chan<- *prometheus.Desc) { c.vec.Describe(ch) }

func (c *writtenCollector) Collect(ch chan<- prometheus.Metric) {
	set := func(engine string, eng *storage.Engine) {
		if eng == nil {
			return
		}
		w := eng.WriteMetrics()
		for kind, v := range map[string]uint64{"wal": w.WALBytes, "flush": w.FlushedBytes, "compaction": w.CompactedBytes} {
			ch <- prometheus.MustNewConstMetric(c.vec.WithLabelValues(engine, kind).Desc(), prometheus.CounterValue, float64(v), engine, kind)
		}
	}
	set("state", c.node.engine)
	set("raft", c.node.raftEngine)
}

// consoleVersionPlaceholder is the token in index.html the served page
// has replaced by its version.
const consoleVersionPlaceholder = "__CONSOLE_VERSION__"

// consoleVersionOf digests the embedded page.
func consoleVersionOf(page []byte) string {
	sum := sha256.Sum256(page)
	return hex.EncodeToString(sum[:8])
}

// consoleScriptsPlaceholder is the token in index.html that the
// console's script files replace.
const consoleScriptsPlaceholder = "__CONSOLE_SCRIPTS__"

// renderConsolePage assembles the served console: the shell with its
// script files concatenated in, in the order ui.ScriptFiles gives (issue
// #151). A file that the embed directive names but that is missing
// fails the build; one that is present but empty, or a shell that lost
// its placeholder, fails here rather than serving a blank console.
func renderConsolePage() ([]byte, error) {
	shell, err := ui.FS.ReadFile("index.html")
	if err != nil {
		return nil, err
	}
	if !bytes.Contains(shell, []byte(consoleScriptsPlaceholder)) {
		return nil, fmt.Errorf("the console shell has no %s placeholder", consoleScriptsPlaceholder)
	}
	names, err := ui.ScriptFiles()
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("the console has no script files")
	}
	var script bytes.Buffer
	for _, name := range names {
		body, err := ui.FS.ReadFile(name)
		if err != nil {
			return nil, err
		}
		if len(bytes.TrimSpace(body)) == 0 {
			return nil, fmt.Errorf("console script %s is empty", name)
		}
		fmt.Fprintf(&script, "\n// ==== %s ====\n", name)
		script.Write(body)
	}
	return bytes.Replace(shell, []byte(consoleScriptsPlaceholder), script.Bytes(), 1), nil
}
