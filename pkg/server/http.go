package server

import (
	"encoding/json"
	"math"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
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

	srv := &http.Server{Handler: mux}
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
	log.Infof("node %s serving /metrics and /status at %s", n.ident.NodeID, n.httpAddr)
	return nil
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
