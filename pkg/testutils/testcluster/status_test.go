package testcluster

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/server"
)

// TestMetricsAndStatus: /metrics serves Prometheus series whose counters
// move with activity, and /status reports the node's ranges.
func TestMetricsAndStatus(t *testing.T) {
	httplis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n, err := server.Start(server.Config{
		Listener:     lis,
		HTTPListener: httplis,
		StaticBootstrap: &server.StaticBootstrap{
			ClusterID: uuid.New(),
			NodeID:    1,
			Range1:    cluster.Range1Descriptor([]base.NodeID{1}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scrape := func(path string) string {
		t.Helper()
		resp, err := http.Get(fmt.Sprintf("http://%s%s", n.HTTPAddr(), path))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	metricValue := func(body, name string) float64 {
		t.Helper()
		m := regexp.MustCompile(`(?m)^` + name + ` ([0-9.e+-]+)$`).FindStringSubmatch(body)
		if m == nil {
			t.Fatalf("metric %s not found in scrape", name)
		}
		v, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}

	before := scrape("/metrics")
	commitsBefore := metricValue(before, "datax_txn_commits_total")

	k := append(keys.TableDataPrefix(770), "k"...)
	for i := 0; i < 5; i++ {
		if err := n.DB().RunTxn(ctx, "m", func(ctx context.Context, txn *kvclient.Txn) error {
			return txn.Put(ctx, k, []byte("v"))
		}); err != nil {
			t.Fatal(err)
		}
	}

	after := scrape("/metrics")
	if got := metricValue(after, "datax_txn_commits_total"); got < commitsBefore+5 {
		t.Fatalf("txn commits did not move: %v -> %v", commitsBefore, got)
	}
	if v := metricValue(after, "datax_ranges"); v < 1 {
		t.Fatalf("datax_ranges = %v", v)
	}
	if v := metricValue(after, "datax_range_leaders"); v < 1 {
		t.Fatalf("datax_range_leaders = %v", v)
	}
	// The latency histogram observed the KV batches.
	if v := metricValue(after, "datax_kv_batch_latency_seconds_count"); v <= 0 {
		t.Fatalf("kv latency histogram empty: %v", v)
	}

	// /status: schema sanity.
	var st server.NodeStatus
	if err := jsonUnmarshal([]byte(scrape("/status")), &st); err != nil {
		t.Fatal(err)
	}
	if st.NodeID != 1 || len(st.Ranges) < 1 || st.LeaderOf < 1 {
		t.Fatalf("status: %+v", st)
	}
	if st.Ranges[0].AppliedIndex == 0 {
		t.Fatalf("range status missing applied index: %+v", st.Ranges[0])
	}
}
