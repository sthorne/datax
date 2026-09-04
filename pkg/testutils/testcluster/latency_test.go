package testcluster

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestInterNodeLatency (issue #82): every node measures its round trip
// and clock offset to every peer, the matrix reaches /api/cluster from
// every node, a partitioned peer reads as unreachable within a few ping
// intervals and recovers, a node whose clock runs ahead shows up as a
// positive offset on its peers (and a negative one from its own view),
// and /metrics carries the series.
func TestInterNodeLatency(t *testing.T) {
	const skew = 100 * time.Millisecond // well under the 500 ms tolerance
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nodeIndex := 0
	tc := StartWithOptions(t, 3, func(cfg *server.Config) {
		i := nodeIndex
		nodeIndex++
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
		if i == 2 {
			cfg.Clock = hlc.NewClock(func() int64 { return time.Now().UnixNano() + int64(skew) }, base.DefaultMaxClockOffset)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	_ = ctx

	rowOf := func(doc server.ClusterStatus, id int) map[base.NodeID]kvpb.PeerLatency {
		for _, cn := range doc.Nodes {
			if cn.NodeID == id {
				m := map[base.NodeID]kvpb.PeerLatency{}
				for _, l := range cn.Latency {
					m[l.Peer] = l
				}
				return m
			}
		}
		return nil
	}
	fetch := func() server.ClusterStatus {
		t.Helper()
		_, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/cluster")
		var doc server.ClusterStatus
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	waitFor := func(what string, cond func(server.ClusterStatus) bool) server.ClusterStatus {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			doc := fetch()
			if cond(doc) {
				return doc
			}
			if time.Now().After(deadline) {
				b, _ := json.MarshalIndent(doc.Nodes, "", "  ")
				t.Fatalf("%s never happened; nodes:\n%s", what, b)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}

	// Every row full and reachable.
	doc := waitFor("full latency matrix", func(doc server.ClusterStatus) bool {
		if doc.MaxOffsetMs != base.DefaultMaxClockOffset.Milliseconds() {
			return false
		}
		for id := 1; id <= 3; id++ {
			row := rowOf(doc, id)
			if len(row) != 2 {
				return false
			}
			for peer, l := range row {
				if int(peer) == id || !l.Reachable || l.AgeMillis < 0 {
					return false
				}
			}
		}
		return true
	})
	for id := 1; id <= 3; id++ {
		for peer, l := range rowOf(doc, id) {
			if l.RTTMicros < 0 || l.RTTMicros > 500000 || l.P99Micros < l.RTTMicros/4 {
				t.Fatalf("n%d→n%d: implausible rtt %dµs (p99 %dµs)", id, peer, l.RTTMicros, l.P99Micros)
			}
		}
	}

	// n3's clock runs 100 ms ahead: its peers see +100 ms, it sees -100 ms.
	within := func(got, want time.Duration) bool {
		d := got - want
		return d > -40*time.Millisecond && d < 40*time.Millisecond
	}
	for _, id := range []int{1, 2} {
		l := rowOf(doc, id)[3]
		if !within(time.Duration(l.OffsetMicros)*time.Microsecond, skew) {
			t.Fatalf("n%d measures n3's offset as %dµs, want about +%s", id, l.OffsetMicros, skew)
		}
		back := rowOf(doc, 3)[base.NodeID(id)]
		if !within(time.Duration(back.OffsetMicros)*time.Microsecond, -skew) {
			t.Fatalf("n3 measures n%d's offset as %dµs, want about -%s", id, back.OffsetMicros, skew)
		}
	}
	// Two nodes on the same clock see each other within the noise.
	if l := rowOf(doc, 1)[2]; !within(time.Duration(l.OffsetMicros)*time.Microsecond, 0) {
		t.Fatalf("n1 measures n2's offset as %dµs, want about 0", l.OffsetMicros)
	}

	// Partition n1 → n3: n1's row marks n3 unreachable; n3 still reaches
	// n1 (the drop is one-directional), and n2 is unaffected.
	tc.Nodes[0].InjectRPCDrop(func(to base.NodeID) bool { return to == 3 })
	waitFor("n3 unreachable from n1", func(doc server.ClusterStatus) bool {
		l, ok := rowOf(doc, 1)[3]
		return ok && !l.Reachable && rowOf(doc, 1)[2].Reachable
	})
	tc.Nodes[0].InjectRPCDrop(nil)
	waitFor("n3 reachable from n1 again", func(doc server.ClusterStatus) bool {
		l, ok := rowOf(doc, 1)[3]
		return ok && l.Reachable
	})

	// /metrics on n1 carries the series per peer.
	_, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/metrics")
	for _, want := range []string{`datax_rpc_rtt_seconds_count{peer="n2"}`, `datax_clock_offset_seconds{peer="n3"}`, `datax_peer_reachable{peer="n3"} 1`} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics lacks %s", want)
		}
	}
}
