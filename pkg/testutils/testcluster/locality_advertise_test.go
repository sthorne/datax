package testcluster

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
)

// tcpForwarder is a second address for a node: it accepts on its own
// loopback port and pipes every connection to the node's listener,
// counting them. It stands in for the node's interface on a private
// network, so a test can tell which of a node's addresses a peer dialed.
type tcpForwarder struct {
	lis      net.Listener
	accepted atomic.Int64
	wg       sync.WaitGroup
}

func newForwarder(t *testing.T, target string) *tcpForwarder {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &tcpForwarder{lis: lis}
	var mu sync.Mutex
	var open []net.Conn
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			in, err := lis.Accept()
			if err != nil {
				return
			}
			f.accepted.Add(1)
			out, err := net.Dial("tcp", target)
			if err != nil {
				_ = in.Close()
				continue
			}
			mu.Lock()
			open = append(open, in, out)
			mu.Unlock()
			pipe := func(dst, src net.Conn) {
				_, _ = io.Copy(dst, src)
				_ = dst.Close()
				_ = src.Close()
			}
			go pipe(out, in)
			go pipe(in, out)
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		f.wg.Wait()
		mu.Lock()
		for _, c := range open {
			_ = c.Close()
		}
		mu.Unlock()
	})
	return f
}

func (f *tcpForwarder) addr() string { return f.lis.Addr().String() }

// waitConnAddrs waits until node from's connection to each peer uses the
// address want[peer].
func waitConnAddrs(t *testing.T, tc *TestCluster, want map[[2]int]string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var bad [][2]int
		for k, addr := range want {
			if tc.Nodes[k[0]].PeerConnAddr(tc.Nodes[k[1]].NodeID()) != addr {
				bad = append(bad, k)
			}
		}
		if len(bad) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for _, k := range bad {
				t.Errorf("n%d dials n%d at %q, want %q", k[0]+1, k[1]+1,
					tc.Nodes[k[0]].PeerConnAddr(tc.Nodes[k[1]].NodeID()), want[k])
			}
			t.FailNow()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// replicatesEverywhere writes through node 0 and waits for every node to
// read the value back.
func replicatesEverywhere(t *testing.T, tc *TestCluster, k, v string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	key := append(keys.TableDataPrefix(871).Clone(), k...)
	if err := tc.Nodes[0].DB().Put(ctx, key, []byte(v)); err != nil {
		t.Fatal(err)
	}
	for i, n := range tc.Nodes {
		got, err := n.DB().Get(ctx, key)
		if err != nil || !bytes.Equal(got, []byte(v)) {
			t.Fatalf("n%d reads %q, %v; want %q", i+1, got, err, v)
		}
	}
}

// TestLocalityAdvertise: two nodes in region=east advertise a second
// address for region=east, and the node in region=west one for
// region=west. The east nodes dial each other at those addresses. Every
// link involving the west node stays on the public addresses: its
// region=west has the key of the east nodes' entries but not the value,
// and the zone=a it shares with n1 is a tier n1 advertised nothing for.
func TestLocalityAdvertise(t *testing.T) {
	fwd := map[base.NodeID]*tcpForwarder{}
	var mu sync.Mutex
	opt := func(cfg *server.Config) {
		id := cfg.StaticBootstrap.NodeID
		f := newForwarder(t, cfg.Listener.Addr().String())
		mu.Lock()
		fwd[id] = f
		mu.Unlock()
		region := "east"
		if id == 3 {
			region = "west"
		}
		cfg.LocalityAddresses = []base.LocalityAddress{{Tier: base.Tier{Key: "region", Value: region}, Address: f.addr()}}
	}
	tc := StartWithOptions(t, 3, opt, "region=east,zone=a", "region=east,zone=b", "region=west,zone=a")

	pub := func(i int) string { return tc.Nodes[i].Addr() }
	waitConnAddrs(t, tc, map[[2]int]string{
		{0, 1}: fwd[2].addr(), {1, 0}: fwd[1].addr(),
		{0, 2}: pub(2), {1, 2}: pub(2), {2, 0}: pub(0), {2, 1}: pub(1),
	})
	replicatesEverywhere(t, tc, "k", "v")

	if fwd[1].accepted.Load() == 0 || fwd[2].accepted.Load() == 0 {
		t.Fatalf("the east nodes' locality addresses saw %d and %d connections, want some", fwd[1].accepted.Load(), fwd[2].accepted.Load())
	}
	if got := fwd[3].accepted.Load(); got != 0 {
		t.Fatalf("n3's region=west address saw %d connections; no peer shares that tier", got)
	}
	if d, f := tc.Nodes[0].LocalityDialStats(); d == 0 || f != 0 {
		t.Fatalf("n1 locality stats: dials %d fallbacks %d", d, f)
	}

	// The entries are published: every node's registry has its peers'.
	nd, ok := tc.Nodes[2].Registry().Get(1)
	if !ok || base.FormatLocalityAddresses(nd.LocalityAddresses) != "region=east@"+fwd[1].addr() {
		t.Fatalf("n3's registry row for n1: %+v", nd)
	}
}

// TestLocalityAdvertiseFallback: every node advertises a locality address
// nothing listens on. The dials to it fail, each node falls back to its
// peers' public addresses, and the cluster works over those.
func TestLocalityAdvertiseFallback(t *testing.T) {
	opt := func(cfg *server.Config) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		dead := lis.Addr().String()
		_ = lis.Close()
		cfg.LocalityAddresses = []base.LocalityAddress{{Tier: base.Tier{Key: "region", Value: "east"}, Address: dead}}
	}
	tc := StartWithOptions(t, 3, opt, "region=east", "region=east", "region=east")

	want := map[[2]int]string{}
	for i := range tc.Nodes {
		for j := range tc.Nodes {
			if i != j {
				want[[2]int{i, j}] = tc.Nodes[j].Addr()
			}
		}
	}
	replicatesEverywhere(t, tc, "k", "v")
	waitConnAddrs(t, tc, want)
	for i, n := range tc.Nodes {
		if d, f := n.LocalityDialStats(); d != 0 || f == 0 {
			t.Fatalf("n%d locality stats: dials %d fallbacks %d; want none dialed and some fallbacks", i+1, d, f)
		}
	}
}
