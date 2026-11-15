package server

import (
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/storage/enc"
)

// TestClusterKeyAskerAllowed (issue #310): the liveness rule of a
// hand-over. A node that is not live is answered from anywhere, as under
// peer unseal; a live one only from the host it is registered at.
func TestClusterKeyAskerAllowed(t *testing.T) {
	grace := 15 * time.Second
	resolve := func(h string) []string {
		if h == "db3.example" {
			return []string{"10.0.0.3"}
		}
		return nil
	}
	for _, c := range []struct {
		age              time.Duration
		registered, from string
		ok               bool
	}{
		{20 * time.Second, "10.0.0.3:26257", "10.9.9.9", true}, // not live: from anywhere
		{time.Second, "10.0.0.3:26257", "10.0.0.3", true},      // live, from its own host
		{time.Second, "10.0.0.3:26257", "10.9.9.9", false},     // live, from elsewhere
		{time.Second, "db3.example:26257", "10.0.0.3", true},   // a registered name that resolves to the caller
		{time.Second, "db3.example:26257", "10.0.0.4", false},  // ... and one that does not
		{time.Second, "10.0.0.3:26257", "", false},             // no caller address: refused while live
		{time.Second, "[::1]:26257", "::1", true},
	} {
		ok, why := clusterKeyAskerAllowed(c.age, grace, c.registered, c.from, resolve)
		if ok != c.ok {
			t.Fatalf("age %s, registered %s, from %q: allowed=%t (%s), want %t", c.age, c.registered, c.from, ok, why, c.ok)
		}
		if !ok && !strings.Contains(why, "second copy of a running node") {
			t.Fatalf("refusal says %q", why)
		}
	}
}

// TestClusterKeyMarker: the record names the current version, the ones a
// rotation is leaving and the imported keys, and nothing else.
func TestClusterKeyMarker(t *testing.T) {
	m := &ckMarker{ID: "aa", Version: 3, Check: "c3",
		Previous: []ckCheck{{ClusterKeyRef: enc.ClusterKeyRef{ID: "aa", Version: 2}, Check: "c2"}},
		Imported: []ckCheck{{ClusterKeyRef: enc.ClusterKeyRef{ID: "bb", Version: 1}, Check: "x", Imported: true}}}
	for _, ref := range []enc.ClusterKeyRef{{ID: "aa", Version: 3}, {ID: "aa", Version: 2}, {ID: "bb", Version: 1}} {
		if _, ok := m.find(ref); !ok {
			t.Fatalf("%s not found", ref)
		}
	}
	if _, ok := m.find(enc.ClusterKeyRef{ID: "aa", Version: 1}); ok {
		t.Fatal("a retired version is still named")
	}
	var none *ckMarker
	if none.accepted() != nil {
		t.Fatal("no record names something")
	}
	var p *ckPending
	if _, ok := p.find(enc.ClusterKeyRef{ID: "aa", Version: 1}); ok {
		t.Fatal("no operation names something")
	}
}
