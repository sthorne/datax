package main

import (
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/base"
)

// TestLocalityAdvertiseFlag: --locality-advertise (and its alias
// --locality-advertise-addr) reaches the config parsed against --locality,
// and a bad value is a startup error naming the flag.
func TestLocalityAdvertiseFlag(t *testing.T) {
	parse := func(args ...string) (*serverFlags, error) {
		f := newServerFlags("start")
		f.fs.SetOutput(io.Discard)
		return f, f.fs.Parse(args)
	}
	want := []base.LocalityAddress{
		{Tier: base.Tier{Key: "region", Value: "us-mia"}, Address: "192.168.144.22:26257"},
		{Tier: base.Tier{Key: "zone", Value: "a"}, Address: "10.1.0.22:26257"},
	}
	for _, name := range []string{"--locality-advertise", "--locality-advertise-addr"} {
		f, err := parse("--locality=region=us-mia,zone=a", name+"=region=us-mia@192.168.144.22:26257,zone=a@10.1.0.22:26257", "--advertise=203.0.113.7:26257")
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := f.config(false)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(cfg.LocalityAddresses, want) || cfg.AdvertiseAddr != "203.0.113.7:26257" {
			t.Fatalf("%s: config has %+v (advertise %q)", name, cfg.LocalityAddresses, cfg.AdvertiseAddr)
		}
	}

	// Without the flag there are no entries.
	f, err := parse("--locality=region=us-mia")
	if err != nil {
		t.Fatal(err)
	}
	if cfg, err := f.config(false); err != nil || cfg.LocalityAddresses != nil {
		t.Fatalf("no flag: %+v %v", cfg.LocalityAddresses, err)
	}

	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"--locality=region=us-mia", "--locality-advertise=region=us-mia"}, "expected key=value@host:port"},
		{[]string{"--locality=region=us-mia", "--locality-advertise=region=us-atl@10.0.0.1:26257"}, "tier region=us-atl is not one of this node's --locality tiers"},
		{[]string{"--locality-advertise=region=us-mia@10.0.0.1:26257"}, "--locality is not set"},
		{[]string{"--locality=region=us-mia", "--locality-advertise=region=us-mia@10.0.0.1"}, "invalid address"},
		{[]string{"--locality=region=us-mia", "--locality-advertise=region=us-mia@10.0.0.1:1,region=us-mia@10.0.0.2:1"}, "duplicate tier region=us-mia"},
	} {
		f, err := parse(c.args...)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.config(false)
		if err == nil || !strings.HasPrefix(err.Error(), "--locality-advertise: ") || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: got %v, want a --locality-advertise error containing %q", c.args, err, c.want)
		}
	}
}
