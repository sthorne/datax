package base

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseLocalityAddresses(t *testing.T) {
	loc, err := ParseLocality("region=us-mia,zone=a")
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseLocalityAddresses("region=us-mia@192.168.144.22:26257, zone=a@[fd00::1]:26257", loc)
	if err != nil {
		t.Fatal(err)
	}
	want := []LocalityAddress{
		{Tier: Tier{Key: "region", Value: "us-mia"}, Address: "192.168.144.22:26257"},
		{Tier: Tier{Key: "zone", Value: "a"}, Address: "[fd00::1]:26257"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if s := FormatLocalityAddresses(got); s != "region=us-mia@192.168.144.22:26257,zone=a@[fd00::1]:26257" {
		t.Fatalf("format: %q", s)
	}
	if got, err := ParseLocalityAddresses("", loc); err != nil || got != nil {
		t.Fatalf("empty: %v %v", got, err)
	}

	for _, c := range []struct{ in, loc, want string }{
		{"region=us-mia", "region=us-mia", "expected key=value@host:port"},
		{"region@10.0.0.1:1", "region=us-mia", "expected key=value@host:port"},
		{"@10.0.0.1:1", "region=us-mia", "expected key=value@host:port"},
		{"region=us-mia@", "region=us-mia", "expected key=value@host:port"},
		{"region=us-mia@10.0.0.1:1,", "region=us-mia", "expected key=value@host:port"},
		{"region=us-mia,zone=a@10.0.0.1:1", "region=us-mia,zone=a", "expected key=value@host:port"},
		{"region=us-atl@10.0.0.1:1", "region=us-mia", "tier region=us-atl is not one of this node's --locality tiers (region=us-mia)"},
		{"rack=1@10.0.0.1:1", "region=us-mia", "tier rack=1 is not one of"},
		{"region=us-mia@10.0.0.1:1", "", "--locality is not set"},
		{"region=us-mia@10.0.0.1", "region=us-mia", "invalid address \"10.0.0.1\""},
		{"region=us-mia@:26257", "region=us-mia", "missing host"},
		{"region=us-mia@10.0.0.1:http", "region=us-mia", "is not a number"},
		{"region=us-mia@10.0.0.1:70000", "region=us-mia", "is not a number"},
		{"region=us-mia@10.0.0.1:1,region=us-mia@10.0.0.2:1", "region=us-mia", "duplicate tier region=us-mia"},
	} {
		l, err := ParseLocality(c.loc)
		if err != nil {
			t.Fatal(err)
		}
		_, err = ParseLocalityAddresses(c.in, l)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("ParseLocalityAddresses(%q, %q) = %v, want an error containing %q", c.in, c.loc, err, c.want)
		}
	}
}

func TestSelectLocalityAddress(t *testing.T) {
	la := func(k, v, addr string) LocalityAddress {
		return LocalityAddress{Tier: Tier{Key: k, Value: v}, Address: addr}
	}
	mustLoc := func(s string) Locality {
		l, err := ParseLocality(s)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	for _, c := range []struct {
		name    string
		entries []LocalityAddress
		local   string
		want    string
		wantOK  bool
	}{
		{"shared tier", []LocalityAddress{la("region", "us-mia", "10.0.0.1:26257")}, "region=us-mia,zone=b", "10.0.0.1:26257", true},
		{"no shared tier", []LocalityAddress{la("region", "us-mia", "10.0.0.1:26257")}, "region=us-atl,zone=b", "", false},
		{"first match wins", []LocalityAddress{
			la("region", "us-atl", "10.9.0.1:26257"),
			la("zone", "b", "10.0.0.2:26257"),
			la("region", "us-mia", "10.0.0.1:26257"),
		}, "region=us-mia,zone=b", "10.0.0.2:26257", true},
		{"same key different value", []LocalityAddress{la("region", "us-mia", "10.0.0.1:26257")}, "region=us-mia2", "", false},
		{"value under another key", []LocalityAddress{la("zone", "us-mia", "10.0.0.1:26257")}, "region=us-mia", "", false},
		{"empty local locality", []LocalityAddress{la("region", "us-mia", "10.0.0.1:26257")}, "", "", false},
		{"no entries", nil, "region=us-mia", "", false},
	} {
		got, ok := SelectLocalityAddress(c.entries, mustLoc(c.local))
		if got != c.want || ok != c.wantOK {
			t.Errorf("%s: got (%q, %v), want (%q, %v)", c.name, got, ok, c.want, c.wantOK)
		}
	}
}
