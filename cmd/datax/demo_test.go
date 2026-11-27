package main

import (
	"strings"
	"testing"
)

// datax demo --locality (issue #405).
func TestDemoLocalities(t *testing.T) {
	got, err := demoLocalities("", 3)
	if err != nil || got[0].String() != "region=demo,rack=a" || got[2].String() != "region=demo,rack=c" {
		t.Fatalf("default: %v %v", got, err)
	}
	got, err = demoLocalities("region=us-east-1; region=us-east-2;region=eu-west-1,zone=b", 3)
	if err != nil || got[0].String() != "region=us-east-1" || got[1].String() != "region=us-east-2" || got[2].String() != "region=eu-west-1,zone=b" {
		t.Fatalf("one per node: %v %v", got, err)
	}
	got, err = demoLocalities("region=eu-west-1", 2)
	if err != nil || got[0].String() != "region=eu-west-1" || got[1].String() != "region=eu-west-1" {
		t.Fatalf("one for all: %v %v", got, err)
	}
	if _, err := demoLocalities("region=a;region=b", 3); err == nil || !strings.Contains(err.Error(), "2 localities for 3 nodes") {
		t.Fatalf("count mismatch: %v", err)
	}
	if _, err := demoLocalities("region=a;bogus", 2); err == nil || !strings.Contains(err.Error(), "node 2") {
		t.Fatalf("bad locality: %v", err)
	}
}
