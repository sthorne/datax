package catalog

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/base"
)

// A descriptor with no policy must encode exactly as it did before the
// field existed: a v15 node reads these, and an added key it does not
// know is a difference it would rewrite away.
func TestDatabaseDescriptorPlacementOmitted(t *testing.T) {
	raw, err := json.Marshal(&DatabaseDescriptor{ID: 7, Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "placement") {
		t.Fatalf("empty policy encoded: %s", raw)
	}
}

func TestDatabaseDescriptorPlacementRoundTrip(t *testing.T) {
	in := &DatabaseDescriptor{ID: 7, Name: "app", Placement: base.PlacementPolicy{
		Replicas:    5,
		Constraints: []base.Constraint{{Key: "region", Value: "eu-west-1"}},
	}}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out DatabaseDescriptor
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if !out.Placement.Equal(in.Placement) {
		t.Fatalf("round trip: %+v", out.Placement)
	}

	// Clone must not share the constraint slice: a cached descriptor and
	// the copy a statement mutates are the same object otherwise.
	cp := in.Clone()
	cp.Placement.Constraints[0].Value = "us-east-1"
	if in.Placement.Constraints[0].Value != "eu-west-1" {
		t.Fatalf("Clone shared the constraints: %+v", in.Placement)
	}
}

// A descriptor written before the field existed reads as no policy.
func TestDatabaseDescriptorPlacementAbsent(t *testing.T) {
	var d DatabaseDescriptor
	if err := json.Unmarshal([]byte(`{"id":3,"name":"legacy"}`), &d); err != nil {
		t.Fatal(err)
	}
	if !d.Placement.IsZero() {
		t.Fatalf("legacy descriptor carried a policy: %+v", d.Placement)
	}
}
