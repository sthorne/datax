package server

import (
	"testing"

	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestStandInDrawsItsCountFromTheRealOnes (issue #225): the stand-in
// verifier's iteration count for a name is one of the counts the stored
// verifiers carry, chosen per name under the cluster's secret — the same
// for a name every time, different names landing on different counts
// in the real ones' proportions — so neither the old count nor the new
// says whether a name exists. With no verifier to follow, or no secret,
// it is the constant.
func TestStandInDrawsItsCountFromTheRealOnes(t *testing.T) {
	verifier := func(iterations int) []byte {
		raw, err := security.MarshalVerifier(&security.ScramVerifier{Salt: make([]byte, 16), Iterations: iterations, StoredKey: make([]byte, 32), ServerKey: make([]byte, 32)})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	secret := []byte("a thirty-two byte cluster secret")
	if got := verifierCounts(nil); len(got) != 0 {
		t.Errorf("no roles: counts %v", got)
	}
	if got := verifierCounts([]*catalog.RoleDescriptor{{Name: "nopass"}, {Name: "bad", Verifier: []byte("{")}}); len(got) != 0 {
		t.Errorf("no verifier: counts %v", got)
	}
	if got := standInCount(nil, secret, "nobody"); got != security.ScramIterations {
		t.Errorf("no counts: %d, want the constant", got)
	}
	old := verifierCounts([]*catalog.RoleDescriptor{{Name: "a", Verifier: verifier(4096)}, {Name: "b", Verifier: verifier(4096)}, {Name: "nopass"}})
	if len(old) != 2 || standInCount(old, secret, "nobody") != 4096 {
		t.Errorf("every verifier at 4096: counts %v, stand-in %d", old, standInCount(old, secret, "nobody"))
	}
	mixed := verifierCounts([]*catalog.RoleDescriptor{
		{Name: "a", Verifier: verifier(4096)}, {Name: "b", Verifier: verifier(4096)}, {Name: "rotated", Verifier: verifier(security.ScramIterations)},
	})
	if len(mixed) != 3 {
		t.Fatalf("mixed counts %v", mixed)
	}
	if standInCount(mixed, nil, "nobody") != security.ScramIterations {
		t.Error("with no secret the stand-in does not fall back to the constant")
	}
	seen := map[int]int{}
	for i := 0; i < 64; i++ {
		name := "nobody-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		c := standInCount(mixed, secret, name)
		if c != standInCount(mixed, secret, name) {
			t.Fatalf("%s: two counts for one name", name)
		}
		if c != 4096 && c != security.ScramIterations {
			t.Fatalf("%s: %d is no stored count", name, c)
		}
		seen[c]++
	}
	if seen[4096] == 0 || seen[security.ScramIterations] == 0 {
		t.Errorf("64 names all drew one count: %v; the stand-in should show both, as the real users do", seen)
	}
	if standInCount(mixed, []byte("another secret"), "nobody-aa") == standInCount(mixed, secret, "nobody-aa") &&
		standInCount(mixed, []byte("another secret"), "nobody-ba") == standInCount(mixed, secret, "nobody-ba") &&
		standInCount(mixed, []byte("another secret"), "nobody-ca") == standInCount(mixed, secret, "nobody-ca") &&
		standInCount(mixed, []byte("another secret"), "nobody-da") == standInCount(mixed, secret, "nobody-da") {
		t.Log("four names drew the same counts under two secrets; possible, not expected")
	}
}
