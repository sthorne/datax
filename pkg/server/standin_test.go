package server

import (
	"testing"

	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestStandInAdvertisesTheCountRealUsersCarry (issue #225): the stand-in
// verifier's iteration count is the highest any stored verifier has —
// the old count while no password has been set since the raise, so a
// name that does not exist shows what every real user shows, and the
// new count once one has — and the constant only when there is no
// verifier to follow.
func TestStandInAdvertisesTheCountRealUsersCarry(t *testing.T) {
	verifier := func(iterations int) []byte {
		raw, err := security.MarshalVerifier(&security.ScramVerifier{Salt: make([]byte, 16), Iterations: iterations, StoredKey: make([]byte, 32), ServerKey: make([]byte, 32)})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if got := advertisedIterations(nil); got != security.ScramIterations {
		t.Errorf("no roles: %d, want the constant %d", got, security.ScramIterations)
	}
	if got := advertisedIterations([]*catalog.RoleDescriptor{{Name: "nopass"}, {Name: "bad", Verifier: []byte("{")}}); got != security.ScramIterations {
		t.Errorf("no verifier: %d, want the constant %d", got, security.ScramIterations)
	}
	old := []*catalog.RoleDescriptor{{Name: "a", Verifier: verifier(4096)}, {Name: "b", Verifier: verifier(4096)}, {Name: "nopass"}}
	if got := advertisedIterations(old); got != 4096 {
		t.Errorf("every verifier at 4096: stand-in advertises %d", got)
	}
	mixed := append(old, &catalog.RoleDescriptor{Name: "rotated", Verifier: verifier(security.ScramIterations)})
	if got := advertisedIterations(mixed); got != security.ScramIterations {
		t.Errorf("one verifier at %d among the old: stand-in advertises %d", security.ScramIterations, got)
	}
}
