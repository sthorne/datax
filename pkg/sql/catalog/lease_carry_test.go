package catalog

import "testing"

// A handed-out expiration survives a same-version renewal and is
// published when the version changes — and only then (issue #234).
func TestHandedOutExpirationSurvivesARenewal(t *testing.T) {
	a := NewAccessor()
	v1, v2 := &TableDescriptor{Version: 1}, &TableDescriptor{Version: 2}

	// A statement took the entry; a renewal at the same version replaces
	// it. Nothing to publish yet, but the expiration rides along.
	a.cache["t"] = &cachedDesc{desc: v1, expiration: 100, handedOut: true}
	prior, handed := a.carryOver("t", 1, 10)
	if prior != 0 || handed != 100 {
		t.Fatalf("same-version renewal: prior %d, handed-out %d; want 0, 100", prior, handed)
	}

	// The fresh entry was taken by nobody. Adopting version 2 publishes
	// the carried expiration as what the drain must wait out, and the
	// new version starts with nothing handed out.
	a.cache["t"] = &cachedDesc{desc: v1, expiration: 200, handedOutExpiration: handed}
	prior, handed = a.carryOver("t", 2, 50)
	if prior != 100 || handed != 0 {
		t.Fatalf("adoption: prior %d, handed-out %d; want 100, 0", prior, handed)
	}

	// At the new version a statement takes the entry. The outstanding
	// drain is still published, but the drain must not wait for holders
	// of the version it is draining to.
	a.cache["t"] = &cachedDesc{desc: v2, expiration: 300, priorExpiration: prior, handedOut: true}
	prior, handed = a.carryOver("t", 2, 60)
	if prior != 100 || handed != 300 {
		t.Fatalf("renewal at the new version: prior %d, handed-out %d; want 100, 300", prior, handed)
	}

	// Once passed, neither is carried.
	a.cache["t"] = &cachedDesc{desc: v2, expiration: 300, priorExpiration: prior, handedOutExpiration: handed}
	if prior, handed = a.carryOver("t", 2, 400); prior != 0 || handed != 0 {
		t.Fatalf("expired carry-overs kept: prior %d, handed-out %d", prior, handed)
	}

	// The entry as the old code saw it — taken, still current at the
	// adoption — is unchanged: its own expiration is what is published.
	a.cache["t"] = &cachedDesc{desc: v2, expiration: 500, handedOut: true}
	if prior, handed = a.carryOver("t", 3, 60); prior != 500 || handed != 0 {
		t.Fatalf("direct adoption over a taken entry: prior %d, handed-out %d; want 500, 0", prior, handed)
	}
}
