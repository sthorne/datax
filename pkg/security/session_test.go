package security

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSessionRoundTrip(t *testing.T) {
	key := DeriveKey([]byte("cluster-secret"), SessionKeyPurpose)
	now := time.Unix(1_700_000_000, 0)
	for _, user := range []string{"root", "ops", "a.b-c_d", "ünïcode", "has space", "has.dots.and:colons"} {
		tok := SignSession(key, user, now, now.Add(time.Hour))
		got, err := ParseSession(key, tok, now.Add(time.Minute))
		if err != nil || got != user {
			t.Fatalf("%q: round trip gave %q, %v", user, got, err)
		}
		if exp, ok := SessionExpiry(tok); !ok || !exp.Equal(now.Add(time.Hour)) {
			t.Fatalf("%q: expiry %v, %v", user, exp, ok)
		}
	}
}

// TestSessionRejects: every way a token can fail to authenticate, and
// the reason each is reported as.
func TestSessionRejects(t *testing.T) {
	key := DeriveKey([]byte("cluster-secret"), SessionKeyPurpose)
	other := DeriveKey([]byte("another-cluster"), SessionKeyPurpose)
	wrongPurpose := DeriveKey([]byte("cluster-secret"), "some-other-purpose")
	now := time.Unix(1_700_000_000, 0)
	tok := SignSession(key, "ops", now, now.Add(time.Hour))

	for _, tc := range []struct {
		name  string
		key   []byte
		token string
		at    time.Time
		want  error
	}{
		{"another cluster's key", other, tok, now, ErrSessionSignature},
		{"another purpose's key", wrongPurpose, tok, now, ErrSessionSignature},
		{"expired", key, tok, now.Add(2 * time.Hour), ErrSessionExpired},
		{"issued far in the future", key, SignSession(key, "ops", now.Add(time.Hour), now.Add(2*time.Hour)), now, ErrSessionExpired},
		{"empty", key, "", now, ErrSessionMalformed},
		{"no signature", key, "abc", now, ErrSessionMalformed},
		{"empty user", key, SignSession(key, "", now, now.Add(time.Hour)), now, ErrSessionMalformed},
		{"truncated signature", key, tok[:len(tok)-3], now, ErrSessionSignature},
	} {
		if _, err := ParseSession(tc.key, tc.token, tc.at); !errors.Is(err, tc.want) {
			t.Fatalf("%s: got %v, want %v", tc.name, err, tc.want)
		}
	}

	// Tampering with any field of a valid token breaks the signature —
	// including swapping in another user or extending the expiry.
	parts := strings.Split(tok, ".")
	for i, replacement := range map[int]string{
		0: b64.EncodeToString([]byte("root")),
		1: "1",
		2: "9999999999",
	} {
		mangled := append([]string(nil), parts...)
		mangled[i] = replacement
		if _, err := ParseSession(key, strings.Join(mangled, "."), now); !errors.Is(err, ErrSessionSignature) {
			t.Fatalf("field %d replaced with %q: got %v, want a signature failure", i, replacement, err)
		}
	}
	// A token whose expiry was extended by re-signing under a key the
	// forger does not have is exactly the signature case above; one
	// re-signed under the real key is, by definition, a real token.
}

// TestSessionKeyIsDerived: the signing key is not the cluster secret
// itself, and differs per purpose, so a compromise of one use of the
// secret is not a compromise of the others.
func TestSessionKeyIsDerived(t *testing.T) {
	secret := []byte("cluster-secret")
	key := DeriveKey(secret, SessionKeyPurpose)
	if string(key) == string(secret) {
		t.Fatal("the session key is the raw secret")
	}
	if string(key) == string(DeriveKey(secret, "scram-mock-salt")) {
		t.Fatal("two purposes derive the same key")
	}
}
