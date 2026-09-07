package pgwire

import (
	"context"
	"testing"

	"github.com/sthorne/datax/pkg/security"
)

// The stand-in verifier a missing user authenticates against (issues
// #137, #196).
//
// #137 made the salt per-name under a cluster-wide secret, so two
// unknown usernames no longer share one. #196 is the residual: when the
// secret could not be read, the node fell back to a per-process value.
// A real verifier lives in the replicated catalog, so a genuine user's
// salt is identical on every node — which made the fallback itself the
// oracle. Ask two nodes for the same username: two different salts means
// no such user, the same salt means the user exists.

// TestStandInSaltIsTheSameOnEveryNode pins the property #137 established
// and #196 was about losing: the salt for a name that does not exist is
// a function of the cluster secret and the name, so every node shows the
// same one and it says nothing.
func TestStandInSaltIsTheSameOnEveryNode(t *testing.T) {
	secret := []byte("a cluster-wide authentication secret")
	n1 := &conn{opts: ServerOptions{MockSecret: func(context.Context) []byte { return secret }}}
	n2 := &conn{opts: ServerOptions{MockSecret: func(context.Context) []byte { return secret }}}

	a := n1.standInVerifier(context.Background(), "nosuchuser")
	b := n2.standInVerifier(context.Background(), "nosuchuser")
	if a == nil || b == nil {
		t.Fatal("no stand-in verifier with a secret available")
	}
	if string(a.Salt) != string(b.Salt) {
		t.Fatal("two nodes show different salts for the same missing username: " +
			"comparing them tells an attacker the user does not exist")
	}
	// And still per-name, which is what #137 fixed.
	if c := n1.standInVerifier(context.Background(), "someoneelse"); string(c.Salt) == string(a.Salt) {
		t.Fatal("two different missing usernames share a salt")
	}
}

// TestStandInRefusesWithoutTheSecret: with no cluster secret there is no
// stand-in that would agree with the other nodes', so there is none. The
// caller refuses for every user rather than only for missing ones —
// tested at the wire level in the cluster tests — which is what keeps
// refusing from being a worse oracle than the one it closes.
func TestStandInRefusesWithoutTheSecret(t *testing.T) {
	none := &conn{opts: ServerOptions{}}
	if v := none.standInVerifier(context.Background(), "nosuchuser"); v != nil {
		t.Fatal("a stand-in verifier was produced with no cluster secret: " +
			"whatever keys it is not what the other nodes use, and the difference is the oracle")
	}
	empty := &conn{opts: ServerOptions{MockSecret: func(context.Context) []byte { return nil }}}
	if v := empty.standInVerifier(context.Background(), "nosuchuser"); v != nil {
		t.Fatal("an unreadable secret produced a stand-in verifier")
	}
}

// A genuine verifier is unaffected by any of this: it comes from the
// catalog and is the same everywhere by construction.
func TestGenuineVerifierIsNotAStandIn(t *testing.T) {
	v, err := security.MakeScramVerifier("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	c := &conn{opts: ServerOptions{MockSecret: func(context.Context) []byte { return []byte("secret") }}}
	if stand := c.standInVerifier(context.Background(), "someone"); string(stand.Salt) == string(v.Salt) {
		t.Fatal("a stand-in salt collided with a real one")
	}
}
