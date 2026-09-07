package pgwire

import (
	"context"
	"testing"
)

// TestCancelBySecretRefusesAZeroSecret (issue #211): zero is the value a
// caller sends when it holds no secret at all, and it must never
// authorize a cancel. Two things keep it from doing so and this pins
// both: the comparison is exact, and no connection is ever issued zero
// as its own secret — so even if register regressed and handed one out,
// a caller supplying zero would still not match it.
func TestCancelBySecretRefusesAZeroSecret(t *testing.T) {
	s := &Server{cancel: newCancelRegistry()}
	c := &conn{}
	s.cancel.register(c, 1)
	if c.secret == 0 {
		t.Fatal("register issued a zero secret")
	}
	ctx, cancel := c.beginStatement(context.Background())
	defer cancel()

	// The wire client's authority is the secret and nothing else.
	if s.CancelBySecret(c.pid, 0) {
		t.Fatal("a zero secret cancelled a connection")
	}
	if s.CancelBySecret(c.pid, c.secret^0xff) {
		t.Fatal("a wrong secret cancelled a connection")
	}
	if s.CancelBySecret(c.pid+1, c.secret) {
		t.Fatal("the right secret cancelled a different process ID")
	}
	select {
	case <-ctx.Done():
		t.Fatal("a refused cancel stopped the statement")
	default:
	}

	// A connection holding zero — which register cannot produce, but a
	// regression there would — is still not cancellable by a caller who
	// supplies zero.
	c.secret = 0
	if s.CancelBySecret(c.pid, 0) {
		t.Fatal("zero matched zero")
	}
	select {
	case <-ctx.Done():
		t.Fatal("a zero-against-zero cancel stopped the statement")
	default:
	}

	// The admin path takes no secret: the authority was established
	// before the call.
	if !s.ControlBackend(c.pid, false) {
		t.Fatal("ControlBackend did not find the connection")
	}
	<-ctx.Done()
}
