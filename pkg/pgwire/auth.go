package pgwire

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/util/log"
)

// standInVerifier is the verifier a missing user (or one with no
// password, or one that cannot log in) authenticates against, so the
// SCRAM exchange runs to completion identically whether or not the user
// exists — the client learns only "password authentication failed", and
// the salt it saw in server-first says nothing either.
//
// It needs the cluster's authentication secret, and returns nil when
// there is none to be had. There used to be a per-process fallback so
// that a node whose KV read failed could still answer, with salts that
// were at least per-name and stable on that node. That is what made the
// oracle (issue #196): a real verifier lives in the replicated catalog,
// so a genuine user's salt is identical on every node, while a
// per-process stand-in is not. Ask two nodes for the same username —
// two different salts means no such user, the same salt means the user
// exists. One client-first per node, no password guessing, no completed
// authentication.
//
// Refusing to answer at all is the honest alternative, and it is only
// safe because the caller refuses for EVERY user rather than only for
// the missing ones: an error where an unknown name gets one and a known
// name gets a challenge would be a better oracle than the one being
// closed.
func (c *conn) standInVerifier(ctx context.Context, user string) *security.ScramVerifier {
	if c.opts.MockSecret == nil {
		return nil
	}
	secret := c.opts.MockSecret(ctx)
	if len(secret) == 0 {
		return nil
	}
	return security.MockVerifier(secret, user)
}

// clientCertUser returns the CommonName of a CA-verified client
// certificate on the TLS session, or "" when none was presented (or it
// did not verify — VerifiedChains is only populated for verified certs).
func (c *conn) clientCertUser() string {
	tc, ok := c.nc.(*tls.Conn)
	if !ok {
		return ""
	}
	st := tc.ConnectionState()
	if len(st.VerifiedChains) == 0 || len(st.VerifiedChains[0]) == 0 {
		return ""
	}
	return st.VerifiedChains[0][0].Subject.CommonName
}

// authenticateSCRAM runs the server side of SCRAM-SHA-256[-PLUS] during
// startup.
func (c *conn) authenticateSCRAM(user string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	verifier, lookupErr := c.opts.Auth(ctx, user)
	genuine := verifier != nil && lookupErr == nil
	if verifier == nil {
		verifier = c.standInVerifier(ctx, user)
	}
	if verifier == nil {
		// No cluster secret, so no stand-in that agrees with the other
		// nodes'. Refuse before the mechanism is advertised, identically
		// for every username: a node in this state answers nobody, which
		// says nothing about who exists (issue #196).
		metrics.AuthSecretUnavailable.Inc()
		log.Warnf("the cluster's authentication secret is unavailable on this node; refusing password authentication until it can be read")
		c.sendError(&sql.Error{Code: "08006", Msg: "the cluster's authentication secret is unavailable on this node; try another node"})
		_ = c.backend.Flush()
		return fmt.Errorf("authentication secret unavailable")
	}

	failed := func() error {
		metrics.AuthFailures.Inc()
		log.Audit("sql-auth-failure", "principal", user, "remote", c.nc.RemoteAddr().String())
		c.sendError(&sql.Error{Code: "28P01", Msg: fmt.Sprintf("password authentication failed for user %q", user)})
		_ = c.backend.Flush()
		return fmt.Errorf("authentication failed for %q", user)
	}

	// On TLS, advertise SCRAM-SHA-256-PLUS (tls-server-end-point channel
	// binding) alongside the plain mechanism.
	mechs := []string{security.MechScram}
	var cb []byte
	if c.tlsDone && c.opts.TLS != nil && len(c.opts.TLS.Certificates) > 0 && len(c.opts.TLS.Certificates[0].Certificate) > 0 {
		if cb = security.EndpointChannelBinding(c.opts.TLS.Certificates[0].Certificate[0]); cb != nil {
			mechs = []string{security.MechScramPlus, security.MechScram}
		}
	}
	c.backend.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: mechs})
	if err := c.backend.Flush(); err != nil {
		return err
	}
	if err := c.backend.SetAuthType(pgproto3.AuthTypeSASL); err != nil {
		return err
	}
	msg, err := c.backend.Receive()
	if err != nil {
		return err
	}
	initial, ok := msg.(*pgproto3.SASLInitialResponse)
	if !ok {
		return failed()
	}
	mechOK := false
	for _, m := range mechs {
		if initial.AuthMechanism == m {
			mechOK = true
			break
		}
	}
	if !mechOK {
		return failed()
	}

	server := security.NewScramServer(verifier)
	if cb != nil {
		server = security.NewScramServerTLS(verifier, cb)
	}
	serverFirst, err := server.HandleClientFirst(initial.AuthMechanism, string(initial.Data))
	if err != nil {
		return failed()
	}
	c.backend.Send(&pgproto3.AuthenticationSASLContinue{Data: []byte(serverFirst)})
	if err := c.backend.Flush(); err != nil {
		return err
	}
	if err := c.backend.SetAuthType(pgproto3.AuthTypeSASLContinue); err != nil {
		return err
	}
	msg, err = c.backend.Receive()
	if err != nil {
		return err
	}
	resp, ok := msg.(*pgproto3.SASLResponse)
	if !ok {
		return failed()
	}
	serverFinal, err := server.HandleClientFinal(string(resp.Data))
	if err != nil || !genuine {
		return failed()
	}
	c.backend.Send(&pgproto3.AuthenticationSASLFinal{Data: []byte(serverFinal)})
	c.backend.Send(&pgproto3.AuthenticationOk{})
	return nil
}
