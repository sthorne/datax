package pgwire

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql"
)

// dummyVerifier stands in for missing users so the SCRAM exchange runs to
// completion identically whether or not the user exists — the client learns
// only "password authentication failed".
var dummyVerifier = security.DummyVerifier()

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
		verifier = dummyVerifier
	}

	failed := func() error {
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
