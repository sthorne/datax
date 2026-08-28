package pgwire

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql"
)

// dummyVerifier stands in for missing users so the SCRAM exchange runs to
// completion identically whether or not the user exists — the client learns
// only "password authentication failed".
var dummyVerifier = func() *security.ScramVerifier {
	v, err := security.MakeScramVerifier("this-password-can-never-verify")
	if err != nil {
		panic(err)
	}
	return v
}()

// authenticateSCRAM runs the server side of SCRAM-SHA-256 during startup.
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

	c.backend.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}})
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
	if !ok || initial.AuthMechanism != "SCRAM-SHA-256" {
		return failed()
	}

	server := security.NewScramServer(verifier)
	serverFirst, err := server.HandleClientFirst(string(initial.Data))
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
