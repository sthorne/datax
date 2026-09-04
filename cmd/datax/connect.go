package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cli"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// connectTimeoutFlag registers --connect-timeout on fs.
func connectTimeoutFlag(fs *flag.FlagSet) *time.Duration {
	return fs.Duration("connect-timeout", cli.DefaultConnectTimeout, "how long to wait for the connection to be established")
}

// connectAdmin opens the RPC transport for an admin call against addr:
// client TLS from certsDir when set, then a dial and handshake under the
// connect timeout with progress on stderr, so a node that is down or
// unreachable is reported as such — with the address and the cause —
// rather than as the operation's own timeout expiring in silence.
func connectAdmin(addr, certsDir, certUser string, timeout time.Duration) (*rpc.Transport, error) {
	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	kind := "admin rpc"
	if certsDir != "" {
		tlsCfg, err := security.LoadClientTLS(certsDir, certUser)
		if err != nil {
			return nil, err
		}
		trans.SetTLS(tlsCfg)
		kind = "admin rpc, TLS with client certificate"
	}
	err := cli.Connect(context.Background(), nil, addr, kind, timeout, func(ctx context.Context) error {
		return trans.Probe(ctx, addr)
	})
	if err != nil {
		return nil, err
	}
	return trans, nil
}

// adminCallError explains an admin RPC failure that a plain dial cannot
// catch: a secure node accepts the TCP connection and then drops a client
// that does not start TLS, which gRPC reports as a reset while reading the
// server preface. Without --certs-dir that is almost always the cause.
func adminCallError(err error, certsDir string) error {
	if err == nil || certsDir != "" {
		return err
	}
	msg := err.Error()
	if strings.Contains(msg, "server preface") || strings.Contains(msg, "connection reset") || strings.Contains(msg, "EOF") {
		return fmt.Errorf("%w (the node closed the connection before any reply; a secure cluster requires --certs-dir)", err)
	}
	return err
}
