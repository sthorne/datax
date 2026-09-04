package cli

import (
	"fmt"
	"net"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/security"
)

// SQLConfig builds the connection configuration for a datax SQL client
// from a connection URL plus the certificate-directory conventions the
// rest of the CLI uses. With certsDir set, the connection is made over TLS
// verified against certsDir/ca.crt and authenticated by the client
// certificate client.<user>.crt (the server takes the username from its
// CommonName), whatever sslmode the URL carries; user, when non-empty,
// also becomes the connection's username. Without certsDir the URL is
// used as written, apart from the username override.
func SQLConfig(url, certsDir, user string) (*pgx.ConnConfig, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if user != "" {
		cfg.User = user
	}
	if certsDir != "" {
		if cfg.User == "" {
			return nil, fmt.Errorf("--certs-dir needs a username: pass --user or put one in the URL")
		}
		tlsCfg, err := security.LoadClientTLS(certsDir, cfg.User)
		if err != nil {
			return nil, err
		}
		tlsCfg.ServerName = cfg.Host
		cfg.TLSConfig = tlsCfg
		// pgx's sslmode=prefer/allow fallbacks would retry without TLS on
		// a handshake failure; a certificate-authenticated client must not.
		cfg.Fallbacks = nil
	}
	return cfg, nil
}

// SQLTarget renders the address a config connects to, for progress lines.
func SQLTarget(cfg *pgx.ConnConfig) string {
	return net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port)))
}

// SQLKind describes the connection for progress lines: "sql", plus the
// transport security in use.
func SQLKind(cfg *pgx.ConnConfig, certsDir string) string {
	switch {
	case certsDir != "":
		return "sql, TLS with client certificate"
	case cfg.TLSConfig != nil:
		return "sql, TLS"
	default:
		return "sql"
	}
}
