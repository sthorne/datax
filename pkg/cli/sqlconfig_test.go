package cli

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/security"
)

func TestSQLConfigPlainURL(t *testing.T) {
	cfg, err := SQLConfig("postgres://root@127.0.0.1:26433/datax?sslmode=disable", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLSConfig != nil || cfg.User != "root" || SQLTarget(cfg) != "127.0.0.1:26433" {
		t.Fatalf("unexpected config: tls=%v user=%q target=%q", cfg.TLSConfig != nil, cfg.User, SQLTarget(cfg))
	}
	if SQLKind(cfg, "") != "sql" {
		t.Fatalf("kind: %q", SQLKind(cfg, ""))
	}
	cfg, err = SQLConfig("postgres://root@127.0.0.1:26433/datax?sslmode=disable", "", "analyst")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "analyst" {
		t.Fatalf("--user should override the URL's user, got %q", cfg.User)
	}
}

func TestSQLConfigWithCerts(t *testing.T) {
	dir := t.TempDir()
	if err := security.CreateCA(dir); err != nil {
		t.Fatal(err)
	}
	if err := security.CreateClientCert(dir, "ops"); err != nil {
		t.Fatal(err)
	}
	// sslmode=disable in the URL is overridden: certs mean TLS.
	cfg, err := SQLConfig("postgres://root@db1.internal:26433/datax?sslmode=disable", dir, "ops")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "ops" {
		t.Fatalf("user: %q", cfg.User)
	}
	if cfg.TLSConfig == nil {
		t.Fatal("expected TLS")
	}
	if len(cfg.TLSConfig.Certificates) != 1 || cfg.TLSConfig.RootCAs == nil {
		t.Fatal("expected the client certificate and the CA pool")
	}
	if cfg.TLSConfig.ServerName != "db1.internal" {
		t.Fatalf("server name should follow the host for verification, got %q", cfg.TLSConfig.ServerName)
	}
	if len(cfg.Fallbacks) != 0 {
		t.Fatalf("no cleartext fallbacks with a client certificate, got %d", len(cfg.Fallbacks))
	}
	if SQLKind(cfg, dir) != "sql, TLS with client certificate" {
		t.Fatalf("kind: %q", SQLKind(cfg, dir))
	}

	// The URL's user is the certificate to present when --user is absent.
	cfg, err = SQLConfig("postgres://ops@db1.internal:26433/datax", dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "ops" || cfg.TLSConfig == nil {
		t.Fatalf("expected ops over TLS, got user=%q tls=%v", cfg.User, cfg.TLSConfig != nil)
	}

	// A user without a certificate is told which command creates one.
	_, err = SQLConfig("postgres://root@db1.internal:26433/datax", dir, "nobody")
	if err == nil || !strings.Contains(err.Error(), "datax cert create-client --user nobody") {
		t.Fatalf("expected a pointer to create-client, got %v", err)
	}
	// A URL without a user cannot pick a certificate.
	_, err = SQLConfig("postgres://db1.internal:26433/datax", dir, "")
	if err == nil || !strings.Contains(err.Error(), "--user") {
		t.Fatalf("expected a username error, got %v", err)
	}
}
