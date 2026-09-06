// Package security implements datax's certificate tooling and TLS
// configuration: a self-signed cluster CA, node certificates (used for both
// sides of mutual internode TLS and for the SQL listener), and client
// certificates. Everything is standard library crypto — no new
// dependencies.
package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	CAKeyFile   = "ca.key"
	CACertFile  = "ca.crt"
	NodeKeyFile = "node.key"
	NodeCert    = "node.crt"
)

// CreateCA generates the cluster CA into certsDir.
func CreateCA(certsDir string) error {
	if err := os.MkdirAll(certsDir, 0o700); err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               pkix.Name{CommonName: "datax CA", Organization: []string{"datax"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return err
	}
	if err := writeKey(filepath.Join(certsDir, CAKeyFile), key); err != nil {
		return err
	}
	return writeCert(filepath.Join(certsDir, CACertFile), der)
}

// NodePrincipal is the CommonName of the node certificate: the identity
// cluster peers present to each other. It is reserved — never a SQL user
// and never a client certificate.
const NodePrincipal = "node"

// CreateNodeCert generates a node certificate (server + client usage, for
// mutual internode TLS and the SQL listener) with the given host SANs.
func CreateNodeCert(certsDir string, hosts []string) error {
	return createSignedCert(certsDir, NodeCert, NodeKeyFile, NodePrincipal, hosts,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})
}

// CreateClientCert generates a client certificate whose CommonName is the
// given user. "node" is the cluster's own identity (the node certificate)
// and is never issued as a client certificate: it would carry node
// authority over the internode KV and Raft surfaces.
func CreateClientCert(certsDir, user string) error {
	if user == NodePrincipal {
		return fmt.Errorf("%q is the cluster's node identity, not a user; client certificates are issued to SQL users", user)
	}
	return createSignedCert(certsDir, fmt.Sprintf("client.%s.crt", user), fmt.Sprintf("client.%s.key", user),
		user, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
}

func createSignedCert(certsDir, certFile, keyFile, cn string, hosts []string, usages []x509.ExtKeyUsage) error {
	caCert, caKey, err := loadCA(certsDir)
	if err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	tmpl := &x509.Certificate{
		SerialNumber: randomSerial(),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"datax"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(5, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	if err := writeKey(filepath.Join(certsDir, keyFile), key); err != nil {
		return err
	}
	return writeCert(filepath.Join(certsDir, certFile), der)
}

func loadCA(certsDir string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certPEM, err := os.ReadFile(filepath.Join(certsDir, CACertFile))
	if err != nil {
		return nil, nil, fmt.Errorf("reading CA cert (run 'datax cert create-ca' first): %w", err)
	}
	keyPEM, err := os.ReadFile(filepath.Join(certsDir, CAKeyFile))
	if err != nil {
		return nil, nil, err
	}
	certBlock, _ := pem.Decode(certPEM)
	keyBlock, _ := pem.Decode(keyPEM)
	if certBlock == nil || keyBlock == nil {
		return nil, nil, fmt.Errorf("malformed CA PEM files")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func randomSerial() *big.Int {
	s, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		panic(err) // rand.Reader failure is unrecoverable
	}
	return s
}

func writeKey(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0o600)
}

func writeCert(path string, der []byte) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644)
}

// CertInfo is one loaded certificate's identity and validity window.
//
// Silently expiring node certificates are a well-known way to lose a
// cluster, so the node reports what it loaded rather than leaving the
// dates in files nobody looks at (issue #156). It is taken from the
// certificates parsed at load time — nothing re-reads the certificate
// directory to answer a console poll or a metrics scrape.
type CertInfo struct {
	// Kind is "ca", "node" or "client", the role the certificate plays.
	Kind      string    `json:"kind"`
	Subject   string    `json:"subject"`
	Issuer    string    `json:"issuer,omitempty"`
	NotBefore time.Time `json:"not_before"`
	NotAfter  time.Time `json:"not_after"`
}

// Expired reports whether the certificate is outside its validity window
// at t. A certificate that is not yet valid is as unusable as an expired
// one, so both answer true.
func (c CertInfo) Expired(t time.Time) bool {
	return t.After(c.NotAfter) || t.Before(c.NotBefore)
}

// TLSConfigs bundles a node's TLS material for its three roles.
type TLSConfigs struct {
	// Server authenticates internode gRPC servers and REQUIRES a
	// CA-signed client certificate (mutual TLS).
	Server *tls.Config
	// Client authenticates this node to its peers.
	Client *tls.Config
	// PGServer serves SQL clients: server-authenticated TLS (clients
	// verify the CA). User authentication is SCRAM, or a CA-signed client
	// certificate whose CommonName is the SQL user (optional — hence
	// VerifyClientCertIfGiven).
	PGServer *tls.Config
	// Certs describes what was loaded: the cluster CA and this node's
	// certificate. Kept here so expiry can be reported from the material
	// already in memory (issue #156).
	Certs []CertInfo
}

// certInfo describes a parsed certificate.
func certInfo(kind string, c *x509.Certificate) CertInfo {
	return CertInfo{
		Kind:      kind,
		Subject:   c.Subject.CommonName,
		Issuer:    c.Issuer.CommonName,
		NotBefore: c.NotBefore,
		NotAfter:  c.NotAfter,
	}
}

// CertInfoFrom describes a peer certificate the server was presented —
// a client certificate on the HTTP or SQL listener. Exported so the
// server can report the identities reaching it without re-parsing PEM.
func CertInfoFrom(c *x509.Certificate) CertInfo { return certInfo("client", c) }

// LoadClientTLS loads a client TLS configuration from certsDir: the
// cluster CA as the trust root plus client.<user>.crt/.key as the
// identity presented to servers. Used by the CLI (datax debug, backup,
// restore) to authenticate against a secure cluster's RPC and HTTP
// ports; the server authorizes by the certificate's CommonName.
func LoadClientTLS(certsDir, user string) (*tls.Config, error) {
	caPEM, err := os.ReadFile(filepath.Join(certsDir, CACertFile))
	if err != nil {
		return nil, fmt.Errorf("reading CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certificates in %s", CACertFile)
	}
	pair, err := tls.LoadX509KeyPair(
		filepath.Join(certsDir, fmt.Sprintf("client.%s.crt", user)),
		filepath.Join(certsDir, fmt.Sprintf("client.%s.key", user)))
	if err != nil {
		return nil, fmt.Errorf("reading client cert pair (run 'datax cert create-client --user %s' first): %w", user, err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// LoadNodeTLS loads the node's certificates from certsDir.
func LoadNodeTLS(certsDir string) (*TLSConfigs, error) {
	caPEM, err := os.ReadFile(filepath.Join(certsDir, CACertFile))
	if err != nil {
		return nil, fmt.Errorf("reading CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certificates in %s", CACertFile)
	}
	pair, err := tls.LoadX509KeyPair(filepath.Join(certsDir, NodeCert), filepath.Join(certsDir, NodeKeyFile))
	if err != nil {
		return nil, fmt.Errorf("reading node cert pair: %w", err)
	}
	// The CA is otherwise only a trust root in the pool, and the node's
	// leaf may not be parsed by LoadX509KeyPair; parse both here so the
	// dates travel with the configuration they belong to.
	var certs []CertInfo
	if block, _ := pem.Decode(caPEM); block != nil {
		if ca, cerr := x509.ParseCertificate(block.Bytes); cerr == nil {
			certs = append(certs, certInfo("ca", ca))
		}
	}
	if len(pair.Certificate) > 0 {
		leaf := pair.Leaf
		if leaf == nil {
			leaf, _ = x509.ParseCertificate(pair.Certificate[0])
		}
		if leaf != nil {
			certs = append(certs, certInfo("node", leaf))
		}
	}
	return &TLSConfigs{
		Certs: certs,
		Server: &tls.Config{
			Certificates: []tls.Certificate{pair},
			ClientCAs:    pool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
		},
		Client: &tls.Config{
			Certificates: []tls.Certificate{pair},
			RootCAs:      pool,
			MinVersion:   tls.VersionTLS12,
		},
		PGServer: &tls.Config{
			Certificates: []tls.Certificate{pair},
			ClientCAs:    pool,
			ClientAuth:   tls.VerifyClientCertIfGiven,
			MinVersion:   tls.VersionTLS12,
		},
	}, nil
}
