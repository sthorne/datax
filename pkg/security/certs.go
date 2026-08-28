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

// CreateNodeCert generates a node certificate (server + client usage, for
// mutual internode TLS and the SQL listener) with the given host SANs.
func CreateNodeCert(certsDir string, hosts []string) error {
	return createSignedCert(certsDir, NodeCert, NodeKeyFile, "node", hosts,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})
}

// CreateClientCert generates a client certificate whose CommonName is the
// given user.
func CreateClientCert(certsDir, user string) error {
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

// TLSConfigs bundles a node's TLS material for its three roles.
type TLSConfigs struct {
	// Server authenticates internode gRPC servers and REQUIRES a
	// CA-signed client certificate (mutual TLS).
	Server *tls.Config
	// Client authenticates this node to its peers.
	Client *tls.Config
	// PGServer serves SQL clients: server-authenticated TLS (clients verify
	// the CA; user authentication is SCRAM, not certificates).
	PGServer *tls.Config
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
	return &TLSConfigs{
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
			MinVersion:   tls.VersionTLS12,
		},
	}, nil
}
