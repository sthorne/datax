package security

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/secure/precis"
)

// SCRAM-SHA-256 (RFC 5802 / RFC 7677), server side. The conversation is a
// plain state machine over the SCRAM message strings; pgwire frames them in
// SASL protocol messages. Verifiers (never plaintext passwords) are stored
// per user; comparisons use hmac.Equal.

// ScramIterations is the PBKDF2 iteration count for new verifiers (the RFC
// 7677 recommended minimum).
const ScramIterations = 4096

// ScramVerifier is a user's stored SCRAM credential (no plaintext).
type ScramVerifier struct {
	Salt       []byte `json:"salt"`
	Iterations int    `json:"iterations"`
	StoredKey  []byte `json:"stored_key"`
	ServerKey  []byte `json:"server_key"`
}

// SASLprep normalizes a SCRAM password per RFC 4013 (via the PRECIS
// OpaqueString profile, its successor). Inputs the profile rejects —
// invalid UTF-8 or prohibited characters — fall back to the raw string,
// matching PostgreSQL's and pgx's behavior, so such passwords still work
// as exact byte strings.
func SASLprep(s string) string {
	if !utf8.ValidString(s) {
		return s // PG: non-UTF-8 passwords are used as raw bytes
	}
	out, err := precis.OpaqueString.String(s)
	if err != nil {
		return s
	}
	return out
}

// MakeScramVerifier derives a verifier from a password with a fresh salt.
// The password is SASLprep-normalized, so spec-compliant clients (which
// normalize before proving) agree with the stored verifier for non-ASCII
// passwords.
func MakeScramVerifier(password string) (*ScramVerifier, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	return makeVerifier(SASLprep(password), salt, ScramIterations)
}

// EndpointChannelBinding computes RFC 5929 tls-server-end-point data for a
// server certificate (DER): a hash of the certificate using its signature
// hash algorithm, with MD5/SHA-1 upgraded to SHA-256.
func EndpointChannelBinding(certDER []byte) []byte {
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil
	}
	switch cert.SignatureAlgorithm {
	case x509.SHA384WithRSA, x509.ECDSAWithSHA384, x509.SHA384WithRSAPSS:
		h := sha512.Sum384(certDER)
		return h[:]
	case x509.SHA512WithRSA, x509.ECDSAWithSHA512, x509.SHA512WithRSAPSS:
		h := sha512.Sum512(certDER)
		return h[:]
	default: // SHA-256 family, and MD5/SHA-1 upgraded per the RFC
		h := sha256.Sum256(certDER)
		return h[:]
	}
}

func makeVerifier(password string, salt []byte, iterations int) (*ScramVerifier, error) {
	salted, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return nil, err
	}
	clientKey := hmacSHA256(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	return &ScramVerifier{
		Salt:       salt,
		Iterations: iterations,
		StoredKey:  stored[:],
		ServerKey:  hmacSHA256(salted, "Server Key"),
	}, nil
}

// MarshalVerifier / UnmarshalVerifier are the storage encoding.
func MarshalVerifier(v *ScramVerifier) ([]byte, error) { return json.Marshal(v) }

func UnmarshalVerifier(raw []byte) (*ScramVerifier, error) {
	var v ScramVerifier
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// SCRAM mechanism names.
const (
	MechScram     = "SCRAM-SHA-256"
	MechScramPlus = "SCRAM-SHA-256-PLUS"
)

// ScramServer runs one server-side SCRAM-SHA-256[-PLUS] conversation.
type ScramServer struct {
	verifier *ScramVerifier
	// cbData is the tls-server-end-point binding data. Non-nil means the
	// server advertised SCRAM-SHA-256-PLUS.
	cbData []byte

	clientFirstBare string
	serverFirst     string
	fullNonce       string
	expectedC       string // required c= attribute of the client-final
}

// NewScramServer starts a conversation with no channel binding available
// (cleartext, or no server certificate).
func NewScramServer(v *ScramVerifier) *ScramServer { return &ScramServer{verifier: v} }

// NewScramServerTLS starts a conversation on a TLS session whose
// tls-server-end-point data is cbData; the caller advertises the -PLUS
// mechanism alongside the plain one.
func NewScramServerTLS(v *ScramVerifier, cbData []byte) *ScramServer {
	return &ScramServer{verifier: v, cbData: cbData}
}

// HandleClientFirst consumes the client-first-message for the client's
// selected mechanism and returns the server-first-message.
func (s *ScramServer) HandleClientFirst(mech, clientFirst string) (string, error) {
	return s.handleClientFirst(mech, clientFirst, "")
}

func (s *ScramServer) handleClientFirst(mech, clientFirst, forcedServerNonce string) (string, error) {
	// gs2 header: cb flag ("n" none / "y" client-supports-but-thinks-server-
	// doesn't / "p=<name>" bind), then authzid, then the bare message.
	flagEnd := strings.Index(clientFirst, ",")
	if flagEnd < 0 {
		return "", fmt.Errorf("scram: malformed client-first-message")
	}
	flag := clientFirst[:flagEnd]
	rest := clientFirst[flagEnd+1:]
	plus := false
	switch {
	case flag == "n":
		if mech == MechScramPlus {
			return "", fmt.Errorf("scram: %s requires channel binding", MechScramPlus)
		}
	case flag == "y":
		if mech == MechScramPlus {
			return "", fmt.Errorf("scram: %s requires channel binding", MechScramPlus)
		}
		// The client supports channel binding but believes the server does
		// not. If we advertised -PLUS, something downgraded the negotiation.
		if s.cbData != nil {
			return "", fmt.Errorf("scram: channel-binding downgrade detected")
		}
	case strings.HasPrefix(flag, "p="):
		if mech != MechScramPlus {
			return "", fmt.Errorf("scram: channel binding requires %s", MechScramPlus)
		}
		if flag[2:] != "tls-server-end-point" {
			return "", fmt.Errorf("scram: unsupported channel binding %q", flag[2:])
		}
		if s.cbData == nil {
			return "", fmt.Errorf("scram: no channel binding data available")
		}
		plus = true
	default:
		return "", fmt.Errorf("scram: malformed gs2 header")
	}
	i := strings.Index(rest, ",")
	if i < 0 {
		return "", fmt.Errorf("scram: malformed client-first-message")
	}
	authzid := rest[:i]
	s.clientFirstBare = rest[i+1:]

	// The client-final c= attribute is the base64 of the gs2 header — plus
	// the binding data itself under -PLUS (RFC 5802 §7).
	cbInput := []byte(flag + "," + authzid + ",")
	if plus {
		cbInput = append(cbInput, s.cbData...)
	}
	s.expectedC = base64.StdEncoding.EncodeToString(cbInput)

	var clientNonce string
	for _, attr := range strings.Split(s.clientFirstBare, ",") {
		if v, ok := strings.CutPrefix(attr, "r="); ok {
			clientNonce = v
		}
	}
	if clientNonce == "" {
		return "", fmt.Errorf("scram: client-first-message carries no nonce")
	}
	serverNonce := forcedServerNonce
	if serverNonce == "" {
		raw := make([]byte, 18)
		if _, err := rand.Read(raw); err != nil {
			return "", err
		}
		serverNonce = base64.StdEncoding.EncodeToString(raw)
	}
	s.fullNonce = clientNonce + serverNonce
	s.serverFirst = fmt.Sprintf("r=%s,s=%s,i=%d",
		s.fullNonce, base64.StdEncoding.EncodeToString(s.verifier.Salt), s.verifier.Iterations)
	return s.serverFirst, nil
}

// HandleClientFinal consumes the client-final-message, verifies the proof,
// and returns the server-final-message ("v=..."). A verification failure
// returns an error — the caller maps it to a uniform auth failure.
func (s *ScramServer) HandleClientFinal(clientFinal string) (string, error) {
	var channel, nonce, proofB64 string
	withoutProof := clientFinal
	for _, attr := range strings.Split(clientFinal, ",") {
		switch {
		case strings.HasPrefix(attr, "c="):
			channel = attr[2:]
		case strings.HasPrefix(attr, "r="):
			nonce = attr[2:]
		case strings.HasPrefix(attr, "p="):
			proofB64 = attr[2:]
		}
	}
	if i := strings.LastIndex(clientFinal, ",p="); i >= 0 {
		withoutProof = clientFinal[:i]
	}
	if channel == "" || nonce == "" || proofB64 == "" {
		return "", fmt.Errorf("scram: malformed client-final-message")
	}
	if nonce != s.fullNonce {
		return "", fmt.Errorf("scram: nonce mismatch")
	}
	if channel != s.expectedC {
		return "", fmt.Errorf("scram: channel binding mismatch")
	}
	proof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return "", fmt.Errorf("scram: malformed proof")
	}

	authMessage := s.clientFirstBare + "," + s.serverFirst + "," + withoutProof
	clientSignature := hmacSHA256(s.verifier.StoredKey, authMessage)
	if len(proof) != len(clientSignature) {
		return "", fmt.Errorf("scram: proof verification failed")
	}
	clientKey := make([]byte, len(proof))
	for i := range proof {
		clientKey[i] = proof[i] ^ clientSignature[i]
	}
	recovered := sha256.Sum256(clientKey)
	if !hmac.Equal(recovered[:], s.verifier.StoredKey) {
		return "", fmt.Errorf("scram: proof verification failed")
	}
	serverSignature := hmacSHA256(s.verifier.ServerKey, authMessage)
	return "v=" + base64.StdEncoding.EncodeToString(serverSignature), nil
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

var _ = strconv.Itoa // keep strconv for future attribute parsing
