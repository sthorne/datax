package security

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

// MakeScramVerifier derives a verifier from a password with a fresh salt.
func MakeScramVerifier(password string) (*ScramVerifier, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	return makeVerifier(password, salt, ScramIterations)
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

// ScramServer runs one server-side SCRAM-SHA-256 conversation.
type ScramServer struct {
	verifier *ScramVerifier

	clientFirstBare string
	serverFirst     string
	fullNonce       string
}

// NewScramServer starts a conversation against the given verifier.
func NewScramServer(v *ScramVerifier) *ScramServer { return &ScramServer{verifier: v} }

// HandleClientFirst consumes the client-first-message and returns the
// server-first-message.
func (s *ScramServer) HandleClientFirst(clientFirst string) (string, error) {
	return s.handleClientFirst(clientFirst, "")
}

func (s *ScramServer) handleClientFirst(clientFirst, forcedServerNonce string) (string, error) {
	// gs2 header: "n" (no channel binding) or "y", then authzid, then bare.
	rest, ok := strings.CutPrefix(clientFirst, "n,")
	if !ok {
		if rest, ok = strings.CutPrefix(clientFirst, "y,"); !ok {
			return "", fmt.Errorf("scram: channel binding not supported")
		}
	}
	i := strings.Index(rest, ",")
	if i < 0 {
		return "", fmt.Errorf("scram: malformed client-first-message")
	}
	s.clientFirstBare = rest[i+1:]

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
