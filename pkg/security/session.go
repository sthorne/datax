package security

import (
	"crypto/hmac"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Signed session tokens for the HTTP console (issue #158). A browser
// signs in once and carries a token instead of replaying its password on
// every poll; the token is self-contained and verified from a key every
// node derives from the cluster's shared authentication secret, so a
// token minted by n1 is accepted by n2 with no session store and no
// replication of session state.
//
// The token is not a bearer credential for anything but identity: the
// caller's roles are still resolved per request from the catalog (so a
// dropped role, NOLOGIN, or a revoked admin membership closes the door
// at once), and the transport is the console's TLS. Being stateless, a
// token cannot be revoked before it expires — signing out clears the
// cookie, which ends the session on that browser, and rotating the
// cluster's authentication secret invalidates every outstanding token.
// The TTL is what bounds a stolen token's life, so it is short.

// SessionKeyPurpose is the domain-separation label under which the
// session signing key is derived from the cluster's authentication
// secret. The secret has other users (the SCRAM stand-in salt of issue
// #137); no two of them may see the same key.
const SessionKeyPurpose = "http-session-key-v1"

// sessionMACPurpose separates the signature's input from any other
// message ever MAC'd under the session key.
const sessionMACPurpose = "datax-http-session-v1\x00"

// Errors from ParseSession. They are distinguished so the caller can
// audit an expired token differently from a forged one; neither is ever
// shown to the client, which sees only "sign in again".
var (
	ErrSessionMalformed = errors.New("session token malformed")
	ErrSessionSignature = errors.New("session token signature invalid")
	ErrSessionExpired   = errors.New("session token expired")
)

// DeriveKey derives a purpose-specific key from a shared secret.
func DeriveKey(secret []byte, purpose string) []byte { return hmacSHA256(secret, purpose) }

// SignSession returns a token asserting that user signed in at issued
// and that the assertion is good until expires.
func SignSession(key []byte, user string, issued, expires time.Time) string {
	payload := sessionPayload(user, issued, expires)
	return payload + "." + b64.EncodeToString(hmacSHA256(key, sessionMACPurpose+payload))
}

// ParseSession verifies a token's signature and validity window and
// returns the user it names. now is the clock to judge the window
// against; a token issued in the future beyond sessionSkew is refused,
// so a token minted by a node whose clock has run away does not outlive
// its intended TTL elsewhere.
func ParseSession(key []byte, token string, now time.Time) (string, error) {
	i := strings.LastIndexByte(token, '.')
	if i < 0 {
		return "", ErrSessionMalformed
	}
	payload, sig := token[:i], token[i+1:]
	mac, err := b64.DecodeString(sig)
	if err != nil {
		return "", ErrSessionMalformed
	}
	// Signature first: nothing in the payload is trusted until it holds.
	if !hmac.Equal(mac, hmacSHA256(key, sessionMACPurpose+payload)) {
		return "", ErrSessionSignature
	}
	parts := strings.Split(payload, ".")
	if len(parts) != 3 {
		return "", ErrSessionMalformed
	}
	raw, err := b64.DecodeString(parts[0])
	if err != nil {
		return "", ErrSessionMalformed
	}
	issued, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", ErrSessionMalformed
	}
	expires, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return "", ErrSessionMalformed
	}
	if now.Unix() >= expires || now.Add(sessionSkew).Unix() < issued {
		return "", ErrSessionExpired
	}
	user := string(raw)
	if user == "" {
		return "", ErrSessionMalformed
	}
	return user, nil
}

// sessionSkew is how far ahead of this node's clock a token's issue time
// may sit before it reads as invalid rather than merely young.
const sessionSkew = 5 * time.Minute

// SessionExpiry returns the expiry a token asserts, without verifying
// it — for the console to know when to prompt again. A caller must not
// make an authorization decision from it.
func SessionExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 4 {
		return time.Time{}, false
	}
	expires, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(expires, 0), true
}

func sessionPayload(user string, issued, expires time.Time) string {
	return b64.EncodeToString([]byte(user)) + "." +
		strconv.FormatInt(issued.Unix(), 10) + "." +
		strconv.FormatInt(expires.Unix(), 10)
}

// b64 is URL-safe and unpadded: a token travels in a cookie.
var b64 = base64.RawURLEncoding
