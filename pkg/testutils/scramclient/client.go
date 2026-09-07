// Package scramclient is the client side of SCRAM-SHA-256 (RFC 7677),
// enough of it to prove a password to the SQL listener from a test
// without a database driver — so a test can hold an exchange at any
// point, or run many in a chosen order, which a driver does not allow.
package scramclient

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/security"
)

// Client runs one exchange. Nonce is the client nonce; any non-empty
// printable string will do for a test.
type Client struct {
	User, Password, Nonce string
	firstBare             string
}

// First is client-first, for a SASLInitialResponse under
// security.MechScram (no channel binding: gs2 flag n).
func (c *Client) First() string {
	c.firstBare = "n=" + c.User + ",r=" + c.Nonce
	return "n,," + c.firstBare
}

// Final is client-final, for a SASLResponse, given the server-first
// carried by AuthenticationSASLContinue.
func (c *Client) Final(serverFirst string) (string, error) {
	var nonce string
	var salt []byte
	var iterations int
	for _, part := range strings.Split(serverFirst, ",") {
		if len(part) < 2 || part[1] != '=' {
			return "", fmt.Errorf("bad server-first %q", serverFirst)
		}
		var err error
		switch part[0] {
		case 'r':
			nonce = part[2:]
		case 's':
			salt, err = base64.StdEncoding.DecodeString(part[2:])
		case 'i':
			iterations, err = strconv.Atoi(part[2:])
		}
		if err != nil {
			return "", err
		}
	}
	if !strings.HasPrefix(nonce, c.Nonce) {
		return "", fmt.Errorf("server nonce %q does not extend the client's", nonce)
	}
	withoutProof := "c=biws,r=" + nonce
	authMessage := c.firstBare + "," + serverFirst + "," + withoutProof
	salted, err := pbkdf2.Key(sha256.New, security.SASLprep(c.Password), salt, iterations, sha256.Size)
	if err != nil {
		return "", err
	}
	clientKey := hmacSHA256(salted, "Client Key")
	storedKey := sha256.Sum256(clientKey)
	sig := hmacSHA256(storedKey[:], authMessage)
	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ sig[i]
	}
	return withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof), nil
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}
