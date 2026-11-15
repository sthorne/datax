package kmstest

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"
)

// tokens is the set of bearer tokens a fake has issued and still honours.
type tokens struct {
	mu    sync.Mutex
	valid map[string]bool
	// Issued counts tokens handed out, by source.
	issued map[string]int
}

func (t *tokens) issue(source string) string {
	tok := newToken("ya29")
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.valid == nil {
		t.valid, t.issued = map[string]bool{}, map[string]int{}
	}
	t.valid[tok] = true
	t.issued[source]++
	return tok
}

func (t *tokens) ok(r *http.Request) bool {
	tok, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	t.mu.Lock()
	defer t.mu.Unlock()
	return found && t.valid[tok]
}

// TokensIssued is how many tokens the fake has handed out from source.
func (t *tokens) TokensIssued(source string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.issued[source]
}

// RevokeTokens makes every token issued so far stop working.
func (t *tokens) RevokeTokens() {
	t.mu.Lock()
	t.valid = map[string]bool{}
	t.mu.Unlock()
}

// ---- Google Cloud KMS ----

// GCP is a fake of Google Cloud KMS holding one symmetric key, Name,
// together with a metadata server and an OAuth token endpoint that
// honours one service account.
type GCP struct {
	Faults
	tokens
	*httptest.Server
	Name string
	ring *keyring
	// account is the service account whose signed assertions the token
	// endpoint accepts.
	account *rsa.PrivateKey
	email   string
}

// NewGCP starts a fake Cloud KMS.
func NewGCP() *GCP {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	g := &GCP{
		Name: "projects/acme/locations/europe-west1/keyRings/datax/cryptoKeys/store-root",
		ring: newKeyring(), account: key, email: "datax@acme.iam.gserviceaccount.com",
	}
	g.Server = httptest.NewServer(http.HandlerFunc(g.serve))
	return g
}

// Rotate makes a new primary version and returns its number.
func (g *GCP) Rotate() int { return g.ring.rotate() }

// ServiceAccountJSON is a service-account key file for the account the
// fake's token endpoint honours.
func (g *GCP) ServiceAccountJSON() []byte {
	der, err := x509.MarshalPKCS8PrivateKey(g.account)
	if err != nil {
		panic(err)
	}
	raw, _ := json.Marshal(map[string]string{
		"type": "service_account", "client_email": g.email, "private_key_id": "key-1",
		"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"token_uri":   g.URL + "/token",
	})
	return raw
}

func (g *GCP) serve(w http.ResponseWriter, r *http.Request) {
	if g.dropped(w) {
		return
	}
	body := readBody(r)
	g.note(body, r.Header.Get("Authorization"))
	fail := func(status int, state, msg string) {
		writeJSON(w, status, map[string]any{"error": map[string]any{"code": status, "message": msg, "status": state}})
	}
	switch {
	case r.URL.Path == "/computeMetadata/v1/instance/service-accounts/default/token":
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "Missing Metadata-Flavor:Google header.", http.StatusForbidden)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"access_token": g.issue("metadata"), "expires_in": 3599, "token_type": "Bearer"})
		return
	case r.URL.Path == "/token":
		g.note(nil, parseForm(body)["assertion"])
		if why := g.checkAssertion(parseForm(body)); why != "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_grant", "error_description": why})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"access_token": g.issue("service-account"), "expires_in": 3599, "token_type": "Bearer"})
		return
	}
	if status := g.faulted(r); status != 0 {
		fail(status, "UNAVAILABLE", "The service is currently unavailable.")
		return
	}
	if !g.ok(r) {
		fail(http.StatusUnauthorized, "UNAUTHENTICATED", "Request had invalid authentication credentials.")
		return
	}
	name, verb, ok := strings.Cut(strings.TrimPrefix(r.URL.Path, "/v1/"), ":")
	if !ok || r.Method != http.MethodPost || (verb != "encrypt" && verb != "decrypt") {
		fail(http.StatusNotFound, "NOT_FOUND", "no such method")
		return
	}
	if name != g.Name {
		fail(http.StatusNotFound, "NOT_FOUND", "CryptoKey "+name+" not found.")
		return
	}
	var req struct {
		Plaintext, Ciphertext       string
		AdditionalAuthenticatedData string
	}
	if json.Unmarshal(body, &req) != nil {
		fail(http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid JSON payload received.")
		return
	}
	aad, err := base64.StdEncoding.DecodeString(req.AdditionalAuthenticatedData)
	if err != nil {
		fail(http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid value at 'additional_authenticated_data'")
		return
	}
	if verb == "encrypt" {
		plain, err := base64.StdEncoding.DecodeString(req.Plaintext)
		if err != nil {
			fail(http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid value at 'plaintext'")
			return
		}
		blob, version := g.ring.seal(plain, aad)
		writeJSON(w, http.StatusOK, map[string]string{
			"name":       fmt.Sprintf("%s/cryptoKeyVersions/%d", g.Name, version),
			"ciphertext": base64.StdEncoding.EncodeToString(append([]byte{byte(version)}, blob...)),
		})
		return
	}
	blob, err := base64.StdEncoding.DecodeString(req.Ciphertext)
	if err != nil || len(blob) < 1 {
		fail(http.StatusBadRequest, "INVALID_ARGUMENT", "Decryption failed: the ciphertext is invalid.")
		return
	}
	plain, ok := g.ring.open(blob[1:], int(blob[0]), aad, g.lax())
	if !ok {
		fail(http.StatusBadRequest, "INVALID_ARGUMENT", "Decryption failed: the ciphertext is invalid.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"plaintext": base64.StdEncoding.EncodeToString(plain), "usedPrimary": int(blob[0]) == g.ring.latest()})
}

// checkAssertion verifies a JWT bearer grant: an RS256 signature by the
// service account, over claims that name it, the KMS scope and this
// endpoint, and that have not expired.
func (g *GCP) checkAssertion(form map[string]string) string {
	if form["grant_type"] != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
		return "unsupported grant type"
	}
	parts := strings.Split(form["assertion"], ".")
	if len(parts) != 3 {
		return "the assertion is not a JWT"
	}
	dec := base64.RawURLEncoding.DecodeString
	header, err1 := dec(parts[0])
	claims, err2 := dec(parts[1])
	sig, err3 := dec(parts[2])
	if err1 != nil || err2 != nil || err3 != nil {
		return "the assertion is not base64url"
	}
	var h struct{ Alg, Typ string }
	if json.Unmarshal(header, &h) != nil || h.Alg != "RS256" {
		return "the assertion is not signed with RS256"
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(&g.account.PublicKey, crypto.SHA256, digest[:], sig) != nil {
		return "Invalid JWT Signature."
	}
	var c struct {
		Iss, Scope, Aud string
		Iat, Exp        int64
	}
	if json.Unmarshal(claims, &c) != nil {
		return "the claims are not JSON"
	}
	now := time.Now().Unix()
	switch {
	case c.Iss != g.email:
		return "the issuer is not a known service account"
	case c.Aud != g.URL+"/token":
		return "the audience is not this token endpoint"
	case !strings.Contains(c.Scope, "cloudkms") && !strings.Contains(c.Scope, "cloud-platform"):
		return "the scope does not cover Cloud KMS"
	case c.Exp <= now || c.Exp-c.Iat > 3600 || c.Iat > now+60:
		return "the assertion's lifetime is not acceptable"
	}
	return ""
}

// ---- Azure Key Vault ----

// Azure is a fake of Azure Key Vault holding one RSA key, Key, with
// versions, together with the two places a node gets a token: the
// client-credentials endpoint of the tenant Tenant, and the instance's
// managed identity.
type Azure struct {
	Faults
	tokens
	*httptest.Server
	Key                            string
	Tenant, ClientID, ClientSecret string

	zmu      sync.Mutex
	versions map[string]*rsa.PrivateKey
	current  string
}

// NewAzure starts a fake Key Vault.
func NewAzure() *Azure {
	z := &Azure{
		Key: "store-root", Tenant: "72f988bf-0000-0000-0000-2d7cd011db47", ClientID: "datax-app",
		ClientSecret: newToken("client-secret"), versions: map[string]*rsa.PrivateKey{},
	}
	z.Rotate()
	z.Server = httptest.NewServer(http.HandlerFunc(z.serve))
	return z
}

// Rotate makes a new current version of the key and returns its name.
func (z *Azure) Rotate() string {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	name := make([]byte, 16)
	if _, err := rand.Read(name); err != nil {
		panic(err)
	}
	z.zmu.Lock()
	defer z.zmu.Unlock()
	z.current = hex.EncodeToString(name)
	z.versions[z.current] = key
	return z.current
}

func (z *Azure) serve(w http.ResponseWriter, r *http.Request) {
	if z.dropped(w) {
		return
	}
	body := readBody(r)
	z.note(body, r.Header.Get("Authorization"))
	fail := func(status int, code, msg string) {
		writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
	}
	switch {
	case r.URL.Path == "/metadata/identity/oauth2/token":
		if r.Header.Get("Metadata") != "true" || r.URL.Query().Get("resource") != "https://vault.azure.net" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "error_description": "Required metadata header not specified"})
			return
		}
		// The instance endpoint writes its numbers as strings.
		writeJSON(w, http.StatusOK, map[string]string{"access_token": z.issue("managed-identity:" + r.URL.Query().Get("client_id")), "expires_in": "3599", "token_type": "Bearer"})
		return
	case r.URL.Path == "/"+z.Tenant+"/oauth2/v2.0/token":
		form := parseForm(body)
		z.note(nil, form["client_secret"])
		if form["grant_type"] != "client_credentials" || form["client_id"] != z.ClientID || form["client_secret"] != z.ClientSecret || form["scope"] != "https://vault.azure.net/.default" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_client", "error_description": "AADSTS7000215: Invalid client secret provided."})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"access_token": z.issue("client-secret"), "expires_in": 3599, "token_type": "Bearer"})
		return
	}
	if status := z.faulted(r); status != 0 {
		fail(status, "ServiceUnavailable", "The service is temporarily unavailable.")
		return
	}
	if !z.ok(r) {
		fail(http.StatusUnauthorized, "Unauthorized", "AKV10000: Request is missing a Bearer or PoP token.")
		return
	}
	// /keys/<key>[/<version>]/<wrapkey|unwrapkey>
	p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if r.Method != http.MethodPost || len(p) < 3 || len(p) > 4 || p[0] != "keys" || r.URL.Query().Get("api-version") == "" {
		fail(http.StatusNotFound, "NotFound", "no such operation")
		return
	}
	verb := p[len(p)-1]
	z.zmu.Lock()
	version := z.current
	if len(p) == 4 {
		version = p[2]
	}
	key := z.versions[version]
	z.zmu.Unlock()
	if p[1] != z.Key || key == nil {
		fail(http.StatusNotFound, "KeyNotFound", "A key with (name/id) "+strings.Join(p[1:len(p)-1], "/")+" was not found in this key vault.")
		return
	}
	var req struct{ Alg, Value string }
	if json.Unmarshal(body, &req) != nil || req.Alg != "RSA-OAEP-256" {
		fail(http.StatusBadRequest, "BadParameter", "Operation is not supported with this algorithm.")
		return
	}
	value, err := base64.RawURLEncoding.DecodeString(req.Value)
	if err != nil {
		fail(http.StatusBadRequest, "BadParameter", "The value is not base64url.")
		return
	}
	var out []byte
	switch verb {
	case "wrapkey":
		out, err = rsa.EncryptOAEP(sha256.New(), rand.Reader, &key.PublicKey, value, nil)
	case "unwrapkey":
		out, err = rsa.DecryptOAEP(sha256.New(), nil, key, value, nil)
	default:
		fail(http.StatusNotFound, "NotFound", "no such operation")
		return
	}
	if err != nil {
		fail(http.StatusBadRequest, "BadParameter", "The operation failed: the value could not be processed with this key.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"kid":   fmt.Sprintf("%s/keys/%s/%s", z.URL, z.Key, version),
		"value": base64.RawURLEncoding.EncodeToString(out),
	})
}
