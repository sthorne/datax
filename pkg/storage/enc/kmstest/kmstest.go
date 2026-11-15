// Package kmstest holds fakes of the key management services datax can
// keep a store key under (issue #308): enough of each service's REST API,
// served from this process, for the roots in pkg/storage/enc to be tested
// against without a network or an account.
//
// Each fake does real cryptography with keys of its own and checks what
// the real service checks of a request — the token, the signature, the
// associated data — so that a root which sends the wrong thing fails here
// too. It deliberately shares no code with the roots: the request signing
// in particular is implemented again, so that the signer is held to a
// second reading of the specification rather than to itself.
//
// A fake is not the service. It answers the calls the roots make, with
// the error shapes the services document, and nothing here shows that a
// root works against the real thing.
package kmstest

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Faults is what a fake can be told to do wrong. Every fake embeds one.
type Faults struct {
	mu sync.Mutex
	// down: connections are accepted and dropped, as by a host that is
	// there and a service that is not.
	down bool
	// failStatus, failLeft: the next failLeft calls to the key (all of
	// them when negative) are answered with failStatus.
	failStatus, failLeft int
	delay                time.Duration
	// ignoreContext: decrypt whatever the request says of the context,
	// as a service would that did not authenticate it.
	ignoreContext bool
	calls         int
	seen, creds   []string
}

// Down makes the service unreachable, or reachable again.
func (f *Faults) Down(down bool) { f.mu.Lock(); f.down = down; f.mu.Unlock() }

// Fail answers the next n calls to the key with status (every call, when
// n is negative; none, when n is zero).
func (f *Faults) Fail(status, n int) {
	f.mu.Lock()
	f.failStatus, f.failLeft = status, n
	f.mu.Unlock()
}

// Delay makes every call wait before it is answered.
func (f *Faults) Delay(d time.Duration) { f.mu.Lock(); f.delay = d; f.mu.Unlock() }

// IgnoreContext makes decrypt disregard the context a request names.
func (f *Faults) IgnoreContext(on bool) { f.mu.Lock(); f.ignoreContext = on; f.mu.Unlock() }

// Calls is how many calls to the key have arrived, answered or not.
func (f *Faults) Calls() int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }

// Seen returns every request body the fake has received, for a test to
// search for what must not have been sent.
func (f *Faults) Seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

// Credentials returns every credential the fake has received — tokens,
// signatures, secrets, assertions — for a test to know what no message
// may hold.
func (f *Faults) Credentials() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.creds...)
}

func (f *Faults) lax() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.ignoreContext }

func (f *Faults) note(body []byte, creds ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, string(body))
	for _, c := range creds {
		if c != "" {
			f.creds = append(f.creds, c)
		}
	}
}

// dropped reports whether the request was dropped because the service is
// down; it applies to every endpoint of a fake.
func (f *Faults) dropped(w http.ResponseWriter) bool {
	f.mu.Lock()
	down := f.down
	f.mu.Unlock()
	if !down {
		return false
	}
	if hj, ok := w.(http.Hijacker); ok {
		if c, _, err := hj.Hijack(); err == nil {
			_ = c.Close()
		}
	}
	return true
}

// faulted applies the delay and the status faults to a call to the key,
// and reports the status to answer with (0: carry on).
func (f *Faults) faulted(r *http.Request) int {
	f.mu.Lock()
	f.calls++
	delay, status := f.delay, 0
	if f.failLeft != 0 {
		status = f.failStatus
		if f.failLeft > 0 {
			f.failLeft--
		}
	}
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
		}
	}
	return status
}

// keyring is a symmetric key with versions, as a service rotates one.
type keyring struct {
	mu       sync.Mutex
	versions [][]byte
}

func newKeyring() *keyring {
	k := &keyring{}
	k.rotate()
	return k
}

func (k *keyring) rotate() int {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(err)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.versions = append(k.versions, key)
	return len(k.versions)
}

func (k *keyring) latest() int { k.mu.Lock(); defer k.mu.Unlock(); return len(k.versions) }

func (k *keyring) gcm(version int) (cipher.AEAD, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if version < 1 || version > len(k.versions) {
		return nil, false
	}
	b, err := aes.NewCipher(k.versions[version-1])
	if err != nil {
		panic(err)
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		panic(err)
	}
	return g, true
}

// seal encrypts under the latest version. The associated data is stored
// beside the ciphertext as well as authenticated, which is what lets a
// fake told to ignore the context decrypt regardless.
func (k *keyring) seal(plaintext, aad []byte) (blob []byte, version int) {
	version = k.latest()
	g, _ := k.gcm(version)
	nonce := make([]byte, g.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		panic(err)
	}
	blob = append([]byte{byte(len(aad) >> 8), byte(len(aad))}, aad...)
	return g.Seal(append(blob, nonce...), nonce, plaintext, aad), version
}

func (k *keyring) open(blob []byte, version int, aad []byte, lax bool) ([]byte, bool) {
	g, ok := k.gcm(version)
	if !ok || len(blob) < 2 {
		return nil, false
	}
	n := int(blob[0])<<8 | int(blob[1])
	if len(blob) < 2+n+g.NonceSize() {
		return nil, false
	}
	stored, rest := blob[2:2+n], blob[2+n:]
	if lax {
		aad = stored
	}
	out, err := g.Open(nil, rest[:g.NonceSize()], rest[g.NonceSize():], aad)
	return out, err == nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readBody(r *http.Request) []byte {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	return b
}

func newToken(prefix string) string {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return prefix + "." + base64.RawURLEncoding.EncodeToString(b)
}

// ---- Vault Transit ----

// Vault is a fake of HashiCorp Vault's Transit engine holding one key,
// Key, on the mount Mount.
type Vault struct {
	Faults
	*httptest.Server
	Mount, Key string
	ring       *keyring

	vmu       sync.Mutex
	token     string
	namespace string
	derived   bool
}

// NewVault starts a fake Vault that accepts token.
func NewVault(token string) *Vault {
	v := &Vault{Mount: "transit", Key: "datax", ring: newKeyring(), token: token}
	v.Server = httptest.NewServer(http.HandlerFunc(v.serve))
	return v
}

// SetToken replaces the token the fake accepts.
func (v *Vault) SetToken(t string) { v.vmu.Lock(); v.token = t; v.vmu.Unlock() }

// RequireNamespace makes the fake refuse requests for any other namespace.
func (v *Vault) RequireNamespace(ns string) { v.vmu.Lock(); v.namespace = ns; v.vmu.Unlock() }

// Derived makes the key a derived one: every call must carry a context,
// and associated data is not accepted.
func (v *Vault) Derived(on bool) { v.vmu.Lock(); v.derived = on; v.vmu.Unlock() }

// Rotate adds a key version and returns its number.
func (v *Vault) Rotate() int { return v.ring.rotate() }

func (v *Vault) serve(w http.ResponseWriter, r *http.Request) {
	if v.dropped(w) {
		return
	}
	body := readBody(r)
	v.note(body, r.Header.Get("X-Vault-Token"))
	fail := func(status int, msg string) { writeJSON(w, status, map[string]any{"errors": []string{msg}}) }
	if status := v.faulted(r); status != 0 {
		fail(status, "Vault is sealed")
		return
	}
	v.vmu.Lock()
	token, ns, derived := v.token, v.namespace, v.derived
	v.vmu.Unlock()
	if !hmac.Equal([]byte(r.Header.Get("X-Vault-Token")), []byte(token)) {
		fail(http.StatusForbidden, "permission denied")
		return
	}
	if ns != "" && r.Header.Get("X-Vault-Namespace") != ns {
		fail(http.StatusForbidden, "permission denied")
		return
	}
	op, ok := strings.CutPrefix(r.URL.Path, "/v1/"+v.Mount+"/")
	verb, key, _ := strings.Cut(op, "/")
	if !ok || r.Method != http.MethodPost || (verb != "encrypt" && verb != "decrypt") {
		fail(http.StatusNotFound, "unsupported path")
		return
	}
	if key != v.Key {
		fail(http.StatusBadRequest, "encryption key not found")
		return
	}
	var req struct {
		Plaintext, Ciphertext, Context string
		AssociatedData                 string `json:"associated_data"`
	}
	if json.Unmarshal(body, &req) != nil {
		fail(http.StatusBadRequest, "failed to parse JSON input")
		return
	}
	aadText := req.AssociatedData
	if derived {
		if req.Context == "" {
			fail(http.StatusBadRequest, "missing 'context' for key derivation; the key was created using a derived key, which means additional, per-request information must be included in order to perform operations with the key")
			return
		}
		aadText = req.Context
	}
	aad, err := base64.StdEncoding.DecodeString(aadText)
	if err != nil {
		fail(http.StatusBadRequest, "failed to base64-decode associated data")
		return
	}
	if verb == "encrypt" {
		plain, err := base64.StdEncoding.DecodeString(req.Plaintext)
		if err != nil {
			fail(http.StatusBadRequest, "failed to base64-decode plaintext")
			return
		}
		blob, version := v.ring.seal(plain, aad)
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{
			"ciphertext":  fmt.Sprintf("vault:v%d:%s", version, base64.StdEncoding.EncodeToString(blob)),
			"key_version": version,
		}})
		return
	}
	var version int
	p := strings.SplitN(req.Ciphertext, ":", 3)
	if len(p) != 3 || p[0] != "vault" {
		fail(http.StatusBadRequest, "invalid ciphertext: no prefix")
		return
	}
	if _, err := fmt.Sscanf(p[1], "v%d", &version); err != nil {
		fail(http.StatusBadRequest, "invalid ciphertext: version number could not be decoded")
		return
	}
	blob, err := base64.StdEncoding.DecodeString(p[2])
	if err != nil {
		fail(http.StatusBadRequest, "invalid ciphertext: could not decode base64")
		return
	}
	plain, ok := v.ring.open(blob, version, aad, v.lax())
	if !ok {
		fail(http.StatusBadRequest, "cipher: message authentication failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"plaintext": base64.StdEncoding.EncodeToString(plain)}})
}

// ---- AWS KMS ----

// AWS is a fake of AWS KMS holding one key, ARN, together with the
// places a node finds credentials for it: the instance metadata service
// (version 2 only), STS for a web identity, and a container credentials
// endpoint at /container.
type AWS struct {
	Faults
	*httptest.Server
	ARN, Region string
	ring        *keyring

	amu sync.Mutex
	// creds maps an access key id to its secret and session token.
	creds map[string][2]string
	// WebIdentityToken is the token STS accepts; ContainerAuth, when
	// set, is the Authorization the container endpoint requires.
	WebIdentityToken, ContainerAuth string
	// Role is the instance's role name.
	Role string
	imds map[string]bool
	// Issued counts temporary credentials handed out, by source.
	Issued map[string]int
}

// NewAWS starts a fake KMS (and its credential sources) in region.
func NewAWS(region string) *AWS {
	a := &AWS{
		Region: region, ARN: "arn:aws:kms:" + region + ":111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
		ring: newKeyring(), creds: map[string][2]string{}, Role: "datax-node", imds: map[string]bool{}, Issued: map[string]int{},
	}
	a.Server = httptest.NewServer(http.HandlerFunc(a.serve))
	return a
}

// AddCredentials makes the fake accept requests signed with these.
func (a *AWS) AddCredentials(accessKey, secret, sessionToken string) {
	a.amu.Lock()
	a.creds[accessKey] = [2]string{secret, sessionToken}
	a.amu.Unlock()
}

// Rotate replaces the key's material. As with the real service, nothing
// a caller sees changes: old ciphertexts still decrypt.
func (a *AWS) Rotate() int { return a.ring.rotate() }

func (a *AWS) issue(source string) (id, secret, token string) {
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		panic(err)
	}
	id, secret, token = "ASIA"+strings.ToUpper(hex.EncodeToString(suffix)), newToken("secret"), newToken("session")
	a.AddCredentials(id, secret, token)
	a.amu.Lock()
	a.Issued[source]++
	a.amu.Unlock()
	return
}

func (a *AWS) credentialDoc(source string) map[string]any {
	id, secret, token := a.issue(source)
	return map[string]any{
		"Code": "Success", "AccessKeyId": id, "SecretAccessKey": secret, "Token": token,
		"Expiration": time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	}
}

func (a *AWS) serve(w http.ResponseWriter, r *http.Request) {
	if a.dropped(w) {
		return
	}
	body := readBody(r)
	a.note(body, r.Header.Get("Authorization"), r.Header.Get("X-Amz-Security-Token"), r.Header.Get("X-aws-ec2-metadata-token"))
	switch {
	case r.URL.Path == "/latest/api/token":
		// IMDSv2: a session token, by PUT and with a lifetime.
		if r.Method != http.MethodPut || r.Header.Get("X-aws-ec2-metadata-token-ttl-seconds") == "" {
			http.Error(w, "", http.StatusForbidden)
			return
		}
		t := newToken("imds")
		a.amu.Lock()
		a.imds[t] = true
		a.amu.Unlock()
		_, _ = io.WriteString(w, t)
	case strings.HasPrefix(r.URL.Path, "/latest/meta-data/iam/security-credentials/"):
		a.amu.Lock()
		ok := a.imds[r.Header.Get("X-aws-ec2-metadata-token")]
		a.amu.Unlock()
		if !ok {
			// Version 1, or no session: refused.
			http.Error(w, "", http.StatusUnauthorized)
			return
		}
		switch role := strings.TrimPrefix(r.URL.Path, "/latest/meta-data/iam/security-credentials/"); role {
		case "":
			_, _ = io.WriteString(w, a.Role)
		case a.Role:
			writeJSON(w, http.StatusOK, a.credentialDoc("imds"))
		default:
			http.Error(w, "", http.StatusNotFound)
		}
	case r.URL.Path == "/container":
		if a.ContainerAuth != "" && r.Header.Get("Authorization") != a.ContainerAuth {
			http.Error(w, "", http.StatusForbidden)
			return
		}
		writeJSON(w, http.StatusOK, a.credentialDoc("container"))
	case r.URL.Path == "/" && r.Header.Get("X-Amz-Target") == "":
		a.serveSTS(w, r, body)
	case r.URL.Path == "/":
		a.serveKMS(w, r, body)
	default:
		http.Error(w, "", http.StatusNotFound)
	}
}

func (a *AWS) serveSTS(w http.ResponseWriter, r *http.Request, body []byte) {
	q := parseForm(body)
	a.note(nil, q["WebIdentityToken"])
	sterr := func(code, msg string) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, "<ErrorResponse><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error></ErrorResponse>", code, msg)
	}
	if q["Action"] != "AssumeRoleWithWebIdentity" || q["RoleArn"] == "" || q["RoleSessionName"] == "" {
		sterr("InvalidAction", "only AssumeRoleWithWebIdentity is served")
		return
	}
	if a.WebIdentityToken == "" || q["WebIdentityToken"] != a.WebIdentityToken {
		sterr("InvalidIdentityToken", "the web identity token is not valid")
		return
	}
	id, secret, token := a.issue("sts")
	fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>%s</AccessKeyId><SecretAccessKey>%s</SecretAccessKey><SessionToken>%s</SessionToken><Expiration>%s</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`,
		id, secret, token, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
}

func parseForm(body []byte) map[string]string {
	out := map[string]string{}
	vals, _ := url.ParseQuery(string(body))
	for k := range vals {
		out[k] = vals.Get(k)
	}
	return out
}

// verifySigV4 recomputes the request's signature from the request as it
// arrived and the secret the fake holds for the access key, and returns
// why it is refused ("" when it is good). It follows the Signature
// Version 4 specification on its own, not the signer under test.
func (a *AWS) verifySigV4(r *http.Request, body []byte) string {
	auth := r.Header.Get("Authorization")
	rest, ok := strings.CutPrefix(auth, "AWS4-HMAC-SHA256 ")
	if !ok {
		return "the Authorization header is not AWS4-HMAC-SHA256"
	}
	fields := map[string]string{}
	for _, part := range strings.Split(rest, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		fields[k] = v
	}
	cred := strings.Split(fields["Credential"], "/")
	if len(cred) != 5 || cred[4] != "aws4_request" {
		return "the credential scope is malformed"
	}
	accessKey, date, region, service := cred[0], cred[1], cred[2], cred[3]
	if region != a.Region || service != "kms" {
		return fmt.Sprintf("the credential scope is %s/%s, not %s/kms", region, service, a.Region)
	}
	a.amu.Lock()
	known, ok := a.creds[accessKey]
	a.amu.Unlock()
	if !ok {
		return "the access key id is not known"
	}
	if known[1] != r.Header.Get("X-Amz-Security-Token") {
		return "the security token does not belong to the access key"
	}
	amzDate := r.Header.Get("X-Amz-Date")
	when, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil || !strings.HasPrefix(amzDate, date) {
		return "X-Amz-Date is missing or disagrees with the scope"
	}
	if d := time.Since(when); d > 5*time.Minute || d < -5*time.Minute {
		return "the signature has expired"
	}
	signed := strings.Split(fields["SignedHeaders"], ";")
	if !sort.StringsAreSorted(signed) {
		return "the signed headers are not sorted"
	}
	must := map[string]bool{"host": false, "x-amz-date": false, "x-amz-target": false}
	if known[1] != "" {
		must["x-amz-security-token"] = false
	}
	var canon strings.Builder
	canon.WriteString(r.Method + "\n" + r.URL.EscapedPath() + "\n" + r.URL.RawQuery + "\n")
	for _, name := range signed {
		value := r.Header.Get(name)
		if name == "host" {
			value = r.Host
		}
		canon.WriteString(name + ":" + strings.Join(strings.Fields(value), " ") + "\n")
		if _, ok := must[name]; ok {
			must[name] = true
		}
	}
	for name, ok := range must {
		if !ok {
			return "the header " + name + " is not signed"
		}
	}
	payload := sha256.Sum256(body)
	canon.WriteString("\n" + fields["SignedHeaders"] + "\n" + hex.EncodeToString(payload[:]))
	canonHash := sha256.Sum256([]byte(canon.String()))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + date + "/" + region + "/" + service + "/aws4_request\n" + hex.EncodeToString(canonHash[:])
	mac := func(key []byte, msg string) []byte {
		m := hmac.New(sha256.New, key)
		_, _ = io.WriteString(m, msg)
		return m.Sum(nil)
	}
	key := mac(mac(mac(mac([]byte("AWS4"+known[0]), date), region), service), "aws4_request")
	if want := hex.EncodeToString(mac(key, toSign)); !hmac.Equal([]byte(want), []byte(fields["Signature"])) {
		return "the request signature we calculated does not match the signature you provided"
	}
	return ""
}

func (a *AWS) serveKMS(w http.ResponseWriter, r *http.Request, body []byte) {
	fail := func(status int, typ, msg string) {
		writeJSON(w, status, map[string]string{"__type": typ, "message": msg})
	}
	if status := a.faulted(r); status != 0 {
		// A throttled request is a 400 in this protocol; a refusal names
		// its own exception.
		switch {
		case status == http.StatusBadRequest:
			fail(status, "ThrottlingException", "Rate exceeded")
		case status >= 500:
			fail(status, "KMSInternalException", "the service is having a bad day")
		default:
			fail(status, "AccessDeniedException", "User: arn:aws:iam::111122223333:role/datax is not authorized to perform this operation on the key")
		}
		return
	}
	if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/x-amz-json-1.1" {
		fail(http.StatusBadRequest, "UnknownOperationException", "not a JSON 1.1 request")
		return
	}
	if why := a.verifySigV4(r, body); why != "" {
		fail(http.StatusForbidden, "InvalidSignatureException", why)
		return
	}
	var req struct {
		KeyID, Plaintext, CiphertextBlob string
		EncryptionContext                map[string]string
	}
	if json.Unmarshal(body, &req) != nil {
		fail(http.StatusBadRequest, "SerializationException", "the body is not JSON")
		return
	}
	if req.KeyID != a.ARN {
		fail(http.StatusBadRequest, "NotFoundException", "Key '"+req.KeyID+"' does not exist")
		return
	}
	// The encryption context as associated data: its pairs, sorted.
	names := make([]string, 0, len(req.EncryptionContext))
	for n := range req.EncryptionContext {
		names = append(names, n)
	}
	sort.Strings(names)
	var aad []byte
	for _, n := range names {
		aad = append(aad, (n + "\x00" + req.EncryptionContext[n] + "\x00")...)
	}
	switch r.Header.Get("X-Amz-Target") {
	case "TrentService.Encrypt":
		plain, err := base64.StdEncoding.DecodeString(req.Plaintext)
		if err != nil || len(plain) == 0 {
			fail(http.StatusBadRequest, "ValidationException", "Plaintext is not base64")
			return
		}
		blob, version := a.ring.seal(plain, aad)
		// The material's number rides in the blob, as the real one's
		// identifies its key.
		writeJSON(w, http.StatusOK, map[string]string{"KeyId": a.ARN, "CiphertextBlob": base64.StdEncoding.EncodeToString(append([]byte{byte(version)}, blob...))})
	case "TrentService.Decrypt":
		blob, err := base64.StdEncoding.DecodeString(req.CiphertextBlob)
		if err != nil || len(blob) < 1 {
			fail(http.StatusBadRequest, "InvalidCiphertextException", "")
			return
		}
		plain, ok := a.ring.open(blob[1:], int(blob[0]), aad, a.lax())
		if !ok {
			fail(http.StatusBadRequest, "InvalidCiphertextException", "")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"KeyId": a.ARN, "Plaintext": base64.StdEncoding.EncodeToString(plain)})
	default:
		fail(http.StatusBadRequest, "UnknownOperationException", "only Encrypt and Decrypt are served")
	}
}
