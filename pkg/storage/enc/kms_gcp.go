package enc

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

// The gcpkms root: Google Cloud Key Management Service.
//
//	gcpkms:projects/<p>/locations/<l>/keyRings/<r>/cryptoKeys/<k>[?options]
//
// The calls are cryptoKeys.encrypt and cryptoKeys.decrypt on a symmetric
// key. The binding is the request's additionalAuthenticatedData, which
// the service authenticates: a decrypt under any other fails there.
//
// The access token comes from a service-account key file when one is
// named (the credentials option, or GOOGLE_APPLICATION_CREDENTIALS) —
// a signed JWT is exchanged for a token at the file's token_uri — and
// otherwise from the metadata server, as the instance's or the pod's
// service account.
//
// Options:
//
//	credentials   a service-account key file (JSON)
//	endpoint      the KMS endpoint (default https://cloudkms.googleapis.com)
//	metadata      the metadata server (GCE_METADATA_HOST)
//	ca, timeout, retries
type gcpService struct {
	h        *kmsHTTP
	name     string
	endpoint string
	credFile string
	metadata string
	token    kmsToken
}

var gcpKeyName = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+$`)

const gcpKMSScope = "https://www.googleapis.com/auth/cloudkms"

func newGCPKMSRoot(rest string) (Root, error) {
	const kind = "gcpkms"
	name, opts, err := splitKMSSpec(kind, rest)
	if err != nil {
		return nil, err
	}
	if !gcpKeyName.MatchString(name) {
		return nil, fmt.Errorf("gcpkms root: %q is not a key's resource name (want gcpkms:projects/<project>/locations/<location>/keyRings/<ring>/cryptoKeys/<key>, without a version)", name)
	}
	s := &gcpService{name: name, endpoint: "https://cloudkms.googleapis.com"}
	if v := opts.take("endpoint"); v != "" {
		if s.endpoint, err = endpointURL(kind, "endpoint", v); err != nil {
			return nil, err
		}
	}
	s.metadata = "http://metadata.google.internal"
	if v := opts.take("metadata"); v != "" {
		if s.metadata, err = metadataURL(kind, "metadata", v); err != nil {
			return nil, err
		}
	} else if host := os.Getenv("GCE_METADATA_HOST"); host != "" {
		s.metadata = "http://" + host
	}
	s.credFile = opts.takeOr("credentials", "GOOGLE_APPLICATION_CREDENTIALS")
	if s.h, err = newKMSHTTP(opts, "Google Cloud KMS key "+name); err != nil {
		return nil, err
	}
	if err := opts.done(); err != nil {
		return nil, err
	}
	return &kmsRoot{kind: kind, spec: rest, key: name, svc: s}, nil
}

// oauthTokenDoc is a token endpoint's answer. Azure's instance endpoint
// writes expires_in as a string, hence the Number.
type oauthTokenDoc struct {
	AccessToken string      `json:"access_token"`
	ExpiresIn   json.Number `json:"expires_in"`
	Error       string      `json:"error"`
	Description string      `json:"error_description"`
}

func parseOAuthToken(raw []byte) (string, time.Duration, error) {
	defer Zero(raw)
	var d oauthTokenDoc
	if err := json.Unmarshal(raw, &d); err != nil || d.AccessToken == "" {
		return "", 0, &kmsError{msg: "the token endpoint answered without a token"}
	}
	secs, _ := d.ExpiresIn.Int64()
	if secs <= 0 {
		// An endpoint that does not say is taken to mean "not for long".
		secs = 300
	}
	return d.AccessToken, time.Duration(secs) * time.Second, nil
}

// oauthReason is a token endpoint's refusal: {"error": ...,
// "error_description": ...}.
func oauthReason(_ int, body []byte) (string, bool) {
	var d oauthTokenDoc
	if json.Unmarshal(body, &d) != nil || d.Error == "" {
		return "", false
	}
	return strings.TrimSpace(d.Error + ": " + d.Description), false
}

func (s *gcpService) fetchToken(ctx context.Context) (string, time.Duration, error) {
	if s.credFile != "" {
		return s.serviceAccountToken(ctx)
	}
	raw, err := s.h.once(ctx, kmsRequest{op: "fetching a token from the metadata server", local: true, reason: oauthReason, build: func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.metadata+"/computeMetadata/v1/instance/service-accounts/default/token", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Metadata-Flavor", "Google")
		return req, nil
	}})
	if err != nil {
		return "", 0, &kmsError{msg: fmt.Sprintf("no Google credentials: no service-account key file is named (?credentials=<file> or GOOGLE_APPLICATION_CREDENTIALS) and the metadata server gave no token (%v)", err), transient: kmsTransient(err)}
	}
	return parseOAuthToken(raw)
}

// serviceAccountToken signs a JWT with the service account's key and
// exchanges it for an access token (the OAuth 2.0 JWT bearer grant). The
// file is read each time a token is needed, about once an hour, so a
// replaced key file is picked up without a restart.
func (s *gcpService) serviceAccountToken(ctx context.Context) (string, time.Duration, error) {
	raw, err := os.ReadFile(s.credFile)
	if err != nil {
		return "", 0, &kmsError{msg: fmt.Sprintf("reading the service-account key file: %v", err)}
	}
	defer Zero(raw)
	var sa struct {
		Type         string `json:"type"`
		ClientEmail  string `json:"client_email"`
		PrivateKey   string `json:"private_key"`
		PrivateKeyID string `json:"private_key_id"`
		TokenURI     string `json:"token_uri"`
	}
	if err := json.Unmarshal(raw, &sa); err != nil {
		return "", 0, &kmsError{msg: fmt.Sprintf("%s is not a service-account key file (it is not JSON)", s.credFile)}
	}
	if sa.Type != "service_account" || sa.ClientEmail == "" || sa.PrivateKey == "" {
		return "", 0, &kmsError{msg: fmt.Sprintf("%s is not a service-account key file (type %q); other Google credential types are not supported", s.credFile, sa.Type)}
	}
	if sa.TokenURI == "" {
		sa.TokenURI = "https://oauth2.googleapis.com/token"
	}
	block, _ := pem.Decode([]byte(sa.PrivateKey))
	if block == nil {
		return "", 0, &kmsError{msg: fmt.Sprintf("the private key in %s is not PEM", s.credFile)}
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return "", 0, &kmsError{msg: fmt.Sprintf("the private key in %s could not be parsed", s.credFile)}
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return "", 0, &kmsError{msg: fmt.Sprintf("the private key in %s is not an RSA key", s.credFile)}
	}
	now := time.Now()
	b64 := base64.RawURLEncoding.EncodeToString
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": sa.PrivateKeyID})
	claims, _ := json.Marshal(map[string]any{
		"iss": sa.ClientEmail, "scope": gcpKMSScope, "aud": sa.TokenURI,
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	})
	signing := b64(header) + "." + b64(claims)
	digest := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", 0, &kmsError{msg: "signing the token request failed"}
	}
	assertion := s.h.secret(signing + "." + b64(sig))
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {assertion}}.Encode()
	body, err := s.h.once(ctx, kmsRequest{op: "exchanging the service account's assertion for a token", reason: oauthReason, build: func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, sa.TokenURI, strings.NewReader(form))
		if err != nil {
			return nil, &kmsError{msg: fmt.Sprintf("the token_uri in %s is not a URL", s.credFile)}
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return req, nil
	}})
	if err != nil {
		return "", 0, &kmsError{msg: fmt.Sprintf("a token for service account %s: %v", sa.ClientEmail, err), transient: kmsTransient(err)}
	}
	return parseOAuthToken(body)
}

// googleReason is a Google API error body: {"error": {"code": 403,
// "message": "...", "status": "PERMISSION_DENIED"}}.
func googleReason(_ int, body []byte) (string, bool) {
	var e struct {
		Error struct {
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) != nil || e.Error.Message == "" {
		return "", false
	}
	return strings.TrimSpace(e.Error.Status + ": " + e.Error.Message), false
}

func (s *gcpService) call(ctx context.Context, op string, body map[string]string) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	defer Zero(raw)
	return s.h.do(ctx, kmsRequest{op: op, build: func(ctx context.Context) (*http.Request, error) {
		token, err := s.token.get(ctx, s.fetchToken)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint+"/v1/"+s.name+":"+op, bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+s.h.secret(token))
		return req, nil
	}, reason: func(status int, body []byte) (string, bool) {
		text, _ := googleReason(status, body)
		// A refused token is dropped and the call tried again with a
		// fresh one: a token revoked or expired early is the one refusal
		// that asking again can cure. One that is refused again is a
		// credential that does not work, and the retries are bounded.
		if status == http.StatusUnauthorized {
			s.token.drop()
			return text, true
		}
		return text, false
	}})
}

func (s *gcpService) encrypt(ctx context.Context, plaintext []byte, b Binding) ([]byte, string, error) {
	raw, err := s.call(ctx, "encrypt", map[string]string{
		"plaintext":                   base64.StdEncoding.EncodeToString(plaintext),
		"additionalAuthenticatedData": base64.StdEncoding.EncodeToString(b.AAD()),
	})
	if err != nil {
		return nil, "", err
	}
	var resp struct {
		Name       string `json:"name"`
		Ciphertext string `json:"ciphertext"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, "", fmt.Errorf("%s: encrypt answered with something that is not JSON", s.h.what)
	}
	out, err := base64.StdEncoding.DecodeString(resp.Ciphertext)
	if err != nil || len(out) == 0 {
		return nil, "", fmt.Errorf("%s: encrypt answered without a ciphertext", s.h.what)
	}
	// The answer names the version that encrypted:
	// ".../cryptoKeys/<key>/cryptoKeyVersions/<n>".
	version := ""
	if i := strings.LastIndex(resp.Name, "/cryptoKeyVersions/"); i >= 0 {
		version = resp.Name[i+len("/cryptoKeyVersions/"):]
	}
	return out, version, nil
}

func (s *gcpService) decrypt(ctx context.Context, ciphertext []byte, _ string, b Binding) ([]byte, error) {
	raw, err := s.call(ctx, "decrypt", map[string]string{
		"ciphertext":                  base64.StdEncoding.EncodeToString(ciphertext),
		"additionalAuthenticatedData": base64.StdEncoding.EncodeToString(b.AAD()),
	})
	if err != nil {
		return nil, err
	}
	defer Zero(raw)
	var resp struct {
		Plaintext string `json:"plaintext"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("%s: decrypt answered with something that is not JSON", s.h.what)
	}
	out, err := base64.StdEncoding.DecodeString(resp.Plaintext)
	if err != nil || len(out) == 0 {
		return nil, fmt.Errorf("%s: decrypt answered without a plaintext", s.h.what)
	}
	return out, nil
}
