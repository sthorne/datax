package enc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// The azurekv root: Azure Key Vault.
//
//	azurekv:<vault>/<key>[/<version>][?options]
//	azurekv:datax-prod/store-root
//
// The vault is its name (https://<name>.vault.azure.net) or, for another
// cloud, its full host name. With a version the root wraps under that
// version always; without, under the key's current one. Either way an
// unwrap asks for the version that wrapped, which the wrapped key
// records.
//
// The calls are wrapKey and unwrapKey with RSA-OAEP-256, on an RSA key.
// RSA wrapping has no associated data: the service is told nothing of
// the binding and cannot enforce it. The binding is instead inside what
// is wrapped (kmsRoot.wrap) and checked here after the unwrap, which
// holds against a blob moved between nodes but gives a key policy
// nothing to condition on.
//
// The access token comes from the client-secret environment variables
// when they are set (AZURE_TENANT_ID, AZURE_CLIENT_ID,
// AZURE_CLIENT_SECRET) and otherwise from the instance's managed
// identity.
//
// Options:
//
//	client_id   the user-assigned managed identity to use (AZURE_CLIENT_ID
//	            when no client secret is set)
//	endpoint    the vault's URL, in place of the one its name gives
//	login       the token authority (AZURE_AUTHORITY_HOST; default
//	            https://login.microsoftonline.com)
//	imds        the instance metadata endpoint
//	ca, timeout, retries
type azureService struct {
	h        *kmsHTTP
	endpoint string
	key      string
	// pinned is the version the spec names; "" wraps under the current.
	pinned   string
	login    string
	imds     string
	clientID string
	token    kmsToken
}

const (
	azureAPIVersion = "7.4"
	azureWrapAlg    = "RSA-OAEP-256"
	azureResource   = "https://vault.azure.net"
)

func newAzureKVRoot(rest string) (Root, error) {
	const kind = "azurekv"
	target, opts, err := splitKMSSpec(kind, rest)
	if err != nil {
		return nil, err
	}
	p := strings.Split(target, "/")
	if len(p) < 2 || len(p) > 3 || p[0] == "" || p[1] == "" || (len(p) == 3 && p[2] == "") {
		return nil, fmt.Errorf("azurekv root: want azurekv:<vault>/<key> or azurekv:<vault>/<key>/<version>, got %q", target)
	}
	s := &azureService{key: p[1]}
	if len(p) == 3 {
		s.pinned = p[2]
	}
	host := p[0]
	if !strings.Contains(host, ".") {
		host += ".vault.azure.net"
	}
	s.endpoint = "https://" + host
	if v := opts.take("endpoint"); v != "" {
		if s.endpoint, err = endpointURL(kind, "endpoint", v); err != nil {
			return nil, err
		}
	}
	s.login = "https://login.microsoftonline.com"
	if v := opts.takeOr("login", "AZURE_AUTHORITY_HOST"); v != "" {
		if s.login, err = endpointURL(kind, "login", v); err != nil {
			return nil, err
		}
	}
	s.imds = "http://169.254.169.254"
	if v := opts.take("imds"); v != "" {
		if s.imds, err = metadataURL(kind, "imds", v); err != nil {
			return nil, err
		}
	}
	s.clientID = opts.take("client_id")
	if s.h, err = newKMSHTTP(opts, fmt.Sprintf("Azure Key Vault key %s in %s", s.key, p[0])); err != nil {
		return nil, err
	}
	if err := opts.done(); err != nil {
		return nil, err
	}
	return &kmsRoot{kind: kind, spec: rest, key: p[0] + "/" + p[1], svc: s}, nil
}

func (s *azureService) fetchToken(ctx context.Context) (string, time.Duration, error) {
	tenant, client, secret := os.Getenv("AZURE_TENANT_ID"), os.Getenv("AZURE_CLIENT_ID"), os.Getenv("AZURE_CLIENT_SECRET")
	if tenant != "" && client != "" && secret != "" {
		form := url.Values{
			"grant_type": {"client_credentials"}, "client_id": {client},
			"client_secret": {s.h.secret(secret)}, "scope": {azureResource + "/.default"},
		}.Encode()
		raw, err := s.h.once(ctx, kmsRequest{op: "fetching a token with the client secret", reason: oauthReason, build: func(ctx context.Context) (*http.Request, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.login+"/"+url.PathEscape(tenant)+"/oauth2/v2.0/token", strings.NewReader(form))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			return req, nil
		}})
		if err != nil {
			return "", 0, &kmsError{msg: fmt.Sprintf("a token for client %s in tenant %s: %v", client, tenant, err), transient: kmsTransient(err)}
		}
		return parseOAuthToken(raw)
	}
	q := url.Values{"api-version": {"2018-02-01"}, "resource": {azureResource}}
	if id := s.clientID; id != "" {
		q.Set("client_id", id)
	} else if client != "" {
		q.Set("client_id", client)
	}
	raw, err := s.h.once(ctx, kmsRequest{op: "fetching a token for the managed identity", local: true, reason: oauthReason, build: func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.imds+"/metadata/identity/oauth2/token?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Metadata", "true")
		return req, nil
	}})
	if err != nil {
		return "", 0, &kmsError{msg: fmt.Sprintf("no Azure credentials: AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET are not all set, and the managed identity gave no token (%v)", err), transient: kmsTransient(err)}
	}
	return parseOAuthToken(raw)
}

// azureReason is Key Vault's error body: {"error": {"code": "...",
// "message": "..."}}.
func azureReason(_ int, body []byte) (string, bool) {
	var e struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) != nil || e.Error.Code == "" {
		return "", false
	}
	return strings.TrimSpace(e.Error.Code + ": " + e.Error.Message), false
}

func (s *azureService) call(ctx context.Context, op, version string, value []byte) (kid string, out []byte, err error) {
	raw, err := json.Marshal(map[string]string{"alg": azureWrapAlg, "value": base64.RawURLEncoding.EncodeToString(value)})
	if err != nil {
		return "", nil, err
	}
	defer Zero(raw)
	path := "/keys/" + url.PathEscape(s.key)
	if version != "" {
		path += "/" + url.PathEscape(version)
	}
	body, err := s.h.do(ctx, kmsRequest{op: op, build: func(ctx context.Context) (*http.Request, error) {
		token, err := s.token.get(ctx, s.fetchToken)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint+path+"/"+strings.ToLower(op)+"?api-version="+azureAPIVersion, bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+s.h.secret(token))
		return req, nil
	}, reason: func(status int, body []byte) (string, bool) {
		text, _ := azureReason(status, body)
		// As for Google (kms_gcp.go): a refused token is replaced and
		// the call tried again.
		if status == http.StatusUnauthorized {
			s.token.drop()
			return text, true
		}
		return text, false
	}})
	if err != nil {
		return "", nil, err
	}
	defer Zero(body)
	var resp struct {
		Kid   string `json:"kid"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", nil, fmt.Errorf("%s: %s answered with something that is not JSON", s.h.what, op)
	}
	// Key Vault writes base64url without padding; padding is tolerated.
	out, err = base64.RawURLEncoding.DecodeString(strings.TrimRight(resp.Value, "="))
	if err != nil || len(out) == 0 {
		return "", nil, fmt.Errorf("%s: %s answered without a value", s.h.what, op)
	}
	return resp.Kid, out, nil
}

func (s *azureService) encrypt(ctx context.Context, plaintext []byte, _ Binding) ([]byte, string, error) {
	kid, out, err := s.call(ctx, "wrapKey", s.pinned, plaintext)
	if err != nil {
		return nil, "", err
	}
	// The answer's kid is the key's URL with the version that wrapped:
	// https://<vault>/keys/<key>/<version>. Without it a later unwrap
	// could not name the version, so its absence is an error.
	version := s.pinned
	if i := strings.Index(kid, "/keys/"); i >= 0 {
		if p := strings.Split(strings.Trim(kid[i+len("/keys/"):], "/"), "/"); len(p) == 2 {
			version = p[1]
		}
	}
	if version == "" {
		return nil, "", fmt.Errorf("%s: wrapKey did not say which version of the key it used", s.h.what)
	}
	return out, version, nil
}

func (s *azureService) decrypt(ctx context.Context, ciphertext []byte, version string, _ Binding) ([]byte, error) {
	if version == "" {
		return nil, fmt.Errorf("%s: the wrapped key does not record the key version that wrapped it", s.h.what)
	}
	_, out, err := s.call(ctx, "unwrapKey", version, ciphertext)
	return out, err
}
