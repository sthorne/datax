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
	"path/filepath"
	"strconv"
	"strings"
)

// The vault root: HashiCorp Vault's Transit secrets engine.
//
//	vault:<address>/<mount>/<key>[?options]
//	vault:https://vault.example:8200/transit/datax?token_file=/run/vault/token
//
// The address is the scheme and host; the last path element is the key
// and what lies between is the mount (which may itself contain a slash).
//
// Options, each with the environment variable read when it is absent:
//
//	token_file   a file holding the token (VAULT_TOKEN_FILE). Read on
//	             every call, so a token an agent renews is picked up.
//	             Without it: VAULT_TOKEN, then ~/.vault-token.
//	namespace    the Vault Enterprise namespace (VAULT_NAMESPACE)
//	ca           PEM file of the CA that signed Vault's certificate
//	             (VAULT_CACERT)
//	derived      true for a key created with derived=true: the binding
//	             is then sent as the key-derivation context. Otherwise
//	             it is sent as associated data, which Vault authenticates
//	             for its AEAD key types.
//	timeout, retries
type vaultService struct {
	h         *kmsHTTP
	base      string // <address>/v1/<mount>
	key       string
	tokenFile string
	namespace string
	derived   bool
}

func newVaultRoot(rest string) (Root, error) {
	const kind = "vault"
	target, opts, err := splitKMSSpec(kind, rest)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(target)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("vault root: want vault:<address>/<mount>/<key>, for example vault:https://vault.example:8200/transit/datax")
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 || parts[0] == "" || parts[len(parts)-1] == "" {
		return nil, fmt.Errorf("vault root: %q names no mount and key after the address (want vault:<address>/<mount>/<key>)", target)
	}
	key, mount := parts[len(parts)-1], strings.Join(parts[:len(parts)-1], "/")
	addr, err := endpointURL(kind, "address", u.Scheme+"://"+u.Host)
	if err != nil {
		return nil, err
	}
	s := &vaultService{base: addr + "/v1/" + mount, key: key}
	s.tokenFile = opts.takeOr("token_file", "VAULT_TOKEN_FILE")
	s.namespace = opts.takeOr("namespace", "VAULT_NAMESPACE")
	if s.derived, err = opts.takeBool("derived"); err != nil {
		return nil, err
	}
	if s.h, err = newKMSHTTP(opts, fmt.Sprintf("Vault Transit key %s/%s at %s", mount, key, addr), "VAULT_CACERT"); err != nil {
		return nil, err
	}
	if err := opts.done(); err != nil {
		return nil, err
	}
	return &kmsRoot{kind: kind, spec: rest, key: mount + "/" + key, svc: s}, nil
}

// token finds the token for one call. Nothing is cached: the file is the
// operator's (or an agent's) to replace at any time.
func (s *vaultService) token() (string, error) {
	if s.tokenFile != "" {
		t, err := readSecretFile(s.tokenFile)
		if err != nil {
			return "", &kmsError{msg: fmt.Sprintf("reading the Vault token file: %v", err)}
		}
		return t, nil
	}
	if t := strings.TrimSpace(os.Getenv("VAULT_TOKEN")); t != "" {
		return t, nil
	}
	if home, err := os.UserHomeDir(); err == nil {
		if t, err := readSecretFile(filepath.Join(home, ".vault-token")); err == nil {
			return t, nil
		}
	}
	return "", &kmsError{msg: "no Vault token: name a token file with ?token_file=<path> (or VAULT_TOKEN_FILE), or set VAULT_TOKEN"}
}

// vaultReason is Vault's error body, {"errors": ["..."]}, as one line.
func vaultReason(_ int, body []byte) (string, bool) {
	var e struct {
		Errors []string `json:"errors"`
	}
	if json.Unmarshal(body, &e) != nil {
		return "", false
	}
	return strings.Join(e.Errors, "; "), false
}

// bind adds the binding to a request the way the key takes it.
func (s *vaultService) bind(body map[string]any, b Binding) {
	aad := base64.StdEncoding.EncodeToString(b.AAD())
	if s.derived {
		body["context"] = aad
	} else {
		body["associated_data"] = aad
	}
}

func (s *vaultService) call(ctx context.Context, op string, body map[string]any) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	defer Zero(raw)
	return s.h.do(ctx, kmsRequest{op: op, reason: vaultReason, build: func(ctx context.Context) (*http.Request, error) {
		token, err := s.token()
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+"/"+op+"/"+url.PathEscape(s.key), bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Vault-Token", s.h.secret(token))
		req.Header.Set("X-Vault-Request", "true")
		if s.namespace != "" {
			req.Header.Set("X-Vault-Namespace", s.namespace)
		}
		return req, nil
	}})
}

func (s *vaultService) encrypt(ctx context.Context, plaintext []byte, b Binding) ([]byte, string, error) {
	body := map[string]any{"plaintext": base64.StdEncoding.EncodeToString(plaintext)}
	s.bind(body, b)
	raw, err := s.call(ctx, "encrypt", body)
	if err != nil {
		return nil, "", err
	}
	var resp struct {
		Data struct {
			Ciphertext string `json:"ciphertext"`
			KeyVersion int    `json:"key_version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil || resp.Data.Ciphertext == "" {
		return nil, "", fmt.Errorf("%s: encrypt answered without a ciphertext", s.h.what)
	}
	// Vault's ciphertext is "vault:v<N>:<base64>"; the version is taken
	// from there when the answer does not carry it separately.
	version := ""
	if resp.Data.KeyVersion > 0 {
		version = "v" + strconv.Itoa(resp.Data.KeyVersion)
	} else if p := strings.SplitN(resp.Data.Ciphertext, ":", 3); len(p) == 3 {
		version = p[1]
	}
	return []byte(resp.Data.Ciphertext), version, nil
}

func (s *vaultService) decrypt(ctx context.Context, ciphertext []byte, _ string, b Binding) ([]byte, error) {
	body := map[string]any{"ciphertext": string(ciphertext)}
	s.bind(body, b)
	raw, err := s.call(ctx, "decrypt", body)
	if err != nil {
		return nil, err
	}
	defer Zero(raw)
	var resp struct {
		Data struct {
			Plaintext string `json:"plaintext"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("%s: decrypt answered with something that is not JSON", s.h.what)
	}
	out, err := base64.StdEncoding.DecodeString(resp.Data.Plaintext)
	if err != nil || len(out) == 0 {
		return nil, fmt.Errorf("%s: decrypt answered without a plaintext", s.h.what)
	}
	return out, nil
}
