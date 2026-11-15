package enc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Roots that are a key management service spoken to directly (issue
// #308): HashiCorp Vault's Transit engine, AWS KMS, Google Cloud KMS and
// Azure Key Vault. Wrap and unwrap are calls to the service; the
// service's key never reaches datax, and what the data directory holds
// is the service's ciphertext of the store key.
//
// They are written against each service's REST API with the standard
// library: the project takes no cloud SDK. What the four share is here —
// how options are written, the calls' timeout and retries, what an error
// says and what it must never say, the binding, the key version and the
// health check — and each service's file holds only its requests, its
// credentials and its errors.
//
// # Options
//
// A root is one spec, and --enc-root separates specs with commas, so
// options ride on the spec the way a URL's query does:
//
//	vault:https://vault.example:8200/transit/datax?token_file=/run/vault/token&ca=/etc/datax/vault-ca.pem
//
// A value that needs a comma, an ampersand or a question mark writes it
// percent-encoded. No option carries a secret: a credential is a file
// the option names, or comes from where the service's own tools look
// (the environment, the instance's identity). So a spec can be logged
// and shown.

const (
	// kmsDefaultTimeout bounds one request to the service. It is shorter
	// than DefaultRootTimeout because a start makes several attempts.
	kmsDefaultTimeout = 10 * time.Second
	// kmsDefaultRetries is how many times a call that failed transiently
	// is tried again.
	kmsDefaultRetries = 3
	kmsMaxRetries     = 10
	kmsDefaultBackoff = 200 * time.Millisecond
	kmsMaxBackoff     = 5 * time.Second
	// kmsMaxResponse bounds what is read of a response: these APIs answer
	// in a few hundred bytes.
	kmsMaxResponse = 1 << 20
	// kmsMaxErrorText bounds how much of the service's own words go into
	// a message.
	kmsMaxErrorText = 512
)

// kmsOptions are a spec's options, each consumed as it is read so that
// one nobody asked for is an error rather than a silent typo.
type kmsOptions struct {
	kind string
	vals url.Values
}

// splitKMSSpec separates a spec's target from its options.
func splitKMSSpec(kind, rest string) (string, *kmsOptions, error) {
	target, query, _ := strings.Cut(rest, "?")
	vals, err := url.ParseQuery(query)
	if err != nil {
		return "", nil, fmt.Errorf("%s root: options are written ?name=value&name=value: %w", kind, err)
	}
	for name, v := range vals {
		if len(v) != 1 {
			return "", nil, fmt.Errorf("%s root: option %q is given %d times", kind, name, len(v))
		}
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return "", nil, fmt.Errorf("%s root: no key named", kind)
	}
	return target, &kmsOptions{kind: kind, vals: vals}, nil
}

func (o *kmsOptions) take(name string) string {
	v := o.vals.Get(name)
	o.vals.Del(name)
	return v
}

// takeOr is take with a fallback to the environment variables the
// service's own tools read, in order.
func (o *kmsOptions) takeOr(name string, env ...string) string {
	if v := o.take(name); v != "" {
		return v
	}
	for _, e := range env {
		if v := os.Getenv(e); v != "" {
			return v
		}
	}
	return ""
}

func (o *kmsOptions) takeBool(name string) (bool, error) {
	v := o.take(name)
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s root: option %s=%q is not true or false", o.kind, name, v)
	}
	return b, nil
}

// done refuses options nothing took.
func (o *kmsOptions) done() error {
	if len(o.vals) == 0 {
		return nil
	}
	names := make([]string, 0, len(o.vals))
	for n := range o.vals {
		names = append(names, n)
	}
	sort.Strings(names)
	return fmt.Errorf("%s root: unknown option %s", o.kind, strings.Join(names, ", "))
}

// kmsError is a failed request: what to tell the operator, and whether
// trying again could help.
type kmsError struct {
	msg       string
	transient bool
}

func (e *kmsError) Error() string { return e.msg }

func kmsTransient(err error) bool {
	var ke *kmsError
	return errors.As(err, &ke) && ke.transient
}

// kmsHTTP is how a KMS root talks: one request under a timeout, tried
// again a bounded number of times when the failure is the kind that
// passes.
type kmsHTTP struct {
	// what names the service and the key in messages: "Vault Transit key
	// transit/datax at https://vault.example:8200".
	what string
	// client reaches the service and its token endpoints; local reaches
	// an instance's metadata service, which is never behind a proxy.
	client, local *http.Client
	timeout       time.Duration
	retries       int
	backoff       time.Duration

	mu sync.Mutex
	// secrets are the credentials this root has sent, most recent last,
	// kept only to be struck from any message it produces: a transport
	// error quotes a URL, and a service is free to echo what it was sent.
	secrets []string
}

// newKMSHTTP reads the options every KMS root takes: timeout, retries
// and ca (a PEM file of the certificate authorities to trust in place of
// the system's, for a service with a private CA).
func newKMSHTTP(o *kmsOptions, what string, caEnv ...string) (*kmsHTTP, error) {
	h := &kmsHTTP{what: what, timeout: kmsDefaultTimeout, retries: kmsDefaultRetries, backoff: kmsDefaultBackoff}
	if v := o.take("timeout"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("%s root: option timeout=%q is not a duration such as 10s", o.kind, v)
		}
		h.timeout = d
	}
	if v := o.take("retries"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > kmsMaxRetries {
			return nil, fmt.Errorf("%s root: option retries=%q is not a number from 0 to %d", o.kind, v, kmsMaxRetries)
		}
		h.retries = n
	}
	var tlsCfg *tls.Config
	if ca := o.takeOr("ca", caEnv...); ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("%s root: reading the CA file: %w", o.kind, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s root: %s holds no PEM certificate", o.kind, ca)
		}
		tlsCfg = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	transport := func(proxy func(*http.Request) (*url.URL, error)) *http.Transport {
		return &http.Transport{
			Proxy:               proxy,
			DialContext:         (&net.Dialer{Timeout: h.timeout}).DialContext,
			TLSClientConfig:     tlsCfg,
			TLSHandshakeTimeout: h.timeout,
			MaxIdleConns:        4,
			IdleConnTimeout:     90 * time.Second,
		}
	}
	// A redirect is not followed: it would carry the credential to a
	// host the operator did not name.
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	h.client = &http.Client{Transport: transport(http.ProxyFromEnvironment), CheckRedirect: noRedirect}
	h.local = &http.Client{Transport: transport(nil), CheckRedirect: noRedirect}
	return h, nil
}

// secret records a credential about to be sent, so no message can hold
// it. Short strings are not recorded: striking them would mangle text
// that merely shares a few characters.
func (h *kmsHTTP) secret(s string) string {
	if len(s) < 8 {
		return s
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, have := range h.secrets {
		if have == s {
			return s
		}
	}
	// A few generations are kept: a message may be about the credential
	// that was replaced a moment ago.
	if len(h.secrets) >= 16 {
		h.secrets = h.secrets[1:]
	}
	h.secrets = append(h.secrets, s)
	return s
}

func (h *kmsHTTP) redact(s string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sec := range h.secrets {
		s = strings.ReplaceAll(s, sec, "[redacted]")
	}
	return s
}

// kmsRequest is one request, built afresh for every attempt: a signature
// carries the time, and a token may have been renewed since the last.
type kmsRequest struct {
	// op is the verb in messages: "encrypt", "fetching a token from the
	// instance metadata service".
	op string
	// build makes the request. An error from it that is transient (a
	// credential source that did not answer) is retried like any other.
	build func(ctx context.Context) (*http.Request, error)
	// reason turns a refusal's body into the service's own words, and
	// may mark as transient a refusal the status alone would not.
	reason func(status int, body []byte) (text string, transient bool)
	// local sends the request without a proxy (a metadata service).
	local bool
}

// once makes one attempt.
func (h *kmsHTTP) once(ctx context.Context, r kmsRequest) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	req, err := r.build(ctx)
	if err != nil {
		return nil, err
	}
	client := h.client
	if r.local {
		client = h.local
	}
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		var ne net.Error
		if ctx.Err() == context.DeadlineExceeded || (errors.As(err, &ne) && ne.Timeout()) {
			return nil, &kmsError{msg: fmt.Sprintf("no answer from %s within %s", req.URL.Host, h.timeout), transient: true}
		}
		// The transport's error names the method and URL; the cause is
		// what is worth saying.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, &kmsError{msg: fmt.Sprintf("could not reach %s: %v", req.URL.Host, err), transient: true}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, kmsMaxResponse+1))
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, &kmsError{msg: fmt.Sprintf("no complete answer within %s", h.timeout), transient: true}
		}
		return nil, &kmsError{msg: fmt.Sprintf("reading the answer from %s: %v", req.URL.Host, err), transient: true}
	}
	if len(body) > kmsMaxResponse {
		return nil, &kmsError{msg: fmt.Sprintf("%s answered with more than %d bytes", req.URL.Host, kmsMaxResponse)}
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, nil
	}
	// Too many requests and the service's own failures pass; a refusal
	// does not, and asking again only fills the service's audit log.
	transient := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
	text := ""
	if r.reason != nil {
		var more bool
		text, more = r.reason(resp.StatusCode, body)
		transient = transient || more
	}
	if text == "" {
		text = strings.TrimSpace(string(body))
	}
	Zero(body)
	if text == "" {
		text = "it gave no reason"
	}
	if len(text) > kmsMaxErrorText {
		text = text[:kmsMaxErrorText] + "…"
	}
	return nil, &kmsError{msg: fmt.Sprintf("the service refused with HTTP %d: %s", resp.StatusCode, text), transient: transient}
}

// do makes the request, trying again after a transient failure with a
// doubling, jittered pause, and returns the response body. Its error
// names the service, the key and the service's reason, and nothing that
// was sent as a credential.
func (h *kmsHTTP) do(ctx context.Context, r kmsRequest) ([]byte, error) {
	var err error
	for attempt := 0; ; attempt++ {
		var body []byte
		if body, err = h.once(ctx, r); err == nil {
			return body, nil
		}
		if !kmsTransient(err) || attempt >= h.retries || ctx.Err() != nil {
			attempts := "1 attempt"
			if attempt > 0 {
				attempts = fmt.Sprintf("%d attempts", attempt+1)
			}
			return nil, errors.New(h.redact(fmt.Sprintf("%s: %s failed after %s: %v", h.what, r.op, attempts, err)))
		}
		pause := h.backoff << attempt
		if pause > kmsMaxBackoff {
			pause = kmsMaxBackoff
		}
		// Jitter, so the nodes of a cluster restarted together do not
		// return to a recovering service in step.
		pause = pause/2 + time.Duration(mrand.Int64N(int64(pause/2)+1))
		select {
		case <-ctx.Done():
		case <-time.After(pause):
		}
	}
}

// kmsToken is a bearer token with the time it stops being good.
type kmsToken struct {
	mu      sync.Mutex
	value   string
	expires time.Time
}

// kmsTokenMargin is how long before its expiry a cached token is
// replaced: longer than any one call can take.
const kmsTokenMargin = 2 * time.Minute

// get returns the cached token, or fetches one. A service that refuses
// a token calls drop, so a revoked one is not used until it expires.
func (t *kmsToken) get(ctx context.Context, fetch func(context.Context) (string, time.Duration, error)) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.value != "" && time.Until(t.expires) > kmsTokenMargin {
		return t.value, nil
	}
	v, life, err := fetch(ctx)
	if err != nil {
		return "", err
	}
	if v == "" {
		return "", &kmsError{msg: "the token endpoint answered without a token"}
	}
	t.value, t.expires = v, time.Now().Add(life)
	return v, nil
}

func (t *kmsToken) drop() {
	t.mu.Lock()
	t.value = ""
	t.mu.Unlock()
}

// kmsService is one service's two calls.
type kmsService interface {
	// encrypt has the service encrypt plaintext under its key, bound to
	// b where the service can bind, and says which version of the key it
	// used ("" when the service does not say).
	encrypt(ctx context.Context, plaintext []byte, b Binding) (ciphertext []byte, version string, err error)
	// decrypt reverses encrypt. version is what encrypt reported.
	decrypt(ctx context.Context, ciphertext []byte, version string, b Binding) ([]byte, error)
}

// kmsRoot is a root whose wrapping is a kmsService.
type kmsRoot struct {
	kind string
	// spec is the root as written, options and all. It holds no secret.
	spec string
	// key is the service's name for the key, without a version.
	key string
	svc kmsService

	mu sync.Mutex
	// version is the key version of the store key's own wrapping: of the
	// last wrap or unwrap that was not a health probe.
	version string
}

const (
	// kmsBlobMagic starts a store key wrapped by a KMS root: the key
	// version, then the service's ciphertext.
	kmsBlobMagic = "DXK1"
	// kmsEnvelopeMagic starts what the service is given to encrypt: a
	// digest of the binding, then the store key.
	kmsEnvelopeMagic = "DXE1"
	kmsMaxVersionLen = 255
)

func (r *kmsRoot) Kind() string   { return r.kind }
func (r *kmsRoot) String() string { return r.kind + ":" + r.spec }

// KeyID is the service's key and, once the root has wrapped or unwrapped
// the store key, the version it is wrapped under: "transit/datax@v3". A
// rewrap after the service has rotated its key is visible as the
// version moving.
func (r *kmsRoot) KeyID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return kmsKeyID(r.key, r.version)
}

func kmsKeyID(key, version string) string {
	if version == "" {
		return key
	}
	return key + "@" + version
}

// OwnsKeyID reports whether id is this root's key at any version.
func (r *kmsRoot) OwnsKeyID(id string) bool {
	if i := strings.LastIndexByte(id, '@'); i >= 0 {
		id = id[:i]
	}
	return id == r.key
}

func (r *kmsRoot) Wrap(ctx context.Context, plaintext []byte) ([]byte, error) {
	return r.WrapContext(ctx, plaintext, Binding{})
}

func (r *kmsRoot) Unwrap(ctx context.Context, wrapped []byte) ([]byte, error) {
	return r.UnwrapContext(ctx, wrapped, Binding{})
}

func (r *kmsRoot) WrapContext(ctx context.Context, plaintext []byte, b Binding) ([]byte, error) {
	blob, version, err := r.wrap(ctx, plaintext, b)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.version = version
	r.mu.Unlock()
	return blob, nil
}

func (r *kmsRoot) UnwrapContext(ctx context.Context, wrapped []byte, b Binding) ([]byte, error) {
	key, version, err := r.unwrap(ctx, wrapped, b)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.version = version
	r.mu.Unlock()
	return key, nil
}

// wrap sends the service an envelope — the binding's digest and the key —
// rather than the key alone. The service's own binding, where it has
// one, is enforced by the service; the envelope is checked here, and so
// holds for a service with no such notion and for one that answers for
// a context it was not asked about.
func (r *kmsRoot) wrap(ctx context.Context, plaintext []byte, b Binding) ([]byte, string, error) {
	digest := sha256.Sum256(b.AAD())
	envelope := make([]byte, 0, len(kmsEnvelopeMagic)+len(digest)+len(plaintext))
	envelope = append(append(append(envelope, kmsEnvelopeMagic...), digest[:]...), plaintext...)
	ciphertext, version, err := r.svc.encrypt(ctx, envelope, b)
	Zero(envelope)
	if err != nil {
		return nil, "", err
	}
	if len(ciphertext) == 0 {
		return nil, "", fmt.Errorf("the %s service answered an encrypt with no ciphertext", r.kind)
	}
	if len(version) > kmsMaxVersionLen || strings.ContainsAny(version, "@\r\n\x00") {
		return nil, "", fmt.Errorf("the %s service named a key version that is not a name", r.kind)
	}
	blob := make([]byte, 0, len(kmsBlobMagic)+1+len(version)+len(ciphertext))
	blob = append(append(blob, kmsBlobMagic...), byte(len(version)))
	blob = append(append(blob, version...), ciphertext...)
	return blob, version, nil
}

func (r *kmsRoot) unwrap(ctx context.Context, wrapped []byte, b Binding) ([]byte, string, error) {
	rest, ok := bytes.CutPrefix(wrapped, []byte(kmsBlobMagic))
	if !ok || len(rest) < 1 || len(rest) < 1+int(rest[0]) {
		return nil, "", fmt.Errorf("not a store key wrapped by a %s root", r.kind)
	}
	version, ciphertext := string(rest[1:1+int(rest[0])]), rest[1+int(rest[0]):]
	envelope, err := r.svc.decrypt(ctx, ciphertext, version, b)
	if err != nil {
		return nil, "", err
	}
	defer Zero(envelope)
	digest := sha256.Sum256(b.AAD())
	body, ok := bytes.CutPrefix(envelope, []byte(kmsEnvelopeMagic))
	if !ok || len(body) < len(digest) {
		return nil, "", fmt.Errorf("the %s service decrypted the blob to something that is not a wrapped store key", r.kind)
	}
	if subtle.ConstantTimeCompare(body[:len(digest)], digest[:]) != 1 {
		return nil, "", fmt.Errorf("the %s service returned a key that was wrapped for a different cluster or node than %s; refusing it", r.kind, b)
	}
	return append([]byte(nil), body[len(digest):]...), version, nil
}

// CheckHealth has the service wrap and unwrap a throwaway value, which
// is the whole of what a restart needs from it — reachable, credential
// accepted, allowed to encrypt and to decrypt under this key — and
// needs no permission beyond those two. The store key is not involved.
// What the wrap reports is also the key's current version.
func (r *kmsRoot) CheckHealth(ctx context.Context) (RootHealth, error) {
	probe := make([]byte, KeyLen)
	if _, err := rand.Read(probe); err != nil {
		return RootHealth{}, err
	}
	// The probe is bound like a real key so that a policy conditioned on
	// the binding permits it; a node that does not yet know who it is
	// passes none.
	b := BindingFrom(ctx)
	blob, version, err := r.wrap(ctx, probe, b)
	if err != nil {
		return RootHealth{}, err
	}
	back, _, err := r.unwrap(ctx, blob, b)
	if err != nil {
		return RootHealth{}, err
	}
	if !bytes.Equal(back, probe) {
		return RootHealth{}, fmt.Errorf("the %s service unwrapped a probe to a different value", r.kind)
	}
	return RootHealth{LatestKeyID: kmsKeyID(r.key, version)}, nil
}

// readSecretFile reads a one-line credential file, as an agent or an
// orchestrator writes one.
func readSecretFile(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return s, nil
}

// endpointURL validates an endpoint override: a scheme and a host, and
// plain HTTP only to this machine, since every request carries a
// credential.
func endpointURL(kind, option, value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return "", fmt.Errorf("%s root: %s %q is not an http(s) URL", kind, option, value)
	}
	if u.Scheme == "http" && !isLoopbackHost(u.Hostname()) {
		return "", fmt.Errorf("%s root: %s %q is plain HTTP to another machine; a credential would cross the network in the clear (use https)", kind, option, value)
	}
	return strings.TrimRight(u.Scheme+"://"+u.Host+u.Path, "/"), nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
