package enc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/storage/enc/kmstest"
)

// The KMS roots are tested against fakes of each service's API
// (kmstest): nothing here reaches a network, and nothing here shows that
// a root works against the real service.

// isolateEnv clears everything a KMS root reads from the environment,
// and points the home directory somewhere empty, so a test sees only
// the credentials it places.
func isolateEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"VAULT_TOKEN", "VAULT_TOKEN_FILE", "VAULT_NAMESPACE", "VAULT_CACERT",
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
		"AWS_SHARED_CREDENTIALS_FILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
		"AWS_EC2_METADATA_SERVICE_ENDPOINT", "AWS_EC2_METADATA_DISABLED", "AWS_CA_BUNDLE",
		"GOOGLE_APPLICATION_CREDENTIALS", "GCE_METADATA_HOST",
		"AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_AUTHORITY_HOST",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("HOME", t.TempDir())
}

func writeFile(t *testing.T, name string, content []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func mustRoot(t *testing.T, spec string) Root {
	t.Helper()
	r, err := ParseRoot(spec)
	if err != nil {
		t.Fatalf("ParseRoot(%q): %v", spec, err)
	}
	// The pause between attempts is real time; the tests shorten it.
	if h := httpOf(r); h != nil {
		h.backoff = 5 * time.Millisecond
	}
	return r
}

func httpOf(r Root) *kmsHTTP {
	k, ok := r.(*kmsRoot)
	if !ok {
		return nil
	}
	switch s := k.svc.(type) {
	case *vaultService:
		return s.h
	case *awsService:
		return s.h
	case *gcpService:
		return s.h
	case *azureService:
		return s.h
	}
	return nil
}

// kmsFake is one service's fake as the tests drive it.
type kmsFake struct {
	*kmstest.Faults
	// spec is the root's spec against this fake, with opts appended.
	spec func(opts string) string
	// rotate rotates the service's key; versioned says whether the
	// service names the versions.
	rotate    func()
	versioned bool
	// secrets are the credentials the test placed, which no message may
	// hold.
	secrets []string
	handler *http.Handler
}

func withOpts(spec, opts string) string {
	if opts == "" {
		return spec
	}
	if strings.Contains(spec, "?") {
		return spec + "&" + opts
	}
	return spec + "?" + opts
}

func fakeVault(t *testing.T) *kmsFake {
	t.Helper()
	const token = "hvs.CAESIJ-test-token-0123456789"
	v := kmstest.NewVault(token)
	t.Cleanup(v.Close)
	tokenFile := writeFile(t, "vault-token", []byte(token+"\n"))
	return &kmsFake{Faults: &v.Faults, versioned: true, rotate: func() { v.Rotate() }, secrets: []string{token},
		handler: &v.Config.Handler,
		spec: func(opts string) string {
			return withOpts("vault:"+v.URL+"/transit/datax?token_file="+tokenFile, opts)
		}}
}

func fakeAWS(t *testing.T) *kmsFake {
	t.Helper()
	const id, secret, session = "AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "IQoJb3JpZ2luX2VjE-test-session-token"
	a := kmstest.NewAWS("eu-west-1")
	t.Cleanup(a.Close)
	a.AddCredentials(id, secret, session)
	t.Setenv("AWS_ACCESS_KEY_ID", id)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secret)
	t.Setenv("AWS_SESSION_TOKEN", session)
	return &kmsFake{Faults: &a.Faults, rotate: func() { a.Rotate() }, secrets: []string{secret, session},
		handler: &a.Config.Handler,
		spec:    func(opts string) string { return withOpts("awskms:"+a.ARN+"?endpoint="+a.URL, opts) }}
}

func fakeGCP(t *testing.T) *kmsFake {
	t.Helper()
	g := kmstest.NewGCP()
	t.Cleanup(g.Close)
	sa := g.ServiceAccountJSON()
	credFile := writeFile(t, "sa.json", sa)
	return &kmsFake{Faults: &g.Faults, versioned: true, rotate: func() { g.Rotate() },
		handler: &g.Config.Handler,
		spec: func(opts string) string {
			return withOpts("gcpkms:"+g.Name+"?endpoint="+g.URL+"&credentials="+credFile, opts)
		}}
}

func fakeAzure(t *testing.T) *kmsFake {
	t.Helper()
	z := kmstest.NewAzure()
	t.Cleanup(z.Close)
	// The secret is the environment's, and the environment is one: two
	// fakes in a test share it.
	z.ClientSecret = "Hx8Q~fixed-client-secret-for-tests"
	t.Setenv("AZURE_TENANT_ID", z.Tenant)
	t.Setenv("AZURE_CLIENT_ID", z.ClientID)
	t.Setenv("AZURE_CLIENT_SECRET", z.ClientSecret)
	return &kmsFake{Faults: &z.Faults, versioned: true, rotate: func() { z.Rotate() }, secrets: []string{z.ClientSecret},
		handler: &z.Config.Handler,
		spec: func(opts string) string {
			return withOpts("azurekv:datax-prod/"+z.Key+"?endpoint="+z.URL+"&login="+z.URL, opts)
		}}
}

var kmsFakes = []struct {
	name string
	make func(*testing.T) *kmsFake
}{{"vault", fakeVault}, {"awskms", fakeAWS}, {"gcpkms", fakeGCP}, {"azurekv", fakeAzure}}

// forEachKMS runs f against a fresh fake of each service.
func forEachKMS(t *testing.T, f func(t *testing.T, k *kmsFake)) {
	for _, c := range kmsFakes {
		t.Run(c.name, func(t *testing.T) {
			isolateEnv(t)
			f(t, c.make(t))
		})
	}
}

var (
	nodeA = Binding{ClusterID: "6f1c2a0e-8a53-4c60-9d0c-0c8f8f1d3b11", NodeID: 1}
	nodeB = Binding{ClusterID: "6f1c2a0e-8a53-4c60-9d0c-0c8f8f1d3b11", NodeID: 2}
)

// TestRootConformance holds every root — the file and exec roots, and
// each KMS root against its fake — to the same promises: what it wraps
// it unwraps; the wrapped form does not contain the key; a blob that has
// been altered, or that another key made, is an error and never a key;
// and a store key wrapped for one node is refused for another.
func TestRootConformance(t *testing.T) {
	type roots struct{ root, other Root }
	cases := []struct {
		name string
		make func(t *testing.T) roots
	}{
		{"file", func(t *testing.T) roots {
			a, _ := fileRootIn(t, 1)
			b, _ := fileRootIn(t, 2)
			return roots{a, b}
		}},
		{"exec", func(t *testing.T) roots {
			a := helperRoot(t, "")
			// The helper's key is in a file the environment names; a
			// second root over a different key is the file root, which
			// is as foreign to it as another command would be.
			b, _ := fileRootIn(t, 3)
			return roots{a, b}
		}},
	}
	for _, k := range kmsFakes {
		cases = append(cases, struct {
			name string
			make func(t *testing.T) roots
		}{k.name, func(t *testing.T) roots {
			isolateEnv(t)
			a := mustRoot(t, k.make(t).spec("retries=0"))
			// The same service, another deployment of it: another key.
			b := mustRoot(t, k.make(t).spec("retries=0"))
			return roots{a, b}
		}})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := c.make(t)
			if r.root.Kind() != c.name {
				t.Fatalf("the %s case made a %s root", c.name, r.root.Kind())
			}
			key, err := NewStoreKey()
			if err != nil {
				t.Fatal(err)
			}

			// Round trip, and the wrapped form.
			w, err := r.root.Wrap(bg, key)
			if err != nil {
				t.Fatalf("wrap: %v", err)
			}
			for form, enc := range map[string][]byte{
				"raw": key, "hex": []byte(hex.EncodeToString(key)),
				"base64": []byte(base64.StdEncoding.EncodeToString(key)), "base64url": []byte(base64.RawURLEncoding.EncodeToString(key)),
			} {
				if bytes.Contains(w, enc) {
					t.Fatalf("the wrapped key contains the key (%s)", form)
				}
			}
			back, err := r.root.Unwrap(bg, w)
			if err != nil || !bytes.Equal(back, key) {
				t.Fatalf("unwrap: %v (same key: %t)", err, bytes.Equal(back, key))
			}

			// An altered blob: a bit flipped at the start, the middle and
			// the end, and a truncation. Never a key.
			for _, at := range []int{0, len(w) / 2, len(w) - 1} {
				bad := append([]byte(nil), w...)
				bad[at] ^= 0x01
				if got, err := r.root.Unwrap(bg, bad); err == nil {
					t.Fatalf("a blob altered at byte %d unwrapped (to the key: %t)", at, bytes.Equal(got, key))
				}
			}
			if _, err := r.root.Unwrap(bg, w[:len(w)-3]); err == nil {
				t.Fatal("a truncated blob unwrapped")
			}

			// The wrong key.
			if got, err := r.other.Unwrap(bg, w); err == nil {
				t.Fatalf("a root with another key unwrapped the blob (to the key: %t)", bytes.Equal(got, key))
			}

			// Bound to a node: the entry opens for that node, and for a
			// caller that does not say who it is, and is refused for
			// another node before the root is asked.
			dir := t.TempDir()
			if err := RewrapStoreKey(WithBinding(bg, nodeA), vfs.Default, dir, r.root, key); err != nil {
				t.Fatalf("wrapping for a node: %v", err)
			}
			for _, ctx := range []context.Context{bg, WithBinding(bg, nodeA)} {
				got, err := UnwrapStoreKeys(ctx, vfs.Default, dir, []Root{r.root})
				if err != nil || len(got) != 1 || !bytes.Equal(got[0].Key, key) || got[0].Binding != nodeA {
					t.Fatalf("unwrapping the node's own key: %v %+v", err, got)
				}
			}
			if _, err := UnwrapStoreKeys(WithBinding(bg, nodeB), vfs.Default, dir, []Root{r.root}); err == nil || !strings.Contains(err.Error(), "another node's") {
				t.Fatalf("a key wrapped for node 1, opened as node 2: %v", err)
			}
			// The entry's own account of what it was wrapped for cannot
			// be edited: changed, or removed, the entry is refused — by
			// the service where it enforces the binding, by the entry's
			// tag where it does not.
			entries, err := ReadWrappedKeys(vfs.Default, dir)
			if err != nil {
				t.Fatal(err)
			}
			for what, edit := range map[string]func(e *WrappedEntry){
				"rebound to another node": func(e *WrappedEntry) { b := nodeB; e.Binding = &b },
				"with its binding removed": func(e *WrappedEntry) {
					e.Binding = nil
				},
			} {
				e := entries[0]
				edit(&e)
				if err := writeWrappedKeys(vfs.Default, dir, []WrappedEntry{e}); err != nil {
					t.Fatal(err)
				}
				if got, err := UnwrapStoreKeys(bg, vfs.Default, dir, []Root{r.root}); err == nil {
					t.Fatalf("an entry %s unwrapped: %+v", what, got)
				}
			}
			// A root that takes the binding itself refuses the wrong one.
			if cr, ok := r.root.(ContextRoot); ok {
				wa, err := cr.WrapContext(bg, key, nodeA)
				if err != nil {
					t.Fatal(err)
				}
				if got, err := cr.UnwrapContext(bg, wa, nodeA); err != nil || !bytes.Equal(got, key) {
					t.Fatalf("unwrap under the binding it was wrapped with: %v", err)
				}
				for _, wrong := range []Binding{nodeB, {}} {
					if _, err := cr.UnwrapContext(bg, wa, wrong); err == nil {
						t.Fatalf("a key wrapped for %s unwrapped for %s", nodeA, wrong)
					}
				}
			} else if c.name != "file" && c.name != "exec" {
				t.Fatal("a KMS root does not take a binding")
			}
		})
	}
}

// What a KMS root says when the service is down, slow, failing or
// refusing: the service, the key and the service's own reason, after a
// bounded number of attempts — and one attempt for a refusal.
func TestKMSFailures(t *testing.T) {
	forEachKMS(t, func(t *testing.T, k *kmsFake) {
		key, _ := NewStoreKey()
		root := mustRoot(t, k.spec("retries=2&timeout=2s"))
		wrapped, err := root.Wrap(bg, key)
		if err != nil {
			t.Fatal(err)
		}
		named := func(err error) {
			t.Helper()
			if !strings.Contains(err.Error(), "key") || !strings.Contains(err.Error(), httpOf(root).what) {
				t.Fatalf("the message does not name the service and key: %v", err)
			}
		}

		// Down: every attempt fails, and the message says it could not
		// be reached.
		k.Down(true)
		_, err = root.Unwrap(bg, wrapped)
		if err == nil || !strings.Contains(err.Error(), "could not reach") || !strings.Contains(err.Error(), "after 3 attempts") {
			t.Fatalf("service down: %v", err)
		}
		named(err)
		k.Down(false)

		// Failing and then not: retried, and the caller never knows.
		before := k.Calls()
		k.Fail(http.StatusServiceUnavailable, 2)
		if got, err := root.Unwrap(bg, wrapped); err != nil || !bytes.Equal(got, key) {
			t.Fatalf("two 503s and then an answer: %v", err)
		}
		if n := k.Calls() - before; n != 3 {
			t.Fatalf("two 503s and then an answer took %d calls, want 3", n)
		}

		// Failing for good: bounded.
		before = k.Calls()
		k.Fail(http.StatusInternalServerError, -1)
		_, err = root.Unwrap(bg, wrapped)
		if err == nil || !strings.Contains(err.Error(), "HTTP 500") || !strings.Contains(err.Error(), "after 3 attempts") {
			t.Fatalf("a service returning 500: %v", err)
		}
		named(err)
		if n := k.Calls() - before; n != 3 {
			t.Fatalf("a failing service was asked %d times, want 3", n)
		}

		// Too many requests is the service asking to be asked again.
		before = k.Calls()
		k.Fail(http.StatusTooManyRequests, 1)
		if _, err := root.Unwrap(bg, wrapped); err != nil {
			t.Fatalf("a 429 and then an answer: %v", err)
		}
		if n := k.Calls() - before; n != 2 {
			t.Fatalf("a 429 and then an answer took %d calls, want 2", n)
		}

		// A refusal is not retried, and its reason is the service's own.
		before = k.Calls()
		k.Fail(http.StatusForbidden, -1)
		_, err = root.Unwrap(bg, wrapped)
		if err == nil || !strings.Contains(err.Error(), "HTTP 403") || !strings.Contains(err.Error(), "after 1 attempt") {
			t.Fatalf("a refusal: %v", err)
		}
		named(err)
		if n := k.Calls() - before; n != 1 {
			t.Fatalf("a refusal was retried: %d calls", n)
		}
		k.Fail(0, 0)

		// Slow: each attempt is cut off at the timeout.
		slow := mustRoot(t, k.spec("retries=1&timeout=150ms"))
		k.Delay(2 * time.Second)
		start := time.Now()
		_, err = slow.Unwrap(bg, wrapped)
		if err == nil || !strings.Contains(err.Error(), "within 150ms") || !strings.Contains(err.Error(), "after 2 attempts") {
			t.Fatalf("a slow service: %v", err)
		}
		if d := time.Since(start); d > 1500*time.Millisecond {
			t.Fatalf("a slow service held the caller for %s", d)
		}
		k.Delay(0)

		// And a caller that gives up is not kept waiting for retries.
		ctx, cancel := context.WithCancel(bg)
		cancel()
		if _, err := root.Unwrap(ctx, wrapped); err == nil {
			t.Fatal("an unwrap under a cancelled context succeeded")
		}
	})
}

// A service that decrypts under a context it was not asked about — one
// that does not authenticate the context, or has been made not to — is
// found out by what datax put inside the ciphertext.
func TestKMSKeyForTheWrongContextIsRefused(t *testing.T) {
	forEachKMS(t, func(t *testing.T, k *kmsFake) {
		key, _ := NewStoreKey()
		root := mustRoot(t, k.spec("retries=0")).(ContextRoot)
		wrapped, err := root.WrapContext(bg, key, nodeA)
		if err != nil {
			t.Fatal(err)
		}
		k.IgnoreContext(true)
		for _, as := range []Binding{nodeB, {}} {
			got, err := root.UnwrapContext(bg, wrapped, as)
			if err == nil || !strings.Contains(err.Error(), "wrapped for a different cluster or node") {
				t.Fatalf("a key for %s returned to %s: %v (the key: %t)", nodeA, as, err, bytes.Equal(got, key))
			}
		}
		if got, err := root.UnwrapContext(bg, wrapped, nodeA); err != nil || !bytes.Equal(got, key) {
			t.Fatalf("its own context: %v", err)
		}
	})
}

// No credential — token, secret, signature, assertion — appears in any
// message a root produces, even from a service that echoes what it was
// sent.
func TestKMSMessagesHoldNoCredential(t *testing.T) {
	forEachKMS(t, func(t *testing.T, k *kmsFake) {
		key, _ := NewStoreKey()
		root := mustRoot(t, k.spec("retries=1&timeout=300ms"))
		wrapped, err := root.Wrap(bg, key)
		if err != nil {
			t.Fatal(err)
		}
		var msgs []string
		collect := func(err error) {
			if err != nil {
				msgs = append(msgs, err.Error())
			}
		}
		_, err = root.Unwrap(bg, append([]byte(nil), wrapped[:len(wrapped)-1]...))
		collect(err)
		k.Down(true)
		_, err = root.Unwrap(bg, wrapped)
		collect(err)
		k.Down(false)
		k.Fail(http.StatusForbidden, 1)
		_, err = root.Wrap(bg, key)
		collect(err)
		// A service that quotes the request back: headers and body.
		inner := *k.handler
		*k.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.Header.Get("Authorization")+r.Header.Get("X-Vault-Token") == "" {
				inner.ServeHTTP(w, r)
				return
			}
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintf(w, "denied: you sent %v", r.Header)
		})
		_, err = root.Unwrap(bg, wrapped)
		collect(err)
		_, err = root.(HealthChecker).CheckHealth(bg)
		collect(err)
		*k.handler = inner
		if len(msgs) < 5 {
			t.Fatalf("only %d of the failures failed: %q", len(msgs), msgs)
		}
		// What must not appear: the credentials the test placed, and
		// every credential-bearing header the service received.
		secrets := append([]string(nil), k.secrets...)
		for _, c := range k.Credentials() {
			// An Authorization header is a scheme and a credential; of
			// an AWS one, the signature is the part that is secret.
			for _, field := range strings.FieldsFunc(c, func(r rune) bool { return r == ' ' || r == ',' }) {
				field = strings.TrimPrefix(field, "Signature=")
				if len(field) >= 20 && !strings.HasPrefix(field, "Credential=") && !strings.HasPrefix(field, "SignedHeaders=") {
					secrets = append(secrets, field)
				}
			}
		}
		if len(secrets) == 0 {
			t.Fatal("the test knows of no credential to look for")
		}
		for _, m := range msgs {
			for _, s := range secrets {
				if strings.Contains(m, s) {
					t.Fatalf("a message holds a credential (%q…): %s", s[:6], m)
				}
			}
			if strings.Contains(m, hex.EncodeToString(key)) || strings.Contains(m, base64.StdEncoding.EncodeToString(key)) {
				t.Fatalf("a message holds the store key: %s", m)
			}
		}
		// The root as it is shown holds none either.
		for _, s := range k.secrets {
			if strings.Contains(root.String(), s) || strings.Contains(root.KeyID(), s) {
				t.Fatalf("the root's name holds a credential: %s", root)
			}
		}
	})
}

// The service's own key rotation: what is wrapped stays readable, a
// health check reports the new version, and a rewrap moves the store key
// onto it — visibly, where the service names its versions.
func TestKMSRewrapAfterServiceRotation(t *testing.T) {
	forEachKMS(t, func(t *testing.T, k *kmsFake) {
		dir := t.TempDir()
		root := mustRoot(t, k.spec(""))
		key, err := InitWrappedStoreKey(WithBinding(bg, nodeA), vfs.Default, dir, root)
		if err != nil {
			t.Fatal(err)
		}
		entryID := func() string {
			e, err := ReadWrappedKeys(vfs.Default, dir)
			if err != nil || len(e) != 1 {
				t.Fatalf("reading the wrapped key: %v", err)
			}
			return e[0].KeyID
		}
		id1 := entryID()
		if id1 != root.KeyID() || id1 == "" {
			t.Fatalf("entry key id %q, root key id %q", id1, root.KeyID())
		}
		if k.versioned == !strings.Contains(id1, "@") {
			t.Fatalf("key id %q: versioned service %t", id1, k.versioned)
		}
		h, err := root.(HealthChecker).CheckHealth(bg)
		if err != nil || h.LatestKeyID != id1 {
			t.Fatalf("health before rotation: %v, latest %q, want %q", err, h.LatestKeyID, id1)
		}

		k.rotate()

		// A health check sees the new version and does not disturb what
		// the root says of the store key's wrapping.
		h, err = root.(HealthChecker).CheckHealth(bg)
		if err != nil {
			t.Fatal(err)
		}
		if k.versioned == (h.LatestKeyID == id1) {
			t.Fatalf("after the rotation the latest key id is %q (was %q; versioned service %t)", h.LatestKeyID, id1, k.versioned)
		}
		if root.KeyID() != id1 {
			t.Fatalf("a health check changed the root's key id to %q", root.KeyID())
		}
		// A node restarted now — a root that has seen nothing — still
		// opens the key wrapped under the old version.
		fresh := mustRoot(t, k.spec(""))
		got, err := UnwrapStoreKeys(WithBinding(bg, nodeA), vfs.Default, dir, []Root{fresh})
		if err != nil || !bytes.Equal(got[0].Key, key) {
			t.Fatalf("unwrapping under the retired version: %v", err)
		}
		if fresh.KeyID() != id1 {
			t.Fatalf("after unwrapping the old entry the root's key id is %q, want %q", fresh.KeyID(), id1)
		}
		before, _ := ReadWrappedKeys(vfs.Default, dir)

		if err := RewrapStoreKey(WithBinding(bg, nodeA), vfs.Default, dir, fresh, key); err != nil {
			t.Fatalf("rewrap: %v", err)
		}
		id2 := entryID()
		if k.versioned {
			if id2 == id1 || id2 != h.LatestKeyID {
				t.Fatalf("after the rewrap the entry's key id is %q; before %q, latest %q", id2, id1, h.LatestKeyID)
			}
		} else if id2 != id1 {
			t.Fatalf("a service without versions changed key id: %q to %q", id1, id2)
		}
		after, _ := ReadWrappedKeys(vfs.Default, dir)
		if bytes.Equal(before[0].Wrapped, after[0].Wrapped) {
			t.Fatal("the rewrap left the same wrapped bytes")
		}
		got, err = UnwrapStoreKeys(WithBinding(bg, nodeA), vfs.Default, dir, []Root{mustRoot(t, k.spec(""))})
		if err != nil || !bytes.Equal(got[0].Key, key) || got[0].KeyID != id2 {
			t.Fatalf("after the rewrap: %v", err)
		}
	})
}

// The health check: a throwaway value, never the store key, and an
// error that says what a restart would meet.
func TestKMSHealthCheck(t *testing.T) {
	forEachKMS(t, func(t *testing.T, k *kmsFake) {
		root := mustRoot(t, k.spec("retries=0&timeout=1s"))
		key, _ := NewStoreKey()
		if _, err := root.Wrap(bg, key); err != nil {
			t.Fatal(err)
		}
		hc := root.(HealthChecker)
		seen, calls := len(k.Seen()), k.Calls()
		if _, err := hc.CheckHealth(WithBinding(bg, nodeA)); err != nil {
			t.Fatalf("healthy service: %v", err)
		}
		// A check is one wrap and one unwrap, of something that is not
		// the store key.
		if n := k.Calls() - calls; n != 2 {
			t.Fatalf("a health check made %d calls to the key, want 2", n)
		}
		for _, s := range k.Seen()[seen:] {
			for _, form := range []string{base64.StdEncoding.EncodeToString(key), base64.RawURLEncoding.EncodeToString(key)} {
				if strings.Contains(s, strings.TrimRight(form, "=")) {
					t.Fatal("a health check sent the store key to the service")
				}
			}
		}
		k.Down(true)
		if _, err := hc.CheckHealth(bg); err == nil || !strings.Contains(err.Error(), "could not reach") {
			t.Fatalf("health with the service down: %v", err)
		}
		k.Down(false)
		k.Fail(http.StatusForbidden, -1)
		if _, err := hc.CheckHealth(bg); err == nil || !strings.Contains(err.Error(), "HTTP 403") {
			t.Fatalf("health with the key refused: %v", err)
		}
		k.Fail(0, 0)
		if _, err := hc.CheckHealth(bg); err != nil {
			t.Fatalf("health after recovery: %v", err)
		}
	})
}

func TestKMSSpecs(t *testing.T) {
	isolateEnv(t)
	for spec, want := range map[string]string{
		"vault:https://vault.example:8200/datax":                                    "no mount and key",
		"vault:vault.example/transit/datax":                                         "want vault:<address>/<mount>/<key>",
		"vault:http://vault.example:8200/transit/datax":                             "plain HTTP to another machine",
		"vault:https://vault.example:8200/transit/datax?token=abc":                  "unknown option token",
		"vault:https://vault.example:8200/transit/datax?timeout=soon":               "not a duration",
		"vault:https://vault.example:8200/transit/datax?retries=99":                 "not a number from 0 to 10",
		"vault:https://vault.example:8200/transit/datax?derived=perhaps":            "not true or false",
		"vault:https://vault.example:8200/transit/datax?ca=/nonexistent.pem":        "reading the CA file",
		"awskms:1234abcd-12ab-34cd-56ef-1234567890ab":                               "not a KMS key ARN",
		"awskms:arn:aws:s3:::bucket":                                                "not a KMS key ARN",
		"awskms:arn:aws:kms:eu-west-1:111122223333:key/k?endpoint=ftp://x":          "not an http(s) URL",
		"gcpkms:projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1": "not a key's resource name",
		"gcpkms:my-key":             "not a key's resource name",
		"azurekv:vault":             "want azurekv:<vault>/<key>",
		"azurekv:vault/key/v/more":  "want azurekv:<vault>/<key>",
		"azurekv:vault/key?a=1&a=2": "given 2 times",
	} {
		if _, err := ParseRoot(spec); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("ParseRoot(%q): %v, want %q", spec, err, want)
		}
	}
	// Building a root asks the service nothing, and several are told
	// apart by the commas between them — which a spec's options never
	// need.
	roots, err := ParseRoots("vault:https://vault.example:8200/kms/transit/datax?token_file=/run/t&namespace=ops/db," +
		"awskms:arn:aws:kms:eu-west-1:111122223333:key/k,gcpkms:projects/p/locations/l/keyRings/r/cryptoKeys/k," +
		"azurekv:datax-prod/store-root/0123abcd?client_id=1f2e")
	if err != nil {
		t.Fatal(err)
	}
	want := []struct{ kind, id string }{
		{"vault", "kms/transit/datax"}, {"awskms", "arn:aws:kms:eu-west-1:111122223333:key/k"},
		{"gcpkms", "projects/p/locations/l/keyRings/r/cryptoKeys/k"}, {"azurekv", "datax-prod/store-root"},
	}
	if len(roots) != len(want) {
		t.Fatalf("%d roots, want %d", len(roots), len(want))
	}
	for i, w := range want {
		if roots[i].Kind() != w.kind || roots[i].KeyID() != w.id {
			t.Errorf("root %d: %s %q, want %s %q", i, roots[i].Kind(), roots[i].KeyID(), w.kind, w.id)
		}
		if !roots[i].(keyOwner).OwnsKeyID(w.id+"@v7") || roots[i].(keyOwner).OwnsKeyID(w.id+"x@v7") {
			t.Errorf("root %d does not know its own key by id", i)
		}
	}
	// A percent-encoded comma is how a value holds one.
	r, err := ParseRoot("vault:https://vault.example:8200/transit/datax?token_file=%2Frun%2Fa%2Cb")
	if err != nil || r.(*kmsRoot).svc.(*vaultService).tokenFile != "/run/a,b" {
		t.Fatalf("a percent-encoded option: %v", err)
	}
}

// Vault: the token file is read on every call, the namespace is sent,
// and a derived key takes the binding as its context.
func TestVaultRoot(t *testing.T) {
	isolateEnv(t)
	v := kmstest.NewVault("first-token-0123456789")
	defer v.Close()
	tokenFile := writeFile(t, "token", []byte("first-token-0123456789\n"))
	v.RequireNamespace("ops/db")
	root := mustRoot(t, "vault:"+v.URL+"/transit/datax?token_file="+tokenFile+"&namespace=ops/db&retries=0")
	key, _ := NewStoreKey()
	w, err := root.Wrap(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if root.KeyID() != "transit/datax@v1" {
		t.Fatalf("key id %q", root.KeyID())
	}
	// The agent renews: a new token in the file, the old one dead.
	v.SetToken("second-token-9876543210")
	if _, err := root.Unwrap(bg, w); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("with a dead token: %v", err)
	}
	if err := os.WriteFile(tokenFile, []byte("second-token-9876543210"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := root.Unwrap(bg, w); err != nil || !bytes.Equal(got, key) {
		t.Fatalf("after the token file was replaced: %v", err)
	}
	// Without the namespace the request is for another tenant's Vault.
	bare := mustRoot(t, "vault:"+v.URL+"/transit/datax?token_file="+tokenFile+"&retries=0")
	if _, err := bare.Unwrap(bg, w); err == nil {
		t.Fatal("a request without the namespace was served")
	}
	// The environment's token, when no file is named.
	t.Setenv("VAULT_TOKEN", "second-token-9876543210")
	t.Setenv("VAULT_NAMESPACE", "ops/db")
	if got, err := mustRoot(t, "vault:"+v.URL+"/transit/datax?retries=0").Unwrap(bg, w); err != nil || !bytes.Equal(got, key) {
		t.Fatalf("with VAULT_TOKEN: %v", err)
	}
	t.Setenv("VAULT_TOKEN", "")
	if _, err := mustRoot(t, "vault:"+v.URL+"/transit/datax?retries=0").Unwrap(bg, w); err == nil || !strings.Contains(err.Error(), "no Vault token") {
		t.Fatalf("with no token anywhere: %v", err)
	}
	// A key that is not there.
	if _, err := mustRoot(t, "vault:"+v.URL+"/transit/absent?token_file="+tokenFile+"&retries=0").Wrap(bg, key); err == nil || !strings.Contains(err.Error(), "encryption key not found") {
		t.Fatalf("a key Vault does not have: %v", err)
	}

	// A derived key: associated data is refused, the context required.
	v.Derived(true)
	if _, err := root.Wrap(bg, key); err == nil || !strings.Contains(err.Error(), "missing 'context'") {
		t.Fatalf("a derived key without derived=true: %v", err)
	}
	derived := mustRoot(t, "vault:"+v.URL+"/transit/datax?token_file="+tokenFile+"&derived=true&retries=0").(ContextRoot)
	wd, err := derived.WrapContext(bg, key, nodeA)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := derived.UnwrapContext(bg, wd, nodeA); err != nil || !bytes.Equal(got, key) {
		t.Fatalf("derived key round trip: %v", err)
	}
	if _, err := derived.UnwrapContext(bg, wd, nodeB); err == nil {
		t.Fatal("a derived key unwrapped under another node's context")
	}
}

// AWS: each place credentials come from, against a fake that verifies
// the signature of every request for itself.
func TestAWSKMSCredentialSources(t *testing.T) {
	key, _ := NewStoreKey()
	roundTrip := func(t *testing.T, a *kmstest.AWS, opts string) Root {
		t.Helper()
		root := mustRoot(t, withOpts("awskms:"+a.ARN+"?endpoint="+a.URL+"&retries=0", opts))
		for i := 0; i < 3; i++ {
			w, err := root.(ContextRoot).WrapContext(bg, key, nodeA)
			if err != nil {
				t.Fatalf("wrap: %v", err)
			}
			if got, err := root.(ContextRoot).UnwrapContext(bg, w, nodeA); err != nil || !bytes.Equal(got, key) {
				t.Fatalf("unwrap: %v", err)
			}
		}
		return root
	}
	start := func(t *testing.T) *kmstest.AWS {
		isolateEnv(t)
		a := kmstest.NewAWS("eu-west-1")
		t.Cleanup(a.Close)
		// Nothing else answers: a test that falls through to the
		// instance metadata service must reach the fake's or fail.
		t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", a.URL)
		return a
	}
	t.Run("environment", func(t *testing.T) {
		a := start(t)
		a.AddCredentials("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "")
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
		root := roundTrip(t, a, "")
		// A wrong secret is a signature the service does not accept.
		t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEZ")
		if _, err := root.Wrap(bg, key); err == nil || !strings.Contains(err.Error(), "InvalidSignatureException") {
			t.Fatalf("a wrong secret: %v", err)
		}
	})
	t.Run("shared credentials file", func(t *testing.T) {
		a := start(t)
		a.AddCredentials("AKIAFILE00000000", "file-secret-0123456789abcdef", "")
		a.AddCredentials("AKIAOTHER0000000", "other-secret-0123456789abcdef", "tok-for-other-profile")
		file := writeFile(t, "credentials", []byte("# comment\n[default]\naws_access_key_id = AKIAFILE00000000\naws_secret_access_key = file-secret-0123456789abcdef\n\n"+
			"[datax]\naws_access_key_id=AKIAOTHER0000000\naws_secret_access_key=other-secret-0123456789abcdef\naws_session_token=tok-for-other-profile\n"))
		roundTrip(t, a, "credentials_file="+file)
		roundTrip(t, a, "credentials_file="+file+"&profile=datax")
		t.Setenv("AWS_SHARED_CREDENTIALS_FILE", file)
		t.Setenv("AWS_PROFILE", "datax")
		roundTrip(t, a, "")
		if n := a.Issued["imds"]; n != 0 {
			t.Fatalf("the instance role was used %d times though a credentials file was given", n)
		}
	})
	t.Run("web identity", func(t *testing.T) {
		a := start(t)
		a.WebIdentityToken = "eyJhbGciOiJSUzI1NiJ9.projected.token"
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", writeFile(t, "token", []byte(a.WebIdentityToken)))
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::111122223333:role/datax")
		roundTrip(t, a, "sts_endpoint="+a.URL)
		if n := a.Issued["sts"]; n != 1 {
			t.Fatalf("the role was assumed %d times for six calls, want once", n)
		}
		a.WebIdentityToken = "another"
		_, err := mustRoot(t, "awskms:"+a.ARN+"?endpoint="+a.URL+"&sts_endpoint="+a.URL+"&retries=0").Wrap(bg, key)
		if err == nil || !strings.Contains(err.Error(), "InvalidIdentityToken") || strings.Contains(err.Error(), "projected") {
			t.Fatalf("a web identity token STS refuses: %v", err)
		}
	})
	t.Run("container", func(t *testing.T) {
		a := start(t)
		a.ContainerAuth = "container-auth-token-0123456789"
		t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", a.URL+"/container")
		t.Setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", writeFile(t, "auth", []byte(a.ContainerAuth)))
		roundTrip(t, a, "")
		if a.Issued["container"] != 1 || a.Issued["imds"] != 0 {
			t.Fatalf("credentials issued: %v", a.Issued)
		}
	})
	t.Run("instance role", func(t *testing.T) {
		a := start(t)
		roundTrip(t, a, "")
		if n := a.Issued["imds"]; n != 1 {
			t.Fatalf("the instance role's credentials were fetched %d times for six calls, want once", n)
		}
		// The option names the endpoint as well as the variable does.
		t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:1")
		roundTrip(t, a, "imds="+a.URL)
		t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
		if _, err := mustRoot(t, "awskms:"+a.ARN+"?endpoint="+a.URL+"&retries=0").Wrap(bg, key); err == nil || !strings.Contains(err.Error(), "no AWS credentials") {
			t.Fatalf("with no credentials anywhere: %v", err)
		}
	})
	t.Run("another key", func(t *testing.T) {
		a := start(t)
		a.AddCredentials("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "")
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
		other := strings.Replace(a.ARN, "1234abcd", "ffffffff", 1)
		if _, err := mustRoot(t, "awskms:"+other+"?endpoint="+a.URL+"&retries=0").Wrap(bg, key); err == nil || !strings.Contains(err.Error(), "NotFoundException") {
			t.Fatalf("a key the service does not have: %v", err)
		}
		// A throttled request is refused with status 400 and passes.
		root := mustRoot(t, "awskms:"+a.ARN+"?endpoint="+a.URL+"&retries=2")
		a.Fail(http.StatusBadRequest, 2)
		if _, err := root.Wrap(bg, key); err != nil {
			t.Fatalf("two throttled requests and then an answer: %v", err)
		}
	})
}

// Google: a token from the metadata server, and from a service-account
// key file by a signed assertion the fake verifies.
func TestGCPKMSCredentialSources(t *testing.T) {
	isolateEnv(t)
	g := kmstest.NewGCP()
	defer g.Close()
	key, _ := NewStoreKey()
	use := func(root Root) {
		t.Helper()
		for i := 0; i < 3; i++ {
			w, err := root.Wrap(bg, key)
			if err != nil {
				t.Fatalf("wrap: %v", err)
			}
			if got, err := root.Unwrap(bg, w); err != nil || !bytes.Equal(got, key) {
				t.Fatalf("unwrap: %v", err)
			}
		}
	}
	base := "gcpkms:" + g.Name + "?endpoint=" + g.URL + "&retries=1"
	meta := mustRoot(t, base+"&metadata="+g.URL)
	use(meta)
	if n := g.TokensIssued("metadata"); n != 1 {
		t.Fatalf("%d tokens fetched from the metadata server for six calls, want one", n)
	}
	// A token the service stops honouring is replaced, not waited out.
	g.RevokeTokens()
	use(meta)
	if n := g.TokensIssued("metadata"); n != 2 {
		t.Fatalf("after its token was revoked the root fetched %d tokens in all, want 2", n)
	}
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(g.URL, "http://"))
	use(mustRoot(t, base))

	sa := writeFile(t, "sa.json", g.ServiceAccountJSON())
	use(mustRoot(t, base+"&credentials="+sa))
	if n := g.TokensIssued("service-account"); n != 1 {
		t.Fatalf("%d tokens exchanged for six calls, want one", n)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", sa)
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")
	use(mustRoot(t, base))

	// Another account's key signs an assertion the endpoint refuses, and
	// the message names the account and not the key.
	stranger := kmstest.NewGCP()
	defer stranger.Close()
	forged := bytes.Replace(stranger.ServiceAccountJSON(), []byte(stranger.URL), []byte(g.URL), 1)
	_, err := mustRoot(t, base+"&credentials="+writeFile(t, "forged.json", forged)).Wrap(bg, key)
	if err == nil || !strings.Contains(err.Error(), "Invalid JWT Signature") || strings.Contains(err.Error(), "PRIVATE KEY") {
		t.Fatalf("an assertion signed by another key: %v", err)
	}
	if _, err := mustRoot(t, base+"&credentials="+writeFile(t, "user.json", []byte(`{"type":"authorized_user"}`))).Wrap(bg, key); err == nil || !strings.Contains(err.Error(), "not a service-account key file") {
		t.Fatalf("a credential file of another type: %v", err)
	}
}

// Azure: a token from the client secret and from the managed identity,
// and a key pinned to a version.
func TestAzureKVCredentialSourcesAndVersions(t *testing.T) {
	isolateEnv(t)
	z := kmstest.NewAzure()
	defer z.Close()
	key, _ := NewStoreKey()
	base := "azurekv:datax-prod/" + z.Key
	opts := "?endpoint=" + z.URL + "&login=" + z.URL + "&imds=" + z.URL + "&retries=1"

	// The managed identity, system-assigned and then a named one.
	mi := mustRoot(t, base+opts)
	w1, err := mi.Wrap(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	v1 := strings.TrimPrefix(mi.KeyID(), "datax-prod/"+z.Key+"@")
	if len(v1) != 32 {
		t.Fatalf("key id %q carries no version", mi.KeyID())
	}
	if _, err := mustRoot(t, base+opts+"&client_id=1f2e3d4c").Unwrap(bg, w1); err != nil {
		t.Fatal(err)
	}
	if z.TokensIssued("managed-identity:") != 1 || z.TokensIssued("managed-identity:1f2e3d4c") != 1 {
		t.Fatal("the managed identity endpoint was not asked as expected")
	}
	z.RevokeTokens()
	if _, err := mi.Unwrap(bg, w1); err != nil {
		t.Fatalf("after its token was revoked: %v", err)
	}

	// The client secret, when the environment has one.
	t.Setenv("AZURE_TENANT_ID", z.Tenant)
	t.Setenv("AZURE_CLIENT_ID", z.ClientID)
	t.Setenv("AZURE_CLIENT_SECRET", z.ClientSecret)
	sp := mustRoot(t, base+opts)
	if got, err := sp.Unwrap(bg, w1); err != nil || !bytes.Equal(got, key) {
		t.Fatalf("with the client secret: %v", err)
	}
	if z.TokensIssued("client-secret") != 1 {
		t.Fatal("the client-credentials endpoint was not asked")
	}
	t.Setenv("AZURE_CLIENT_SECRET", "not-the-secret-0123456789")
	_, err = mustRoot(t, base+opts).Unwrap(bg, w1)
	if err == nil || !strings.Contains(err.Error(), "AADSTS7000215") || strings.Contains(err.Error(), "not-the-secret") {
		t.Fatalf("a wrong client secret: %v", err)
	}
	t.Setenv("AZURE_CLIENT_SECRET", z.ClientSecret)

	// A rotation: the unpinned root wraps under the new version, a root
	// pinned to the old one stays there, and each unwraps the other's.
	v2 := z.Rotate()
	w2, err := sp.Wrap(bg, key)
	if err != nil || sp.KeyID() != "datax-prod/"+z.Key+"@"+v2 {
		t.Fatalf("after a rotation: %v, key id %q", err, sp.KeyID())
	}
	pinned := mustRoot(t, base+"/"+v1+opts)
	w3, err := pinned.Wrap(bg, key)
	if err != nil || pinned.KeyID() != "datax-prod/"+z.Key+"@"+v1 {
		t.Fatalf("pinned to the first version: %v, key id %q", err, pinned.KeyID())
	}
	for _, w := range [][]byte{w1, w2, w3} {
		for _, r := range []Root{sp, pinned} {
			if got, err := r.Unwrap(bg, w); err != nil || !bytes.Equal(got, key) {
				t.Fatalf("%s: %v", r, err)
			}
		}
	}
	if _, err := mustRoot(t, base+"/00000000000000000000000000000000"+opts).Wrap(bg, key); err == nil || !strings.Contains(err.Error(), "KeyNotFound") {
		t.Fatalf("a version the vault does not have: %v", err)
	}
}
