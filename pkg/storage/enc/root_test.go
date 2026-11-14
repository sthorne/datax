package enc

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// The exec root's command, for the tests, is this test binary: run with
// DATAX_TEST_ROOT_HELPER set it is not a test run but a root — it wraps
// and unwraps under the key file DATAX_TEST_ROOT_KEY names, or misbehaves
// the way DATAX_TEST_ROOT_MODE says. That keeps the tests free of any
// tool the machine may not have.
func TestMain(m *testing.M) {
	if os.Getenv("DATAX_TEST_ROOT_HELPER") == "1" {
		os.Exit(rootHelper())
	}
	os.Exit(m.Run())
}

func rootHelper() int {
	verb := os.Args[len(os.Args)-1]
	mode := os.Getenv("DATAX_TEST_ROOT_MODE")
	in, _ := io.ReadAll(os.Stdin)
	fail := func(format string, a ...any) int {
		fmt.Fprintf(os.Stderr, format+"\n", a...)
		return 3
	}
	if mode == "hang" && verb != "key-id" {
		time.Sleep(time.Hour)
	}
	if mode == "fail" && verb != "key-id" {
		return fail("the hardware said no")
	}
	key, err := LoadKeyFile(os.Getenv("DATAX_TEST_ROOT_KEY"))
	if err != nil {
		return fail("helper: %v", err)
	}
	switch verb {
	case "key-id":
		if mode == "no-id" {
			return fail("no id here")
		}
		fmt.Println("helper-key-1")
	case "wrap":
		switch mode {
		case "identity":
			os.Stdout.Write(in)
			return 0
		case "huge":
			os.Stdout.Write(bytes.Repeat([]byte{'x'}, maxWrappedLen+1))
			return 0
		case "empty":
			return 0
		}
		out, err := Seal("TEST", key, in)
		if err != nil {
			return fail("helper: %v", err)
		}
		os.Stdout.Write(out)
	case "unwrap":
		out, err := Unseal("TEST", key, in)
		if err != nil {
			return fail("helper: %v", err)
		}
		switch mode {
		case "hex":
			fmt.Println(hex.EncodeToString(out))
		case "short":
			os.Stdout.Write(out[:16])
		case "other-key":
			os.Stdout.Write(bytes.Repeat([]byte{7}, KeyLen))
		default:
			os.Stdout.Write(out)
		}
	default:
		return fail("helper: unknown verb %q", verb)
	}
	return 0
}

// helperRoot is an exec root backed by this test binary, wrapping under a
// fresh key.
func helperRoot(t *testing.T, mode string) Root {
	t.Helper()
	keyPath := filepath.Join(t.TempDir(), "helper.key")
	if err := os.WriteFile(keyPath, bytes.Repeat([]byte{0x5a}, KeyLen), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATAX_TEST_ROOT_HELPER", "1")
	t.Setenv("DATAX_TEST_ROOT_KEY", keyPath)
	t.Setenv("DATAX_TEST_ROOT_MODE", mode)
	r, err := newExecRoot(os.Args[0], 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func fileRootIn(t *testing.T, fill byte) (Root, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "root.key")
	if err := os.WriteFile(p, bytes.Repeat([]byte{fill}, KeyLen), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := ParseRoot("file:" + p)
	if err != nil {
		t.Fatal(err)
	}
	return r, p
}

var bg = context.Background()

// Every root keeps the same promises: what it wraps it unwraps, the
// wrapped form does not contain the key, and a blob it did not make is
// an error rather than a key.
func TestRootsWrapAndUnwrap(t *testing.T) {
	file, _ := fileRootIn(t, 1)
	for _, r := range []Root{file, helperRoot(t, ""), helperRoot(t, "hex"), helperRoot(t, "no-id")} {
		key, err := NewStoreKey()
		if err != nil {
			t.Fatal(err)
		}
		w, err := r.Wrap(bg, key)
		if err != nil {
			t.Fatalf("%s: wrap: %v", r, err)
		}
		if bytes.Contains(w, key) {
			t.Errorf("%s: the wrapped key contains the key", r.Kind())
		}
		got, err := r.Unwrap(bg, w)
		if err != nil || !bytes.Equal(got, key) {
			t.Errorf("%s: unwrap = %x, %v; want the key back", r.Kind(), got, err)
		}
		w[len(w)-1] ^= 1
		if got, err := r.Unwrap(bg, w); err == nil {
			t.Errorf("%s: a wrapped key with one bit changed unwrapped to %x", r.Kind(), got)
		}
	}
	if id := helperRoot(t, "").KeyID(); id != "helper-key-1" {
		t.Errorf("exec root's key id = %q, want what the command printed", id)
	}
	if id := helperRoot(t, "no-id").KeyID(); id != "" {
		t.Errorf("exec root's key id = %q for a command that names none, want none", id)
	}
	a, _ := fileRootIn(t, 1)
	b, _ := fileRootIn(t, 2)
	if a.KeyID() == "" || a.KeyID() == b.KeyID() {
		t.Errorf("file roots over different keys have ids %q and %q", a.KeyID(), b.KeyID())
	}
	if strings.Contains(a.KeyID(), hex.EncodeToString(bytes.Repeat([]byte{1}, 4))) {
		t.Errorf("a file root's key id %q shows bytes of its key", a.KeyID())
	}
}

// A root that misbehaves is refused where it would do harm: at the wrap,
// before anything is written or sealed under a key that could not be
// had back.
func TestAMisbehavingRootIsRefused(t *testing.T) {
	key, _ := NewStoreKey()
	for mode, want := range map[string]string{
		"identity":  "unwrapped",
		"huge":      "more than",
		"empty":     "wrote nothing",
		"fail":      "the hardware said no",
		"short":     "want the store key",
		"other-key": "different key",
	} {
		dir := t.TempDir()
		err := RewrapStoreKey(bg, vfs.Default, dir, helperRoot(t, mode), key)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("mode %s: RewrapStoreKey = %v, want an error mentioning %q", mode, err, want)
		}
		if WrappedKeyExists(vfs.Default, dir) {
			t.Errorf("mode %s: a wrapped-key file was written for a root that failed", mode)
		}
	}
	// A command that never answers is cut off at the timeout, with a
	// message that says so.
	t.Setenv("DATAX_TEST_ROOT_HELPER", "1")
	t.Setenv("DATAX_TEST_ROOT_MODE", "hang")
	t.Setenv("DATAX_TEST_ROOT_KEY", "unused")
	slow, err := newExecRoot(os.Args[0], 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := slow.Wrap(bg, key); err == nil || !strings.Contains(err.Error(), "did not finish within") {
		t.Errorf("a hung command: %v, want a timeout", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("a hung command held the caller for %s", d)
	}
	if _, err := ParseRoot("exec:/no/such/command"); err == nil {
		t.Error("an exec root naming no command was accepted")
	}
	for _, spec := range []string{"", "file:", "exec:", "kms:abc", "nocolon"} {
		if _, err := ParseRoot(spec); err == nil {
			t.Errorf("root spec %q was accepted", spec)
		}
	}
}

// The wrapped-key file: a store key goes in wrapped and comes out the
// same, through the root that wrapped it and no other.
func TestWrappedStoreKeyRoundTrip(t *testing.T) {
	dir := t.TempDir()
	root, _ := fileRootIn(t, 1)
	other, _ := fileRootIn(t, 2)
	key, err := InitWrappedStoreKey(bg, vfs.Default, dir, root)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, WrappedKeyName))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, key) || bytes.Contains(raw, []byte(hex.EncodeToString(key))) {
		t.Fatal("the wrapped-key file holds the store key in the clear")
	}
	got, err := UnwrapStoreKeys(bg, vfs.Default, dir, []Root{other, root})
	if err != nil || len(got) != 1 || !bytes.Equal(got[0].Key, key) || got[0].Root != root {
		t.Fatalf("UnwrapStoreKeys = %+v, %v; want the one key, through the root that wrapped it", got, err)
	}
	// A root that did not wrap it is told what did.
	if _, err := UnwrapStoreKeys(bg, vfs.Default, dir, []Root{other}); !errors.Is(err, ErrNoRootForEntry) || !strings.Contains(err.Error(), "a file root (key id "+root.KeyID()+")") {
		t.Errorf("unwrapping with another root: %v", err)
	}
	if _, err := InitWrappedStoreKey(bg, vfs.Default, dir, root); err == nil {
		t.Error("a second init over an existing wrapped key was accepted")
	}
}

// The file is not secret but it is not editable: every field of an entry
// is bound to the key it wraps.
func TestWrappedStoreKeyFileIsAuthenticated(t *testing.T) {
	root := helperRoot(t, "")
	edit := func(t *testing.T, f func(*wrappedFile)) error {
		t.Helper()
		dir := t.TempDir()
		if _, err := InitWrappedStoreKey(bg, vfs.Default, dir, root); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, WrappedKeyName)
		raw, _ := os.ReadFile(p)
		var wf wrappedFile
		if err := json.Unmarshal(raw, &wf); err != nil {
			t.Fatal(err)
		}
		f(&wf)
		raw, _ = json.Marshal(wf)
		if err := os.WriteFile(p, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := UnwrapStoreKeys(bg, vfs.Default, dir, []Root{root})
		return err
	}
	if err := edit(t, func(*wrappedFile) {}); err != nil {
		t.Fatalf("an unedited file: %v", err)
	}
	for name, f := range map[string]func(*wrappedFile){
		"the mac":         func(w *wrappedFile) { w.Keys[0].MAC[0] ^= 1 },
		"the wrapped key": func(w *wrappedFile) { w.Keys[0].Wrapped[len(w.Keys[0].Wrapped)-1] ^= 1 },
		"no mac":          func(w *wrappedFile) { w.Keys[0].MAC = nil },
	} {
		if err := edit(t, f); err == nil {
			t.Errorf("a file with %s changed was accepted", name)
		}
	}
	// The key id is covered by the MAC and nothing else: a root with no
	// id of its own would unwrap the entry whatever it said.
	noID := helperRoot(t, "no-id")
	dir := t.TempDir()
	if _, err := InitWrappedStoreKey(bg, vfs.Default, dir, noID); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, WrappedKeyName)
	raw, _ := os.ReadFile(p)
	var wf wrappedFile
	_ = json.Unmarshal(raw, &wf)
	wf.Keys[0].KeyID = "someone-elses-key"
	raw, _ = json.Marshal(wf)
	_ = os.WriteFile(p, raw, 0o600)
	if _, err := UnwrapStoreKeys(bg, vfs.Default, dir, []Root{noID}); err == nil || !strings.Contains(err.Error(), "altered") {
		t.Errorf("a file with its key id changed: %v, want it refused as altered", err)
	}
	for _, bad := range []string{`{`, `{"version":2,"keys":[{}]}`, `{"version":1,"keys":[]}`} {
		_ = os.WriteFile(p, []byte(bad), 0o600)
		if _, err := UnwrapStoreKeys(bg, vfs.Default, dir, []Root{noID}); err == nil {
			t.Errorf("a wrapped-key file reading %s was accepted", bad)
		}
	}
}

// A rotation interrupted at any step leaves a store that opens: the
// wrapped-key file always holds an entry for the key the registry is
// under.
func TestRotateWrappedStoreKeySurvivesACrashAtEachStep(t *testing.T) {
	root, _ := fileRootIn(t, 1)
	open := func(t *testing.T, dir string) []byte {
		t.Helper()
		got, err := UnwrapStoreKeys(bg, vfs.Default, dir, []Root{root})
		if err != nil {
			t.Fatal(err)
		}
		cands := make([][]byte, len(got))
		for i := range got {
			cands[i] = got[i].Key
		}
		idx, err := MatchStoreKey(vfs.Default, dir, cands)
		if err != nil {
			t.Fatalf("no unwrapped key opens the registry: %v", err)
		}
		if _, err := LoadOrInitRegistry(vfs.Default, dir, cands[idx]); err != nil {
			t.Fatal(err)
		}
		if err := PruneWrappedKeys(vfs.Default, dir, got[idx].Index); err != nil {
			t.Fatal(err)
		}
		return cands[idx]
	}
	reseal := func(dir string) func(o, n []byte) error {
		return func(o, n []byte) error { return RotateStoreKey(vfs.Default, dir, o, n) }
	}
	boom := errors.New("crash")

	// Crash before the registry is resealed: the store opens under the
	// old key, and the new key's entry is retired.
	dir := t.TempDir()
	k1, err := InitWrappedStoreKey(bg, vfs.Default, dir, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrInitRegistry(vfs.Default, dir, k1); err != nil {
		t.Fatal(err)
	}
	if _, err := RotateWrappedStoreKey(bg, vfs.Default, dir, root, k1, func(o, n []byte) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("rotation with a failing reseal: %v", err)
	}
	if entries, _ := ReadWrappedKeys(vfs.Default, dir); len(entries) != 2 {
		t.Fatalf("after an interrupted rotation the file holds %d entries, want both keys'", len(entries))
	}
	if got := open(t, dir); !bytes.Equal(got, k1) {
		t.Fatal("after a crash before the reseal the store did not open under the old key")
	}
	if entries, _ := ReadWrappedKeys(vfs.Default, dir); len(entries) != 1 {
		t.Fatalf("the start left %d entries, want the one in use", len(entries))
	}

	// Crash after the registry is resealed but before the old entry is
	// retired: the store opens under the new key.
	var k2 []byte
	if _, err := RotateWrappedStoreKey(bg, vfs.Default, dir, root, k1, func(o, n []byte) error {
		k2 = append([]byte(nil), n...)
		if err := reseal(dir)(o, n); err != nil {
			return err
		}
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("rotation crashing after the reseal: %v", err)
	}
	if got := open(t, dir); !bytes.Equal(got, k2) || bytes.Equal(k2, k1) {
		t.Fatal("after a crash past the reseal the store did not open under the new key")
	}

	// And one that completes: the new key alone, in the file and over
	// the registry.
	k3, err := RotateWrappedStoreKey(bg, vfs.Default, dir, root, k2, reseal(dir))
	if err != nil {
		t.Fatal(err)
	}
	if entries, _ := ReadWrappedKeys(vfs.Default, dir); len(entries) != 1 {
		t.Fatalf("a completed rotation left %d entries", len(entries))
	}
	if got := open(t, dir); !bytes.Equal(got, k3) {
		t.Fatal("a completed rotation did not leave the store under the new key")
	}
	if _, err := MatchStoreKey(vfs.Default, dir, [][]byte{k1, k2}); err == nil {
		t.Fatal("a retired store key still opens the registry")
	}
}

// Moving a store from one root to another, and back to a key file,
// changes the wrapping and nothing under it.
func TestRewrapMovesAStoreBetweenRoots(t *testing.T) {
	a, _ := fileRootIn(t, 1)
	b := helperRoot(t, "")
	dir := t.TempDir()
	key, err := InitWrappedStoreKey(bg, vfs.Default, dir, a)
	if err != nil {
		t.Fatal(err)
	}
	if err := RewrapStoreKey(bg, vfs.Default, dir, b, key); err != nil {
		t.Fatal(err)
	}
	if _, err := UnwrapStoreKeys(bg, vfs.Default, dir, []Root{a}); !errors.Is(err, ErrNoRootForEntry) {
		t.Errorf("the root a store moved from still unwraps it: %v", err)
	}
	got, err := UnwrapStoreKeys(bg, vfs.Default, dir, []Root{a, b})
	if err != nil || len(got) != 1 || !bytes.Equal(got[0].Key, key) || got[0].Root != b {
		t.Fatalf("after the move: %+v, %v", got, err)
	}
	if err := RemoveWrappedKeys(vfs.Default, dir); err != nil || WrappedKeyExists(vfs.Default, dir) {
		t.Fatalf("removing the wrapped key: %v", err)
	}
	if err := RemoveWrappedKeys(vfs.Default, dir); err != nil {
		t.Errorf("removing it twice: %v", err)
	}
	if rs, err := ParseRoots(" " + a.String() + " , , " + a.String()); err != nil || len(rs) != 2 {
		t.Errorf("ParseRoots of a list: %v, %v", rs, err)
	}
	if _, err := ParseRoots(" , "); err == nil {
		t.Error("an empty root list was accepted")
	}
}
