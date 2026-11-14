package enc

import (
	"bytes"
	"context"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// A root is what a store's key is kept under (issue #292, #305).
//
// Until now the store key was a file the operator wrote and the node
// read: the key that opens everything sat beside what it opens, or on a
// path the operator had to protect by other means. With a root, the store
// key is made on the node, and what is kept in the data directory is
// that key WRAPPED — encrypted by something that is not in the data
// directory. The root is that something, and all it has to be able to do
// is wrap a small blob and unwrap it again:
//
//	root            wraps    store key    seals    registry of data keys    which encrypt    files
//	(this file)              (per node)            (registry.go)                             (fs.go, aead.go)
//
// Nothing below the store key changes. What changes is where the top of
// the chain lives, and that a store key no longer has to exist in the
// clear anywhere but the node's memory: not in a file, and not in the
// request that rotates it.
//
// Two roots are here. "file" keeps a wrapping key in a file, which moves
// the problem one step and is for tests and for migrating; "exec" hands
// the wrapping to a command the operator names, which is the bridge to
// anything with a command line — a TPM, a cloud KMS's CLI, an HSM
// vendor's tool. The roots that make this more than a bridge (a root key
// split among operators, one handed between peers, a KMS spoken to
// directly) are the issues that follow this one.
type Root interface {
	// Kind names the sort of root: "file", "exec".
	Kind() string
	// KeyID identifies the key the root wraps under, for the audit record
	// and to tell which of several roots wrapped an entry. It is not
	// secret and may be empty when the root cannot say.
	KeyID() string
	// Wrap encrypts plaintext (a store key) under the root.
	Wrap(ctx context.Context, plaintext []byte) ([]byte, error)
	// Unwrap reverses Wrap. A blob the root did not produce is an error,
	// never a wrong key.
	Unwrap(ctx context.Context, wrapped []byte) ([]byte, error)
	// String is the root as the operator wrote it, for messages. It holds
	// no secret: a path, a command line.
	String() string
}

// WrappedKeyName is the file in the store directory holding the store key
// wrapped by the root. Its presence says the store's key is under a root;
// it is not secret.
const WrappedKeyName = "ENCRYPTION-STOREKEY"

const (
	wrappedFileVersion = 1
	// fileRootMagic prefixes a store key wrapped by a file root.
	fileRootMagic = "DXW1"
	// maxWrappedLen bounds a wrapped store key: a KMS ciphertext of a
	// 32-byte key is a few hundred bytes; nothing legitimate is near this.
	maxWrappedLen = 64 << 10
)

// DefaultRootTimeout bounds one call to a root that leaves the process.
const DefaultRootTimeout = 30 * time.Second

// WrappedEntry is one wrapped copy of a store key. The file holds one,
// or two while a rotation is between writing the new key's entry and
// retiring the old one's.
type WrappedEntry struct {
	// Root and KeyID say what wrapped this entry, so a node configured
	// with several roots knows which to ask.
	Root  string `json:"root"`
	KeyID string `json:"key_id,omitempty"`
	// Wrapped is the root's output, opaque here.
	Wrapped []byte `json:"wrapped"`
	// MAC authenticates the three fields above under a key derived from
	// the store key they wrap, so the file cannot be edited — a key id
	// changed, one entry's blob put under another's label — without the
	// store key. The root authenticates Wrapped itself; this covers the
	// rest, and the case of a root that authenticates nothing.
	MAC []byte `json:"mac"`
}

type wrappedFile struct {
	Version int            `json:"version"`
	Keys    []WrappedEntry `json:"keys"` // newest first
}

// ErrNoRootForEntry is returned when none of the roots given can unwrap
// anything in the store's wrapped-key file.
var ErrNoRootForEntry = errors.New("no root given matches the store's wrapped key")

// WrappedKeyExists reports whether dir holds a wrapped store key.
func WrappedKeyExists(base vfs.FS, dir string) bool {
	if st, err := base.Stat(base.PathJoin(dir, WrappedKeyName)); err == nil && !st.IsDir() {
		return true
	}
	return false
}

// ReadWrappedKeys returns the entries of dir's wrapped-key file, newest
// first.
func ReadWrappedKeys(base vfs.FS, dir string) ([]WrappedEntry, error) {
	raw, err := readAll(base, base.PathJoin(dir, WrappedKeyName))
	if err != nil {
		return nil, err
	}
	var f wrappedFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("corrupt %s: %w", WrappedKeyName, err)
	}
	if f.Version != wrappedFileVersion {
		return nil, fmt.Errorf("%s is version %d; this binary reads version %d", WrappedKeyName, f.Version, wrappedFileVersion)
	}
	if len(f.Keys) == 0 {
		return nil, fmt.Errorf("corrupt %s: it holds no key", WrappedKeyName)
	}
	return f.Keys, nil
}

// writeWrappedKeys replaces dir's wrapped-key file atomically and
// durably: a crash leaves the old file or the new one.
func writeWrappedKeys(base vfs.FS, dir string, entries []WrappedEntry) error {
	raw, err := json.Marshal(wrappedFile{Version: wrappedFileVersion, Keys: entries})
	if err != nil {
		return err
	}
	return writeFileAtomic(base, dir, WrappedKeyName, raw)
}

// writeFileAtomic writes name in dir through a temporary file, a rename
// and a directory sync — the registry's own discipline (writeRegistry),
// for the same reason: a rename is durable only once the directory is.
func writeFileAtomic(base vfs.FS, dir, name string, content []byte) error {
	path := base.PathJoin(dir, name)
	tmp := path + ".tmp"
	f, err := base.Create(tmp, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	if _, err := f.Write(content); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := base.Rename(tmp, path); err != nil {
		return err
	}
	if dir == "" {
		return nil
	}
	d, err := base.OpenDir(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// entryMAC is the tag over an entry's fields, keyed from the store key.
// The MAC key is derived, not the store key itself: the store key seals
// the registry with AES-GCM, and one key is not used for two things.
func entryMAC(storeKey []byte, e *WrappedEntry) ([]byte, error) {
	macKey, err := hkdf.Key(sha256.New, storeKey, nil, "datax wrapped store key mac v1", KeyLen)
	if err != nil {
		return nil, err
	}
	m := hmac.New(sha256.New, macKey)
	for _, field := range [][]byte{[]byte(e.Root), []byte(e.KeyID), e.Wrapped} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(field)))
		m.Write(n[:])
		m.Write(field)
	}
	return m.Sum(nil), nil
}

// WrapStoreKey wraps storeKey under root and returns the entry to store.
func WrapStoreKey(ctx context.Context, root Root, storeKey []byte) (WrappedEntry, error) {
	if len(storeKey) != KeyLen {
		return WrappedEntry{}, fmt.Errorf("store key must be %d bytes, got %d", KeyLen, len(storeKey))
	}
	wrapped, err := root.Wrap(ctx, storeKey)
	if err != nil {
		return WrappedEntry{}, fmt.Errorf("the %s root could not wrap the store key: %w", root.Kind(), err)
	}
	if len(wrapped) == 0 || len(wrapped) > maxWrappedLen {
		return WrappedEntry{}, fmt.Errorf("the %s root returned %d bytes for a wrapped store key (want 1 to %d)", root.Kind(), len(wrapped), maxWrappedLen)
	}
	// A root that hands back what it was given has wrapped nothing, and
	// the "wrapped" key would then sit in the data directory in the
	// clear. Refuse it here, where it would be written.
	if bytes.Contains(wrapped, storeKey) {
		return WrappedEntry{}, fmt.Errorf("the %s root returned the store key unwrapped; refusing to write it to the data directory", root.Kind())
	}
	// What the root wrapped must be what it gives back, before anything
	// is sealed under this key: a root that cannot unwrap its own output
	// is found now, not at the next start.
	back, err := root.Unwrap(ctx, wrapped)
	if err != nil {
		return WrappedEntry{}, fmt.Errorf("the %s root could not unwrap what it had just wrapped: %w", root.Kind(), err)
	}
	if !bytes.Equal(back, storeKey) {
		return WrappedEntry{}, fmt.Errorf("the %s root unwrapped its own output to a different key", root.Kind())
	}
	e := WrappedEntry{Root: root.Kind(), KeyID: root.KeyID(), Wrapped: wrapped}
	if e.MAC, err = entryMAC(storeKey, &e); err != nil {
		return WrappedEntry{}, err
	}
	return e, nil
}

// rootFor picks, from roots, the one that wrapped e: same kind, and the
// same key id when both sides state one.
func rootFor(roots []Root, e *WrappedEntry) Root {
	for _, r := range roots {
		if r.Kind() != e.Root {
			continue
		}
		if id := r.KeyID(); id != "" && e.KeyID != "" && id != e.KeyID {
			continue
		}
		return r
	}
	return nil
}

// unwrapEntry returns the store key e holds, through whichever of roots
// wrapped it.
func unwrapEntry(ctx context.Context, roots []Root, e *WrappedEntry) ([]byte, Root, error) {
	r := rootFor(roots, e)
	if r == nil {
		return nil, nil, ErrNoRootForEntry
	}
	if len(e.Wrapped) == 0 || len(e.Wrapped) > maxWrappedLen {
		return nil, nil, fmt.Errorf("corrupt %s: a wrapped key of %d bytes", WrappedKeyName, len(e.Wrapped))
	}
	key, err := r.Unwrap(ctx, e.Wrapped)
	if err != nil {
		return nil, nil, fmt.Errorf("the %s root could not unwrap the store key: %w", r.Kind(), err)
	}
	if len(key) != KeyLen {
		return nil, nil, fmt.Errorf("the %s root unwrapped the store key to %d bytes, want %d", r.Kind(), len(key), KeyLen)
	}
	want, err := entryMAC(key, e)
	if err != nil {
		return nil, nil, err
	}
	if !hmac.Equal(want, e.MAC) {
		return nil, nil, fmt.Errorf("%s has been altered: an entry's fields do not match the key it wraps", WrappedKeyName)
	}
	return key, r, nil
}

// Unwrapped is one store key recovered from the wrapped-key file.
type Unwrapped struct {
	Key  []byte
	Root Root
	// Index is the entry's position in the file (0 is the newest).
	Index int
}

// UnwrapStoreKeys recovers every store key in dir's wrapped-key file that
// one of roots can unwrap, newest first. The caller decides which opens
// the store (MatchStoreKey): there is more than one only while a
// rotation is unfinished.
//
// An entry no root here wrapped is skipped — it is another root's, staged
// or retired. An entry a root claims and then fails on is an error: that
// is a root that is down, or a file that has been altered, and neither
// should be passed over in favour of an older key.
func UnwrapStoreKeys(ctx context.Context, base vfs.FS, dir string, roots []Root) ([]Unwrapped, error) {
	if len(roots) == 0 {
		return nil, errors.New("no root given")
	}
	entries, err := ReadWrappedKeys(base, dir)
	if err != nil {
		return nil, err
	}
	var out []Unwrapped
	for i := range entries {
		key, r, err := unwrapEntry(ctx, roots, &entries[i])
		if errors.Is(err, ErrNoRootForEntry) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, Unwrapped{Key: key, Root: r, Index: i})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: it was wrapped by %s", ErrNoRootForEntry, describeEntries(entries))
	}
	return out, nil
}

func describeEntries(entries []WrappedEntry) string {
	parts := make([]string, 0, len(entries))
	for _, e := range entries {
		s := "a " + e.Root + " root"
		if e.KeyID != "" {
			s += " (key id " + e.KeyID + ")"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, " and ")
}

// NewStoreKey returns a fresh random store key.
func NewStoreKey() ([]byte, error) {
	k := make([]byte, KeyLen)
	if _, err := rand.Read(k); err != nil {
		return nil, err
	}
	return k, nil
}

// InitWrappedStoreKey makes a store key for a new store, wraps it under
// root and writes the wrapped-key file. The key is returned for the open
// that follows and is written nowhere.
func InitWrappedStoreKey(ctx context.Context, base vfs.FS, dir string, root Root) ([]byte, error) {
	if WrappedKeyExists(base, dir) {
		return nil, fmt.Errorf("%s already exists in %s", WrappedKeyName, dir)
	}
	key, err := NewStoreKey()
	if err != nil {
		return nil, err
	}
	if err := RewrapStoreKey(ctx, base, dir, root, key); err != nil {
		return nil, err
	}
	return key, nil
}

// RewrapStoreKey writes dir's wrapped-key file as storeKey wrapped under
// root alone. It is how a store adopts a root (the key came from a key
// file), how it follows a root whose own key has rotated, and how it
// moves from one root to another: in each case the store key and
// everything under it stay as they are, and one small file is replaced
// in one rename.
func RewrapStoreKey(ctx context.Context, base vfs.FS, dir string, root Root, storeKey []byte) error {
	e, err := WrapStoreKey(ctx, root, storeKey)
	if err != nil {
		return err
	}
	return writeWrappedKeys(base, dir, []WrappedEntry{e})
}

// RotateWrappedStoreKey replaces a rooted store's key: a new key is made
// here, wrapped, and the registry resealed under it by reseal. No key
// leaves the process. Returned is the new key, for the caller to seal
// with from now on.
//
// The order is what makes a crash anywhere survivable:
//
//  1. the file is rewritten to hold the new key's entry AND the old
//     one's. From here a start can unwrap both.
//  2. reseal moves the registry (and a split store's second registry)
//     from the old key to the new. Before it a start finds the registry
//     under the old key, after it under the new; either is in the file.
//  3. the file is rewritten to hold the new entry alone.
//
// A failure in step 2 leaves the file with both entries and the registry
// under whichever key it reached; the next start, or a rerun, sorts it
// out. current is the store's present key.
func RotateWrappedStoreKey(ctx context.Context, base vfs.FS, dir string, root Root, current []byte, reseal func(oldKey, newKey []byte) error) ([]byte, error) {
	old, err := ReadWrappedKeys(base, dir)
	if err != nil {
		return nil, err
	}
	next, err := NewStoreKey()
	if err != nil {
		return nil, err
	}
	e, err := WrapStoreKey(ctx, root, next)
	if err != nil {
		return nil, err
	}
	if err := writeWrappedKeys(base, dir, append([]WrappedEntry{e}, old...)); err != nil {
		return nil, err
	}
	if err := reseal(current, next); err != nil {
		return nil, err
	}
	if err := writeWrappedKeys(base, dir, []WrappedEntry{e}); err != nil {
		return nil, err
	}
	return next, nil
}

// PruneWrappedKeys rewrites dir's wrapped-key file to the one entry at
// index keep: what a start does once it knows which key the store is
// under, to retire the other half of an interrupted rotation.
func PruneWrappedKeys(base vfs.FS, dir string, keep int) error {
	entries, err := ReadWrappedKeys(base, dir)
	if err != nil {
		return err
	}
	if keep < 0 || keep >= len(entries) {
		return fmt.Errorf("%s has %d entries; cannot keep entry %d", WrappedKeyName, len(entries), keep)
	}
	if len(entries) == 1 {
		return nil
	}
	return writeWrappedKeys(base, dir, []WrappedEntry{entries[keep]})
}

// RemoveWrappedKeys deletes dir's wrapped-key file: the last step of
// taking a store back from a root to a key file.
func RemoveWrappedKeys(base vfs.FS, dir string) error {
	if err := base.Remove(base.PathJoin(dir, WrappedKeyName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if dir == "" {
		return nil
	}
	d, err := base.OpenDir(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// ---- Parsing ----

// ParseRoot builds a root from its spec: "file:<path>",
// "exec:<command> [args...]", or "shares" (shares.go), which is made
// sealed.
func ParseRoot(spec string) (Root, error) {
	if strings.TrimSpace(spec) == "shares" {
		// Nothing to configure: the shares say what they are shares of.
		return NewShareRoot(), nil
	}
	kind, rest, ok := strings.Cut(strings.TrimSpace(spec), ":")
	rest = strings.TrimSpace(rest)
	if !ok || rest == "" {
		return nil, fmt.Errorf("root %q: want file:<path>, exec:<command> or shares", spec)
	}
	switch kind {
	case "file":
		return newFileRoot(rest)
	case "exec":
		return newExecRoot(rest, DefaultRootTimeout)
	}
	return nil, fmt.Errorf("root %q: unknown kind %q (want file, exec or shares)", spec, kind)
}

// ParseRoots builds the roots of an --enc-root value: one spec, or
// several separated by commas. Several are candidates — the one that
// wrapped the store's key is used, which is what lets an operator stage
// the root a store is moving to beside the one it is under. A spec
// cannot itself contain a comma.
func ParseRoots(value string) ([]Root, error) {
	var out []Root
	for _, spec := range strings.Split(value, ",") {
		if strings.TrimSpace(spec) == "" {
			continue
		}
		r, err := ParseRoot(spec)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if len(out) == 0 {
		return nil, errors.New("no root given")
	}
	return out, nil
}

// ---- The file root ----

// fileRoot wraps under a 32-byte key read from a file. It moves the
// problem one step — the wrapping key is a file where the store key was —
// and is here for tests, for a wrapping key kept on storage other than
// the data's, and as the first step of moving a store to another root.
type fileRoot struct {
	path string
	key  []byte
	id   string
}

func newFileRoot(path string) (*fileRoot, error) {
	key, err := LoadKeyFile(path)
	if err != nil {
		return nil, fmt.Errorf("file root: %w", err)
	}
	// The id is a hash of the key under a label of its own. It tells two
	// wrapping keys apart and tells nothing else: the key is 256 random
	// bits.
	sum := sha256.Sum256(append([]byte("datax file root key id v1\x00"), key...))
	return &fileRoot{path: path, key: key, id: hex.EncodeToString(sum[:8])}, nil
}

func (r *fileRoot) Kind() string   { return "file" }
func (r *fileRoot) KeyID() string  { return r.id }
func (r *fileRoot) String() string { return "file:" + r.path }

func (r *fileRoot) Wrap(_ context.Context, plaintext []byte) ([]byte, error) {
	return Seal(fileRootMagic, r.key, plaintext)
}

func (r *fileRoot) Unwrap(_ context.Context, wrapped []byte) ([]byte, error) {
	return Unseal(fileRootMagic, r.key, wrapped)
}

// ---- The exec root ----

// execRoot hands wrapping to a command the operator names. The command
// is run with one more argument, the verb:
//
//	<command> wrap      stdin: the store key, 32 raw bytes      stdout: the wrapped key, any bytes
//	<command> unwrap    stdin: the wrapped key, as wrap wrote it stdout: the store key, 32 raw bytes or 64 hex characters
//	<command> key-id    stdout: a name for the wrapping key (optional: a failure means "unnamed")
//
// It exits zero on success; anything else is a failure and its standard
// error is the reason given to the operator. The command is run
// directly, not through a shell, with the node's environment — which is
// where a cloud CLI finds its credentials — and under a timeout.
//
// The command line comes from the node's own flags and from nowhere
// else: no request can name a command for a node to run.
type execRoot struct {
	spec    string
	argv    []string
	timeout time.Duration
	id      string
}

const (
	execMaxStdout = maxWrappedLen
	execMaxStderr = 4 << 10
)

func newExecRoot(commandLine string, timeout time.Duration) (*execRoot, error) {
	argv := strings.Fields(commandLine)
	if len(argv) == 0 {
		return nil, errors.New("exec root: no command")
	}
	path, err := exec.LookPath(argv[0])
	if err != nil {
		return nil, fmt.Errorf("exec root: %w", err)
	}
	argv[0] = path
	r := &execRoot{spec: commandLine, argv: argv, timeout: timeout}
	if out, err := r.run(context.Background(), "key-id", nil); err == nil {
		r.id = strings.TrimSpace(string(out))
		if len(r.id) > 256 || strings.ContainsAny(r.id, "\r\n\x00") {
			return nil, fmt.Errorf("exec root: %s key-id returned something that is not a one-line name", argv[0])
		}
	}
	return r, nil
}

func (r *execRoot) Kind() string   { return "exec" }
func (r *execRoot) KeyID() string  { return r.id }
func (r *execRoot) String() string { return "exec:" + r.spec }

// limitedBuffer keeps the first max bytes written and counts the rest.
type limitedBuffer struct {
	buf      bytes.Buffer
	max      int
	overflow bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := b.max - b.buf.Len(); room < len(p) {
		b.overflow = true
		if room > 0 {
			b.buf.Write(p[:room])
		}
		return len(p), nil
	}
	return b.buf.Write(p)
}

func (r *execRoot) run(ctx context.Context, verb string, stdin []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.argv[0], append(append([]string(nil), r.argv[1:]...), verb)...)
	cmd.Stdin = bytes.NewReader(stdin)
	stdout := &limitedBuffer{max: execMaxStdout}
	stderr := &limitedBuffer{max: execMaxStderr}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	// A command that ignores the kill still has its pipes closed and is
	// not waited on for ever.
	cmd.WaitDelay = 2 * time.Second
	err := cmd.Run()
	if ctx.Err() != nil {
		return nil, fmt.Errorf("%s %s did not finish within %s", r.argv[0], verb, r.timeout)
	}
	if err != nil {
		reason := strings.TrimSpace(stderr.buf.String())
		if reason == "" {
			reason = "it wrote nothing to standard error"
		}
		return nil, fmt.Errorf("%s %s failed (%v): %s", r.argv[0], verb, err, reason)
	}
	if stdout.overflow {
		return nil, fmt.Errorf("%s %s wrote more than %d bytes", r.argv[0], verb, execMaxStdout)
	}
	return stdout.buf.Bytes(), nil
}

func (r *execRoot) Wrap(ctx context.Context, plaintext []byte) ([]byte, error) {
	out, err := r.run(ctx, "wrap", plaintext)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s wrap wrote nothing", r.argv[0])
	}
	return out, nil
}

func (r *execRoot) Unwrap(ctx context.Context, wrapped []byte) ([]byte, error) {
	out, err := r.run(ctx, "unwrap", wrapped)
	if err != nil {
		return nil, err
	}
	if len(out) == KeyLen {
		return out, nil
	}
	// As a key file may be: 64 hex characters, for a command that would
	// rather not write raw bytes.
	if s := strings.TrimSpace(string(out)); len(s) == 2*KeyLen {
		if key, err := hex.DecodeString(s); err == nil {
			return key, nil
		}
	}
	return nil, fmt.Errorf("%s unwrap wrote %d bytes; want the store key as %d raw bytes or %d hex characters", r.argv[0], len(out), KeyLen, 2*KeyLen)
}
