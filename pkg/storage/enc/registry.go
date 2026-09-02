package enc

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble/vfs"
)

// The registry file seals the data keys under the store key:
//
//	"DXR1" | 12-byte GCM nonce | AES-256-GCM(storeKey, JSON registryData)
//
// The store key never encrypts file content directly — data keys do — so
// rotating the store key is a registry reseal, not a store rewrite. A fresh
// data key is minted and made active on every Open, bounding how much
// ciphertext any single data key covers; old keys are retained so existing
// files stay readable.
const (
	registryMagic = "DXR1"
	// KeyLen is the required store/data key length (AES-256).
	KeyLen = 32
)

type registryData struct {
	Version     int               `json:"version"`
	ActiveKeyID uint32            `json:"active_key_id"`
	Keys        map[string]string `json:"keys"` // decimal key ID -> hex key
}

// KeySet is the decrypted registry: the active data key new files are
// written with, plus every historical data key for reading old files.
type KeySet struct {
	activeID uint32
	keys     map[uint32][]byte
}

// Active returns the data key new files are encrypted with.
func (ks *KeySet) Active() (uint32, []byte) {
	return ks.activeID, ks.keys[ks.activeID]
}

// Lookup returns the data key with the given ID.
func (ks *KeySet) Lookup(id uint32) ([]byte, bool) {
	k, ok := ks.keys[id]
	return k, ok
}

// Seal encrypts blob under a 32-byte key with AES-256-GCM, prefixed with
// magic (4 bytes) and the random nonce. Used for the key registry and for
// sealing non-Pebble artifacts like the metadata backup.
func Seal(magic string, key, blob []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(magic)+len(nonce)+len(blob)+gcm.Overhead())
	out = append(out, magic...)
	out = append(out, nonce...)
	return gcm.Seal(out, nonce, blob, nil), nil
}

// Unseal reverses Seal. A wrong key fails GCM authentication.
func Unseal(magic string, key, sealed []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(sealed) < len(magic)+gcm.NonceSize()+gcm.Overhead() {
		return nil, fmt.Errorf("sealed blob truncated (%d bytes)", len(sealed))
	}
	if string(sealed[:len(magic)]) != magic {
		return nil, fmt.Errorf("bad magic %q (want %q)", sealed[:len(magic)], magic)
	}
	nonce := sealed[len(magic) : len(magic)+gcm.NonceSize()]
	plain, err := gcm.Open(nil, nonce, sealed[len(magic)+gcm.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed (wrong key?): %w", err)
	}
	return plain, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != KeyLen {
		return nil, fmt.Errorf("key must be %d bytes, got %d", KeyLen, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// LoadOrInitRegistry opens (or creates) the key registry in dir on the base
// (unencrypted) FS, unsealing it with the 32-byte store key. It always
// mints a fresh active data key — one per store open — reseals, and writes
// the registry atomically (tmp + rename).
func LoadOrInitRegistry(base vfs.FS, dir string, storeKey []byte) (*KeySet, error) {
	if len(storeKey) != KeyLen {
		return nil, fmt.Errorf("store key must be %d bytes, got %d", KeyLen, len(storeKey))
	}
	if err := base.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := base.PathJoin(dir, RegistryName)

	reg := registryData{Version: 1, Keys: map[string]string{}}
	if sealed, err := readAll(base, path); err == nil {
		plain, err := Unseal(registryMagic, storeKey, sealed)
		if err != nil {
			return nil, fmt.Errorf("encryption key does not match store: %w", err)
		}
		if err := json.Unmarshal(plain, &reg); err != nil {
			return nil, fmt.Errorf("corrupt key registry: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// Mint the new active data key.
	newID := reg.ActiveKeyID + 1
	for {
		if _, dup := reg.Keys[fmt.Sprint(newID)]; !dup {
			break
		}
		newID++
	}
	dataKey := make([]byte, KeyLen)
	if _, err := rand.Read(dataKey); err != nil {
		return nil, err
	}
	reg.ActiveKeyID = newID
	reg.Keys[fmt.Sprint(newID)] = hex.EncodeToString(dataKey)

	if err := writeRegistry(base, dir, storeKey, &reg); err != nil {
		return nil, err
	}
	return keySetFrom(&reg)
}

func keySetFrom(reg *registryData) (*KeySet, error) {
	ks := &KeySet{activeID: reg.ActiveKeyID, keys: map[uint32][]byte{}}
	for idStr, hexKey := range reg.Keys {
		var id uint32
		if _, err := fmt.Sscan(idStr, &id); err != nil {
			return nil, fmt.Errorf("corrupt key registry: bad key ID %q", idStr)
		}
		k, err := hex.DecodeString(hexKey)
		if err != nil || len(k) != KeyLen {
			return nil, fmt.Errorf("corrupt key registry: bad key for ID %s", idStr)
		}
		ks.keys[id] = k
	}
	if _, ok := ks.keys[ks.activeID]; !ok {
		return nil, fmt.Errorf("corrupt key registry: active key %d missing", ks.activeID)
	}
	return ks, nil
}

func writeRegistry(base vfs.FS, dir string, storeKey []byte, reg *registryData) error {
	plain, err := json.Marshal(reg)
	if err != nil {
		return err
	}
	sealed, err := Seal(registryMagic, storeKey, plain)
	if err != nil {
		return err
	}
	path := base.PathJoin(dir, RegistryName)
	tmp := path + ".tmp"
	f, err := base.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(sealed); err != nil {
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
	// The rename is only durable once the directory entry is: without a
	// directory fsync a crash after a "successful" rotation can bring the
	// registry back sealed under the OLD store key — which the operator
	// may already have retired. (In-memory stores have no directory.)
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

// RotateStoreKey reseals the registry (and nothing else) under a new store
// key. Offline only: a running node keeps sealing artifacts with the key it
// loaded at startup — stop the node first.
func RotateStoreKey(base vfs.FS, dir string, oldKey, newKey []byte) error {
	if len(newKey) != KeyLen {
		return fmt.Errorf("new store key must be %d bytes, got %d", KeyLen, len(newKey))
	}
	path := base.PathJoin(dir, RegistryName)
	sealed, err := readAll(base, path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("store is not encrypted (no %s in %s)", RegistryName, dir)
		}
		return err
	}
	plain, err := Unseal(registryMagic, oldKey, sealed)
	if err != nil {
		return fmt.Errorf("old encryption key does not match store: %w", err)
	}
	var reg registryData
	if err := json.Unmarshal(plain, &reg); err != nil {
		return fmt.Errorf("corrupt key registry: %w", err)
	}
	return writeRegistry(base, dir, newKey, &reg)
}

// MatchStoreKey picks, from candidate store keys, the one that unseals the
// key registry in dir and returns its index. Without a registry (a store
// about to be initialized — or a plaintext one; Open decides which) the
// first candidate is chosen. Only reads: the registry is untouched. This
// is what lets an operator stage a new store key next to the current one
// (`--enc-key old.key,new.key`) before rotating online, so a restart in
// the window between the rotation and the key-file swap still opens the
// store (issue #67).
func MatchStoreKey(base vfs.FS, dir string, candidates [][]byte) (int, error) {
	if len(candidates) == 0 {
		return 0, errors.New("no store key given")
	}
	for i, k := range candidates {
		if len(k) != KeyLen {
			return 0, fmt.Errorf("store key %d of %d must be %d bytes, got %d", i+1, len(candidates), KeyLen, len(k))
		}
	}
	sealed, err := readAll(base, base.PathJoin(dir, RegistryName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	for i, k := range candidates {
		if _, err := Unseal(registryMagic, k, sealed); err == nil {
			return i, nil
		}
	}
	return 0, fmt.Errorf("none of the %d store keys given matches the encrypted store in %s", len(candidates), dir)
}

// SplitKeyPaths parses an --enc-key value: one key file path, or several
// separated by commas (surrounding whitespace ignored, empty entries
// dropped). Several paths are candidates tried in order against the
// store's registry; see MatchStoreKey.
func SplitKeyPaths(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// LoadKeyFiles reads every path with LoadKeyFile.
func LoadKeyFiles(paths []string) ([][]byte, error) {
	keys := make([][]byte, 0, len(paths))
	for _, p := range paths {
		k, err := LoadKeyFile(p)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, nil
}

// RegistryExists reports whether dir contains a key registry (i.e. the
// store is encrypted).
func RegistryExists(base vfs.FS, dir string) bool {
	if st, err := base.Stat(base.PathJoin(dir, RegistryName)); err == nil && !st.IsDir() {
		return true
	}
	return false
}

// LoadKeyFile reads a 32-byte store key from path: either 32 raw bytes or
// 64 hex characters (surrounding whitespace ignored).
func LoadKeyFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) == KeyLen {
		return raw, nil
	}
	s := strings.TrimSpace(string(raw))
	if len(s) == 2*KeyLen {
		key, err := hex.DecodeString(s)
		if err == nil {
			return key, nil
		}
	}
	return nil, fmt.Errorf("%s: want %d raw bytes or %d hex characters, got %d bytes", path, KeyLen, 2*KeyLen, len(raw))
}

func readAll(base vfs.FS, path string) ([]byte, error) {
	f, err := base.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, st.Size())
	if _, err := f.ReadAt(buf, 0); err != nil && st.Size() > 0 {
		return nil, err
	}
	return buf, nil
}
