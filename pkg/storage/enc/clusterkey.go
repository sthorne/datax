package enc

import (
	"bytes"
	"context"
	"crypto/ecdh"
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
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// The cluster key (issue #310, docs/design-key-service.md).
//
//	root (per node)  wraps →  cluster key (one per cluster, versioned)  wraps →  named keys (datax_keys)
//
// Each node keeps the cluster key wrapped by its own root, in its data
// directory beside the wrapped store key (ClusterKeyFileName), and
// unwrapped only in held memory while it runs. What is here is the
// key's form on disk and on the wire; when to make one, who may be
// handed it and how the cluster agrees which version is current are the
// server's (pkg/server/clusterkey.go).
//
// A cluster key is named by an id (made when the key is first made, kept
// across rotations) and a version (1, then one more per rotation). A
// node may hold several: the current version, the one a rotation is
// leaving, and keys imported from another cluster so that its backups
// can be restored here.

// ClusterKeyFileName is the file in the data directory holding this
// node's copies of the cluster key, each wrapped by its root. It is not
// secret, and it cannot be edited: each entry carries a tag keyed from
// the cluster key it wraps.
const ClusterKeyFileName = "CLUSTER-KEY"

const (
	clusterKeyFileVersion = 1
	// clusterKeyTransportMagic prefixes cluster keys sealed for one
	// exchange between two nodes, or between a node and the CLI.
	clusterKeyTransportMagic = "DXC1"
	// ClusterKeyExportMagic marks an export file.
	ClusterKeyExportMagic = "datax-cluster-key-export-v1"
)

// ClusterKeyRef names one cluster key: its id and version.
type ClusterKeyRef struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
}

func (r ClusterKeyRef) String() string { return fmt.Sprintf("%s/v%d", r.ID, r.Version) }

// IsZero reports whether r names nothing.
func (r ClusterKeyRef) IsZero() bool { return r.ID == "" && r.Version == 0 }

// NewClusterKeyID returns a fresh id for a cluster key: 16 hex digits,
// random, so that two clusters' keys are never confused.
func NewClusterKeyID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// NewClusterKey returns fresh key material for a cluster key.
func NewClusterKey() ([]byte, error) { return NewStoreKey() }

// ClusterKeyCheck is the value that tells whoever holds a key whether it
// is the cluster key ref names: HMAC-SHA256 under the key over a
// constant, the id and the version. It is recorded, replicated, beside
// the key's name; it says nothing about the key to anyone who does not
// hold it.
func ClusterKeyCheck(key []byte, ref ClusterKeyRef) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("datax cluster key check v1\x00"))
	m.Write([]byte(ref.ID))
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], uint64(ref.Version))
	m.Write(v[:])
	return hex.EncodeToString(m.Sum(nil)[:16])
}

// CheckClusterKey reports whether key is the cluster key ref names, by
// the check value recorded for it.
func CheckClusterKey(key []byte, ref ClusterKeyRef, check string) bool {
	if len(key) != KeyLen || check == "" {
		return false
	}
	return hmac.Equal([]byte(ClusterKeyCheck(key, ref)), []byte(check))
}

// ClusterKeyEntry is one cluster key wrapped by a root: on a node, in
// its ClusterKeyFileName; in an export, under the operator's root.
type ClusterKeyEntry struct {
	ClusterKeyRef
	// Imported: the key is another cluster's, held so that its backups
	// can be restored here (FromCluster names that cluster).
	Imported    bool   `json:"imported,omitempty"`
	FromCluster string `json:"from_cluster,omitempty"`
	// Root and RootKeyID say what wrapped the entry.
	Root      string `json:"root"`
	RootKeyID string `json:"root_key_id,omitempty"`
	Wrapped   []byte `json:"wrapped"`
	// Binding is the cluster and node the entry was wrapped for, as a
	// store key's is (binding.go); nil in an export.
	Binding *Binding `json:"binding,omitempty"`
	// MAC authenticates every field above under a key derived from the
	// cluster key the entry wraps.
	MAC []byte `json:"mac"`
}

type clusterKeyFile struct {
	Version int               `json:"version"`
	Keys    []ClusterKeyEntry `json:"keys"`
}

func clusterKeyEntryMAC(key []byte, e *ClusterKeyEntry) ([]byte, error) {
	macKey, err := hkdf.Key(sha256.New, key, nil, "datax cluster key entry mac v1", KeyLen)
	if err != nil {
		return nil, err
	}
	defer Zero(macKey)
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], uint64(e.Version))
	imported := []byte{0}
	if e.Imported {
		imported[0] = 1
	}
	var binding []byte
	if e.Binding != nil {
		binding = e.Binding.AAD()
	}
	m := hmac.New(sha256.New, macKey)
	for _, f := range [][]byte{[]byte(e.ID), v[:], imported, []byte(e.FromCluster), []byte(e.Root), []byte(e.RootKeyID), e.Wrapped, binding} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(f)))
		m.Write(n[:])
		m.Write(f)
	}
	return m.Sum(nil), nil
}

// WrapClusterKey wraps a cluster key under root, bound to the cluster and
// node ctx names (WithBinding) when it names one. The root's answer is
// held to what a store key's is (WrapStoreKey): non-empty, bounded, not
// containing the key, and unwrapping to the same key.
func WrapClusterKey(ctx context.Context, root Root, ref ClusterKeyRef, key []byte) (ClusterKeyEntry, error) {
	if len(key) != KeyLen {
		return ClusterKeyEntry{}, fmt.Errorf("a cluster key must be %d bytes, got %d", KeyLen, len(key))
	}
	if ref.ID == "" || ref.Version < 1 {
		return ClusterKeyEntry{}, fmt.Errorf("a cluster key needs an id and a version from 1 (got %q, %d)", ref.ID, ref.Version)
	}
	binding := BindingFrom(ctx)
	wrapped, err := wrapWith(ctx, root, key, binding)
	if err != nil {
		return ClusterKeyEntry{}, fmt.Errorf("the %s root could not wrap the cluster key: %w", root.Kind(), err)
	}
	if len(wrapped) == 0 || len(wrapped) > maxWrappedLen {
		return ClusterKeyEntry{}, fmt.Errorf("the %s root returned %d bytes for a wrapped cluster key (want 1 to %d)", root.Kind(), len(wrapped), maxWrappedLen)
	}
	if bytes.Contains(wrapped, key) {
		return ClusterKeyEntry{}, fmt.Errorf("the %s root returned the cluster key unwrapped; refusing to write it", root.Kind())
	}
	back, err := unwrapWith(ctx, root, wrapped, binding)
	if err != nil {
		return ClusterKeyEntry{}, fmt.Errorf("the %s root could not unwrap what it had just wrapped: %w", root.Kind(), err)
	}
	same := bytes.Equal(back, key)
	Zero(back)
	if !same {
		return ClusterKeyEntry{}, fmt.Errorf("the %s root unwrapped its own output to a different key", root.Kind())
	}
	e := ClusterKeyEntry{ClusterKeyRef: ref, Root: root.Kind(), RootKeyID: root.KeyID(), Wrapped: wrapped}
	if !binding.IsZero() {
		e.Binding = &binding
	}
	if e.MAC, err = clusterKeyEntryMAC(key, &e); err != nil {
		return ClusterKeyEntry{}, err
	}
	return e, nil
}

// SetImported marks a wrapped entry as another cluster's key and
// re-tags it; key is the cluster key the entry wraps.
func (e *ClusterKeyEntry) SetImported(key []byte, fromCluster string) error {
	e.Imported, e.FromCluster = true, fromCluster
	mac, err := clusterKeyEntryMAC(key, e)
	if err != nil {
		return err
	}
	e.MAC = mac
	return nil
}

// UnwrapClusterKey returns the cluster key e holds, through whichever of
// roots wrapped it. The caller owns the result and zeroes it.
func UnwrapClusterKey(ctx context.Context, roots []Root, e *ClusterKeyEntry) ([]byte, error) {
	se := WrappedEntry{Root: e.Root, KeyID: e.RootKeyID}
	r := rootFor(roots, &se)
	if r == nil {
		return nil, ErrNoRootForEntry
	}
	if len(e.Wrapped) == 0 || len(e.Wrapped) > maxWrappedLen {
		return nil, fmt.Errorf("a wrapped cluster key of %d bytes", len(e.Wrapped))
	}
	var binding Binding
	if e.Binding != nil {
		binding = *e.Binding
	}
	if want := BindingFrom(ctx); !want.IsZero() && e.Binding != nil && binding != want {
		return nil, fmt.Errorf("the cluster key %s was wrapped for %s, not for %s: it is another node's", e.ClusterKeyRef, binding, want)
	}
	key, err := unwrapWith(ctx, r, e.Wrapped, binding)
	if err != nil {
		return nil, fmt.Errorf("the %s root could not unwrap cluster key %s: %w", r.Kind(), e.ClusterKeyRef, err)
	}
	if len(key) != KeyLen {
		Zero(key)
		return nil, fmt.Errorf("the %s root unwrapped cluster key %s to %d bytes, want %d", r.Kind(), e.ClusterKeyRef, len(key), KeyLen)
	}
	want, err := clusterKeyEntryMAC(key, e)
	if err != nil {
		Zero(key)
		return nil, err
	}
	if !hmac.Equal(want, e.MAC) {
		Zero(key)
		return nil, fmt.Errorf("the entry for cluster key %s has been altered: its fields do not match the key it wraps", e.ClusterKeyRef)
	}
	return key, nil
}

// ReadClusterKeyFile returns the entries of dir's cluster key file, and
// an error satisfying os.IsNotExist when there is none.
func ReadClusterKeyFile(fs vfs.FS, dir string) ([]ClusterKeyEntry, error) {
	raw, err := readAll(fs, fs.PathJoin(dir, ClusterKeyFileName))
	if err != nil {
		return nil, err
	}
	var f clusterKeyFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("corrupt %s: %w", ClusterKeyFileName, err)
	}
	if f.Version != clusterKeyFileVersion {
		return nil, fmt.Errorf("%s is version %d; this binary reads version %d", ClusterKeyFileName, f.Version, clusterKeyFileVersion)
	}
	return f.Keys, nil
}

// WriteClusterKeyFile replaces dir's cluster key file atomically and
// durably. Entries are kept in a stable order (id, then version).
func WriteClusterKeyFile(fs vfs.FS, dir string, entries []ClusterKeyEntry) error {
	sorted := append([]ClusterKeyEntry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ID != sorted[j].ID {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].Version < sorted[j].Version
	})
	raw, err := json.Marshal(clusterKeyFile{Version: clusterKeyFileVersion, Keys: sorted})
	if err != nil {
		return err
	}
	return writeFileAtomic(fs, dir, ClusterKeyFileName, raw)
}

// ClusterKeyFileExists reports whether dir holds a cluster key file.
func ClusterKeyFileExists(fs vfs.FS, dir string) bool {
	_, err := fs.Stat(fs.PathJoin(dir, ClusterKeyFileName))
	return err == nil
}

// RemoveClusterKeyFile deletes dir's cluster key file; absent is fine.
func RemoveClusterKeyFile(fs vfs.FS, dir string) error {
	err := fs.Remove(fs.PathJoin(dir, ClusterKeyFileName))
	if err != nil && !errors.Is(err, os.ErrNotExist) && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// TransportKey is one cluster key as it travels inside a sealed bundle.
type TransportKey struct {
	ClusterKeyRef
	Imported    bool   `json:"imported,omitempty"`
	FromCluster string `json:"from_cluster,omitempty"`
	Key         []byte `json:"key"`
}

// ZeroTransportKeys zeroes the key material of ks.
func ZeroTransportKeys(ks []TransportKey) {
	for i := range ks {
		Zero(ks[i].Key)
	}
}

// SealClusterKeys seals keys for the asker of one exchange, whose public
// half is askerPub. binding is the request as both sides know it, so an
// answer cannot be replayed to another. It returns the giver's public
// half and the sealed bundle. The exchange is peer unseal's (peer.go)
// under a label of its own.
func SealClusterKeys(askerPub, binding []byte, keys []TransportKey) (giverPub, sealed []byte, err error) {
	pub, err := ecdh.X25519().NewPublicKey(askerPub)
	if err != nil {
		return nil, nil, fmt.Errorf("the request's exchange key is malformed: %w", err)
	}
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	shared, err := priv.ECDH(pub)
	if err != nil {
		return nil, nil, err
	}
	giverPub = priv.PublicKey().Bytes()
	k, err := exchangeKey("datax cluster key transport v1", shared, askerPub, giverPub, binding)
	Zero(shared)
	if err != nil {
		return nil, nil, err
	}
	defer Zero(k)
	plain, err := json.Marshal(keys)
	if err != nil {
		return nil, nil, err
	}
	defer Zero(plain)
	sealed, err = Seal(clusterKeyTransportMagic, k, plain)
	if err != nil {
		return nil, nil, err
	}
	return giverPub, sealed, nil
}

// OpenClusterKeys opens a bundle sealed for exchange x. The caller owns
// the keys returned and zeroes them (ZeroTransportKeys).
func (x *PeerExchange) OpenClusterKeys(giverPub, sealed, binding []byte) ([]TransportKey, error) {
	pub, err := ecdh.X25519().NewPublicKey(giverPub)
	if err != nil {
		return nil, fmt.Errorf("the peer's exchange key is malformed: %w", err)
	}
	shared, err := x.priv.ECDH(pub)
	if err != nil {
		return nil, err
	}
	k, err := exchangeKey("datax cluster key transport v1", shared, x.Public(), giverPub, binding)
	Zero(shared)
	if err != nil {
		return nil, err
	}
	plain, err := Unseal(clusterKeyTransportMagic, k, sealed)
	Zero(k)
	if err != nil {
		return nil, fmt.Errorf("the answer does not open under this exchange: %w", err)
	}
	defer Zero(plain)
	var keys []TransportKey
	if err := json.Unmarshal(plain, &keys); err != nil {
		return nil, fmt.Errorf("the answer is malformed: %w", err)
	}
	for _, tk := range keys {
		if len(tk.Key) != KeyLen {
			ZeroTransportKeys(keys)
			return nil, fmt.Errorf("the answer carries a cluster key of %d bytes", len(tk.Key))
		}
	}
	return keys, nil
}

// ClusterKeyExport is the file `datax debug cluster-key export` writes:
// a cluster's keys, each wrapped under a root the operator chose, so that
// another cluster can be given them (`import`) and restore this one's
// backups. Nothing in it is readable without that root.
type ClusterKeyExport struct {
	Magic     string `json:"magic"`
	ClusterID string `json:"cluster_id"`
	// Current is the version the source cluster used when it was
	// exported; every entry is listed.
	Current ClusterKeyRef     `json:"current"`
	Keys    []ClusterKeyEntry `json:"keys"`
}

// ExportClusterKeys wraps keys under root for an export file.
func ExportClusterKeys(ctx context.Context, root Root, clusterID string, current ClusterKeyRef, keys []TransportKey) (*ClusterKeyExport, error) {
	out := &ClusterKeyExport{Magic: ClusterKeyExportMagic, ClusterID: clusterID, Current: current}
	for _, tk := range keys {
		e, err := WrapClusterKey(ctx, root, tk.ClusterKeyRef, tk.Key)
		if err != nil {
			return nil, err
		}
		if tk.Imported {
			if err := e.SetImported(tk.Key, tk.FromCluster); err != nil {
				return nil, err
			}
		}
		out.Keys = append(out.Keys, e)
	}
	return out, nil
}

// ParseClusterKeyExport reads an export file's contents.
func ParseClusterKeyExport(raw []byte) (*ClusterKeyExport, error) {
	var x ClusterKeyExport
	if err := json.Unmarshal(raw, &x); err != nil {
		return nil, fmt.Errorf("not a cluster key export: %w", err)
	}
	if x.Magic != ClusterKeyExportMagic {
		return nil, fmt.Errorf("not a cluster key export (magic %q)", x.Magic)
	}
	if len(x.Keys) == 0 {
		return nil, errors.New("the export holds no key")
	}
	return &x, nil
}

// Open unwraps every key of an export through roots. The caller owns the
// keys and zeroes them.
func (x *ClusterKeyExport) Open(ctx context.Context, roots []Root) ([]TransportKey, error) {
	var out []TransportKey
	for i := range x.Keys {
		e := &x.Keys[i]
		k, err := UnwrapClusterKey(ctx, roots, e)
		if err != nil {
			ZeroTransportKeys(out)
			return nil, err
		}
		from := e.FromCluster
		if !e.Imported {
			from = x.ClusterID
		}
		out = append(out, TransportKey{ClusterKeyRef: e.ClusterKeyRef, Imported: true, FromCluster: from, Key: k})
	}
	return out, nil
}

// ParseClusterKeyRef reads a ref as ClusterKeyRef.String writes it.
func ParseClusterKeyRef(s string) (ClusterKeyRef, error) {
	var r ClusterKeyRef
	i := strings.LastIndex(s, "/v")
	if i <= 0 {
		return r, fmt.Errorf("malformed cluster key reference %q", s)
	}
	v, err := strconv.Atoi(s[i+2:])
	if err != nil || v < 1 {
		return r, fmt.Errorf("malformed cluster key reference %q", s)
	}
	return ClusterKeyRef{ID: s[:i], Version: v}, nil
}
