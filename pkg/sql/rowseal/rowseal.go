// Package rowseal is the boundary of sealed tables (issue #310,
// docs/design-key-service.md, "B. Per-table and per-user data keys"):
// where a sealed table's row value — the output of rowenc.EncodeValue —
// is encrypted on its way to KV, and decrypted on its way back.
//
// Everything below the boundary — KV, raft, the storage engine,
// replicas, snapshots, the consistency checker, backups — sees only the
// sealed bytes and needs no change. Everything above it — constraint
// checks, filters, sorts, EXPLAIN — works on plaintext. Keys are not
// sealed: primary-key columns and indexed columns stay in KV keys, which
// must sort, and are protected by file-level encryption only.
//
// # The sealed form
//
//	marker (1 byte, rowenc.SealedValueMarker)
//	key version (uvarint)
//	nonce (12 bytes, random)
//	ciphertext ‖ tag (AES-256-GCM, 16-byte tag)
//
// The associated data binds the value to where it belongs:
//
//	"datax sealed row v1\x00" ‖ seal id ‖ key id ‖ key version ‖ row key
//
// each number as 8 big-endian bytes, the row key being the value's KV key
// after the table's prefix (/t/<id>): its index id and encoded columns.
// The seal id is the table's id when it was created
// (catalog.TableEncryptionKey.SealID), which a table restore keeps when it
// gives the table a new id. A value moved to another row, another table
// or presented under another key or version fails authentication.
//
// The overhead is 1 + 1 + 12 + 16 = 30 bytes per value (the version takes
// one byte up to 127). Nonces are random, so one key version should seal
// no more than about 2^32 values (NIST SP 800-38D): rotating the table's
// key starts a new version.
//
// # Choosing the key: the resolver
//
// A Resolver says which key a row is sealed under, from the descriptor
// and the row's KV key. A whole sealed table has one key (TableKey). The
// per-tenant tables of step 4 plug in a resolver that decodes the leading
// primary-key column from the row key and maps its value to that
// tenant's key, creating it on a first write (forWrite); nothing else in
// the boundary changes, since the key id is bound into the associated
// data and the read path asks the same resolver.
package rowseal

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore/keycache"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// Marker is the first byte of a sealed value.
const Marker = rowenc.SealedValueMarker

const (
	nonceLen = 12
	tagLen   = 16
	adPrefix = "datax sealed row v1\x00"
)

// Overhead is the bytes sealing adds to a value under key version v.
func Overhead(v int) int {
	return 1 + len(encoding.EncodeUvarint(nil, uint64(v))) + nonceLen + tagLen
}

// IsSealed reports whether a stored value is in the sealed form.
func IsSealed(v []byte) bool { return len(v) > 0 && v[0] == Marker }

// AD is the associated data a value is sealed under; rowKey is the
// value's KV key after the table's prefix.
func AD(sealID uint64, keyID int64, version int, rowKey []byte) []byte {
	ad := make([]byte, 0, len(adPrefix)+24+len(rowKey))
	ad = append(ad, adPrefix...)
	ad = binary.BigEndian.AppendUint64(ad, sealID)
	ad = binary.BigEndian.AppendUint64(ad, uint64(keyID))
	ad = binary.BigEndian.AppendUint64(ad, uint64(version))
	return append(ad, rowKey...)
}

// Seal encrypts plain under aead (key version v) with associated data ad.
func Seal(aead cipher.AEAD, v int, ad, plain []byte) ([]byte, error) {
	if aead.NonceSize() != nonceLen || aead.Overhead() != tagLen {
		return nil, errors.New("rowseal: the key is not AES-GCM")
	}
	if v <= 0 {
		return nil, fmt.Errorf("rowseal: key version %d", v)
	}
	out := make([]byte, 0, Overhead(v)+len(plain))
	out = append(out, Marker)
	out = encoding.EncodeUvarint(out, uint64(v))
	at := len(out)
	out = out[:at+nonceLen]
	if _, err := rand.Read(out[at:]); err != nil {
		return nil, err
	}
	return aead.Seal(out, out[at:at+nonceLen], plain, ad), nil
}

// ErrMalformed is a value that begins with the marker and is not a
// well-formed sealed value.
var ErrMalformed = errors.New("malformed sealed value")

// errAuth is a value that does not open: tampered with, moved, or sealed
// under another key.
var errAuth = errors.New("sealed value does not authenticate")

// Version returns the key version a sealed value is under.
func Version(sealed []byte) (int, error) {
	v, _, err := split(sealed)
	return v, err
}

func split(sealed []byte) (version int, body []byte, err error) {
	if !IsSealed(sealed) {
		return 0, nil, ErrMalformed
	}
	rest, v, err := encoding.DecodeUvarint(sealed[1:])
	if err != nil || v == 0 || v > 1<<31 {
		return 0, nil, ErrMalformed
	}
	if len(rest) < nonceLen+tagLen {
		return 0, nil, ErrMalformed
	}
	return int(v), rest, nil
}

// Open decrypts a sealed value under aead with associated data ad.
func Open(aead cipher.AEAD, ad, sealed []byte) ([]byte, error) {
	_, body, err := split(sealed)
	if err != nil {
		return nil, err
	}
	plain, err := aead.Open(nil, body[:nonceLen], body[nonceLen:], ad)
	if err != nil {
		return nil, errAuth
	}
	return plain, nil
}

// Keys is where the boundary gets key versions: the gateway's
// keycache.Cache.
type Keys interface {
	Current(ctx context.Context, id int64) (keycache.Key, error)
	Version(ctx context.Context, id int64, v int) (keycache.Key, error)
	Invalidate(id int64)
}

// KeyStore is the node's key store as the SQL layer needs it for the
// keys of sealed tables: pkg/keystore's Store, adapted by the server.
type KeyStore interface {
	// Ready refuses, with the reason, while sealed tables cannot be
	// made: before cluster version v22 is finalized or before the
	// cluster key is enabled.
	Ready(ctx context.Context) error
	// CreateTableKey makes a key of kind table in txn and returns its id.
	CreateTableKey(ctx context.Context, txn *kvclient.Txn, name string) (int64, error)
	// DestroyKey destroys a key and purges its record's history.
	DestroyKey(ctx context.Context, id int64) error
}

// Resolver picks the key a row is sealed under. rowKey is the row's whole
// KV key. forWrite says the row is being written, when a resolver may
// create the key (a tenant's first row); a read never creates one.
type Resolver interface {
	Resolve(ctx context.Context, desc *catalog.TableDescriptor, rowKey keys.Key, forWrite bool) (keyID int64, err error)
}

// TableKey is the resolver of a whole sealed table: every row is under
// the table's one key.
type TableKey struct{}

// Resolve implements Resolver.
func (TableKey) Resolve(_ context.Context, desc *catalog.TableDescriptor, _ keys.Key, _ bool) (int64, error) {
	if desc.EncryptionKey == nil || desc.EncryptionKey.KeyID == 0 {
		return 0, fmt.Errorf("table %q is not sealed", desc.Name)
	}
	return desc.EncryptionKey.KeyID, nil
}

// Boundary seals and opens row values for the tables of one node.
type Boundary struct {
	keys     Keys
	store    KeyStore
	resolver Resolver
}

// New returns a boundary over keys and store; a nil resolver is TableKey.
func New(k Keys, store KeyStore, r Resolver) *Boundary {
	if r == nil {
		r = TableKey{}
	}
	return &Boundary{keys: k, store: store, resolver: r}
}

// Store is the key store the boundary was made with (nil without one).
func (b *Boundary) Store() KeyStore {
	if b == nil {
		return nil
	}
	return b.store
}

// Invalidate drops key id from the gateway's cache.
func (b *Boundary) Invalidate(id int64) {
	if b != nil && b.keys != nil {
		b.keys.Invalidate(id)
	}
}

// For returns the boundary set on a catalog accessor, or nil.
func For(cat *catalog.Accessor) *Boundary {
	b, _ := cat.RowSealer().(*Boundary)
	return b
}

// KeyUnavailableError is a sealed row whose key gives no material: it was
// destroyed, is disabled, or cannot be read on this node. The statement
// fails; the row is never skipped and its ciphertext never returned.
type KeyUnavailableError struct {
	Table   string
	KeyID   int64
	Version int // 0: the current version
	Err     error
}

func (e *KeyUnavailableError) Error() string {
	why := "is unavailable on this node: " + e.Err.Error()
	switch {
	case errors.Is(e.Err, keycache.ErrDestroyed):
		why = "has been destroyed"
	case errors.Is(e.Err, keycache.ErrDisabled):
		why = "is disabled"
	case errors.Is(e.Err, keycache.ErrNotFound):
		why = "does not exist"
	case errors.Is(e.Err, keycache.ErrNoVersion):
		why = fmt.Sprintf("has no version %d", e.Version)
	}
	return fmt.Sprintf("table %q: its encryption key %s; its sealed rows cannot be read or written", e.Table, why)
}

func (e *KeyUnavailableError) Unwrap() error { return e.Err }

// CorruptError is a sealed table's value that is not a sealed value of
// that row: malformed, failing authentication, or not sealed at all.
type CorruptError struct {
	Table  string
	Key    keys.Key
	Reason string
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("table %q: the value at %s %s", e.Table, e.Key, e.Reason)
}

// rowKeySuffix is key after the table's prefix.
func rowKeySuffix(desc *catalog.TableDescriptor, key keys.Key) ([]byte, error) {
	p := keys.TableDataPrefix(desc.ID)
	if !bytes.HasPrefix(key, p) {
		return nil, fmt.Errorf("table %q: key %s is not in the table", desc.Name, key)
	}
	return key[len(p):], nil
}

// passThrough reports an error a statement must see as it is: the
// statement's own cancellation or deadline.
func passThrough(ctx context.Context, err error) bool {
	return ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
}

// SealValue seals plain, the row value at key, under the row's key's
// current version. A table that is not sealed gets plain back.
func (b *Boundary) SealValue(ctx context.Context, desc *catalog.TableDescriptor, key keys.Key, plain []byte) ([]byte, error) {
	if !desc.Sealed() {
		return plain, nil
	}
	if b == nil {
		return nil, &KeyUnavailableError{Table: desc.Name, KeyID: desc.EncryptionKey.KeyID, Err: errors.New("this node has no key store")}
	}
	suffix, err := rowKeySuffix(desc, key)
	if err != nil {
		return nil, err
	}
	id, err := b.resolver.Resolve(ctx, desc, key, true)
	if err != nil {
		return nil, err
	}
	k, err := b.keys.Current(ctx, id)
	if err != nil {
		if passThrough(ctx, err) {
			return nil, err
		}
		return nil, &KeyUnavailableError{Table: desc.Name, KeyID: id, Err: err}
	}
	return Seal(k.AEAD, k.Version, AD(desc.EncryptionKey.SealID, id, k.Version, suffix), plain)
}

// OpenValue opens value, the stored value at key. A table that is not
// sealed gets value back. A sealed table's value must be sealed: this
// version writes no other kind, so a plain one is corruption (the
// rewrite job that seals an existing table, later, relaxes this).
func (b *Boundary) OpenValue(ctx context.Context, desc *catalog.TableDescriptor, key keys.Key, value []byte) ([]byte, error) {
	if !desc.Sealed() {
		return value, nil
	}
	if !IsSealed(value) {
		return nil, &CorruptError{Table: desc.Name, Key: key, Reason: "is not sealed, and the table is"}
	}
	v, err := Version(value)
	if err != nil {
		return nil, &CorruptError{Table: desc.Name, Key: key, Reason: "is malformed"}
	}
	if b == nil {
		return nil, &KeyUnavailableError{Table: desc.Name, KeyID: desc.EncryptionKey.KeyID, Version: v, Err: errors.New("this node has no key store")}
	}
	suffix, err := rowKeySuffix(desc, key)
	if err != nil {
		return nil, err
	}
	id, err := b.resolver.Resolve(ctx, desc, key, false)
	if err != nil {
		return nil, err
	}
	k, err := b.keys.Version(ctx, id, v)
	if err != nil {
		if passThrough(ctx, err) {
			return nil, err
		}
		return nil, &KeyUnavailableError{Table: desc.Name, KeyID: id, Version: v, Err: err}
	}
	plain, err := Open(k.AEAD, AD(desc.EncryptionKey.SealID, id, v, suffix), value)
	if err != nil {
		return nil, &CorruptError{Table: desc.Name, Key: key, Reason: "does not authenticate as this row's sealed value"}
	}
	return plain, nil
}

// PlaintextColumns is the set of a table's columns that sealing does not
// cover: the primary-key columns and every indexed column, whose values
// are in KV keys (and, for a vector index, in its entries' values). For
// a sealed table every other column is sealed.
func PlaintextColumns(desc *catalog.TableDescriptor) map[catalog.ColumnID]bool {
	out := map[catalog.ColumnID]bool{}
	for _, id := range desc.PrimaryKey {
		out[id] = true
	}
	for _, idx := range desc.Indexes {
		for _, id := range idx.ColumnIDs {
			out[id] = true
		}
	}
	return out
}
