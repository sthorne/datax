package rowseal

import (
	"container/list"
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// Per-tenant sealed tables (issue #310, step 4; CREATE TABLE ... WITH
// (encryption_key_by = '<column>')). The column leads the primary key,
// and each of its values — a tenant — has a key of its own, of kind
// tenant, made on the tenant's first write.
//
// # The sealed form of a per-tenant table
//
//	marker (1 byte, rowenc.SealedValueMarker)
//	key id (8 bytes, big-endian)
//	key version (uvarint)
//	nonce (12 bytes, random)
//	ciphertext ‖ tag (AES-256-GCM, 16-byte tag)
//
// The value names its key, so a read needs no lookup of the tenant's key:
// it opens the value under the key it names (a cache hit once the key is
// cached), and a row sealed under a tenant key that was destroyed fails
// with that key's id even after the tenant has a fresh key. The associated
// data is a whole table's (AD), which binds the key id too: a value moved
// to another row, or presented under another key, does not open. The
// overhead is 38 bytes per value.
//
// # Naming a tenant's key
//
// A tenant's key is the named key
//
//	tenant/<seal id>/<hex HMAC-SHA256(naming key, label ‖ 0 ‖ seal id ‖ value)>
//
// where value is the tenant column's key encoding (the bytes KV tells
// tenants apart by) and the naming key is the table's own key (its
// descriptor's KeyID, of kind table, version 1), which seals nothing. The
// name is found through datax_keys' unique index on names — one point
// read however many tenants there are — and is the same on every gateway,
// so concurrent first writes of one tenant converge on one key: the
// creation that loses the race on the name finds the winner's key. The
// digest is keyed so that the names, which admins can list, do not let
// anyone test a guessed tenant value without the cluster's keys; once the
// table is dropped (which destroys the naming key) no one can. Destroying
// a tenant's key renames it out of the way (pkg/keystore, DestroyedName),
// so the tenant's next write makes a fresh key.

// TenantOverhead is the bytes sealing adds to a value of a per-tenant
// table under key version v.
func TenantOverhead(v int) int { return 8 + Overhead(v) }

// SealTenant encrypts plain under aead (version v of key id) with
// associated data ad, in the per-tenant form.
func SealTenant(aead cipher.AEAD, id int64, v int, ad, plain []byte) ([]byte, error) {
	if aead.NonceSize() != nonceLen || aead.Overhead() != tagLen {
		return nil, errors.New("rowseal: the key is not AES-GCM")
	}
	if v <= 0 || id == 0 {
		return nil, fmt.Errorf("rowseal: key %d version %d", id, v)
	}
	out := make([]byte, 0, TenantOverhead(v)+len(plain))
	out = append(out, Marker)
	out = binary.BigEndian.AppendUint64(out, uint64(id))
	out = encoding.EncodeUvarint(out, uint64(v))
	at := len(out)
	out = out[:at+nonceLen]
	if _, err := rand.Read(out[at:]); err != nil {
		return nil, err
	}
	return aead.Seal(out, out[at:at+nonceLen], plain, ad), nil
}

// TenantKeyOf returns the key id and version a per-tenant sealed value is
// under.
func TenantKeyOf(sealed []byte) (id int64, version int, err error) {
	id, version, _, err = splitTenant(sealed)
	return id, version, err
}

func splitTenant(sealed []byte) (id int64, version int, body []byte, err error) {
	if !IsSealed(sealed) || len(sealed) < 1+8 {
		return 0, 0, nil, ErrMalformed
	}
	id = int64(binary.BigEndian.Uint64(sealed[1:9]))
	rest, v, err := encoding.DecodeUvarint(sealed[9:])
	if err != nil || v == 0 || v > 1<<31 || id <= 0 || len(rest) < nonceLen+tagLen {
		return 0, 0, nil, ErrMalformed
	}
	return id, int(v), rest, nil
}

// OpenTenant decrypts a per-tenant sealed value under aead with
// associated data ad.
func OpenTenant(aead cipher.AEAD, ad, sealed []byte) ([]byte, error) {
	_, _, body, err := splitTenant(sealed)
	if err != nil {
		return nil, err
	}
	plain, err := aead.Open(nil, body[:nonceLen], body[nonceLen:], ad)
	if err != nil {
		return nil, errAuth
	}
	return plain, nil
}

// TenantColumnFamilies are the types a tenant column may have: the ones
// an application identifies tenants by. Each is a self-delimiting key
// encoding whose bytes tell two values apart exactly when SQL does.
var TenantColumnFamilies = []types.Family{types.Int, types.String, types.Bytes, types.Uuid}

// TenantColumnTypeOK reports whether a column of family f can be a
// tenant column.
func TenantColumnTypeOK(f types.Family) bool {
	for _, ok := range TenantColumnFamilies {
		if f == ok {
			return true
		}
	}
	return false
}

// TenantOf returns the tenant of a row key of a per-tenant table: the key
// encoding of its leading primary-key column.
func TenantOf(desc *catalog.TableDescriptor, rowKey keys.Key) ([]byte, error) {
	enc, _, err := rowenc.LeadingPKColumn(desc, rowKey)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", desc.Name, err)
	}
	return enc, nil
}

// TenantValue returns the tenant a value of the tenant column is: its key
// encoding, the value coerced to the column's type first. It is what
// datax_destroy_tenant_key names a tenant by.
func TenantValue(desc *catalog.TableDescriptor, v types.Datum) ([]byte, error) {
	if !desc.SealedPerTenant() {
		return nil, fmt.Errorf("table %q is not sealed per tenant", desc.Name)
	}
	col, ok := desc.ColByID(desc.EncryptionKey.TenantColumn)
	if !ok {
		return nil, fmt.Errorf("table %q: its tenant column %d is gone", desc.Name, desc.EncryptionKey.TenantColumn)
	}
	if v.Null {
		return nil, fmt.Errorf("a tenant is never NULL")
	}
	k, err := rowenc.AppendKeyDatum(nil, col.Type, v)
	if err != nil {
		return nil, fmt.Errorf("the tenant %q is not a value of column %q (%s): %w", v.Text(), col.Name, col.Type, err)
	}
	return k, nil
}

// TenantNameLabel separates the naming key's digests from any other use
// of a key's material.
const TenantNameLabel = "datax tenant key name v1"

// TenantKeyName is the name of a tenant's key: the prefix of the table's
// seal id and the digest of its tenant (TenantNamePrefix).
func TenantKeyName(sealID uint64, digest []byte) string {
	return TenantNamePrefix(sealID) + hex.EncodeToString(digest)
}

// TenantNamePrefix is the prefix every tenant key of a table's seal id is
// named under (pkg/keystore.TenantKeyPrefix, the seal id, a slash).
func TenantNamePrefix(sealID uint64) string {
	return "tenant/" + strconv.FormatUint(sealID, 10) + "/"
}

// KeyMACer is a Keys that can also give a keyed digest under a key
// (keystore.Cache.MAC): what names tenants' keys.
type KeyMACer interface {
	MAC(ctx context.Context, id int64, version int, label string, data []byte) ([]byte, error)
}

// TenantKeyStore is a KeyStore that also keeps tenants' keys.
type TenantKeyStore interface {
	// TenantKey returns the id of the tenant key named name; with
	// create, it makes the key when there is none (in a transaction of its
	// own), and two callers creating one name concurrently get the same
	// key. Without create, a missing key is ErrKeyNotFound.
	TenantKey(ctx context.Context, name string, create bool) (int64, error)
	// DestroyTenantKey destroys the live key named name and purges its
	// record's history; with none, it destroys again (purging again) the
	// keys the name had before, and reports them as already destroyed.
	// ErrKeyNotFound: the name never had a key.
	DestroyTenantKey(ctx context.Context, name string) (TenantKeyDestroyed, error)
	// DestroyTenantKeys arranges for every tenant key of a seal id to be
	// destroyed, in the background (a dropped table's).
	DestroyTenantKeys(sealID uint64)
}

// TenantKeyDestroyed is what DestroyTenantKey did.
type TenantKeyDestroyed struct {
	// KeyID is the key destroyed (the most recent, when the name had
	// several before).
	KeyID int64
	// Already: the name had no live key; KeyID was destroyed before.
	Already bool
	// PurgeIncomplete, when set, says which replicas did not finish
	// purging the key's record's history: the key is destroyed, and
	// destroying again retries the purge.
	PurgeIncomplete string
}

// DefaultTenantCacheSize is how many tenants' key ids a gateway keeps by
// default (each a few dozen bytes; the keys' material is in the bounded
// key cache).
const DefaultTenantCacheSize = 65536

// TenantResolver is the Resolver of per-tenant tables: it maps a row's
// tenant to the tenant's key, through a bounded cache of key ids, and
// makes the key on a tenant's first write. A cached mapping needs no
// lease: a tenant's key id changes only when the key is destroyed, which
// the key cache's lease check reports (ErrKeyDestroyed), and the boundary
// then forgets the mapping and resolves again.
type TenantResolver struct {
	keys  Keys
	store KeyStore

	mu  sync.Mutex
	max int
	ids map[string]*list.Element
	lru *list.List // of *tenantEntry, most recent at the front
}

type tenantEntry struct {
	k  string
	id int64
}

// NewTenantResolver returns a resolver over the gateway's keys and key
// store, keeping at most size tenants' key ids (0:
// DefaultTenantCacheSize).
func NewTenantResolver(k Keys, store KeyStore, size int) *TenantResolver {
	if size <= 0 {
		size = DefaultTenantCacheSize
	}
	return &TenantResolver{keys: k, store: store, max: size, ids: map[string]*list.Element{}, lru: list.New()}
}

// Len is how many tenants' key ids the resolver holds.
func (r *TenantResolver) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ids)
}

func cacheKey(sealID uint64, tenant []byte) string {
	b := make([]byte, 0, 8+len(tenant))
	b = binary.BigEndian.AppendUint64(b, sealID)
	return string(append(b, tenant...))
}

func (r *TenantResolver) lookup(k []byte) (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.ids[string(k)]; ok {
		r.lru.MoveToFront(e)
		return e.Value.(*tenantEntry).id, true
	}
	return 0, false
}

func (r *TenantResolver) put(k string, id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.ids[k]; ok {
		e.Value.(*tenantEntry).id = id
		r.lru.MoveToFront(e)
		return
	}
	r.ids[k] = r.lru.PushFront(&tenantEntry{k: k, id: id})
	for len(r.ids) > r.max {
		last := r.lru.Back()
		delete(r.ids, last.Value.(*tenantEntry).k)
		r.lru.Remove(last)
	}
}

// ForgetTenant drops the cached key id of a tenant (its encoding).
func (r *TenantResolver) ForgetTenant(desc *catalog.TableDescriptor, tenant []byte) {
	k := cacheKey(desc.EncryptionKey.SealID, tenant)
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.ids[k]; ok {
		delete(r.ids, k)
		r.lru.Remove(e)
	}
}

// Name returns the name of the key of a tenant (its encoding) of a
// per-tenant table.
func (r *TenantResolver) Name(ctx context.Context, desc *catalog.TableDescriptor, tenant []byte) (string, error) {
	m, ok := r.keys.(KeyMACer)
	if !ok {
		return "", errors.New("this node's keys cannot name tenants' keys")
	}
	data := make([]byte, 0, 8+len(tenant))
	data = binary.BigEndian.AppendUint64(data, desc.EncryptionKey.SealID)
	data = append(data, tenant...)
	digest, err := m.MAC(ctx, desc.EncryptionKey.KeyID, 1, TenantNameLabel, data)
	if err != nil {
		if passThrough(ctx, err) {
			return "", err
		}
		return "", &KeyUnavailableError{Table: desc.Name, KeyID: desc.EncryptionKey.KeyID, Version: 1, Err: err, Naming: true}
	}
	return TenantKeyName(desc.EncryptionKey.SealID, digest), nil
}

// Resolve implements Resolver: the key of the row's tenant, made when
// forWrite and the tenant has none.
func (r *TenantResolver) Resolve(ctx context.Context, desc *catalog.TableDescriptor, rowKey keys.Key, forWrite bool) (int64, error) {
	if !desc.SealedPerTenant() {
		return 0, fmt.Errorf("table %q is not sealed per tenant", desc.Name)
	}
	tenant, err := TenantOf(desc, rowKey)
	if err != nil {
		return 0, err
	}
	return r.resolveTenant(ctx, desc, tenant, forWrite)
}

func (r *TenantResolver) resolveTenant(ctx context.Context, desc *catalog.TableDescriptor, tenant []byte, forWrite bool) (int64, error) {
	var buf [64]byte
	ck := binary.BigEndian.AppendUint64(buf[:0], desc.EncryptionKey.SealID)
	ck = append(ck, tenant...)
	if id, ok := r.lookup(ck); ok {
		return id, nil
	}
	k := string(ck)
	ts, ok := r.store.(TenantKeyStore)
	if !ok || r.store == nil {
		return 0, &KeyUnavailableError{Table: desc.Name, Tenant: true, Err: errors.New("this node keeps no tenant keys")}
	}
	name, err := r.Name(ctx, desc, tenant)
	if err != nil {
		return 0, err
	}
	id, err := ts.TenantKey(ctx, name, forWrite)
	if err != nil {
		if passThrough(ctx, err) {
			return 0, err
		}
		return 0, &KeyUnavailableError{Table: desc.Name, Tenant: true, Err: err}
	}
	r.put(k, id)
	return id, nil
}

// sealTenant seals a per-tenant table's row value under its tenant's key,
// resolving the tenant again once if the cached key turns out to be
// destroyed (the tenant's next key is made then).
func (b *Boundary) sealTenant(ctx context.Context, desc *catalog.TableDescriptor, key keys.Key, suffix, plain []byte) ([]byte, error) {
	tenant, err := TenantOf(desc, key)
	if err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		id, err := b.tenant.resolveTenant(ctx, desc, tenant, true)
		if err != nil {
			return nil, err
		}
		k, err := b.keys.Current(ctx, id)
		if err != nil {
			if passThrough(ctx, err) {
				return nil, err
			}
			if attempt == 0 && (errors.Is(err, ErrKeyDestroyed) || errors.Is(err, ErrKeyNotFound)) {
				b.tenant.ForgetTenant(desc, tenant)
				continue
			}
			return nil, &KeyUnavailableError{Table: desc.Name, KeyID: id, Err: err, Tenant: true}
		}
		return SealTenant(k.AEAD, id, k.Version, AD(desc.EncryptionKey.SealID, id, k.Version, suffix), plain)
	}
}

// openTenant opens a per-tenant table's row value under the key it
// names.
func (b *Boundary) openTenant(ctx context.Context, desc *catalog.TableDescriptor, key keys.Key, suffix, value []byte) ([]byte, error) {
	id, v, err := TenantKeyOf(value)
	if err != nil {
		return nil, &CorruptError{Table: desc.Name, Key: key, Reason: "is malformed"}
	}
	k, err := b.keys.Version(ctx, id, v)
	if err != nil {
		if passThrough(ctx, err) {
			return nil, err
		}
		return nil, &KeyUnavailableError{Table: desc.Name, KeyID: id, Version: v, Err: err, Tenant: true}
	}
	plain, err := OpenTenant(k.AEAD, AD(desc.EncryptionKey.SealID, id, v, suffix), value)
	if err != nil {
		return nil, &CorruptError{Table: desc.Name, Key: key, Reason: "does not authenticate as this row's sealed value"}
	}
	return plain, nil
}

// Tenants is the boundary's resolver of per-tenant tables (nil on a nil
// boundary).
func (b *Boundary) Tenants() *TenantResolver {
	if b == nil {
		return nil
	}
	return b.tenant
}
