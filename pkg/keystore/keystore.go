// Package keystore holds the named keys of the key service (issue #310,
// docs/design-key-service.md): keys an application asks datax to keep,
// and later the keys of sealed tables and of their tenants. They live in
// the datax_keys system table (pkg/sql/keystable.go), one row per key,
// each version of the key's material sealed by the cluster key
// (wrap.go); this package is the only reader and writer of that table.
//
// What a later step builds on:
//
//   - Create makes a key of a kind under a name; Get, GetByID and List
//     read metadata, never material.
//   - Unwrap returns one version of a key's material in held memory
//     (enc.HoldKey). Rotate adds a version; older versions still unwrap.
//   - Disable and Enable switch a key off and on; Destroy tombstones its
//     material and purges the row's history, so it does not wait out the
//     GC window.
//   - Every change to a key's state bumps its Generation. A gateway that
//     caches a key's material checks the generation it cached against the
//     row within its descriptor lease (10 seconds) and drops the cached
//     material when it has moved: that is how disabling or destroying a
//     key reaches every gateway within one lease (design, "Key cache and
//     revocation"). The cache is Cache (cache.go).
//
// The cluster key is the server's (pkg/server/clusterkey.go); it reaches
// this package through ClusterKeys.
package keystore

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// Kind is what a named key is for.
type Kind string

const (
	// KindWrapping is a key of the key service: it wraps an
	// application's data keys.
	KindWrapping Kind = "wrapping"
	// KindTable is the key of one sealed table.
	KindTable Kind = "table"
	// KindTenant is the key of one tenant's rows in a sealed table.
	KindTenant Kind = "tenant"
)

func (k Kind) valid() bool { return k == KindWrapping || k == KindTable || k == KindTenant }

// State is where a named key stands.
type State string

const (
	StateEnabled   State = "enabled"
	StateDisabled  State = "disabled"
	StateDestroyed State = "destroyed"
)

// MaxNameLen bounds a key's name.
const MaxNameLen = 256

var (
	// ErrNotFound: no key has that name or id.
	ErrNotFound = errors.New("no such key")
	// ErrExists: a key of that name exists (destroyed keys keep their
	// names).
	ErrExists = errors.New("a key of that name exists")
	// ErrDisabled: the key is disabled.
	ErrDisabled = errors.New("the key is disabled")
	// ErrDestroyed: the key has been destroyed; its material is gone.
	ErrDestroyed = errors.New("the key has been destroyed")
	// ErrNoVersion: the key has no such version.
	ErrNoVersion = errors.New("the key has no such version")
	// ErrVersionDestroyed: the version existed and was destroyed
	// (DestroyVersions), after a rotation; the key's later versions
	// still unwrap.
	ErrVersionDestroyed = errors.New("that version of the key has been destroyed")
)

// ClusterKeys gives the store the cluster key. Keys returned are copies
// the store zeroes when done with.
type ClusterKeys interface {
	// Current is the cluster key new and rewrapped material is sealed
	// under.
	Current(ctx context.Context) (enc.ClusterKeyRef, []byte, error)
	// Key is the cluster key ref names: the current one, one a rotation
	// is leaving, or one imported from another cluster.
	Key(ctx context.Context, ref enc.ClusterKeyRef) ([]byte, error)
}

// Config is what a Store needs.
type Config struct {
	DB          *kvclient.DB
	Catalog     *catalog.Accessor
	ClusterKeys ClusterKeys
	// Ready refuses Create until the cluster key is enabled — its
	// replicated record exists — with the reason; nil allows always.
	Ready func(ctx context.Context) error
	// Accept is checked inside every transaction that writes material
	// sealed under ref, so the write is ordered against a change of the
	// cluster key's record (a rotation retiring a version); nil accepts.
	Accept func(ctx context.Context, txn *kvclient.Txn, ref enc.ClusterKeyRef) error
	// Now is the clock for created and rotated times; nil is time.Now.
	Now func() time.Time
}

// Store reads and writes datax_keys.
type Store struct {
	cfg Config
}

// listRowsRead counts the datax_keys entries the listings of every store
// of the process read (rows and name index entries): ListRowsRead.
var listRowsRead atomic.Int64

// ListRowsRead is how many datax_keys entries — rows and name index
// entries — the listings (ListIn, WrappingKeysIn) of every store of the
// process have read since it started: for tests and diagnostics that
// check a listing's cost does not grow with the number of tenant keys.
func ListRowsRead() int64 { return listRowsRead.Load() }

// New returns a store.
func New(cfg Config) *Store {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Store{cfg: cfg}
}

// VersionInfo is one version of a key, without its material.
type VersionInfo struct {
	Version int       `json:"version"`
	Created time.Time `json:"created"`
}

// Key is a named key's metadata. It never carries material.
type Key struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Kind       Kind   `json:"kind"`
	State      State  `json:"state"`
	Version    int    `json:"version"`
	Generation int64  `json:"generation"`
	// ClusterKey is the cluster key the material is sealed under now;
	// zero once the key is destroyed.
	ClusterKey enc.ClusterKeyRef `json:"cluster_key"`
	Versions   []VersionInfo     `json:"versions,omitempty"`
	Created    time.Time         `json:"created"`
	Rotated    time.Time         `json:"rotated,omitempty"`
	// ExternalRoot is reserved for a per-key external root (design,
	// "Erasure"); always empty in this version.
	ExternalRoot string `json:"external_root,omitempty"`
	// Owner is the role that owns the key (empty: root); Privileges maps
	// a grantee to what it was granted on the key (PrivWrap, PrivUnwrap).
	// The key service's (service.go); keys of sealed tables have neither.
	Owner      string              `json:"owner,omitempty"`
	Privileges map[string][]string `json:"privileges,omitempty"`
}

// Material is one version of a key's material, in held memory. Release
// zeroes it.
type Material struct {
	ID         int64
	Name       string
	Kind       Kind
	Version    int
	Generation int64
	Key        []byte
}

// Release zeroes and gives back the material.
func (m *Material) Release() {
	if m != nil {
		enc.ReleaseKey(m.Key)
		m.Key = nil
	}
}

// row is one datax_keys row decoded.
type row struct {
	Key
	versions []sealedVersion
}

// descIn resolves the table in txn, part of the caller's transaction
// (see sql.ReadKeysTableIn); nil txn is desc(ctx, create). In a
// transaction the table is neither created nor upgraded: those are
// statements of their own. A table from before the access columns
// cannot take an owner or a grant, so a change to one is refused.
func (s *Store) descIn(ctx context.Context, txn *kvclient.Txn, create bool) (*catalog.TableDescriptor, error) {
	if txn == nil {
		return s.desc(ctx, create)
	}
	d, err := sql.ReadKeysTableIn(ctx, txn, s.cfg.Catalog)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, ErrNotFound
	}
	if create {
		if _, ok := d.Col("privileges"); !ok {
			return nil, fmt.Errorf("the %s table predates key owners and grants: change a key's access outside a transaction once, to upgrade it", catalog.KeysTableName)
		}
	}
	return d, nil
}

func (s *Store) desc(ctx context.Context, create bool) (*catalog.TableDescriptor, error) {
	if create {
		return sql.EnsureKeysTable(ctx, s.cfg.DB, s.cfg.Catalog)
	}
	d, err := sql.LookupKeysTable(ctx, s.cfg.DB, s.cfg.Catalog)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, ErrNotFound
	}
	return d, nil
}

func nameIndex(d *catalog.TableDescriptor) (*catalog.IndexDescriptor, error) {
	for i := range d.Indexes {
		idx := &d.Indexes[i]
		if idx.Unique && len(idx.ColumnIDs) == 1 && idx.ColumnIDs[0] == sql.KeysColName {
			return idx, nil
		}
	}
	return nil, fmt.Errorf("the %s table has no index on its names", catalog.KeysTableName)
}

func pkKey(d *catalog.TableDescriptor, id int64) (keys.Key, error) {
	return rowenc.EncodePK(d, []types.Datum{types.NewInt(id)})
}

func nanosToTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// decodeRow reads one row of the table.
func decodeRow(d *catalog.TableDescriptor, k keys.Key, v []byte) (*row, error) {
	pk, err := rowenc.DecodePK(d, k)
	if err != nil {
		return nil, err
	}
	vals, err := rowenc.DecodeValue(d, v)
	if err != nil {
		return nil, err
	}
	r := &row{Key: Key{
		ID:           pk[0].I,
		Name:         vals[sql.KeysColName].S,
		Kind:         Kind(vals[sql.KeysColKind].S),
		State:        State(vals[sql.KeysColState].S),
		Version:      int(vals[sql.KeysColVersion].I),
		Generation:   vals[sql.KeysColGeneration].I,
		Created:      nanosToTime(vals[sql.KeysColCreated].I),
		Rotated:      nanosToTime(vals[sql.KeysColRotated].I),
		ExternalRoot: vals[sql.KeysColExternalRoot].S,
		Owner:        vals[sql.KeysColOwner].S,
	}}
	if raw := vals[sql.KeysColPrivileges].S; raw != "" {
		if err := json.Unmarshal([]byte(raw), &r.Privileges); err != nil {
			return nil, fmt.Errorf("key %d: corrupt privileges: %w", r.ID, err)
		}
	}
	if ck := vals[sql.KeysColClusterKey].S; ck != "" {
		if r.ClusterKey, err = enc.ParseClusterKeyRef(ck); err != nil {
			return nil, fmt.Errorf("key %d: %w", r.ID, err)
		}
	}
	if r.versions, err = decodeVersions([]byte(vals[sql.KeysColMaterial].S)); err != nil {
		return nil, fmt.Errorf("key %d: %w", r.ID, err)
	}
	for _, sv := range r.versions {
		r.Versions = append(r.Versions, VersionInfo{Version: sv.Version, Created: nanosToTime(sv.Created)})
	}
	return r, nil
}

// encode renders r as the row's value.
func (r *row) encode(d *catalog.TableDescriptor) ([]byte, error) {
	vals := map[catalog.ColumnID]types.Datum{
		sql.KeysColID:         types.NewInt(r.ID),
		sql.KeysColName:       types.NewString(r.Name),
		sql.KeysColKind:       types.NewString(string(r.Kind)),
		sql.KeysColVersion:    types.NewInt(int64(r.Version)),
		sql.KeysColVersions:   types.NewInt(int64(len(r.versions))),
		sql.KeysColState:      types.NewString(string(r.State)),
		sql.KeysColGeneration: types.NewInt(r.Generation),
		sql.KeysColCreated:    types.NewTimestamp(r.Created.UnixNano()),
	}
	if !r.ClusterKey.IsZero() {
		vals[sql.KeysColClusterKey] = types.NewString(r.ClusterKey.String())
	}
	if len(r.versions) > 0 {
		raw, err := encodeVersions(r.versions)
		if err != nil {
			return nil, err
		}
		vals[sql.KeysColMaterial] = types.NewBytes(raw)
	}
	if !r.Rotated.IsZero() {
		vals[sql.KeysColRotated] = types.NewTimestamp(r.Rotated.UnixNano())
	}
	if r.ExternalRoot != "" {
		vals[sql.KeysColExternalRoot] = types.NewString(r.ExternalRoot)
	}
	if r.Owner != "" {
		vals[sql.KeysColOwner] = types.NewString(r.Owner)
	}
	if len(r.Privileges) > 0 {
		raw, err := json.Marshal(r.Privileges)
		if err != nil {
			return nil, err
		}
		vals[sql.KeysColPrivileges] = types.NewString(string(raw))
	}
	return rowenc.EncodeValue(d, vals)
}

func (r *row) indexEntry(d *catalog.TableDescriptor) (rowenc.IndexEntry, error) {
	idx, err := nameIndex(d)
	if err != nil {
		return rowenc.IndexEntry{}, err
	}
	es, err := rowenc.IndexEntries(d, idx, map[catalog.ColumnID]types.Datum{
		sql.KeysColID:   types.NewInt(r.ID),
		sql.KeysColName: types.NewString(r.Name),
	})
	if err != nil {
		return rowenc.IndexEntry{}, err
	}
	if len(es) != 1 {
		return rowenc.IndexEntry{}, fmt.Errorf("the name index gave %d entries for one key", len(es))
	}
	return es[0], nil
}

func newKeyID() (int64, error) {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, err
		}
		if id := int64(binary.BigEndian.Uint64(b[:]) >> 1); id != 0 {
			return id, nil
		}
	}
}

func validName(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("a key needs a name")
	}
	if len(name) > MaxNameLen {
		return fmt.Errorf("a key's name is at most %d bytes", MaxNameLen)
	}
	return nil
}

// Create makes a key of kind under name, with a first version of fresh
// material sealed under the current cluster key.
func (s *Store) Create(ctx context.Context, kind Kind, name string) (Key, error) {
	return s.CreateOwned(ctx, kind, name, "")
}

// CreateOwned is Create with the key owned by owner (empty: root).
func (s *Store) CreateOwned(ctx context.Context, kind Kind, name, owner string) (Key, error) {
	if err := s.preCreate(ctx, kind, name); err != nil {
		return Key{}, err
	}
	var out Key
	err := s.cfg.DB.RunTxn(ctx, "keys-create", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		out, err = s.createIn(ctx, txn, kind, name, owner)
		return err
	})
	return out, err
}

// CreateIn is Create in the caller's transaction, so that the key exists
// exactly when what it was made for does (a sealed table's key commits
// with its CREATE TABLE).
func (s *Store) CreateIn(ctx context.Context, txn *kvclient.Txn, kind Kind, name string) (Key, error) {
	if err := s.preCreate(ctx, kind, name); err != nil {
		return Key{}, err
	}
	return s.createIn(ctx, txn, kind, name, "")
}

// preCreate is what Create checks and prepares outside any transaction:
// the arguments, the cluster key being enabled, and the table existing.
func (s *Store) preCreate(ctx context.Context, kind Kind, name string) error {
	if !kind.valid() {
		return fmt.Errorf("unknown kind of key %q (want %s, %s or %s)", kind, KindWrapping, KindTable, KindTenant)
	}
	if err := validName(name); err != nil {
		return err
	}
	if s.cfg.Ready != nil {
		if err := s.cfg.Ready(ctx); err != nil {
			return err
		}
	}
	_, err := s.desc(ctx, true)
	return err
}

func (s *Store) createIn(ctx context.Context, txn *kvclient.Txn, kind Kind, name, owner string) (Key, error) {
	d, err := s.descIn(ctx, txn, false)
	if err != nil {
		return Key{}, err
	}
	ref, ck, err := s.cfg.ClusterKeys.Current(ctx)
	if err != nil {
		return Key{}, err
	}
	defer enc.Zero(ck)
	id, err := newKeyID()
	if err != nil {
		return Key{}, err
	}
	material, err := enc.NewStoreKey()
	if err != nil {
		return Key{}, err
	}
	sealed, err := WrapMaterial(ck, id, name, 1, kind, material)
	enc.Zero(material)
	if err != nil {
		return Key{}, err
	}
	now := s.cfg.Now().UTC()
	r := &row{Key: Key{ID: id, Name: name, Kind: kind, State: StateEnabled, Version: 1, Generation: 1, ClusterKey: ref, Created: now, Owner: owner}}
	r.versions = []sealedVersion{{Version: 1, Sealed: sealed, Created: now.UnixNano()}}
	val, err := r.encode(d)
	if err != nil {
		return Key{}, err
	}
	ie, err := r.indexEntry(d)
	if err != nil {
		return Key{}, err
	}
	pk, err := pkKey(d, id)
	if err != nil {
		return Key{}, err
	}
	got, err := txn.GetBatch(ctx, []keys.Key{ie.Key, pk})
	if err != nil {
		return Key{}, err
	}
	if got[0] != nil {
		return Key{}, ErrExists
	}
	if got[1] != nil {
		return Key{}, fmt.Errorf("key id %d is taken; retry", id)
	}
	if err := s.accept(ctx, txn, ref); err != nil {
		return Key{}, err
	}
	var wb kvclient.WriteBatch
	wb.Put(pk, val)
	wb.Put(ie.Key, ie.Value)
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return Key{}, err
	}
	return r.Key, nil
}

// read reads one row in txn; ErrNotFound when it is not there.
func (s *Store) read(ctx context.Context, txn *kvclient.Txn, d *catalog.TableDescriptor, id int64, forUpdate bool) (*row, keys.Key, error) {
	pk, err := pkKey(d, id)
	if err != nil {
		return nil, nil, err
	}
	var v []byte
	if forUpdate {
		v, err = txn.GetForUpdate(ctx, pk)
	} else {
		v, err = txn.Get(ctx, pk)
	}
	if err != nil {
		return nil, nil, err
	}
	if v == nil {
		return nil, nil, ErrNotFound
	}
	r, err := decodeRow(d, pk, v)
	return r, pk, err
}

// GetByID returns a key's metadata.
func (s *Store) GetByID(ctx context.Context, id int64) (Key, error) {
	d, err := s.desc(ctx, false)
	if err != nil {
		return Key{}, err
	}
	var out Key
	err = s.cfg.DB.RunTxn(ctx, "keys-get", func(ctx context.Context, txn *kvclient.Txn) error {
		r, _, err := s.read(ctx, txn, d, id, false)
		if err != nil {
			return err
		}
		out = r.Key
		return nil
	})
	return out, err
}

// GetByIDs returns the metadata of the keys of ids that exist, read in
// one transaction and one batch: what a gateway's cache checks the keys
// it is using with (BatchSource).
func (s *Store) GetByIDs(ctx context.Context, ids []int64) (map[int64]Key, error) {
	d, err := s.desc(ctx, false)
	if err != nil {
		return nil, err
	}
	pks := make([]keys.Key, len(ids))
	for i, id := range ids {
		if pks[i], err = pkKey(d, id); err != nil {
			return nil, err
		}
	}
	out := map[int64]Key{}
	err = s.cfg.DB.RunTxn(ctx, "keys-get-many", func(ctx context.Context, txn *kvclient.Txn) error {
		clear(out)
		vals, err := txn.GetBatch(ctx, pks)
		if err != nil {
			return err
		}
		for i, v := range vals {
			if v == nil {
				continue
			}
			r, err := decodeRow(d, pks[i], v)
			if err != nil {
				return err
			}
			out[r.ID] = r.Key
		}
		return nil
	})
	return out, err
}

// Get returns the metadata of the key named name.
func (s *Store) Get(ctx context.Context, name string) (Key, error) {
	d, err := s.desc(ctx, false)
	if err != nil {
		return Key{}, err
	}
	var out Key
	err = s.cfg.DB.RunTxn(ctx, "keys-get", func(ctx context.Context, txn *kvclient.Txn) error {
		idx, err := nameIndex(d)
		if err != nil {
			return err
		}
		vals := []types.Datum{types.NewString(name)}
		ik, err := rowenc.EncodeIndexPrefix(d, idx, vals)
		if err != nil {
			return err
		}
		iv, err := txn.Get(ctx, ik)
		if err != nil {
			return err
		}
		if iv == nil {
			return ErrNotFound
		}
		pk, err := rowenc.IndexEntryPrimaryKey(d, idx, ik, iv)
		if err != nil {
			return err
		}
		v, err := txn.Get(ctx, pk)
		if err != nil {
			return err
		}
		if v == nil {
			return ErrNotFound
		}
		r, err := decodeRow(d, pk, v)
		if err != nil {
			return err
		}
		out = r.Key
		return nil
	})
	return out, err
}

// List returns every key's metadata, in id order.
func (s *Store) List(ctx context.Context) ([]Key, error) {
	d, err := sql.LookupKeysTable(ctx, s.cfg.DB, s.cfg.Catalog)
	if err != nil || d == nil {
		return nil, err
	}
	var out []Key
	err = s.cfg.DB.RunTxn(ctx, "keys-list", func(ctx context.Context, txn *kvclient.Txn) error {
		out = nil
		lo, hi := rowenc.PrimarySpanFor(d)
		kvs, err := txn.Scan(ctx, lo, hi, 0)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			r, err := decodeRow(d, kv.Key, kv.Value)
			if err != nil {
				return err
			}
			out = append(out, r.Key)
		}
		return nil
	})
	return out, err
}

// Generation returns a key's generation: the counter every change of
// its state moves, which a cache of its material checks.
func (s *Store) Generation(ctx context.Context, id int64) (int64, error) {
	k, err := s.GetByID(ctx, id)
	return k.Generation, err
}

// Unwrap returns version of key id (0: its current version) in held
// memory. A disabled or destroyed key is refused.
func (s *Store) Unwrap(ctx context.Context, id int64, version int) (*Material, error) {
	d, err := s.desc(ctx, false)
	if err != nil {
		return nil, err
	}
	var r *row
	if err := s.cfg.DB.RunTxn(ctx, "keys-unwrap", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		r, _, err = s.read(ctx, txn, d, id, false)
		return err
	}); err != nil {
		return nil, err
	}
	switch r.State {
	case StateDestroyed:
		return nil, ErrDestroyed
	case StateDisabled:
		return nil, ErrDisabled
	}
	if version == 0 {
		version = r.Version
	}
	var sealed []byte
	for _, sv := range r.versions {
		if sv.Version == version {
			sealed = sv.Sealed
		}
	}
	if sealed == nil {
		if version > 0 && version < r.Version {
			// Versions are numbered from 1 and only DestroyVersions
			// takes one away.
			return nil, ErrVersionDestroyed
		}
		return nil, ErrNoVersion
	}
	ck, err := s.cfg.ClusterKeys.Key(ctx, r.ClusterKey)
	if err != nil {
		return nil, err
	}
	plain, err := UnwrapMaterial(ck, r.ID, r.Name, version, r.Kind, sealed)
	enc.Zero(ck)
	if err != nil {
		return nil, fmt.Errorf("key %d (%s) version %d: %w", r.ID, r.Name, version, err)
	}
	m := &Material{ID: r.ID, Name: r.Name, Kind: r.Kind, Version: version, Generation: r.Generation, Key: enc.HoldKey(plain)}
	enc.Zero(plain)
	return m, nil
}

// update reads key id for update, lets change alter it, and writes it
// back with its generation moved on. A change of the key's name moves
// its entry in the name index.
func (s *Store) update(ctx context.Context, name string, id int64, change func(r *row) error) (Key, error) {
	d, err := s.desc(ctx, true)
	if err != nil {
		return Key{}, err
	}
	var out Key
	err = s.cfg.DB.RunTxn(ctx, name, func(ctx context.Context, txn *kvclient.Txn) error {
		r, pk, err := s.read(ctx, txn, d, id, true)
		if err != nil {
			return err
		}
		was := r.Name
		if err := change(r); err != nil {
			return err
		}
		if r.Name != was {
			if err := s.rename(ctx, txn, d, r, was); err != nil {
				return err
			}
		}
		if !r.ClusterKey.IsZero() {
			if err := s.accept(ctx, txn, r.ClusterKey); err != nil {
				return err
			}
		}
		r.Generation++
		val, err := r.encode(d)
		if err != nil {
			return err
		}
		if err := txn.Put(ctx, pk, val); err != nil {
			return err
		}
		out = r.Key
		return nil
	})
	return out, err
}

// Rotate gives key id a new version of fresh material, which becomes its
// current version; older versions still unwrap. The whole row is sealed
// under the current cluster key on the way.
func (s *Store) Rotate(ctx context.Context, id int64) (Key, error) {
	ref, ck, err := s.cfg.ClusterKeys.Current(ctx)
	if err != nil {
		return Key{}, err
	}
	defer enc.Zero(ck)
	return s.update(ctx, "keys-rotate", id, func(r *row) error {
		if r.State == StateDestroyed {
			return ErrDestroyed
		}
		vs := r.versions
		if r.ClusterKey != ref {
			from, err := s.cfg.ClusterKeys.Key(ctx, r.ClusterKey)
			if err != nil {
				return err
			}
			vs, err = rewrapVersions(from, ck, r.ID, r.Name, r.Kind, vs)
			enc.Zero(from)
			if err != nil {
				return err
			}
		}
		material, err := enc.NewStoreKey()
		if err != nil {
			return err
		}
		next := r.Version + 1
		sealed, err := WrapMaterial(ck, r.ID, r.Name, next, r.Kind, material)
		enc.Zero(material)
		if err != nil {
			return err
		}
		now := s.cfg.Now().UTC()
		r.versions = append(append([]sealedVersion(nil), vs...), sealedVersion{Version: next, Sealed: sealed, Created: now.UnixNano()})
		r.Version, r.Rotated, r.ClusterKey = next, now, ref
		r.Versions = append(r.Versions, VersionInfo{Version: next, Created: now})
		return nil
	})
}

// Disable switches key id off: Unwrap refuses it until Enable.
func (s *Store) Disable(ctx context.Context, id int64) (Key, error) {
	return s.update(ctx, "keys-disable", id, func(r *row) error {
		if r.State == StateDestroyed {
			return ErrDestroyed
		}
		r.State = StateDisabled
		return nil
	})
}

// Enable switches a disabled key back on.
func (s *Store) Enable(ctx context.Context, id int64) (Key, error) {
	return s.update(ctx, "keys-enable", id, func(r *row) error {
		if r.State == StateDestroyed {
			return ErrDestroyed
		}
		r.State = StateEnabled
		return nil
	})
}

// rename moves key r's name index entry from was to r.Name, refusing a
// name another key holds.
func (s *Store) rename(ctx context.Context, txn *kvclient.Txn, d *catalog.TableDescriptor, r *row, was string) error {
	ie, err := r.indexEntry(d)
	if err != nil {
		return err
	}
	if v, err := txn.Get(ctx, ie.Key); err != nil {
		return err
	} else if v != nil {
		return ErrExists
	}
	old := &row{Key: Key{ID: r.ID, Name: was}}
	oe, err := old.indexEntry(d)
	if err != nil {
		return err
	}
	var wb kvclient.WriteBatch
	wb.Delete(oe.Key)
	wb.Put(ie.Key, ie.Value)
	return txn.RunBatch(ctx, &wb)
}

// The names of tenant keys (issue #310, step 4). A per-tenant sealed
// table's tenant keys are named TenantKeyPrefix, the table's seal id, a
// slash, and a keyed digest of the tenant's value (pkg/sql/rowseal makes
// the name; this package does not read it). A destroyed tenant key is
// renamed DestroyedPrefix + its name + "/" + its id, so that the name is
// free for the tenant's next key and so that the names of live tenant
// keys — what a scan for a table's tenant keys reads — are not crowded
// by tombstones.
const (
	TenantKeyPrefix = "tenant/"
	DestroyedPrefix = "destroyed/"
	// TableKeyPrefix begins the name of every sealed table's key (and a
	// per-tenant table's naming key): "table/" and the table's id when
	// it was created (pkg/sql/sealed.go).
	TableKeyPrefix = "table/"
)

// ReservedPrefixes are the name prefixes the keys of sealed tables and
// their tenants are named under. A key of the key service may not take
// a name under them (ReservedName), so that the key service's keys are
// the names outside them: what WrappingKeysIn reads, without reading a
// cluster's tenant keys, which may number millions. In name order.
var ReservedPrefixes = []string{DestroyedPrefix, TableKeyPrefix, TenantKeyPrefix}

// ReservedName reports whether name is under a reserved prefix.
func ReservedName(name string) bool {
	for _, p := range ReservedPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// DestroyedName is the name a destroyed tenant key named name takes.
func DestroyedName(name string, id int64) string {
	return DestroyedPrefix + name + "/" + strconv.FormatInt(id, 10)
}

// PrefixEnd is the first name after every name that begins with prefix
// (prefix with its last byte moved on): the end of a scan of KeysByName
// for the prefix. An empty prefix or one of 0xff bytes has no end ("").
func PrefixEnd(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

// KeysByName returns the metadata of the keys whose names are in [from,
// to), in name order, at most limit of them (limit > 0): a scan of the
// name index, so that a caller looking for the keys of one table or one
// tenant does not read every key.
func (s *Store) KeysByName(ctx context.Context, from, to string, limit int) ([]Key, error) {
	d, err := sql.LookupKeysTable(ctx, s.cfg.DB, s.cfg.Catalog)
	if err != nil || d == nil {
		return nil, err
	}
	idx, err := nameIndex(d)
	if err != nil {
		return nil, err
	}
	lo, err := rowenc.EncodeIndexPrefix(d, idx, []types.Datum{types.NewString(from)})
	if err != nil {
		return nil, err
	}
	hi := keys.TableIndexPrefix(d.ID, idx.ID).PrefixEnd()
	if to != "" {
		if hi, err = rowenc.EncodeIndexPrefix(d, idx, []types.Datum{types.NewString(to)}); err != nil {
			return nil, err
		}
	}
	if limit <= 0 {
		limit = 1000
	}
	var out []Key
	err = s.cfg.DB.RunTxn(ctx, "keys-by-name", func(ctx context.Context, txn *kvclient.Txn) error {
		out = nil
		ents, err := txn.Scan(ctx, lo, hi, int64(limit))
		if err != nil {
			return err
		}
		pks := make([]keys.Key, 0, len(ents))
		for _, kv := range ents {
			pk, err := rowenc.IndexEntryPrimaryKey(d, idx, kv.Key, kv.Value)
			if err != nil {
				return err
			}
			pks = append(pks, pk)
		}
		if len(pks) == 0 {
			return nil
		}
		vals, err := txn.GetBatch(ctx, pks)
		if err != nil {
			return err
		}
		for i, v := range vals {
			if v == nil {
				continue
			}
			r, err := decodeRow(d, pks[i], v)
			if err != nil {
				return err
			}
			out = append(out, r.Key)
		}
		return nil
	})
	return out, err
}

// Destroy destroys key id: its state becomes destroyed and its material
// is overwritten with nothing (the row stays, so the name is not given
// to another key — except a tenant key's, which is renamed DestroyedName
// so that its tenant's next write makes a fresh key), and then the row's
// history is purged (the purge of issue #295), so the material does not
// linger in older MVCC versions until the GC window passes. The purge's
// report is returned; an incomplete one can be completed by destroying
// again, which purges again. Backups taken before stay as they were (the
// design's "documented dependency").
func (s *Store) Destroy(ctx context.Context, id int64) (Key, *kvpb.PurgeReport, error) {
	d, err := s.desc(ctx, false)
	if err != nil {
		return Key{}, nil, err
	}
	k, err := s.update(ctx, "keys-destroy", id, func(r *row) error {
		r.State, r.versions, r.ClusterKey = StateDestroyed, nil, enc.ClusterKeyRef{}
		if r.Kind == KindTenant && !strings.HasPrefix(r.Name, DestroyedPrefix) {
			r.Name = DestroyedName(r.Name, r.ID)
		}
		return nil
	})
	if err != nil {
		return Key{}, nil, err
	}
	pk, err := pkKey(d, id)
	if err != nil {
		return k, nil, err
	}
	rep, err := s.cfg.DB.Purge(ctx, pk, pk.PrefixEnd())
	if err != nil {
		return k, nil, fmt.Errorf("key %d is destroyed, but purging its history failed (destroy it again to retry the purge): %w", id, err)
	}
	return k, rep, nil
}

// DestroyVersions destroys every version of key id below version
// (issue #310, step 5: what a rotation's rewrite does once no row is
// sealed under the old versions): their material is removed from the
// row, which keeps its later versions and stays enabled, and then the
// row's history is purged, as Destroy's is, so the old material does not
// linger in older MVCC versions. It returns the key and how many
// versions it destroyed; with none to destroy it writes and purges
// nothing. A version at or above the key's current one is never
// destroyed this way (Destroy destroys a whole key). Unwrap of a
// destroyed version is refused with ErrVersionDestroyed; the move of the
// key's generation reaches every gateway's cache within its lease.
func (s *Store) DestroyVersions(ctx context.Context, id int64, below int) (Key, int, *kvpb.PurgeReport, error) {
	d, err := s.desc(ctx, false)
	if err != nil {
		return Key{}, 0, nil, err
	}
	var n int
	var noop bool
	k, err := s.update(ctx, "keys-destroy-versions", id, func(r *row) error {
		n, noop = 0, false
		if r.State == StateDestroyed {
			return ErrDestroyed
		}
		limit := min(below, r.Version)
		var keep []sealedVersion
		var info []VersionInfo
		for _, sv := range r.versions {
			if sv.Version < limit {
				n++
				continue
			}
			keep = append(keep, sv)
			info = append(info, VersionInfo{Version: sv.Version, Created: nanosToTime(sv.Created)})
		}
		if n == 0 {
			noop = true
			return errNothingToDo
		}
		r.versions, r.Versions = keep, info
		return nil
	})
	if noop {
		k, err = s.GetByID(ctx, id)
		return k, 0, nil, err
	}
	if err != nil {
		return Key{}, 0, nil, err
	}
	pk, err := pkKey(d, id)
	if err != nil {
		return k, n, nil, err
	}
	rep, err := s.cfg.DB.Purge(ctx, pk, pk.PrefixEnd())
	if err != nil {
		return k, n, nil, fmt.Errorf("%d versions of key %d are destroyed, but purging their history failed (their material lingers in the row's history until it is garbage-collected): %w", n, id, err)
	}
	return k, n, rep, nil
}

// errNothingToDo ends an update that would change nothing, without
// writing.
var errNothingToDo = errors.New("nothing to do")

// destroyManyBatch is how many keys one transaction of DestroyMany
// destroys.
const destroyManyBatch = 100

// DestroyMany destroys keys ids as Destroy does — tombstoned, a tenant
// key renamed — destroyManyBatch to a transaction, and then purges the
// history of the whole table's rows once, rather than once per key: a
// purge forces a garbage collection and a compaction, which dominates
// the cost of destroying a key, and a dropped per-tenant table may have
// millions. Keys already destroyed, or gone, are skipped. It returns how
// many keys it destroyed and the purge's report; should the purge fail,
// the keys are destroyed and their material lingers in history until the
// table's GC window passes or a later purge (another Destroy) removes it.
//
// spare, when given, runs in each batch's transaction before anything is
// written, and returns which keys to leave alone: what a caller deciding
// that keys are unused reads again there, so that the decision and the
// destruction are one serializable transaction (a table restore that
// marks the keys as wanted orders before or after it, never between).
func (s *Store) DestroyMany(ctx context.Context, ids []int64, spare func(ctx context.Context, txn *kvclient.Txn) (func(Key) bool, error)) (int, *kvpb.PurgeReport, error) {
	d, err := s.desc(ctx, false)
	if err != nil {
		return 0, nil, err
	}
	destroyed := 0
	for at := 0; at < len(ids); at += destroyManyBatch {
		batch := ids[at:min(at+destroyManyBatch, len(ids))]
		var n int
		err := s.cfg.DB.RunTxn(ctx, "keys-destroy-many", func(ctx context.Context, txn *kvclient.Txn) error {
			n = 0
			keep := func(Key) bool { return false }
			if spare != nil {
				var err error
				if keep, err = spare(ctx, txn); err != nil {
					return err
				}
			}
			for _, id := range batch {
				r, pk, err := s.read(ctx, txn, d, id, true)
				if errors.Is(err, ErrNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				if r.State == StateDestroyed || keep(r.Key) {
					continue
				}
				was := r.Name
				r.State, r.versions, r.ClusterKey = StateDestroyed, nil, enc.ClusterKeyRef{}
				if r.Kind == KindTenant && !strings.HasPrefix(r.Name, DestroyedPrefix) {
					r.Name = DestroyedName(r.Name, r.ID)
					if err := s.rename(ctx, txn, d, r, was); err != nil {
						return err
					}
				}
				r.Generation++
				val, err := r.encode(d)
				if err != nil {
					return err
				}
				if err := txn.Put(ctx, pk, val); err != nil {
					return err
				}
				n++
			}
			return nil
		})
		if err != nil {
			return destroyed, nil, err
		}
		destroyed += n
	}
	if destroyed == 0 {
		return 0, nil, nil
	}
	lo, hi := keys.TableDataSpan(d.ID)
	rep, err := s.cfg.DB.Purge(ctx, lo, hi)
	if err != nil {
		return destroyed, nil, fmt.Errorf("%d keys are destroyed, but purging their history failed: %w", destroyed, err)
	}
	return destroyed, rep, nil
}

// WrappingKeysIn returns the metadata of the key service's keys (kind
// wrapping), in name order, read in txn: a scan of the name index over
// the names outside ReservedPrefixes, then a point read of each row. Its
// cost is the number of key service keys (and of any key named outside
// the prefixes), not the number of keys: what SHOW ENCRYPTION KEYS, DROP
// OWNED, REASSIGN OWNED and DROP ROLE list by, on a cluster whose
// per-tenant tables may have millions of tenant keys.
func (s *Store) WrappingKeysIn(ctx context.Context, txn *kvclient.Txn) ([]Key, error) {
	d, err := sql.ReadKeysTableIn(ctx, txn, s.cfg.Catalog)
	if err != nil || d == nil {
		return nil, err
	}
	idx, err := nameIndex(d)
	if err != nil {
		return nil, err
	}
	at := func(name string) (keys.Key, error) {
		return rowenc.EncodeIndexPrefix(d, idx, []types.Datum{types.NewString(name)})
	}
	// The spans between the reserved prefixes: [start, first), [end of
	// first, second), ..., [end of last, end of the index).
	var spans [][2]keys.Key
	lo := keys.TableIndexPrefix(d.ID, idx.ID)
	for _, p := range ReservedPrefixes {
		hi, err := at(p)
		if err != nil {
			return nil, err
		}
		spans = append(spans, [2]keys.Key{lo, hi})
		if lo, err = at(PrefixEnd(p)); err != nil {
			return nil, err
		}
	}
	spans = append(spans, [2]keys.Key{lo, keys.TableIndexPrefix(d.ID, idx.ID).PrefixEnd()})
	var pks []keys.Key
	for _, sp := range spans {
		ents, err := txn.Scan(ctx, sp[0], sp[1], 0)
		if err != nil {
			return nil, err
		}
		listRowsRead.Add(int64(len(ents)))
		for _, kv := range ents {
			pk, err := rowenc.IndexEntryPrimaryKey(d, idx, kv.Key, kv.Value)
			if err != nil {
				return nil, err
			}
			pks = append(pks, pk)
		}
	}
	if len(pks) == 0 {
		return nil, nil
	}
	vals, err := txn.GetBatch(ctx, pks)
	if err != nil {
		return nil, err
	}
	out := make([]Key, 0, len(pks))
	for i, v := range vals {
		if v == nil {
			continue
		}
		listRowsRead.Add(1)
		r, err := decodeRow(d, pks[i], v)
		if err != nil {
			return nil, err
		}
		if r.Kind == KindWrapping {
			out = append(out, r.Key)
		}
	}
	return out, nil
}

// ListIn returns every key's metadata, in id order, read in txn.
func (s *Store) ListIn(ctx context.Context, txn *kvclient.Txn) ([]Key, error) {
	d, err := sql.ReadKeysTableIn(ctx, txn, s.cfg.Catalog)
	if err != nil || d == nil {
		return nil, err
	}
	lo, hi := rowenc.PrimarySpanFor(d)
	kvs, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	listRowsRead.Add(int64(len(kvs)))
	out := make([]Key, 0, len(kvs))
	for _, kv := range kvs {
		r, err := decodeRow(d, kv.Key, kv.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, r.Key)
	}
	return out, nil
}

// SetAccess changes key id's owner or privileges: change edits k (only
// Owner and Privileges are written back) and reports whether it changed
// anything. A change bumps the key's generation, so a gateway's cache
// sees it at its next lease check. txn nil runs in a transaction of its
// own; otherwise the change is part of txn (DROP OWNED, REASSIGN OWNED).
func (s *Store) SetAccess(ctx context.Context, txn *kvclient.Txn, id int64, change func(k *Key) (bool, error)) (Key, bool, error) {
	d, err := s.descIn(ctx, txn, true)
	if err != nil {
		return Key{}, false, err
	}
	var out Key
	var changed bool
	apply := func(ctx context.Context, txn *kvclient.Txn) error {
		r, pk, err := s.read(ctx, txn, d, id, true)
		if err != nil {
			return err
		}
		k := r.Key
		k.Privileges = clonePrivileges(r.Privileges)
		if changed, err = change(&k); err != nil || !changed {
			out = r.Key
			return err
		}
		r.Owner, r.Privileges = k.Owner, k.Privileges
		if len(r.Privileges) == 0 {
			r.Privileges = nil
		}
		r.Generation++
		val, err := r.encode(d)
		if err != nil {
			return err
		}
		if err := txn.Put(ctx, pk, val); err != nil {
			return err
		}
		out = r.Key
		return nil
	}
	if txn != nil {
		err = apply(ctx, txn)
	} else {
		err = s.cfg.DB.RunTxn(ctx, "keys-access", apply)
	}
	return out, changed, err
}

func clonePrivileges(m map[string][]string) map[string][]string {
	if m == nil {
		return nil
	}
	out := make(map[string][]string, len(m))
	for k, v := range m {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// RewrapResult counts a rewrap pass.
type RewrapResult struct {
	Rewrapped int `json:"rewrapped"`
	// Unopened lists rows that could not be rewrapped because no cluster
	// key this node holds opens them, by the ref they are under.
	Unopened map[string]int `json:"unopened,omitempty"`
}

// rewrapPage is how many rows one rewrap transaction moves.
const rewrapPage = 100

// RewrapAll seals every key that is not under the current cluster key
// under it: after a cluster key rotation, and after a restore brought
// rows sealed under another cluster's key. It is resumable — a row
// already under the current key is skipped — and moves rows in small
// transactions. A row under a cluster key this node does not hold is
// counted and left; with strict, that is an error.
func (s *Store) RewrapAll(ctx context.Context, strict bool) (RewrapResult, error) {
	var res RewrapResult
	d, err := sql.LookupKeysTable(ctx, s.cfg.DB, s.cfg.Catalog)
	if err != nil || d == nil {
		return res, err
	}
	ref, ck, err := s.cfg.ClusterKeys.Current(ctx)
	if err != nil {
		return res, err
	}
	defer enc.Zero(ck)
	lo, hi := rowenc.PrimarySpanFor(d)
	for start := lo; ; {
		var last keys.Key
		var n int
		err := s.cfg.DB.RunTxn(ctx, "keys-rewrap", func(ctx context.Context, txn *kvclient.Txn) error {
			n, last = 0, nil
			kvs, err := txn.Scan(ctx, start, hi, rewrapPage)
			if err != nil {
				return err
			}
			var wb kvclient.WriteBatch
			for _, kv := range kvs {
				last = kv.Key
				r, err := decodeRow(d, kv.Key, kv.Value)
				if err != nil {
					return err
				}
				if r.State == StateDestroyed || len(r.versions) == 0 || r.ClusterKey == ref {
					continue
				}
				from, err := s.cfg.ClusterKeys.Key(ctx, r.ClusterKey)
				if err != nil {
					if strict {
						return fmt.Errorf("key %d (%s) is sealed under cluster key %s, which this cluster does not hold: %w", r.ID, r.Name, r.ClusterKey, err)
					}
					if res.Unopened == nil {
						res.Unopened = map[string]int{}
					}
					res.Unopened[r.ClusterKey.String()]++
					continue
				}
				vs, err := rewrapVersions(from, ck, r.ID, r.Name, r.Kind, r.versions)
				enc.Zero(from)
				if err != nil {
					return err
				}
				r.versions, r.ClusterKey = vs, ref
				r.Generation++
				val, err := r.encode(d)
				if err != nil {
					return err
				}
				wb.Put(kv.Key, val)
				n++
			}
			if wb.Len() == 0 {
				return nil
			}
			if err := s.accept(ctx, txn, ref); err != nil {
				return err
			}
			return txn.RunBatch(ctx, &wb)
		})
		if err != nil {
			return res, err
		}
		res.Rewrapped += n
		if last == nil {
			return res, nil
		}
		start = last.Next()
	}
}

// Refs counts the keys under each cluster key: what a rotation still has
// to move, and what a restore needs to be able to open.
func (s *Store) Refs(ctx context.Context) (map[enc.ClusterKeyRef]int, error) {
	d, err := sql.LookupKeysTable(ctx, s.cfg.DB, s.cfg.Catalog)
	if err != nil {
		return nil, err
	}
	var out map[enc.ClusterKeyRef]int
	err = s.cfg.DB.RunTxn(ctx, "keys-refs", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		out, err = RefsIn(ctx, txn, d)
		return err
	})
	return out, err
}

// RefOfRow returns the cluster key one datax_keys row's material is
// sealed under (zero for a destroyed key), from the row as stored: what
// a restore reads from a backup before it applies anything. Keys of the
// table's secondary index carry no material and return zero.
func RefOfRow(d *catalog.TableDescriptor, k keys.Key, v []byte) (enc.ClusterKeyRef, error) {
	lo, hi := rowenc.PrimarySpanFor(d)
	if k.Compare(lo) < 0 || k.Compare(hi) >= 0 || len(v) == 0 {
		return enc.ClusterKeyRef{}, nil
	}
	vals, err := rowenc.DecodeValue(d, v)
	if err != nil {
		return enc.ClusterKeyRef{}, err
	}
	ck := vals[sql.KeysColClusterKey].S
	if ck == "" || len(vals[sql.KeysColMaterial].S) == 0 {
		return enc.ClusterKeyRef{}, nil
	}
	return enc.ParseClusterKeyRef(ck)
}

func (s *Store) accept(ctx context.Context, txn *kvclient.Txn, ref enc.ClusterKeyRef) error {
	if s.cfg.Accept == nil {
		return nil
	}
	return s.cfg.Accept(ctx, txn, ref)
}

// RefsIn counts, inside txn, the keys under each cluster key. A nil
// descriptor (no table yet) counts nothing.
func RefsIn(ctx context.Context, txn *kvclient.Txn, d *catalog.TableDescriptor) (map[enc.ClusterKeyRef]int, error) {
	out := map[enc.ClusterKeyRef]int{}
	if d == nil {
		return out, nil
	}
	lo, hi := rowenc.PrimarySpanFor(d)
	kvs, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		r, err := RefOfRow(d, kv.Key, kv.Value)
		if err != nil {
			return nil, err
		}
		if !r.IsZero() {
			out[r]++
		}
	}
	return out, nil
}
