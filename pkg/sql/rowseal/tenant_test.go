package rowseal

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// tenantKeys is fakeKeys that also MACs (under a fixed secret per key id)
// and can be told a key is destroyed.
type tenantKeys struct {
	fakeKeys
	mu        sync.Mutex
	destroyed map[int64]bool
	macs      int
}

func (k *tenantKeys) Current(ctx context.Context, id int64) (Key, error) {
	k.mu.Lock()
	gone := k.destroyed[id]
	k.mu.Unlock()
	if gone {
		return Key{}, ErrKeyDestroyed
	}
	return k.fakeKeys.Current(ctx, id)
}

func (k *tenantKeys) Version(ctx context.Context, id int64, v int) (Key, error) {
	k.mu.Lock()
	gone := k.destroyed[id]
	k.mu.Unlock()
	if gone {
		return Key{}, ErrKeyDestroyed
	}
	return k.fakeKeys.Version(ctx, id, v)
}

func (k *tenantKeys) MAC(_ context.Context, id int64, _ int, label string, data []byte) ([]byte, error) {
	k.mu.Lock()
	k.macs++
	k.mu.Unlock()
	h := hmac.New(sha256.New, []byte{byte(id)})
	h.Write([]byte(label))
	h.Write([]byte{0})
	h.Write(data)
	return h.Sum(nil), nil
}

// tenantStore keeps tenant keys by name, making each with a fresh AEAD in
// keys.
type tenantStore struct {
	t      *testing.T
	keys   *tenantKeys
	mu     sync.Mutex
	byName map[string]int64
	next   int64
	gets   int
}

func (s *tenantStore) Ready(context.Context) error { return nil }
func (s *tenantStore) CreateTableKey(context.Context, *kvclient.Txn, string) (int64, error) {
	return 0, errors.New("unused")
}
func (s *tenantStore) DestroyKey(context.Context, int64) error { return nil }

func (s *tenantStore) TenantKey(_ context.Context, name string, create bool) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if id, ok := s.byName[name]; ok {
		return id, nil
	}
	if !create {
		return 0, ErrKeyNotFound
	}
	s.next++
	id := 1000 + s.next
	s.byName[name] = id
	s.keys.mu.Lock()
	s.keys.keys[id] = map[int]cipher.AEAD{1: newAEAD(s.t)}
	s.keys.cur[id] = 1
	s.keys.mu.Unlock()
	return id, nil
}

func (s *tenantStore) DestroyTenantKey(_ context.Context, name string) (TenantKeyDestroyed, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byName[name]
	if !ok {
		return TenantKeyDestroyed{}, ErrKeyNotFound
	}
	delete(s.byName, name)
	s.keys.mu.Lock()
	s.keys.destroyed[id] = true
	s.keys.mu.Unlock()
	return TenantKeyDestroyed{KeyID: id}, nil
}

func (s *tenantStore) DestroyTenantKeys(uint64) {}

func newTenantFakes(t *testing.T) (*tenantKeys, *tenantStore) {
	k := &tenantKeys{fakeKeys: fakeKeys{keys: map[int64]map[int]cipher.AEAD{}, cur: map[int64]int{}}, destroyed: map[int64]bool{}}
	return k, &tenantStore{t: t, keys: k, byName: map[string]int64{}}
}

func perTenantTable(id uint64, tenantType types.Family) *catalog.TableDescriptor {
	return &catalog.TableDescriptor{ID: id, Name: "items", Columns: []catalog.Column{
		{ID: 1, Name: "tenant_id", Type: tenantType, NotNull: true},
		{ID: 2, Name: "id", Type: types.Int, NotNull: true},
		{ID: 3, Name: "title", Type: types.String},
	}, PrimaryKey: []catalog.ColumnID{1, 2}, EncryptionKey: &catalog.TableEncryptionKey{KeyID: 7, SealID: id, TenantColumn: 1}}
}

// TestTenantOfEveryType: the tenant of a row key is its leading column's
// key encoding, for every type a tenant column may have, and it is the
// encoding datax_destroy_tenant_key derives from a value of the column
// (given as the column's type or as text): two rows of one tenant agree,
// two tenants differ, and the rest of the key does not leak in.
func TestTenantOfEveryType(t *testing.T) {
	cases := []struct {
		fam  types.Family
		a, b types.Datum
		text string // a, as a client would pass it in text
	}{
		{types.Int, types.NewInt(-42), types.NewInt(1 << 40), "-42"},
		{types.String, types.NewString("acme\x00corp"), types.NewString("acme"), "acme\x00corp"},
		{types.Bytes, types.NewBytes([]byte{0, 1, 0xff}), types.NewBytes([]byte{0, 1}), ""},
		{types.Uuid, types.NewUUID([16]byte{0x4f, 0x6c, 1}), types.NewUUID([16]byte{0x4f, 0x6c, 2}), "4f6c0100-0000-0000-0000-000000000000"},
	}
	for _, c := range cases {
		t.Run(c.fam.String(), func(t *testing.T) {
			if !TenantColumnTypeOK(c.fam) {
				t.Fatalf("%s is refused as a tenant column", c.fam)
			}
			d := perTenantTable(70, c.fam)
			ka1, _ := rowenc.EncodePK(d, []types.Datum{c.a, types.NewInt(1)})
			ka2, _ := rowenc.EncodePK(d, []types.Datum{c.a, types.NewInt(2)})
			kb1, _ := rowenc.EncodePK(d, []types.Datum{c.b, types.NewInt(1)})
			ta1, err := TenantOf(d, ka1)
			if err != nil {
				t.Fatal(err)
			}
			ta2, _ := TenantOf(d, ka2)
			tb1, _ := TenantOf(d, kb1)
			if !bytes.Equal(ta1, ta2) || bytes.Equal(ta1, tb1) {
				t.Fatalf("tenants: %x %x %x", ta1, ta2, tb1)
			}
			v, err := TenantValue(d, c.a)
			if err != nil || !bytes.Equal(v, ta1) {
				t.Fatalf("TenantValue %x, row key's %x (%v)", v, ta1, err)
			}
			if c.text != "" {
				v, err := TenantValue(d, types.NewString(c.text))
				if err != nil || !bytes.Equal(v, ta1) {
					t.Fatalf("TenantValue of text %q: %x, row key's %x (%v)", c.text, v, ta1, err)
				}
			}
			_, got, err := rowenc.LeadingPKColumn(d, ka1)
			if cmp, cerr := got.Compare(c.a); err != nil || cerr != nil || cmp != 0 {
				t.Fatalf("decoded %v, want %v (%v)", got, c.a, err)
			}
		})
	}
	for _, f := range []types.Family{types.Float, types.Bool, types.Timestamp, types.Decimal, types.Jsonb, types.Enum} {
		if TenantColumnTypeOK(f) {
			t.Fatalf("%s accepted as a tenant column", f)
		}
	}
	d := perTenantTable(70, types.Int)
	if _, err := TenantValue(d, types.NewString("not a number")); err == nil {
		t.Fatal("a tenant that is not a value of the column")
	}
	if _, err := TenantOf(d, []byte("elsewhere")); err == nil {
		t.Fatal("a key outside the table")
	}
}

// TestTenantKeyNames: a tenant's key name is the seal id's prefix and a
// keyed digest — the same for one tenant everywhere, different across
// tenants, seal ids and naming keys, and never the tenant's value.
func TestTenantKeyNames(t *testing.T) {
	ctx := context.Background()
	k, s := newTenantFakes(t)
	r := NewTenantResolver(k, s, 0)
	d := perTenantTable(71, types.String)
	enc := func(v string) []byte { b, _ := TenantValue(d, types.NewString(v)); return b }
	n1, err := r.Name(ctx, d, enc("acme"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(n1, "tenant/71/") || len(n1) != len("tenant/71/")+64 || strings.Contains(n1, "acme") {
		t.Fatalf("name %q", n1)
	}
	if again, _ := r.Name(ctx, d, enc("acme")); again != n1 {
		t.Fatal("not deterministic")
	}
	other := perTenantTable(72, types.String)
	otherKey := perTenantTable(71, types.String)
	otherKey.EncryptionKey.KeyID = 8
	for _, n := range []func() (string, error){
		func() (string, error) { return r.Name(ctx, d, enc("acme2")) },
		func() (string, error) { return r.Name(ctx, other, enc("acme")) },
		func() (string, error) { return r.Name(ctx, otherKey, enc("acme")) },
	} {
		if got, err := n(); err != nil || got == n1 {
			t.Fatalf("collision: %q %v", got, err)
		}
	}
	if TenantNamePrefix(71) != "tenant/71/" {
		t.Fatal(TenantNamePrefix(71))
	}
}

// TestTenantBoundary: rows of two tenants are sealed under two keys, each
// value naming its own; a tenant's key is made once and then served from
// the cache without asking the store; reads take the key from the value
// and never resolve; after a tenant's key is destroyed its old rows fail
// with XXK01-class errors while its next write gets a fresh key.
func TestTenantBoundary(t *testing.T) {
	ctx := context.Background()
	k, s := newTenantFakes(t)
	b := New(k, s, nil)
	d := perTenantTable(73, types.Int)
	row := func(tenant, id int64) []byte {
		key, _ := rowenc.EncodePK(d, []types.Datum{types.NewInt(tenant), types.NewInt(id)})
		return key
	}
	plain := []byte{rowenc.ValueVersion, 9}
	sa, err := b.SealValue(ctx, d, row(1, 1), plain)
	if err != nil {
		t.Fatal(err)
	}
	sa2, _ := b.SealValue(ctx, d, row(1, 2), plain)
	sb, _ := b.SealValue(ctx, d, row(2, 1), plain)
	ida, va, _ := TenantKeyOf(sa)
	ida2, _, _ := TenantKeyOf(sa2)
	idb, _, _ := TenantKeyOf(sb)
	if ida == 0 || ida != ida2 || ida == idb || va != 1 {
		t.Fatalf("key ids: tenant 1 %d,%d tenant 2 %d", ida, ida2, idb)
	}
	if len(sa) != len(plain)+TenantOverhead(1) || TenantOverhead(1) != 38 {
		t.Fatalf("overhead %d", len(sa)-len(plain))
	}
	if s.gets != 2 || k.macs != 2 {
		t.Fatalf("%d store lookups and %d digests for two tenants' three rows (want 2 and 2)", s.gets, k.macs)
	}
	for i := 0; i < 100; i++ {
		if _, err := b.SealValue(ctx, d, row(1, int64(i)), plain); err != nil {
			t.Fatal(err)
		}
		if got, err := b.OpenValue(ctx, d, row(2, 1), sb); err != nil || !bytes.Equal(got, plain) {
			t.Fatalf("open: %v", err)
		}
	}
	if s.gets != 2 {
		t.Fatalf("cached tenants went to the store: %d lookups", s.gets)
	}
	// Bound to its row, tenant and key.
	if _, err := b.OpenValue(ctx, d, row(2, 1), sa); err == nil {
		t.Fatal("tenant 1's value opened at tenant 2's row")
	}
	forged := append([]byte(nil), sa...)
	copy(forged[1:9], sb[1:9]) // claim tenant 2's key
	var ce *CorruptError
	if _, err := b.OpenValue(ctx, d, row(1, 1), forged); !errors.As(err, &ce) {
		t.Fatalf("value presented under another key: %v", err)
	}
	if _, err := b.OpenValue(ctx, d, row(1, 1), sa[:12]); !errors.As(err, &ce) {
		t.Fatalf("truncated: %v", err)
	}

	// Destroy tenant 1's key: its rows fail, naming the key; tenant 2 is
	// unaffected; tenant 1's next write makes a fresh key.
	name, _ := b.Tenants().Name(ctx, d, mustTenant(t, d, row(1, 1)))
	if _, err := s.DestroyTenantKey(ctx, name); err != nil {
		t.Fatal(err)
	}
	var ku *KeyUnavailableError
	if _, err := b.OpenValue(ctx, d, row(1, 1), sa); !errors.As(err, &ku) || ku.KeyID != ida || !errors.Is(err, ErrKeyDestroyed) || !strings.Contains(err.Error(), "tenant") {
		t.Fatalf("row under a destroyed key: %v", err)
	}
	if _, err := b.OpenValue(ctx, d, row(2, 1), sb); err != nil {
		t.Fatalf("the other tenant: %v", err)
	}
	fresh, err := b.SealValue(ctx, d, row(1, 3), plain)
	if err != nil {
		t.Fatalf("write after destroy: %v", err)
	}
	idf, _, _ := TenantKeyOf(fresh)
	if idf == ida || idf == 0 {
		t.Fatalf("fresh key %d, destroyed %d", idf, ida)
	}
	if got, err := b.OpenValue(ctx, d, row(1, 3), fresh); err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("fresh row: %v", err)
	}
	if _, err := b.OpenValue(ctx, d, row(1, 1), sa); !errors.As(err, &ku) {
		t.Fatalf("the old row after the fresh key: %v", err)
	}
}

func mustTenant(t *testing.T, d *catalog.TableDescriptor, key []byte) []byte {
	t.Helper()
	v, err := TenantOf(d, key)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestTenantResolverBound: the resolver keeps at most its size of
// tenants, dropping the least recently used, and a dropped tenant is
// looked up again (finding the same key).
func TestTenantResolverBound(t *testing.T) {
	ctx := context.Background()
	k, s := newTenantFakes(t)
	r := NewTenantResolver(k, s, 8)
	d := perTenantTable(74, types.Int)
	ids := map[int64]int64{}
	for i := int64(0); i < 100; i++ {
		key, _ := rowenc.EncodePK(d, []types.Datum{types.NewInt(i), types.NewInt(1)})
		id, err := r.Resolve(ctx, d, key, true)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
		if r.Len() > 8 {
			t.Fatalf("%d tenants held", r.Len())
		}
	}
	before := s.gets
	key, _ := rowenc.EncodePK(d, []types.Datum{types.NewInt(0), types.NewInt(9)})
	if id, err := r.Resolve(ctx, d, key, false); err != nil || id != ids[0] {
		t.Fatalf("tenant 0 again: %d %v (was %d)", id, err, ids[0])
	}
	if s.gets != before+1 {
		t.Fatal("an evicted tenant was not looked up again")
	}
	// A read of a tenant with no key does not make one.
	key, _ = rowenc.EncodePK(d, []types.Datum{types.NewInt(500), types.NewInt(1)})
	var ku *KeyUnavailableError
	if _, err := r.Resolve(ctx, d, key, false); !errors.As(err, &ku) || !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("read of a new tenant: %v", err)
	}
}

// BenchmarkTenantBoundary is the per-tenant boundary's cost per row value
// for cached tenants: a seal (tenant decode, cache hit) and an open.
func BenchmarkTenantBoundary(b *testing.B) {
	t := &testing.T{}
	k, s := newTenantFakes(t)
	bd := New(k, s, nil)
	d := perTenantTable(75, types.Uuid)
	var keysByTenant [][]byte
	for i := 0; i < 64; i++ {
		key, _ := rowenc.EncodePK(d, []types.Datum{types.NewUUID([16]byte{byte(i), 9}), types.NewInt(1)})
		keysByTenant = append(keysByTenant, key)
	}
	plain := bytes.Repeat([]byte{1}, 64)
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := keysByTenant[i%len(keysByTenant)]
		sv, err := bd.SealValue(ctx, d, key, plain)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := bd.OpenValue(ctx, d, key, sv); err != nil {
			b.Fatal(err)
		}
	}
}
