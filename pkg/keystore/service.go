package keystore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/util/log"
)

// Service is the key service of issue #310, step 2: the statements and
// functions of docs/design-key-service.md, "A. The key service", over
// the store and a gateway cache of the keys' material. It implements
// sql.KeyService; the SQL layer has checked who may run a statement
// before it calls one, and gives each function call the caller's role
// set, which the service checks itself.
//
// Every function call is audited — who (session user and role), which
// key (name and id) and version, which operation, and the outcome; never
// the material, the data, the blob or the context — through the node's
// buffered audit path (log.Audit; the audit table folds repeats of the
// same use within a flush into one record with a count). Calls are
// rate-limited per role on each node by a token bucket whose rate is the
// cluster setting key_service.rate_limit; a call past it is refused with
// 53400 and recorded as a refusal (which the audit table rate-limits in
// its turn).
type Service struct {
	store *Store
	cache *Cache
	now   func() time.Time
	rate  func(ctx context.Context) (int, error)

	names sync.Map // name → key id, for keys seen (names are never reused)

	// ready is set once Ready has passed: the cluster key, once enabled,
	// stays so, and the functions should not read its record every call.
	ready atomic.Bool

	rateMu  sync.Mutex
	rateVal int
	rateAt  time.Time
	buckets map[string]*bucket
}

// ServiceConfig configures a Service.
type ServiceConfig struct {
	Store *Store
	// Lease is the cache's lease (0: DefaultCacheLease).
	Lease time.Duration
	// Rate reads the per-role rate limit (operations a second on this
	// node; 0: none). It is read at most once a lease. nil: the default.
	Rate func(ctx context.Context) (int, error)
	// Now is the clock (nil: time.Now).
	Now func() time.Time
}

// NewService returns the key service over cfg.Store.
func NewService(cfg ServiceConfig) *Service {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Rate == nil {
		cfg.Rate = func(context.Context) (int, error) { return sql.DefaultKeyServiceRate, nil }
	}
	return &Service{
		store:   cfg.Store,
		cache:   NewCache(cfg.Store, CacheConfig{Lease: cfg.Lease, Now: cfg.Now}),
		now:     cfg.Now,
		rate:    cfg.Rate,
		buckets: map[string]*bucket{},
	}
}

// ClusterSettingRate reads the rate limit from the cluster setting.
func ClusterSettingRate(db *kvclient.DB) func(ctx context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		v, err := sql.ReadClusterSetting(ctx, db, sql.ClusterSettingKeyRate)
		if err != nil {
			return 0, err
		}
		return strconv.Atoi(v)
	}
}

// Cache is the service's cache of key material, for anything else on
// the gateway that uses named keys.
func (s *Service) Cache() *Cache { return s.cache }

// Run sweeps the cache until ctx is done.
func (s *Service) Run(ctx context.Context) { s.cache.Run(ctx) }

var _ sql.KeyService = (*Service)(nil)

// Ready reports why named keys cannot be used yet.
func (s *Service) Ready(ctx context.Context) error {
	if s.ready.Load() || s.store.cfg.Ready == nil {
		return nil
	}
	if err := s.store.cfg.Ready(ctx); err != nil {
		return &sql.Error{Code: sql.CodeObjectNotInState, Msg: "encryption keys cannot be used yet: " + err.Error()}
	}
	s.ready.Store(true)
	return nil
}

// Lease is the cache's lease.
func (s *Service) Lease() time.Duration { return s.cache.Lease() }

func toSQLKey(k Key) sql.EncryptionKey {
	return sql.EncryptionKey{ID: k.ID, Name: k.Name, Kind: string(k.Kind), State: string(k.State), Version: k.Version,
		Owner: k.Owner, Privileges: clonePrivileges(k.Privileges), Created: k.Created}
}

func errNoKey(name string) error {
	return &sql.Error{Code: sql.CodeUndefinedObject, Msg: fmt.Sprintf("encryption key %q does not exist", name)}
}

func errNotWrapping(k Key) error {
	return &sql.Error{Code: sql.CodeWrongObjectType, Msg: fmt.Sprintf("key %q is a %s key, which datax manages itself: it is not a key of the key service", k.Name, k.Kind)}
}

// stateError turns the store's sentinel errors into SQL errors.
func stateError(name string, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return errNoKey(name)
	case errors.Is(err, ErrDisabled):
		return &sql.Error{Code: sql.CodeObjectNotInState, Msg: fmt.Sprintf("encryption key %q is disabled (ALTER ENCRYPTION KEY %s ENABLE)", name, name)}
	case errors.Is(err, ErrDestroyed):
		return &sql.Error{Code: sql.CodeObjectNotInState, Msg: fmt.Sprintf("encryption key %q has been destroyed", name)}
	case errors.Is(err, ErrExists):
		return &sql.Error{Code: sql.CodeDuplicateObject, Msg: fmt.Sprintf("encryption key %q already exists (a dropped key keeps its name)", name)}
	}
	return err
}

// Lookup reads the key named name from its row.
func (s *Service) Lookup(ctx context.Context, name string) (sql.EncryptionKey, error) {
	k, err := s.store.Get(ctx, name)
	if errors.Is(err, ErrNotFound) {
		if rerr := s.Ready(ctx); rerr != nil {
			return sql.EncryptionKey{}, rerr
		}
		return sql.EncryptionKey{}, errNoKey(name)
	}
	if err != nil {
		return sql.EncryptionKey{}, err
	}
	if k.Kind != KindWrapping {
		return sql.EncryptionKey{}, errNotWrapping(k)
	}
	s.names.Store(name, k.ID)
	return toSQLKey(k), nil
}

// Create makes a wrapping key owned by owner.
func (s *Service) Create(ctx context.Context, name, owner string) (sql.EncryptionKey, error) {
	k, err := s.store.CreateOwned(ctx, KindWrapping, name, owner)
	if err != nil {
		return sql.EncryptionKey{}, stateError(name, err)
	}
	return toSQLKey(k), nil
}

// Alter rotates, disables or enables key id.
func (s *Service) Alter(ctx context.Context, id int64, action string) (sql.EncryptionKey, error) {
	var k Key
	var err error
	switch action {
	case "rotate":
		k, err = s.store.Rotate(ctx, id)
	case "disable":
		k, err = s.store.Disable(ctx, id)
	case "enable":
		k, err = s.store.Enable(ctx, id)
	default:
		return sql.EncryptionKey{}, fmt.Errorf("unknown key action %q", action)
	}
	s.cache.Forget(id)
	if err != nil {
		if errors.Is(err, ErrDestroyed) {
			return sql.EncryptionKey{}, &sql.Error{Code: sql.CodeObjectNotInState, Msg: "the encryption key has been destroyed"}
		}
		return sql.EncryptionKey{}, err
	}
	return toSQLKey(k), nil
}

// Destroy destroys every version of key id and purges the row's
// history. A purge that did not complete is an error after the key is
// destroyed (the returned key says so); destroying again retries it.
func (s *Service) Destroy(ctx context.Context, id int64) (sql.EncryptionKey, error) {
	k, rep, err := s.store.Destroy(ctx, id)
	s.cache.Forget(id)
	if err != nil {
		return toSQLKey(k), err
	}
	if rep != nil && !rep.Complete {
		return toSQLKey(k), &sql.Error{Code: sql.CodeObjectNotInState, Msg: fmt.Sprintf(
			"encryption key %q is destroyed, but purging its record's history did not finish (%s): run DROP ENCRYPTION KEY %s again to retry the purge",
			k.Name, strings.Join(rep.Incomplete, "; "), k.Name)}
	}
	return toSQLKey(k), nil
}

// SetAccess changes key id's owner or privileges.
func (s *Service) SetAccess(ctx context.Context, txn *kvclient.Txn, id int64, change func(k *sql.EncryptionKey) bool) (sql.EncryptionKey, bool, error) {
	k, changed, err := s.store.SetAccess(ctx, txn, id, func(k *Key) (bool, error) {
		if k.Kind != KindWrapping {
			return false, errNotWrapping(*k)
		}
		ek := toSQLKey(*k)
		if !change(&ek) {
			return false, nil
		}
		k.Owner, k.Privileges = ek.Owner, ek.Privileges
		return true, nil
	})
	if err != nil {
		return sql.EncryptionKey{}, false, err
	}
	if changed && txn == nil {
		s.cache.Forget(id)
	}
	return toSQLKey(k), changed, nil
}

// List returns every key of the key service, read in txn.
func (s *Service) List(ctx context.Context, txn *kvclient.Txn) ([]sql.EncryptionKey, error) {
	ks, err := s.store.ListIn(ctx, txn)
	if err != nil {
		return nil, err
	}
	out := make([]sql.EncryptionKey, 0, len(ks))
	for _, k := range ks {
		if k.Kind == KindWrapping {
			out = append(out, toSQLKey(k))
		}
	}
	return out, nil
}

// ---- the functions ----

// errBlob is the one error a blob that does not open gives.
func errBlob(op, name string) error {
	return &sql.Error{Code: sql.CodeExternalRoutine, Msg: fmt.Sprintf("datax_%s: the blob does not open under encryption key %q with this context (the wrong key or context, or a damaged blob)", map[string]string{
		sql.KeyOpUnwrap: "key_unwrap", sql.KeyOpDecrypt: "decrypt"}[op], name)}
}

// resolve finds the id of the key named name, from memory when it has
// been seen (a key's name is never given to another key).
func (s *Service) resolve(ctx context.Context, name string) (int64, error) {
	if id, ok := s.names.Load(name); ok {
		return id.(int64), nil
	}
	k, err := s.Lookup(ctx, name)
	if err != nil {
		return 0, err
	}
	return k.ID, nil
}

// Use runs one call of a key function.
func (s *Service) Use(ctx context.Context, u sql.KeyUse) ([]byte, error) {
	if err := s.allow(ctx, u.Role); err != nil {
		log.Audit("key-use-throttled", "principal", u.Principal, "target", u.Name, "role", u.Role, "op", u.Op, "outcome", "throttled")
		return nil, err
	}
	var id int64
	version := 0
	outcome := "failed"
	defer func() {
		log.Audit("key-use", "principal", u.Principal, "target", u.Name, "role", u.Role, "op", u.Op, "key_id", id, "version", int64(version), "outcome", outcome)
	}()
	out, err := s.use(ctx, u, &id, &version, &outcome)
	if err == nil {
		outcome = "ok"
	}
	return out, err
}

func (s *Service) use(ctx context.Context, u sql.KeyUse, idOut *int64, versionOut *int, outcome *string) ([]byte, error) {
	priv := sql.KeyOpPrivilege(u.Op)
	format := BlobWrapped
	if u.Op == sql.KeyOpEncrypt || u.Op == sql.KeyOpDecrypt {
		format = BlobEncrypted
	}
	sealing := u.Op == sql.KeyOpWrap || u.Op == sql.KeyOpEncrypt
	if (sealing && len(u.Data) > MaxBlobPlaintext) || len(u.Context) > MaxBlobContext {
		*outcome = "too-large"
		return nil, &sql.Error{Code: sql.CodeProgramLimitExceeded, Msg: fmt.Sprintf("at most %d bytes can be wrapped or encrypted, with a context of at most %d bytes: use envelope encryption (wrap a data key) for anything larger", MaxBlobPlaintext, MaxBlobContext)}
	}

	var k Key
	for attempt := 0; ; attempt++ {
		id, err := s.resolve(ctx, u.Name)
		if err != nil {
			if se, ok := err.(*sql.Error); ok && se.Code == sql.CodeUndefinedObject {
				*outcome = "no-such-key"
			}
			return nil, err
		}
		*idOut = id
		k, err = s.cache.Key(ctx, id)
		if errors.Is(err, ErrNotFound) && attempt == 0 {
			s.names.Delete(u.Name) // a restore replaced the keys
			continue
		}
		if err != nil {
			return nil, stateError(u.Name, err)
		}
		break
	}
	if k.Kind != KindWrapping || k.Name != u.Name {
		*outcome = "no-such-key"
		return nil, errNoKey(u.Name)
	}
	// A refusal is checked against the row before it is given, so a grant
	// or an ENABLE made on another gateway is not held back by the lease.
	if !sql.KeyAllows(toSQLKey(k), u.Roles, priv) || k.State != StateEnabled {
		var err error
		if k, err = s.cache.Refresh(ctx, k.ID); err != nil {
			return nil, stateError(u.Name, err)
		}
	}
	if !sql.KeyAllows(toSQLKey(k), u.Roles, priv) {
		*outcome = "denied"
		return nil, &sql.Error{Code: sql.CodeInsufficientPriv, Msg: fmt.Sprintf("permission denied for encryption key %q: %q needs %s on it (GRANT %s ON ENCRYPTION KEY %s TO %s)", u.Name, u.Role, priv, priv, u.Name, u.Role)}
	}
	switch k.State {
	case StateDisabled:
		*outcome = "disabled"
		return nil, stateError(u.Name, ErrDisabled)
	case StateDestroyed:
		*outcome = "destroyed"
		return nil, stateError(u.Name, ErrDestroyed)
	}

	var out []byte
	if sealing {
		err := s.cache.With(ctx, k.ID, 0, func(m *Material) error {
			*versionOut = m.Version
			var err error
			out, err = SealBlob(format, k.ID, m.Version, m.Key, u.Data, u.Context)
			return err
		})
		if err != nil {
			s.noteState(outcome, err)
			return nil, stateError(u.Name, err)
		}
		return out, nil
	}
	h, err := ParseBlobHeader(u.Data)
	if err != nil || h.Format != format || h.KeyID != k.ID || h.Version <= 0 {
		return nil, errBlob(u.Op, u.Name)
	}
	*versionOut = h.Version
	err = s.cache.With(ctx, k.ID, h.Version, func(m *Material) error {
		var err error
		out, err = OpenBlob(format, k.ID, m.Key, u.Data, u.Context)
		return err
	})
	switch {
	case err == nil:
		return out, nil
	case errors.Is(err, ErrBlob), errors.Is(err, ErrNoVersion):
		return nil, errBlob(u.Op, u.Name)
	}
	s.noteState(outcome, err)
	return nil, stateError(u.Name, err)
}

func (s *Service) noteState(outcome *string, err error) {
	switch {
	case errors.Is(err, ErrDisabled):
		*outcome = "disabled"
	case errors.Is(err, ErrDestroyed):
		*outcome = "destroyed"
	}
}

// ---- the rate limit ----

type bucket struct {
	tokens float64
	at     time.Time
}

// maxBuckets bounds the roles tracked at once; past it, full buckets
// (roles that have been idle a second or more) are dropped.
const maxBuckets = 4096

// allow takes a token from role's bucket, refusing when it is empty.
func (s *Service) allow(ctx context.Context, role string) error {
	now := s.now()
	s.rateMu.Lock()
	if s.rateAt.IsZero() || now.Sub(s.rateAt) >= s.cache.Lease() {
		s.rateMu.Unlock()
		r, err := s.rate(ctx)
		s.rateMu.Lock()
		if err == nil {
			s.rateVal, s.rateAt = r, now
		} else if s.rateAt.IsZero() {
			s.rateVal, s.rateAt = sql.DefaultKeyServiceRate, now
		}
	}
	defer s.rateMu.Unlock()
	rate := s.rateVal
	if rate <= 0 {
		return nil
	}
	burst := float64(rate)
	b := s.buckets[role]
	if b == nil {
		if len(s.buckets) >= maxBuckets {
			s.trimBuckets(now, float64(rate))
		}
		b = &bucket{tokens: burst, at: now}
		s.buckets[role] = b
	}
	b.tokens += now.Sub(b.at).Seconds() * float64(rate)
	if b.tokens > burst {
		b.tokens = burst
	}
	b.at = now
	if b.tokens < 1 {
		wait := time.Duration((1 - b.tokens) / float64(rate) * float64(time.Second))
		return &sql.Error{Code: sql.CodeConfigLimitExceeded, Msg: fmt.Sprintf(
			"role %q is over the key service's rate limit of %d operations a second on this node (cluster setting %s): retry in %s, and cache unwrapped data keys rather than unwrapping per read",
			role, rate, sql.ClusterSettingKeyRate, wait.Round(time.Millisecond))}
	}
	b.tokens--
	return nil
}

func (s *Service) trimBuckets(now time.Time, rate float64) {
	for role, b := range s.buckets {
		if b.tokens+now.Sub(b.at).Seconds()*rate >= rate {
			delete(s.buckets, role)
		}
	}
}
