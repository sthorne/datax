package sql

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// The key service (issue #310, step 2; docs/design-key-service.md, "A.
// The key service"): named keys an application asks datax to keep, and
// the functions that wrap and unwrap its data keys under them.
//
// The SQL layer parses the statements, checks who may run them, and
// calls the KeyService the server gives each session (pkg/keystore's
// Service, over the datax_keys table and a gateway cache of the keys'
// material). The functions' privilege checks, rate limit, audit and
// cryptography are the service's: it is what holds the material.
//
// Privileges. A key is owned by the role that created it (CREATE needs
// the admin role). The owner and admins may alter, drop and grant on it.
// Using it needs WRAP (datax_key_wrap, datax_encrypt) or UNWRAP
// (datax_key_unwrap, datax_decrypt); USAGE is both. The owner holds both;
// an admin who is not the owner does not, and grants itself what it
// needs, in a statement the audit records — so the roles that can
// recover data keys are the ones a key's grants name.

// KeyService is what a session's key statements and functions call
// (pkg/keystore.Service). Its errors are *Error where a SQLSTATE fits.
type KeyService interface {
	// Ready reports why named keys cannot be made or used yet (the
	// cluster key is not enabled); nil when they can.
	Ready(ctx context.Context) error
	// Lease is how long a change to a key can go unseen by some gateway:
	// what DROP, DISABLE and REVOKE wait out before they return.
	Lease() time.Duration
	// Lookup reads the key named name from its row (42704 when none).
	Lookup(ctx context.Context, name string) (EncryptionKey, error)
	// Create makes a wrapping key owned by owner (42710 when the name is
	// taken, destroyed keys included).
	Create(ctx context.Context, name, owner string) (EncryptionKey, error)
	// Alter rotates, disables or enables key id (action "rotate",
	// "disable", "enable").
	Alter(ctx context.Context, id int64, action string) (EncryptionKey, error)
	// Destroy destroys every version of key id and purges the row's
	// history.
	Destroy(ctx context.Context, id int64) (EncryptionKey, error)
	// SetAccess changes key id's owner or privileges: change edits k and
	// reports whether it changed anything. txn nil: a transaction of its
	// own.
	SetAccess(ctx context.Context, txn *kvclient.Txn, id int64, change func(k *EncryptionKey) bool) (EncryptionKey, bool, error)
	// List returns every key of the key service, read in txn.
	List(ctx context.Context, txn *kvclient.Txn) ([]EncryptionKey, error)
	// Use runs one call of a key function.
	Use(ctx context.Context, u KeyUse) ([]byte, error)
}

// EncryptionKey is a named key as the SQL layer sees it: never material.
type EncryptionKey struct {
	ID      int64
	Name    string
	Kind    string
	State   string
	Version int
	// Owner is the owning role ("" is root); Privileges maps a grantee
	// to PrivWrap / PrivUnwrap.
	Owner      string
	Privileges map[string][]string
	Created    time.Time
}

// The key functions' operations.
const (
	KeyOpWrap    = "wrap"
	KeyOpUnwrap  = "unwrap"
	KeyOpEncrypt = "encrypt"
	KeyOpDecrypt = "decrypt"
)

// The privileges on an encryption key, in storage order.
const (
	PrivWrap   = "WRAP"
	PrivUnwrap = "UNWRAP"
)

var keyPrivOrder = []string{PrivWrap, PrivUnwrap}

// KeyUse is one call of a key function.
type KeyUse struct {
	Op   string
	Name string
	// Principal is the session user and Role the role the call runs as
	// (current_user): the rate limit and the privilege check are the
	// role's; Roles is its effective role set.
	Principal string
	Role      string
	Roles     catalog.RoleSet
	// Data is the data key or value (wrap, encrypt) or the blob (unwrap,
	// decrypt); Context is bound into the blob.
	Data, Context []byte
}

// KeyOpPrivilege is the privilege an operation needs.
func KeyOpPrivilege(op string) string {
	if op == KeyOpUnwrap || op == KeyOpDecrypt {
		return PrivUnwrap
	}
	return PrivWrap
}

// KeyAllows reports whether a role set may use k for priv: it holds the
// owning role, or priv was granted to one of its roles (or PUBLIC).
// Admin authority alone is not enough (see the package comment above).
func KeyAllows(k EncryptionKey, roles catalog.RoleSet, priv string) bool {
	return roles.Has(catalog.OwnerOf(k.Owner)) || catalog.HasPrivilege(k.Privileges, roles, priv)
}

// keyFuncs are the key functions and the operation each runs.
var keyFuncs = map[string]string{
	"datax_key_wrap":   KeyOpWrap,
	"datax_key_unwrap": KeyOpUnwrap,
	"datax_encrypt":    KeyOpEncrypt,
	"datax_decrypt":    KeyOpDecrypt,
}

func init() {
	for name := range keyFuncs {
		// Evaluated by the session, once per call, with constant
		// arguments — as the large-object functions are.
		volatileFuncs[name] = true
		splicedFuncs[name] = true
	}
}

// SetKeyService gives the session the node's key service (the SQL
// server does, for every connection); without one the key statements
// and functions are refused.
func (s *Session) SetKeyService(k KeyService) { s.keys = k }

// SetTransport records how the session's client is connected: over TLS,
// and from this machine. The key functions carry key material and are
// refused on a connection that is neither (KeyTransportOK). A session
// the SQL server did not mark is in-process and allowed.
func (s *Session) SetTransport(tls, loopback bool) {
	s.transportMarked = true
	s.transportOK = KeyTransportOK(tls, loopback)
}

// KeyTransportOK is the rule for connections that may carry key
// material (data keys, blobs, values to encrypt): TLS, or loopback — the
// rule every path that carries key material follows.
func KeyTransportOK(tls, loopback bool) bool { return tls || loopback }

// keyReady refuses the key service before v22 is finalized, on a node
// without the service, and before the cluster key is enabled.
func (s *Session) keyReady(ctx context.Context) *Error {
	if s.db.ClusterVersion() < version.V22 {
		return newErrf(CodeFeatureNotSupported, "encryption keys need cluster version %s: finalize the upgrade with `datax debug upgrade` first", version.V22)
	}
	if s.keys == nil {
		return newErrf(CodeFeatureNotSupported, "encryption keys are not available in this session")
	}
	if err := s.keys.Ready(ctx); err != nil {
		if se := ToSQLError(err); se.Code != CodeInternal {
			return se
		}
		return newErrf(CodeObjectNotInState, "encryption keys cannot be used yet: %v", err)
	}
	return nil
}

func (s *Session) keyTransportError(fn string) *Error {
	if !s.transportMarked || s.transportOK {
		return nil
	}
	return newErrf(CodeInvalidAuthorization, "%s() carries key material and is refused on this connection: it is neither TLS nor from this machine (connect with TLS, or over a loopback address)", fn)
}

// evalKeyFunc runs one call of a key function.
func (s *Session) evalKeyFunc(ctx context.Context, txn *kvclient.Txn, name string, args []types.Datum) (types.Datum, error) {
	op := keyFuncs[name]
	if len(args) != 3 {
		return types.Datum{}, newErrf(CodeUndefinedFunction, "%s() takes three arguments: the key's name, the %s, and a context", name, map[bool]string{true: "data", false: "blob"}[op == KeyOpWrap || op == KeyOpEncrypt])
	}
	for _, a := range args {
		if a.Null {
			return types.DNull, nil // strict, as PostgreSQL's functions are
		}
	}
	if serr := s.keyReady(ctx); serr != nil {
		return types.Datum{}, serr
	}
	if serr := s.keyTransportError(name); serr != nil {
		return types.Datum{}, serr
	}
	bytesArg := func(i int) ([]byte, error) {
		d, err := args[i].Coerce(types.Bytes)
		if err != nil {
			return nil, newErrf(CodeInvalidTextRepresentation, "%s: argument %d must be bytea: %v", name, i+1, err)
		}
		return []byte(d.S), nil
	}
	data, err := bytesArg(1)
	if err != nil {
		return types.Datum{}, err
	}
	kctx, err := bytesArg(2)
	if err != nil {
		return types.Datum{}, err
	}
	roles, err := s.roleSet(ctx, txn)
	if err != nil {
		return types.Datum{}, err
	}
	out, err := s.keys.Use(ctx, KeyUse{Op: op, Name: args[0].Text(), Principal: s.sessionUser, Role: s.actor(), Roles: roles, Data: data, Context: kctx})
	if err != nil {
		return types.Datum{}, err
	}
	return types.NewBytes(out), nil
}

// keyArgsConstant refuses a key function whose arguments read a column:
// the functions are evaluated once per call, before the rows are read.
func keyArgsConstant(e parser.Expr) *Error {
	for _, a := range e.Args {
		if exprHas(a, func(x parser.Expr) bool { return x.Column != "" || x.Sub != nil }) {
			return newErrf(CodeFeatureNotSupported, "%s() takes constants or parameters as its arguments, not columns or subqueries: read the value first and pass it as a parameter", e.Func)
		}
	}
	return nil
}

// ---- statements ----

// isKeyStatement reports whether stmt is one the key service runs.
func isKeyStatement(stmt parser.Statement) bool {
	switch t := stmt.(type) {
	case *parser.CreateEncryptionKey, *parser.AlterEncryptionKey, *parser.DropEncryptionKey, *parser.ShowEncryptionKeys,
		*parser.SetClusterSetting, *parser.ShowClusterSetting:
		return true
	case *parser.GrantRevoke:
		return t.ObjectKind == "encryption key"
	}
	return false
}

// execKeyStatement runs a key statement. Each changes the datax_keys row
// in a transaction of its own, so none can run in a transaction block;
// those that take a use away wait out the gateways' lease before they
// return, as schema changes do.
func (s *Session) execKeyStatement(ctx context.Context, stmt parser.Statement) (*Result, *Error) {
	switch t := stmt.(type) {
	case *parser.SetClusterSetting:
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "SET CLUSTER SETTING cannot run inside a transaction block")
		}
		return s.execSetClusterSetting(ctx, t)
	case *parser.ShowClusterSetting:
		return s.execShowClusterSetting(ctx, t)
	}
	if s.state == StateOpen {
		return nil, newErrf(CodeActiveTransaction, "%s cannot run inside a transaction block", keyStatementTag(stmt))
	}
	if serr := s.keyReady(ctx); serr != nil {
		return nil, serr
	}
	switch t := stmt.(type) {
	case *parser.CreateEncryptionKey:
		return s.execCreateEncryptionKey(ctx, t)
	case *parser.AlterEncryptionKey:
		return s.execAlterEncryptionKey(ctx, t)
	case *parser.DropEncryptionKey:
		return s.execDropEncryptionKey(ctx, t)
	case *parser.ShowEncryptionKeys:
		return s.execShowEncryptionKeys(ctx)
	case *parser.GrantRevoke:
		return s.execGrantOnKeys(ctx, t)
	}
	return nil, newErrf(CodeInternal, "not a key statement: %T", stmt)
}

func keyStatementTag(stmt parser.Statement) string {
	switch t := stmt.(type) {
	case *parser.CreateEncryptionKey:
		return "CREATE ENCRYPTION KEY"
	case *parser.AlterEncryptionKey:
		return "ALTER ENCRYPTION KEY"
	case *parser.DropEncryptionKey:
		return "DROP ENCRYPTION KEY"
	case *parser.ShowEncryptionKeys:
		return "SHOW ENCRYPTION KEYS"
	case *parser.GrantRevoke:
		if t.Revoke {
			return "REVOKE"
		}
		return "GRANT"
	}
	return "this statement"
}

// keyCheck runs fn with a transaction for the privilege checks.
func (s *Session) keyCheck(ctx context.Context, fn func(txn *kvclient.Txn) error) *Error {
	var ferr error
	if err := s.db.RunTxn(ctx, "key-check", func(ctx context.Context, txn *kvclient.Txn) error {
		ferr = fn(txn)
		return nil
	}); err != nil {
		return ToSQLError(err)
	}
	return ToSQLError(ferr)
}

// waitKeyLease waits out the gateways' lease on key material: once it
// returns, no gateway acts on what the key was before the change.
func (s *Session) waitKeyLease(ctx context.Context) *Error {
	if s.keys == nil {
		return nil
	}
	t := time.NewTimer(s.keys.Lease())
	defer t.Stop()
	select {
	case <-ctx.Done():
		return newErrf(CodeQueryCanceled, "the change is committed, but the statement was canceled before every gateway was known to have seen it (within %s of the change they all have)", s.keys.Lease())
	case <-t.C:
		return nil
	}
}

func (s *Session) lookupKey(ctx context.Context, name string) (EncryptionKey, *Error) {
	k, err := s.keys.Lookup(ctx, name)
	if err != nil {
		return EncryptionKey{}, ToSQLError(err)
	}
	return k, nil
}

func keyNotFound(err *Error) bool { return err != nil && err.Code == CodeUndefinedObject }

func (s *Session) checkKeyOwner(ctx context.Context, k EncryptionKey) *Error {
	return s.keyCheck(ctx, func(txn *kvclient.Txn) error {
		return s.checkOwner(ctx, txn, "encryption key", k.Name, k.Owner)
	})
}

func (s *Session) execCreateEncryptionKey(ctx context.Context, t *parser.CreateEncryptionKey) (*Result, *Error) {
	if serr := s.keyCheck(ctx, func(txn *kvclient.Txn) error {
		err := s.checkAdmin(ctx, txn)
		if se, ok := err.(*Error); ok && se.Code == CodeInsufficientPriv {
			return newErrf(CodeInsufficientPriv, "permission denied to create encryption key %q: CREATE ENCRYPTION KEY needs the admin role (an admin can then hand the key to a role with ALTER ENCRYPTION KEY ... OWNER TO)", t.Name)
		}
		return err
	}); serr != nil {
		return nil, serr
	}
	if t.IfNotExists {
		if _, serr := s.lookupKey(ctx, t.Name); serr == nil {
			return &Result{Tag: "CREATE ENCRYPTION KEY"}, nil
		} else if !keyNotFound(serr) {
			return nil, serr
		}
	}
	owner := s.actor()
	if owner == catalog.RootRole {
		owner = ""
	}
	k, err := s.keys.Create(ctx, t.Name, owner)
	if err != nil {
		se := ToSQLError(err)
		if t.IfNotExists && se.Code == CodeDuplicateObject {
			return &Result{Tag: "CREATE ENCRYPTION KEY"}, nil
		}
		return nil, se
	}
	log.Audit("key-ddl", "stmt", "CREATE ENCRYPTION KEY", "target", k.Name, "key_id", k.ID, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "CREATE ENCRYPTION KEY"}, nil
}

func (s *Session) execAlterEncryptionKey(ctx context.Context, t *parser.AlterEncryptionKey) (*Result, *Error) {
	k, serr := s.lookupKey(ctx, t.Name)
	if serr != nil {
		return nil, serr
	}
	if serr := s.checkKeyOwner(ctx, k); serr != nil {
		return nil, serr
	}
	tag := "ALTER ENCRYPTION KEY"
	if t.Action == "owner" {
		owner := t.Owner
		if serr := s.keyCheck(ctx, func(txn *kvclient.Txn) error { return s.checkNewOwner(ctx, txn, owner) }); serr != nil {
			return nil, serr
		}
		if owner == catalog.RootRole {
			owner = ""
		}
		if _, _, err := s.keys.SetAccess(ctx, nil, k.ID, func(k *EncryptionKey) bool {
			if k.Owner == owner {
				return false
			}
			k.Owner = owner
			return true
		}); err != nil {
			return nil, ToSQLError(err)
		}
		log.Audit("ownership-ddl", "stmt", tag+" OWNER TO", "kind", "encryption key", "target", k.Name, "owner", t.Owner, "principal", s.sessionUser, "role", s.user)
		// The old owner's implicit use of the key is gone.
		if serr := s.waitKeyLease(ctx); serr != nil {
			return nil, serr
		}
		return &Result{Tag: tag}, nil
	}
	after, err := s.keys.Alter(ctx, k.ID, t.Action)
	if err != nil {
		return nil, ToSQLError(err)
	}
	log.Audit("key-ddl", "stmt", tag+" "+strings.ToUpper(t.Action), "target", k.Name, "key_id", k.ID, "version", int64(after.Version), "principal", s.sessionUser, "role", s.user)
	if t.Action == "disable" {
		if serr := s.waitKeyLease(ctx); serr != nil {
			return nil, serr
		}
	}
	return &Result{Tag: tag}, nil
}

func (s *Session) execDropEncryptionKey(ctx context.Context, t *parser.DropEncryptionKey) (*Result, *Error) {
	k, serr := s.lookupKey(ctx, t.Name)
	if keyNotFound(serr) && t.IfExists {
		return &Result{Tag: "DROP ENCRYPTION KEY"}, nil
	}
	if serr != nil {
		return nil, serr
	}
	if serr := s.checkKeyOwner(ctx, k); serr != nil {
		return nil, serr
	}
	after, err := s.keys.Destroy(ctx, k.ID)
	if after.State == "destroyed" {
		log.Audit("key-ddl", "stmt", "DROP ENCRYPTION KEY", "target", k.Name, "key_id", k.ID, "principal", s.sessionUser, "role", s.user)
		// Destroyed, whatever the purge reported: the gateways' lease is
		// waited out either way before the statement returns.
		if serr := s.waitKeyLease(ctx); serr != nil {
			return nil, serr
		}
	}
	if err != nil {
		return nil, ToSQLError(err)
	}
	return &Result{Tag: "DROP ENCRYPTION KEY"}, nil
}

// expandKeyPrivileges validates a GRANT / REVOKE privilege list for an
// encryption key: WRAP, UNWRAP, and USAGE or ALL for both.
func expandKeyPrivileges(privs []string) ([]string, *Error) {
	set := map[string]bool{}
	for _, p := range privs {
		switch p {
		case PrivWrap, PrivUnwrap:
			set[p] = true
		case catalog.PrivUsage, "ALL":
			set[PrivWrap], set[PrivUnwrap] = true, true
		default:
			return nil, newErrf(CodeInvalidGrantOperation, "invalid privilege type %s for encryption key (encryption key privileges: WRAP, UNWRAP, USAGE, ALL)", p)
		}
	}
	var out []string
	for _, p := range keyPrivOrder {
		if set[p] {
			out = append(out, p)
		}
	}
	return out, nil
}

func (s *Session) execGrantOnKeys(ctx context.Context, t *parser.GrantRevoke) (*Result, *Error) {
	tag := "GRANT"
	if t.Revoke {
		tag = "REVOKE"
	}
	if t.GrantOption {
		return nil, newErrf(CodeFeatureNotSupported, "grant options are not supported on encryption keys: the owner and admins grant")
	}
	privs, serr := expandKeyPrivileges(t.Privileges)
	if serr != nil {
		return nil, serr
	}
	var grantees []string
	if serr := s.keyCheck(ctx, func(txn *kvclient.Txn) error {
		var err error
		grantees, err = s.resolveGrantees(ctx, txn, t.Grantees)
		return err
	}); serr != nil {
		return nil, serr
	}
	var ks []EncryptionKey
	for _, name := range t.Objects {
		k, serr := s.lookupKey(ctx, name)
		if serr != nil {
			return nil, serr
		}
		if serr := s.checkKeyOwner(ctx, k); serr != nil {
			return nil, newErrf(CodeInsufficientPriv, "permission denied to %s on encryption key %q: %s", strings.ToLower(tag), k.Name, serr.Msg)
		}
		ks = append(ks, k)
	}
	took := false
	for _, k := range ks {
		_, changed, err := s.keys.SetAccess(ctx, nil, k.ID, func(k *EncryptionKey) bool {
			var none map[string][]string
			changed := false
			for _, g := range grantees {
				if applyGrant(&k.Privileges, &none, g, privs, keyPrivOrder, t.Revoke, false, false) {
					changed = true
				}
			}
			return changed
		})
		if err != nil {
			return nil, ToSQLError(err)
		}
		took = took || (changed && t.Revoke)
	}
	log.Audit("privilege-ddl", "stmt", tag, "privileges", strings.Join(t.Privileges, ","), "kind", t.ObjectKind,
		"targets", strings.Join(t.Objects, ","), "grantees", strings.Join(grantees, ","), "grant_option", false,
		"principal", s.sessionUser, "role", s.user)
	if took {
		if serr := s.waitKeyLease(ctx); serr != nil {
			return nil, serr
		}
	}
	return &Result{Tag: tag}, nil
}

func showEncryptionKeysColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "name", Type: types.String}, {Name: "state", Type: types.String}, {Name: "version", Type: types.Int},
		{Name: "owner", Type: types.String}, {Name: "privileges", Type: types.String}, {Name: "created", Type: types.Timestamp},
	}
}

// execShowEncryptionKeys lists the key service's keys: every one for an
// admin, otherwise those the role owns or holds a privilege on. Metadata
// only.
func (s *Session) execShowEncryptionKeys(ctx context.Context) (*Result, *Error) {
	res := &Result{Columns: showEncryptionKeysColumns()}
	err := s.db.RunTxn(ctx, "show-keys", func(ctx context.Context, txn *kvclient.Txn) error {
		res.Rows = nil
		set, err := s.roleSet(ctx, txn)
		if err != nil {
			return err
		}
		keys, err := s.keys.List(ctx, txn)
		if err != nil {
			return err
		}
		for _, k := range keys {
			if !set.IsAdmin() && s.actor() != catalog.RootRole && !KeyAllows(k, set, PrivWrap) && !KeyAllows(k, set, PrivUnwrap) {
				continue
			}
			priv := types.DNull
			if len(k.Privileges) > 0 {
				priv = types.NewString(keyACLText(k.Privileges))
			}
			res.Rows = append(res.Rows, []types.Datum{types.NewString(k.Name), types.NewString(k.State), types.NewInt(int64(k.Version)),
				types.NewString(catalog.OwnerOf(k.Owner)), priv, types.NewTimestamp(k.Created.UnixNano())})
		}
		return nil
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	sort.Slice(res.Rows, func(i, j int) bool { return res.Rows[i][0].S < res.Rows[j][0].S })
	res.Tag = "SHOW ENCRYPTION KEYS"
	return res, nil
}

// ---- ownership hooks: DROP ROLE, DROP OWNED, REASSIGN OWNED ----

// keysIn lists the key service's keys in txn, or none when the service
// is not in use here.
func (s *Session) keysIn(ctx context.Context, txn *kvclient.Txn) ([]EncryptionKey, error) {
	if s.keys == nil || s.db.ClusterVersion() < version.V22 {
		return nil, nil
	}
	return s.keys.List(ctx, txn)
}

// ownedKeys names the keys the role owns.
func (s *Session) ownedKeys(ctx context.Context, txn *kvclient.Txn, owned map[string]bool) ([]string, error) {
	keys, err := s.keysIn(ctx, txn)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, k := range keys {
		if k.State != "destroyed" && owned[catalog.OwnerOf(k.Owner)] {
			out = append(out, k.Name)
		}
	}
	return out, nil
}

// revokeKeysFrom strips every privilege on a key granted to the roles,
// in txn, and reports how many keys changed. The statement then waits
// out the lease after it commits (keyLeaseAfterCommit).
func (s *Session) revokeKeysFrom(ctx context.Context, txn *kvclient.Txn, owned map[string]bool) (int, error) {
	keys, err := s.keysIn(ctx, txn)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, k := range keys {
		holds := false
		for r := range owned {
			if _, ok := k.Privileges[r]; ok {
				holds = true
			}
		}
		if !holds {
			continue
		}
		if _, changed, err := s.keys.SetAccess(ctx, txn, k.ID, func(k *EncryptionKey) bool {
			changed := false
			for r := range owned {
				if _, ok := k.Privileges[r]; ok {
					delete(k.Privileges, r)
					changed = true
				}
			}
			return changed
		}); err != nil {
			return 0, err
		} else if changed {
			n++
			s.keyLeaseAfterCommit = true
		}
	}
	return n, nil
}

// reassignKeys hands the keys the roles own to another role, in txn.
func (s *Session) reassignKeys(ctx context.Context, txn *kvclient.Txn, owned map[string]bool, to string) (int, error) {
	keys, err := s.keysIn(ctx, txn)
	if err != nil {
		return 0, err
	}
	if to == catalog.RootRole {
		to = ""
	}
	n := 0
	for _, k := range keys {
		if !owned[catalog.OwnerOf(k.Owner)] {
			continue
		}
		if _, changed, err := s.keys.SetAccess(ctx, txn, k.ID, func(k *EncryptionKey) bool {
			if k.Owner == to {
				return false
			}
			k.Owner = to
			return true
		}); err != nil {
			return 0, err
		} else if changed {
			n++
			s.keyLeaseAfterCommit = true
		}
	}
	return n, nil
}

// finishKeyLease waits out the lease for a committed statement that took
// a use of a key away (keyLeaseAfterCommit).
func (s *Session) finishKeyLease(ctx context.Context) *Error {
	if !s.keyLeaseAfterCommit {
		return nil
	}
	s.keyLeaseAfterCommit = false
	return s.waitKeyLease(ctx)
}
