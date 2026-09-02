package sql

import (
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/util/log"
)

// SQL users. Only SCRAM verifiers are ever stored (never plaintext), at
// /system/users/<name>. User management requires the admin role (see
// privileges.go); per-table access is governed by GRANT/REVOKE. Insecure
// clusters use trust auth: these statements manage credentials that
// nothing checks, and the enforced identity is client-claimed.

func (s *Session) execCreateUser(ctx context.Context, txn *kvclient.Txn, t *parser.CreateUser) (*Result, error) {
	if t.Name == security.NodePrincipal {
		// The node certificate's CommonName is an admin principal on the
		// HTTP and admin-RPC surfaces; a SQL user of that name would gain
		// that authority through HTTP Basic auth and leave audit records
		// indistinguishable from the cluster's own.
		return nil, newErrf(CodeInvalidParameterValue, "user name %q is reserved for the cluster's node identity", t.Name)
	}
	key := keys.UserKey(t.Name)
	existing, err := txn.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if t.Alter && existing == nil {
		return nil, newErrf(CodeUndefinedObject, "user %q does not exist", t.Name)
	}
	if !t.Alter && existing != nil {
		return nil, newErrf(CodeDuplicateObject, "user %q already exists", t.Name)
	}
	if t.Password == "" {
		return nil, newErrf(CodeSyntaxError, "password must not be empty")
	}
	v, err := security.MakeScramVerifier(t.Password)
	if err != nil {
		return nil, newErrf(CodeInternal, "%v", err)
	}
	raw, err := security.MarshalVerifier(v)
	if err != nil {
		return nil, newErrf(CodeInternal, "%v", err)
	}
	if err := txn.Put(ctx, key, raw); err != nil {
		return nil, err
	}
	tag := "CREATE USER"
	if t.Alter {
		tag = "ALTER USER"
	}
	log.Audit("user-ddl", "stmt", tag, "target", t.Name, "principal", s.user)
	return &Result{Tag: tag}, nil
}

func (s *Session) execDropUser(ctx context.Context, txn *kvclient.Txn, t *parser.DropUser) (*Result, error) {
	if t.Name == "root" {
		return nil, newErrf(CodeFeatureNotSupported, "cannot drop user %q", t.Name)
	}
	key := keys.UserKey(t.Name)
	existing, err := txn.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, newErrf(CodeUndefinedObject, "user %q does not exist", t.Name)
	}
	if err := txn.Delete(ctx, key); err != nil {
		return nil, err
	}
	log.Audit("user-ddl", "stmt", "DROP USER", "target", t.Name, "principal", s.user)
	return &Result{Tag: "DROP USER"}, nil
}

var _ = fmt.Sprintf
