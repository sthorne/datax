package sql

import (
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// SQL users. Only SCRAM verifiers are ever stored (never plaintext), at
// /system/users/<name>. There are no roles or privileges (documented
// limitation): any authenticated user can do anything, including managing
// users. Insecure clusters use trust auth and these statements simply
// manage credentials that nothing checks.

func (s *Session) execCreateUser(ctx context.Context, txn *kvclient.Txn, t *parser.CreateUser) (*Result, error) {
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
	return &Result{Tag: "DROP USER"}, nil
}

var _ = fmt.Sprintf
