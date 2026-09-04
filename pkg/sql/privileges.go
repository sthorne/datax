package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/util/log"
)

// Authorization. root is all-powerful; members of the admin role
// (GRANT ADMIN TO user; root implicitly a member) may run DDL and manage
// users and grants; everyone else needs per-table privileges. In insecure
// (trust) mode the username is client-claimed, so enforcement is advisory
// there — documented in docs/sql.md.

// tablePrivs are the grantable per-table privileges.
var tablePrivs = map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true}

// requiresAdmin reports whether a statement is restricted to the admin
// role (DDL, user management, grants).
func requiresAdmin(stmt parser.Statement) bool {
	switch stmt.(type) {
	case *parser.CreateTable, *parser.DropTable, *parser.AlterTable,
		*parser.CreateIndex, *parser.CreateUser, *parser.Analyze, *parser.DropUser, *parser.GrantRevoke,
		*parser.CreateDatabase, *parser.DropDatabase, *parser.AlterDatabase:
		return true
	}
	return false
}

// isAdmin reports whether the session's user is root or an admin-role
// member.
func (s *Session) isAdmin(ctx context.Context, txn *kvclient.Txn) (bool, error) {
	if s.user == "root" {
		return true, nil
	}
	v, err := txn.Get(ctx, keys.AdminUserKey(s.user))
	if err != nil {
		return false, err
	}
	return v != nil, nil
}

// checkAdmin returns 42501 unless the user is root or an admin.
func (s *Session) checkAdmin(ctx context.Context, txn *kvclient.Txn) error {
	ok, err := s.isAdmin(ctx, txn)
	if err != nil {
		return err
	}
	if !ok {
		return newErrf(CodeInsufficientPriv, "permission denied: %q is not an admin", s.user)
	}
	return nil
}

// checkTablePriv returns 42501 unless the user holds priv on the table
// (or is root/admin).
func (s *Session) checkTablePriv(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, priv string) error {
	if s.user == "root" {
		return nil
	}
	if catalog.IsSystemTable(desc.Name) && priv != "SELECT" {
		// Only the cluster and its admins write the metrics table; a
		// SELECT grant is how a reporting user reads it.
		if err := s.checkAdmin(ctx, txn); err != nil {
			return newErrf(CodeInsufficientPriv, "permission denied for table %q: only admins may %s it", desc.Name, priv)
		}
		return nil
	}
	for _, p := range desc.Privileges[s.user] {
		if p == priv {
			return nil
		}
	}
	ok, err := s.isAdmin(ctx, txn)
	if err != nil {
		return err
	}
	if !ok {
		return newErrf(CodeInsufficientPriv, "permission denied for table %q (%s as user %q)", desc.Name, priv, s.user)
	}
	return nil
}

// checkCreateInDatabase admits CREATE TABLE for root, admins, and users
// holding CREATE on the target database.
func (s *Session) checkCreateInDatabase(ctx context.Context, txn *kvclient.Txn, table string) error {
	if s.user == "root" {
		return nil
	}
	ok, err := s.isAdmin(ctx, txn)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	dbName, _ := catalog.SplitTableName(table)
	if dbName == "" {
		dbName = s.database
	}
	db, err := s.cat.Database(ctx, txn, dbName)
	if err != nil {
		return ToSQLError(err)
	}
	if db.HasPrivilege(s.user, catalog.PrivCreate) {
		return nil
	}
	return newErrf(CodeInsufficientPriv, "permission denied: %q needs the admin role or CREATE on database %q", s.user, dbName)
}

// execGrantRevoke applies a GRANT/REVOKE (the admin gate has already run).
func (s *Session) execGrantRevoke(ctx context.Context, txn *kvclient.Txn, t *parser.GrantRevoke) (*Result, error) {
	tag := "GRANT"
	if t.Revoke {
		tag = "REVOKE"
	}
	if t.Database != "" {
		return s.execGrantRevokeDatabase(ctx, txn, t, tag)
	}

	if t.Admin {
		if t.User == "root" {
			return nil, newErrf(CodeFeatureNotSupported, "root's admin membership is implicit and cannot be changed")
		}
		key := keys.AdminUserKey(t.User)
		if t.Revoke {
			if err := txn.Delete(ctx, key); err != nil {
				return nil, err
			}
		} else if err := txn.Put(ctx, key, []byte("1")); err != nil {
			return nil, err
		}
		log.Audit("privilege-ddl", "stmt", tag, "privileges", "ADMIN", "target", t.User, "principal", s.user)
		return &Result{Tag: tag}, nil
	}

	// Expand and validate the privilege list.
	set := map[string]bool{}
	for _, p := range t.Privileges {
		if p == "ALL" {
			for tp := range tablePrivs {
				set[tp] = true
			}
			continue
		}
		if !tablePrivs[p] {
			return nil, newErrf(CodeSyntaxError, "%s is not a table privilege (table privileges: SELECT, INSERT, UPDATE, DELETE, ALL)", p)
		}
		set[p] = true
	}

	shared, err := s.lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	desc := shared.Clone()
	if desc.Privileges == nil {
		desc.Privileges = map[string][]string{}
	}
	cur := map[string]bool{}
	for _, p := range desc.Privileges[t.User] {
		cur[p] = true
	}
	for p := range set {
		cur[p] = !t.Revoke
	}
	var next []string
	for _, p := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} { // stable order
		if cur[p] {
			next = append(next, p)
		}
	}
	if len(next) == 0 {
		delete(desc.Privileges, t.User)
	} else {
		desc.Privileges[t.User] = next
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	log.Audit("privilege-ddl", "stmt", tag, "privileges", strings.Join(t.Privileges, ","),
		"table", t.Table, "target", t.User, "principal", s.user)
	return &Result{Tag: tag}, nil
}
