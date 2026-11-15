package sql

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// TestKeyTransport (issue #310): the key functions carry key material and
// run only over TLS or from this machine. A session the SQL server never
// marked is the node's own, in process, and allowed.
func TestKeyTransport(t *testing.T) {
	for _, c := range []struct {
		tls, loopback, ok bool
	}{
		{true, false, true},
		{false, true, true},
		{true, true, true},
		{false, false, false},
	} {
		if got := KeyTransportOK(c.tls, c.loopback); got != c.ok {
			t.Errorf("tls=%v loopback=%v: %v, want %v", c.tls, c.loopback, got, c.ok)
		}
		s := &Session{}
		s.SetTransport(c.tls, c.loopback)
		serr := s.keyTransportError("datax_key_unwrap")
		if (serr == nil) != c.ok {
			t.Errorf("tls=%v loopback=%v: session refusal %v", c.tls, c.loopback, serr)
		}
		if serr != nil && serr.Code != CodeInvalidAuthorization {
			t.Errorf("refused with %s, want %s", serr.Code, CodeInvalidAuthorization)
		}
	}
	if serr := (&Session{}).keyTransportError("datax_key_unwrap"); serr != nil {
		t.Errorf("an unmarked (in-process) session was refused: %v", serr)
	}
}

// TestKeyAllows: using a key needs its owner's role or a grant of the
// privilege (to a role held, or PUBLIC); admin authority alone is not a
// grant.
func TestKeyAllows(t *testing.T) {
	k := EncryptionKey{Owner: "keeper", Privileges: map[string][]string{"ingest": {PrivWrap}, "public": {PrivUnwrap}}}
	set := func(names ...string) catalog.RoleSet {
		s := catalog.RoleSet{"public": true}
		for _, n := range names {
			s[n] = true
		}
		return s
	}
	for _, c := range []struct {
		roles catalog.RoleSet
		priv  string
		ok    bool
	}{
		{set("keeper"), PrivWrap, true},
		{set("keeper"), PrivUnwrap, true},
		{set("ingest"), PrivWrap, true},
		{set("ingest"), PrivUnwrap, true}, // through PUBLIC
		{set("admin"), PrivWrap, false},
		{set("root", "admin"), PrivWrap, false},
	} {
		if got := KeyAllows(k, c.roles, c.priv); got != c.ok {
			t.Errorf("%v %s: %v, want %v", c.roles.Names(), c.priv, got, c.ok)
		}
	}
	if !KeyAllows(EncryptionKey{}, set("root"), PrivUnwrap) {
		t.Error("root does not use a key it owns (owner \"\")")
	}
	if got, err := expandKeyPrivileges([]string{"USAGE"}); err != nil || len(got) != 2 {
		t.Errorf("USAGE: %v %v", got, err)
	}
	if _, err := expandKeyPrivileges([]string{"SELECT"}); err == nil {
		t.Error("SELECT on an encryption key was accepted")
	}
}

// TestKeysTableColumnIDs: the key store reads and writes datax_keys by
// column ID, the IDs being positions in the DDL; the columns added
// after the table was first defined are its last ones, in the order
// EnsureKeysTable adds them to an older table, so they get the same IDs
// either way.
func TestKeysTableColumnIDs(t *testing.T) {
	stmts, err := parser.Parse(KeysTableDDL)
	if err != nil {
		t.Fatal(err)
	}
	cols := stmts[0].(*parser.CreateTable).Columns
	for id, name := range map[catalog.ColumnID]string{
		KeysColID: "id", KeysColName: "name", KeysColMaterial: "material", KeysColGeneration: "generation",
		KeysColExternalRoot: "external_root", KeysColOwner: "owner", KeysColPrivileges: "privileges",
	} {
		if int(id) > len(cols) || cols[id-1].Name != name {
			t.Errorf("column %d is not %q", id, name)
		}
	}
	n := len(cols)
	for i, c := range keysTableAddedColumns {
		if cols[n-len(keysTableAddedColumns)+i].Name != c.name || !strings.HasPrefix(c.ddl, c.name+" ") {
			t.Errorf("added column %d (%s) is not the table's column %d", i, c.name, n-len(keysTableAddedColumns)+i+1)
		}
	}
}
