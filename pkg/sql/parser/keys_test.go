package parser

import (
	"reflect"
	"testing"
)

// TestParseKeyStatements (issue #310): the key service's statements, the
// key privileges on GRANT and REVOKE, and the cluster settings.
func TestParseKeyStatements(t *testing.T) {
	for src, want := range map[string]Statement{
		`CREATE ENCRYPTION KEY billing`:                         &CreateEncryptionKey{Name: "billing"},
		`create encryption key if not exists "Billing"`:         &CreateEncryptionKey{Name: "Billing", IfNotExists: true},
		`ALTER ENCRYPTION KEY billing ROTATE`:                   &AlterEncryptionKey{Name: "billing", Action: "rotate"},
		`ALTER ENCRYPTION KEY billing DISABLE;`:                 &AlterEncryptionKey{Name: "billing", Action: "disable"},
		`ALTER ENCRYPTION KEY billing ENABLE`:                   &AlterEncryptionKey{Name: "billing", Action: "enable"},
		`ALTER ENCRYPTION KEY billing OWNER TO App`:             &AlterEncryptionKey{Name: "billing", Action: "owner", Owner: "app"},
		`DROP ENCRYPTION KEY billing`:                           &DropEncryptionKey{Name: "billing"},
		`DROP ENCRYPTION KEY IF EXISTS billing`:                 &DropEncryptionKey{Name: "billing", IfExists: true},
		`SHOW ENCRYPTION KEYS`:                                  &ShowEncryptionKeys{},
		`SET CLUSTER SETTING key_service.rate_limit = 50`:       &SetClusterSetting{Name: "key_service.rate_limit", Value: "50"},
		`SET CLUSTER SETTING key_service.rate_limit TO DEFAULT`: &SetClusterSetting{Name: "key_service.rate_limit", Default: true},
		`SET CLUSTER SETTING key_service.rate_limit = -1`:       &SetClusterSetting{Name: "key_service.rate_limit", Value: "-1"},
		`SHOW CLUSTER SETTING key_service.rate_limit`:           &ShowClusterSetting{Name: "key_service.rate_limit"},
		`GRANT WRAP ON ENCRYPTION KEY billing TO ingest`: &GrantRevoke{Privileges: []string{"WRAP"}, ObjectKind: "encryption key",
			Objects: []string{"billing"}, Grantees: []string{"ingest"}},
		`GRANT wrap, unwrap ON ENCRYPTION KEY a, b TO x, y`: &GrantRevoke{Privileges: []string{"WRAP", "UNWRAP"}, ObjectKind: "encryption key",
			Objects: []string{"a", "b"}, Grantees: []string{"x", "y"}},
		`REVOKE USAGE ON ENCRYPTION KEY billing FROM app`: &GrantRevoke{Revoke: true, Privileges: []string{"USAGE"}, ObjectKind: "encryption key",
			Objects: []string{"billing"}, Grantees: []string{"app"}},
		// A role named wrap is still a role.
		`GRANT wrap TO alice`:    &GrantRevoke{Roles: []string{"wrap"}, Grantees: []string{"alice"}},
		`REVOKE unwrap FROM bob`: &GrantRevoke{Revoke: true, Roles: []string{"unwrap"}, Grantees: []string{"bob"}},
	} {
		got := parseOne(t, src)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s:\n got %#v\nwant %#v", src, got, want)
		}
	}
	for _, src := range []string{
		`CREATE ENCRYPTION KEY`,
		`CREATE ENCRYPTION KEY a b`,
		`ALTER ENCRYPTION KEY a`,
		`ALTER ENCRYPTION KEY a RENAME TO b`,
		`DROP ENCRYPTION KEY a, b`,
		`SET CLUSTER SETTING x`,
		`SET CLUSTER SETTING x = - y`,
	} {
		if _, err := Parse(src); err == nil {
			t.Errorf("%s: parsed", src)
		}
	}
}
