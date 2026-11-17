package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestTableStableAcrossSystemTableMadeMidChain (issue #327): a restore cut
// inside an interval refuses a user table created in that interval, but
// not the cluster's own table a node made there, whose definition is the
// binary's and whose rows at the cut are read from the history.
func TestTableStableAcrossSystemTableMadeMidChain(t *testing.T) {
	desc := func(id uint64, name string) json.RawMessage {
		raw, err := json.Marshal(&catalog.TableDescriptor{ID: id, Name: name})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	prev := &backupManifest{EndTS: hlc.Timestamp{WallTime: 100}}
	audit := backupTable{ID: catalog.AuditTableID, Name: catalog.AuditTableName, Descriptor: desc(catalog.AuditTableID, catalog.AuditTableName)}
	user := backupTable{ID: 3, Name: "orders", Descriptor: desc(3, "orders")}
	cur := &backupManifest{EndTS: hlc.Timestamp{WallTime: 300}, Tables: []backupTable{audit, user}}
	cut := hlc.Timestamp{WallTime: 200}

	if err := tableStableAcross(prev, cur, audit, cut); err != nil {
		t.Fatalf("the audit table, made inside the interval: %v", err)
	}
	if err := tableStableAcross(prev, cur, user, cut); err == nil || !strings.Contains(err.Error(), "it was created between") {
		t.Fatalf("a user table made inside the interval: %v; want the created-between refusal", err)
	}

	// A system table whose definition changed inside the interval is still
	// refused: its rows at the cut may be in the other layout.
	wider, err := json.Marshal(&catalog.TableDescriptor{ID: catalog.AuditTableID, Name: catalog.AuditTableName, Columns: []catalog.Column{{Name: "extra"}}})
	if err != nil {
		t.Fatal(err)
	}
	changed := audit
	changed.Descriptor = wider
	prev.Tables = []backupTable{audit}
	cur.Tables = []backupTable{changed}
	if err := tableStableAcross(prev, cur, changed, cut); err == nil || !strings.Contains(err.Error(), "definition changed") {
		t.Fatalf("a system table whose definition changed: %v; want the definition-changed refusal", err)
	}
}
