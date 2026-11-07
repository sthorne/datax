package server

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
)

// The viewer fails closed (issue #213): a caller whose roles could not
// be resolved sees no table, and neither does one asking about a table
// the schema cache does not know; the admin form sees everything.
func TestKeyViewerFailsClosed(t *testing.T) {
	if (keyViewer{all: true}).sees(7) != true {
		t.Error("the admin form does not see a table")
	}
	if (keyViewer{err: errors.New("catalog unavailable")}).sees(7) {
		t.Error("a viewer whose roles could not be resolved sees a table")
	}
	if (keyViewer{set: catalog.RoleSet{}, descs: map[uint64]*catalog.TableDescriptor{}}).sees(7) {
		t.Error("a viewer sees a table the schema cache does not know")
	}
}

// keys_redacted states that a key was rendered at its prefix (issue
// #241): the all-keys viewer never says so, a restricted viewer says so
// once it has, and one built without anywhere to record it fails
// closed and says so regardless.
func TestKeyViewerReportsWhatItRedacted(t *testing.T) {
	if allKeysViewer().redactedAny() {
		t.Error("the all-keys viewer reports a redaction")
	}
	v := keyViewer{set: catalog.RoleSet{}, descs: map[uint64]*catalog.TableDescriptor{}, redacted: new(atomic.Bool)}
	if v.redactedAny() {
		t.Error("a restricted viewer reports a redaction before rendering a key")
	}
	v.redacted.Store(true)
	if !v.redactedAny() {
		t.Error("a restricted viewer does not report the redaction it recorded")
	}
	if !(keyViewer{err: errors.New("catalog unavailable")}).redactedAny() {
		t.Error("a restricted viewer built with nowhere to record a redaction fails open")
	}
	if (keyViewer{all: true}).redactedAny() {
		t.Error("a bare all-keys viewer reports a redaction")
	}
}

// Any grant names a table; only a read privilege shows a row of it
// (issue #253): a range boundary is a row key, so a grantee holding
// INSERT and no SELECT gets the table's name and its boundaries at their
// prefix. write_all is write everything, not read everything; the
// owner, read_all and a SELECT grant through a role read.
func TestKeyViewerDecodesOnlyWhatTheCallerMayRead(t *testing.T) {
	d := &catalog.TableDescriptor{ID: 7, Name: "sealedbook", Owner: "keeper",
		Privileges: map[string][]string{"ingest": {"INSERT"}, "seer": {"SELECT"}, "editors": {"SELECT", "UPDATE"}}}
	descs := map[uint64]*catalog.TableDescriptor{7: d}
	for _, c := range []struct {
		who         string
		set         catalog.RoleSet
		sees, reads bool
	}{
		{"an INSERT-only grantee", catalog.RoleSet{"ingest": true, "public": true}, true, false},
		{"a SELECT grantee", catalog.RoleSet{"seer": true, "public": true}, true, true},
		{"a member of a role holding SELECT", catalog.RoleSet{"alice": true, "editors": true, "public": true}, true, true},
		{"write_all", catalog.RoleSet{"w": true, "write_all": true, "public": true}, true, false},
		{"read_all", catalog.RoleSet{"r": true, "read_all": true, "public": true}, true, true},
		{"the owner", catalog.RoleSet{"keeper": true, "public": true}, true, true},
		{"nobody", catalog.RoleSet{"nobody": true, "public": true}, false, false},
	} {
		v := keyViewer{set: c.set, descs: descs, redacted: new(atomic.Bool)}
		if got := v.sees(7); got != c.sees {
			t.Errorf("%s: sees = %v, want %v", c.who, got, c.sees)
		}
		if got := v.reads(7); got != c.reads {
			t.Errorf("%s: reads = %v, want %v", c.who, got, c.reads)
		}
	}
	if !(keyViewer{all: true}).reads(7) {
		t.Error("the admin form does not read a table")
	}
	if (keyViewer{err: errors.New("catalog unavailable")}).reads(7) {
		t.Error("a viewer whose roles could not be resolved reads a table")
	}
}
