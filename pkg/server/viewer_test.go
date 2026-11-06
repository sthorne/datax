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
