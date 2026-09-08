package server

import (
	"errors"
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
