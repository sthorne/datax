package rowenc

import (
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestPrettyKey: with a descriptor, keys name the table and index and
// type every datum; outside the table the shape-based rendering applies.
func TestPrettyKey(t *testing.T) {
	desc := &catalog.TableDescriptor{ID: 3, Name: "orders", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "city", Type: types.String},
		{ID: 3, Name: "at", Type: types.Timestamp},
	}, PrimaryKey: []catalog.ColumnID{1}, Indexes: []catalog.IndexDescriptor{
		{ID: 2, Name: "by_city", ColumnIDs: []catalog.ColumnID{2}},
	}}
	pk, err := EncodePK(desc, []types.Datum{types.NewInt(42)})
	if err != nil {
		t.Fatal(err)
	}
	entry, _, _, err := EncodeIndexEntry(desc, &desc.Indexes[0], map[catalog.ColumnID]types.Datum{1: types.NewInt(42), 2: types.NewString("oslo")})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		key  keys.Key
		want string
	}{
		{keys.TableDataPrefix(3), "/table/orders"},
		{keys.TableDataPrefix(3).PrefixEnd(), "/table/4"},
		{pk, "/table/orders/primary/42"},
		{entry, `/table/orders/by_city/"oslo"/42`},
		{keys.TableIndexPrefix(3, 9), "/table/orders/9"},
		{keys.TableDescKey(3), "/system/desc/3"},
	} {
		if got := PrettyKey(desc, tc.key); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.key.Raw(), got, tc.want)
		}
	}
}
