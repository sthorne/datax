package rowenc

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestEncodeValueRetypeRoundsDecimal: while ALTER COLUMN TYPE narrows a
// DECIMAL scale, the shadow column's value is the original's rounded to
// the new scale (halves away from zero), as any write into a
// DECIMAL(p,s) column is, and a value that no longer fits is 22003.
func TestEncodeValueRetypeRoundsDecimal(t *testing.T) {
	desc := &catalog.TableDescriptor{
		ID: 43, Name: "t",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", Type: types.Int},
			{ID: 2, Name: "c", Type: types.Decimal, Precision: 10, Scale: 3},
			{ID: 3, Name: "a", Type: types.ArrayOf(types.Decimal), Precision: 10, Scale: 3},
			{ID: 4, Name: "__retype_2", Type: types.Decimal, Precision: 4, Scale: 2, Hidden: true, RetypeFrom: 2},
			{ID: 5, Name: "__retype_3", Type: types.ArrayOf(types.Decimal), Precision: 4, Scale: 2, Hidden: true, RetypeFrom: 3},
		},
		PrimaryKey: []catalog.ColumnID{1},
	}
	for _, c := range []struct{ in, want string }{{"1.005", "1.01"}, {"-1.005", "-1.01"}, {"2.125", "2.13"}, {"12.344", "12.34"}} {
		row := map[catalog.ColumnID]types.Datum{
			1: types.NewInt(1),
			2: types.NewDecimal(c.in),
			3: types.NewArray(types.Decimal, []types.Datum{types.NewDecimal(c.in), types.DNull}),
		}
		if _, err := EncodeValue(desc, row); err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if got := row[4]; got.S != c.want || got.Dscale != 2 {
			t.Errorf("%s: shadow got %q (scale %d), want %q", c.in, got.S, got.Dscale, c.want)
		}
		if got := row[5]; len(got.A) != 2 || got.A[0].S != c.want || !got.A[1].Null {
			t.Errorf("%s: array shadow got %+v, want {%s,NULL}", c.in, got.A, c.want)
		}
	}
	for _, in := range []string{"99.995", "123.4"} {
		_, err := EncodeValue(desc, map[catalog.ColumnID]types.Datum{1: types.NewInt(1), 2: types.NewDecimal(in)})
		if ve, ok := err.(*catalog.ValueError); !ok || ve.Code != "22003" {
			t.Errorf("%s into DECIMAL(4,2): got %v, want 22003", in, err)
		}
	}
}
