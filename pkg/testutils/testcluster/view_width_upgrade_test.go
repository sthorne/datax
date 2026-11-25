package testcluster

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestIntegerWidthOfOldView: a view created before views kept integer
// widths (#401) has int8 (width 0) stored for its int2 / int4 columns.
// pg_attribute, information_schema.columns and format_type derive the
// width from the view's query, as for a view created now, so such a
// view describes as PostgreSQL's would without being recreated. A
// genuine int8 column, a text column and a column of a view over such a
// view are right too, and the rows are those of the same view created
// now. Before, the old view's catalog rows said
// bigint until it was recreated.
func TestIntegerWidthOfOldView(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE ow (id INT8 PRIMARY KEY, a INT2, b INT4, c INT8, arr INT4[])`)
	execSQL(t, ctx, s, `CREATE VIEW owv AS SELECT a, b, c, arr, a::text AS t FROM ow`)
	execSQL(t, ctx, s, `CREATE VIEW owv2 AS SELECT b, c FROM owv`)
	// A view over a catalog: planning it to derive widths while the
	// catalog is gathered must not recurse.
	execSQL(t, ctx, s, `CREATE VIEW owcat AS SELECT attrelid, attnum FROM pg_attribute`)

	// Rewrite both descriptors as an older binary stored them: integer
	// columns without a width, and no ViewIntWidths mark.
	acc := catalog.NewAccessor()
	if err := tc.Nodes[0].DB().RunTxn(ctx, "old-view", func(ctx context.Context, txn *kvclient.Txn) error {
		for _, name := range []string{"owv", "owv2", "owcat"} {
			d, err := acc.LookupFreshIn(ctx, txn, catalog.DefaultDatabase, name)
			if err != nil {
				return err
			}
			c := d.Clone()
			for i := range c.Columns {
				c.Columns[i].Width = 0
			}
			c.ViewIntWidths = false
			if err := acc.Update(ctx, txn, c); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	s2 := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s2, `CREATE VIEW nwv AS SELECT a, b, c, arr, a::text AS t FROM ow`)
	rowsOf := func(view string) []string {
		t.Helper()
		return rowsText(t, ctx, s2, `SELECT a.attname, a.atttypid::int8, format_type(a.atttypid, a.atttypmod), c.data_type, c.udt_name
			FROM pg_attribute a JOIN information_schema.columns c ON c.table_name = '`+view+`' AND c.column_name = a.attname
			WHERE a.attrelid = '`+view+`'::regclass AND a.attnum > 0 ORDER BY a.attnum`)
	}
	check := func(view string, want []string) {
		t.Helper()
		if got := rowsOf(view); !reflect.DeepEqual(got, want) {
			t.Errorf("%s's catalog rows:\n got %q\nwant %q", view, got, want)
		}
	}
	check("owv", []string{
		"a|21|smallint|smallint|int2",
		"b|23|integer|integer|int4",
		"c|20|bigint|bigint|int8",
		"arr|1007|integer[]|ARRAY|_int4",
		"t|25|text|text|text",
	})
	if got, want := rowsOf("owv"), rowsOf("nwv"); !reflect.DeepEqual(got, want) {
		t.Errorf("the old view's rows %q differ from a new view's %q", got, want)
	}
	check("owv2", []string{
		"b|23|integer|integer|int4",
		"c|20|bigint|bigint|int8",
	})
	execSQL(t, ctx, s2, `CREATE VIEW nwcat AS SELECT attrelid, attnum FROM pg_attribute`)
	if got, want := rowsOf("owcat"), rowsOf("nwcat"); !reflect.DeepEqual(got, want) {
		t.Errorf("the old catalog view's rows %q differ from a new one's %q", got, want)
	}
}
