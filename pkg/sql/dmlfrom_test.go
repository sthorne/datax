package sql

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestDMLTargetFirst: where an UPDATE ... FROM / DELETE ... USING joins
// its target. Last — looked up per row of the FROM list — when the WHERE
// equates the leading column of its key or of an index with the list,
// or the list has joins of its own (they must not see the target);
// first otherwise, so an unindexed join column scans the target once.
func TestDMLTargetFirst(t *testing.T) {
	desc := &catalog.TableDescriptor{Name: "orders", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "uid", Type: types.Int},
		{ID: 3, Name: "sku", Type: types.String},
	}, PrimaryKey: []catalog.ColumnID{1}, Indexes: []catalog.IndexDescriptor{{Name: "by_sku", ColumnIDs: []catalog.ColumnID{3}}}}
	parse := func(q string) *parser.Update {
		t.Helper()
		stmts, err := parser.Parse(q)
		if err != nil {
			t.Fatal(err)
		}
		return stmts[0].(*parser.Update)
	}
	for q, first := range map[string]bool{
		`UPDATE orders o SET uid = 1 FROM staging s WHERE o.id = s.id`:                    false, // key lookup into the target
		`UPDATE orders o SET uid = 1 FROM staging s WHERE s.id = o.id`:                    false,
		`UPDATE orders o SET uid = 1 FROM staging s WHERE s.sku = o.sku`:                  false, // index lookup
		`UPDATE orders SET uid = 1 FROM staging s WHERE orders.id = s.id`:                 false,
		`UPDATE orders SET uid = 1 FROM staging s WHERE id = s.k`:                         false,
		`UPDATE orders o SET uid = 1 FROM users u WHERE o.uid = u.id`:                     true, // uid unindexed: scan orders once
		`UPDATE orders o SET uid = 1 FROM users u`:                                        true,
		`UPDATE orders o SET uid = 1 FROM users u WHERE o.id = 5`:                         true,
		`UPDATE orders o SET uid = 1 FROM users u WHERE o.id = o.uid`:                     true,
		`UPDATE orders o SET uid = 1 FROM a JOIN b ON b.k = a.k WHERE o.uid = a.k`:        false, // the list's joins
		`UPDATE orders o SET uid = 1 FROM a LEFT JOIN b ON b.k = a.k WHERE o.uid = a.k`:   false,
		`UPDATE orders o SET uid = 1 FROM a, LATERAL (SELECT 1 AS x) l WHERE o.uid = a.k`: false,
	} {
		u := parse(q)
		if got := dmlTargetFirst(desc, u.Alias, u.From, u.Where); got != first {
			t.Errorf("%s: target first = %v, want %v", q, got, first)
		}
	}
}
