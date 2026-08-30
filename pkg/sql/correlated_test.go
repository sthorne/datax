package sql

import (
	"reflect"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

func testScope() corrScope {
	outer := &catalog.TableDescriptor{Name: "depts", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "region", Type: types.String},
	}}
	inner := &catalog.TableDescriptor{Name: "emp", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "dept_id", Type: types.Int},
	}}
	return corrScope{outerDesc: outer, outerAlias: "d", innerDesc: inner, innerAlias: "emp"}
}

// TestCorrScopeClassify: the two-scope resolution matrix — inner shadows
// outer for bare names, qualifiers pin a scope, typos are 42703, unknown
// qualifiers are 42P01.
func TestCorrScopeClassify(t *testing.T) {
	sc := testScope()
	cases := []struct {
		name    string
		inner   bool
		errCode string
	}{
		{name: "dept_id", inner: true},         // bare, inner only
		{name: "region", inner: false},         // bare, outer only → correlated
		{name: "id", inner: true},              // bare, both → inner shadows
		{name: "emp.id", inner: true},          // inner-qualified
		{name: "d.id", inner: false},           // outer-alias-qualified
		{name: "depts.region", inner: false},   // outer-table-qualified
		{name: "emp.region", errCode: "42703"}, // inner-qualified, not an inner column
		{name: "d.dept_id", errCode: "42703"},  // outer-qualified, not an outer column
		{name: "nosuch", errCode: "42703"},     // bare, neither scope
		{name: "other.id", errCode: "42P01"},   // unknown qualifier
	}
	for _, tc := range cases {
		inner, err := sc.classify(tc.name)
		if tc.errCode != "" {
			serr, ok := err.(*Error)
			if !ok || string(serr.Code) != tc.errCode {
				t.Fatalf("%s: err %v, want code %s", tc.name, err, tc.errCode)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if inner != tc.inner {
			t.Fatalf("%s: inner=%v, want %v", tc.name, inner, tc.inner)
		}
	}
}

// TestSubstituteSubImmutable: substitution never mutates the source AST
// (prepared statements re-execute it) and produces the expected literal
// splice, including the mirrored outer-on-the-left transform.
func TestSubstituteSubImmutable(t *testing.T) {
	sc := testScope()
	stmts, err := parser.Parse(`SELECT 1 FROM emp WHERE dept_id = d.id AND d.id > salary_floor AND region = 'west'`)
	if err != nil {
		t.Fatal(err)
	}
	sub := stmts[0].(*parser.Select)
	// salary_floor is not a real column anywhere; make it an inner one for
	// the mirror case by renaming: use dept_id instead.
	sub.Where[1].Value = parser.Expr{Column: "dept_id"}

	before := *sub
	beforeWhere := append([]parser.Comparison(nil), sub.Where...)

	row := map[catalog.ColumnID]types.Datum{1: types.NewInt(7), 2: types.NewString("west")}
	got, err := substituteSub(sub, &sc, row, nil)
	if err != nil {
		t.Fatal(err)
	}

	// dept_id = d.id → dept_id = 7 (literal on the right).
	if got.Where[0].Value.Lit == nil || got.Where[0].Value.Lit.I != 7 || got.Where[0].Value.Column != "" {
		t.Fatalf("scalar splice: %+v", got.Where[0])
	}
	// d.id > dept_id → dept_id < 7 (mirrored).
	if got.Where[1].Column != "dept_id" || got.Where[1].Op != "<" || got.Where[1].Value.Lit.I != 7 {
		t.Fatalf("mirror transform: %+v", got.Where[1])
	}
	// region = 'west' → constant TRUE for this row.
	if got.Where[2].Op != "TRUE" {
		t.Fatalf("constant fold: %+v", got.Where[2])
	}

	// Source AST untouched.
	if !reflect.DeepEqual(*sub, before) || !reflect.DeepEqual(sub.Where, beforeWhere) {
		t.Fatal("substitution mutated the source AST")
	}

	// A NULL outer value folds an equality to FALSE.
	nullRow := map[catalog.ColumnID]types.Datum{1: types.DNull, 2: types.DNull}
	got, err = substituteSub(sub, &sc, nullRow, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Where[2].Op != "FALSE" {
		t.Fatalf("NULL constant fold: %+v", got.Where[2])
	}
}
