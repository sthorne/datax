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
	return corrScope{
		inner:  scopeLevel{desc: inner, alias: "emp"},
		outers: []scopeLevel{{desc: outer, alias: "d"}},
	}
}

// TestCorrScopeClassify: the scope-stack resolution matrix — nearer scopes
// shadow farther ones for bare names, qualifiers pin a scope, typos are
// 42703, unknown qualifiers are 42P01.
func TestCorrScopeClassify(t *testing.T) {
	sc := testScope()
	cases := []struct {
		name    string
		level   int
		errCode string
	}{
		{name: "dept_id", level: 0},              // bare, inner only
		{name: "region", level: 1},               // bare, outer only → correlated
		{name: "id", level: 0},                   // bare, both → inner shadows
		{name: "emp.id", level: 0},               // inner-qualified
		{name: "d.id", level: 1},                 // outer-alias-qualified
		{name: "depts.region", errCode: "42P01"}, // an aliased table answers to its alias only (PostgreSQL)
		{name: "emp.region", errCode: "42703"},   // inner-qualified, not an inner column
		{name: "d.dept_id", errCode: "42703"},    // outer-qualified, not an outer column
		{name: "nosuch", errCode: "42703"},       // bare, neither scope
		{name: "other.id", errCode: "42P01"},     // unknown qualifier
	}
	for _, tc := range cases {
		level, err := sc.classify(tc.name)
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
		if level != tc.level {
			t.Fatalf("%s: level=%d, want %d", tc.name, level, tc.level)
		}
	}
}

// TestCorrScopeStack: with two enclosing scopes, bare names resolve to the
// NEAREST scope that has them, and qualifiers reach any level.
func TestCorrScopeStack(t *testing.T) {
	grand := &catalog.TableDescriptor{Name: "regions", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "zone", Type: types.String},
	}}
	mid := &catalog.TableDescriptor{Name: "depts", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "region_id", Type: types.Int},
	}}
	inner := &catalog.TableDescriptor{Name: "emp", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "dept_id", Type: types.Int},
	}}
	sc := corrScope{
		inner:  scopeLevel{desc: inner, alias: "emp"},
		outers: []scopeLevel{{desc: mid, alias: "d"}, {desc: grand, alias: "r"}},
	}
	for _, tc := range []struct {
		name  string
		level int
	}{
		{"id", 0},        // all three have it: innermost wins
		{"region_id", 1}, // mid only
		{"zone", 2},      // grandparent only
		{"d.id", 1},      // qualified to mid
		{"r.id", 2},      // qualified to grandparent
	} {
		level, err := sc.classify(tc.name)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if level != tc.level {
			t.Fatalf("%s: level=%d, want %d", tc.name, level, tc.level)
		}
	}
	if sc.bindLevel() != 2 {
		t.Fatalf("bindLevel=%d, want 2", sc.bindLevel())
	}
}

// TestSubstituteSubImmutable: substitution never mutates the source AST
// (prepared statements re-execute it) and produces the expected literal
// splice, including the mirrored outer-on-the-left transform.
func TestSubstituteSubImmutable(t *testing.T) {
	node := &scopeNode{scope: testScope()}
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
	got, err := substituteSub(sub, node, row, nil)
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
	got, err = substituteSub(sub, node, nullRow, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Where[2].Op != "FALSE" {
		t.Fatalf("NULL constant fold: %+v", got.Where[2])
	}
}

// TestSubstituteNestedKeepsIntermediate: binding the OUTERMOST level of a
// two-deep tree substitutes only its refs — references to the middle
// query stay symbolic in the nested clone, and the source tree is
// untouched.
func TestSubstituteNestedKeepsIntermediate(t *testing.T) {
	grand := &catalog.TableDescriptor{Name: "regions", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "zone", Type: types.String},
	}}
	midDesc := &catalog.TableDescriptor{Name: "depts", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "region_id", Type: types.Int},
	}}
	innerDesc := &catalog.TableDescriptor{Name: "emp", Columns: []catalog.Column{
		{ID: 1, Name: "id", Type: types.Int},
		{ID: 2, Name: "dept_id", Type: types.Int},
	}}

	stmts, err := parser.Parse(`SELECT 1 FROM depts d WHERE region_id = r.id AND EXISTS (SELECT 1 FROM emp WHERE dept_id = d.id AND id > r.id)`)
	if err != nil {
		t.Fatal(err)
	}
	mid := stmts[0].(*parser.Select)
	nested := mid.Where[1].Sub

	midNode := &scopeNode{
		scope: corrScope{
			inner:  scopeLevel{desc: midDesc, alias: "d"},
			outers: []scopeLevel{{desc: grand, alias: "r"}},
		},
	}
	nestedNode := &scopeNode{
		scope: corrScope{
			inner:  scopeLevel{desc: innerDesc, alias: "emp"},
			outers: []scopeLevel{{desc: midDesc, alias: "d"}, {desc: grand, alias: "r"}},
		},
	}
	midNode.children = map[*parser.Select]*scopeNode{nested: nestedNode}

	row := map[catalog.ColumnID]types.Datum{1: types.NewInt(9), 2: types.NewString("z")}
	got, err := substituteSub(mid, midNode, row, nil)
	if err != nil {
		t.Fatal(err)
	}
	// region_id = r.id → literal 9 on the right.
	if got.Where[0].Value.Lit == nil || got.Where[0].Value.Lit.I != 9 {
		t.Fatalf("mid splice: %+v", got.Where[0])
	}
	// Nested: dept_id = d.id stays symbolic; id > r.id → id > 9.
	ns := got.Where[1].Sub
	if ns == nested {
		t.Fatal("nested sub not cloned")
	}
	if ns.Where[0].Value.Column != "d.id" {
		t.Fatalf("intermediate ref substituted early: %+v", ns.Where[0])
	}
	if ns.Where[1].Value.Lit == nil || ns.Where[1].Value.Lit.I != 9 {
		t.Fatalf("grandparent ref not substituted: %+v", ns.Where[1])
	}
	// Source untouched.
	if nested.Where[1].Value.Lit != nil || nested.Where[0].Value.Column != "d.id" {
		t.Fatalf("source nested AST mutated: %+v", nested.Where)
	}
}
