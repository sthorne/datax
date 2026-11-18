package parser

import (
	"reflect"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// TestParsePurgeWhere: ALTER TABLE ... PURGE WHERE predicate (issue
// #375). The predicate is kept as conjuncts and as the text written; the
// text parses back to the same conjuncts; a row equality lowers to one
// equality per column; and the forms the parser cannot take are syntax
// errors. Whether a predicate names a key prefix is the executor's
// judgement, so OR, functions and other columns parse here.
func TestParsePurgeWhere(t *testing.T) {
	for _, tc := range []struct {
		sql, text string
		want      []Comparison
	}{
		{`ALTER TABLE e PURGE WHERE t = 'a'`, `t = 'a'`, []Comparison{
			{Column: "t", Op: "=", Value: Expr{Lit: strLit("a")}},
		}},
		{`ALTER TABLE e PURGE WHERE (a, b) = (1, 2);`, `(a, b) = (1, 2)`, []Comparison{
			{Column: "a", Op: "=", Value: Expr{Lit: intLit(1)}},
			{Column: "b", Op: "=", Value: Expr{Lit: intLit(2)}},
		}},
		{`alter table db.e purge where (a, b) = (1, 2) and c between 5 and 6`, `(a, b) = (1, 2) and c between 5 and 6`, []Comparison{
			{Column: "a", Op: "=", Value: Expr{Lit: intLit(1)}},
			{Column: "b", Op: "=", Value: Expr{Lit: intLit(2)}},
			{Column: "c", Op: ">=", Value: Expr{Lit: intLit(5)}},
			{Column: "c", Op: "<=", Value: Expr{Lit: intLit(6)}},
		}},
		{`ALTER TABLE e PURGE WHERE a = $1 AND b >= 10 AND b < $2`, `a = $1 AND b >= 10 AND b < $2`, []Comparison{
			{Column: "a", Op: "=", Value: Expr{Param: 1}},
			{Column: "b", Op: ">=", Value: Expr{Lit: intLit(10)}},
			{Column: "b", Op: "<", Value: Expr{Param: 2}},
		}},
	} {
		at, ok := parseOne(t, tc.sql).(*AlterTable)
		if !ok || !at.Purge {
			t.Fatalf("%s: not a purge: %+v", tc.sql, at)
		}
		if at.PurgeWhereText != tc.text {
			t.Errorf("%s: text %q, want %q", tc.sql, at.PurgeWhereText, tc.text)
		}
		if !reflect.DeepEqual(at.PurgeWhere, tc.want) {
			t.Errorf("%s: conjuncts\n %+v\nwant\n %+v", tc.sql, at.PurgeWhere, tc.want)
		}
		// The text is the clause: it parses back to the same conjuncts.
		again := parseOne(t, `ALTER TABLE e PURGE WHERE `+at.PurgeWhereText).(*AlterTable)
		if !reflect.DeepEqual(again.PurgeWhere, at.PurgeWhere) || again.PurgeWhereText != at.PurgeWhereText {
			t.Errorf("%s: the text does not round-trip: %+v", tc.sql, again.PurgeWhere)
		}
	}
	if n := CountParams(parseOne(t, `ALTER TABLE e PURGE WHERE a = $1 AND b < $2`)); n != 2 {
		t.Errorf("CountParams: %d, want 2", n)
	}
	if at := parseOne(t, `ALTER TABLE e PURGE`).(*AlterTable); at.PurgeWhere != nil || at.PurgeWhereText != "" {
		t.Errorf("a whole-table purge carries a predicate: %+v", at)
	}
	// Shapes the executor refuses still parse, so it can say why.
	if at := parseOne(t, `ALTER TABLE e PURGE WHERE t = 'a' OR t = 'b'`).(*AlterTable); len(at.PurgeWhere) != 1 || at.PurgeWhere[0].Op != "IN" {
		t.Errorf("OR: %+v", at.PurgeWhere)
	}
	for _, bad := range []string{
		`ALTER TABLE e PURGE WHERE`,
		`ALTER TABLE e PURGE WHERE (a, b) = (1)`,
		`ALTER TABLE e PURGE WHERE (a, b) = (1, 2, 3)`,
		`ALTER TABLE e PURGE WHERE (a, b) < (1, 2)`,
		`ALTER TABLE e PURGE t = 'a'`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s parsed", bad)
		}
	}
}

func strLit(s string) *types.Datum { d := types.NewString(s); return &d }
func intLit(i int64) *types.Datum  { d := types.NewInt(i); return &d }
