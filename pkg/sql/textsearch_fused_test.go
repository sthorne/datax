package sql

import (
	"fmt"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// genericCond is how a conjunct was evaluated before the fused road:
// both sides as values (the vector built and rendered), then @@ as
// applyCmpOpEsc applies it. It returns WHERE's verdict (condsHold) and
// the three-valued one (cond3), each with its error.
func genericCond(cmp parser.Comparison, env exprEnv) (where bool, whereErr error, val, null bool, valErr error) {
	lhs, err := evalExprEnv(*cmp.Expr, env, nil)
	if err != nil {
		return false, err, false, false, err
	}
	rhs, err := evalExprEnv(cmp.Value, env, nil)
	if err != nil {
		return false, err, false, false, err
	}
	if lhs.Null || rhs.Null {
		return false, nil, false, true, nil
	}
	ok, err := applyCmpOpEsc(cmp.Op, lhs, rhs, "")
	return err == nil && ok, nil, ok, false, err
}

// TestTSVectorMatchFused: `to_tsvector(config, text) @@ query` read
// straight into the query gives what building the vector gave — the
// rows, NULL, and every error, in WHERE (where a failing @@ is no
// match) and as a value (where it is the error).
func TestTSVectorMatchFused(t *testing.T) {
	desc := textSearchTestDesc()
	bodies := []types.Datum{
		types.NewString("The fat cats sat on the mat"), types.NewString("a fat rat"), types.NewString("cat in the hat"),
		types.NewString("Connecting the connections"), types.NewString(""), types.NewString("the and of"), types.DNull,
	}
	titles := []types.Datum{types.NewString("fat & cat"), types.NewString("hat <-> !cat"), types.NewString("bad &"), types.DNull}
	conds := []string{
		"to_tsvector('english', body) @@ to_tsquery('english', 'cat')",
		"to_tsvector('english', body) @@ to_tsquery('english', 'fat & !rat')",
		"to_tsvector('english', body) @@ to_tsquery('english', 'cat <-> hat')",
		"to_tsvector('english', body) @@ to_tsquery('english', 'connect:*')",
		"to_tsvector('english', body) @@ to_tsquery('english', 'fat:A')",
		"to_tsvector('english', body) @@ to_tsquery('english', 'the')",
		"to_tsvector('simple', body) @@ to_tsquery('simple', 'the & cat')",
		"to_tsvector('english', body) @@ plainto_tsquery('english', 'fat cats')",
		"to_tsvector('english', body) @@ phraseto_tsquery('english', 'cat in the hat')",
		"to_tsvector('english', body) @@ websearch_to_tsquery('english', 'fat -rat')",
		"to_tsvector('english', body) @@ 'cat & rat'",
		"to_tsvector('english', body) @@ 'bad &'",
		"to_tsvector('english', body) @@ to_tsquery('english', title)",
		"to_tsvector('english', body) @@ title::tsquery",
		"to_tsvector('english', body) @@ to_tsquery('english', NULL)",
		"to_tsvector('nope', body) @@ to_tsquery('english', 'cat')",
		"to_tsvector('english', NULL) @@ to_tsquery('english', 'cat')",
		"to_tsvector('english', body) @@ 42",
		"to_tsvector('english', id) @@ to_tsquery('english', '5')",
		"NOT (to_tsvector('english', body) @@ to_tsquery('english', 'cat'))",
	}
	fused := 0
	for _, c := range conds {
		where := whereOf(t, c)
		if len(where) != 1 {
			t.Fatalf("%s: %d conjuncts", c, len(where))
		}
		cmp := where[0]
		if !tsVectorMatchCall(&cmp) {
			continue
		}
		fused++
		for _, body := range bodies {
			for _, title := range titles {
				row := map[catalog.ColumnID]types.Datum{1: types.NewInt(5), 3: body, 4: title}
				env := tableEnv{desc: desc, row: row}
				where, whereErr, val, null, valErr := genericCond(cmp, env)
				name := fmt.Sprintf("%s [body %v, title %v]", c, body, title)
				got, err := matchesWhere([]parser.Comparison{cmp}, desc, row, nil)
				if got != where || fmt.Sprint(err) != fmt.Sprint(whereErr) {
					t.Errorf("%s: WHERE %v, %v; want %v, %v", name, got, err, where, whereErr)
				}
				gv, gnull, gerr := cond3(cmp, env, nil)
				if fmt.Sprint(gerr) != fmt.Sprint(valErr) || (gerr == nil && (gnull != null || (!null && gv != val))) {
					t.Errorf("%s: value %v (null %v), %v; want %v (null %v), %v", name, gv, gnull, gerr, val, null, valErr)
				}
			}
		}
	}
	if fused < len(conds)-1 {
		t.Fatalf("only %d of %d conditions took the fused road", fused, len(conds))
	}
}

// TestFoldTSQueryCall: a tsquery made from constants is made once, at
// resolve time; one that reads a row, or fails, is left to the row.
func TestFoldTSQueryCall(t *testing.T) {
	lit := func(s string) parser.Expr { d := types.NewString(s); return parser.Expr{Lit: &d} }
	call := parser.Expr{Func: "to_tsquery", Args: []parser.Expr{lit("english"), lit("Cats & !hats")}}
	out, ok := foldTSQueryCall(call, nil)
	if !ok || out.Lit == nil || out.Func != "" || out.Lit.Fam != types.Tsquery || out.Lit.S != "'cat' & !'hat'" {
		t.Fatalf("constant call: %+v, %v", out, ok)
	}
	param := parser.Expr{Func: "plainto_tsquery", Args: []parser.Expr{lit("english"), {Param: 1}}}
	if out, ok := foldTSQueryCall(param, []types.Datum{types.NewString("fat rats")}); !ok || out.Lit.S != "'fat' & 'rat'" {
		t.Fatalf("parameter: %+v, %v", out, ok)
	}
	for name, e := range map[string]parser.Expr{
		"column":      {Func: "to_tsquery", Args: []parser.Expr{lit("english"), {Column: "title"}}},
		"syntax":      {Func: "to_tsquery", Args: []parser.Expr{lit("english"), lit("bad &")}},
		"config":      {Func: "to_tsquery", Args: []parser.Expr{lit("nope"), lit("cat")}},
		"not a query": {Func: "lower", Args: []parser.Expr{lit("A")}},
	} {
		if out, ok := foldTSQueryCall(e, nil); ok || out.Lit != nil {
			t.Errorf("%s: folded to %+v", name, out)
		}
	}
}
