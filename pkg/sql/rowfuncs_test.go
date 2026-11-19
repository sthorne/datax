package sql

import (
	"errors"
	"strconv"
	"testing"

	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestRowFuncsUnreachableFromSQL: the per-row closures every session
// registers share one process-wide registry, so a client must never
// reach it. A call naming the hook, or any other internal name with a
// ':', is refused by the parser with 42883, and ids are random, so a
// closure another session registered cannot be named or guessed.
func TestRowFuncsUnreachableFromSQL(t *testing.T) {
	called := false
	id := storeRowFunc(func([]types.Datum) (types.Datum, error) {
		called = true
		return types.NewString("another session's closure"), nil
	})
	defer rowFuncs.Delete(id)

	for _, q := range []string{
		`SELECT "` + rowSessionFunc + `"(` + strconv.FormatInt(id, 10) + `)`,
		`SELECT "` + rowSessionFunc + `"(` + strconv.FormatInt(id, 10) + `, 'x')`,
		`SELECT 1 WHERE "` + rowSessionFunc + `"(` + strconv.FormatInt(id, 10) + `) IS NULL`,
		`SELECT "` + unrecognizedSettingFunc + `"('nosuch')`,
	} {
		_, err := parser.Parse(q)
		var se *parser.SyntaxError
		if !errors.As(err, &se) || se.Code != "42883" {
			t.Errorf("%s: %v, want 42883", q, err)
		}
	}
	if called {
		t.Fatal("a client query reached a registered closure")
	}

	// Ids are random: consecutive registrations are not neighbours.
	a := storeRowFunc(func([]types.Datum) (types.Datum, error) { return types.DNull, nil })
	b := storeRowFunc(func([]types.Datum) (types.Datum, error) { return types.DNull, nil })
	defer rowFuncs.Delete(a)
	defer rowFuncs.Delete(b)
	if a <= 0 || b <= 0 || b-a == 1 || a-b == 1 {
		t.Fatalf("ids %d, %d: want random positive ids", a, b)
	}
}
