package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestDeeplyNestedStatementIsRefused (issue #135): a statement nested
// past the parser's limit — the shape that exhausted the goroutine stack
// and took the node down — is answered with a syntax error, and the
// session and node keep serving.
func TestDeeplyNestedStatementIsRefused(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	for _, n := range []int{2000, 120000} {
		deep := "SELECT " + strings.Repeat("(", n) + "1" + strings.Repeat(")", n)
		_, serr := trySQL(ctx, s, deep)
		if serr == nil {
			t.Fatalf("%d levels of nesting parsed", n)
		}
		if serr.Code != sql.CodeSyntaxError || !strings.Contains(serr.Msg, "nests too deeply") {
			t.Fatalf("%d levels: [%s] %s", n, serr.Code, serr.Msg)
		}
	}
	if rows := execSQL(t, ctx, s, `SELECT ((((1))))`); len(rows.Rows) != 1 {
		t.Fatalf("a shallow statement after the refusals: %d rows", len(rows.Rows))
	}
}
