package sql

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/parser"
)

func parseOne(t *testing.T, q string) parser.Statement {
	t.Helper()
	stmts, err := parser.Parse(q)
	if err != nil || len(stmts) != 1 {
		t.Fatalf("parse %q: %v", q, err)
	}
	return stmts[0]
}

// TestStripTableAliasRerunAllocatesNothing: a prepared statement's
// select is stripped at every execution; once stripped, a rerun only
// reads it, and allocates nothing (it copied the whole select before).
func TestStripTableAliasRerunAllocatesNothing(t *testing.T) {
	sel := parseOne(t, `SELECT kv.v FROM kv WHERE kv.k = 1 ORDER BY kv.v`).(*parser.Select)
	rewrote, err := stripTableAliasN(sel)
	if err != nil || !rewrote {
		t.Fatalf("first strip: rewrote=%v err=%v", rewrote, err)
	}
	if got := sel.Exprs[0].Expr.Column; got != "v" {
		t.Fatalf("select item not stripped: %q", got)
	}
	if rewrote, err := stripTableAliasN(sel); err != nil || rewrote {
		t.Fatalf("rerun: rewrote=%v err=%v", rewrote, err)
	}
	if n := testing.AllocsPerRun(100, func() { _ = stripTableAlias(sel) }); n != 0 {
		t.Fatalf("rerun allocated %v times", n)
	}
}

// TestStripTableAliasScopes: the qualifiers stripped and refused, as
// before the rewrite of the strip's bookkeeping.
func TestStripTableAliasScopes(t *testing.T) {
	for _, tc := range []struct {
		q, want, err string
	}{
		{q: `SELECT kv.v FROM kv`, want: "v"},
		{q: `SELECT public.kv.v FROM public.kv`, want: "v"},
		{q: `SELECT kv.v FROM public.kv`, want: "v"},
		{q: `SELECT e.v FROM kv e`, want: "v"},
		{q: `SELECT kv.v FROM kv e`, err: "42P01"},
		{q: `SELECT other.v FROM kv`, err: "42P01"},
		{q: `SELECT v FROM kv`, want: "v"},
	} {
		sel := parseOne(t, tc.q).(*parser.Select)
		err := stripTableAlias(sel)
		if tc.err != "" {
			serr, ok := err.(*Error)
			if !ok || serr.Code != tc.err {
				t.Errorf("%s: want %s, got %v", tc.q, tc.err, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.q, err)
			continue
		}
		if got := sel.Exprs[0].Expr.Column; got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.q, got, tc.want)
		}
	}
}

// TestStripUpdateAliasKeepsIdentity: an UPDATE with nothing to strip is
// returned as is (the plan cache's key), qualified or not, run again.
func TestStripUpdateAliasKeepsIdentity(t *testing.T) {
	u := parseOne(t, `UPDATE kv SET v = $1 WHERE k = $2`).(*parser.Update)
	got, err := stripUpdateAlias(u)
	if err != nil || got != u {
		t.Fatalf("unqualified: got %p (want %p), %v", got, u, err)
	}
	q := parseOne(t, `UPDATE kv SET v = $1 WHERE kv.k = $2`).(*parser.Update)
	got, err = stripUpdateAlias(q)
	if err != nil || got == q || got.Where[0].Column != "k" {
		t.Fatalf("qualified: got %+v, %v", got, err)
	}
	if q.Where[0].Column != "kv.k" {
		t.Fatalf("the statement itself was rewritten: %q", q.Where[0].Column)
	}
}

// TestResolutionReused: the session's resolution context is built once
// and reused until its search path or its role changes.
func TestResolutionReused(t *testing.T) {
	s := &Session{user: "alice"}
	s.vars.searchPath = defaultSearchPath
	r1 := s.resolution()
	if r2 := s.resolution(); r2 != r1 {
		t.Fatal("an unchanged session built a second resolution")
	}
	if n := testing.AllocsPerRun(100, func() { _ = s.resolution() }); n != 0 {
		t.Fatalf("reuse allocated %v times", n)
	}
	s.vars.searchPath = "app, public"
	r3 := s.resolution()
	if r3 == r1 || len(r3.Path) != 2 || r3.Path[0] != "app" {
		t.Fatalf("SET search_path not seen: %+v", r3)
	}
	s.user = "bob"
	if r4 := s.resolution(); r4 == r3 || r4.User != "bob" {
		t.Fatalf("SET ROLE not seen: %+v", r4)
	}
	s.viewPath = []string{"v1"}
	if r5 := s.resolution(); len(r5.Path) != 1 || r5.Path[0] != "v1" {
		t.Fatalf("a view's path not seen: %+v", r5)
	}
	s.viewPath = nil
	if r6 := s.resolution(); len(r6.Path) != 2 || r6.User != "bob" {
		t.Fatalf("the session's path not restored after the view: %+v", r6)
	}
}
