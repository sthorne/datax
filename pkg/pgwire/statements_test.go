package pgwire

import (
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql/parser"
)

func shape(t *testing.T, src string) parser.Shape {
	t.Helper()
	stmts, err := parser.Parse(src)
	if err != nil {
		t.Fatalf("parse %q: %v", src, err)
	}
	return parser.Fingerprint(stmts[0])
}

// TestStatementsGroupAndRank (issue #157): executions of one shape are
// one row, and the ranking is by TOTAL time — the whole premise is that
// a fast statement run often outranks a slow one run rarely.
func TestStatementsGroupAndRank(t *testing.T) {
	s := newStatements()
	fast := shape(t, `SELECT * FROM accounts WHERE id = 1`)
	slow := shape(t, `SELECT * FROM items`)

	// 400 executions at 8ms = 3.2s; one at 900ms.
	for i := 0; i < 400; i++ {
		s.record(fast, "select", fmt.Sprintf("SELECT * FROM accounts WHERE id = %d", i),
			8*time.Millisecond, 1, 1, false, false)
	}
	s.record(slow, "select", "SELECT * FROM items", 900*time.Millisecond, 5000, 90000, false, false)

	top, evicted := s.Top(10)
	if evicted != 0 {
		t.Errorf("nothing should have been evicted: %d", evicted)
	}
	if len(top) != 2 {
		t.Fatalf("400 executions of one shape and one of another are two rows, got %d: %+v", len(top), top)
	}
	if top[0].Fingerprint != fast.Hash {
		t.Fatalf("the shape costing the most total time must rank first; got %q (%dus) over %q (%dus)",
			top[0].Shape, top[0].TotalMicros, top[1].Shape, top[1].TotalMicros)
	}
	if top[0].Count != 400 {
		t.Errorf("count %d, want 400", top[0].Count)
	}
	if top[0].MeanMicros != 8000 {
		t.Errorf("mean %dus, want 8000", top[0].MeanMicros)
	}
	if top[0].RowsReturned != 400 || top[0].RowsScanned != 400 {
		t.Errorf("rows returned %d scanned %d, want 400/400", top[0].RowsReturned, top[0].RowsScanned)
	}
	// The row names the shape, not one of the four hundred texts.
	if top[0].Shape != fast.Text {
		t.Errorf("shape %q, want %q", top[0].Shape, fast.Text)
	}
	if top[1].RowsScanned != 90000 {
		t.Errorf("the scan's rows are charged to the scan: %+v", top[1])
	}
}

// Retries and errors attribute to the shape that produced them.
func TestStatementsAttributeRetriesAndErrors(t *testing.T) {
	s := newStatements()
	a := shape(t, `UPDATE accounts SET balance = 1 WHERE id = 1`)
	b := shape(t, `SELECT * FROM items`)
	s.record(a, "update", "UPDATE accounts SET balance = 1 WHERE id = 1", time.Millisecond, 1, 1, true, true)
	s.record(a, "update", "UPDATE accounts SET balance = 2 WHERE id = 2", time.Millisecond, 1, 1, false, false)
	s.record(b, "select", "SELECT * FROM items", time.Millisecond, 1, 1, false, false)

	top, _ := s.Top(0)
	byPrint := map[string]StatementStat{}
	for _, st := range top {
		byPrint[st.Fingerprint] = st
	}
	if got := byPrint[a.Hash]; got.Retries != 1 || got.Errors != 1 || got.Count != 2 {
		t.Errorf("the update: retries %d errors %d count %d, want 1/1/2", got.Retries, got.Errors, got.Count)
	}
	if got := byPrint[b.Hash]; got.Retries != 0 || got.Errors != 0 {
		t.Errorf("the select did not retry or fail: %+v", got)
	}
}

// TestStatementsBounded (issue #157): a client that inlines its
// parameters can generate unbounded shapes, and an accounting table that
// grows with what clients send is a leak with a nice name. The bound
// holds, and what it drops is counted rather than silently lost.
func TestStatementsBounded(t *testing.T) {
	s := newStatements()
	// Far more distinct shapes than the bound. Distinct columns, so the
	// shapes really differ rather than collapsing.
	const flood = stmtShapeMax * 3
	for i := 0; i < flood; i++ {
		src := fmt.Sprintf(`SELECT c%d FROM t WHERE id = 1`, i)
		s.record(shape(t, src), "select", src, time.Millisecond, 1, 1, false, false)
	}
	top, evicted := s.Top(0)
	if len(top) > stmtShapeMax {
		t.Fatalf("the table grew to %d shapes, past its bound of %d", len(top), stmtShapeMax)
	}
	if evicted == 0 {
		t.Fatal("shapes were dropped and none were counted: the view would claim the list is complete")
	}
	if uint64(len(top))+evicted != flood {
		t.Errorf("%d kept + %d evicted != %d recorded", len(top), evicted, flood)
	}
	// The survivors are the most recent, because eviction is by least
	// recently executed.
	last := shape(t, fmt.Sprintf(`SELECT c%d FROM t WHERE id = 1`, flood-1))
	found := false
	for _, st := range top {
		if st.Fingerprint == last.Hash {
			found = true
		}
	}
	if !found {
		t.Error("the most recently executed shape was evicted")
	}
}

// A shape executed again is refreshed in the LRU, so a busy shape is not
// evicted by a flood of one-off ones.
func TestStatementsBusyShapeSurvivesFlood(t *testing.T) {
	s := newStatements()
	busy := shape(t, `SELECT * FROM accounts WHERE id = 1`)
	s.record(busy, "select", "SELECT * FROM accounts WHERE id = 1", time.Millisecond, 1, 1, false, false)
	for i := 0; i < stmtShapeMax*2; i++ {
		src := fmt.Sprintf(`SELECT c%d FROM t`, i)
		s.record(shape(t, src), "select", src, time.Millisecond, 1, 1, false, false)
		// Kept warm by continuing to run, as a busy shape would be.
		s.record(busy, "select", "SELECT * FROM accounts WHERE id = 1", time.Millisecond, 1, 1, false, false)
	}
	top, _ := s.Top(0)
	for _, st := range top {
		if st.Fingerprint == busy.Hash {
			return
		}
	}
	t.Fatal("a shape executing throughout the flood was evicted anyway")
}

// The representative is available for EXPLAIN while the shape is held,
// and honestly absent once it is not.
func TestStatementsRepresentative(t *testing.T) {
	s := newStatements()
	sh := shape(t, `SELECT * FROM accounts WHERE id = 1`)
	s.record(sh, "select", "SELECT * FROM accounts WHERE id = 42", time.Millisecond, 1, 1, false, false)
	if got := s.Representative(sh.Hash); got != "SELECT * FROM accounts WHERE id = 42" {
		t.Errorf("representative %q", got)
	}
	if got := s.Representative("nosuchshape"); got != "" {
		t.Errorf("an unknown fingerprint must not resolve to a statement: %q", got)
	}
}

// Percentiles come from the shape's own ring, and a shape with one
// sample reports it rather than zero.
func TestStatementsPercentiles(t *testing.T) {
	s := newStatements()
	sh := shape(t, `SELECT * FROM accounts WHERE id = 1`)
	for i := 0; i < 100; i++ {
		d := time.Duration(i+1) * time.Millisecond
		s.record(sh, "select", "SELECT * FROM accounts WHERE id = 1", d, 1, 1, false, false)
	}
	top, _ := s.Top(1)
	st := top[0]
	if st.P50Micros <= 0 || st.P99Micros < st.P50Micros {
		t.Fatalf("p50 %d p99 %d", st.P50Micros, st.P99Micros)
	}
	if st.MaxMicros != 100000 {
		t.Errorf("max %dus, want 100000", st.MaxMicros)
	}
}
