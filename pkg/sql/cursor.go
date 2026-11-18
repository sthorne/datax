package sql

import (
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// SQL cursors: DECLARE ... CURSOR FOR query, FETCH, MOVE and CLOSE
// (pg_dump walks pg_largeobject_metadata with one). A cursor runs its
// query when it is declared, in the transaction, and keeps the rows;
// FETCH hands them out forward only. A cursor lasts until CLOSE or the
// end of its transaction (WITH HOLD: of the session).

// CodeDuplicateCursor and CodeInvalidCursorName are PostgreSQL's.
const (
	CodeDuplicateCursor   = "42P03"
	CodeInvalidCursorName = "34000"
)

// sqlCursor is one declared cursor.
type sqlCursor struct {
	cols []ResultColumn
	rows [][]types.Datum
	pos  int
	hold bool
}

// execDeclareCursor runs DECLARE.
func (s *Session) execDeclareCursor(ctx context.Context, t *parser.DeclareCursor, params []types.Datum) (*Result, *Error) {
	if s.state != StateOpen && !t.Hold {
		return nil, newErrf(CodeNoActiveTransaction, "DECLARE CURSOR can only be used in transaction blocks")
	}
	if _, ok := s.cursors[t.Name]; ok {
		return nil, newErrf(CodeDuplicateCursor, "cursor %q already exists", t.Name)
	}
	switch t.Query.(type) {
	case *parser.Select:
	default:
		return nil, newErrf(CodeSyntaxError, "DECLARE CURSOR takes a SELECT or VALUES query")
	}
	res, serr := s.executeData(ctx, t.Query, params)
	if serr != nil {
		return nil, serr
	}
	c := &sqlCursor{cols: res.Columns, rows: res.Rows, hold: t.Hold}
	if res.Stream != nil {
		for {
			row, ok, err := res.Stream.Next(ctx)
			if err != nil {
				res.Stream.Close(ctx)
				return nil, ToSQLError(err)
			}
			if !ok {
				break
			}
			c.rows = append(c.rows, row)
		}
		res.Stream.Close(ctx)
	}
	if s.cursors == nil {
		s.cursors = map[string]*sqlCursor{}
	}
	s.cursors[t.Name] = c
	return &Result{Tag: "DECLARE CURSOR"}, nil
}

// execFetch runs FETCH and MOVE.
func (s *Session) execFetch(t *parser.Fetch) (*Result, *Error) {
	c, ok := s.cursors[t.Name]
	if !ok {
		return nil, newErrf(CodeInvalidCursorName, "cursor %q does not exist", t.Name)
	}
	n := len(c.rows) - c.pos
	if t.Count >= 0 && int64(n) > t.Count {
		n = int(t.Count)
	}
	rows := c.rows[c.pos : c.pos+n]
	c.pos += n
	if t.Move {
		return &Result{Tag: fmt.Sprintf("MOVE %d", n)}, nil
	}
	return &Result{Columns: c.cols, Rows: rows, Tag: fmt.Sprintf("FETCH %d", n)}, nil
}

// execCloseCursor runs CLOSE.
func (s *Session) execCloseCursor(t *parser.CloseCursor) (*Result, *Error) {
	if t.All {
		s.cursors = nil
		return &Result{Tag: "CLOSE CURSOR ALL"}, nil
	}
	if _, ok := s.cursors[t.Name]; !ok {
		return nil, newErrf(CodeInvalidCursorName, "cursor %q does not exist", t.Name)
	}
	delete(s.cursors, t.Name)
	return &Result{Tag: "CLOSE CURSOR"}, nil
}

// endTxnCursors drops the cursors of a transaction that ended (all but
// WITH HOLD ones, and those too when it rolled back).
func (s *Session) endTxnCursors(committed bool) {
	for name, c := range s.cursors {
		if !c.hold || !committed {
			delete(s.cursors, name)
		}
	}
}
