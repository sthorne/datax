package sql

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"strconv"
	"sync"

	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// rowSessionFunc is the function a session-aware call whose arguments
// read the row is rewritten to (see resolveValueNode): its first
// argument is the id of a closure the session registered, which
// evalFunc calls with the remaining arguments, once per row.
//
// The case that needs it is pg_dump's
//
//	SELECT set_config(name, 'view, foreign-table', false)
//	FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'
//
// whose set_config takes its name from each row. evalFunc has no
// session, so the closure carries it. The registry is shared by every
// session, so a client must never reach it: the parser refuses any
// call whose name holds a ':' (42883), and ids are random rather than
// sequential, so one session cannot guess another's.
const rowSessionFunc = "datax:row_session_func"

var rowFuncs sync.Map // int64 -> func([]types.Datum) (types.Datum, error)

// storeRowFunc stores fn under an unused random positive id.
func storeRowFunc(fn func([]types.Datum) (types.Datum, error)) int64 {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			panic(err) // crypto/rand.Read does not fail on supported platforms
		}
		id := int64(binary.LittleEndian.Uint64(b[:]) &^ (1 << 63))
		if id == 0 {
			continue
		}
		if _, taken := rowFuncs.LoadOrStore(id, fn); !taken {
			return id
		}
	}
}

// registerRowFunc registers fn for this session and returns the call
// that runs it per row with args. The registration lasts until the
// session's next statement (a streamed result outlives Execute) or its
// Close.
func (s *Session) registerRowFunc(fn func([]types.Datum) (types.Datum, error), args []parser.Expr) parser.Expr {
	var mu sync.Mutex
	id := storeRowFunc(func(a []types.Datum) (types.Datum, error) {
		mu.Lock()
		defer mu.Unlock()
		return fn(a)
	})
	s.rowFuncIDs = append(s.rowFuncIDs, id)
	lit := types.NewInt(id)
	return parser.Expr{Func: rowSessionFunc, Args: append([]parser.Expr{{Lit: &lit}}, args...)}
}

// releaseRowFuncs drops the closures registerRowFunc registered.
func (s *Session) releaseRowFuncs() {
	for _, id := range s.rowFuncIDs {
		rowFuncs.Delete(id)
	}
	s.rowFuncIDs = nil
}

// callRowFunc is evalFunc's side of rowSessionFunc.
func callRowFunc(args []types.Datum) (types.Datum, error) {
	if len(args) == 0 || args[0].Null {
		return types.Datum{}, newErrf(CodeUndefinedFunction, "function %s() does not exist", rowSessionFunc)
	}
	fn, ok := rowFuncs.Load(args[0].I)
	if !ok {
		return types.Datum{}, newErrf(CodeUndefinedFunction, "function %s(%s) does not exist", rowSessionFunc, strconv.FormatInt(args[0].I, 10))
	}
	return fn.(func([]types.Datum) (types.Datum, error))(args[1:])
}

// setConfig is set_config(name, value, is_local) on evaluated
// arguments: SET (SET LOCAL when is_local), returning the value.
func (s *Session) setConfig(ctx context.Context, vals []types.Datum) (types.Datum, error) {
	if vals[0].Null {
		return types.Datum{}, newErrf(CodeInvalidParameterValue, "set_config: the setting's name is NULL")
	}
	value := ""
	if !vals[1].Null {
		value = vals[1].Text()
	}
	local, cerr := vals[2].Coerce(types.Bool)
	if cerr != nil {
		return types.Datum{}, newErrf(CodeInvalidTextRepresentation, "set_config: is_local: %v", cerr)
	}
	if _, serr := s.execSetVar(ctx, &parser.SetVar{Name: vals[0].Text(), Value: value, Local: !local.Null && local.B}); serr != nil {
		return types.Datum{}, serr
	}
	return types.NewString(value), nil
}
