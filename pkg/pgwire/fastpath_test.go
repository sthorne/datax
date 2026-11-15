package pgwire

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// TestFastPathArg: arguments decode at the widths libpq sends them. An
// object id is the one unsigned four-byte value: ids are generated
// across the whole 32-bit range, and half of them have the top bit set.
func TestFastPathArg(t *testing.T) {
	for _, c := range []struct {
		kind   byte
		raw    []byte
		binary bool
		want   types.Datum
	}{
		{'i', []byte{0, 0, 0, 7}, true, types.NewInt(7)},
		{'i', []byte{0xff, 0xff, 0xff, 0xfb}, true, types.NewInt(-5)},
		{'o', []byte{0xff, 0xff, 0xff, 0xfb}, true, types.NewInt(4294967291)},
		{'o', []byte{0, 1, 0, 0}, true, types.NewInt(65536)},
		{'i', []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}, true, types.NewInt(-2)},
		{'i', []byte{0, 0, 0, 1, 0, 0, 0, 0}, true, types.NewInt(1 << 32)},
		{'i', []byte("42"), false, types.NewInt(42)},
		{'b', []byte{0, 1, 2, 0xff}, true, types.NewBytes([]byte{0, 1, 2, 0xff})},
		{'b', []byte{}, true, types.NewBytes(nil)},
		{'i', nil, true, types.DNull},
	} {
		got, err := fastPathArg(c.kind, c.raw, c.binary)
		if err != nil || got.Null != c.want.Null || got.Fam != c.want.Fam || got.I != c.want.I || got.S != c.want.S {
			t.Errorf("fastPathArg(%c, %v, binary=%v) = %+v, %v; want %+v", c.kind, c.raw, c.binary, got, err, c.want)
		}
	}
	for _, raw := range [][]byte{{1, 2, 3}, {1}, {}} {
		if _, err := fastPathArg('i', raw, true); err == nil {
			t.Errorf("an integer of %d bytes was accepted", len(raw))
		}
	}
	if _, err := fastPathArg('i', []byte("4x"), false); err == nil {
		t.Error("a text integer that is not one was accepted")
	}
}

// TestFastPathFunctionsExist: every function the fast path serves is a
// registered builtin that pg_proc lists, with the number of arguments
// the fast path will pass, and its OID resolves back to it — the three
// places a name has to agree.
func TestFastPathFunctionsExist(t *testing.T) {
	oids := map[string]int64{}
	for i := int64(1 << 28); ; i++ {
		name, ok := vtable.ProcName(i)
		if !ok {
			break
		}
		oids[name] = i
	}
	for name, fn := range fastPathFuncs {
		b, ok := builtins.Lookup(name)
		if !ok {
			t.Errorf("%s is served by the fast path and is not a builtin", name)
			continue
		}
		if !b.ArityOK(len(fn.args)) {
			t.Errorf("%s: the fast path passes %d argument(s), the builtin takes %s", name, len(fn.args), b.ArityText())
		}
		if _, ok := oids[name]; !ok {
			t.Errorf("%s has no pg_proc OID", name)
		}
	}
	if _, ok := vtable.ProcName(1<<28 - 1); ok {
		t.Error("an OID below pg_proc's range resolved to a function")
	}
}
