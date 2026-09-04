package keys

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/util/decimal"
	"github.com/sthorne/datax/pkg/util/encoding"
)

func TestKeyspaceOrdering(t *testing.T) {
	// Local < meta < system < table < max.
	ordered := []Key{
		TransactionKey(Key("user"), uuid.Nil),
		RaftHardStateKey(base.RangeID(1)),
		RaftLogKey(base.RangeID(1), 1),
		RaftLogKey(base.RangeID(1), 2),
		RaftLogKey(base.RangeID(2), 1),
		StoreClusterVersionKey(), // "store-cluster-version" < "store-ident"
		StoreIdentKey(),
		MinKey,
		RangeMetaKey(Key("a")),
		ClusterVersionKey(), // "cluster-version" < "desc" < "nodes" within /system
		TableDescKey(1),
		NodeRegistryKey(1),
		TableDataPrefix(1),
		TableDataPrefix(2),
		MaxKey,
	}
	for i := 1; i < len(ordered); i++ {
		if bytes.Compare(ordered[i-1], ordered[i]) >= 0 {
			t.Fatalf("order violated at %d: %q >= %q", i, []byte(ordered[i-1]), []byte(ordered[i]))
		}
	}
}

func TestAddr(t *testing.T) {
	user := Key("some/user/key")
	if a, err := Addr(user); err != nil || !a.Equal(user) {
		t.Fatalf("global key addresses itself: %v %v", a, err)
	}
	txnKey := TransactionKey(user, uuid.New())
	a, err := Addr(txnKey)
	if err != nil || !a.Equal(user) {
		t.Fatalf("txn key addr: got %v, %v", a, err)
	}
	if !IsLocal(txnKey) || IsLocal(user) {
		t.Fatal("IsLocal misclassifies")
	}
	if _, err := Addr(RaftHardStateKey(1)); err == nil {
		t.Fatal("store-local keys must not be addressable")
	}
}

func TestRangeLocalAddressedSpan(t *testing.T) {
	start, end := Key("b"), Key("d")
	lo, hi := RangeLocalAddressedSpan(start, end)
	inRange := TransactionKey(Key("c"), uuid.New())
	before := TransactionKey(Key("a"), uuid.New())
	atEnd := TransactionKey(Key("d"), uuid.New())
	if !(bytes.Compare(lo, inRange) <= 0 && bytes.Compare(inRange, hi) < 0) {
		t.Fatal("in-range txn key outside span")
	}
	if bytes.Compare(before, lo) >= 0 {
		t.Fatal("before-range txn key inside span")
	}
	if bytes.Compare(atEnd, hi) < 0 {
		t.Fatal("at-end txn key inside span")
	}
	// A txn key anchored exactly at start is included.
	atStart := TransactionKey(Key("b"), uuid.New())
	if bytes.Compare(lo, atStart) > 0 {
		t.Fatal("at-start txn key outside span")
	}
}

func TestPrefixEnd(t *testing.T) {
	if got := Key([]byte{0x03, 0x41}).PrefixEnd(); !bytes.Equal(got, []byte{0x03, 0x42}) {
		t.Fatalf("got %x", got)
	}
	if got := Key([]byte{0x03, 0xff}).PrefixEnd(); !bytes.Equal(got, []byte{0x04}) {
		t.Fatalf("carry: got %x", got)
	}
	p := TableDataPrefix(7)
	row := append(p.Clone(), []byte("anything")...)
	if !(bytes.Compare(p, row) <= 0 && bytes.Compare(row, p.PrefixEnd()) < 0) {
		t.Fatal("row outside its table span")
	}
}

func TestTableStatsKey(t *testing.T) {
	k := TableStatsKey(42)
	id, ok := TableStatsID(k)
	if !ok || id != 42 {
		t.Fatalf("round trip: %d, %v", id, ok)
	}
	lo, hi := TableStatsSpan()
	if k.Compare(lo) < 0 || k.Compare(hi) >= 0 {
		t.Fatalf("key outside span")
	}
	// Ordered by table ID within the span.
	if TableStatsKey(1).Compare(TableStatsKey(2)) >= 0 {
		t.Fatal("not ordered")
	}
	// Foreign keys don't decode.
	if _, ok := TableStatsID(TableDescKey(42)); ok {
		t.Fatal("desc key decoded as stats key")
	}
}

// TestPretty: every kind of key renders as a readable path, and the
// datum heuristics tell strings, integers and decimals apart.
func TestPretty(t *testing.T) {
	row := Key(encoding.EncodeInt64(TableIndexPrefix(3, 1), 42))
	row = Key(encoding.EncodeString(row, "oslo"))
	dec := Key(encoding.EncodeDecimal(TableIndexPrefix(3, 2), mustDec("12.5")))
	txn := TransactionKey(TableDataPrefix(3), uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
	for _, tc := range []struct {
		key  Key
		want string
	}{
		{MinKey, "/Min"},
		{MaxKey, "/Max"},
		{RangeMetaKey(MaxKey), "/meta/Max"},
		{RangeMetaKey(TableDataPrefix(3)), "/meta/table/3"},
		{ClusterVersionKey(), "/system/cluster-version"},
		{NodeRegistryKey(2), "/system/nodes/2"},
		{DescIDGenKey(), `/system/idgen/"desc"`},
		{TableDescKey(7), "/system/desc/7"},
		{TableNamespaceKey(1, "orders"), `/system/nsdb/1/"orders"`},
		{TableDataPrefix(3), "/table/3"},
		{TableDataPrefix(3).PrefixEnd(), "/table/4"},
		{TableIndexPrefix(3, 1), "/table/3/1"},
		{row, `/table/3/1/42/"oslo"`},
		{dec, "/table/3/2/12.5"},
		{RaftLogKey(5, 9), "/local/r5/log/9"},
		{RangeDescriptorKey(5), "/local/r5/desc"},
		{txn, "/local/addressed/table/3/txn/6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
	} {
		if got := tc.key.String(); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.key.Raw(), got, tc.want)
		}
	}
}

func mustDec(s string) decimal.Dec {
	d, err := decimal.Parse(s)
	if err != nil {
		panic(err)
	}
	return d
}
