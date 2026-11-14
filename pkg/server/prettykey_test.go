package server

import (
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// A reserved table is a span before it is a descriptor: the console's
// preferences table is created by the first write to it, and until then
// the ranges table printed its boundary as "/table/1099511627777" beside
// siblings printed by name. A key in a reserved span is named from the
// catalog's own list; any other table without a descriptor (a dropped
// one) keeps its id, which is all there is to say of it.
func TestPrettyKeyNamesAReservedTableWithNoDescriptor(t *testing.T) {
	n := &Node{}
	for name, want := range map[string]string{
		catalog.PrefsTableName:   "/table/" + catalog.PrefsTableName,
		catalog.MetricsTableName: "/table/" + catalog.MetricsTableName,
	} {
		id, ok := catalog.SystemTableID(name)
		if !ok {
			t.Fatalf("%s is not a reserved name", name)
		}
		if got := n.prettyKey(keys.TableDataPrefix(id)); got != want {
			t.Errorf("prettyKey of %s's span = %q, want %q", name, got, want)
		}
		// A key inside the span keeps what follows the table.
		inside := keys.Key(append(append([]byte(nil), keys.TableDataPrefix(id)...), 0x88))
		raw := keys.Pretty(inside)
		if got := n.prettyKey(inside); got != want+raw[len(keys.Pretty(keys.TableDataPrefix(id))):] {
			t.Errorf("prettyKey inside %s = %q; by shape it is %q", name, got, raw)
		}
	}
	dropped := keys.TableDataPrefix(4242)
	if got := n.prettyKey(dropped); got != keys.Pretty(dropped) {
		t.Errorf("prettyKey of an unknown table = %q, want its shape %q", got, keys.Pretty(dropped))
	}
}
