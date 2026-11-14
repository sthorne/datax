package kvserver

import (
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/events"
)

// recordKeyed records an event whose summary carries keys — a split, a
// merge — in two forms: format with every keys.Key argument rendered in
// full (the table by name, the boundary's row values decoded), and the
// same with every key at its table-and-index prefix. The ring serves the
// first to a reader who may see every table the keys belong to and the
// second to one who may not; a boundary key is a row value, and the
// event feed is open to any authenticated user (issue #213). Other
// arguments render the same in both.
//
// This layer has no catalog, so "in full" here is the key's raw form
// (/table/5/1/3). The event also carries the summary with a mark where
// each key goes and the keys themselves, so the server that serves the
// feed, which has the catalog, can print them the way it prints a
// range's bounds (issue #280).
func recordKeyed(ring *events.Ring, kind, format string, args ...any) {
	full := make([]any, len(args))
	prefix := make([]any, len(args))
	marked := make([]any, len(args))
	var tables []uint64
	var ks [][]byte
	for i, a := range args {
		k, ok := a.(keys.Key)
		if !ok {
			full[i], prefix[i], marked[i] = a, a, a
			continue
		}
		full[i], prefix[i], marked[i] = k.String(), keys.PrettyPrefix(k), events.KeyMark
		ks = append(ks, append([]byte(nil), k...))
		if id, ok := keys.TableIDOf(k); ok {
			tables = append(tables, id)
		}
	}
	ring.RecordKeyed(kind, tables, fmt.Sprintf(format, full...), fmt.Sprintf(format, prefix...), fmt.Sprintf(format, marked...), ks)
}
