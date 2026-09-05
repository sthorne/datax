package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// Batched point reads (issue #103). The DML paths need many point reads
// whose keys are known before any of them is needed — a statement's
// primary-key and unique-index uniqueness probes, its foreign-key parent
// lookups, an index join's primary rows — and used to issue them one
// routed round trip at a time. A probeSet collects the keys, reads them
// in ONE routed batch (Txn.GetBatch fans the per-range sub-batches out
// in parallel) and answers the per-row code from the cache; a key the
// set was not primed with falls back to a single read, so every caller
// stays correct whether or not it was primed. The values are what the
// transaction would read at its timestamp, buffered writes of the same
// statement excluded — exactly what Txn.Get returns — so the intra-
// statement bookkeeping (inserted, seen) is unchanged.
type probeSet struct {
	vals map[string][]byte
}

// newProbeSet reads every key in one batch (duplicates read once).
func newProbeSet(ctx context.Context, txn *kvclient.Txn, ks []keys.Key) (*probeSet, error) {
	ps := &probeSet{vals: make(map[string][]byte, len(ks))}
	uniq := ks[:0:0]
	for _, k := range ks {
		if _, dup := ps.vals[string(k)]; dup {
			continue
		}
		ps.vals[string(k)] = nil
		uniq = append(uniq, k)
	}
	if len(uniq) == 0 {
		return ps, nil
	}
	vals, err := txn.GetBatch(ctx, uniq)
	if err != nil {
		return nil, err
	}
	for i, k := range uniq {
		ps.vals[string(k)] = vals[i]
	}
	return ps, nil
}

// get answers from the cache, or reads the key when it was not primed.
// A nil set always reads.
func (ps *probeSet) get(ctx context.Context, txn *kvclient.Txn, k keys.Key) ([]byte, error) {
	if ps != nil {
		if v, ok := ps.vals[string(k)]; ok {
			return v, nil
		}
	}
	return txn.Get(ctx, k)
}

// primaryFetchPage is how many primary rows an index join fetches per
// batch: the index scan's entries are taken in pages of this many, each
// page's rows come back in one routed batch, in the page's order, and
// only one page of rows is held while the next is fetched.
const primaryFetchPage = 256

// fetchPrimaryRows reads the rows behind pks in pages of primaryFetchPage,
// positionally (nil = absent), calling emit for each page in order so a
// caller can stop early.
func fetchPrimaryRows(ctx context.Context, txn *kvclient.Txn, pks []keys.Key, emit func(first int, raws [][]byte) (bool, error)) error {
	for first := 0; first < len(pks); first += primaryFetchPage {
		end := first + primaryFetchPage
		if end > len(pks) {
			end = len(pks)
		}
		if TestingBeforePrimaryFetch != nil {
			TestingBeforePrimaryFetch()
		}
		raws, err := txn.GetBatch(ctx, pks[first:end])
		if err != nil {
			return err
		}
		more, err := emit(first, raws)
		if err != nil || !more {
			return err
		}
	}
	return nil
}

// TestingBeforePrimaryFetch, when set (tests only), runs before each page
// of an index join's primary rows is fetched — a hook to move a range
// boundary between the index scan and the fetch.
var TestingBeforePrimaryFetch func()
