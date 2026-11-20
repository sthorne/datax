package catalog

import (
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/util/log"
)

// Descriptor IDs (issue #390). Tables, sequences, types, schemas and
// databases draw their IDs from one counter, and every lookup trusts the
// ID a name entry holds: a table's name leads to its ID, and the ID to
// whatever descriptor is stored under it. An ID handed out twice — a
// counter that lost an increment, or a transaction still running after
// a conflicting one aborted it and took its increment back — would let
// the second creation write its descriptor over the first's, and the
// first table's name would then lead to the second table: a statement
// on it would plan against another table's columns and report one of
// its own missing (42703), with nothing to say why. So the counter's
// answer is checked against the catalog before it is used, and an ID
// that already names a descriptor is skipped.

// maxDescIDSkips bounds how many IDs in use one allocation steps over
// before it gives up: far more than any lost increment accounts for.
const maxDescIDSkips = 1000

// allocateDescID returns a descriptor ID that no descriptor holds, read
// in txn: the ID's descriptor keys are part of what txn must find
// unchanged at commit, so a concurrent creation under the same ID makes
// one of the two transactions restart.
func allocateDescID(ctx context.Context, txn *kvclient.Txn) (uint64, error) {
	for range maxDescIDSkips {
		n, err := txn.Increment(ctx, keys.DescIDGenKey(), 1)
		if err != nil {
			return 0, err
		}
		id := uint64(n)
		used, err := descIDInUse(ctx, txn, id)
		if err != nil {
			return 0, err
		}
		if !used {
			return id, nil
		}
		log.Warnf("descriptor id %d is already in use: the descriptor id counter is behind the catalog; skipping it", id)
	}
	return 0, fmt.Errorf("the descriptor id counter is more than %d ids behind the catalog", maxDescIDSkips)
}

// descIDInUse reports whether any kind of descriptor is stored under id.
func descIDInUse(ctx context.Context, txn *kvclient.Txn, id uint64) (bool, error) {
	vals, err := txn.GetBatch(ctx, []keys.Key{
		keys.TableDescKey(id),
		keys.SequenceDescKey(id),
		keys.TypeDescKey(id),
		keys.SchemaDescKey(id),
		keys.DatabaseDescKey(id),
	})
	if err != nil {
		return false, err
	}
	for _, v := range vals {
		if v != nil {
			return true, nil
		}
	}
	return false, nil
}
