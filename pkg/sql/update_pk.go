package sql

import (
	"bytes"
	"context"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Updating primary-key columns (issue #322).
//
// A row whose primary key an UPDATE changes moves: its old row and index
// entries are deleted and the new version written at the new key, with
// its index entries, in the statement's transaction. A sealed table's
// value is sealed afresh for the new key (the key is in its associated
// data), under the new tenant's key when the tenant column changes.
//
// Uniqueness is checked for the statement as a whole, as the SQL
// standard has it (and PostgreSQL does for a DEFERRABLE constraint):
// the new keys must differ from each other and from every key the
// statement leaves in place, but may take a key another row of the same
// statement gives up. So UPDATE t SET id = id + 1 succeeds whatever
// order the rows are visited in — PostgreSQL, checking a non-deferrable
// key row by row, refuses it when it visits the rows in ascending order.
// The unique indexes are checked the same way, for every UPDATE.
//
// Because one statement may delete a key another of its rows writes,
// the statement's own row and index writes go through updateWrites,
// which writes each key once.

// updateWrites buffers an UPDATE's own row and index writes so that each
// key is written once: a key one row of the statement deletes (its old
// version) and another writes (its new version) is written, not deleted.
// Every delete an UPDATE buffers is of a key's version from before the
// statement and every write is of the version after it, so a write wins
// whatever order the two arrive in.
type updateWrites struct {
	order []string
	vals  map[string][]byte // nil value: a delete
	puts  map[string]bool
}

func newUpdateWrites() *updateWrites {
	return &updateWrites{vals: map[string][]byte{}, puts: map[string]bool{}}
}

func (u *updateWrites) Put(key keys.Key, value []byte) {
	k := string(key)
	if _, ok := u.vals[k]; !ok {
		u.order = append(u.order, k)
	}
	u.vals[k] = value
	u.puts[k] = true
}

func (u *updateWrites) Delete(key keys.Key) {
	k := string(key)
	if u.puts[k] {
		return
	}
	if _, ok := u.vals[k]; !ok {
		u.order = append(u.order, k)
	}
	u.vals[k] = nil
}

// flush moves the writes into the statement's batch.
func (u *updateWrites) flush(wb *kvclient.WriteBatch) {
	for _, k := range u.order {
		if u.puts[k] {
			wb.Put(keys.Key(k), u.vals[k])
		} else {
			wb.Delete(keys.Key(k))
		}
	}
}

// withShardBucket returns row with the hidden shard column of a
// hash-sharded table recomputed from the logical primary key (the row
// unchanged for any other table): the bucket a moved row's new key is
// filed under.
func withShardBucket(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) (map[catalog.ColumnID]types.Datum, error) {
	if desc.ShardBuckets <= 0 {
		return row, nil
	}
	logical := make([]types.Datum, len(desc.PrimaryKey)-1)
	for i, id := range desc.PrimaryKey[1:] {
		logical[i] = rowVal(row, id)
	}
	bucket, err := rowenc.ShardBucket(desc, logical)
	if err != nil {
		return nil, err
	}
	row[desc.PrimaryKey[0]] = bucket
	return row, nil
}

// checkUpdateKeys checks the unique keys of an UPDATE's rows as a whole:
// the primary key and every unique index. A new key is a duplicate when
// another row of the statement takes it too, or when it is taken now by
// a row the statement does not move out of it. oldKeys and newKeys are
// the rows' primary keys before and after.
func checkUpdateKeys(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, oldRows, newRows []map[catalog.ColumnID]types.Datum, oldKeys, newKeys []keys.Key, ps *probeSet) error {
	check := func(olds, news []keys.Key, dup func() error) error {
		held := make(map[string]bool, len(olds))
		for _, k := range olds {
			if k != nil {
				held[string(k)] = true
			}
		}
		taken := make(map[string]bool, len(news))
		for i, k := range news {
			if k == nil {
				continue
			}
			if taken[string(k)] {
				return dup()
			}
			taken[string(k)] = true
			if held[string(k)] || bytes.Equal(olds[i], k) {
				// The key is one of this statement's rows' now: either
				// that row moves out of it, or it keeps it and then
				// took it above.
				continue
			}
			existing, err := ps.get(ctx, txn, k)
			if err != nil {
				return err
			}
			if existing != nil {
				return dup()
			}
		}
		return nil
	}
	if err := check(oldKeys, newKeys, func() error {
		return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint on %q", desc.Name)
	}); err != nil {
		return err
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.Unique {
			continue
		}
		olds := make([]keys.Key, len(oldRows))
		news := make([]keys.Key, len(newRows))
		moved := false
		for j := range oldRows {
			ok, _, oskip, err := rowenc.EncodeIndexEntry(desc, idx, oldRows[j])
			if err != nil {
				return invertedEntryErr(idx, err)
			}
			nk, _, nskip, err := rowenc.EncodeIndexEntry(desc, idx, newRows[j])
			if err != nil {
				return invertedEntryErr(idx, err)
			}
			if nskip && !uniqueNullsDistinct(txn) {
				return nullInUniqueIndex(idx)
			}
			if !oskip {
				olds[j] = ok
			}
			if !nskip {
				news[j] = nk
			}
			moved = moved || oskip != nskip || !bytes.Equal(ok, nk)
		}
		if !moved {
			continue
		}
		if err := check(olds, news, func() error {
			return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint %q", idx.Name)
		}); err != nil {
			return err
		}
	}
	return nil
}

// cascadeOverlap refuses an UPDATE one of whose rows a foreign-key
// cascade of the same statement also rewrote — a table whose foreign key
// references the table itself with ON UPDATE CASCADE or SET NULL, when
// the statement changes both a referenced row and a row referencing it.
// The cascade rewrote the row from the version the statement read, and
// the statement then writes its own version of it: one of the two
// changes would be lost. PostgreSQL applies the cascade after the
// statement, to the rows as the statement left them; here the statement
// is refused instead, and the rows can be changed one statement at a
// time.
func (g *rowGuard) cascadeOverlap(desc *catalog.TableDescriptor, oldKeys, newKeys []keys.Key) error {
	if g == nil || g.st == nil || len(g.st.rows) == 0 {
		return nil
	}
	for i := range oldKeys {
		_, a := g.st.rows[string(oldKeys[i])]
		_, b := g.st.rows[string(newKeys[i])]
		if a || b {
			return newErrf(CodeFeatureNotSupported, "UPDATE of %q changes a row that a foreign-key cascade of the same statement also changes (a foreign key referencing the table itself): change the rows in separate statements", desc.Name)
		}
	}
	return nil
}
