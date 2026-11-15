package server

import (
	"context"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// Naming a table from its ID (issue #311). A range label, a rendered
// key, a log line and an index-usage series all need the descriptor of
// the one table a key belongs to. They used to take it from the schema
// document's by-products, which meant every node rebuilt the whole
// document every few seconds so that a name was at hand. This cache
// holds only the tables something on this node has asked about, read by
// ID in batched point reads, and reads nothing while nobody asks.
//
// Asking never waits: the answer is what the cache holds, and an ID it
// does not hold, or holds for longer than tableDescFreshFor, is fetched
// in the background for the next ask. That keeps the property the
// dashboard endpoints were built around — they answer while the catalog
// is unreachable — and it means the first rendering of a table this node
// has not looked at prints its ID, as the first poll after a CREATE
// TABLE always did — unless the schema document happens to have been
// built here, in which case its descriptors answer until the read lands.

// tableDescFreshFor is how old a cached descriptor may be before an ask
// refreshes it: a rename or a new index shows within this, plus a poll.
const tableDescFreshFor = schemaCacheFor

// tableDescCacheMax bounds the cache. Past it, the entries nobody has
// asked about lately go first.
const tableDescCacheMax = 4096

type tableDescEntry struct {
	// desc is nil for an ID with no descriptor: a dropped table, or a
	// reserved one not created yet.
	desc    *catalog.TableDescriptor
	fetched time.Time
	asked   time.Time
}

type tableDescCache struct {
	mu      sync.Mutex
	entries map[uint64]*tableDescEntry
	// pending are the IDs waiting to be read; fetching marks the one
	// background reader in flight.
	pending  map[uint64]struct{}
	fetching bool
}

// tableDesc returns table id's descriptor as this node last read it
// (nil when it has not, or there is none) and arranges for a missing or
// aging one to be read.
func (n *Node) tableDesc(id uint64) *catalog.TableDescriptor {
	c := &n.tableDescs
	now := time.Now()
	c.mu.Lock()
	e := c.entries[id]
	var desc *catalog.TableDescriptor
	fresh := false
	if e != nil {
		e.asked = now
		desc = e.desc
		fresh = now.Sub(e.fetched) < tableDescFreshFor
	}
	start := false
	if !fresh && n.stopper != nil {
		if c.pending == nil {
			c.pending = map[uint64]struct{}{}
		}
		c.pending[id] = struct{}{}
		if !c.fetching {
			c.fetching, start = true, true
		}
	}
	c.mu.Unlock()
	if start {
		if err := n.stopper.RunWorker(n.fetchTableDescs); err != nil {
			c.mu.Lock()
			c.fetching = false
			c.mu.Unlock()
		}
	}
	if e == nil {
		// Not read yet. If the schema document has been built on this
		// node it knows the table: someone who has just looked at the
		// schema page sees the same names on the next page at once.
		desc = n.schemaDescs()[id]
	}
	return desc
}

// fetchTableDescs reads the pending IDs until there are none. A read
// that fails ends the run and forgets what was asked: the next ask
// starts another, so an unreachable catalog costs one bounded attempt
// per rendering and what is cached stays.
func (n *Node) fetchTableDescs(ctx context.Context) {
	c := &n.tableDescs
	for {
		c.mu.Lock()
		ids := make([]uint64, 0, len(c.pending))
		for id := range c.pending {
			ids = append(ids, id)
		}
		c.pending = nil
		if len(ids) == 0 {
			c.fetching = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		descs, err := n.readTableDescs(ctx, ids)
		c.mu.Lock()
		if err != nil {
			c.pending = nil
			c.fetching = false
			c.mu.Unlock()
			return
		}
		now := time.Now()
		if c.entries == nil {
			c.entries = map[uint64]*tableDescEntry{}
		}
		for _, id := range ids {
			e := c.entries[id]
			if e == nil {
				e = &tableDescEntry{asked: now}
				c.entries[id] = e
			}
			e.desc, e.fetched = descs[id], now
		}
		if len(c.entries) > tableDescCacheMax {
			for id, e := range c.entries {
				if now.Sub(e.asked) > 2*tableDescFreshFor {
					delete(c.entries, id)
				}
			}
			for id := range c.entries {
				if len(c.entries) <= tableDescCacheMax {
					break
				}
				delete(c.entries, id)
			}
		}
		c.mu.Unlock()
	}
}

// readTableDescs reads the descriptors of the given tables, now, by ID;
// an ID with no descriptor is absent from the result.
func (n *Node) readTableDescs(ctx context.Context, ids []uint64) (map[uint64]*catalog.TableDescriptor, error) {
	ctx, cancel := context.WithTimeout(ctx, schemaBuildTimeout)
	defer cancel()
	out := make(map[uint64]*catalog.TableDescriptor, len(ids))
	err := n.db.RunTxn(ctx, "table-descs", func(ctx context.Context, txn *kvclient.Txn) error {
		clear(out)
		descs, err := catalog.ReadTables(ctx, txn, ids)
		for _, d := range descs {
			out[d.ID] = d
		}
		return err
	})
	return out, err
}

// tableNameByID is the key printer's lookup (keys.SetTableNamer): the
// name of table id when this node knows it.
func (n *Node) tableNameByID(id uint64) (string, bool) {
	if d := n.tableDesc(id); d != nil {
		return d.Name, true
	}
	return "", false
}

// tableNamesFor is the table ID → name map an admin response carries so
// that the client can print its spans by name: the tables its ranges'
// boundaries belong to, read now, so a table created a moment ago is
// named too.
func (n *Node) tableNamesFor(ctx context.Context, ranges ...kvpb.RangeDescriptor) map[uint64]string {
	seen := map[uint64]bool{}
	var ids []uint64
	for _, r := range ranges {
		for _, k := range []keys.Key{r.StartKey, r.EndKey} {
			if id, ok := keys.TableIDOf(k); ok && !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	out := map[uint64]string{}
	if len(ids) == 0 {
		return out
	}
	descs, err := n.readTableDescs(ctx, ids)
	if err != nil {
		return out
	}
	for id, d := range descs {
		out[id] = d.Name
	}
	return out
}
