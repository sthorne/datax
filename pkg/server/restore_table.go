package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Table restore (issue #301): named tables, or the tables of one
// database, brought back from a backup chain into a cluster that is
// serving traffic, beside what is there and without touching it.
//
// A restore of everything keeps table ids, because its target is empty.
// Here the id a table had in the backup may belong to another table by
// now, so each restored table takes a fresh id and its rows are re-keyed
// to it. A row's key is the table's prefix (/t/<id>) and then the index
// id and the encoded columns; nothing after the prefix names the table,
// and no value does either (a unique index entry's value is the bare
// primary-key columns), so re-keying is swapping the prefix. Index ids
// are kept, and with them every secondary index: nothing is backfilled.
//
// The order of work is what makes it safe on a live cluster:
//
//  1. Everything is checked first — the chain, the tables asked for, the
//     names they would take, their foreign keys — and every data file
//     they need is read to its end, which for a sealed backup
//     authenticates all of it. That pass also works out each table's
//     checksum as the chain defines it.
//  2. The new ids are allocated and a progress record written
//     (keys.RestoreProgressKey) in one transaction.
//  3. The rows are written under the new ids in ordinary transactions of
//     restoreChunk writes: replicated, encrypted and versioned like any
//     other write, and on keys no statement can reach, because no
//     descriptor names those ids yet. Traffic on other tables shares
//     nothing with it but the cluster.
//  4. The loaded rows are exported again and their checksum compared
//     with the one from step 1.
//  5. One transaction writes the descriptors, the names, the sequences
//     and (for a database restore) the database, and removes the
//     progress record. That commit is the moment the tables exist; all
//     of them appear at once, complete.
//
// A failure before step 5 removes the rows and the record. A node that
// dies instead leaves the record, and the janitor (restoreJanitorLoop,
// on every node) removes the rows it names; either way no table is left
// half restored, and none can be seen before it is whole. A restore that
// was cut short is run again from the start: it is not resumed.

// restoreProgress is the record of a table restore in flight.
type restoreProgress struct {
	OpID   string      `json:"op_id"`
	NodeID base.NodeID `json:"node_id"`
	// TableIDs are the ids the rows are being loaded under. Nothing names
	// them until the restore publishes, and they are never issued again.
	TableIDs []uint64 `json:"table_ids"`
	// Heartbeat is the wall time the running restore last touched the
	// record. A record gone quiet belongs to a restore that is not
	// running any more.
	Heartbeat int64 `json:"heartbeat"`
	// Reclaiming is set, in a transaction of its own, before any row of
	// an abandoned restore is removed. The publishing transaction reads
	// the record and refuses when it is set or gone, so a restore slow
	// enough to have been given up on cannot publish over rows that are
	// being taken away.
	Reclaiming bool `json:"reclaiming,omitempty"`
	// SealIDs are the seal ids of the per-tenant sealed tables being
	// restored, whose tenant keys the restore brings back before it
	// publishes: the tenant key janitor leaves them alone while the
	// record exists (tenantkeys.go).
	SealIDs []uint64 `json:"seal_ids,omitempty"`
}

const (
	restoreHeartbeatInterval = 15 * time.Second
	// restoreAbandonAfter is how long a progress record may go without a
	// heartbeat before any node takes the restore for dead.
	restoreAbandonAfter    = 5 * time.Minute
	restoreJanitorInterval = time.Minute
)

// activeRestores is the table restores this process is running, by op
// id: what tells a node's own record of a live restore from the record a
// previous life of the node left behind.
var activeRestores sync.Map // *Node+opID → struct{}

type activeRestoreKey struct {
	n    *Node
	opID string
}

// The descriptor fields table restore has been written against. A table
// in a backup may carry a field this list does not have — a feature
// added to tables after this was written — and a restore that did not
// know the field would drop it, or keep it without re-keying whatever it
// refers to. Such a table is refused, by name of the field, rather than
// restored as something it is not; a restore of everything, which keeps
// descriptors byte for byte, still restores it.
var (
	restoreTableFields = fieldSet("id", "name", "database_id", "columns", "primary_key", "indexes", "next_index_id",
		"constraints", "next_constraint_id", "inbound_fks", "next_column_id", "comment", "view_query", "view_depends",
		"owner", "privileges", "grant_options", "version", "timeseries", "retention_seconds", "shard_buckets",
		"primary_index", "reshard", "resharded_at", "retired_layouts",
		// A restore's bands are the source cluster's timestamps: the
		// restored copy gets its own load's (restoredDescriptor).
		"restored_rows",
		// A GC window is a number and a row expiry names a column by id:
		// neither refers to the table's id, so both carry over as they are.
		"gc_ttl_seconds", "row_ttl",
		// Row security names columns by name and roles by name, never
		// the table's id: the flags and the policies carry over, the
		// policies' roles cut to the ones this cluster has
		// (restoredDescriptor), so the restored copy is no less
		// protected than the table it was taken from.
		"row_security", "force_row_security", "policies",
		// A sealed table's key is named by its id in datax_keys and its
		// rows by the seal id, neither of which is the table's own id:
		// both carry over, and the key is shared or brought back
		// (restore_sealed.go).
		"encryption_key")
	restoreColumnFields = fieldSet("id", "name", "type", "precision", "scale", "width", "max_len", "char", "no_tz",
		"enum_type", "enum_name", "enum_labels", "time_precision", "not_null", "default", "default_expr", "identity",
		"sequence_id", "fill_default", "hidden", "comment", "retype_from",
		// The sequences a default is bound to: remapped to the restored
		// sequences a column owns, the others left to resolve by name
		// (restoredDescriptor).
		"seq_bindings")
	// An inverted index's entries are keyed under the table prefix like
	// any index's, so they are re-keyed with the rest.
	// So are a vector index's, and so are its centres, which sit under
	// the table prefix beside the indexes (keys.VectorCentroidPrefix): the
	// field names them by a build identifier, not by the table's id, and
	// carries over as it is.
	// A sealed table's key: the naming key and seal id carry over (see
	// "encryption_key" above), and a per-tenant table's tenant column is
	// a column id.
	// A table restored while it was being sealed carries sealing, and
	// its seal job is made again under the new id at the publish.
	restoreEncryptionKeyFields = fieldSet("key_id", "seal_id", "tenant_column", "sealing")
	restoreIndexFields         = fieldSet("id", "name", "unique", "column_ids", "comment", "state", "inverted", "vector",
		// Kept as they are under the new table id: the key encoding
		// (nulls_not_distinct), what is shown (opclasses, collations,
		// directions), and expressions and a predicate, which name
		// the table's own columns, restored with the same names.
		"nulls_not_distinct", "opclasses", "collations", "directions", "exprs", "expr_types", "predicate")
	restoreConstraintFields = fieldSet("id", "name", "kind", "columns", "expr", "ref_table", "ref_columns", "on_delete",
		"on_update", "index_id", "auto_index", "validated")
)

func fieldSet(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

// checkRestorableFields refuses a descriptor carrying a field table
// restore does not know (see restoreTableFields).
func checkRestorableFields(name string, raw json.RawMessage) error {
	unknown := func(what string, obj map[string]json.RawMessage, known map[string]bool) error {
		for k := range obj {
			if !known[k] {
				return fmt.Errorf("table %s carries %s %q, which this version's table restore does not know how to bring back under a new table id; restore the whole backup into an empty cluster, which keeps every table as it was, and copy the rows from there", name, what, k)
			}
		}
		return nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return fmt.Errorf("table %s: corrupt descriptor in the backup: %w", name, err)
	}
	if err := unknown("the descriptor field", top, restoreTableFields); err != nil {
		return err
	}
	if raw := top["encryption_key"]; len(raw) > 0 && string(raw) != "null" {
		var ek map[string]json.RawMessage
		if err := json.Unmarshal(raw, &ek); err != nil {
			return fmt.Errorf("table %s: corrupt descriptor in the backup: %w", name, err)
		}
		if err := unknown("the encryption key field", ek, restoreEncryptionKeyFields); err != nil {
			return err
		}
	}
	for _, part := range []struct {
		field, what string
		known       map[string]bool
	}{
		{"columns", "the column field", restoreColumnFields},
		{"indexes", "the index field", restoreIndexFields},
		{"constraints", "the constraint field", restoreConstraintFields},
	} {
		if len(top[part.field]) == 0 || string(top[part.field]) == "null" {
			continue
		}
		var items []map[string]json.RawMessage
		if err := json.Unmarshal(top[part.field], &items); err != nil {
			return fmt.Errorf("table %s: corrupt descriptor in the backup: %w", name, err)
		}
		for _, item := range items {
			if err := unknown(part.what, item, part.known); err != nil {
				return err
			}
		}
	}
	return nil
}

// tableRestorePlan is one table of a table restore.
type tableRestorePlan struct {
	src     backupTable
	srcDesc *catalog.TableDescriptor
	// srcDB, intoDB and intoName are the database the table is in in the
	// backup, and the database and name it is restored under.
	srcDB, intoDB, intoName string
	// seqs are the sequences the table's columns own, from the backup.
	seqs []restoreSequence
	// newID is the id the rows are loaded under.
	newID uint64
	// records, bytes and sha are the table as the chain defines it, in
	// the backup's own key space: what the loaded rows must match.
	records, bytes int64
	sha            string
	// expiry applies a time-series table's retention to the rows
	// (restore_retention.go); bands are those its load wrote, recorded in
	// the published descriptor.
	expiry *commitTimeExpiry
	bands  []catalog.RestoredRowsBand
}

// restoreSequence is a sequence a restored table's column owns.
type restoreSequence struct {
	desc  catalog.SequenceDescriptor
	value []byte // the counter as the backup has it
	col   catalog.ColumnID
}

func (p *tableRestorePlan) from() string { return p.srcDB + "." + p.src.Name }
func (p *tableRestorePlan) into() string { return p.intoDB + "." + p.intoName }

// restorePlan is a whole table restore, worked out from the backup alone.
type restorePlan struct {
	tables []*tableRestorePlan
	// newDB, for a database restore, is the database to create; its ID is
	// allocated when the restore publishes.
	newDB *catalog.DatabaseDescriptor
	notes []string
	// sealed is what the restore of the plan's sealed tables needs (nil
	// without one; restore_sealed.go).
	sealed *sealedRestore
	// types are the backup's enum type descriptors by id (empty for a
	// backup from before types were backed up: the columns' own copies
	// of the labels are what a missing type is made from).
	types map[uint64]catalog.TypeDescriptor
}

// planTableRestore works out, from the chain's last manifest, which
// tables a restore brings back and under what names. It reads nothing of
// the cluster.
func planTableRestore(final *backupManifest, opts RestoreOptions) (*restorePlan, error) {
	if len(opts.Tables) > 0 && opts.Database != "" {
		return nil, fmt.Errorf("--table and --database are two restores; run them one after the other")
	}
	if opts.IntoDatabase != "" && opts.Database == "" {
		return nil, fmt.Errorf("--into names a database only with --database; with --table it names the table")
	}
	// The backup's databases, by id and by name.
	dbDescStart, dbDescEnd := keys.DatabaseDescSpan()
	dbByID := map[uint64]*catalog.DatabaseDescriptor{}
	dbByName := map[string]*catalog.DatabaseDescriptor{}
	for _, kv := range final.Databases {
		if !inAnySpan(keys.Key(kv.Key), [][2]keys.Key{{dbDescStart, dbDescEnd}}) {
			continue // a name entry
		}
		var d catalog.DatabaseDescriptor
		if err := json.Unmarshal(kv.Value, &d); err != nil {
			return nil, fmt.Errorf("corrupt database descriptor in the backup: %w", err)
		}
		dbByID[d.ID], dbByName[d.Name] = &d, &d
	}
	dbNameOf := func(id uint64) string {
		if d, ok := dbByID[id]; ok {
			return d.Name
		}
		return catalog.DefaultDatabase // a table from before databases
	}
	// The backup's sequences: descriptors by id, counters by key.
	seqDescStart, seqDescEnd := keys.SequenceDescSpan()
	var seqs []catalog.SequenceDescriptor
	raw := map[string][]byte{}
	for _, kv := range final.Sequences {
		raw[string(kv.Key)] = kv.Value
		if !inAnySpan(keys.Key(kv.Key), [][2]keys.Key{{seqDescStart, seqDescEnd}}) {
			continue
		}
		var d catalog.SequenceDescriptor
		if err := json.Unmarshal(kv.Value, &d); err != nil {
			return nil, fmt.Errorf("corrupt sequence descriptor in the backup: %w", err)
		}
		seqs = append(seqs, d)
	}

	plan := &restorePlan{types: map[uint64]catalog.TypeDescriptor{}}
	typeDescStart, typeDescEnd := keys.TypeDescSpan()
	for _, kv := range final.Types {
		if !inAnySpan(keys.Key(kv.Key), [][2]keys.Key{{typeDescStart, typeDescEnd}}) {
			continue // a name entry
		}
		var d catalog.TypeDescriptor
		if err := json.Unmarshal(kv.Value, &d); err != nil {
			return nil, fmt.Errorf("corrupt type descriptor in the backup: %w", err)
		}
		plan.types[d.ID] = d
	}
	add := func(t backupTable, intoDB, intoName string) error {
		if err := checkRestorableFields(t.Name, t.Descriptor); err != nil {
			return err
		}
		var d catalog.TableDescriptor
		if err := json.Unmarshal(t.Descriptor, &d); err != nil {
			return fmt.Errorf("table %s: corrupt descriptor in the backup: %w", t.Name, err)
		}
		p := &tableRestorePlan{src: t, srcDesc: &d, srcDB: dbNameOf(d.DatabaseID), intoDB: intoDB, intoName: intoName}
		if p.intoDB == "" {
			p.intoDB = p.srcDB
		}
		if p.intoName == "" {
			p.intoName = t.Name
		}
		for _, c := range d.Columns {
			if c.SequenceID == 0 {
				continue
			}
			found := false
			for _, sd := range seqs {
				if sd.ID == c.SequenceID {
					p.seqs = append(p.seqs, restoreSequence{desc: sd, value: raw[string(keys.SequenceValueKey(sd.ID))], col: c.ID})
					found = true
				}
			}
			if !found {
				return fmt.Errorf("table %s: column %q owns sequence %d, which the backup does not hold", p.from(), c.Name, c.SequenceID)
			}
		}
		plan.tables = append(plan.tables, p)
		return nil
	}
	has := func() string {
		var names []string
		for _, t := range final.Tables {
			var d catalog.TableDescriptor
			if json.Unmarshal(t.Descriptor, &d) == nil && !catalog.IsSystemTableID(t.ID) {
				names = append(names, dbNameOf(d.DatabaseID)+"."+t.Name)
			}
		}
		sort.Strings(names)
		return strings.Join(names, ", ")
	}

	if opts.Database != "" {
		src, ok := dbByName[opts.Database]
		if !ok || opts.Database == catalog.SystemDatabase {
			return nil, fmt.Errorf("the backup has no database %q to restore", opts.Database)
		}
		nd := src.Clone()
		nd.ID = 0
		if opts.IntoDatabase != "" {
			nd.Name = opts.IntoDatabase
		}
		plan.newDB = nd
		for _, t := range final.Tables {
			var d catalog.TableDescriptor
			if err := json.Unmarshal(t.Descriptor, &d); err != nil {
				return nil, fmt.Errorf("table %s: corrupt descriptor in the backup: %w", t.Name, err)
			}
			if d.DatabaseID != src.ID || catalog.IsSystemTableID(t.ID) {
				continue
			}
			if d.IsView() {
				// A view is a query over tables by id; the tables it reads
				// have new ids here. Its text is in the note for whoever
				// wants it back.
				plan.notes = append(plan.notes, fmt.Sprintf("view %s.%s was not restored (a view names its tables by id); recreate it: %s", src.Name, t.Name, d.ViewQuery))
				continue
			}
			if err := add(t, nd.Name, ""); err != nil {
				return nil, err
			}
		}
		if len(plan.tables) == 0 {
			return nil, fmt.Errorf("database %q has no tables in the backup", opts.Database)
		}
	}
	for _, want := range opts.Tables {
		db, name := catalog.SplitTableName(want.Name)
		if db == "" {
			db = catalog.DefaultDatabase
		}
		var found *backupTable
		for i, t := range final.Tables {
			var d catalog.TableDescriptor
			if json.Unmarshal(t.Descriptor, &d) == nil && t.Name == name && dbNameOf(d.DatabaseID) == db && d.SchemaID == 0 {
				found = &final.Tables[i]
			}
		}
		if found == nil {
			return nil, fmt.Errorf("the backup has no table %s.%s (it has: %s)", db, name, has())
		}
		intoDB, intoName := catalog.SplitTableName(want.Into)
		if err := add(*found, intoDB, intoName); err != nil {
			return nil, err
		}
	}

	ids := map[uint64]bool{}
	for _, p := range plan.tables {
		ids[p.src.ID] = true
	}
	targets := map[string]bool{}
	for _, p := range plan.tables {
		d := p.srcDesc
		switch {
		case d != nil && d.SchemaID != 0:
			// Its name entry, and the schema it needs on this side, are
			// not planned yet (docs/design/schemas.md, Deferred).
			return nil, fmt.Errorf("table %s is in a schema other than public, which a table-level restore does not support yet (restore the whole cluster)", p.from())
		case catalog.IsSystemTableID(p.src.ID) || catalog.IsSystemTable(p.src.Name):
			return nil, fmt.Errorf("table %s belongs to the cluster itself and is not restored on its own", p.from())
		case d.IsView():
			return nil, fmt.Errorf("%s is a view: it holds no rows, and names the tables it reads by id. Restore those tables and recreate it", p.from())
		case d.Reshard != nil || len(d.RetiredLayouts) > 0:
			return nil, fmt.Errorf("table %s was being re-sharded, or still kept a layout from before a re-shard, when it was backed up; a table restore brings back one layout. Restore from a backup taken once the old layout has been reclaimed, or restore the whole backup into an empty cluster", p.from())
		case catalog.IsSystemTable(p.intoName):
			return nil, fmt.Errorf("%q is a name the cluster keeps for itself", p.intoName)
		case p.intoDB == catalog.SystemDatabase:
			return nil, fmt.Errorf("the %s database belongs to the cluster; restore into another", catalog.SystemDatabase)
		}
		for _, idx := range d.Indexes {
			if !idx.Public() {
				return nil, fmt.Errorf("table %s: index %q was still being built when the table was backed up; restore from a backup taken after CREATE INDEX finished", p.from(), idx.Name)
			}
		}
		for _, c := range d.Columns {
			if c.RetypeFrom != 0 {
				return nil, fmt.Errorf("table %s: a column's type was being changed when the table was backed up; restore from a backup taken after ALTER COLUMN TYPE finished", p.from())
			}
		}
		if d.Alter != nil {
			return nil, fmt.Errorf("table %s: an ALTER TABLE was in progress when the table was backed up; restore from a backup taken after it finished", p.from())
		}
		if targets[p.into()] {
			return nil, fmt.Errorf("two tables would be restored as %s", p.into())
		}
		targets[p.into()] = true
		// The foreign-key rule. A foreign key to a table restored in the
		// same run follows it to its new id. One to a table outside the
		// run cannot: the restored rows would be checked against a table
		// that has moved on since the backup, or is not there at all, and
		// claiming the constraint held would be a lie. The restore is
		// refused unless the operator says to bring the table back
		// without them.
		for _, c := range d.Constraints {
			if c.Kind != catalog.ConstraintForeign || ids[c.RefTable] {
				continue
			}
			if !opts.DropForeignKeys {
				return nil, fmt.Errorf("table %s has the foreign key %q to a table that is not being restored with it. Restore the two together, or pass --drop-foreign-keys to restore %s without the foreign keys that reach outside the restore", p.from(), c.Name, p.src.Name)
			}
			plan.notes = append(plan.notes, fmt.Sprintf("%s: foreign key %q dropped (it references a table outside the restore)", p.into(), c.Name))
		}
	}
	return plan, nil
}

// restoredDescriptor builds the descriptor a table is published under:
// the backup's, moved to its new id, database and name. ids maps the
// backup's table ids to the new ones; seqIDs the backup's sequence ids
// to the sequences created for it; roles says which roles the cluster
// has.
func restoredDescriptor(p *tableRestorePlan, dbID uint64, ids, seqIDs map[uint64]uint64, seqNames map[uint64]string, roles func(string) bool) (*catalog.TableDescriptor, []string) {
	var notes []string
	d := p.srcDesc.Clone()
	d.ID, d.DatabaseID, d.Name, d.Version = p.newID, dbID, p.intoName, 1
	d.RestoredRows = p.bands
	kept := d.Constraints[:0:0]
	for _, c := range d.Constraints {
		if c.Kind == catalog.ConstraintForeign {
			to, ok := ids[c.RefTable]
			if !ok {
				continue // dropped; planTableRestore noted it
			}
			c.RefTable = to
		}
		kept = append(kept, c)
	}
	d.Constraints = kept
	// Only the foreign keys of tables restored alongside point here: the
	// tables that referenced the original still reference the original.
	var inbound []catalog.ForeignKeyRef
	for _, ref := range d.InboundFKs {
		if to, ok := ids[ref.TableID]; ok {
			inbound = append(inbound, catalog.ForeignKeyRef{TableID: to, ConstraintID: ref.ConstraintID})
		}
	}
	d.InboundFKs = inbound
	for i := range d.Columns {
		c := &d.Columns[i]
		// A default bound to a sequence the restore brings back is bound
		// to the restored copy; to any other, it resolves by its name
		// when used (gap PG-D-035), as defaults did before bindings.
		var bindings map[string]uint64
		if c.SeqBindings != nil {
			bindings = make(map[string]uint64, len(c.SeqBindings))
			for name, id := range c.SeqBindings {
				if to, ok := seqIDs[id]; ok && id != 0 {
					bindings[name] = to
				} else {
					bindings[name] = 0
				}
			}
		}
		if c.SequenceID != 0 {
			old := c.SequenceID
			c.SequenceID = seqIDs[old]
			for _, s := range p.seqs {
				if s.desc.ID == old && c.DefaultExpr == fmt.Sprintf("nextval('%s')", s.desc.Name) {
					c.DefaultExpr = fmt.Sprintf("nextval('%s')", seqNames[old])
					if bindings != nil {
						delete(bindings, s.desc.Name)
						bindings[seqNames[old]] = c.SequenceID
					}
				}
			}
		}
		c.SeqBindings = bindings
	}
	// Grants come back for the roles this cluster has. A grant to a role
	// it lacks would be a grant to whoever is given that name next.
	if owner := catalog.OwnerOf(d.Owner); !roles(owner) {
		notes = append(notes, fmt.Sprintf("%s: owner %q does not exist here; owned by %s", p.into(), owner, catalog.RootRole))
		d.Owner = catalog.RootRole
	}
	for _, m := range []map[string][]string{d.Privileges, d.GrantOptions} {
		for grantee := range m {
			if grantee != catalog.PublicRole && !roles(grantee) {
				delete(m, grantee)
				if note := fmt.Sprintf("%s: grants to %q not restored (no such role here)", p.into(), grantee); !containsString(notes, note) {
					notes = append(notes, note)
				}
			}
		}
	}
	// Row security comes back as it was: enabled, forced, and with its
	// policies. A policy's TO list is cut to the roles this cluster has,
	// for the reason grants are; a policy left naming no role is dropped
	// rather than kept with an empty list, which would mean every role.
	// Either way the table stays protected — with fewer policies a role
	// sees less, never more.
	if len(d.Policies) > 0 {
		kept := d.Policies[:0:0]
		for _, pol := range d.Policies {
			if len(pol.Roles) == 0 {
				kept = append(kept, pol)
				continue
			}
			var have []string
			for _, r := range pol.Roles {
				if r == catalog.PublicRole || roles(r) {
					have = append(have, r)
				}
			}
			if len(have) == 0 {
				notes = append(notes, fmt.Sprintf("%s: policy %q not restored (none of its roles exists here)", p.into(), pol.Name))
				continue
			}
			if len(have) < len(pol.Roles) {
				notes = append(notes, fmt.Sprintf("%s: policy %q restored for the roles that exist here (%s)", p.into(), pol.Name, strings.Join(have, ", ")))
			}
			pol.Roles = have
			kept = append(kept, pol)
		}
		d.Policies = kept
	}
	sort.Strings(notes)
	return d, notes
}

func sameLabels(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// restoredSequenceName is the name a restored table's owned sequence
// takes: the one CREATE TABLE would have given it under the table's new
// name when it had the usual name under the old one, and its own name
// otherwise.
func restoredSequenceName(p *tableRestorePlan, s restoreSequence) string {
	if col, ok := p.srcDesc.ColByID(s.col); ok && s.desc.Name == p.src.Name+"_"+col.Name+"_seq" {
		return p.intoName + "_" + col.Name + "_seq"
	}
	return s.desc.Name
}

// restoreTargets is what a table restore learns from the cluster: where
// each table goes and which roles exist. Read once before anything is
// written, to refuse early, and again inside the publishing transaction,
// which is the reading that counts.
type restoreTargets struct {
	dbIDs map[string]uint64
	roles map[string]bool
	// types is where each enum type the restored columns are of goes:
	// the id of the same type, same labels, already in the target
	// database, or zero for a type the restore creates there.
	types map[restoreTypeKey]uint64
}

// restoreTypeKey is an enum type of a table restore: the database it is
// restored into and its name there.
type restoreTypeKey struct{ db, name string }

func (n *Node) readRestoreTargets(ctx context.Context, txn *kvclient.Txn, plan *restorePlan) (*restoreTargets, error) {
	tg := &restoreTargets{dbIDs: map[string]uint64{}, roles: map[string]bool{}, types: map[restoreTypeKey]uint64{}}
	if plan.newDB != nil {
		if raw, err := txn.Get(ctx, keys.DatabaseNamespaceKey(plan.newDB.Name)); err != nil {
			return nil, err
		} else if raw != nil {
			return nil, fmt.Errorf("database %q already exists; a database is restored as a new one (pass --into to name it)", plan.newDB.Name)
		}
	}
	for _, p := range plan.tables {
		if plan.newDB != nil {
			continue // the database is new: every name in it is free
		}
		dbID, ok := tg.dbIDs[p.intoDB]
		if !ok {
			db, err := catalog.LookupDatabase(ctx, txn, p.intoDB)
			if err != nil {
				var nf *catalog.ErrDatabaseNotFound
				if errors.As(err, &nf) {
					return nil, fmt.Errorf("database %q does not exist here; create it, or restore %s into another with --into", p.intoDB, p.from())
				}
				return nil, err
			}
			if db.ID == 0 {
				return nil, fmt.Errorf("database %q has not been migrated to the database catalog yet", p.intoDB)
			}
			dbID = db.ID
			tg.dbIDs[p.intoDB] = dbID
		}
		names := []string{p.intoName}
		for _, s := range p.seqs {
			names = append(names, restoredSequenceName(p, s))
		}
		for i, name := range names {
			for _, k := range []keys.Key{keys.TableNamespaceKey(dbID, name), keys.SequenceNamespaceKey(dbID, name), keys.TypeNamespaceKey(dbID, name)} {
				raw, err := txn.Get(ctx, k)
				if err != nil {
					return nil, err
				}
				if raw == nil {
					continue
				}
				if i == 0 {
					return nil, fmt.Errorf("%s already exists. A table restore never overwrites: restore under another name with --into, compare, and then replace the table yourself (DROP TABLE, then ALTER TABLE ... RENAME TO)", p.into())
				}
				return nil, fmt.Errorf("restoring %s needs the name %s.%s for a sequence its column owns, and that name is taken", p.into(), p.intoDB, name)
			}
		}
	}
	restored := map[restoreTypeKey]bool{} // the names the restored tables and sequences take
	for _, p := range plan.tables {
		restored[restoreTypeKey{p.intoDB, p.intoName}] = true
		for _, s := range p.seqs {
			restored[restoreTypeKey{p.intoDB, restoredSequenceName(p, s)}] = true
		}
	}
	labelsOf := map[restoreTypeKey][]string{}
	for _, p := range plan.tables {
		// An enum column stores a label's position in its type, so it is
		// restored only onto a type with the same labels in the same
		// order: the one of that name in the target database, or, where
		// the database has no type of that name, the type the restore
		// creates there from the backup (issue #313).
		for _, c := range p.srcDesc.Columns {
			if c.EnumType == 0 {
				continue
			}
			tk := restoreTypeKey{p.intoDB, c.EnumName}
			if prev, seen := labelsOf[tk]; seen {
				if !sameLabels(prev, c.EnumLabels) {
					return nil, fmt.Errorf("table %s: column %q is of the enum type %q, and another table of this restore has a type of that name with other labels", p.from(), c.Name, c.EnumName)
				}
				continue
			}
			labelsOf[tk] = c.EnumLabels
			if plan.newDB != nil {
				tg.types[tk] = 0 // the database is new: the type is made in it
				continue
			}
			dbID := tg.dbIDs[p.intoDB]
			raw, err := txn.Get(ctx, keys.TypeNamespaceKey(dbID, c.EnumName))
			if err != nil {
				return nil, err
			}
			if raw == nil {
				for _, k := range []keys.Key{keys.TableNamespaceKey(dbID, c.EnumName), keys.SequenceNamespaceKey(dbID, c.EnumName)} {
					if other, err := txn.Get(ctx, k); err != nil {
						return nil, err
					} else if other != nil {
						return nil, fmt.Errorf("table %s: column %q is of the enum type %q, which %s does not have, and the restore cannot create it there because a table or sequence has that name", p.from(), c.Name, c.EnumName, p.intoDB)
					}
				}
				if restored[tk] {
					return nil, fmt.Errorf("table %s: column %q is of the enum type %q, and the restore would give that name to a table or sequence too", p.from(), c.Name, c.EnumName)
				}
				tg.types[tk] = 0
				continue
			}
			td, err := catalog.LookupType(ctx, txn, dbID, c.EnumName)
			if err != nil {
				return nil, err
			}
			if !sameLabels(td.Labels, c.EnumLabels) {
				return nil, fmt.Errorf("table %s: column %q is of the enum type %q with the labels (%s), and %s has a type of that name with the labels (%s): a value is stored as its label's position, so the column cannot be restored onto it. Restore into another database (--into), or restore the whole backup into an empty cluster and copy the rows from there",
					p.from(), c.Name, c.EnumName, strings.Join(c.EnumLabels, ", "), p.intoDB, strings.Join(td.Labels, ", "))
			}
			tg.types[tk] = td.ID
		}
		check := []string{catalog.OwnerOf(p.srcDesc.Owner)}
		for g := range p.srcDesc.Privileges {
			check = append(check, g)
		}
		for g := range p.srcDesc.GrantOptions {
			check = append(check, g)
		}
		for i := range p.srcDesc.Policies {
			check = append(check, p.srcDesc.Policies[i].Roles...)
		}
		for _, name := range check {
			if _, seen := tg.roles[name]; seen || name == catalog.PublicRole {
				continue
			}
			rd, err := catalog.LookupRole(ctx, txn, name)
			if err != nil {
				return nil, err
			}
			tg.roles[name] = rd != nil
		}
	}
	return tg, nil
}

// mergeTable walks one table through a chain as it stands at the
// chain's cut: every key the chain's backups hold for it, in key order,
// each as the last backup to mention it left it, and only the ones left
// alive. Each data file is in key order, so the walk holds one record
// per backup; a file that is not in order is refused (tableStream).
func mergeTable(chain *restoreChain, tableID uint64, fn func(kvpb.ExportRecord) error) error {
	type source struct {
		s   *tableStream
		rec kvpb.ExportRecord
		ok  bool
	}
	var srcs []*source
	defer func() {
		for _, src := range srcs {
			src.s.Close()
		}
	}()
	for i, man := range chain.mans {
		for _, t := range man.Tables {
			if t.ID != tableID {
				continue
			}
			// The name is derived, not taken from the manifest.
			s, err := openTableStream(filepath.Join(chain.srcs[i], backupDataFile(t.ID)), chain.dataKeys[i], t, chain.cutoffFor(i), true)
			if err != nil {
				return err
			}
			src := &source{s: s}
			srcs = append(srcs, src)
			if src.rec, src.ok, err = s.next(); err != nil {
				return err
			}
		}
	}
	for {
		// The smallest key any backup still has, and the last backup in
		// the chain that has it.
		var at *source
		for _, src := range srcs {
			if src.ok && (at == nil || bytes.Compare(src.rec.Key, at.rec.Key) <= 0) {
				at = src
			}
		}
		if at == nil {
			return nil
		}
		rec := at.rec
		for _, src := range srcs {
			if !src.ok || !bytes.Equal(src.rec.Key, rec.Key) {
				continue
			}
			var err error
			if src.rec, src.ok, err = src.s.next(); err != nil {
				return err
			}
		}
		if rec.Deleted {
			continue
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

// runTableRestore is RunRestoreOpts for a restore of tables.
func (n *Node) runTableRestore(ctx context.Context, opID string, opts RestoreOptions, chain *restoreChain) (*cluster.BackupSummary, error) {
	if n.db.ClusterVersion() < version.V21 {
		return nil, fmt.Errorf("restoring tables into a running cluster needs cluster version %s (finalize the upgrade first)", version.V21)
	}
	final := chain.final()
	plan, err := planTableRestore(final, opts)
	if err != nil {
		return nil, err
	}
	if err := n.db.RunTxn(ctx, "restore-table-check", func(ctx context.Context, txn *kvclient.Txn) error {
		_, err := n.readRestoreTargets(ctx, txn, plan)
		return err
	}); err != nil {
		return nil, err
	}
	if plan.sealed, err = n.planSealedRestore(ctx, chain, plan); err != nil {
		return nil, err
	}

	// Every data file the restore will read, read to its end before
	// anything is written: held to its table's span and its order, and
	// for a sealed backup authenticated. The pass also yields each
	// table's checksum as the chain defines it.
	for _, p := range plan.tables {
		if !chain.cutoff.IsEmpty() {
			if err := tableStableAcross(chain.mans[len(chain.mans)-2], final, p.src, chain.cutoff); err != nil {
				return nil, err
			}
		}
		e, note := n.planCommitTimeExpiry(chain, p.src.ID, p.srcDesc, p.from())
		p.expiry = e
		if note != "" {
			plan.notes = append(plan.notes, note)
		}
		h := sha256.New()
		if err := mergeTable(chain, p.src.ID, func(rec kvpb.ExportRecord) error {
			if p.expiry != nil && !p.expiry.keep(rec) {
				return nil // expired at the source by the chain's end
			}
			p.records++
			p.bytes += int64(len(rec.Key) + len(rec.Value))
			hashLiveRecord(h, rec)
			return nil
		}); err != nil {
			return nil, err
		}
		p.sha = hex.EncodeToString(h.Sum(nil))
	}

	// The ids, and the record that says rows are about to appear under
	// them, in one transaction: there is never an id in use by a restore
	// that no record names.
	key := activeRestoreKey{n, opID}
	activeRestores.Store(key, struct{}{})
	defer activeRestores.Delete(key)
	progress := restoreProgress{OpID: opID, NodeID: n.ident.NodeID, SealIDs: plan.sealed.tenantSealIDs()}
	if err := n.db.RunTxn(ctx, "restore-table-begin", func(ctx context.Context, txn *kvclient.Txn) error {
		top, err := txn.Increment(ctx, keys.DescIDGenKey(), int64(len(plan.tables)))
		if err != nil {
			return err
		}
		progress.TableIDs = progress.TableIDs[:0]
		for i, p := range plan.tables {
			p.newID = uint64(top) - uint64(len(plan.tables)-1-i)
			progress.TableIDs = append(progress.TableIDs, p.newID)
		}
		progress.Heartbeat = n.clock.Now().WallTime
		raw, err := json.Marshal(&progress)
		if err != nil {
			return err
		}
		return txn.Put(ctx, keys.RestoreProgressKey(opID), raw)
	}); err != nil {
		return nil, err
	}

	// From here a failure takes back what was written. The cleanup runs
	// even when it is the caller's context that ended the restore; should
	// it fail too, the record stays and the janitor finishes the job.
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	heartbeatDone := make(chan struct{})
	go n.restoreHeartbeat(loadCtx, opID, stopLoad, heartbeatDone)
	sum, err := n.loadAndPublishTables(loadCtx, opID, opts, chain, plan)
	// The load's context ending while the caller's has not is the
	// heartbeat finding the record gone or marked.
	lost := loadCtx.Err() != nil && ctx.Err() == nil
	stopLoad()
	<-heartbeatDone
	if errors.Is(err, errRestoreAbandoned) {
		return nil, err
	}
	if err != nil {
		if lost && !errors.Is(err, errRestoreReclaimed) {
			err = fmt.Errorf("%w (%v)", errRestoreReclaimed, err)
		}
		n.reclaimRestore(context.WithoutCancel(ctx), opID)
		return nil, err
	}
	if cat, cerr := n.catalogAccessor(); cerr == nil && cat != nil {
		// This node's cache of database names predates a database the
		// restore created.
		cat.InvalidateAll()
	}
	return sum, nil
}

var (
	// errRestoreAbandoned is RestoreOptions.TestingAbandonAfterLoad.
	errRestoreAbandoned = errors.New("restore abandoned after the load (testing)")
	errRestoreReclaimed = errors.New("the restore went too long without recording progress and was taken for dead; its rows are being removed. Run it again")
)

// restoreHeartbeat keeps a running restore's record fresh, and stops the
// restore if the record is gone or marked for reclaiming.
func (n *Node) restoreHeartbeat(ctx context.Context, opID string, stop context.CancelFunc, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(restoreHeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		lost := false
		err := n.db.RunTxn(ctx, "restore-table-heartbeat", func(ctx context.Context, txn *kvclient.Txn) error {
			lost = false
			rec, err := readRestoreProgress(ctx, txn, opID)
			if err != nil {
				return err
			}
			if rec == nil || rec.Reclaiming {
				lost = true
				return nil
			}
			rec.Heartbeat = n.clock.Now().WallTime
			raw, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			return txn.Put(ctx, keys.RestoreProgressKey(opID), raw)
		})
		if err == nil && lost {
			stop()
			return
		}
	}
}

func readRestoreProgress(ctx context.Context, txn *kvclient.Txn, opID string) (*restoreProgress, error) {
	raw, err := txn.Get(ctx, keys.RestoreProgressKey(opID))
	if err != nil || raw == nil {
		return nil, err
	}
	var rec restoreProgress
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("corrupt restore record %s: %w", opID, err)
	}
	return &rec, nil
}

// loadAndPublishTables writes the plan's tables under their new ids,
// checks what it wrote, and publishes them.
func (n *Node) loadAndPublishTables(ctx context.Context, opID string, opts RestoreOptions, chain *restoreChain, plan *restorePlan) (*cluster.BackupSummary, error) {
	// Each table in ranges of its own before the load, as CREATE TABLE
	// and a restore of everything do; best-effort.
	for _, p := range plan.tables {
		at := *p.srcDesc
		at.ID = p.newID
		splitAt := []keys.Key{keys.TableDataPrefix(p.newID), keys.TableDataPrefix(p.newID).PrefixEnd()}
		for b := int32(1); b < at.ShardBuckets; b++ {
			if k, err := rowenc.AppendKeyDatum(rowenc.PrimaryKeyPrefixFor(&at), types.Int, types.NewInt(int64(b))); err == nil {
				splitAt = append(splitAt, k)
			}
		}
		for _, k := range splitAt {
			if _, err := n.db.AdminSplit(ctx, k); err != nil {
				log.Debugf("restore pre-split at %s: %v", k, err)
			}
		}
	}

	for _, p := range plan.tables {
		oldPrefix, newPrefix := keys.TableDataPrefix(p.src.ID), keys.TableDataPrefix(p.newID)
		if p.expiry != nil {
			bands, expired, err := n.loadByCommitTime(ctx, chain, p.src.ID, p.expiry, "restore-table-data", func(k keys.Key) keys.Key {
				return append(newPrefix.Clone(), k[len(oldPrefix):]...)
			})
			if err != nil {
				return nil, fmt.Errorf("restoring table %s: %w", p.from(), err)
			}
			p.bands = bands
			plan.notes = append(plan.notes, expiryNote(p.from(), p.expiry, expired))
			continue
		}
		var wb kvclient.WriteBatch
		flush := func() error {
			if wb.Len() == 0 {
				return nil
			}
			batch := wb
			wb = kvclient.WriteBatch{}
			return n.db.RunTxn(ctx, "restore-table-data", func(ctx context.Context, txn *kvclient.Txn) error {
				b := batch
				return txn.RunBatch(ctx, &b)
			})
		}
		err := mergeTable(chain, p.src.ID, func(rec kvpb.ExportRecord) error {
			// The stream has held the key to the table's span, so it
			// starts with the table's prefix.
			k := append(newPrefix.Clone(), rec.Key[len(oldPrefix):]...)
			wb.Put(k, rec.Value)
			if wb.Len() >= restoreChunk {
				return flush()
			}
			return nil
		})
		if err == nil {
			err = flush()
		}
		if err != nil {
			return nil, fmt.Errorf("restoring table %s: %w", p.from(), err)
		}
	}
	if opts.TestingAbandonAfterLoad {
		return nil, errRestoreAbandoned
	}

	// What was loaded, read back and compared with what the chain says
	// the table is. The data files were read twice — once to check, once
	// to load — and this is what a file changed between the two runs
	// into; it is also the check that the load wrote what it read.
	sum := &cluster.BackupSummary{ClusterID: chain.final().ClusterID, EndTS: formatBackupTS(chain.asOf()), Notes: plan.notes}
	verifyTS := n.clock.Now()
	for _, p := range plan.tables {
		oldPrefix, newPrefix := keys.TableDataPrefix(p.src.ID), keys.TableDataPrefix(p.newID)
		h := sha256.New()
		var records, size int64
		start, end := keys.TableDataSpan(p.newID)
		if err := n.exportAll(ctx, start, end, hlc.Timestamp{}, verifyTS, func(rec kvpb.ExportRecord) error {
			if rec.Deleted {
				return nil
			}
			// Hashed under the key the backup knows the row by, so the
			// checksum is the one a backup of the source table has.
			rec.Key = append(oldPrefix.Clone(), rec.Key[len(newPrefix):]...)
			records++
			size += int64(len(rec.Key) + len(rec.Value))
			hashLiveRecord(h, rec)
			return nil
		}); err != nil {
			return nil, err
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != p.sha || records != p.records {
			return nil, fmt.Errorf("table %s: the rows loaded (%d, checksum %s) are not the rows the backup holds (%d, checksum %s): a backup file changed while it was being restored, or the load went wrong. Nothing has been made visible", p.from(), records, got, p.records, p.sha)
		}
		sum.Tables = append(sum.Tables, cluster.BackupTableSummary{
			ID: p.newID, Name: p.into(), Records: records, Bytes: size, SHA256: p.sha, From: p.from(), FromID: p.src.ID,
		})
	}

	cat, err := n.catalogAccessor()
	if err != nil {
		return nil, err
	}
	var notes []string
	// The tenant keys of per-tenant tables, possibly many, come back in
	// batches of their own before the publish (restore_sealed.go); the
	// progress record names their seal ids, so the tenant key janitor
	// leaves them alone until the restore publishes or is reclaimed.
	if err := n.reinstateTenantKeys(ctx, plan.sealed); err != nil {
		return nil, err
	}
	err = n.db.RunTxn(ctx, "restore-table-publish", func(ctx context.Context, txn *kvclient.Txn) error {
		notes = notes[:0]
		rec, err := readRestoreProgress(ctx, txn, opID)
		if err != nil {
			return err
		}
		if rec == nil || rec.Reclaiming {
			return errRestoreReclaimed
		}
		tg, err := n.readRestoreTargets(ctx, txn, plan)
		if err != nil {
			return err
		}
		if plan.newDB != nil {
			nd := plan.newDB.Clone()
			if owner := catalog.OwnerOf(nd.Owner); owner != catalog.RootRole {
				if rd, err := catalog.LookupRole(ctx, txn, owner); err != nil {
					return err
				} else if rd == nil {
					notes = append(notes, fmt.Sprintf("database %s: owner %q does not exist here; owned by %s", nd.Name, owner, catalog.RootRole))
					nd.Owner = catalog.RootRole
				}
			}
			// The database's own grants, like its tables', come back for
			// the roles this cluster has.
			known := map[string]bool{catalog.PublicRole: true}
			has := func(name string) (bool, error) {
				if ok, seen := known[name]; seen {
					return ok, nil
				}
				rd, err := catalog.LookupRole(ctx, txn, name)
				known[name] = rd != nil
				return rd != nil, err
			}
			for _, m := range []map[string][]string{nd.Privileges, nd.GrantOptions, nd.SchemaPrivileges, nd.SchemaGrantOptions} {
				for grantee := range m {
					ok, err := has(grantee)
					if err != nil {
						return err
					}
					if !ok {
						delete(m, grantee)
						if note := fmt.Sprintf("database %s: grants to %q not restored (no such role here)", nd.Name, grantee); !containsString(notes, note) {
							notes = append(notes, note)
						}
					}
				}
			}
			keptDefaults := nd.DefaultPrivileges[:0:0]
			for _, dp := range nd.DefaultPrivileges {
				forOK, err := has(dp.ForRole)
				if err != nil {
					return err
				}
				toOK, err := has(dp.Grantee)
				if err != nil {
					return err
				}
				if forOK && toOK {
					keptDefaults = append(keptDefaults, dp)
				}
			}
			nd.DefaultPrivileges = keptDefaults
			if err := cat.CreateDatabase(ctx, txn, nd); err != nil {
				return err
			}
			for _, p := range plan.tables {
				tg.dbIDs[p.intoDB] = nd.ID
			}
		}
		// The enum types the restored columns need and the target lacks,
		// made from the backup: its labels are the columns' own copies,
		// which are what the rows were written against; the owner is the
		// backup's when that role exists here.
		for _, p := range plan.tables {
			for _, c := range p.srcDesc.Columns {
				tk := restoreTypeKey{p.intoDB, c.EnumName}
				if c.EnumType == 0 || tg.types[tk] != 0 {
					continue
				}
				td := &catalog.TypeDescriptor{Name: c.EnumName, DatabaseID: tg.dbIDs[p.intoDB], Labels: append([]string(nil), c.EnumLabels...)}
				if src, ok := plan.types[c.EnumType]; ok && src.Owner != "" && src.Owner != catalog.RootRole {
					if rd, err := catalog.LookupRole(ctx, txn, src.Owner); err != nil {
						return err
					} else if rd != nil {
						td.Owner = src.Owner
					}
				}
				if err := catalog.CreateType(ctx, txn, td); err != nil {
					return err
				}
				tg.types[tk] = td.ID
				notes = append(notes, fmt.Sprintf("enum type %s.%s created for %s", p.intoDB, c.EnumName, p.into()))
			}
		}
		ids := map[uint64]uint64{}
		for _, p := range plan.tables {
			ids[p.src.ID] = p.newID
		}
		knotes, err := n.reinstateSealedKeys(ctx, txn, plan.sealed)
		if err != nil {
			return err
		}
		notes = append(notes, knotes...)
		roles := func(name string) bool { return tg.roles[name] }
		for _, p := range plan.tables {
			dbID := tg.dbIDs[p.intoDB]
			seqIDs, seqNames := map[uint64]uint64{}, map[uint64]string{}
			for _, s := range p.seqs {
				sd := s.desc
				sd.ID, sd.DatabaseID, sd.Name = 0, dbID, restoredSequenceName(p, s)
				sd.OwnerTable = p.newID
				sd.SchemaID = 0 // with its table, in public (the only schema a table restore goes to)
				if !roles(catalog.OwnerOf(sd.Owner)) {
					sd.Owner = catalog.RootRole
				}
				for _, m := range []map[string][]string{sd.Privileges, sd.GrantOptions} {
					for grantee := range m {
						if grantee != catalog.PublicRole && !roles(grantee) {
							delete(m, grantee)
						}
					}
				}
				if err := catalog.CreateSequence(ctx, txn, &sd); err != nil {
					return err
				}
				// The counter resumes where the backup left it, which is
				// at or past every value the restored rows hold.
				if s.value != nil {
					if err := txn.Put(ctx, keys.SequenceValueKey(sd.ID), s.value); err != nil {
						return err
					}
				}
				seqIDs[s.desc.ID], seqNames[s.desc.ID] = sd.ID, sd.Name
			}
			d, dnotes := restoredDescriptor(p, dbID, ids, seqIDs, seqNames, roles)
			notes = append(notes, dnotes...)
			for i := range d.Columns {
				if c := &d.Columns[i]; c.EnumType != 0 {
					c.EnumType = tg.types[restoreTypeKey{p.intoDB, c.EnumName}]
				}
			}
			raw, err := json.Marshal(d)
			if err != nil {
				return err
			}
			if err := txn.Put(ctx, keys.TableDescKey(d.ID), raw); err != nil {
				return err
			}
			if err := txn.Put(ctx, keys.TableNamespaceKey(dbID, d.Name), []byte(fmt.Sprintf("%d", d.ID))); err != nil {
				return err
			}
			if d.Sealed() && d.EncryptionKey.Sealing {
				// Backed up while its rows were being sealed: the
				// restored copy holds plain rows too, and needs the job.
				if err := putSealJobFor(ctx, txn, d); err != nil {
					return err
				}
			}
		}
		return txn.Delete(ctx, keys.RestoreProgressKey(opID))
	})
	if err != nil {
		return nil, err
	}
	sum.Notes = append(sum.Notes, notes...)
	for _, p := range plan.tables {
		log.Infof("restore: table %s restored as %s (id %d): %d live records", p.from(), p.into(), p.newID, p.records)
	}
	return sum, nil
}

// reclaimRestore removes what a table restore that will not finish has
// written: the record is marked first, so the restore cannot publish
// from here on; then the rows go; then the record. Each step can be
// repeated, and a failure leaves the record for the next attempt.
func (n *Node) reclaimRestore(ctx context.Context, opID string) bool {
	var rec *restoreProgress
	err := n.db.RunTxn(ctx, "restore-table-reclaim", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		if rec, err = readRestoreProgress(ctx, txn, opID); err != nil || rec == nil || rec.Reclaiming {
			return err
		}
		rec.Reclaiming = true
		raw, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return txn.Put(ctx, keys.RestoreProgressKey(opID), raw)
	})
	if err != nil {
		log.Warnf("restore %s: marking for reclaim: %v", opID, err)
		return false
	}
	if rec == nil {
		return false // published, or already reclaimed
	}
	for _, id := range rec.TableIDs {
		start, end := keys.TableDataSpan(id)
		if err := n.wipeSpan(ctx, start, end); err != nil {
			log.Warnf("restore %s: removing the rows loaded under table id %d: %v", opID, id, err)
			return false
		}
	}
	if err := n.db.Delete(ctx, keys.RestoreProgressKey(opID)); err != nil {
		log.Warnf("restore %s: removing its record: %v", opID, err)
		return false
	}
	log.Infof("restore %s: removed the rows of %d unpublished table(s)", opID, len(rec.TableIDs))
	return true
}

// wipeSpan deletes every key in [start, end) in transactions of
// restoreChunk deletes. Each transaction resumes after the last key the
// one before it deleted, so a large span is walked once rather than
// rescanned past its own tombstones from the start every time. The
// callers' spans take no new writes (no descriptor names them), so
// nothing appears behind the cursor.
func (n *Node) wipeSpan(ctx context.Context, start, end keys.Key) error {
	cur := start.Clone()
	for {
		var found int
		var last keys.Key
		err := n.db.RunTxn(ctx, "wipe-span", func(ctx context.Context, txn *kvclient.Txn) error {
			kvs, err := txn.Scan(ctx, cur, end, restoreChunk)
			if err != nil {
				return err
			}
			found = len(kvs)
			var wb kvclient.WriteBatch
			for _, kv := range kvs {
				wb.Delete(kv.Key)
			}
			if wb.Len() == 0 {
				return nil
			}
			last = keys.Key(kvs[len(kvs)-1].Key).Clone()
			return txn.RunBatch(ctx, &wb)
		})
		if err != nil || found < restoreChunk {
			return err
		}
		cur = last.Next()
	}
}

// restoreJanitorLoop removes the rows of table restores that died with
// their node. Every node runs it, because the node that ran the restore
// may be the one that is gone.
func (n *Node) restoreJanitorLoop(ctx context.Context) {
	// The first pass comes soon after start: a node back from a crash
	// holds the records of the restores it was running.
	wait := 10 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = restoreJanitorInterval
		n.RunRestoreJanitorOnce(ctx)
	}
}

// RunRestoreJanitorOnce reclaims every table restore that is not running
// any more: one this node's record names and this process is not running
// (the node restarted under it), one already marked for reclaiming, and
// one whose record has gone restoreAbandonAfter without a heartbeat. It
// returns how many it reclaimed.
func (n *Node) RunRestoreJanitorOnce(ctx context.Context) int {
	if n.db.ClusterVersion() < version.V21 {
		return 0
	}
	start, end := keys.RestoreProgressSpan()
	kvs, err := n.db.Scan(ctx, start, end, 0)
	if err != nil {
		log.Debugf("restore janitor: scan: %v", err)
		return 0
	}
	reclaimed := 0
	now := n.clock.Now().WallTime
	for _, kv := range kvs {
		var rec restoreProgress
		if json.Unmarshal(kv.Value, &rec) != nil || rec.OpID == "" {
			continue
		}
		_, running := activeRestores.Load(activeRestoreKey{n, rec.OpID})
		mine := rec.NodeID == n.ident.NodeID
		stale := now-rec.Heartbeat > restoreAbandonAfter.Nanoseconds()
		if running || !(mine || rec.Reclaiming || stale) {
			continue
		}
		opID := newOpID()
		n.events.RecordStart("restore-reclaim", opID, "removing the rows of table restore %s, which did not finish", rec.OpID)
		if n.reclaimRestore(ctx, rec.OpID) {
			reclaimed++
			n.events.RecordEnd("restore-reclaim", opID, "ok", "removed the rows of table restore %s (%d table(s))", rec.OpID, len(rec.TableIDs))
		} else {
			n.events.RecordEnd("restore-reclaim", opID, "failed", "table restore %s: rows not removed yet; will retry", rec.OpID)
		}
	}
	return reclaimed
}
