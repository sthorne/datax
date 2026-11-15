package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowseal"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Sealed tables (issue #310, step 3; docs/encryption.md, "Sealed
// tables"): CREATE TABLE ... WITH (encryption_key = 'auto') gives the
// table a key of its own in datax_keys, and every row value is sealed
// under it on its way to KV and opened on its way back (pkg/sql/rowseal;
// putRowValue and decodeFullRow are the two doors). Primary-key and
// indexed columns stay in KV keys, in plaintext; CREATE TABLE and CREATE
// INDEX say which, in a NOTICE.
//
// DROP TABLE destroys the table's key once the drop has committed (and
// after the descriptor lease has drained, as the drop's own DDL finish
// waits for that): the dropped table's rows, which stay in KV until the
// table's data is reclaimed, are unreadable from then on, AS OF SYSTEM
// TIME included. That is the erasure a drop of a sealed table promises;
// restoring a backup taken before the drop brings the table back with its
// key (the documented dependency on backup retention).

// Table options of sealing.
const (
	optEncryptionKey   = "encryption_key"
	optEncryptionKeyBy = "encryption_key_by"
)

// sealOption reports whether name is an option of sealing.
func sealOption(name string) bool {
	return name == optEncryptionKey || name == optEncryptionKeyBy
}

// CodeKeyUnavailable is the SQLSTATE of a statement that touches a row of
// a sealed table whose key is destroyed, disabled or unavailable: class
// XX, like PostgreSQL's data_corrupted (XX001) — the data cannot be read,
// and retrying will not change that — with a subclass of datax's own
// ("K" is in the implementation-defined range), so that a client can tell
// a key that is gone, which is erasure working as asked, from corruption.
// A sealed value that does not authenticate is XX001 proper.
const (
	CodeKeyUnavailable = "XXK01"
	CodeDataCorrupted  = "XX001"
)

// checkSealOptions validates a CREATE TABLE's sealing options against
// desc before the table is created, and reports whether it is to be
// sealed.
func (s *Session) checkSealOptions(ctx context.Context, desc *catalog.TableDescriptor, options map[string]string) (bool, error) {
	if _, ok := options[optEncryptionKeyBy]; ok {
		return false, newErrf(CodeFeatureNotSupported, "per-tenant keys (encryption_key_by) are not built yet: seal the whole table with encryption_key = 'auto'")
	}
	v, ok := options[optEncryptionKey]
	if !ok {
		return false, nil
	}
	if !strings.EqualFold(strings.TrimSpace(v), "auto") {
		return false, newErrf(CodeInvalidParameterValue, "encryption_key must be 'auto' (a key of the table's own, made now), not %q", v)
	}
	if desc.Timeseries {
		return false, newErrf(CodeFeatureNotSupported, "table %q is a time-series table, and time-series tables cannot be sealed: their rows are stored in chunks that mix series. Keep personal data in an ordinary sealed table that references it", desc.Name)
	}
	if desc.ID != 0 {
		return false, newErrf(CodeInsufficientPriv, "the cluster's own tables cannot be sealed")
	}
	if s.db.ClusterVersion() < version.V22 {
		return false, newErrf(CodeFeatureNotSupported, "sealed tables need cluster version %s: finalize the upgrade with `datax debug upgrade` first", version.V22)
	}
	store := s.rowSealer().Store()
	if store == nil {
		return false, newErrf(CodeFeatureNotSupported, "sealed tables need a key store, and this node has none")
	}
	if err := store.Ready(ctx); err != nil {
		return false, newErrf(CodeFeatureNotSupported, "%v", err)
	}
	return true, nil
}

// sealNewTable makes the new table's key, in the creating transaction,
// and records it in desc (which the caller writes).
func (s *Session) sealNewTable(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor) error {
	id, err := s.rowSealer().Store().CreateTableKey(ctx, txn, fmt.Sprintf("table/%d", desc.ID))
	if err != nil {
		return newErrf(CodeFeatureNotSupported, "making the key of sealed table %q: %v", desc.Name, err)
	}
	desc.EncryptionKey = &catalog.TableEncryptionKey{KeyID: id, SealID: desc.ID}
	log.Audit("sealed-table", "stmt", "CREATE TABLE", "target", desc.Name, "key_id", fmt.Sprint(id), "principal", s.sessionUser, "role", s.user)
	return nil
}

// refuseAlterSeal refuses turning sealing on, off or over on an
// existing table: its rows would have to be rewritten, which the
// background job of a later step does.
func refuseAlterSeal(options map[string]string) *Error {
	for k := range options {
		if sealOption(k) {
			return newErrf(CodeFeatureNotSupported, "ALTER TABLE ... SET (%s = ...) is not built yet: sealing an existing table needs its rows rewritten. Create a new table WITH (encryption_key = 'auto') and copy the rows into it", k)
		}
	}
	return nil
}

// plaintextColumnNames names a sealed table's columns that are not
// sealed, each with why, in column order.
func plaintextColumnNames(desc *catalog.TableDescriptor) []string {
	why := map[catalog.ColumnID][]string{}
	for _, id := range desc.PrimaryKey {
		why[id] = append(why[id], "primary key")
	}
	for _, idx := range desc.Indexes {
		for _, id := range idx.ColumnIDs {
			why[id] = append(why[id], fmt.Sprintf("index %s", idx.Name))
		}
	}
	var out []string
	for _, c := range desc.Columns {
		w, ok := why[c.ID]
		if !ok {
			continue
		}
		name := c.Name
		if c.Hidden {
			name = fmt.Sprintf("(hidden column %s)", c.Name)
		}
		out = append(out, fmt.Sprintf("%s (%s)", name, strings.Join(w, ", ")))
	}
	return out
}

// sealedTableNotice is CREATE TABLE's NOTICE for a sealed table.
func sealedTableNotice(desc *catalog.TableDescriptor) string {
	return fmt.Sprintf("table %q is sealed: its row values are encrypted under a key of its own. Primary-key and indexed columns are not sealed and stay readable to anyone who can read the database's storage: %s",
		desc.Name, strings.Join(plaintextColumnNames(desc), ", "))
}

// sealedIndexNotice is CREATE INDEX's NOTICE on a sealed table.
func sealedIndexNotice(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor) string {
	var names []string
	for _, id := range idx.ColumnIDs {
		if c, ok := desc.ColByID(id); ok {
			names = append(names, c.Name)
		}
	}
	extra := ""
	if idx.Vector != nil {
		extra = " (a vector index also keeps each row's vector in its entries)"
	}
	return fmt.Sprintf("table %q is sealed, and indexed columns are not: the values of %s are stored in plaintext in index %s%s",
		desc.Name, strings.Join(names, ", "), idx.Name, extra)
}

// queueKeyDestroy arranges for a dropped sealed table's key to be
// destroyed once the drop commits — unless another table still uses it
// (a table restored from this one shares its key).
func (s *Session) queueKeyDestroy(ctx context.Context, txn *kvclient.Txn, dropped *catalog.TableDescriptor) error {
	if !dropped.Sealed() {
		return nil
	}
	all, err := s.cat.List(ctx, txn)
	if err != nil {
		return err
	}
	for _, d := range all {
		if d.ID != dropped.ID && d.Sealed() && d.EncryptionKey.KeyID == dropped.EncryptionKey.KeyID {
			return nil
		}
	}
	s.pendingWipes = append(s.pendingWipes, indexWipe{tableID: dropped.ID, destroyKey: dropped.EncryptionKey.KeyID, table: dropped.Name})
	return nil
}

// destroyTableKey destroys a dropped sealed table's key (best effort,
// after commit: the table is gone either way, and the node's janitor
// destroys a table key no table uses if this fails).
func (s *Session) destroyTableKey(ctx context.Context, w indexWipe) {
	store := s.rowSealer().Store()
	if store == nil {
		log.Warnf("sealed table %q dropped, but this node has no key store to destroy its key %d", w.table, w.destroyKey)
		return
	}
	if err := store.DestroyKey(ctx, w.destroyKey); err != nil {
		log.Warnf("sealed table %q dropped, but destroying its key %d failed: %v", w.table, w.destroyKey, err)
		return
	}
	s.rowSealer().Invalidate(w.destroyKey)
	log.Audit("sealed-table", "stmt", "DROP TABLE", "target", w.table, "key_id", fmt.Sprint(w.destroyKey), "outcome", "key destroyed", "principal", s.sessionUser, "role", s.user)
}

// sealErrorToSQL maps the boundary's errors to SQLSTATEs.
func sealErrorToSQL(err error) (*Error, bool) {
	var ku *rowseal.KeyUnavailableError
	if asErr(err, &ku) {
		return &Error{Code: CodeKeyUnavailable, Msg: ku.Error()}, true
	}
	var ce *rowseal.CorruptError
	if asErr(err, &ce) {
		return &Error{Code: CodeDataCorrupted, Msg: ce.Error()}, true
	}
	return nil, false
}
