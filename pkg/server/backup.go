package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Backup and restore (issue #45). A backup is a directory the serving node
// writes: a JSON manifest plus one framed data file per table, all
// captured with consistent Export reads at a single timestamp. Restore
// applies a chain of such directories (one full, then incrementals) into
// an EMPTY cluster, preserving table IDs so index data restores raw.
//
// The manifest deliberately re-snapshots descriptors and users on every
// backup (they are tiny), so any chain element could reconstruct the
// schema; restore uses the LAST element's metadata — tables dropped
// mid-chain simply have their leftover data files skipped.

const (
	backupManifestMagic = "DXBK1"
	backupManifestName  = "BACKUP.json"
	// backupExportChunk bounds one Export call's record count: memory on
	// both sides and the JSON/proto envelope size.
	backupExportChunk = 4096
	// restoreChunk bounds one restore transaction's write batch.
	restoreChunk = 1024
)

// backupDataFile is the name of table id's data file inside its backup
// directory. Backup writes it; restore derives it again from the id
// rather than joining whatever the manifest says onto the directory
// (issue #214): a manifest is operator-supplied input, and one carrying
// "../../etc/shadow" would otherwise be read with the node's own uid.
func backupDataFile(id uint64) string {
	return fmt.Sprintf("table_%d.dxbk", id)
}

// backupKV is one raw system key/value captured verbatim (users, admin
// markers).
type backupKV struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type backupTable struct {
	ID   uint64 `json:"id"`
	Name string `json:"name"`
	// Descriptor is the verbatim descriptor JSON at the backup timestamp
	// (privileges ride inside it).
	Descriptor json.RawMessage `json:"descriptor"`
	// File is the data file's name inside the backup directory.
	File    string `json:"file"`
	Records int64  `json:"records"`
	Bytes   int64  `json:"bytes"`
	// SHA256 covers the LIVE (non-tombstone) records' key/value bytes in
	// key order — comparable to a fresh full export of a restored table.
	SHA256 string `json:"sha256"`
}

type backupManifest struct {
	Magic     string        `json:"magic"`
	ClusterID string        `json:"cluster_id"`
	EndTS     hlc.Timestamp `json:"end_ts"`
	// BaseTS is zero for a full backup; an incremental exports the window
	// (BaseTS, EndTS] and restores only on top of its exact base.
	BaseTS    hlc.Timestamp `json:"base_ts"`
	CreatedAt time.Time     `json:"created_at"`
	Tables    []backupTable `json:"tables"`
	Users     []backupKV    `json:"users"`
	Admins    []backupKV    `json:"admins"`
	// Roles are the v11 role descriptors (login roles, groups, their
	// memberships and verifiers); Users and Admins the pre-v11 records.
	Roles []backupKV `json:"roles,omitempty"`
	// Databases are the database descriptors and their name entries (v6);
	// a manifest without them restores every table into the flat layout,
	// which the catalog migration then moves under the default database.
	Databases []backupKV `json:"databases,omitempty"`
	// Sequences are the sequence descriptors, names and counters (v7).
	Sequences []backupKV `json:"sequences,omitempty"`
}

// exportAll streams every export record in [start, end) for the window
// (startTS, endTS] through fn, chunking and pushing intents as needed. A
// live conflicting transaction is waited out with backoff — a backup
// prefers lateness to inconsistency.
func (n *Node) exportAll(ctx context.Context, start, end keys.Key, startTS, endTS hlc.Timestamp, fn func(kvpb.ExportRecord) error) error {
	cur := start.Clone()
	for {
		resp, err := n.db.ExportSpan(ctx, cur, end, startTS, endTS, backupExportChunk)
		if err != nil {
			var kerr *kvpb.Error
			if errors.As(err, &kerr) && kerr.WriteIntent != nil {
				resolved, perr := n.db.PushAndResolveIntents(ctx, kerr.WriteIntent.Intents)
				if perr != nil {
					return perr
				}
				if !resolved {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(100 * time.Millisecond):
					}
				}
				continue
			}
			return err
		}
		for _, rec := range resp.Records {
			if err := fn(rec); err != nil {
				return err
			}
		}
		if len(resp.Resume) == 0 {
			return nil
		}
		cur = keys.Key(resp.Resume).Clone()
	}
}

// writeBackupRecord frames one record into w: uvarint key length, key,
// uvarint value length, value, one deleted byte.
func writeBackupRecord(w io.Writer, rec kvpb.ExportRecord) error {
	buf := encoding.EncodeUvarint(nil, uint64(len(rec.Key)))
	buf = append(buf, rec.Key...)
	buf = encoding.EncodeUvarint(buf, uint64(len(rec.Value)))
	buf = append(buf, rec.Value...)
	if rec.Deleted {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	_, err := w.Write(buf)
	return err
}

// readBackupRecords streams a data file's records through fn.
// readBackupFile reads one file of a backup directory — the manifest, or
// a table's data file — and only if it is the plain file backup wrote.
// The names are derived (backupDataFile), so a manifest can no longer
// name a path outside its directory — but the filesystem still can: a
// symlink at the derived name reaches wherever it points, with the
// node's uid, and restore would apply what it found there. A datax
// backup writes plain files and nothing else, so a symlink, a directory
// or a device at one of its names is refused (issue #214, from the PR's
// review). The name is looked at without following it, which is what
// tells a symlink from what it points at; then the file is opened and
// the object actually opened is compared with what was looked at, so a
// symlink swapped in between the two is refused too rather than
// followed. A hard link is a plain file and is read: it names the same
// object, and making one already requires being able to write it.
func readBackupFile(path string) ([]byte, error) {
	fi, err := backupFileIsPlain(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(fi, opened) || !opened.Mode().IsRegular() {
		return nil, fmt.Errorf("%s changed between being looked at and being opened: refusing to read it", path)
	}
	return io.ReadAll(f)
}

// backupFileIsPlain looks at a file of a backup directory without
// following it and reports what it saw, refusing anything but a regular
// file.
func backupFileIsPlain(path string) (os.FileInfo, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file (%s): a datax backup writes plain files, and restore reads nothing else", path, fi.Mode().Type())
	}
	return fi, nil
}

// checkBackupFiles looks at every data file a manifest names, before
// restore has applied anything: the data files are read only after the
// schema is, so a refusal that waited for the read would leave the
// target with the tables and none of the rows. This is restore's
// concern, not the manifest reader's: an incremental's --base takes only
// the cluster id and the end timestamp from its base's manifest, and an
// operator who keeps a completed backup's manifest to hand and its data
// files in cold storage can still chain onto it.
func checkBackupFiles(dir string, man *backupManifest) error {
	for _, t := range man.Tables {
		if _, err := backupFileIsPlain(filepath.Join(dir, backupDataFile(t.ID))); err != nil {
			return err
		}
	}
	return nil
}

func readBackupRecords(path string, fn func(kvpb.ExportRecord) error) error {
	raw, err := readBackupFile(path)
	if err != nil {
		return err
	}
	for len(raw) > 0 {
		rest, klen, err := encoding.DecodeUvarint(raw)
		if err != nil {
			return fmt.Errorf("%s: corrupt record framing: %v", path, err)
		}
		if uint64(len(rest)) < klen {
			return fmt.Errorf("%s: truncated record key", path)
		}
		key := keys.Key(rest[:klen]).Clone()
		rest, vlen, err := encoding.DecodeUvarint(rest[klen:])
		if err != nil {
			return fmt.Errorf("%s: corrupt record framing: %v", path, err)
		}
		if uint64(len(rest)) < vlen+1 {
			return fmt.Errorf("%s: truncated record value", path)
		}
		val := append([]byte(nil), rest[:vlen]...)
		rest = rest[vlen:]
		rec := kvpb.ExportRecord{Key: key, Deleted: rest[0] == 1}
		if !rec.Deleted {
			rec.Value = val
		}
		raw = rest[1:]
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

// hashLiveRecord folds a live record into a table checksum.
func hashLiveRecord(h hash.Hash, rec kvpb.ExportRecord) {
	_, _ = h.Write(encoding.EncodeUvarint(nil, uint64(len(rec.Key))))
	_, _ = h.Write(rec.Key)
	_, _ = h.Write(encoding.EncodeUvarint(nil, uint64(len(rec.Value))))
	_, _ = h.Write(rec.Value)
}

// RunBackup executes a backup to dest on this node's filesystem.
func (n *Node) RunBackup(ctx context.Context, dest, basePath string, allowPlaintext, includeMetrics bool) (_ *cluster.BackupSummary, err error) {
	if n.cfg.EncKeyPath != "" && !allowPlaintext {
		return nil, fmt.Errorf("the store is encrypted but backup files are written in plaintext; pass --allow-plaintext to proceed")
	}
	// A backup is long enough to be worth watching while it runs
	// (issue #153): the start is recorded here and the end below, under
	// one op id, so the operations view can show it in flight with an
	// elapsed time rather than only as a row that appears when it is
	// already over.
	opID := newOpID()
	n.events.RecordStart("backup", opID, "backup to %s started", dest)
	defer func() {
		if err != nil {
			n.events.RecordEnd("backup", opID, "failed", "backup to %s failed: %v", dest, err)
		}
	}()
	var baseTS hlc.Timestamp
	if basePath != "" {
		base, err := readBackupManifest(basePath)
		if err != nil {
			return nil, fmt.Errorf("reading base backup: %w", err)
		}
		if base.ClusterID != n.ident.ClusterID.String() {
			return nil, fmt.Errorf("base backup is from cluster %s, this is %s", base.ClusterID, n.ident.ClusterID)
		}
		baseTS = base.EndTS
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dest, backupManifestName)); err == nil {
		return nil, fmt.Errorf("%s already holds a backup", dest)
	}

	endTS := n.clock.Now()
	man := backupManifest{
		Magic:     backupManifestMagic,
		ClusterID: n.ident.ClusterID.String(),
		EndTS:     endTS,
		BaseTS:    baseTS,
		CreatedAt: time.Now().UTC(),
	}

	// Descriptors, users, and admin markers: always the full set at endTS
	// (tiny; makes every chain element self-describing for the schema).
	descStart, descEnd := keys.TableDescSpan()
	var descs []backupTable
	err = n.exportAll(ctx, descStart, descEnd, hlc.Timestamp{}, endTS, func(rec kvpb.ExportRecord) error {
		if rec.Deleted {
			return nil
		}
		var d catalog.TableDescriptor
		if err := json.Unmarshal(rec.Value, &d); err != nil {
			return fmt.Errorf("corrupt table descriptor at %s: %v", keys.Key(rec.Key), err)
		}
		if d.Name == catalog.MetricsTableName && !includeMetrics {
			return nil // the cluster's own metrics: bulky, regenerable, opt-in
		}
		// Every other system table is backed up unconditionally. The
		// console's preferences (datax_ui_prefs) are small and are not
		// regenerable from anything — losing them across a restore
		// silently resets every operator's display choices.
		descs = append(descs, backupTable{ID: d.ID, Name: d.Name, Descriptor: append(json.RawMessage(nil), rec.Value...)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	collectRaw := func(start, end keys.Key) ([]backupKV, error) {
		var out []backupKV
		err := n.exportAll(ctx, start, end, hlc.Timestamp{}, endTS, func(rec kvpb.ExportRecord) error {
			if !rec.Deleted {
				out = append(out, backupKV{Key: rec.Key, Value: rec.Value})
			}
			return nil
		})
		return out, err
	}
	uStart, uEnd := keys.UserSpan()
	if man.Users, err = collectRaw(uStart, uEnd); err != nil {
		return nil, err
	}
	aStart, aEnd := keys.AdminUserSpan()
	if man.Admins, err = collectRaw(aStart, aEnd); err != nil {
		return nil, err
	}
	rStart, rEnd := keys.RoleSpan()
	if man.Roles, err = collectRaw(rStart, rEnd); err != nil {
		return nil, err
	}
	dStart, dEnd := keys.DatabaseDescSpan()
	if man.Databases, err = collectRaw(dStart, dEnd); err != nil {
		return nil, err
	}
	nStart, nEnd := keys.DatabaseNamespaceSpan()
	dbNames, err := collectRaw(nStart, nEnd)
	if err != nil {
		return nil, err
	}
	man.Databases = append(man.Databases, dbNames...)
	for _, span := range [][2]keys.Key{spanOf(keys.SequenceDescSpan()), spanOf(keys.AllSequenceNamespaceSpan()), spanOf(keys.SequenceValueSpan())} {
		kvs, err := collectRaw(span[0], span[1])
		if err != nil {
			return nil, err
		}
		man.Sequences = append(man.Sequences, kvs...)
	}

	// Table data: the (baseTS, endTS] window over each table's full data
	// span — every index, raw, so restore needs no backfill.
	for _, bt := range descs {
		bt.File = backupDataFile(bt.ID)
		f, err := os.Create(filepath.Join(dest, bt.File))
		if err != nil {
			return nil, err
		}
		h := sha256.New()
		start, end := keys.TableDataSpan(bt.ID)
		err = n.exportAll(ctx, start, end, baseTS, endTS, func(rec kvpb.ExportRecord) error {
			if err := writeBackupRecord(f, rec); err != nil {
				return err
			}
			bt.Records++
			if !rec.Deleted {
				bt.Bytes += int64(len(rec.Key) + len(rec.Value))
				hashLiveRecord(h, rec)
			}
			return nil
		})
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, fmt.Errorf("exporting table %s: %w", bt.Name, err)
		}
		bt.SHA256 = hex.EncodeToString(h.Sum(nil))
		man.Tables = append(man.Tables, bt)
		log.Infof("backup: table %s (id %d): %d records, %d live bytes", bt.Name, bt.ID, bt.Records, bt.Bytes)
	}

	n.events.RecordEnd("backup", opID, "ok", "backup written to %s: %d tables", dest, len(descs))
	// Manifest last, atomically: its presence marks the backup complete.
	raw, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return nil, err
	}
	tmp := filepath.Join(dest, backupManifestName+".tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, filepath.Join(dest, backupManifestName)); err != nil {
		return nil, err
	}
	return backupSummary(dest, &man), nil
}

func readBackupManifest(dir string) (*backupManifest, error) {
	raw, err := readBackupFile(filepath.Join(dir, backupManifestName))
	if err != nil {
		return nil, err
	}
	var man backupManifest
	if err := json.Unmarshal(raw, &man); err != nil {
		return nil, err
	}
	if man.Magic != backupManifestMagic {
		return nil, fmt.Errorf("%s: not a datax backup (magic %q)", dir, man.Magic)
	}
	// A backup names its own files, and nothing else: refused here, before
	// any data file is opened, so neither door — restore, or the base of
	// an incremental — reaches a path the manifest chose (issue #214).
	for _, t := range man.Tables {
		if want := backupDataFile(t.ID); t.File != want {
			return nil, fmt.Errorf("%s: manifest names %q as the data file for table %d, which a datax backup keeps in %q",
				dir, t.File, t.ID, want)
		}
	}
	if err := checkManifestKeys(dir, &man); err != nil {
		return nil, err
	}
	return &man, nil
}

// checkManifestKeys holds a manifest's raw key/value groups — users,
// admins, roles, databases, sequences — to the key spans backup collects
// each from. Restore writes those keys back verbatim, so a crafted
// manifest could otherwise have it write any key in the cluster, as the
// node, inside restore's own transaction: the authentication secret
// (keys.AuthSecretKey, outside every span a backup collects, so no
// legitimate backup carries it) would hand the console's session
// signing key to whoever wrote the manifest (issue #238). Checked when
// the manifest is read, like the file names, so both readers refuse it
// before anything is applied. Databases and sequences legitimately mix
// spans — descriptors and names, and counters — so each group is held
// to the set backup reads for it, not to one span.
func checkManifestKeys(dir string, man *backupManifest) error {
	for _, g := range []struct {
		name  string
		kvs   []backupKV
		spans [][2]keys.Key
	}{
		{"users", man.Users, [][2]keys.Key{spanOf(keys.UserSpan())}},
		{"admins", man.Admins, [][2]keys.Key{spanOf(keys.AdminUserSpan())}},
		{"roles", man.Roles, [][2]keys.Key{spanOf(keys.RoleSpan())}},
		{"databases", man.Databases, [][2]keys.Key{spanOf(keys.DatabaseDescSpan()), spanOf(keys.DatabaseNamespaceSpan())}},
		{"sequences", man.Sequences, [][2]keys.Key{spanOf(keys.SequenceDescSpan()), spanOf(keys.AllSequenceNamespaceSpan()), spanOf(keys.SequenceValueSpan())}},
	} {
		for _, kv := range g.kvs {
			if !inAnySpan(keys.Key(kv.Key), g.spans) {
				return fmt.Errorf("%s: manifest's %s carries the key %s, which a datax backup never collects there", dir, g.name, keys.Key(kv.Key))
			}
		}
	}
	return nil
}

func inAnySpan(k keys.Key, spans [][2]keys.Key) bool {
	for _, s := range spans {
		if k.Compare(s[0]) >= 0 && k.Compare(s[1]) < 0 {
			return true
		}
	}
	return false
}

func backupSummary(path string, man *backupManifest) *cluster.BackupSummary {
	sum := &cluster.BackupSummary{
		Path:        path,
		ClusterID:   man.ClusterID,
		EndTSNanos:  man.EndTS.WallTime,
		Incremental: !man.BaseTS.IsEmpty(),
		Users:       len(man.Users) + len(man.Roles),
	}
	for _, t := range man.Tables {
		sum.Tables = append(sum.Tables, cluster.BackupTableSummary{
			ID: t.ID, Name: t.Name, Records: t.Records, Bytes: t.Bytes, SHA256: t.SHA256,
		})
	}
	return sum
}

// RunRestore applies a backup chain (full first, then incrementals in
// order) into this — empty — cluster, then re-exports each table and
// reports fresh checksums for verification against the source.
func (n *Node) RunRestore(ctx context.Context, srcs []string) (_ *cluster.BackupSummary, err error) {
	if len(srcs) == 0 {
		return nil, fmt.Errorf("restore requires at least one backup directory")
	}
	opID := newOpID()
	n.events.RecordStart("restore", opID, "restore from %d backup(s) started", len(srcs))
	defer func() {
		if err != nil {
			n.events.RecordEnd("restore", opID, "failed", "restore failed: %v", err)
		}
	}()
	mans := make([]*backupManifest, len(srcs))
	for i, src := range srcs {
		man, err := readBackupManifest(src)
		if err != nil {
			return nil, err
		}
		if err := checkBackupFiles(src, man); err != nil {
			return nil, err
		}
		mans[i] = man
	}
	if !mans[0].BaseTS.IsEmpty() {
		return nil, fmt.Errorf("%s is an incremental backup; the chain must start with a full one", srcs[0])
	}
	for i := 1; i < len(mans); i++ {
		if mans[i].ClusterID != mans[0].ClusterID {
			return nil, fmt.Errorf("%s is from a different cluster than %s", srcs[i], srcs[0])
		}
		if !mans[i].BaseTS.Equal(mans[i-1].EndTS) {
			return nil, fmt.Errorf("%s (base %s) does not chain onto %s (end %s)",
				srcs[i], mans[i].BaseTS, srcs[i-1], mans[i-1].EndTS)
		}
	}
	final := mans[len(mans)-1]

	// The target must hold no user tables: restored data keys bake in the
	// backed-up table IDs, which an existing catalog could collide with.
	// The cluster's own metrics table does not count: it lives at a
	// reserved ID no user table can carry, and a backup that includes it
	// simply lands on top (same ID, same name; the rows merge). The
	// recorder is held off while the restore runs.
	descStart, descEnd := keys.TableDescSpan()
	existing, err := n.db.Scan(ctx, descStart, descEnd, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range existing {
		var d catalog.TableDescriptor
		if json.Unmarshal(kv.Value, &d) != nil || !catalog.IsSystemTableID(d.ID) {
			return nil, fmt.Errorf("the target cluster already has tables; restore requires an empty cluster")
		}
	}
	n.metricsPaused.Store(true)
	defer n.metricsPaused.Store(false)

	// Metadata from the final manifest: descriptors + namespace entries,
	// users, admin markers — one small transaction.
	var maxID uint64
	err = n.db.RunTxn(ctx, "restore-metadata", func(ctx context.Context, txn *kvclient.Txn) error {
		var wb kvclient.WriteBatch
		for _, kv := range final.Databases {
			wb.Put(keys.Key(kv.Key), kv.Value)
		}
		for _, kv := range final.Sequences {
			wb.Put(keys.Key(kv.Key), kv.Value)
			if id, ok := catalog.SequenceIDOfKey(keys.Key(kv.Key)); ok && id > maxID {
				maxID = id
			}
		}
		for _, t := range final.Tables {
			wb.Put(keys.TableDescKey(t.ID), []byte(t.Descriptor))
			var d catalog.TableDescriptor
			if json.Unmarshal(t.Descriptor, &d) == nil && d.DatabaseID != 0 {
				wb.Put(keys.TableNamespaceKey(d.DatabaseID, t.Name), []byte(fmt.Sprintf("%d", t.ID)))
			} else {
				wb.Put(keys.NamespaceKey(t.Name), []byte(fmt.Sprintf("%d", t.ID)))
			}
			if t.ID > maxID && !catalog.IsSystemTableID(t.ID) {
				maxID = t.ID
			}
		}
		for _, kv := range final.Users {
			wb.Put(keys.Key(kv.Key), kv.Value)
		}
		for _, kv := range final.Admins {
			wb.Put(keys.Key(kv.Key), kv.Value)
		}
		for _, kv := range final.Roles {
			wb.Put(keys.Key(kv.Key), kv.Value)
		}
		return txn.RunBatch(ctx, &wb)
	})
	if err != nil {
		return nil, err
	}
	// The descriptor ID generator must never re-issue a restored ID.
	if maxID > 0 {
		// Transactional, so a concurrent allocation (the catalog migration
		// creating the default databases on a fresh cluster) retries
		// instead of failing on its intent.
		if err := n.db.RunTxn(ctx, "restore-idgen", func(ctx context.Context, txn *kvclient.Txn) error {
			cur, err := txn.Increment(ctx, keys.DescIDGenKey(), 0)
			if err != nil {
				return err
			}
			if uint64(cur) <= maxID {
				_, err = txn.Increment(ctx, keys.DescIDGenKey(), int64(maxID)-cur+1)
			}
			return err
		}); err != nil {
			return nil, err
		}
	}

	// Pre-split each table's span (and shard buckets) so the bulk load
	// parallelizes immediately; best-effort like CREATE TABLE's pre-split.
	for _, t := range final.Tables {
		var d catalog.TableDescriptor
		if err := json.Unmarshal(t.Descriptor, &d); err != nil {
			return nil, err
		}
		splitAt := []keys.Key{keys.TableDataPrefix(d.ID), keys.TableDataPrefix(d.ID).PrefixEnd()}
		for b := int32(1); b < d.ShardBuckets; b++ {
			if k, err := rowenc.AppendKeyDatum(rowenc.PrimaryKeyPrefixFor(&d), types.Int, types.NewInt(int64(b))); err == nil {
				splitAt = append(splitAt, k)
			}
		}
		for _, k := range splitAt {
			if _, err := n.db.AdminSplit(ctx, k); err != nil {
				log.Debugf("restore pre-split at %s: %v", k, err)
			}
		}
	}

	// Data: every chain element in order, only tables that still exist in
	// the final manifest, chunked transactions.
	live := map[uint64]bool{}
	for _, t := range final.Tables {
		live[t.ID] = true
	}
	var applied int64
	for i, man := range mans {
		for _, t := range man.Tables {
			if !live[t.ID] {
				continue // dropped later in the chain
			}
			// The name is derived, not taken from the manifest (which
			// readBackupManifest has already held to the same).
			path := filepath.Join(srcs[i], backupDataFile(t.ID))
			var wb kvclient.WriteBatch
			flush := func() error {
				if wb.Len() == 0 {
					return nil
				}
				batch := wb
				wb = kvclient.WriteBatch{}
				return n.db.RunTxn(ctx, "restore-data", func(ctx context.Context, txn *kvclient.Txn) error {
					b := batch
					return txn.RunBatch(ctx, &b)
				})
			}
			err := readBackupRecords(path, func(rec kvpb.ExportRecord) error {
				if rec.Deleted {
					wb.Delete(keys.Key(rec.Key))
				} else {
					wb.Put(keys.Key(rec.Key), rec.Value)
				}
				applied++
				if wb.Len() >= restoreChunk {
					return flush()
				}
				return nil
			})
			if err == nil {
				err = flush()
			}
			if err != nil {
				return nil, fmt.Errorf("restoring table %s from %s: %w", t.Name, srcs[i], err)
			}
		}
	}

	// Verification: a fresh full export of every restored table, hashed the
	// same way — the caller compares against the source's checksums.
	sum := &cluster.BackupSummary{ClusterID: final.ClusterID, Users: len(final.Users) + len(final.Roles)}
	verifyTS := n.clock.Now()
	for _, t := range final.Tables {
		h := sha256.New()
		var records, bytes int64
		start, end := keys.TableDataSpan(t.ID)
		err := n.exportAll(ctx, start, end, hlc.Timestamp{}, verifyTS, func(rec kvpb.ExportRecord) error {
			if !rec.Deleted {
				records++
				bytes += int64(len(rec.Key) + len(rec.Value))
				hashLiveRecord(h, rec)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		sum.Tables = append(sum.Tables, cluster.BackupTableSummary{
			ID: t.ID, Name: t.Name, Records: records, Bytes: bytes, SHA256: hex.EncodeToString(h.Sum(nil)),
		})
		log.Infof("restore: table %s (id %d): %d live records restored", t.Name, t.ID, records)
	}
	log.Infof("restore: applied %d records from %d backup(s)", applied, len(srcs))
	n.events.RecordEnd("restore", opID, "ok", "restore applied %d records from %d backup(s)", applied, len(srcs))
	return sum, nil
}

// spanOf pairs a (start, end) return into one value.
func spanOf(start, end keys.Key) [2]keys.Key { return [2]keys.Key{start, end} }
