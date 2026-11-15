package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Backup and restore (issue #45). A backup is a directory the serving node
// writes: a JSON manifest plus one framed data file per table, all
// captured with consistent Export reads at a single timestamp. Restore
// applies a chain of such directories (one full, then incrementals) into
// an EMPTY cluster, preserving table IDs so index data restores raw.
//
// Two things build on that: an incremental may keep every version of its
// interval, so a chain restores to a moment inside it (backup_history.go,
// issue #302), and a restore may bring back chosen tables into a cluster
// that is in use, under new table IDs (restore_table.go, issue #301).
//
// The manifest deliberately re-snapshots descriptors and users on every
// backup (they are tiny), so any chain element could reconstruct the
// schema; restore uses the LAST element's metadata — tables dropped
// mid-chain simply have their leftover data files skipped.

const (
	backupManifestMagic = "DXBK1"
	// backupHistoryMagic is the manifest magic of a backup with revision
	// history (issue #302), whose data files frame every record with its
	// timestamp. It is a different magic, and not a field beside the old
	// one, because of who reads it: a binary from before the format reads
	// a manifest's magic and nothing it does not know, so under the old
	// magic it would read the timestamps as the next record's length and
	// restore whatever came of it. Under this one it says "not a datax
	// backup" and stops. A sealed backup keeps its envelope ("DXBKS1");
	// the manifest inside carries this magic and is refused the same way.
	backupHistoryMagic = "DXBK2"
	backupManifestName = "BACKUP.json"
	// backupExportChunk bounds one Export call's record count: memory on
	// both sides and the JSON/proto envelope size.
	backupExportChunk = 4096
	// restoreChunk bounds one restore transaction's write batch.
	restoreChunk = 1024
)

// Sealed backups (issue #291). A backup taken with a backup key has the
// same files under the same names, each sealed:
//
//   - BACKUP.json is a small JSON envelope (magic "DXBKS1") holding the
//     backup's data key — 32 random bytes, made for this one backup —
//     wrapped under the backup key with enc.Seal, and the manifest itself
//     as a sealed stream under the data key. Nothing else is in the
//     clear: the cluster ID, the timestamps, the table names, counts and
//     checksums and the role records are all inside.
//   - Each table_<id>.dxbk is a sealed stream (enc.StreamWriter) of the
//     same framed records a plaintext backup holds. The stream's key is
//     derived from the data key and the file's name, and the name, each
//     chunk's position and whether the chunk is the last are bound into
//     every tag: a changed byte, a file under another name, chunks
//     reordered and a file cut short — between chunks too — all fail to
//     open, as does a file from another backup, whose data key differs.
//
// The file names still carry table IDs, as they always did, and not table
// names: restore derives a file's name from the ID rather than trusting
// the manifest for it (issue #214), and that is kept. What a sealed
// directory shows is therefore how many tables there are, their IDs and
// roughly how big each is.
//
// The checksums in the manifest and the summary are over the plaintext
// records, exactly as in an unsealed backup, so the two compare equal.
//
// The seam for a key that is not a file (a KMS, an --enc-root): the
// backup key is used in exactly two places, wrapBackupDataKey and
// unwrapBackupDataKey. A root would wrap the data key with enc.Root.Wrap
// there instead, and the envelope would say which root.
const (
	sealedManifestMagic = "DXBKS1"
	// backupKeyWrapMagic prefixes the wrapped data key (enc.Seal's magic).
	backupKeyWrapMagic = "DXBW"
)

// sealedManifest is BACKUP.json of a sealed backup.
type sealedManifest struct {
	Magic string `json:"magic"`
	// WrappedKey is the backup's data key under the backup key.
	WrappedKey []byte `json:"wrapped_key"`
	// Manifest is the backupManifest JSON as a sealed stream under the
	// data key, bound to the manifest's file name.
	Manifest []byte `json:"manifest"`
}

func wrapBackupDataKey(backupKey, dataKey []byte) ([]byte, error) {
	return enc.Seal(backupKeyWrapMagic, backupKey, dataKey)
}

func unwrapBackupDataKey(backupKey, wrapped []byte) ([]byte, error) {
	dataKey, err := enc.Unseal(backupKeyWrapMagic, backupKey, wrapped)
	if err != nil {
		return nil, err
	}
	if len(dataKey) != enc.KeyLen {
		return nil, fmt.Errorf("wrapped data key is %d bytes, want %d", len(dataKey), enc.KeyLen)
	}
	return dataKey, nil
}

// sealBackupManifest builds a sealed backup's BACKUP.json from the
// manifest JSON.
func sealBackupManifest(backupKey, dataKey, manifest []byte) ([]byte, error) {
	wrapped, err := wrapBackupDataKey(backupKey, dataKey)
	if err != nil {
		return nil, err
	}
	var sealed bytes.Buffer
	w, err := enc.NewStreamWriter(&sealed, dataKey, backupManifestName)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(manifest); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return json.Marshal(sealedManifest{Magic: sealedManifestMagic, WrappedKey: wrapped, Manifest: sealed.Bytes()})
}

// BackupOptions is what a backup is asked for.
type BackupOptions struct {
	// Dest is the directory to write, BasePath a prior backup to take an
	// incremental against; both on this node's filesystem.
	Dest, BasePath string
	// Key seals the backup when set (enc.KeyLen bytes). BaseKey opens a
	// sealed base; when it is empty a sealed base is opened with Key.
	Key, BaseKey []byte
	// AllowPlaintext lets an encrypted store write an unsealed backup.
	AllowPlaintext bool
	// IncludeMetrics backs up the cluster's own metrics tables too.
	IncludeMetrics bool
	// RevisionHistory makes an incremental keep every version written
	// since its base, with timestamps, so the chain restores to any
	// moment of the interval and not only to its end (issue #302).
	RevisionHistory bool
}

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
	// Versions is how many versions the data file holds, in a backup with
	// revision history. Records, Bytes and SHA256 there describe the
	// newest version of each key — what the same backup without history
	// would have held — so the two kinds compare.
	Versions int64 `json:"versions,omitempty"`
	// history says the data file frames each record with its timestamp;
	// set from the manifest when it is read, never stored.
	history bool
	// window is the manifest's (BaseTS, EndTS], which every version of a
	// history file must lie in; set with history.
	window [2]hlc.Timestamp
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
	// Types are the user-defined (enum) type descriptors and their names
	// (v10). A column of an enum type carries a copy of its labels, but
	// the type itself is a catalog object of its own: without it a
	// restored cluster would have the columns and no CREATE TYPE behind
	// them (issue #313). A manifest from before the field has none.
	Types []backupKV `json:"types,omitempty"`
	// RevisionHistory says the data files hold every version written in
	// (BaseTS, EndTS], newest first within a key, each with its timestamp.
	// True exactly when Magic is backupHistoryMagic.
	RevisionHistory bool `json:"revision_history,omitempty"`
}

// exportAll streams every export record in [start, end) for the window
// (startTS, endTS] through fn, chunking and pushing intents as needed. A
// live conflicting transaction is waited out with backoff — a backup
// prefers lateness to inconsistency.
func (n *Node) exportAll(ctx context.Context, start, end keys.Key, startTS, endTS hlc.Timestamp, fn func(kvpb.ExportRecord) error) error {
	return n.exportSpan(ctx, start, end, startTS, endTS, false, fn)
}

// exportSpan is exportAll, or with revisions set the same walk over every
// version in the window: newest first within a key, each record carrying
// its timestamp.
func (n *Node) exportSpan(ctx context.Context, start, end keys.Key, startTS, endTS hlc.Timestamp, revisions bool, fn func(kvpb.ExportRecord) error) error {
	export := n.db.ExportSpan
	if revisions {
		export = n.db.ExportSpanRevisions
	}
	cur := start.Clone()
	for {
		resp, err := export(ctx, cur, end, startTS, endTS, backupExportChunk)
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
	f, err := openBackupFile(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// openBackupFile opens a file of a backup directory under readBackupFile's
// rules, for a caller that reads it as a stream.
func openBackupFile(path string) (*os.File, error) {
	fi, err := backupFileIsPlain(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	opened, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !os.SameFile(fi, opened) || !opened.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("%s changed between being looked at and being opened: refusing to read it", path)
	}
	return f, nil
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

// checkBackupRecords holds every record in a backup's data files to the
// data span of the table the file is for, before restore has applied
// anything. Restore writes a record's key back verbatim, so a record
// appended to table_N.dxbk could otherwise have it write — or, as a
// tombstone, delete — any key in the cluster, the authentication secret
// first among them, with the manifest left byte-for-byte as backup
// wrote it (issue #255, #238's other half). The same pass holds each
// file to its manifest's record count and checksum (issue #313): not a
// seal — whoever can rewrite a file can rewrite the manifest — but a
// damaged or swapped file is refused before anything is written. A table's file legitimately carries only keys in
// keys.TableDataSpan(t.ID), the span backup exported it from. Checked
// here, alongside the file names, so a crafted file is refused before
// the first chunk is committed rather than after the chunks before it.
// This pass and the apply loop both read the file through
// readCheckedBackupRecords, so a file changed between the two reads is
// refused too; the price is that restore reads every data file twice,
// chosen over applying a chunk and then finding the file bad.
//
// For a sealed backup (dataKey set) this same pass is what authenticates
// every chunk of every data file before the first record is applied.
func checkBackupRecords(dir string, dataKey []byte, man *backupManifest) error {
	for _, t := range man.Tables {
		path := filepath.Join(dir, backupDataFile(t.ID))
		if err := readCheckedBackupRecords(path, dataKey, t, func(kvpb.ExportRecord) error { return nil }); err != nil {
			return err
		}
	}
	return nil
}

// readCheckedBackupRecords reads path's records, refusing any key outside
// t's data span before fn sees it (issue #255), a file out of key order,
// and — at its end — a file that is not the one its manifest describes:
// another record count or checksum (issue #313; before it a restore of
// everything reported the checksums for the operator to compare and
// enforced nothing, so a data file swapped for another backup's file of
// the same table restored silently). Both of restore's reads
// of a data file — the pass before anything is applied and the apply
// loop — go through here, so neither can be written without the check:
// the second read is deliberate, for a file rewritten between the two,
// and this is what keeps it from being tidied away as already done. The
// reader beneath is named for what it lacks, so a call site that reaches
// past this one reads as what it is.
func readCheckedBackupRecords(path string, dataKey []byte, t backupTable, fn func(kvpb.ExportRecord) error) error {
	return readTableRecords(path, dataKey, t, hlc.Timestamp{}, true, fn)
}

// checkBackupRecord refuses a record of table t's data file whose key
// lies outside t's data span, tombstones included.
func checkBackupRecord(path string, t backupTable, rec kvpb.ExportRecord) error {
	start, end := keys.TableDataSpan(t.ID)
	if k := keys.Key(rec.Key); k.Compare(start) < 0 || k.Compare(end) >= 0 {
		return fmt.Errorf("%s: table %s's data file carries the key %s, which lies outside the table's own span", path, t.Name, k)
	}
	return nil
}

// readBackupRecordsUnchecked streams a data file's records through fn,
// holding one record (and, for a sealed file, one chunk) in memory at a
// time. dataKey is the backup's data key for a sealed backup and nil for a
// plaintext one; a sealed file's records reach fn only from chunks that
// authenticated. The file is read in the framing of a backup without
// revision history.
func readBackupRecordsUnchecked(path string, dataKey []byte, fn func(kvpb.ExportRecord) error) error {
	rd, err := openBackupRecords(path, dataKey, false)
	if err != nil {
		return err
	}
	defer rd.Close()
	for {
		rec, ok, err := rd.next()
		if err != nil || !ok {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

// hashLiveRecord folds a live record into a table checksum.
func hashLiveRecord(h hash.Hash, rec kvpb.ExportRecord) {
	_, _ = h.Write(encoding.EncodeUvarint(nil, uint64(len(rec.Key))))
	_, _ = h.Write(rec.Key)
	_, _ = h.Write(encoding.EncodeUvarint(nil, uint64(len(rec.Value))))
	_, _ = h.Write(rec.Value)
}

// RunBackup executes an unsealed backup to dest on this node's filesystem.
func (n *Node) RunBackup(ctx context.Context, dest, basePath string, allowPlaintext, includeMetrics bool) (*cluster.BackupSummary, error) {
	return n.RunBackupWith(ctx, BackupOptions{Dest: dest, BasePath: basePath, AllowPlaintext: allowPlaintext, IncludeMetrics: includeMetrics})
}

// RunBackupWith executes a backup to opts.Dest on this node's filesystem,
// sealed under opts.Key when one is given.
func (n *Node) RunBackupWith(ctx context.Context, opts BackupOptions) (_ *cluster.BackupSummary, err error) {
	dest, basePath, includeMetrics := opts.Dest, opts.BasePath, opts.IncludeMetrics
	for _, k := range [][]byte{opts.Key, opts.BaseKey} {
		if k != nil && len(k) != enc.KeyLen {
			return nil, fmt.Errorf("a backup key must be %d bytes, got %d", enc.KeyLen, len(k))
		}
	}
	// Two answers to one question. Taking the key and ignoring the other
	// flag would be harmless, but an operator who passed both believes
	// something about the result that one of them contradicts.
	if opts.Key != nil && opts.AllowPlaintext {
		return nil, fmt.Errorf("--backup-key seals the backup and --allow-plaintext asks for an unsealed one; pass one of them")
	}
	if opts.RevisionHistory {
		// A full backup is the floor of what a chain restores to: the
		// history below it is whatever garbage collection has left, which
		// no timestamp can be promised from. History lives in the
		// incrementals, each holding exactly its own interval.
		if basePath == "" {
			return nil, fmt.Errorf("--revision-history keeps the versions written since a base backup; a full backup has no base (take the full backup without it, and the incrementals after it with it)")
		}
		if !n.db.RevisionExportsOK() {
			return nil, fmt.Errorf("a backup with revision history needs cluster version %s (finalize the upgrade first)", version.V21)
		}
	}
	if (n.cfg.EncKeyPath != "" || n.cfg.EncRoot != "") && opts.Key == nil && !opts.AllowPlaintext {
		return nil, fmt.Errorf("the store is encrypted and a backup without a key is written in plaintext; pass --backup-key to seal it, or --allow-plaintext to proceed unsealed")
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
		// The base gives only its cluster ID and end timestamp. A key
		// named for it (BaseKey) must open it; with none named, a sealed
		// base is tried with this backup's key and a plaintext one is
		// read as it is, so a sealed incremental can follow a full
		// backup taken before sealing was in use.
		baseKey := opts.BaseKey
		if baseKey == nil {
			sealed, serr := backupIsSealed(basePath)
			if serr != nil {
				return nil, fmt.Errorf("reading base backup: %w", serr)
			}
			if sealed && opts.Key == nil {
				return nil, fmt.Errorf("the base backup %s is sealed; pass its key with --base-key", basePath)
			}
			if sealed {
				baseKey = opts.Key
			}
		}
		base, _, err := openBackupManifest(basePath, baseKey)
		if err != nil {
			return nil, fmt.Errorf("reading base backup: %w", err)
		}
		if base.ClusterID != n.ident.ClusterID.String() {
			return nil, fmt.Errorf("base backup is from cluster %s, this is %s", base.ClusterID, n.ident.ClusterID)
		}
		baseTS = base.EndTS
	}
	// The directory and its files are the node's user's alone: a
	// plaintext backup holds every row, and a sealed one still shows the
	// tables' ids and sizes (issue #313). A directory that already
	// exists keeps its mode.
	if err := os.MkdirAll(dest, 0o700); err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dest, backupManifestName)); err == nil {
		return nil, fmt.Errorf("%s already holds a backup", dest)
	}
	// The data files are written into a directory of their own inside
	// dest and moved beside the manifest only once every table has been
	// exported, so a backup that fails part way — and is cleaned up here
	// — leaves nothing in dest, and one whose node dies leaves only that
	// directory, whose name says what it is: never data files that could
	// be taken for part of a backup (issue #313). The manifest is still
	// written last, and is what marks a backup complete.
	staging, err := os.MkdirTemp(dest, ".backup-in-progress-")
	if err != nil {
		return nil, err
	}
	var moved []string
	defer func() {
		if err != nil {
			for _, name := range moved {
				_ = os.Remove(filepath.Join(dest, name))
			}
		}
		_ = os.RemoveAll(staging)
	}()

	// A sealed backup gets a data key of its own: the backup key wraps
	// only that, so it seals 32 bytes per backup however large the data.
	var dataKey []byte
	if opts.Key != nil {
		dataKey = make([]byte, enc.KeyLen)
		if _, err := rand.Read(dataKey); err != nil {
			return nil, err
		}
	}

	endTS := n.clock.Now()
	man := backupManifest{
		Magic:     backupManifestMagic,
		ClusterID: n.ident.ClusterID.String(),
		EndTS:     endTS,
		BaseTS:    baseTS,
		CreatedAt: time.Now().UTC(),
	}
	if opts.RevisionHistory {
		man.Magic, man.RevisionHistory = backupHistoryMagic, true
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
		if (d.Name == catalog.MetricsTableName || d.Name == catalog.ShapesMetricsTableName || d.Name == catalog.StatementShapesTableName) && !includeMetrics {
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
	// The raw groups — users, admins, roles, databases (descriptors and
	// names), sequences (descriptors, names, counters), types
	// (descriptors and names) — each from the
	// spans manifestKeyGroups lists for it, which are the spans restore
	// holds the group to (issue #256): one list for both sides.
	for _, g := range manifestKeyGroups {
		dst := g.kvs(&man)
		for _, span := range g.spans() {
			kvs, err := collectRaw(span[0], span[1])
			if err != nil {
				return nil, err
			}
			*dst = append(*dst, kvs...)
		}
	}

	// Table data: the (baseTS, endTS] window over each table's full data
	// span — every index, raw, so restore needs no backfill.
	for _, bt := range descs {
		bt.File = backupDataFile(bt.ID)
		f, err := os.OpenFile(filepath.Join(staging, bt.File), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, err
		}
		// Records go to the file as they are exported; sealing buffers
		// one chunk of them and no more. The checksum is taken here, over
		// the records and not the file, so it is the same sealed or not.
		var w io.Writer = f
		var sw *enc.StreamWriter
		if dataKey != nil {
			if sw, err = enc.NewStreamWriter(f, dataKey, bt.File); err != nil {
				_ = f.Close()
				return nil, err
			}
			w = sw
		}
		h := sha256.New()
		start, end := keys.TableDataSpan(bt.ID)
		var lastKey keys.Key
		err = n.exportSpan(ctx, start, end, baseTS, endTS, opts.RevisionHistory, func(rec kvpb.ExportRecord) error {
			if opts.RevisionHistory {
				if err := writeBackupRevision(w, rec); err != nil {
					return err
				}
				bt.Versions++
				// Counted and hashed once per key, at its newest version,
				// which is the first the export hands over.
				if lastKey != nil && lastKey.Equal(rec.Key) {
					return nil
				}
				lastKey = keys.Key(rec.Key).Clone()
			} else if err := writeBackupRecord(w, rec); err != nil {
				return err
			}
			bt.Records++
			if !rec.Deleted {
				bt.Bytes += int64(len(rec.Key) + len(rec.Value))
				hashLiveRecord(h, rec)
			}
			return nil
		})
		if sw != nil && err == nil {
			// Only a completed export gets its final chunk: a file left
			// by a failed one must not open as a whole table.
			err = sw.Close()
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			if !baseTS.IsEmpty() && strings.Contains(err.Error(), "below the GC threshold") {
				// The replica's refusal, in the operator's terms: part of
				// (base, now] has been collected, so neither the deletions
				// nor the versions in it can be told from this cluster any
				// more. Nothing short of a new full backup restarts the chain.
				return nil, fmt.Errorf("exporting table %s: the base backup is older than the garbage-collection window, so changes since it have been collected in part and an incremental cannot hold them; take a full backup, and take incrementals more often than the GC TTL: %w", bt.Name, err)
			}
			return nil, fmt.Errorf("exporting table %s: %w", bt.Name, err)
		}
		bt.SHA256 = hex.EncodeToString(h.Sum(nil))
		man.Tables = append(man.Tables, bt)
		log.Infof("backup: table %s (id %d): %d records, %d live bytes", bt.Name, bt.ID, bt.Records, bt.Bytes)
	}

	// Manifest last, atomically: its presence marks the backup complete.
	raw, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return nil, err
	}
	if dataKey != nil {
		if raw, err = sealBackupManifest(opts.Key, dataKey, raw); err != nil {
			return nil, err
		}
	}
	tmp := filepath.Join(staging, backupManifestName)
	if err = os.WriteFile(tmp, raw, 0o600); err != nil {
		return nil, err
	}
	for _, bt := range man.Tables {
		if err = os.Rename(filepath.Join(staging, bt.File), filepath.Join(dest, bt.File)); err != nil {
			return nil, fmt.Errorf("moving %s into %s: %w", bt.File, dest, err)
		}
		moved = append(moved, bt.File)
	}
	if err = os.Rename(tmp, filepath.Join(dest, backupManifestName)); err != nil {
		return nil, err
	}
	n.events.RecordEnd("backup", opID, "ok", "backup written to %s: %d tables", dest, len(descs))
	sum := backupSummary(dest, &man)
	sum.Sealed = dataKey != nil
	return sum, nil
}

// readBackupManifest reads the manifest of a plaintext backup.
func readBackupManifest(dir string) (*backupManifest, error) {
	man, _, err := openBackupManifest(dir, nil)
	return man, err
}

// backupIsSealed reports whether dir's manifest is a sealed one.
func backupIsSealed(dir string) (bool, error) {
	raw, err := readBackupFile(filepath.Join(dir, backupManifestName))
	if err != nil {
		return false, err
	}
	var probe struct {
		Magic string `json:"magic"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false, err
	}
	return probe.Magic == sealedManifestMagic, nil
}

// openBackupManifest reads dir's manifest: with backupKey nil a plaintext
// one, with a key a sealed one, returning the backup's data key too. The
// two do not stand in for each other. A sealed backup without its key
// cannot be read at all; and a plaintext backup where a key was given is
// refused rather than read, because the key is the caller saying which
// backup they mean — the one they sealed — and a plaintext directory in
// its place is not that backup, whatever it holds. Anyone who can write
// the directory can write a plaintext backup; only a holder of the key
// can write a sealed one.
func openBackupManifest(dir string, backupKey []byte) (*backupManifest, []byte, error) {
	raw, err := readBackupFile(filepath.Join(dir, backupManifestName))
	if err != nil {
		return nil, nil, err
	}
	var probe struct {
		Magic string `json:"magic"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, nil, err
	}
	var dataKey []byte
	switch {
	case probe.Magic == sealedManifestMagic && backupKey == nil:
		return nil, nil, fmt.Errorf("%s is a sealed backup and no key was given for it; pass its key with --backup-key", dir)
	case probe.Magic == sealedManifestMagic:
		var env sealedManifest
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, nil, err
		}
		if dataKey, err = unwrapBackupDataKey(backupKey, env.WrappedKey); err != nil {
			return nil, nil, fmt.Errorf("%s: the backup key given does not open this backup (a different key, or a damaged manifest)", dir)
		}
		r, err := enc.NewStreamReader(bytes.NewReader(env.Manifest), dataKey, backupManifestName)
		if err == nil {
			raw, err = io.ReadAll(r)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("%s: the sealed manifest does not open: %w", dir, err)
		}
	case backupKey != nil:
		return nil, nil, fmt.Errorf("%s is not a sealed backup, and a backup key was given for it: refusing to read a plaintext backup in a sealed one's place", dir)
	}
	man, err := parseBackupManifest(dir, raw)
	if err != nil {
		return nil, nil, err
	}
	return man, dataKey, nil
}

// parseBackupManifest decodes and checks a manifest's plaintext JSON. A
// sealed manifest is held to every check a plaintext one is: the key says
// who wrote it, not that what they wrote is a backup.
func parseBackupManifest(dir string, raw []byte) (*backupManifest, error) {
	var man backupManifest
	if err := json.Unmarshal(raw, &man); err != nil {
		return nil, err
	}
	switch {
	case man.Magic == backupManifestMagic && !man.RevisionHistory:
	case man.Magic == backupHistoryMagic && man.RevisionHistory:
		if man.BaseTS.IsEmpty() || !man.BaseTS.Less(man.EndTS) {
			return nil, fmt.Errorf("%s: a backup with revision history holds the interval after a base backup, and this manifest names none (base %s, end %s)", dir, man.BaseTS, man.EndTS)
		}
	case man.Magic == backupManifestMagic || man.Magic == backupHistoryMagic:
		// The magic is what an older binary goes by and the field what
		// this one reads the files by: a manifest where they disagree
		// would be read one way here and another there.
		return nil, fmt.Errorf("%s: the manifest's magic %q and its revision-history marker disagree", dir, man.Magic)
	default:
		return nil, fmt.Errorf("%s: not a datax backup (magic %q)", dir, man.Magic)
	}
	for i := range man.Tables {
		man.Tables[i].history = man.RevisionHistory
		man.Tables[i].window = [2]hlc.Timestamp{man.BaseTS, man.EndTS}
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

// manifestKeyGroup is one of the manifest's raw-record groups: its JSON
// field name, the manifest field that holds it, and the key spans a
// backup collects it from — the only spans restore accepts it in (issue
// #238).
type manifestKeyGroup struct {
	name string
	// kvs returns the manifest's field for this group. A pointer, so
	// RunBackup fills the field through it and checkManifestKeys reads
	// the same field back.
	kvs   func(*backupManifest) *[]backupKV
	spans func() [][2]keys.Key
}

// manifestKeyGroups is the one list of the manifest's raw groups and
// their spans. RunBackup collects each group from its spans through it,
// and checkManifestKeys holds a manifest's groups to the same spans, so
// what backup collects and what restore accepts cannot drift apart:
// there is no second list to keep in step (issue #256; before it,
// RunBackup named the spans again by hand, and a span added to this
// table alone widened what restore accepted with no test failing). It
// must name every []backupKV field of backupManifest and nothing else:
// a raw group collected but not named here would be restored
// unchecked, which is the hole #238 closed, and
// TestManifestKeyGroupsCoverTheManifest pins the two to each other
// (issue #242). name is the field's JSON tag; kvs must return that very
// field. The spans are listed in the order backup writes them.
var manifestKeyGroups = []manifestKeyGroup{
	{"users", func(m *backupManifest) *[]backupKV { return &m.Users },
		func() [][2]keys.Key { return [][2]keys.Key{spanOf(keys.UserSpan())} }},
	{"admins", func(m *backupManifest) *[]backupKV { return &m.Admins },
		func() [][2]keys.Key { return [][2]keys.Key{spanOf(keys.AdminUserSpan())} }},
	{"roles", func(m *backupManifest) *[]backupKV { return &m.Roles },
		func() [][2]keys.Key { return [][2]keys.Key{spanOf(keys.RoleSpan())} }},
	{"databases", func(m *backupManifest) *[]backupKV { return &m.Databases },
		func() [][2]keys.Key {
			return [][2]keys.Key{spanOf(keys.DatabaseDescSpan()), spanOf(keys.DatabaseNamespaceSpan())}
		}},
	{"sequences", func(m *backupManifest) *[]backupKV { return &m.Sequences },
		func() [][2]keys.Key {
			return [][2]keys.Key{spanOf(keys.SequenceDescSpan()), spanOf(keys.AllSequenceNamespaceSpan()), spanOf(keys.SequenceValueSpan())}
		}},
	{"types", func(m *backupManifest) *[]backupKV { return &m.Types },
		func() [][2]keys.Key {
			return [][2]keys.Key{spanOf(keys.TypeDescSpan()), spanOf(keys.AllTypeNamespaceSpan())}
		}},
}

// checkManifestKeys holds a manifest's raw key/value groups — users,
// admins, roles, databases, sequences, types — to the key spans backup collects
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
	for _, g := range manifestKeyGroups {
		spans := g.spans()
		for _, kv := range *g.kvs(man) {
			if !inAnySpan(keys.Key(kv.Key), spans) {
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

		EndTS:           formatBackupTS(man.EndTS),
		RevisionHistory: man.RevisionHistory,
	}
	if !man.BaseTS.IsEmpty() {
		sum.BaseTS = formatBackupTS(man.BaseTS)
	}
	for _, t := range man.Tables {
		sum.Tables = append(sum.Tables, cluster.BackupTableSummary{
			ID: t.ID, Name: t.Name, Records: t.Records, Bytes: t.Bytes, SHA256: t.SHA256, Versions: t.Versions,
		})
	}
	return sum
}

// RunRestore applies a chain of plaintext backups; see RunRestoreWith.
func (n *Node) RunRestore(ctx context.Context, srcs []string) (*cluster.BackupSummary, error) {
	return n.RunRestoreWith(ctx, srcs, nil)
}

// restoreKeys pairs each directory of a chain with its backup key: no
// keys for a chain of plaintext backups, one key for a chain sealed under
// it throughout, or one per directory in the chain's order, where an
// empty entry says that directory is a plaintext backup. The pairing is
// by position and nothing is tried against anything: which key opens
// which backup is the operator's statement, and a directory that is not
// what they said — sealed where they said plaintext, or the reverse — is
// refused.
func restoreKeys(srcs []string, backupKeys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(srcs))
	switch len(backupKeys) {
	case 0:
		return out, nil
	case 1:
		for i := range out {
			out[i] = backupKeys[0]
		}
	case len(srcs):
		copy(out, backupKeys)
	default:
		return nil, fmt.Errorf("%d backup keys for %d backup directories: give one key for the whole chain, or one per directory in the same order (an empty entry for a plaintext backup)", len(backupKeys), len(srcs))
	}
	for i, k := range out {
		if len(k) == 0 {
			out[i] = nil
		} else if len(k) != enc.KeyLen {
			return nil, fmt.Errorf("a backup key must be %d bytes, got %d", enc.KeyLen, len(k))
		}
	}
	return out, nil
}

// RestoreOptions is what a restore is asked for.
type RestoreOptions struct {
	// Srcs is the chain, full backup first, as directories on this node's
	// filesystem; Keys opens its sealed backups (restoreKeys).
	Srcs []string
	Keys [][]byte
	// AsOf is the moment to restore to; zero is the end of the chain.
	AsOf hlc.Timestamp
	// Tables, or Database, makes this a restore of those tables into a
	// cluster that is in use (restore_table.go); with neither it is a
	// restore of everything into an empty one.
	Tables       []RestoreTable
	Database     string
	IntoDatabase string
	// DropForeignKeys restores a table without its foreign keys to tables
	// outside the restore, where the default is to refuse it.
	DropForeignKeys bool
	// TestingAbandonAfterLoad makes a table restore return once its rows
	// are loaded, without publishing or cleaning up — where a node that
	// died would have left it. Tests only.
	TestingAbandonAfterLoad bool
}

// RestoreTable names one table of a table restore: Name as it is in the
// backup ("db.table", or "table" in the default database) and Into the
// name to restore it under (empty keeps Name).
type RestoreTable struct {
	Name, Into string
}

// restoreChain is a chain opened for a restore: the backups it will read,
// already cut to the moment asked for.
type restoreChain struct {
	srcs     []string
	mans     []*backupManifest
	dataKeys [][]byte
	// cutoff, when set, is a moment inside the last backup's interval:
	// that backup's history is read to it and no further.
	cutoff hlc.Timestamp
}

func (c *restoreChain) final() *backupManifest { return c.mans[len(c.mans)-1] }

// asOf is the moment the restored data is as of.
func (c *restoreChain) asOf() hlc.Timestamp {
	if !c.cutoff.IsEmpty() {
		return c.cutoff
	}
	return c.final().EndTS
}

// cutoffFor is the cutoff to read backup i's data files with.
func (c *restoreChain) cutoffFor(i int) hlc.Timestamp {
	if i == len(c.mans)-1 {
		return c.cutoff
	}
	return hlc.Timestamp{}
}

// openRestoreChain opens every manifest of a chain, holds the chain to
// its shape — a full backup, then incrementals each on the one before —
// and cuts it to asOf. No data file is read here.
func openRestoreChain(srcs []string, backupKeys [][]byte, asOf hlc.Timestamp) (*restoreChain, error) {
	if len(srcs) == 0 {
		return nil, fmt.Errorf("restore requires at least one backup directory")
	}
	srcKeys, err := restoreKeys(srcs, backupKeys)
	if err != nil {
		return nil, err
	}
	c := &restoreChain{srcs: srcs, mans: make([]*backupManifest, len(srcs)), dataKeys: make([][]byte, len(srcs))}
	for i, src := range srcs {
		if c.mans[i], c.dataKeys[i], err = openBackupManifest(src, srcKeys[i]); err != nil {
			return nil, err
		}
	}
	mans := c.mans
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
	n, cutoff, err := chainCut(srcs, mans, asOf)
	if err != nil {
		return nil, err
	}
	c.srcs, c.mans, c.dataKeys, c.cutoff = c.srcs[:n], c.mans[:n], c.dataKeys[:n], cutoff
	return c, nil
}

// RunRestoreWith applies a backup chain (full first, then incrementals in
// order) into this — empty — cluster, then re-exports each table and
// reports fresh checksums for verification against the source.
// backupKeys opens the chain's sealed backups (restoreKeys). Every
// manifest is opened and every data file read to its end — which for a
// sealed one authenticates all of it — before anything is applied.
func (n *Node) RunRestoreWith(ctx context.Context, srcs []string, backupKeys [][]byte) (*cluster.BackupSummary, error) {
	return n.RunRestoreOpts(ctx, RestoreOptions{Srcs: srcs, Keys: backupKeys})
}

// RunRestoreOpts runs a restore: of the whole chain into this — empty —
// cluster, or, when opts names tables or a database, of those into this
// cluster as it runs. Either reads the chain to opts.AsOf.
func (n *Node) RunRestoreOpts(ctx context.Context, opts RestoreOptions) (_ *cluster.BackupSummary, err error) {
	srcs := opts.Srcs
	opID := newOpID()
	what := fmt.Sprintf("restore from %d backup(s)", len(srcs))
	if len(opts.Tables) > 0 || opts.Database != "" {
		what = fmt.Sprintf("table restore from %d backup(s)", len(srcs))
	}
	n.events.RecordStart("restore", opID, "%s started", what)
	defer func() {
		if err != nil {
			n.events.RecordEnd("restore", opID, "failed", "%s failed: %v", what, err)
		}
	}()
	chain, err := openRestoreChain(srcs, opts.Keys, opts.AsOf)
	if err != nil {
		return nil, err
	}
	if len(opts.Tables) > 0 || opts.Database != "" {
		sum, err := n.runTableRestore(ctx, opID, opts, chain)
		if err != nil {
			return nil, err
		}
		n.events.RecordEnd("restore", opID, "ok", "restored %d table(s) from %d backup(s), as of %s", len(sum.Tables), len(chain.srcs), sum.EndTS)
		return sum, nil
	}
	if opts.DropForeignKeys || opts.IntoDatabase != "" {
		return nil, fmt.Errorf("--drop-foreign-keys and --into belong to a restore of tables (--table, --database); a restore of everything brings every table back as it was")
	}
	srcs = chain.srcs
	mans, dataKeys := chain.mans, chain.dataKeys
	for i, src := range srcs {
		if err := checkBackupFiles(src, mans[i]); err != nil {
			return nil, err
		}
		if err := checkBackupRecords(src, dataKeys[i], mans[i]); err != nil {
			return nil, err
		}
	}
	final := chain.final()
	// Named keys come back only where their cluster key can open them
	// (issue #310): refused before anything is applied.
	if err := n.checkRestoreClusterKeys(ctx, chain); err != nil {
		return nil, err
	}
	if !chain.cutoff.IsEmpty() {
		// The cut falls inside the last backup's interval, whose manifest
		// describes the schema at the interval's end.
		for _, t := range final.Tables {
			if err := tableStableAcross(mans[len(mans)-2], final, t, chain.cutoff); err != nil {
				return nil, err
			}
		}
	}

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
	// A table this cluster dropped may still have its rows under an id a
	// restored table carries (issue #313).
	for _, t := range final.Tables {
		if catalog.IsSystemTableID(t.ID) {
			continue
		}
		if err := n.clearTableIDForRestore(ctx, t.ID); err != nil {
			return nil, err
		}
	}
	n.metricsPaused.Store(true)
	defer n.metricsPaused.Store(false)
	restoresAudit := false
	for _, t := range final.Tables {
		restoresAudit = restoresAudit || t.ID == catalog.AuditTableID
	}
	defer n.pauseAuditForRestore(ctx, restoresAudit)()

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
		// Types keep their ids, which the restored columns name.
		for _, kv := range final.Types {
			wb.Put(keys.Key(kv.Key), kv.Value)
			if id, ok := catalog.TypeIDOfKey(keys.Key(kv.Key)); ok && id > maxID {
				maxID = id
			}
		}
		for _, t := range final.Tables {
			wb.Put(keys.TableDescKey(t.ID), []byte(t.Descriptor))
			var d catalog.TableDescriptor
			if json.Unmarshal(t.Descriptor, &d) == nil && d.IsView() {
				// The reverse records of a view's dependencies are not in
				// a backup; they are written with the view (issue #313).
				for _, dep := range d.ViewDepends {
					wb.Put(keys.ViewDependencyKey(dep, d.ID), []byte{1})
				}
			}
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
			// Held to the table's span again at the write (issue
			// #255): the pass above read the file once already, and
			// a file rewritten since is refused rather than applied.
			// readTableRecords is readCheckedBackupRecords with the cut
			// a point-in-time restore reads the last backup to. A
			// file changed since the first pass fails its manifest
			// check at its end, by which point its earlier chunks are
			// applied: the restore then stops with the target part
			// written, as a failed restore of everything always has.
			err := readTableRecords(path, dataKeys[i], t, chain.cutoffFor(i), true, func(rec kvpb.ExportRecord) error {
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

	// Restored named keys are sealed under this cluster's key (issue #310).
	keyNotes, err := n.rewrapRestoredKeys(ctx, chain)
	if err != nil {
		return nil, err
	}

	// Verification: a fresh full export of every restored table, hashed the
	// same way — the caller compares against the source's checksums.
	sum := &cluster.BackupSummary{ClusterID: final.ClusterID, Users: len(final.Users) + len(final.Roles), EndTS: formatBackupTS(chain.asOf()), Notes: keyNotes}
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
