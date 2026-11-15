package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// runBackupCmd is `datax backup`: ask a node to write a consistent backup
// of the whole cluster to a directory on ITS filesystem.
func runBackupCmd(args []string) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:26257", "RPC address of any cluster node")
	dest := fs.String("dest", "", "destination directory ON THE SERVING NODE")
	basePath := fs.String("base", "", "prior backup directory (on the node) to take an incremental against")
	backupKey := fs.String("backup-key", "", "file holding a 32-byte key (raw or hex) to seal the backup's files with; read here and sent to the node")
	baseKey := fs.String("base-key", "", "file holding the key of a sealed --base, when it differs from --backup-key")
	allowPlaintext := fs.Bool("allow-plaintext", false, "write an unsealed (plaintext) backup from an encrypted store")
	revisionHistory := fs.Bool("revision-history", false, "with --base: keep every version written since the base, so the chain restores to any moment in between (restore --as-of) and not only to this backup's end")
	includeMetrics := fs.Bool("include-metrics", false, "also back up the cluster's own datax_metrics table (bulky and regenerable; excluded by default)")
	certsDir := fs.String("certs-dir", "", "certificate directory for a secure cluster (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "username whose client certificate authenticates this call (with --certs-dir); must be an admin")
	timeout := connectTimeoutFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dest == "" {
		return fmt.Errorf("backup requires --dest")
	}
	if *backupKey != "" && *allowPlaintext {
		return fmt.Errorf("--backup-key seals the backup and --allow-plaintext asks for an unsealed one; pass one of them")
	}
	if *baseKey != "" && *basePath == "" {
		return fmt.Errorf("--base-key names the key of a --base backup, and no --base was given")
	}
	if *revisionHistory && *basePath == "" {
		return fmt.Errorf("--revision-history keeps the versions written since a base backup; a full backup has no base (take the full backup without it, and the incrementals after it with it)")
	}
	req := cluster.AdminRequest{
		Op: "backup", Path: *dest, BasePath: *basePath, AllowPlaintext: *allowPlaintext, IncludeMetrics: *includeMetrics,
	}
	if *revisionHistory {
		// An op of its own: a node from before revision history would
		// take "backup", ignore a flag it does not know, and write an
		// incremental without the history.
		req.Op = "backup-history"
	}
	var err error
	if *backupKey != "" {
		if req.BackupKey, err = enc.LoadKeyFile(*backupKey); err != nil {
			return fmt.Errorf("--backup-key: %w", err)
		}
	}
	if *baseKey != "" {
		if req.BaseBackupKey, err = enc.LoadKeyFile(*baseKey); err != nil {
			return fmt.Errorf("--base-key: %w", err)
		}
	}
	resp, err := adminCall(*addr, *certsDir, *certUser, *timeout, req)
	if err != nil {
		return err
	}
	printBackupSummary("backup", resp.Backup)
	return nil
}

// runRestoreCmd is `datax restore`: ask a node to apply a backup chain
// (full first, then incrementals) from directories on ITS filesystem into
// this — empty — cluster; or, with --table or --database, to bring those
// tables back into the cluster as it runs; either to the chain's end or
// to the moment --as-of names.
func runRestoreCmd(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:26257", "RPC address of any cluster node")
	src := fs.String("src", "", "comma-separated backup directories ON THE SERVING NODE, full backup first")
	backupKey := fs.String("backup-key", "", "key file(s) for sealed backups: one for the whole chain, or a comma-separated list matching --src in order (an empty entry for a plaintext backup)")
	asOf := fs.String("as-of", "", "restore to this moment instead of the chain's end: Unix nanoseconds (as a backup prints its timestamps, \"<nanos>\" or \"<nanos>,<logical>\") or an RFC 3339 time. Any moment of an interval backed up with --revision-history, or exactly the end of any backup in the chain")
	tables := fs.String("table", "", "restore only these tables, into a cluster that is in use: a comma-separated list of db.table (or table, in the default database)")
	database := fs.String("database", "", "restore every table of this database, as a new database, into a cluster that is in use")
	into := fs.String("into", "", "with --table: the names to restore under, matching --table in order (db.table or table; an empty entry keeps the name). With --database: the new database's name")
	dropFKs := fs.Bool("drop-foreign-keys", false, "with --table or --database: restore a table without its foreign keys to tables outside the restore, instead of refusing")
	certsDir := fs.String("certs-dir", "", "certificate directory for a secure cluster (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "username whose client certificate authenticates this call (with --certs-dir); must be an admin")
	timeout := connectTimeoutFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *src == "" {
		return fmt.Errorf("restore requires --src")
	}
	req := cluster.AdminRequest{Op: "restore", Paths: strings.Split(*src, ",")}
	if err := restoreSelection(&req, *asOf, *tables, *database, *into, *dropFKs); err != nil {
		return err
	}
	var err error
	if req.BackupKeys, err = loadRestoreKeys(*backupKey, len(req.Paths)); err != nil {
		return err
	}
	resp, err := adminCall(*addr, *certsDir, *certUser, *timeout, req)
	if err != nil {
		return err
	}
	printBackupSummary("restored (fresh export)", resp.Backup)
	return nil
}

// restoreSelection fills in what restore's --as-of, --table, --database,
// --into and --drop-foreign-keys ask for. Any of them makes the request
// a "restore-select": a node from before them would take "restore",
// ignore the fields it does not know and restore everything to the
// chain's end, and under the other name it refuses instead.
func restoreSelection(req *cluster.AdminRequest, asOf, tables, database, into string, dropFKs bool) error {
	if tables != "" && database != "" {
		return fmt.Errorf("--table and --database are two restores; run them one after the other")
	}
	if tables == "" && database == "" && (into != "" || dropFKs) {
		return fmt.Errorf("--into and --drop-foreign-keys belong to a restore of tables; name them with --table or --database")
	}
	if asOf == "" && tables == "" && database == "" {
		return nil
	}
	req.Op = "restore-select"
	req.AsOf, req.Database, req.DropForeignKeys = asOf, database, dropFKs
	if database != "" {
		req.IntoDatabase = into
		return nil
	}
	if tables == "" {
		return nil
	}
	req.Tables = strings.Split(tables, ",")
	if into != "" {
		req.Into = strings.Split(into, ",")
		if len(req.Into) != len(req.Tables) {
			return fmt.Errorf("--into names %d tables for the %d in --table: give one per table, in the same order (an empty entry keeps the name)", len(req.Into), len(req.Tables))
		}
	}
	return nil
}

// loadRestoreKeys reads restore's --backup-key: one key file for every
// directory of the chain, or one per directory in --src order, where an
// empty entry says that directory is a plaintext backup. Keys are paired
// with directories by position; none is tried against another's backup.
func loadRestoreKeys(spec string, dirs int) ([][]byte, error) {
	if spec == "" {
		return nil, nil
	}
	paths := strings.Split(spec, ",")
	if len(paths) != 1 && len(paths) != dirs {
		return nil, fmt.Errorf("--backup-key names %d keys for %d backup directories: give one key for the whole chain, or one per directory in --src order (an empty entry for a plaintext backup)", len(paths), dirs)
	}
	keys := make([][]byte, len(paths))
	for i, p := range paths {
		if p == "" {
			continue
		}
		k, err := enc.LoadKeyFile(p)
		if err != nil {
			return nil, fmt.Errorf("--backup-key: %w", err)
		}
		keys[i] = k
	}
	return keys, nil
}

func adminCall(addr, certsDir, certUser string, connectTimeout time.Duration, req cluster.AdminRequest) (cluster.AdminResponse, error) {
	trans, err := connectAdmin(addr, certsDir, certUser, connectTimeout)
	if err != nil {
		return cluster.AdminResponse{}, err
	}
	// Backups and restores move real data; give them time.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	var resp cluster.AdminResponse
	if err := trans.Call(ctx, addr, "admin", req, &resp); err != nil {
		return resp, adminCallError(err, certsDir)
	}
	if resp.Error != "" {
		return resp, fmt.Errorf("%s", resp.Error)
	}
	return resp, nil
}

func printBackupSummary(what string, sum *cluster.BackupSummary) {
	if sum == nil {
		fmt.Println("done")
		return
	}
	if sum.Path != "" {
		if sum.Sealed {
			what += " (sealed)"
		}
		fmt.Printf("%s: %s (cluster %s)\n", what, sum.Path, sum.ClusterID)
	} else {
		fmt.Printf("%s (cluster %s)\n", what, sum.ClusterID)
	}
	if sum.EndTS != "" {
		line := "  as of: " + sum.EndTS
		if sum.BaseTS != "" {
			line += "  (changes since " + sum.BaseTS
			if sum.RevisionHistory {
				line += ", with revision history"
			}
			line += ")"
		}
		fmt.Println(line)
	}
	tableRestore := false
	for _, t := range sum.Tables {
		line := fmt.Sprintf("  table %-20s id=%-4d records=%-8d bytes=%-10d sha256=%s",
			t.Name, t.ID, t.Records, t.Bytes, t.SHA256[:16])
		if t.Versions != 0 {
			line += fmt.Sprintf(" versions=%d", t.Versions)
		}
		if t.From != "" {
			tableRestore = true
			line += fmt.Sprintf("  (from %s, id %d)", t.From, t.FromID)
		}
		fmt.Println(line)
	}
	for _, note := range sum.Notes {
		fmt.Println("  note: " + note)
	}
	if !tableRestore {
		// A table restore leaves the cluster's users as they are.
		fmt.Printf("  users: %d\n", sum.Users)
	}
}
