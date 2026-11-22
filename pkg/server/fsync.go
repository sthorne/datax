package server

import (
	"os"
	"runtime"
)

// syncFile and syncDir are the durability points of the files this
// package writes outside the storage engines (backups, the metadata
// backup). They are variables so a test can see that they are reached.
var (
	syncFile = func(f *os.File) error { return f.Sync() }
	syncDir  = syncDirectory
)

// syncDirectory makes the entries of dir (files created or renamed into
// it) durable. Windows cannot open a directory for syncing, and its
// renames are metadata-journaled, so it is skipped there.
func syncDirectory(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// writeFileSynced is os.WriteFile (mode 0600) followed by a sync of the
// file before it is closed, for a file about to be renamed into place.
func writeFileSynced(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := syncFile(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
