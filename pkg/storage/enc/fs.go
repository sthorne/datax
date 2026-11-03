// Package enc implements datax's encryption at rest: a vfs.FS wrapper that
// transparently encrypts every file Pebble writes (sstables, WAL,
// MANIFEST, ...), plus the AES-GCM-sealed key registry and helpers for
// sealing non-Pebble artifacts (the metadata backup). stdlib crypto only.
// See docs/encryption.md.
//
// Two file formats. "DXE1" (fs.go) is AES-256-CTR: confidentiality
// against a disk that leaves the building, and nothing else — the content
// carries no authentication tag, so an attacker who can write to a live
// store's files can alter, truncate, substitute or roll back a file
// undetectably, and Pebble's keyless block checksums are no defence
// against it. "DXE2" (aead.go) seals the content in chunks with
// AES-256-GCM, so an altered, moved or truncated-mid-chunk byte is refused
// at read; a store switches to it from cluster version v18 — new files
// first, then, once none of the old format remains, the old format is
// refused for good (issue #220). What the authenticated format still does
// not catch is stated at the top of aead.go.
package enc

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// Per-file layout: a 24-byte header, then AES-256-CTR ciphertext whose
// keystream counter is derived from the header IV and the logical offset —
// random access never re-derives more than one block.
//
//	[0:4)   magic "DXE1"
//	[4:8)   data-key ID, uint32 big-endian
//	[8:24)  16-byte random IV (CTR counter base)
const (
	fileMagic = "DXE1"
	headerLen = 24
)

// RegistryName is the key-registry file in the store directory; its
// presence marks the store as encrypted. It is written through the base
// FS (sealed with the store key, not a data key) and passed through raw.
const RegistryName = "ENCRYPTION-REGISTRY"

// FileKeyID reads the data-key ID from an encrypted file's header (either
// format), through the BASE (unencrypted) filesystem. ok=false when the
// file carries no encryption header (the registry, LOCK, short files).
func FileKeyID(base vfs.FS, path string) (uint32, bool, error) {
	f, err := base.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = f.Close() }()
	var hdr [headerLen]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return 0, false, nil
	}
	if m := string(hdr[0:4]); m != fileMagic && m != fileMagicV2 {
		return 0, false, nil
	}
	return binary.BigEndian.Uint32(hdr[4:8]), true, nil
}

// FileAuthenticated reports whether an encrypted file is in the
// authenticated "DXE2" format; ok=false when it carries no encryption
// header at all.
func FileAuthenticated(base vfs.FS, path string) (authenticated bool, ok bool, err error) {
	magic, _, err := fileFormat(base, path)
	if err != nil {
		return false, false, err
	}
	switch magic {
	case fileMagicV2:
		return true, true, nil
	case fileMagic:
		return false, true, nil
	}
	return false, false, nil
}

// FS encrypts every file created through it. Non-file operations pass
// through to the base FS.
//
// Two formats (see aead.go): "DXE1", CTR, confidentiality only; and
// "DXE2", chunked AES-GCM, authenticated. Which one new files get is
// authenticated (the server turns it on from cluster version v18, so no
// node that cannot read "DXE2" is left in the cluster); both are read
// until strict is set, which the server does once the store holds no
// "DXE1" file any more (enc.LegacyFiles) — until then a reader that
// accepted "DXE1" would hand an attacker the downgrade: rewrite a file's
// header as "DXE1" and its content is a keystream again, unauthenticated.
type FS struct {
	vfs.FS
	keys          *KeySet
	authenticated atomic.Bool
	strict        atomic.Bool
}

// NewFS wraps base so all file content is encrypted with keys. A key set
// whose registry records the authenticated cutover opens strict.
func NewFS(base vfs.FS, keys *KeySet) *FS {
	fs := &FS{FS: base, keys: keys}
	if keys != nil && keys.AuthenticatedWrites() {
		fs.authenticated.Store(true)
	}
	if keys != nil && keys.Authenticated() {
		fs.authenticated.Store(true)
		fs.strict.Store(true)
	}
	return fs
}

// EnableAuthenticatedWrites makes every file created from now on "DXE2".
func (fs *FS) EnableAuthenticatedWrites() { fs.authenticated.Store(true) }

// AuthenticatedWrites reports whether new files are "DXE2".
func (fs *FS) AuthenticatedWrites() bool { return fs.authenticated.Load() }

// RefuseLegacy makes the FS refuse to open "DXE1" files (and write
// "DXE2" ones): the cutover's reader half.
func (fs *FS) RefuseLegacy() {
	fs.authenticated.Store(true)
	fs.strict.Store(true)
}

// RefusesLegacy reports whether "DXE1" files are refused.
func (fs *FS) RefusesLegacy() bool { return fs.strict.Load() }

// create picks the format for a new file.
func (fs *FS) create(f vfs.File, name string) (vfs.File, error) {
	if fs.authenticated.Load() {
		return newAEADFileCreate(f, fs.keys, fs.FS.PathBase(name))
	}
	return newEncFileCreate(f, fs.keys)
}

// open dispatches on an existing file's header.
func (fs *FS) open(f vfs.File, name string) (vfs.File, error) {
	var hdr [headerLenV2]byte
	n, err := f.ReadAt(hdr[:], 0)
	if err != nil && (err != io.EOF || n < headerLen) {
		return nil, fmt.Errorf("reading encryption header: %w", err)
	}
	switch string(hdr[0:4]) {
	case fileMagicV2:
		if n < headerLenV2 {
			return nil, fmt.Errorf("truncated encryption header")
		}
		return newAEADFileOpen(f, fs.keys, fs.FS.PathBase(name), hdr)
	case fileMagic:
		if fs.strict.Load() {
			return nil, fmt.Errorf("unauthenticated (DXE1) file in a store that has switched to authenticated encryption; refusing to read it (issue #220)")
		}
		return newEncFileOpen(f, fs.keys)
	}
	return nil, fmt.Errorf("missing encryption header (plaintext file in an encrypted store?)")
}

func (fs *FS) isRegistry(name string) bool {
	return fs.FS.PathBase(name) == RegistryName
}

func (fs *FS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	if fs.isRegistry(name) {
		return f, nil
	}
	ef, err := fs.create(f, name)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return ef, nil
}

func (fs *FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	if fs.isRegistry(name) {
		return f, nil
	}
	ef, err := fs.open(f, name)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	return ef, nil
}

func (fs *FS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.OpenReadWrite(name, category, opts...)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		ef, err := fs.create(f, name)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		return ef, nil
	}
	ef, err := fs.open(f, name)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	return ef, nil
}

// ReuseForWrite never recycles: rewriting a file under its original key+IV
// would reuse the CTR keystream — a real confidentiality break, not
// hygiene. The old file is removed and a fresh one (new IV, current data
// key) created. This forfeits Pebble's WAL recycling (a recycled WAL's
// fdatasync skips metadata journaling) and is the dominant write-latency
// cost of encryption; see docs/encryption.md.
func (fs *FS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if err := fs.FS.Remove(oldname); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return fs.Create(newname, category)
}

// Unwrap returns the FS this one encrypts on top of (Pebble walks the
// chain for the disk-health and category bookkeeping of the base FS).
func (fs *FS) Unwrap() vfs.FS { return fs.FS }

// Stat reports logical sizes: the header and, for a "DXE2" file, the
// chunk framing are subtracted. Files smaller than a header (the
// zero-length LOCK file, directories) are reported as-is. Telling the
// formats apart costs a read of the header.
func (fs *FS) Stat(name string) (vfs.FileInfo, error) {
	st, err := fs.FS.Stat(name)
	if err != nil {
		return nil, err
	}
	if fs.isRegistry(name) || st.IsDir() || st.Size() < headerLen {
		return st, nil
	}
	magic, chunk, err := fileFormat(fs.FS, name)
	if err != nil {
		return nil, err
	}
	switch magic {
	case fileMagicV2:
		size, ok := logicalSize(st.Size(), chunk)
		if !ok {
			return nil, fmt.Errorf("stat %s: corrupt encrypted file: %d bytes is no whole number of chunks", name, st.Size())
		}
		return sizedFileInfo{FileInfo: st, size: size}, nil
	case fileMagic:
		return sizedFileInfo{FileInfo: st, size: st.Size() - headerLen}, nil
	}
	return st, nil
}

// fileFormat reads a file's magic and, for "DXE2", its chunk size.
func fileFormat(base vfs.FS, name string) (string, int, error) {
	f, err := base.Open(name)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = f.Close() }()
	var hdr [headerLenV2]byte
	n, err := f.ReadAt(hdr[:], 0)
	if err != nil && err != io.EOF {
		return "", 0, err
	}
	if n < 4 {
		return "", 0, nil
	}
	magic := string(hdr[0:4])
	if magic == fileMagicV2 {
		if n < headerLenV2 {
			return "", 0, fmt.Errorf("truncated encryption header")
		}
		return magic, int(binary.BigEndian.Uint32(hdr[12:16])), nil
	}
	return magic, 0, nil
}

// ConvertLegacyFiles rewrites the "DXE1" files in dir that convert says
// to, in the authenticated format, each through a temporary file renamed
// over the original with the directory synced after — a crash leaves
// either version, and both are readable until the store turns strict.
// Meant for the small files Pebble keeps across opens (MANIFEST, OPTIONS,
// its markers, a WAL it will replay) before Pebble opens the store;
// sstables are the re-encryption sweep's, which rewrites them online.
// Returns the names converted.
func ConvertLegacyFiles(base vfs.FS, dir string, keys *KeySet, convert func(name string) bool) ([]string, error) {
	legacy, err := LegacyFiles(base, dir)
	if err != nil {
		return nil, err
	}
	var done []string
	for _, name := range legacy {
		if convert != nil && !convert(name) {
			continue
		}
		if err := convertLegacyFile(base, dir, keys, name); err != nil {
			return done, fmt.Errorf("converting %s to the authenticated format: %w", name, err)
		}
		done = append(done, name)
	}
	return done, nil
}

func convertLegacyFile(base vfs.FS, dir string, keys *KeySet, name string) error {
	path := base.PathJoin(dir, name)
	src, err := base.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	old, err := newEncFileOpen(src, keys)
	if err != nil {
		return err
	}
	st, err := old.Stat()
	if err != nil {
		return err
	}
	plain := make([]byte, st.Size())
	if _, err := old.ReadAt(plain, 0); err != nil && err != io.EOF {
		return err
	}
	tmp := path + ".dxe2tmp"
	dst, err := base.Create(tmp, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	out, err := newAEADFileCreate(dst, keys, name)
	if err != nil {
		_ = dst.Close()
		_ = base.Remove(tmp)
		return err
	}
	if _, err := out.Write(plain); err != nil {
		_ = dst.Close()
		_ = base.Remove(tmp)
		return err
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		_ = base.Remove(tmp)
		return err
	}
	if err := dst.Close(); err != nil {
		_ = base.Remove(tmp)
		return err
	}
	if err := base.Rename(tmp, path); err != nil {
		_ = base.Remove(tmp)
		return err
	}
	if dir == "" {
		return nil
	}
	d, err := base.OpenDir(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// LegacyFiles lists the files in dir (through the base FS) that are
// unauthenticated "DXE1" files: what must be gone before the store can
// refuse the format. The registry and headerless files do not count.
func LegacyFiles(base vfs.FS, dir string) ([]string, error) {
	names, err := base.List(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, name := range names {
		if name == RegistryName {
			continue
		}
		path := base.PathJoin(dir, name)
		st, err := base.Stat(path)
		if err != nil || st.IsDir() {
			continue
		}
		magic, _, err := fileFormat(base, path)
		if err != nil {
			continue
		}
		if magic == fileMagic {
			out = append(out, name)
		}
	}
	return out, nil
}

type sizedFileInfo struct {
	vfs.FileInfo
	size int64
}

func (s sizedFileInfo) Size() int64 { return s.size }

// encFile encrypts at the file's logical offsets; the physical file is
// shifted by headerLen.
type encFile struct {
	f     vfs.File
	block cipher.Block
	iv    [16]byte
	woff  int64 // logical sequential-write offset
	roff  int64 // logical sequential-read offset
	// atOnly: the file was reopened with existing content (OpenReadWrite),
	// where the base file's own write position sits at 0 — sequential
	// writes must go through WriteAt (guaranteed for OpenReadWrite files)
	// or they would clobber the header.
	atOnly bool
}

func newEncFileCreate(f vfs.File, keys *KeySet) (*encFile, error) {
	id, key := keys.Active()
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	e := &encFile{f: f, block: block}
	if _, err := rand.Read(e.iv[:]); err != nil {
		return nil, err
	}
	var hdr [headerLen]byte
	copy(hdr[0:4], fileMagic)
	binary.BigEndian.PutUint32(hdr[4:8], id)
	copy(hdr[8:24], e.iv[:])
	if _, err := f.Write(hdr[:]); err != nil {
		return nil, err
	}
	return e, nil
}

func newEncFileOpen(f vfs.File, keys *KeySet) (*encFile, error) {
	var hdr [headerLen]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return nil, fmt.Errorf("reading encryption header: %w", err)
	}
	if string(hdr[0:4]) != fileMagic {
		return nil, fmt.Errorf("missing encryption header (plaintext file in an encrypted store?)")
	}
	id := binary.BigEndian.Uint32(hdr[4:8])
	key, ok := keys.Lookup(id)
	if !ok {
		return nil, fmt.Errorf("file encrypted with unknown data key %d", id)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	e := &encFile{f: f, block: block, atOnly: true}
	copy(e.iv[:], hdr[8:24])
	// Appends (OpenReadWrite) continue after the existing content.
	if st, err := f.Stat(); err == nil && st.Size() >= headerLen {
		e.woff = st.Size() - headerLen
	}
	return e, nil
}

// streamAt returns a CTR stream positioned at logical offset off.
func (e *encFile) streamAt(off int64) cipher.Stream {
	ctr := e.iv
	add128(&ctr, uint64(off)/16)
	s := cipher.NewCTR(e.block, ctr[:])
	if skip := off % 16; skip != 0 {
		var junk [16]byte
		s.XORKeyStream(junk[:skip], junk[:skip])
	}
	return s
}

// add128 adds n to a 128-bit big-endian counter.
func add128(ctr *[16]byte, n uint64) {
	hi := binary.BigEndian.Uint64(ctr[0:8])
	lo := binary.BigEndian.Uint64(ctr[8:16])
	sum := lo + n
	if sum < lo {
		hi++
	}
	binary.BigEndian.PutUint64(ctr[0:8], hi)
	binary.BigEndian.PutUint64(ctr[8:16], sum)
}

func (e *encFile) Write(p []byte) (int, error) {
	// vfs.File.Write is documented as allowed to modify the passed slice —
	// but the base file's Write may retain it, so encrypt into a copy.
	buf := make([]byte, len(p))
	e.streamAt(e.woff).XORKeyStream(buf, p)
	var n int
	var err error
	if e.atOnly {
		n, err = e.f.WriteAt(buf, e.woff+headerLen)
	} else {
		n, err = e.f.Write(buf)
	}
	e.woff += int64(n)
	return n, err
}

func (e *encFile) WriteAt(p []byte, off int64) (int, error) {
	buf := make([]byte, len(p))
	e.streamAt(off).XORKeyStream(buf, p)
	return e.f.WriteAt(buf, off+headerLen)
}

func (e *encFile) Read(p []byte) (int, error) {
	n, err := e.ReadAt(p, e.roff)
	e.roff += int64(n)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, err
}

func (e *encFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := e.f.ReadAt(p, off+headerLen)
	if n > 0 {
		e.streamAt(off).XORKeyStream(p[:n], p[:n])
	}
	return n, err
}

func (e *encFile) Preallocate(off, length int64) error {
	return e.f.Preallocate(off+headerLen, length)
}

func (e *encFile) Stat() (vfs.FileInfo, error) {
	st, err := e.f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < headerLen {
		return st, nil
	}
	return sizedFileInfo{FileInfo: st, size: st.Size() - headerLen}, nil
}

func (e *encFile) Sync() error     { return e.f.Sync() }
func (e *encFile) SyncData() error { return e.f.SyncData() }

func (e *encFile) SyncTo(length int64) (bool, error) {
	return e.f.SyncTo(length + headerLen)
}

func (e *encFile) Prefetch(off, length int64) error {
	return e.f.Prefetch(off+headerLen, length)
}

// Fd exposes the underlying descriptor. Audited against Pebble v1.1.5:
// the fd is used only for flock, fadvise hints, fallocate (preallocation)
// and sync_file_range — metadata/hint syscalls that never read or write
// file CONTENT, so nothing bypasses the encryption wrapper. Hiding it
// (vfs.InvalidFd) costs real WAL fsync latency: no preallocation means
// every append extends the file and every fdatasync journals metadata.
// Re-audit this choice on any Pebble upgrade — an fd used for mmap or
// direct reads would return ciphertext.
func (e *encFile) Fd() uintptr { return e.f.Fd() }

func (e *encFile) Close() error { return e.f.Close() }
