// Package enc implements datax's encryption at rest: a vfs.FS wrapper that
// transparently encrypts every file Pebble writes (sstables, WAL,
// MANIFEST, ...) with AES-256-CTR, plus the AES-GCM-sealed key registry
// and helpers for sealing non-Pebble artifacts (the metadata backup).
// stdlib crypto only. See docs/encryption.md.
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

// FileKeyID reads the data-key ID from an encrypted file's header,
// through the BASE (unencrypted) filesystem. ok=false when the file
// carries no encryption header (the registry, LOCK, short files).
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
	if string(hdr[0:4]) != fileMagic {
		return 0, false, nil
	}
	return binary.BigEndian.Uint32(hdr[4:8]), true, nil
}

// FS encrypts every file created through it. Non-file operations pass
// through to the base FS.
type FS struct {
	vfs.FS
	keys *KeySet
}

// NewFS wraps base so all file content is encrypted with keys.
func NewFS(base vfs.FS, keys *KeySet) *FS {
	return &FS{FS: base, keys: keys}
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
	return newEncFileCreate(f, fs.keys)
}

func (fs *FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	if fs.isRegistry(name) {
		return f, nil
	}
	ef, err := newEncFileOpen(f, fs.keys)
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
		return newEncFileCreate(f, fs.keys)
	}
	ef, err := newEncFileOpen(f, fs.keys)
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

// Stat subtracts the header so logical sizes line up. Files smaller than a
// header (the zero-length LOCK file, directories) are reported as-is.
func (fs *FS) Stat(name string) (vfs.FileInfo, error) {
	st, err := fs.FS.Stat(name)
	if err != nil {
		return nil, err
	}
	if fs.isRegistry(name) || st.IsDir() || st.Size() < headerLen {
		return st, nil
	}
	return sizedFileInfo{FileInfo: st, size: st.Size() - headerLen}, nil
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
