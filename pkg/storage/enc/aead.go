package enc

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"regexp"
	"sync"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// The authenticated file format (issue #220, tier 3). Where "DXE1" is a
// CTR keystream over the file — confidentiality and nothing else — "DXE2"
// seals the content in fixed-size chunks with AES-256-GCM, so a changed,
// transplanted, reordered or truncated-mid-chunk byte is refused at read
// time rather than served as data.
//
//	[0:4)   magic "DXE2"
//	[4:8)   data-key ID, uint32 big-endian
//	[8:12)  flags, uint32 big-endian (flagBound: the file's base name is
//	        authenticated with every chunk, so a chunk — or the whole
//	        file — moved under another name is refused)
//	[12:16) chunk size in plaintext bytes, uint32 big-endian
//	[16:32) 16-byte random salt
//
// Then chunk 0, 1, 2, … back to back, each
//
//	[gen uint32 BE][ciphertext, chunk-size bytes or fewer for the last][16-byte tag]
//
// The file key is HKDF-SHA256(data key, salt, fileKeyInfo): per file, so
// chunk nonces need only be unique within the file and a chunk sealed for
// one file cannot authenticate in another. The nonce is
// chunkIndex(uint64 BE) ‖ gen(uint32 BE): the chunk's position, and how
// many times that position has been written. A chunk is rewritten whenever
// it is appended to (the tail of a WAL grows by a record at a time), and
// GCM is only safe if no nonce ever seals two different plaintexts, so each
// rewrite carries gen+1 and the nonce moves with it. gen is stored in the
// clear beside the chunk and is what the nonce is rebuilt from at read: a
// changed gen is a changed nonce, and the tag no longer verifies.
//
// What this does and does not catch: any change inside a chunk, a chunk
// moved to another index, a chunk moved to another file, a file
// truncated anywhere but exactly at a chunk boundary, and — for bound
// files, sstables and WAL segments — a file renamed. Not caught: a file
// truncated at a chunk boundary (a WAL cut at a record boundary looks like
// a crash, as it did before; sstables and the MANIFEST carry their own
// length checks), and a whole file replaced by an older version of itself
// or by a different file under the same name of the same kind in another
// store sealed with the same data key, since nothing above the file layer
// hands the layer a version. docs/encryption.md states both.
const (
	fileMagicV2      = "DXE2"
	headerLenV2      = 32
	defaultChunkSize = 4096
	chunkGenLen      = 4
	chunkTagLen      = 16
	chunkOverhead    = chunkGenLen + chunkTagLen
	flagBound        = uint32(1) << 0
	fileKeyInfo      = "datax/enc/v2 file key"
	// maxChunkSize bounds what a header may ask a reader to allocate.
	maxChunkSize = 1 << 20
)

// boundName matches the files Pebble never renames — sstables and WAL
// segments — whose base name is therefore safe to authenticate. OPTIONS
// and the manifest markers are written under a temporary name and renamed,
// so binding theirs would refuse them at open.
var boundName = regexp.MustCompile(`^[0-9]{6}\.(sst|log)$`)

// aeadFile is an open "DXE2" file. Logical offsets are what Pebble sees;
// chunk i of the plaintext lives at physical offset
// headerLenV2 + i*(chunk+chunkOverhead). The partial last chunk, if any,
// is cached in plaintext (tail) so an append rewrites it without reading
// it back.
type aeadFile struct {
	f     vfs.File
	aead  cipher.AEAD
	aad   []byte // the bound base name, or nil
	chunk int    // plaintext bytes per chunk

	mu         sync.Mutex
	size       int64  // logical length
	tail       []byte // plaintext of the last chunk when it is partial
	tailGen    uint32 // gen the tail is sealed with on disk
	tailOnDisk bool
	roff       int64 // sequential-read offset
}

func fileKeyFor(dataKey, salt []byte) (cipher.AEAD, error) {
	key, err := hkdf.Key(sha256.New, dataKey, salt, fileKeyInfo, KeyLen)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// newAEADFileCreate starts a "DXE2" file on f. baseName is the name the
// file will be read under (bound into every chunk when it is a name Pebble
// never changes); it may differ from f's own name while a conversion
// writes the file under a temporary name.
func newAEADFileCreate(f vfs.File, keys *KeySet, baseName string) (*aeadFile, error) {
	id, dataKey := keys.Active()
	var hdr [headerLenV2]byte
	copy(hdr[0:4], fileMagicV2)
	binary.BigEndian.PutUint32(hdr[4:8], id)
	var flags uint32
	if boundName.MatchString(baseName) {
		flags |= flagBound
	}
	binary.BigEndian.PutUint32(hdr[8:12], flags)
	binary.BigEndian.PutUint32(hdr[12:16], defaultChunkSize)
	if _, err := rand.Read(hdr[16:32]); err != nil {
		return nil, err
	}
	aead, err := fileKeyFor(dataKey, hdr[16:32])
	if err != nil {
		return nil, err
	}
	if _, err := f.WriteAt(hdr[:], 0); err != nil {
		return nil, err
	}
	e := &aeadFile{f: f, aead: aead, chunk: defaultChunkSize}
	if flags&flagBound != 0 {
		e.aad = []byte(baseName)
	}
	return e, nil
}

// newAEADFileOpen opens an existing "DXE2" file whose header is hdr,
// verifying the partial last chunk, if any, so a file truncated inside a
// chunk is refused here rather than read short.
func newAEADFileOpen(f vfs.File, keys *KeySet, baseName string, hdr [headerLenV2]byte) (*aeadFile, error) {
	id := binary.BigEndian.Uint32(hdr[4:8])
	dataKey, ok := keys.Lookup(id)
	if !ok {
		return nil, fmt.Errorf("file encrypted with unknown data key %d", id)
	}
	flags := binary.BigEndian.Uint32(hdr[8:12])
	chunk := int(binary.BigEndian.Uint32(hdr[12:16]))
	if chunk <= 0 || chunk > maxChunkSize {
		return nil, fmt.Errorf("corrupt encryption header: chunk size %d", chunk)
	}
	aead, err := fileKeyFor(dataKey, hdr[16:32])
	if err != nil {
		return nil, err
	}
	e := &aeadFile{f: f, aead: aead, chunk: chunk}
	if flags&flagBound != 0 {
		e.aad = []byte(baseName)
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size, ok := logicalSize(st.Size(), chunk)
	if !ok {
		return nil, fmt.Errorf("corrupt encrypted file: %d bytes is no whole number of chunks (truncated inside a chunk's framing)", st.Size())
	}
	e.size = size
	if t := size % int64(chunk); t != 0 {
		plain, gen, err := e.readChunk(size / int64(chunk))
		if err != nil {
			return nil, fmt.Errorf("verifying the last chunk: %w", err)
		}
		e.tail, e.tailGen, e.tailOnDisk = plain, gen, true
	}
	return e, nil
}

// logicalSize derives the plaintext length from a "DXE2" file's physical
// length. A physical tail shorter than a chunk's framing cannot be a
// chunk and is reported as not ok.
func logicalSize(physical int64, chunk int) (int64, bool) {
	body := physical - headerLenV2
	if body < 0 {
		return 0, false
	}
	phys := int64(chunk + chunkOverhead)
	full, rest := body/phys, body%phys
	if rest == 0 {
		return full * int64(chunk), true
	}
	if rest <= chunkOverhead {
		return 0, false
	}
	return full*int64(chunk) + rest - chunkOverhead, true
}

func (e *aeadFile) physOff(i int64) int64 {
	return headerLenV2 + i*int64(e.chunk+chunkOverhead)
}

// chunkLen is the plaintext length of chunk i at the current size.
func (e *aeadFile) chunkLen(i int64) int {
	start := i * int64(e.chunk)
	if start >= e.size {
		return 0
	}
	if n := e.size - start; n < int64(e.chunk) {
		return int(n)
	}
	return e.chunk
}

func (e *aeadFile) nonce(i int64, gen uint32) []byte {
	var n [12]byte
	binary.BigEndian.PutUint64(n[0:8], uint64(i))
	binary.BigEndian.PutUint32(n[8:12], gen)
	return n[:]
}

// readChunk reads and opens chunk i, whose plaintext length is n.
func (e *aeadFile) readChunkN(i int64, n int) ([]byte, uint32, error) {
	buf := make([]byte, chunkOverhead+n)
	if _, err := e.f.ReadAt(buf, e.physOff(i)); err != nil {
		return nil, 0, err
	}
	gen := binary.BigEndian.Uint32(buf[0:chunkGenLen])
	plain, err := e.aead.Open(buf[chunkGenLen:chunkGenLen], e.nonce(i, gen), buf[chunkGenLen:], e.aad)
	if err != nil {
		return nil, 0, fmt.Errorf("chunk %d of the encrypted file does not authenticate (altered, moved, or sealed for another file)", i)
	}
	return plain, gen, nil
}

func (e *aeadFile) readChunk(i int64) ([]byte, uint32, error) {
	return e.readChunkN(i, e.chunkLen(i))
}

// writeChunk seals plain as chunk i under gen and writes it in place.
func (e *aeadFile) writeChunk(i int64, gen uint32, plain []byte) error {
	buf := make([]byte, chunkGenLen, chunkOverhead+len(plain))
	binary.BigEndian.PutUint32(buf[0:chunkGenLen], gen)
	buf = e.aead.Seal(buf, e.nonce(i, gen), plain, e.aad)
	_, err := e.f.WriteAt(buf, e.physOff(i))
	return err
}

// Write appends: Pebble's writers only ever extend a file.
func (e *aeadFile) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.writeAtLocked(p, e.size)
}

// WriteAt writes within or at the end of the file. A write that would
// leave a hole is refused: a chunk that was never written has no place on
// disk, and Pebble never writes one.
func (e *aeadFile) WriteAt(p []byte, off int64) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if off > e.size {
		return 0, fmt.Errorf("encrypted file: write at %d beyond the end at %d", off, e.size)
	}
	return e.writeAtLocked(p, off)
}

func (e *aeadFile) writeAtLocked(p []byte, off int64) (int, error) {
	written := 0
	for len(p) > 0 {
		i := off / int64(e.chunk)
		o := int(off % int64(e.chunk))
		n := len(p)
		if room := e.chunk - o; n > room {
			n = room
		}
		var plain []byte
		var gen uint32
		existed := false
		if cur := e.chunkLen(i); cur > 0 {
			existed = true
			if t := e.size / int64(e.chunk); i == t && e.tail != nil {
				plain = append(make([]byte, 0, e.chunk), e.tail...)
				gen = e.tailGen
			} else {
				var err error
				plain, gen, err = e.readChunk(i)
				if err != nil {
					return written, err
				}
			}
		}
		if len(plain) < o+n {
			plain = append(plain, make([]byte, o+n-len(plain))...)
		}
		copy(plain[o:], p[:n])
		newGen := uint32(0)
		if existed {
			newGen = gen + 1
		}
		if err := e.writeChunk(i, newGen, plain); err != nil {
			return written, err
		}
		if end := off + int64(n); end > e.size {
			e.size = end
		}
		switch {
		case e.size%int64(e.chunk) == 0:
			e.tail, e.tailGen, e.tailOnDisk = nil, 0, false
		case i == e.size/int64(e.chunk):
			e.tail, e.tailGen, e.tailOnDisk = plain, newGen, true
		}
		p = p[n:]
		off += int64(n)
		written += n
	}
	return written, nil
}

func (e *aeadFile) Read(p []byte) (int, error) {
	e.mu.Lock()
	off := e.roff
	e.mu.Unlock()
	n, err := e.ReadAt(p, off)
	e.mu.Lock()
	e.roff += int64(n)
	e.mu.Unlock()
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, err
}

func (e *aeadFile) ReadAt(p []byte, off int64) (int, error) {
	e.mu.Lock()
	size := e.size
	tailIdx := int64(-1)
	var tail []byte
	if e.tail != nil {
		tailIdx = size / int64(e.chunk)
		tail = e.tail
	}
	e.mu.Unlock()
	if off < 0 {
		return 0, fmt.Errorf("encrypted file: negative offset %d", off)
	}
	if off >= size {
		return 0, io.EOF
	}
	end := off + int64(len(p))
	eof := false
	if end > size {
		end, eof = size, true
	}
	read := 0
	for off < end {
		i := off / int64(e.chunk)
		o := int(off % int64(e.chunk))
		var plain []byte
		if i == tailIdx {
			plain = tail
		} else {
			n := e.chunk
			if rem := size - i*int64(e.chunk); rem < int64(n) {
				n = int(rem)
			}
			var err error
			if plain, _, err = e.readChunkN(i, n); err != nil {
				return read, err
			}
		}
		if o >= len(plain) {
			// A chunk shorter than its position says: the file layer never
			// writes one, so this is a read of a damaged file that somehow
			// authenticated. Stop rather than spin.
			return read, io.ErrUnexpectedEOF
		}
		n := copy(p[read:], plain[o:])
		if int64(n) > end-off {
			n = int(end - off)
		}
		read += n
		off += int64(n)
	}
	if eof {
		return read, io.EOF
	}
	return read, nil
}

// Preallocate, Prefetch and SyncTo translate logical spans to physical
// ones chunk by chunk; the hints are advisory, the sync covers at least
// the chunks the logical prefix touches.
func (e *aeadFile) physSpan(off, length int64) (int64, int64) {
	first := off / int64(e.chunk)
	last := (off + length - 1) / int64(e.chunk)
	if length <= 0 {
		last = first
	}
	start := e.physOff(first)
	return start, e.physOff(last+1) - start
}

func (e *aeadFile) Preallocate(off, length int64) error {
	s, l := e.physSpan(off, length)
	return e.f.Preallocate(s, l)
}

func (e *aeadFile) Prefetch(off, length int64) error {
	s, l := e.physSpan(off, length)
	return e.f.Prefetch(s, l)
}

func (e *aeadFile) SyncTo(length int64) (bool, error) {
	chunks := (length + int64(e.chunk) - 1) / int64(e.chunk)
	return e.f.SyncTo(e.physOff(chunks))
}

func (e *aeadFile) Stat() (vfs.FileInfo, error) {
	st, err := e.f.Stat()
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	size := e.size
	e.mu.Unlock()
	return sizedFileInfo{FileInfo: st, size: size}, nil
}

func (e *aeadFile) Sync() error     { return e.f.Sync() }
func (e *aeadFile) SyncData() error { return e.f.SyncData() }

// Fd passes through on the same audit as encFile.Fd: Pebble uses the
// descriptor for metadata and hint syscalls only.
func (e *aeadFile) Fd() uintptr { return e.f.Fd() }

func (e *aeadFile) Close() error { return e.f.Close() }
