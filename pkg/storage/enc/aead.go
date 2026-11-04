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
// seals the content with AES-256-GCM, so a changed, transplanted,
// reordered or truncated-mid-chunk byte is refused at read time rather
// than served as data.
//
//	[0:4)   magic "DXE2"
//	[4:8)   data-key ID, uint32 big-endian
//	[8:12)  flags, uint32 big-endian
//	        flagBound: the file's base name is authenticated with every
//	        chunk, so a chunk — or the whole file — moved under another
//	        name is refused
//	        flagFrames: the body is a sequence of append-only frames (a
//	        log) rather than fixed chunks; see below
//	[12:16) chunk size in plaintext bytes, uint32 big-endian (0 for a
//	        framed file)
//	[16:32) 16-byte random salt
//
// The file key is HKDF-SHA256(data key, salt, fileKeyInfo): per file, so
// nonces need only be unique within a file. The whole header is the
// additional data of every chunk and frame (with the base name appended
// for a bound file), so a header field — the salt above all — cannot be
// edited without every tag failing, and a chunk sealed for one file cannot
// authenticate in another: transplanting it would need a byte-identical
// header, and no two files share a salt.
//
// Two body layouts, chosen by the file's name when it is created:
//
// Chunked (sstables, OPTIONS, markers — anything read at random offsets):
// chunk 0, 1, 2, … back to back, each
//
//	[nonce uint64 BE][ciphertext, chunk-size bytes or fewer for the last][16-byte tag]
//
// at physical offset headerLenV2 + i*(chunk+chunkOverhead), so a logical
// offset maps to a chunk by arithmetic. The GCM nonce is
// chunkIndex(uint32 BE) ‖ the 8 random bytes stored beside the chunk,
// drawn afresh every time the chunk is written. A chunk is rewritten when
// it is appended to (an sstable's writer extends the tail chunk), and GCM
// is only safe if no nonce ever seals two different plaintexts; a random
// nonce needs nothing remembered across a crash, where a counter would
// restart from whatever reached the disk (a lost tail write, and the next
// append would seal a second plaintext under the counter's old value).
//
// Framed (WAL segments, MANIFEST files — logs, appended to and read front
// to back): frame 0, 1, 2, … back to back, each
//
//	[length uint32 BE][nonce uint64 BE][ciphertext, length bytes][16-byte tag]
//
// one frame per Write, nonce = frameIndex(uint32 BE) ‖ the random bytes,
// and a frame once written is never written again. That is what the
// chunked layout cannot give a log: it rewrites the tail chunk on every
// append, over bytes an earlier Sync already made durable, and a crash in
// the middle of that rewrite leaves a chunk that is a mixture of two
// versions and authenticates as neither — the log refused at open, and
// acknowledged writes lost with it. Frames only ever extend the file, so a
// crash damages at most the frame being appended, the one not yet
// acknowledged. The reader treats a final frame that is incomplete or
// does not authenticate as the end of the log — a crash, the same way
// Pebble's own per-record checksum does — and refuses a frame whose tag
// fails with frames after it, which no crash produces. A damaged length
// field is the one edit it cannot place: the frames after it cannot be
// found, so the log ends there, which is a truncation, and a log
// truncated anywhere already reads as a crash (below).
//
// What this does and does not catch: any change inside a chunk or frame,
// one moved to another index, one moved to another file, a chunked file
// truncated anywhere but exactly at a chunk boundary, and — for bound
// files, sstables and WAL segments — a file renamed. Not caught: a
// chunked file truncated at a chunk boundary and a log truncated anywhere
// — cut at or inside a frame, or a frame's length field damaged (either
// looks like a crash, as it did before; sstables and the MANIFEST carry
// their own length checks, and a WAL's records their own) — and a whole
// file replaced by an older version of itself or by a different file
// under the same name of the same kind in another store sealed with the
// same data key, since nothing above the file layer hands the layer a
// version. docs/encryption.md states all of it.
const (
	fileMagicV2      = "DXE2"
	headerLenV2      = 32
	defaultChunkSize = 4096
	chunkNonceLen    = 8
	chunkTagLen      = 16
	chunkOverhead    = chunkNonceLen + chunkTagLen
	frameLenLen      = 4
	frameOverhead    = frameLenLen + chunkNonceLen + chunkTagLen
	flagBound        = uint32(1) << 0
	flagFrames       = uint32(1) << 1
	fileKeyInfo      = "datax/enc/v2 file key"
	// maxChunkSize bounds what a header may ask a reader to allocate;
	// maxFrameLen what a frame's length field may. A Write longer than
	// maxFrameLen becomes several frames.
	maxChunkSize = 1 << 20
	maxFrameLen  = 16 << 20
	// maxIndex is the last chunk or frame index a nonce can carry.
	maxIndex = int64(^uint32(0))
)

// boundName matches the files Pebble never renames — sstables and WAL
// segments — whose base name is therefore safe to authenticate. OPTIONS
// and the manifest markers are written under a temporary name and renamed,
// so binding theirs would refuse them at open.
var boundName = regexp.MustCompile(`^[0-9]{6}\.(sst|log)$`)

// framedName matches the files Pebble appends to and replays front to
// back — WAL segments and MANIFEST files — which take the framed layout.
var framedName = regexp.MustCompile(`^[0-9]{6}\.log$|^MANIFEST-[0-9]{6}$`)

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

// aadFor is the additional data every chunk or frame of a file is sealed
// with: the header, and the base name when the file is bound.
func aadFor(hdr [headerLenV2]byte, baseName string) []byte {
	aad := append([]byte(nil), hdr[:]...)
	if binary.BigEndian.Uint32(hdr[8:12])&flagBound != 0 {
		aad = append(aad, baseName...)
	}
	return aad
}

// nonceFor is index ‖ random, GCM's twelve bytes.
func nonceFor(i int64, random []byte) []byte {
	var n [12]byte
	binary.BigEndian.PutUint32(n[0:4], uint32(i))
	copy(n[4:12], random)
	return n[:]
}

// newAuthenticatedCreate starts a "DXE2" file on f. baseName is the name
// the file will be read under (bound into every chunk when it is a name
// Pebble never changes, and choosing the framed layout when it is a log);
// it may differ from f's own name while a conversion writes the file under
// a temporary name.
func newAuthenticatedCreate(f vfs.File, keys *KeySet, baseName string) (vfs.File, error) {
	id, dataKey := keys.Active()
	var hdr [headerLenV2]byte
	copy(hdr[0:4], fileMagicV2)
	binary.BigEndian.PutUint32(hdr[4:8], id)
	var flags uint32
	if boundName.MatchString(baseName) {
		flags |= flagBound
	}
	framed := framedName.MatchString(baseName)
	if framed {
		flags |= flagFrames
	} else {
		binary.BigEndian.PutUint32(hdr[12:16], defaultChunkSize)
	}
	binary.BigEndian.PutUint32(hdr[8:12], flags)
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
	aad := aadFor(hdr, baseName)
	if framed {
		return &frameFile{f: f, aead: aead, aad: aad, physEnd: headerLenV2, rphys: headerLenV2}, nil
	}
	return &aeadFile{f: f, aead: aead, aad: aad, chunk: defaultChunkSize}, nil
}

// openAuthenticated opens an existing "DXE2" file whose header is hdr. A
// chunked file has its partial last chunk verified here, so one truncated
// inside a chunk is refused at open rather than read short; a framed file
// is scanned for where its complete frames end.
func openAuthenticated(f vfs.File, keys *KeySet, baseName string, hdr [headerLenV2]byte) (vfs.File, error) {
	id := binary.BigEndian.Uint32(hdr[4:8])
	dataKey, ok := keys.Lookup(id)
	if !ok {
		return nil, fmt.Errorf("file encrypted with unknown data key %d", id)
	}
	flags := binary.BigEndian.Uint32(hdr[8:12])
	aead, err := fileKeyFor(dataKey, hdr[16:32])
	if err != nil {
		return nil, err
	}
	aad := aadFor(hdr, baseName)
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if flags&flagFrames != 0 {
		e := &frameFile{f: f, aead: aead, aad: aad, physEnd: headerLenV2, rphys: headerLenV2}
		if err := e.scan(st.Size()); err != nil {
			return nil, err
		}
		return e, nil
	}
	chunk := int(binary.BigEndian.Uint32(hdr[12:16]))
	if chunk <= 0 || chunk > maxChunkSize {
		return nil, fmt.Errorf("corrupt encryption header: chunk size %d", chunk)
	}
	e := &aeadFile{f: f, aead: aead, aad: aad, chunk: chunk}
	size, ok := logicalSize(st.Size(), chunk)
	if !ok {
		return nil, fmt.Errorf("corrupt encrypted file: %d bytes is no whole number of chunks (truncated inside a chunk's framing)", st.Size())
	}
	e.size = size
	if t := size % int64(chunk); t != 0 {
		plain, err := e.readChunk(size / int64(chunk))
		if err != nil {
			return nil, fmt.Errorf("verifying the last chunk: %w", err)
		}
		e.tail = plain
	}
	return e, nil
}

// ---- chunked files -------------------------------------------------------

// aeadFile is an open chunked "DXE2" file. Logical offsets are what Pebble
// sees; chunk i of the plaintext lives at physical offset
// headerLenV2 + i*(chunk+chunkOverhead). The partial last chunk, if any,
// is cached in plaintext (tail) so an append rewrites it without reading
// it back.
type aeadFile struct {
	f     vfs.File
	aead  cipher.AEAD
	aad   []byte
	chunk int // plaintext bytes per chunk

	mu   sync.Mutex
	size int64  // logical length
	tail []byte // plaintext of the last chunk when it is partial
	roff int64  // sequential-read offset
}

// logicalSize derives the plaintext length from a chunked "DXE2" file's
// physical length. A physical tail shorter than a chunk's framing cannot
// be a chunk and is reported as not ok.
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

// readChunkN reads and opens chunk i, whose plaintext length is n.
func (e *aeadFile) readChunkN(i int64, n int) ([]byte, error) {
	buf := make([]byte, chunkOverhead+n)
	if _, err := e.f.ReadAt(buf, e.physOff(i)); err != nil {
		return nil, err
	}
	plain, err := e.aead.Open(buf[chunkNonceLen:chunkNonceLen], nonceFor(i, buf[:chunkNonceLen]), buf[chunkNonceLen:], e.aad)
	if err != nil {
		return nil, fmt.Errorf("chunk %d of the encrypted file does not authenticate (altered, moved, or sealed for another file)", i)
	}
	return plain, nil
}

func (e *aeadFile) readChunk(i int64) ([]byte, error) {
	return e.readChunkN(i, e.chunkLen(i))
}

// writeChunk seals plain as chunk i under a fresh nonce and writes it in
// place.
func (e *aeadFile) writeChunk(i int64, plain []byte) error {
	if i > maxIndex {
		return fmt.Errorf("encrypted file: chunk %d is beyond what the format addresses", i)
	}
	buf := make([]byte, chunkNonceLen, chunkOverhead+len(plain))
	if _, err := rand.Read(buf[:chunkNonceLen]); err != nil {
		return err
	}
	buf = e.aead.Seal(buf, nonceFor(i, buf[:chunkNonceLen]), plain, e.aad)
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
		if cur := e.chunkLen(i); cur > 0 {
			if t := e.size / int64(e.chunk); i == t && e.tail != nil {
				plain = append(make([]byte, 0, e.chunk), e.tail...)
			} else {
				var err error
				plain, err = e.readChunk(i)
				if err != nil {
					return written, err
				}
			}
		}
		if len(plain) < o+n {
			plain = append(plain, make([]byte, o+n-len(plain))...)
		}
		copy(plain[o:], p[:n])
		if err := e.writeChunk(i, plain); err != nil {
			return written, err
		}
		if end := off + int64(n); end > e.size {
			e.size = end
		}
		switch {
		case e.size%int64(e.chunk) == 0:
			e.tail = nil
		case i == e.size/int64(e.chunk):
			e.tail = plain
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
			if plain, err = e.readChunkN(i, n); err != nil {
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

// ---- framed files (logs) --------------------------------------------------

// frameFile is an open framed "DXE2" file: a log, appended to a frame at a
// time and read front to back. Writes only ever extend it; a Write becomes
// one frame (several, past maxFrameLen), at the physical end. Reads are
// sequential — a Read opens the next frame — and ReadAt restarts from the
// front when asked for an offset behind the cursor, which Pebble never
// does with a log.
type frameFile struct {
	f    vfs.File
	aead cipher.AEAD
	aad  []byte

	mu      sync.Mutex
	size    int64 // logical length: the plaintext of every complete frame
	physEnd int64 // physical end of the complete frames
	frames  int64 // number of complete frames; the next frame's index
	// The sequential reader: the plaintext of the current frame not yet
	// delivered, the logical offset of its first byte, and the physical
	// offset and index of the frame after it.
	rbuf   []byte
	rpos   int64
	rphys  int64
	rframe int64
}

// scan finds, in a file of physical length physical, where its complete
// frames end: a final frame cut short by a crash is not one and is
// ignored, like Pebble's own torn tail record. Frame headers are read
// through a buffer, so a log of many small frames costs a read per
// buffer, not per frame. Tags are checked when the frames are read.
func (e *frameFile) scan(physical int64) error {
	const window = 1 << 20
	var buf []byte
	bufStart := int64(0)
	lastStart, lastLen := int64(0), int64(0)
	for pos := int64(headerLenV2); pos+frameOverhead <= physical; {
		if pos < bufStart || pos+frameLenLen > bufStart+int64(len(buf)) {
			n := int64(window)
			if rem := physical - pos; rem < n {
				n = rem
			}
			buf = make([]byte, n)
			if _, err := e.f.ReadAt(buf, pos); err != nil && err != io.EOF {
				return fmt.Errorf("scanning the log's frames: %w", err)
			}
			bufStart = pos
		}
		length := int64(binary.BigEndian.Uint32(buf[pos-bufStart : pos-bufStart+frameLenLen]))
		if length == 0 || length > maxFrameLen || pos+frameOverhead+length > physical {
			// Not a whole frame: the end of what reached the disk.
			break
		}
		lastStart, lastLen = pos, length
		pos += frameOverhead + length
		e.size += length
		e.physEnd = pos
		e.frames++
	}
	// A final frame that is whole but does not authenticate was torn by a
	// crash too (its pages reached the disk out of order). With the key at
	// hand, find that out now, so a writer reopening the log appends over
	// it rather than after it, and Stat agrees with what a reader gets.
	if e.frames > 0 && e.aead != nil {
		hdr := make([]byte, frameOverhead+lastLen)
		if _, err := e.f.ReadAt(hdr, lastStart); err != nil {
			return fmt.Errorf("reading the log's last frame: %w", err)
		}
		random := hdr[frameLenLen : frameLenLen+chunkNonceLen]
		if _, err := e.aead.Open(nil, nonceFor(e.frames-1, random), hdr[frameLenLen+chunkNonceLen:], e.aad); err != nil {
			e.physEnd, e.size, e.frames = lastStart, e.size-lastLen, e.frames-1
		}
	}
	return nil
}

// Write appends p as one frame (several when it is longer than a frame
// may be). Nothing already on disk is touched.
func (e *frameFile) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.appendLocked(p)
}

// WriteAt accepts only an append: a log is never rewritten.
func (e *frameFile) WriteAt(p []byte, off int64) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if off != e.size {
		return 0, fmt.Errorf("encrypted log: write at %d, not at its end %d (a log is only appended to)", off, e.size)
	}
	return e.appendLocked(p)
}

func (e *frameFile) appendLocked(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := len(p)
		if n > maxFrameLen {
			n = maxFrameLen
		}
		if e.frames > maxIndex {
			return written, fmt.Errorf("encrypted log: frame %d is beyond what the format addresses", e.frames)
		}
		buf := make([]byte, frameLenLen+chunkNonceLen, frameOverhead+n)
		binary.BigEndian.PutUint32(buf[0:frameLenLen], uint32(n))
		random := buf[frameLenLen : frameLenLen+chunkNonceLen]
		if _, err := rand.Read(random); err != nil {
			return written, err
		}
		buf = e.aead.Seal(buf, nonceFor(e.frames, random), p[:n], e.aad)
		if _, err := e.f.WriteAt(buf, e.physEnd); err != nil {
			return written, err
		}
		e.physEnd += int64(len(buf))
		e.size += int64(n)
		e.frames++
		p = p[n:]
		written += n
	}
	return written, nil
}

// nextFrameLocked opens the frame at the read cursor into rbuf. At the end
// of the complete frames it reports io.EOF. A frame that does not
// authenticate is the end of the log when it is the last one — a crash
// tore it — and a refusal when frames follow it, which no crash produces.
func (e *frameFile) nextFrameLocked() error {
	if e.rphys >= e.physEnd {
		return io.EOF
	}
	var hdr [frameLenLen + chunkNonceLen]byte
	if _, err := e.f.ReadAt(hdr[:], e.rphys); err != nil {
		return err
	}
	length := int(binary.BigEndian.Uint32(hdr[0:frameLenLen]))
	body := make([]byte, length+chunkTagLen)
	if _, err := e.f.ReadAt(body, e.rphys+frameLenLen+chunkNonceLen); err != nil {
		return err
	}
	plain, err := e.aead.Open(body[:0], nonceFor(e.rframe, hdr[frameLenLen:]), body, e.aad)
	if err != nil {
		if e.rphys+int64(frameOverhead+length) >= e.physEnd {
			// The last frame: torn by a crash. The log ends before it.
			e.physEnd, e.size, e.frames = e.rphys, e.rpos, e.rframe
			return io.EOF
		}
		return fmt.Errorf("frame %d of the encrypted log does not authenticate (altered, moved, or sealed for another file)", e.rframe)
	}
	e.rbuf = plain
	e.rphys += int64(frameOverhead + length)
	e.rframe++
	return nil
}

func (e *frameFile) Read(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	read := 0
	for read < len(p) {
		if len(e.rbuf) == 0 {
			if err := e.nextFrameLocked(); err != nil {
				if err == io.EOF && read > 0 {
					return read, nil
				}
				return read, err
			}
			continue
		}
		n := copy(p[read:], e.rbuf)
		e.rbuf = e.rbuf[n:]
		e.rpos += int64(n)
		read += n
	}
	return read, nil
}

// ReadAt reads at off through the sequential cursor, rewinding to the
// front when off is behind it.
func (e *frameFile) ReadAt(p []byte, off int64) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if off < 0 {
		return 0, fmt.Errorf("encrypted log: negative offset %d", off)
	}
	if off < e.rpos {
		e.rbuf, e.rpos, e.rphys, e.rframe = nil, 0, headerLenV2, 0
	}
	for e.rpos < off {
		if len(e.rbuf) == 0 {
			if err := e.nextFrameLocked(); err != nil {
				return 0, err
			}
			continue
		}
		skip := off - e.rpos
		if skip > int64(len(e.rbuf)) {
			skip = int64(len(e.rbuf))
		}
		e.rbuf = e.rbuf[skip:]
		e.rpos += skip
	}
	read := 0
	for read < len(p) {
		if len(e.rbuf) == 0 {
			if err := e.nextFrameLocked(); err != nil {
				return read, err
			}
			continue
		}
		n := copy(p[read:], e.rbuf)
		e.rbuf = e.rbuf[n:]
		e.rpos += int64(n)
		read += n
	}
	return read, nil
}

// Preallocate, Prefetch and SyncTo are advisory: the log is extended at
// its physical end and synced up to it.
func (e *frameFile) Preallocate(off, length int64) error {
	e.mu.Lock()
	end := e.physEnd
	e.mu.Unlock()
	return e.f.Preallocate(end, length)
}

func (e *frameFile) Prefetch(off, length int64) error { return nil }

func (e *frameFile) SyncTo(length int64) (bool, error) {
	e.mu.Lock()
	end := e.physEnd
	e.mu.Unlock()
	return e.f.SyncTo(end)
}

func (e *frameFile) Stat() (vfs.FileInfo, error) {
	st, err := e.f.Stat()
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	size := e.size
	e.mu.Unlock()
	return sizedFileInfo{FileInfo: st, size: size}, nil
}

func (e *frameFile) Sync() error     { return e.f.Sync() }
func (e *frameFile) SyncData() error { return e.f.SyncData() }
func (e *frameFile) Fd() uintptr     { return e.f.Fd() }
func (e *frameFile) Close() error    { return e.f.Close() }
