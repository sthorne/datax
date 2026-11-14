package enc

import (
	"bufio"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// A sealed stream (issue #291): the chunked "DXE2" layout of aead.go for a
// file that is written once, front to back, outside any store — a backup's
// data file, read on another machine by whoever holds the key. It keeps
// the header, the chunk framing and the nonce construction of a store
// file, and differs in the three places a file that leaves the store
// needs:
//
//   - The key comes from the caller, not from a store's registry (the
//     header's data-key ID is zero), and the file key is derived from it
//     with the file's name as well as the salt:
//     HKDF-SHA256(key, salt, streamKeyInfo ‖ 0x00 ‖ name).
//
//   - The name is always authenticated with every chunk (flagBound), so a
//     file sealed as one name does not open as another.
//
//   - Every chunk's additional data ends in one more byte saying whether
//     it is the last. A store file cut at a chunk boundary still
//     authenticates, which a store tolerates because sstables carry their
//     own length checks; a backup file is a bare run of records with none,
//     so here the end is sealed too. The writer always emits a final
//     chunk (an empty one for an empty stream), and a reader that reaches
//     the end of the file without having opened one refuses.
//
//     [32-byte "DXE2" header: flags = flagBound|flagStream, key ID 0]
//     chunk 0, 1, 2, …: [nonce uint64 BE][ciphertext][16-byte tag]
//
// Every chunk but the last holds exactly the header's chunk size of
// plaintext; the last holds that or fewer. The GCM nonce is
// chunkIndex(uint32 BE) ‖ the 8 random bytes, as in a store file, so a
// chunk moved to another position fails its tag.
//
// Nothing is buffered beyond one chunk on either side: a file of any size
// is sealed and opened in constant memory.
const (
	streamChunkSize = 64 << 10
	streamKeyInfo   = "datax/enc/v2 stream key"
)

var errStreamAuth = errors.New("does not authenticate (altered, truncated, reordered, sealed for another file, or the wrong key)")

func streamAAD(hdr [headerLenV2]byte, name string) []byte {
	// One spare byte of capacity for the last-chunk marker.
	aad := aadFor(hdr, name)
	return append(aad, 0)
}

// StreamWriter seals what is written to it. Close writes the final chunk
// and must be called for the stream to be readable; it does not close the
// underlying writer.
type StreamWriter struct {
	w      io.Writer
	aead   cipher.AEAD
	aad    []byte // header ‖ name ‖ last-chunk marker
	chunk  int
	buf    []byte
	index  int64
	closed bool
}

// NewStreamWriter starts a sealed stream on w under key (KeyLen bytes).
// name is the name the stream will be read under.
func NewStreamWriter(w io.Writer, key []byte, name string) (*StreamWriter, error) {
	return newStreamWriter(w, key, name, streamChunkSize)
}

func newStreamWriter(w io.Writer, key []byte, name string, chunk int) (*StreamWriter, error) {
	if len(key) != KeyLen {
		return nil, fmt.Errorf("key must be %d bytes, got %d", KeyLen, len(key))
	}
	var hdr [headerLenV2]byte
	copy(hdr[0:4], fileMagicV2)
	binary.BigEndian.PutUint32(hdr[8:12], flagBound|flagStream)
	binary.BigEndian.PutUint32(hdr[12:16], uint32(chunk))
	if _, err := rand.Read(hdr[16:32]); err != nil {
		return nil, err
	}
	aead, err := derivedAEAD(key, hdr[16:32], streamKeyInfo+"\x00"+name)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(hdr[:]); err != nil {
		return nil, err
	}
	return &StreamWriter{w: w, aead: aead, aad: streamAAD(hdr, name), chunk: chunk, buf: make([]byte, 0, chunk)}, nil
}

func (s *StreamWriter) seal(last bool) error {
	if s.index > maxIndex {
		return fmt.Errorf("sealed stream: chunk %d is beyond what the format addresses", s.index)
	}
	s.aad[len(s.aad)-1] = 0
	if last {
		s.aad[len(s.aad)-1] = 1
	}
	out := make([]byte, chunkNonceLen, chunkOverhead+len(s.buf))
	if _, err := rand.Read(out); err != nil {
		return err
	}
	out = s.aead.Seal(out, nonceFor(s.index, out[:chunkNonceLen]), s.buf, s.aad)
	if _, err := s.w.Write(out); err != nil {
		return err
	}
	s.index++
	s.buf = s.buf[:0]
	return nil
}

// Write buffers p and seals each chunk as it fills. A full chunk is held
// back until more arrives, since it may yet turn out to be the last.
func (s *StreamWriter) Write(p []byte) (int, error) {
	if s.closed {
		return 0, errors.New("sealed stream: write after close")
	}
	written := 0
	for len(p) > 0 {
		if len(s.buf) == s.chunk {
			if err := s.seal(false); err != nil {
				return written, err
			}
		}
		n := copy(s.buf[len(s.buf):s.chunk], p)
		s.buf = s.buf[:len(s.buf)+n]
		p = p[n:]
		written += n
	}
	return written, nil
}

// Close seals what is buffered as the final chunk.
func (s *StreamWriter) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.seal(true)
}

// StreamReader opens a sealed stream. Read returns plaintext only from
// chunks whose tags verified, and io.EOF only after the chunk the writer
// sealed as its last.
type StreamReader struct {
	r     *bufio.Reader
	aead  cipher.AEAD
	aad   []byte
	chunk int
	index int64
	plain []byte
	done  bool
	err   error
}

// NewStreamReader reads the header from r and prepares to open the stream
// sealed under key for name. A wrong key or name is reported by the first
// Read, as a chunk that does not authenticate.
func NewStreamReader(r io.Reader, key []byte, name string) (*StreamReader, error) {
	if len(key) != KeyLen {
		return nil, fmt.Errorf("key must be %d bytes, got %d", KeyLen, len(key))
	}
	var hdr [headerLenV2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("sealed stream: reading the header: %w", err)
	}
	if string(hdr[0:4]) != fileMagicV2 || binary.BigEndian.Uint32(hdr[8:12]) != flagBound|flagStream {
		return nil, errors.New("not a sealed stream (bad header)")
	}
	chunk := int(binary.BigEndian.Uint32(hdr[12:16]))
	if chunk <= 0 || chunk > maxChunkSize {
		return nil, fmt.Errorf("sealed stream: corrupt header: chunk size %d", chunk)
	}
	aead, err := derivedAEAD(key, hdr[16:32], streamKeyInfo+"\x00"+name)
	if err != nil {
		return nil, err
	}
	return &StreamReader{
		r:    bufio.NewReaderSize(r, chunk+chunkOverhead+1),
		aead: aead, aad: streamAAD(hdr, name), chunk: chunk,
	}, nil
}

// next opens the next chunk into plain. A chunk is the last when the file
// ends with it, and it must then have been sealed as the last: a file cut
// between chunks ends on one that was not, and is refused.
func (s *StreamReader) next() error {
	if s.done {
		return io.EOF
	}
	if s.index > maxIndex {
		return fmt.Errorf("sealed stream: chunk %d is beyond what the format addresses", s.index)
	}
	buf := make([]byte, chunkOverhead+s.chunk)
	n, err := io.ReadFull(s.r, buf)
	last := false
	switch {
	case err == io.EOF || err == io.ErrUnexpectedEOF:
		last = true
	case err != nil:
		return err
	default:
		if _, perr := s.r.Peek(1); perr == io.EOF {
			last = true
		} else if perr != nil {
			return perr
		}
	}
	if n < chunkOverhead {
		return fmt.Errorf("sealed stream: chunk %d %w", s.index, errStreamAuth)
	}
	buf = buf[:n]
	s.aad[len(s.aad)-1] = 0
	if last {
		s.aad[len(s.aad)-1] = 1
	}
	plain, err := s.aead.Open(buf[chunkNonceLen:chunkNonceLen], nonceFor(s.index, buf[:chunkNonceLen]), buf[chunkNonceLen:], s.aad)
	if err != nil {
		return fmt.Errorf("sealed stream: chunk %d %w", s.index, errStreamAuth)
	}
	s.plain = plain
	s.index++
	s.done = last
	return nil
}

func (s *StreamReader) Read(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	for len(s.plain) == 0 {
		if err := s.next(); err != nil {
			s.err = err
			return 0, err
		}
	}
	n := copy(p, s.plain)
	s.plain = s.plain[n:]
	return n, nil
}
