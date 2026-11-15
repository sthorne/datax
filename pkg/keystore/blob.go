package keystore

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sthorne/datax/pkg/storage/enc"
)

// The blobs of the key service (issue #310, step 2): what
// datax_key_wrap and datax_encrypt return, and what datax_key_unwrap and
// datax_decrypt take back.
//
//	offset  size  field
//	0       1     format: BlobWrapped (a data key) or BlobEncrypted (a value)
//	1       8     the named key's id, big-endian
//	9       4     the key's version, big-endian
//	13      12    nonce
//	25      n+16  AES-256-GCM ciphertext and tag, under that version's material
//
// The associated data is a fixed label, the format byte, the id, the
// version and the caller's context (length-prefixed), so a blob opens
// only under the key and version it names, with the context it was made
// with, and only as the kind of blob it is: a wrapped data key is not
// accepted by datax_decrypt, nor a value by datax_key_unwrap. Because
// the header names the version, a rotated key keeps opening blobs made
// under its older versions, and unwrapping needs no version argument.
//
// Overhead: 41 bytes. The nonce is random; at the per-role rate limits
// the service imposes, a version stays far below the 2^32 messages a
// random-nonce GCM key is good for, and rotation starts a fresh one.

const (
	// BlobWrapped marks a data key wrapped by datax_key_wrap.
	BlobWrapped byte = 0x01
	// BlobEncrypted marks a value encrypted by datax_encrypt.
	BlobEncrypted byte = 0x02

	blobHeaderLen = 1 + 8 + 4
	blobNonceLen  = 12
	blobTagLen    = 16
	// BlobOverhead is how many bytes a blob adds to what it seals.
	BlobOverhead = blobHeaderLen + blobNonceLen + blobTagLen

	// MaxBlobPlaintext caps what one wrap or encrypt seals (64 KiB):
	// data keys and small values. Bulk data is for envelope encryption.
	MaxBlobPlaintext = 64 << 10
	// MaxBlobContext caps the context bound into a blob.
	MaxBlobContext = 64 << 10
)

// ErrBlob is the one error a blob that does not open gives, whatever the
// reason: another key's blob, another context, a damaged or forged blob,
// or a blob of the other format. Saying which would tell a caller
// probing with guessed contexts how close it came.
var ErrBlob = errors.New("the blob does not open under this key and context")

// ErrBlobTooLarge is returned for a plaintext or context over its cap.
var ErrBlobTooLarge = fmt.Errorf("at most %d bytes can be sealed, and a context is at most %d bytes", MaxBlobPlaintext, MaxBlobContext)

// BlobHeader is what a blob says about itself before it is opened.
type BlobHeader struct {
	Format  byte
	KeyID   int64
	Version int
}

// ParseBlobHeader reads a blob's header. A blob too short to be one, or
// of an unknown format, is ErrBlob.
func ParseBlobHeader(blob []byte) (BlobHeader, error) {
	if len(blob) < BlobOverhead || (blob[0] != BlobWrapped && blob[0] != BlobEncrypted) {
		return BlobHeader{}, ErrBlob
	}
	return BlobHeader{
		Format:  blob[0],
		KeyID:   int64(binary.BigEndian.Uint64(blob[1:9])),
		Version: int(binary.BigEndian.Uint32(blob[9:13])),
	}, nil
}

func blobAD(h BlobHeader, context []byte) []byte {
	ad := make([]byte, 0, 32+blobHeaderLen+8+len(context))
	ad = append(ad, "datax key service blob v1\x00"...)
	ad = append(ad, h.Format)
	ad = binary.BigEndian.AppendUint64(ad, uint64(h.KeyID))
	ad = binary.BigEndian.AppendUint32(ad, uint32(h.Version))
	ad = binary.BigEndian.AppendUint64(ad, uint64(len(context)))
	return append(ad, context...)
}

func blobGCM(material []byte) (cipher.AEAD, error) {
	if len(material) != enc.KeyLen {
		return nil, fmt.Errorf("a named key's material must be %d bytes, got %d", enc.KeyLen, len(material))
	}
	block, err := aes.NewCipher(material)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// SealBlob seals plaintext under one version of a named key (material),
// bound to context, as a blob of format.
func SealBlob(format byte, keyID int64, version int, material, plaintext, context []byte) ([]byte, error) {
	if format != BlobWrapped && format != BlobEncrypted {
		return nil, fmt.Errorf("unknown blob format %#x", format)
	}
	if len(plaintext) > MaxBlobPlaintext || len(context) > MaxBlobContext {
		return nil, ErrBlobTooLarge
	}
	if version <= 0 || int64(version) > int64(^uint32(0)) {
		return nil, fmt.Errorf("key version %d out of range", version)
	}
	gcm, err := blobGCM(material)
	if err != nil {
		return nil, err
	}
	h := BlobHeader{Format: format, KeyID: keyID, Version: version}
	out := make([]byte, blobHeaderLen+blobNonceLen, BlobOverhead+len(plaintext))
	out[0] = format
	binary.BigEndian.PutUint64(out[1:9], uint64(keyID))
	binary.BigEndian.PutUint32(out[9:13], uint32(version))
	nonce := out[blobHeaderLen:]
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(out, nonce, plaintext, blobAD(h, context)), nil
}

// OpenBlob opens a blob of format made under key keyID with material
// (the version the blob's header names), bound to context. Every way of
// not opening is ErrBlob. The caller owns the result.
func OpenBlob(format byte, keyID int64, material, blob, context []byte) ([]byte, error) {
	h, err := ParseBlobHeader(blob)
	if err != nil {
		return nil, err
	}
	if h.Format != format || h.KeyID != keyID || len(context) > MaxBlobContext {
		return nil, ErrBlob
	}
	gcm, err := blobGCM(material)
	if err != nil {
		return nil, err
	}
	nonce := blob[blobHeaderLen : blobHeaderLen+blobNonceLen]
	plain, err := gcm.Open(nil, nonce, blob[blobHeaderLen+blobNonceLen:], blobAD(h, context))
	if err != nil {
		return nil, ErrBlob
	}
	return plain, nil
}
