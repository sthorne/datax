package keystore

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sthorne/datax/pkg/storage/enc"
)

// The form a named key's material takes in datax_keys: each version
// sealed with AES-256-GCM under the cluster key, with the key's id, name,
// version and kind as associated data. A version's sealed material
// therefore opens only as that version of that key: moved to another
// row, given another name, presented as another version or another kind,
// it fails authentication.

// materialMagic prefixes one sealed version.
const materialMagic = "DXK1"

// materialAD is the associated data a version is sealed under. Each field
// is length-prefixed, so no two different tuples share an encoding.
func materialAD(id int64, name string, version int, kind Kind) []byte {
	ad := make([]byte, 0, 64+len(name))
	ad = append(ad, "datax named key v1\x00"...)
	put := func(b []byte) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(b)))
		ad = append(ad, n[:]...)
		ad = append(ad, b...)
	}
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], uint64(id))
	put(v[:])
	put([]byte(name))
	binary.BigEndian.PutUint64(v[:], uint64(version))
	put(v[:])
	put([]byte(kind))
	return ad
}

func gcmFor(clusterKey []byte) (cipher.AEAD, error) {
	if len(clusterKey) != enc.KeyLen {
		return nil, fmt.Errorf("a cluster key must be %d bytes, got %d", enc.KeyLen, len(clusterKey))
	}
	block, err := aes.NewCipher(clusterKey)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// WrapMaterial seals one version of a named key under the cluster key.
func WrapMaterial(clusterKey []byte, id int64, name string, version int, kind Kind, material []byte) ([]byte, error) {
	gcm, err := gcmFor(clusterKey)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(materialMagic)+len(nonce)+len(material)+gcm.Overhead())
	out = append(out, materialMagic...)
	out = append(out, nonce...)
	return gcm.Seal(out, nonce, material, materialAD(id, name, version, kind)), nil
}

// ErrMaterialMismatch is returned when sealed material does not open as
// the key and version it is presented as, or under the cluster key given.
var ErrMaterialMismatch = errors.New("the key material does not open as this key and version under this cluster key")

// UnwrapMaterial opens one version of a named key. The caller owns the
// result and zeroes it.
func UnwrapMaterial(clusterKey []byte, id int64, name string, version int, kind Kind, sealed []byte) ([]byte, error) {
	gcm, err := gcmFor(clusterKey)
	if err != nil {
		return nil, err
	}
	if len(sealed) < len(materialMagic)+gcm.NonceSize()+gcm.Overhead() || string(sealed[:len(materialMagic)]) != materialMagic {
		return nil, fmt.Errorf("sealed key material is malformed (%d bytes)", len(sealed))
	}
	nonce := sealed[len(materialMagic) : len(materialMagic)+gcm.NonceSize()]
	plain, err := gcm.Open(nil, nonce, sealed[len(materialMagic)+gcm.NonceSize():], materialAD(id, name, version, kind))
	if err != nil {
		return nil, ErrMaterialMismatch
	}
	return plain, nil
}

// sealedVersion is one version of a key as the material column holds it.
type sealedVersion struct {
	Version int    `json:"v"`
	Sealed  []byte `json:"w"`
	// Created is when the version was made (Unix nanoseconds).
	Created int64 `json:"c"`
}

func encodeVersions(vs []sealedVersion) ([]byte, error) { return json.Marshal(vs) }

func decodeVersions(raw []byte) ([]sealedVersion, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var vs []sealedVersion
	if err := json.Unmarshal(raw, &vs); err != nil {
		return nil, fmt.Errorf("corrupt key material: %w", err)
	}
	return vs, nil
}

// rewrapVersions opens every version under from and seals it under to.
func rewrapVersions(from, to []byte, id int64, name string, kind Kind, vs []sealedVersion) ([]sealedVersion, error) {
	out := make([]sealedVersion, len(vs))
	for i, v := range vs {
		plain, err := UnwrapMaterial(from, id, name, v.Version, kind, v.Sealed)
		if err != nil {
			return nil, fmt.Errorf("key %d (%s) version %d: %w", id, name, v.Version, err)
		}
		sealed, err := WrapMaterial(to, id, name, v.Version, kind, plain)
		enc.Zero(plain)
		if err != nil {
			return nil, err
		}
		out[i] = sealedVersion{Version: v.Version, Sealed: sealed, Created: v.Created}
	}
	return out, nil
}
