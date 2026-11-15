package enc

import (
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
)

// Handing a root key between nodes (issues #292, #307).
//
// A node that is running under a root key held as shares has that key in
// memory. A node of the same cluster that restarts needs it and has only
// the shares' holders to ask — unless a running peer hands it over. The
// decision to hand it over is the server's (pkg/server/peerunseal.go:
// who is asking, whether it is a member, whether it is already running
// somewhere). What is here is how the key travels.
//
// It travels inside the cluster's mutual TLS, and inside that under a
// key the two ends agree for this one exchange: the asker makes an
// X25519 key pair and sends the public half; the giver makes its own,
// derives a key from the shared secret, both public halves and the
// request's own description, and seals the root key under it. Something
// that terminates the TLS between them and forwards what it reads — a
// proxy, a capture — sees the two public halves and a sealed blob. This
// second layer is against being read, not against being impersonated:
// an attacker who can ALTER the exchange can substitute its own public
// half, and what stops that is the TLS and the node certificate.

const peerUnsealMagic = "DXP1"

// PeerExchange is the asking side's half of one exchange.
type PeerExchange struct {
	priv *ecdh.PrivateKey
}

// NewPeerExchange starts an exchange; Public is what to send.
func NewPeerExchange() (*PeerExchange, error) {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &PeerExchange{priv: priv}, nil
}

// Public is the asker's public half.
func (x *PeerExchange) Public() []byte { return x.priv.PublicKey().Bytes() }

func peerSealKey(shared, askerPub, giverPub, binding []byte) ([]byte, error) {
	return exchangeKey("datax peer unseal v1", shared, askerPub, giverPub, binding)
}

// exchangeKey derives the key one exchange seals under. label keeps the
// exchanges of different purposes apart: a root key handed over for
// peer unseal can never be taken for a cluster key, nor the reverse.
func exchangeKey(label string, shared, askerPub, giverPub, binding []byte) ([]byte, error) {
	info := make([]byte, 0, 32+len(askerPub)+len(giverPub)+len(binding))
	info = append(info, label...)
	info = append(info, 0)
	info = append(info, askerPub...)
	info = append(info, giverPub...)
	info = append(info, binding...)
	return hkdf.Key(sha256.New, shared, nil, string(info), KeyLen)
}

// HandTo seals the root key this (unsealed) root holds for the asker
// whose public half is askerPub. binding is the request as both sides
// know it — which cluster, which node, which key — so an answer cannot
// be replayed to a different request. It returns the giver's public
// half and the sealed key.
func (r *ShareRoot) HandTo(askerPub, binding []byte) (giverPub, sealed []byte, err error) {
	key, err := r.rootKey()
	if err != nil {
		return nil, nil, err
	}
	pub, err := ecdh.X25519().NewPublicKey(askerPub)
	if err != nil {
		return nil, nil, fmt.Errorf("the request's exchange key is malformed: %w", err)
	}
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	shared, err := priv.ECDH(pub)
	if err != nil {
		return nil, nil, err
	}
	giverPub = priv.PublicKey().Bytes()
	k, err := peerSealKey(shared, askerPub, giverPub, binding)
	Zero(shared)
	if err != nil {
		return nil, nil, err
	}
	sealed, err = Seal(peerUnsealMagic, k, key)
	Zero(k)
	if err != nil {
		return nil, nil, err
	}
	return giverPub, sealed, nil
}

// OpenFromPeer unseals this root with a key a peer handed over in
// answer to exchange x. The root must already know which key it waits
// for (Expect): a peer's answer is taken only if it is that key, so a
// peer cannot give a node a root key of its own choosing.
func (r *ShareRoot) OpenFromPeer(x *PeerExchange, giverPub, sealed, binding []byte) error {
	pub, err := ecdh.X25519().NewPublicKey(giverPub)
	if err != nil {
		return fmt.Errorf("the peer's exchange key is malformed: %w", err)
	}
	shared, err := x.priv.ECDH(pub)
	if err != nil {
		return err
	}
	k, err := peerSealKey(shared, x.Public(), giverPub, binding)
	Zero(shared)
	if err != nil {
		return err
	}
	key, err := Unseal(peerUnsealMagic, k, sealed)
	Zero(k)
	if err != nil {
		return fmt.Errorf("the peer's answer does not open under this exchange: %w", err)
	}
	defer Zero(key)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.key != nil {
		return nil // shares got there first
	}
	if r.expect == "" {
		return errors.New("this root does not know which key it is waiting for; a peer's key is not taken on its word")
	}
	if len(key) != KeyLen || RootKeyID(key) != r.expect {
		return fmt.Errorf("the peer handed over a key that is not root key %s", r.expect)
	}
	r.key = HoldKey(key)
	r.held = map[int]Share{}
	close(r.opened)
	return nil
}
