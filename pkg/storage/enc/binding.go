package enc

import (
	"context"
	"fmt"
	"strconv"
)

// What a wrapped store key is bound to, and what else a root may be able
// to do besides wrap (issue #308).
//
// A root that is a service — a KMS — is reachable by every node that
// holds a credential for it, so a wrapped store key copied from one
// node's data directory to another's would unwrap there as readily. A
// binding ties the wrapped key to the cluster and node it was wrapped
// for. It is carried in three places, each of which covers what the
// others cannot:
//
//   - in the entry of the wrapped-key file, under the entry's tag, where
//     it is checked by datax for every root, including the ones that
//     know nothing of it (file, exec, shares);
//   - in the call to the service, as its encryption context or
//     associated data, where the service enforces it and the operator's
//     policy can condition on it;
//   - inside what the service encrypts, for the KMS roots, so a service
//     that has no such notion (RSA wrapping) or that answers for the
//     wrong context is still found out.
//
// A store key is first wrapped before its node has an identity: the
// store that will hold the identity cannot open without the key. That
// first entry is unbound, and the node binds it as soon as it knows who
// it is.

// Binding names the cluster and node a wrapped store key belongs to. The
// zero value is "unbound".
type Binding struct {
	ClusterID string `json:"cluster"`
	NodeID    uint64 `json:"node"`
}

// IsZero reports whether b binds nothing.
func (b Binding) IsZero() bool { return b.ClusterID == "" && b.NodeID == 0 }

func (b Binding) String() string {
	if b.IsZero() {
		return "no cluster or node"
	}
	return fmt.Sprintf("cluster %s node %d", b.ClusterID, b.NodeID)
}

// bindingLabel starts every form of a binding that is authenticated, so
// what a root's key authenticates for datax cannot be mistaken for
// anything else that key is used for.
const bindingLabel = "datax store key v1"

// AAD is the binding as the bytes a service authenticates. An unbound
// key still has the label: there is always something to authenticate.
func (b Binding) AAD() []byte {
	if b.IsZero() {
		return []byte(bindingLabel)
	}
	return []byte(bindingLabel + "\x00cluster=" + b.ClusterID + "\x00node=" + strconv.FormatUint(b.NodeID, 10))
}

type bindingKey struct{}

// WithBinding returns a context that says which cluster and node a store
// key is being wrapped for, or is expected to have been wrapped for.
// WrapStoreKey binds the entry it makes to it; UnwrapStoreKeys refuses an
// entry bound to anything else.
func WithBinding(ctx context.Context, b Binding) context.Context {
	return context.WithValue(ctx, bindingKey{}, b)
}

// BindingFrom returns the binding ctx carries, zero when it carries none.
func BindingFrom(ctx context.Context) Binding {
	b, _ := ctx.Value(bindingKey{}).(Binding)
	return b
}

// ContextRoot is a root that can bind what it wraps: the binding goes to
// whatever does the wrapping, and an unwrap under a different binding
// fails there. Roots without it are bound by the entry's tag alone.
type ContextRoot interface {
	Root
	WrapContext(ctx context.Context, plaintext []byte, b Binding) ([]byte, error)
	UnwrapContext(ctx context.Context, wrapped []byte, b Binding) ([]byte, error)
}

// RootHealth is what a health check learned besides "it works".
type RootHealth struct {
	// LatestKeyID is the key id a wrap made now would carry: the root's
	// key at the service's current version. It differs from the root's
	// KeyID after the service has rotated the key and before the store
	// key has been rewrapped. Empty when the service does not say.
	LatestKeyID string
}

// HealthChecker is a root that can say whether it would work if asked: a
// running node holds its key and does not need its root, so without a
// check an unreachable root is discovered at the next restart, which is
// the worst time. The check must not touch the store key.
type HealthChecker interface {
	CheckHealth(ctx context.Context) (RootHealth, error)
}

// keyOwner is a root whose key id changes as the service rotates its key
// (it carries the version): OwnsKeyID says whether an entry's key id is
// this root's key at any version.
type keyOwner interface {
	OwnsKeyID(id string) bool
}

func wrapWith(ctx context.Context, r Root, plaintext []byte, b Binding) ([]byte, error) {
	if cr, ok := r.(ContextRoot); ok {
		return cr.WrapContext(ctx, plaintext, b)
	}
	return r.Wrap(ctx, plaintext)
}

func unwrapWith(ctx context.Context, r Root, wrapped []byte, b Binding) ([]byte, error) {
	if cr, ok := r.(ContextRoot); ok {
		return cr.UnwrapContext(ctx, wrapped, b)
	}
	return r.Unwrap(ctx, wrapped)
}
