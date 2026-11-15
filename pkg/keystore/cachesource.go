package keystore

import (
	"context"

	"github.com/sthorne/datax/pkg/keystore/keycache"
)

// CacheSource is the store as a keycache.Source: what a gateway's key
// cache unwraps material and checks generations through.
func (s *Store) CacheSource() keycache.Source { return cacheSource{s} }

type cacheSource struct{ s *Store }

func (c cacheSource) Unwrap(ctx context.Context, id int64, version int) (*keycache.Material, error) {
	m, err := c.s.Unwrap(ctx, id, version)
	if err != nil {
		return nil, err
	}
	return &keycache.Material{ID: m.ID, Version: m.Version, Generation: m.Generation, Key: m.Key}, nil
}

func (c cacheSource) Generation(ctx context.Context, id int64) (int64, error) {
	return c.s.Generation(ctx, id)
}
