//go:build !linux

package enc

import "errors"

// newKeyRegion: no region on this platform; keys stay on the heap and
// KeyMemory reports them as neither locked nor kept out of dumps.
func newKeyRegion() *keyRegion { return nil }

func disableCoreDumps() error {
	return errors.New("not supported on this platform")
}
