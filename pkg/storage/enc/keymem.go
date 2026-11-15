package enc

import (
	"sync"
)

// Where key material lives while the process runs (issues #292, #309).
//
// A key in an ordinary Go slice is in memory the kernel may write to
// swap and will write to a core dump. The long-lived keys — a store
// key, the data keys of a registry, a root key rebuilt from shares —
// are instead held in memory set aside for them: locked, so it is never
// swapped; left out of core dumps; and zeroed when a key is released.
//
// This narrows where a key can be found. It does not make the process
// safe to inspect: a cipher built from a key holds its own expanded
// copy on the ordinary heap, a key passes through a stack on its way
// anywhere, and anyone who can read this process's memory can read it
// all. DisableCoreDumps closes the widest of those doors for the whole
// process; the rest is the limit of what a process can do about its
// own memory, and docs/encryption.md says so.

// keySlots is how many keys one region holds: four pages' worth, which
// is far more than a node has (one store key, one data key per open of
// the store, a root key or two) and well inside the smallest
// RLIMIT_MEMLOCK a distribution ships.
const (
	keyRegionBytes = 16 << 10
	keySlots       = keyRegionBytes / KeyLen
)

// keyRegion is one stretch of memory for keys.
type keyRegion struct {
	mem    []byte
	used   [keySlots]bool
	inUse  int
	locked bool
	noDump bool
}

var keyMem struct {
	mu      sync.Mutex
	regions []*keyRegion
	// held counts keys held; heap counts those that had to fall back to
	// the ordinary heap (no region could be had).
	held, heap int
	// reason is why memory could not be locked or kept out of dumps,
	// when it could not.
	reason string
}

// KeyMemoryStatus says how the process holds its keys.
type KeyMemoryStatus struct {
	// Locked: every key held is in memory that cannot be swapped.
	Locked bool `json:"locked"`
	// NoDump: every key held is in memory left out of core dumps.
	NoDump bool `json:"no_dump"`
	// Held is how many keys are held.
	Held int `json:"held"`
	// Reason says why, when either is false.
	Reason string `json:"reason,omitempty"`
	// CoreDumps is "disabled" once DisableCoreDumps has succeeded, and
	// otherwise why not.
	CoreDumps string `json:"core_dumps"`
}

// HoldKey returns a copy of key in memory kept for keys. The caller
// owns it until ReleaseKey; the argument is not changed. A slice that
// is not a key's length is copied to the ordinary heap, as is any key
// when the platform gives no such memory — KeyMemory then says so.
func HoldKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	keyMem.mu.Lock()
	defer keyMem.mu.Unlock()
	keyMem.held++
	if len(key) == KeyLen {
		for _, r := range keyMem.regions {
			if b := r.take(); b != nil {
				copy(b, key)
				return b
			}
		}
		if r := newKeyRegion(); r != nil {
			keyMem.regions = append(keyMem.regions, r)
			b := r.take()
			copy(b, key)
			return b
		}
	}
	keyMem.heap++
	return append([]byte(nil), key...)
}

func (r *keyRegion) take() []byte {
	if r.inUse == keySlots {
		return nil
	}
	for i := range r.used {
		if !r.used[i] {
			r.used[i] = true
			r.inUse++
			// The capacity is the slot's: an append to a held key copies
			// it out rather than running into its neighbour.
			return r.mem[i*KeyLen : (i+1)*KeyLen : (i+1)*KeyLen]
		}
	}
	return nil
}

// ReleaseKey zeroes a key and, when HoldKey returned it, gives its
// memory back. Releasing nil, or a key twice, is harmless.
func ReleaseKey(key []byte) {
	if len(key) == 0 {
		return
	}
	Zero(key)
	keyMem.mu.Lock()
	defer keyMem.mu.Unlock()
	for _, r := range keyMem.regions {
		if i, ok := r.slotOf(key); ok {
			if r.used[i] {
				r.used[i] = false
				r.inUse--
				keyMem.held--
			}
			return
		}
	}
	if keyMem.heap > 0 {
		keyMem.heap--
		keyMem.held--
	}
}

func (r *keyRegion) slotOf(key []byte) (int, bool) {
	if len(key) != KeyLen || len(r.mem) == 0 {
		return 0, false
	}
	for i := 0; i < keySlots; i++ {
		if &r.mem[i*KeyLen] == &key[0] {
			return i, true
		}
	}
	return 0, false
}

// Zero overwrites b: for the short-lived copies of a key that cannot
// live in held memory.
func Zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// KeyMemory reports how the keys held now are protected.
func KeyMemory() KeyMemoryStatus {
	keyMem.mu.Lock()
	defer keyMem.mu.Unlock()
	st := KeyMemoryStatus{Locked: keyMem.heap == 0, NoDump: keyMem.heap == 0, Held: keyMem.held, Reason: keyMem.reason, CoreDumps: coreDumpState()}
	for _, r := range keyMem.regions {
		if r.inUse == 0 {
			continue
		}
		st.Locked = st.Locked && r.locked
		st.NoDump = st.NoDump && r.noDump
	}
	if keyMem.heap > 0 && st.Reason == "" {
		st.Reason = "this platform gives no memory that can be locked or kept out of core dumps"
	}
	if st.Locked && st.NoDump {
		st.Reason = ""
	}
	return st
}

var coreDumps struct {
	mu    sync.Mutex
	state string
}

func coreDumpState() string {
	coreDumps.mu.Lock()
	defer coreDumps.mu.Unlock()
	if coreDumps.state == "" {
		return "allowed"
	}
	return coreDumps.state
}

// DisableCoreDumps stops the process from being dumped: no core file on
// a crash, and no reading of its memory by another process of the same
// user. It is for the whole process, which is the point — the copies of
// a key that held memory cannot cover are covered by this.
func DisableCoreDumps() error {
	err := disableCoreDumps()
	coreDumps.mu.Lock()
	defer coreDumps.mu.Unlock()
	if err != nil {
		coreDumps.state = "allowed: " + err.Error()
		return err
	}
	coreDumps.state = "disabled"
	return nil
}
