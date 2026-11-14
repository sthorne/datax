//go:build linux

package enc

import (
	"fmt"
	"syscall"
)

const (
	madvDontDump  = 16 // MADV_DONTDUMP
	prSetDumpable = 4  // PR_SET_DUMPABLE
)

// newKeyRegion maps a region for keys, locks it and marks it to be left
// out of dumps. A region that cannot be locked is still used — it is
// still out of dumps, and still apart from the garbage-collected heap —
// and KeyMemory reports the shortfall with its remedy. Called with
// keyMem.mu held.
func newKeyRegion() *keyRegion {
	mem, err := syscall.Mmap(-1, 0, keyRegionBytes, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		keyMem.reason = fmt.Sprintf("mapping memory for keys failed: %v", err)
		return nil
	}
	r := &keyRegion{mem: mem}
	if err := syscall.Mlock(mem); err != nil {
		keyMem.reason = fmt.Sprintf("locking key memory failed (%v): raise the memory-lock limit — `ulimit -l`, or LimitMEMLOCK= in the service unit — by at least %d KiB", err, keyRegionBytes>>10)
	} else {
		r.locked = true
	}
	if err := syscall.Madvise(mem, madvDontDump); err != nil {
		if keyMem.reason == "" {
			keyMem.reason = fmt.Sprintf("excluding key memory from core dumps failed: %v", err)
		}
	} else {
		r.noDump = true
	}
	return r
}

func disableCoreDumps() error {
	if err := syscall.Setrlimit(syscall.RLIMIT_CORE, &syscall.Rlimit{}); err != nil {
		return fmt.Errorf("setting the core-file size limit to zero: %w", err)
	}
	if _, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, prSetDumpable, 0, 0); errno != 0 {
		return fmt.Errorf("marking the process not dumpable: %w", errno)
	}
	return nil
}
