package enc

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"unsafe"
)

func TestHeldKeysAreCopiesZeroedOnRelease(t *testing.T) {
	before := KeyMemory().Held
	src := bytes.Repeat([]byte{0xA5}, KeyLen)
	a, b := HoldKey(src), HoldKey(src)
	if &a[0] == &src[0] || &a[0] == &b[0] {
		t.Fatal("a held key shares memory with its source or with another")
	}
	if !bytes.Equal(a, src) || !bytes.Equal(b, src) {
		t.Fatal("a held key differs from its source")
	}
	if got := KeyMemory().Held; got != before+2 {
		t.Fatalf("held %d, want %d", got, before+2)
	}
	// An append to a held key copies out; it does not run into the
	// neighbouring key.
	grown := append(a, 1)
	if &grown[0] == &a[0] || !bytes.Equal(b, src) {
		t.Fatal("append to a held key wrote into held memory")
	}
	alias := a[:KeyLen]
	ReleaseKey(a)
	if !bytes.Equal(alias, make([]byte, KeyLen)) {
		t.Fatal("a released key was not zeroed")
	}
	ReleaseKey(a) // twice is harmless
	ReleaseKey(nil)
	ReleaseKey(b)
	if got := KeyMemory().Held; got != before {
		t.Fatalf("held %d after release, want %d", got, before)
	}
	// The slot is used again rather than the region growing.
	c := HoldKey(src)
	if &c[0] != &alias[0] && runtime.GOOS == "linux" {
		t.Error("a released slot was not reused")
	}
	ReleaseKey(c)
	// More keys than a region holds spill into another, all distinct.
	var many [][]byte
	seen := map[*byte]bool{}
	for i := 0; i < keySlots+10; i++ {
		k := HoldKey(src)
		if seen[&k[0]] {
			t.Fatal("two held keys share a slot")
		}
		seen[&k[0]] = true
		many = append(many, k)
	}
	for _, k := range many {
		ReleaseKey(k)
	}
	// Not a key's length: held on the heap, still zeroed.
	odd := HoldKey([]byte("short"))
	ReleaseKey(odd)
	if !bytes.Equal(odd, make([]byte, 5)) {
		t.Fatal("an odd-length buffer was not zeroed")
	}
}

// smapsFlags returns the VmFlags of the mapping that holds addr.
func smapsFlags(t *testing.T, addr uintptr) string {
	t.Helper()
	f, err := os.Open("/proc/self/smaps")
	if err != nil {
		t.Skipf("no /proc/self/smaps: %v", err)
	}
	defer f.Close()
	in := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		var lo, hi uintptr
		if n, _ := fmt.Sscanf(line, "%x-%x ", &lo, &hi); n == 2 && strings.Contains(strings.SplitN(line, " ", 2)[0], "-") {
			in = addr >= lo && addr < hi
			continue
		}
		if in && strings.HasPrefix(line, "VmFlags:") {
			return line
		}
	}
	t.Fatalf("no mapping holds %#x", addr)
	return ""
}

// TestKeyMemoryIsLockedAndLeftOutOfDumps reads the kernel's own account
// of the pages a held key is on: "lo" is locked, "dd" is do-not-dump.
func TestKeyMemoryIsLockedAndLeftOutOfDumps(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("key memory is locked on Linux only")
	}
	k := HoldKey(bytes.Repeat([]byte{7}, KeyLen))
	defer ReleaseKey(k)
	st := KeyMemory()
	flags := smapsFlags(t, uintptr(unsafe.Pointer(&k[0])))
	has := func(f string) bool { return strings.Contains(flags+" ", " "+f+" ") }
	if !has("dd") {
		t.Errorf("key memory is not marked do-not-dump: %s", flags)
	}
	if st.NoDump != has("dd") {
		t.Errorf("KeyMemory says NoDump=%t; the kernel says %s", st.NoDump, flags)
	}
	if st.Locked != has("lo") {
		t.Errorf("KeyMemory says Locked=%t; the kernel says %s", st.Locked, flags)
	}
	if !st.Locked {
		// A memory-lock limit of zero is a property of where the test
		// runs; the status must then carry the remedy.
		if !strings.Contains(st.Reason, "memory-lock limit") {
			t.Errorf("unlocked key memory without a remedy: %q", st.Reason)
		}
		t.Skipf("key memory could not be locked here: %s", st.Reason)
	}
	// A key on the ordinary heap, for contrast, is neither.
	heap := bytes.Repeat([]byte{9}, 1<<20)
	hf := smapsFlags(t, uintptr(unsafe.Pointer(&heap[0])))
	if strings.Contains(hf+" ", " dd ") {
		t.Errorf("the ordinary heap reads as do-not-dump (%s): the check above proves nothing", hf)
	}
	runtime.KeepAlive(heap)
}
