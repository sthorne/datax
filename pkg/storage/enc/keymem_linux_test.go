//go:build linux

package enc

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestCoreDumpHoldsNoKey crashes a child process that holds a key and
// looks for the key in what the kernel wrote. It runs where a crash
// leaves a core file in the working directory, and is skipped where
// the system sends dumps elsewhere. A marker on the ordinary heap is the
// control: it must be in the dump, or the absence of the key proves
// nothing.
func TestCoreDumpHoldsNoKey(t *testing.T) {
	if os.Getenv("DATAX_TEST_KEYMEM_CRASH") == "1" {
		keymemCrashChild()
		return
	}
	pattern, err := os.ReadFile("/proc/sys/kernel/core_pattern")
	if err != nil || strings.HasPrefix(string(pattern), "|") || strings.Contains(string(pattern), "/") {
		t.Skipf("core dumps do not land in the working directory here (core_pattern %q)", strings.TrimSpace(string(pattern)))
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run", "^TestCoreDumpHoldsNoKey$")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "DATAX_TEST_KEYMEM_CRASH=1", "GOTRACEBACK=crash")
	out, _ := cmd.CombinedOutput()
	cores, _ := filepath.Glob(filepath.Join(dir, "core*"))
	if len(cores) == 0 {
		t.Skipf("the crash left no core file (limit or sandbox); child said: %.200s", out)
	}
	raw, err := os.ReadFile(cores[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(keymemCrashMarker())) {
		t.Skip("the control marker is not in the core file: the dump is partial, so it cannot show the key is absent")
	}
	if bytes.Contains(raw, keymemCrashKey()) {
		t.Fatal("the held key is in the core dump")
	}
}

// The key and the marker are built at run time, so neither is in the
// binary's own data — which a core file also holds.
//
//go:noinline
func keymemCrashKey() []byte {
	k := make([]byte, KeyLen)
	for i := range k {
		k[i] = byte(0x41 + (i*7)%23)
	}
	return k
}

func keymemCrashMarker() string {
	return strings.Repeat("heap-control-", 3) + fmt.Sprint(len(os.Args)*0+12345)
}

var keymemSink [][]byte

func keymemCrashChild() {
	// The source is kept reachable so that wiping it is a write the
	// compiler cannot drop as dead: what is under test is the held copy,
	// and a second copy left by the test itself would hide the answer.
	src := keymemCrashKey()
	keymemSink = append(keymemSink, src)
	held := HoldKey(src)
	Zero(src)
	keymemSink = append(keymemSink, held, []byte(keymemCrashMarker()))
	// As large a core file as the hard limit allows; zero stays zero and
	// the parent skips.
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_CORE, &lim); err == nil {
		lim.Cur = lim.Max
		_ = syscall.Setrlimit(syscall.RLIMIT_CORE, &lim)
	}
	var p *int
	*p = 1 // with GOTRACEBACK=crash the runtime aborts, and the kernel dumps
}

// TestDisableCoreDumps: in a child, so this process stays as it was. The
// child turns dumps off, checks the limit and the dumpable flag, and
// crashes; no core file may appear, heap marker or not.
func TestDisableCoreDumps(t *testing.T) {
	if os.Getenv("DATAX_TEST_KEYMEM_NODUMP") == "1" {
		if err := DisableCoreDumps(); err != nil {
			fmt.Println("DISABLE-FAILED", err)
			os.Exit(3)
		}
		var lim syscall.Rlimit
		_ = syscall.Getrlimit(syscall.RLIMIT_CORE, &lim)
		dumpable, _, _ := syscall.RawSyscall(syscall.SYS_PRCTL, 3 /* PR_GET_DUMPABLE */, 0, 0)
		fmt.Printf("CORE-LIMIT %d DUMPABLE %d STATE %s\n", lim.Cur, dumpable, KeyMemory().CoreDumps)
		var p *int
		*p = 1
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run", "^TestDisableCoreDumps$")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "DATAX_TEST_KEYMEM_NODUMP=1", "GOTRACEBACK=crash")
	out, _ := cmd.CombinedOutput()
	if !bytes.Contains(out, []byte("CORE-LIMIT 0 DUMPABLE 0 STATE disabled")) {
		t.Fatalf("after DisableCoreDumps the child reported: %.300s", out)
	}
	if cores, _ := filepath.Glob(filepath.Join(dir, "core*")); len(cores) != 0 {
		t.Fatalf("a process with core dumps disabled left %v", cores)
	}
	if got := KeyMemory().CoreDumps; got != "allowed" {
		t.Fatalf("this process, which disabled nothing, reports core dumps %q", got)
	}
}
