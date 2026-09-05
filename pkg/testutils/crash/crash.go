// Package crash runs a datax node as a child process of the test binary
// and kills it — SIGKILL, no shutdown — at a controlled point, then
// restarts it on the same store, for crash-consistency tests (issue
// #100): every acknowledged write must be present after the restart, and
// the raft log and the state machine must agree once replay settles.
//
// The child is the test binary itself, re-executed to run a test named
// TestCrashNodeChild that calls ChildMain; a test package that uses this
// helper declares:
//
//	func TestCrashNodeChild(t *testing.T) { crash.ChildMain(t) }
//
// The kill comes either from a fault point inside the child
// (pkg/util/faultpoint: DATAX_FAULT_POINT=raft-apply:200 kills the node
// as it applies its 200th entry) or from the parent (Kill).
package crash

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/util/faultpoint"
)

const (
	envChild    = "DATAX_CRASH_CHILD"
	envDir      = "DATAX_CRASH_DIR"
	envRPC      = "DATAX_CRASH_RPC"
	envPG       = "DATAX_CRASH_PG"
	envHTTP     = "DATAX_CRASH_HTTP"
	envMemTable = "DATAX_CRASH_MEMTABLE"
)

// Options configure a child node.
type Options struct {
	// Dir is the store directory (a fresh temp dir when empty); a restart
	// reuses it.
	Dir string
	// FaultPoint arms pkg/util/faultpoint in the child ("raft-apply:200").
	FaultPoint string
	// MemTableSize shrinks the memtable so flushes happen within seconds
	// (0 = the profile's 64 MiB).
	MemTableSize int
	// LogPath receives the child's stdout and stderr (default
	// <Dir>/child.log, appended across restarts).
	LogPath string
}

// Node is a child node.
type Node struct {
	t    testing.TB
	opts Options
	rpc  string
	pg   string
	http string
	cmd  *exec.Cmd
	exit chan error
}

// Start spawns a child node and waits until it serves SQL.
func Start(t testing.TB, opts Options) *Node {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	if opts.LogPath == "" {
		opts.LogPath = filepath.Join(opts.Dir, "child.log")
	}
	n := &Node{t: t, opts: opts}
	n.spawn(opts.FaultPoint)
	return n
}

// spawn starts the child on the node's ports and waits for readiness.
// The listeners are opened here and handed to the child as inherited
// descriptors: picking a free port and letting the child bind it a
// process start later lost a race with the packages testing alongside,
// whose nodes and clients take ephemeral ports on the same loopback —
// the child then died on "address already in use" before serving.
// The first spawn takes any free ports; a restart reopens the same ones.
func (n *Node) spawn(fault string) {
	n.t.Helper()
	logf, err := os.OpenFile(n.opts.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		n.t.Fatal(err)
	}
	var files []*os.File
	for _, addr := range []*string{&n.rpc, &n.pg, &n.http} {
		want := *addr
		if want == "" {
			want = "127.0.0.1:0"
		}
		l, err := net.Listen("tcp", want)
		if err != nil {
			n.t.Fatal(err)
		}
		*addr = l.Addr().String()
		f, err := l.(*net.TCPListener).File()
		_ = l.Close() // the socket lives on through the duplicate
		if err != nil {
			n.t.Fatal(err)
		}
		files = append(files, f)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashNodeChild$", "-test.v")
	cmd.Env = append(os.Environ(),
		envChild+"=1", envDir+"="+n.opts.Dir, envRPC+"="+n.rpc, envPG+"="+n.pg, envHTTP+"="+n.http,
		envMemTable+"="+strconv.Itoa(n.opts.MemTableSize), faultpoint.EnvVar+"="+fault)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.ExtraFiles = files // descriptors 3, 4, 5 in the child
	err = cmd.Start()
	for _, f := range files {
		_ = f.Close() // the child holds the only copies now
	}
	if err != nil {
		_ = logf.Close()
		n.t.Fatalf("starting the child node: %v", err)
	}
	n.cmd = cmd
	n.exit = make(chan error, 1)
	go func() {
		err := cmd.Wait()
		_ = logf.Close()
		n.exit <- err
	}()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if n.Exited() {
			n.t.Fatalf("child node exited before serving; its log %s ends:\n%s", n.opts.LogPath, n.logTail())
		}
		if c, err := net.DialTimeout("tcp", n.pg, 200*time.Millisecond); err == nil {
			_ = c.Close()
			if _, err := n.Status(); err == nil {
				return
			}
		}
		if time.Now().After(deadline) {
			n.t.Fatalf("child node never became ready; its log %s ends:\n%s", n.opts.LogPath, n.logTail())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// logTail is the end of the child's log, for a failure message.
func (n *Node) logTail() string {
	raw, err := os.ReadFile(n.opts.LogPath)
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) > 30 {
		lines = lines[len(lines)-30:]
	}
	return strings.Join(lines, "\n")
}

// URL is the node's SQL connection URL.
func (n *Node) URL() string { return "postgres://root@" + n.pg + "/datax?sslmode=disable" }

// HTTPURL is the node's HTTP base URL.
func (n *Node) HTTPURL() string { return "http://" + n.http }

// Dir is the store directory.
func (n *Node) Dir() string { return n.opts.Dir }

// Kill sends SIGKILL and waits for the exit.
func (n *Node) Kill() {
	n.t.Helper()
	if n.Exited() {
		return
	}
	_ = n.cmd.Process.Signal(syscall.SIGKILL)
	if err := n.WaitExit(30 * time.Second); err != nil {
		n.t.Fatal(err)
	}
}

// Exited reports whether the child has exited.
func (n *Node) Exited() bool {
	select {
	case err := <-n.exit:
		n.exit <- err
		return true
	default:
		return false
	}
}

// WaitExit waits for the child to exit (a fault point's kill).
func (n *Node) WaitExit(timeout time.Duration) error {
	select {
	case err := <-n.exit:
		n.exit <- err
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("child node still running after %s", timeout)
	}
}

// Restart starts the child again on the same store and ports, with an
// optional new fault point, and waits until it serves SQL.
func (n *Node) Restart(fault string) {
	n.t.Helper()
	if !n.Exited() {
		n.t.Fatal("Restart: the child is still running (Kill or WaitExit first)")
	}
	n.spawn(fault)
}

// RangeStatus is the part of /status a crash test checks.
type RangeStatus struct {
	RangeID      int64  `json:"range_id"`
	AppliedIndex uint64 `json:"applied_index"`
	LastIndex    uint64 `json:"last_index"`
}

// Status reads the node's ranges from /status.
func (n *Node) Status() ([]RangeStatus, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(n.HTTPURL() + "/status")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("/status: %s", resp.Status)
	}
	var doc struct {
		Ranges []RangeStatus `json:"ranges"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, err
	}
	return doc.Ranges, nil
}

// Metric reads one plain series from the node's /metrics page (0 when
// absent or unreadable).
func (n *Node) Metric(name string) float64 {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(n.HTTPURL() + "/metrics")
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, name+" ") {
			var v float64
			if _, err := fmt.Sscanf(strings.TrimPrefix(line, name+" "), "%g", &v); err == nil {
				return v
			}
		}
	}
	return 0
}

// WaitApplied waits until every range's applied index has caught up with
// its raft log (the state machine agrees with the log) and stayed there
// across two consecutive reads, returning the ranges.
func (n *Node) WaitApplied(timeout time.Duration) ([]RangeStatus, error) {
	deadline := time.Now().Add(timeout)
	var prev []RangeStatus
	for {
		ranges, err := n.Status()
		if err == nil {
			ok := len(ranges) > 0
			for _, r := range ranges {
				if r.LastIndex == 0 || r.AppliedIndex != r.LastIndex {
					ok = false
				}
			}
			if ok && prev != nil && fmt.Sprint(prev) == fmt.Sprint(ranges) {
				return ranges, nil
			}
			if ok {
				prev = ranges
			} else {
				prev = nil
			}
		}
		if time.Now().After(deadline) {
			return ranges, fmt.Errorf("ranges never settled with applied == last (%v): %v", err, ranges)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ChildMain runs the node when the test binary was spawned as a child
// (returns at once otherwise). It never returns as the child: the node
// serves until the fault point or the parent kills the process.
func ChildMain(t testing.TB) {
	if os.Getenv(envChild) != "1" {
		return
	}
	memTable, _ := strconv.Atoi(os.Getenv(envMemTable))
	// The listeners the parent opened, inherited as descriptors 3-5.
	var ls [3]net.Listener
	for i := range ls {
		f := os.NewFile(uintptr(3+i), []string{"rpc", "pg", "http"}[i])
		l, err := net.FileListener(f)
		_ = f.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "child node: inherited listener %d: %v\n", i, err)
			os.Exit(1)
		}
		ls[i] = l
	}
	cfg := server.Config{
		Dir:                 os.Getenv(envDir),
		Listen:              os.Getenv(envRPC),
		Listener:            ls[0],
		PGListen:            os.Getenv(envPG),
		PGListener:          ls[1],
		HTTPListen:          os.Getenv(envHTTP),
		HTTPListener:        ls[2],
		BootstrapSelf:       true, // a store that already has an identity reopens instead
		StorageMemTableSize: memTable,
	}
	if _, err := server.Start(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "child node: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("child node serving sql=%s http=%s fault=%q\n", cfg.PGListen, cfg.HTTPListen, faultpoint.Armed())
	select {}
}

// IsChild reports whether this process is a spawned child (tests skip
// their own bodies then).
func IsChild() bool { return os.Getenv(envChild) == "1" }
