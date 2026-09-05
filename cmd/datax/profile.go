package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/cli"
	"github.com/sthorne/datax/pkg/security"
)

// Profiles (issue #100): the node serves net/http/pprof under
// /debug/pprof/ on its HTTP port (admin-gated); `datax debug profile`
// fetches one to a file, and `datax bench --server-profile` pulls a CPU
// profile for a run's duration.

// profileKinds maps a --kind to its pprof path; cpu and trace take a
// duration.
var profileKinds = map[string]string{
	"cpu":       "/debug/pprof/profile",
	"trace":     "/debug/pprof/trace",
	"heap":      "/debug/pprof/heap",
	"allocs":    "/debug/pprof/allocs",
	"mutex":     "/debug/pprof/mutex",
	"block":     "/debug/pprof/block",
	"goroutine": "/debug/pprof/goroutine",
}

// httpClientFlags are the flags every HTTP-port command takes.
type httpClientFlags struct {
	certsDir, certUser *string
	insecureTLS        *bool
	timeout            *time.Duration
}

func addHTTPClientFlags(fs *flag.FlagSet) httpClientFlags {
	return httpClientFlags{
		insecureTLS: fs.Bool("insecure-skip-verify", false, "skip TLS certificate verification"),
		certsDir:    fs.String("certs-dir", "", "certificate directory for a secure cluster (presents client.<user>.crt)"),
		certUser:    fs.String("user", "root", "username whose client certificate authenticates this call (with --certs-dir)"),
		timeout:     connectTimeoutFlag(fs),
	}
}

// client builds the HTTP client and names the connection kind for
// progress messages.
func (f httpClientFlags) client() (*http.Client, string, error) {
	return newHTTPClient(*f.certsDir, *f.certUser, *f.insecureTLS)
}

func newHTTPClient(certsDir, certUser string, insecureTLS bool) (*http.Client, string, error) {
	client := &http.Client{}
	tlsCfg := &tls.Config{}
	if certsDir != "" {
		var err error
		if tlsCfg, err = security.LoadClientTLS(certsDir, certUser); err != nil {
			return nil, "", err
		}
	}
	tlsCfg.InsecureSkipVerify = insecureTLS
	if certsDir != "" || insecureTLS {
		client.Transport = &http.Transport{TLSClientConfig: tlsCfg}
	}
	kind := "http"
	if certsDir != "" {
		kind = "http, TLS with client certificate"
	}
	return client, kind, nil
}

// httpFetch GETs url and streams the body to w. The connect phase runs
// under timeout with the CLI's progress reporting; the transfer itself
// (a 30 s CPU profile) is bounded by bodyTimeout (0 = none).
func httpFetch(client *http.Client, kind, url string, timeout, bodyTimeout time.Duration, w io.Writer) error {
	var resp *http.Response
	err := cli.Connect(context.Background(), nil, url, kind, timeout, func(ctx context.Context) error {
		// The request's own context outlives the connect phase: the body
		// may take longer than the connect timeout to arrive.
		bctx := context.Background()
		var cancel context.CancelFunc = func() {}
		if bodyTimeout > 0 {
			bctx, cancel = context.WithTimeout(bctx, bodyTimeout)
		}
		req, err := http.NewRequestWithContext(bctx, "GET", url, nil)
		if err != nil {
			cancel()
			return err
		}
		r, err := client.Do(req)
		if err != nil {
			cancel()
			return err
		}
		if r.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
			_ = r.Body.Close()
			cancel()
			return cli.ConnectedError{Err: fmt.Errorf("%s: %s", r.Status, strings.TrimSpace(string(body)))}
		}
		resp = r
		go func() { <-bctx.Done(); cancel() }()
		return nil
	})
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, err = io.Copy(w, resp.Body)
	return err
}

// runDebugProfile fetches a profile from a node's HTTP port.
func runDebugProfile(args []string) error {
	fs := flag.NewFlagSet("debug profile", flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:8080", "a node's HTTP base URL (the port serving /status)")
	kind := fs.String("kind", "cpu", "profile: cpu, heap, allocs, mutex, block, goroutine, or trace")
	seconds := fs.Int("seconds", 30, "cpu and trace: how long to sample")
	out := fs.String("out", "", "output file (default <kind>.pprof, or trace.out)")
	hf := addHTTPClientFlags(fs)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: datax debug profile --kind cpu|heap|mutex|block|goroutine|trace [--seconds 30] --url http://host:8080 [--certs-dir ... --user ...]\n\n")
		fmt.Fprintf(fs.Output(), "Fetches a pprof profile from the node (admin role required in secure mode).\n")
		fmt.Fprintf(fs.Output(), "Inspect with `go tool pprof <file>` (or `go tool trace` for a trace).\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	path, ok := profileKinds[*kind]
	if !ok {
		return fmt.Errorf("unknown profile kind %q (want cpu, heap, allocs, mutex, block, goroutine, or trace)", *kind)
	}
	if *out == "" {
		*out = *kind + ".pprof"
		if *kind == "trace" {
			*out = "trace.out"
		}
	}
	client, connKind, err := hf.client()
	if err != nil {
		return err
	}
	target := strings.TrimSuffix(*url, "/") + path
	var bodyTimeout time.Duration
	if *kind == "cpu" || *kind == "trace" {
		target += fmt.Sprintf("?seconds=%d", *seconds)
		bodyTimeout = time.Duration(*seconds)*time.Second + 30*time.Second
		fmt.Printf("sampling %s for %ds...\n", *kind, *seconds)
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	if err := httpFetch(client, connKind, target, *hf.timeout, bodyTimeout, f); err != nil {
		_ = f.Close()
		_ = os.Remove(*out)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	st, _ := os.Stat(*out)
	fmt.Printf("wrote %s (%d bytes)\n", *out, st.Size())
	return nil
}
