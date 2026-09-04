// Command datax is the datax database server and CLI.
package main

import (
	"fmt"
	"os"

	dataxversion "github.com/sthorne/datax/pkg/version"
)

// version is the build stamp, set by the build workflow via
// -ldflags "-X main.version=..." (an exact git tag, or
// "v<release>+<commit>"); a plain `go build` reports the release with a
// -dev suffix.
var version = "v" + dataxversion.Release + "-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "version", "--version", "-v":
		fmt.Printf("datax %s (cluster protocol v%d, supports v%d..v%d)\n", version, dataxversion.Current, dataxversion.MinSupported, dataxversion.Current)
	case "init":
		err = runInit(os.Args[2:])
	case "start":
		err = runStart(os.Args[2:])
	case "demo":
		err = runDemo(os.Args[2:])
	case "sql":
		err = runSQL(os.Args[2:])
	case "bench":
		err = runBench(os.Args[2:])
	case "cert":
		err = runCert(os.Args[2:])
	case "backup":
		err = runBackupCmd(os.Args[2:])
	case "restore":
		err = runRestoreCmd(os.Args[2:])
	case "debug":
		err = runDebug(os.Args[2:])
	case "help", "--help", "-h":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `datax — a distributed ACID SQL database

Usage:
  datax init   --dir <dir> [flags]     bootstrap a new cluster's first node and start it
  datax start  --dir <dir> --join <addr> [flags]
                                       start a node, joining an existing cluster
  datax demo   [--nodes N]             run an in-process demo cluster across racks
  datax sql    [--url <postgres-url>] [--certs-dir <dir> --user <name>]
                                       interactive SQL shell (or -e "<statement>")
  datax bench  <kv|bank|ingest|timeseries> [flags]  benchmark a running cluster over pgwire
  datax cert   <create-ca|create-node|create-client>
                                       manage TLS certificates (secure mode)
  datax backup  --dest <dir>           write a consistent cluster backup (on the serving node)
  datax restore --src <dir[,dir...]>   restore a backup chain into an empty cluster
  datax debug  <subcommand>            cluster inspection and admin commands
  datax version                        print version

Run "datax <command> -h" for command flags.
`)
}
