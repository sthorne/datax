package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/security"
)

// runCert manages the cluster's certificates: a self-signed CA, node
// certificates (mutual internode TLS + the SQL listener), and client
// certificates. Standard library crypto only.
func runCert(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf(`usage: datax cert <create-ca|create-node|create-client> [flags]`)
	}
	sub := args[0]
	fs := flag.NewFlagSet("cert "+sub, flag.ContinueOnError)
	certsDir := fs.String("certs-dir", "certs", "certificate directory")
	hosts := fs.String("hosts", "localhost,127.0.0.1", "create-node: comma-separated DNS names / IPs")
	user := fs.String("user", "root", "create-client: username (certificate CommonName)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	switch sub {
	case "create-ca":
		if err := security.CreateCA(*certsDir); err != nil {
			return err
		}
		fmt.Printf("wrote %s/ca.crt and ca.key\n", *certsDir)
	case "create-node":
		if err := security.CreateNodeCert(*certsDir, strings.Split(*hosts, ",")); err != nil {
			return err
		}
		fmt.Printf("wrote %s/node.crt and node.key (hosts: %s)\n", *certsDir, *hosts)
	case "create-client":
		if err := security.CreateClientCert(*certsDir, *user); err != nil {
			return err
		}
		fmt.Printf("wrote %s/client.%s.crt and key\n", *certsDir, *user)
	default:
		return fmt.Errorf("unknown cert subcommand %q", sub)
	}
	return nil
}
