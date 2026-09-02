package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// runBackupCmd is `datax backup`: ask a node to write a consistent backup
// of the whole cluster to a directory on ITS filesystem.
func runBackupCmd(args []string) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:26257", "RPC address of any cluster node")
	dest := fs.String("dest", "", "destination directory ON THE SERVING NODE")
	basePath := fs.String("base", "", "prior backup directory (on the node) to take an incremental against")
	allowPlaintext := fs.Bool("allow-plaintext", false, "allow plaintext backup files from an encrypted store")
	certsDir := fs.String("certs-dir", "", "certificate directory for a secure cluster (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "username whose client certificate authenticates this call (with --certs-dir); must be an admin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dest == "" {
		return fmt.Errorf("backup requires --dest")
	}
	resp, err := adminCall(*addr, *certsDir, *certUser, cluster.AdminRequest{
		Op: "backup", Path: *dest, BasePath: *basePath, AllowPlaintext: *allowPlaintext,
	})
	if err != nil {
		return err
	}
	printBackupSummary("backup", resp.Backup)
	return nil
}

// runRestoreCmd is `datax restore`: ask a node to apply a backup chain
// (full first, then incrementals) from directories on ITS filesystem into
// this — empty — cluster.
func runRestoreCmd(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:26257", "RPC address of any cluster node")
	src := fs.String("src", "", "comma-separated backup directories ON THE SERVING NODE, full backup first")
	certsDir := fs.String("certs-dir", "", "certificate directory for a secure cluster (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "username whose client certificate authenticates this call (with --certs-dir); must be an admin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *src == "" {
		return fmt.Errorf("restore requires --src")
	}
	resp, err := adminCall(*addr, *certsDir, *certUser, cluster.AdminRequest{Op: "restore", Paths: strings.Split(*src, ",")})
	if err != nil {
		return err
	}
	printBackupSummary("restored (fresh export)", resp.Backup)
	return nil
}

func adminCall(addr, certsDir, certUser string, req cluster.AdminRequest) (cluster.AdminResponse, error) {
	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	if certsDir != "" {
		tlsCfg, err := security.LoadClientTLS(certsDir, certUser)
		if err != nil {
			return cluster.AdminResponse{}, err
		}
		trans.SetTLS(tlsCfg)
	}
	// Backups and restores move real data; give them time.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	var resp cluster.AdminResponse
	if err := trans.Call(ctx, addr, "admin", req, &resp); err != nil {
		return resp, err
	}
	if resp.Error != "" {
		return resp, fmt.Errorf("%s", resp.Error)
	}
	return resp, nil
}

func printBackupSummary(what string, sum *cluster.BackupSummary) {
	if sum == nil {
		fmt.Println("done")
		return
	}
	if sum.Path != "" {
		fmt.Printf("%s: %s (cluster %s)\n", what, sum.Path, sum.ClusterID)
	} else {
		fmt.Printf("%s (cluster %s)\n", what, sum.ClusterID)
	}
	for _, t := range sum.Tables {
		fmt.Printf("  table %-20s id=%-4d records=%-8d bytes=%-10d sha256=%s\n",
			t.Name, t.ID, t.Records, t.Bytes, t.SHA256[:16])
	}
	fmt.Printf("  users: %d\n", sum.Users)
}
