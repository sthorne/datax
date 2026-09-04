package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/cli"
)

// runSQL is a small interactive SQL shell speaking the PostgreSQL wire
// protocol (dogfooding the same path as psql and pgx applications).
func runSQL(args []string) error {
	fs := flag.NewFlagSet("sql", flag.ContinueOnError)
	url := fs.String("url", "postgres://root@127.0.0.1:26433/datax?sslmode=disable", "database URL")
	certsDir := fs.String("certs-dir", "", "certificate directory for a secure cluster: connect over TLS verified against ca.crt and authenticate with client.<user>.crt (no password)")
	user := fs.String("user", "", "username; with --certs-dir, the client certificate to present (default: the URL's user)")
	execStmt := fs.String("e", "", "execute this statement and exit")
	timeout := connectTimeoutFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := cli.SQLConfig(*url, *certsDir, *user)
	if err != nil {
		return err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	ctx := context.Background()
	var conn *pgx.Conn
	err = cli.Connect(ctx, nil, cli.SQLTarget(cfg), cli.SQLKind(cfg, *certsDir), *timeout, func(ctx context.Context) error {
		var err error
		conn, err = pgx.ConnectConfig(ctx, cfg)
		return err
	})
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	if *execStmt != "" {
		return runOne(ctx, conn, *execStmt)
	}

	fmt.Printf("datax sql shell (connected to %s as %s)\n", cli.SQLTarget(cfg), cfg.User)
	fmt.Println(`Type SQL statements terminated by ';', or \q to quit.`)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	var buf strings.Builder
	prompt := "datax> "
	for {
		fmt.Print(prompt)
		if !scanner.Scan() {
			fmt.Println()
			return scanner.Err()
		}
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if buf.Len() == 0 && (trimmed == `\q` || trimmed == "quit" || trimmed == "exit") {
			return nil
		}
		buf.WriteString(line)
		buf.WriteString("\n")
		if !strings.HasSuffix(strings.TrimSpace(buf.String()), ";") {
			prompt = "    -> "
			continue
		}
		stmtText := buf.String()
		buf.Reset()
		prompt = "datax> "
		if err := runOne(ctx, conn, stmtText); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}
}

func runOne(ctx context.Context, conn *pgx.Conn, stmtText string) error {
	start := time.Now()
	rows, err := conn.Query(ctx, stmtText)
	if err != nil {
		return err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	if len(fields) == 0 {
		// No result set: drain for the command tag.
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			return err
		}
		fmt.Printf("%s  (%.0fms)\n", rows.CommandTag(), float64(time.Since(start).Microseconds())/1000)
		return nil
	}

	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = string(f.Name)
	}
	var out [][]string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return err
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				row[i] = "NULL"
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	printTable(names, out)
	fmt.Printf("(%d rows)  (%.0fms)\n", len(out), float64(time.Since(start).Microseconds())/1000)
	return nil
}

func printTable(names []string, rows [][]string) {
	widths := make([]int, len(names))
	for i, n := range names {
		widths[i] = len(n)
	}
	for _, row := range rows {
		for i, v := range row {
			if len(v) > widths[i] {
				widths[i] = len(v)
			}
		}
	}
	line := func(parts []string) {
		cells := make([]string, len(parts))
		for i, p := range parts {
			cells[i] = fmt.Sprintf(" %-*s ", widths[i], p)
		}
		fmt.Println("|" + strings.Join(cells, "|") + "|")
	}
	sep := make([]string, len(names))
	for i := range sep {
		sep[i] = strings.Repeat("-", widths[i]+2)
	}
	fmt.Println("+" + strings.Join(sep, "+") + "+")
	line(names)
	fmt.Println("+" + strings.Join(sep, "+") + "+")
	for _, row := range rows {
		line(row)
	}
	fmt.Println("+" + strings.Join(sep, "+") + "+")
}
