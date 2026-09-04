package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/term"

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
	fmt.Println(`Type SQL statements terminated by ';'; \? for help, \q to quit.`)
	return shellLoop(ctx, conn, newLineReader())
}

// lineReader reads one line of input with a prompt: a line editor with
// history when stdin and stdout are a terminal, a plain scanner for piped
// input. io.EOF ends the input.
type lineReader interface {
	ReadLine(prompt string) (string, error)
	Close()
}

func newLineReader() lineReader {
	if cli.IsTerminal(os.Stdin) && cli.IsTerminal(os.Stdout) {
		if r, err := newTermReader(); err == nil {
			return r
		}
	}
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	return &scanReader{sc: sc}
}

type scanReader struct{ sc *bufio.Scanner }

func (r *scanReader) ReadLine(prompt string) (string, error) {
	fmt.Print(prompt)
	if !r.sc.Scan() {
		fmt.Println()
		if err := r.sc.Err(); err != nil {
			return "", err
		}
		return "", io.EOF
	}
	return r.sc.Text(), nil
}

func (r *scanReader) Close() {}

// termReader is golang.org/x/term's editor over the terminal in raw mode.
// Raw mode is entered only while a line is being read and left before a
// statement runs, so results and errors print normally. History is
// cli.History, persisted to the history file.
type termReader struct {
	fd   int
	t    *term.Terminal
	hist *cli.History
}

func newTermReader() (*termReader, error) {
	hist, err := cli.LoadHistory(cli.HistoryPath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "history: %v (continuing without it)\n", err)
	}
	t := term.NewTerminal(struct {
		io.Reader
		io.Writer
	}{os.Stdin, os.Stdout}, "")
	t.History = hist
	// Without a known size the editor wraps every character; assume the
	// classic 80x24 when the terminal will not say.
	w, h, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || w <= 0 || h <= 0 {
		w, h = 80, 24
	}
	_ = t.SetSize(w, h)
	return &termReader{fd: int(os.Stdin.Fd()), t: t, hist: hist}, nil
}

func (r *termReader) ReadLine(prompt string) (string, error) {
	state, err := term.MakeRaw(r.fd)
	if err != nil {
		return "", err
	}
	r.t.SetPrompt(prompt)
	line, err := r.t.ReadLine()
	_ = term.Restore(r.fd, state)
	if err == io.EOF {
		fmt.Println() // leave the prompt line before the shell's last word
	}
	return line, err
}

func (r *termReader) Close() {
	if err := r.hist.Compact(); err != nil {
		fmt.Fprintf(os.Stderr, "history: %v\n", err)
	}
}

// shellLoop is the read-eval-print loop over any lineReader: statements
// accumulate until a ';', meta-commands act on a line of their own, and
// end of input cancels a statement in progress before it ends the shell.
func shellLoop(ctx context.Context, conn *pgx.Conn, in lineReader) error {
	defer in.Close()
	var buf strings.Builder
	prompt := "datax> "
	for {
		line, err := in.ReadLine(prompt)
		if err == io.EOF {
			if buf.Len() > 0 {
				buf.Reset()
				prompt = "datax> "
				continue
			}
			return nil
		}
		if err != nil {
			return err
		}
		if buf.Len() == 0 {
			switch cli.MetaCommand(line) {
			case cli.MetaQuit:
				return nil
			case cli.MetaHelp:
				fmt.Print(cli.HelpText)
				continue
			case cli.MetaTables:
				line = "SHOW TABLES;"
			}
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
