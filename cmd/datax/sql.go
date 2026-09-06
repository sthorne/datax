package main

import (
	"bufio"
	"context"
	"errors"
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
	return shellLoop(ctx, conn, newStatementReader())
}

// statementReader reads one whole statement: the multi-line editor when
// stdin and stdout are a terminal, a plain scanner accumulating lines for
// piped input. io.EOF ends the input; cli.ErrInterrupted abandons the
// statement in progress and leaves the shell running.
type statementReader interface {
	ReadStatement() (string, error)
	Close()
}

func newStatementReader() statementReader {
	if cli.IsTerminal(os.Stdin) && cli.IsTerminal(os.Stdout) {
		if r, err := newTermReader(); err == nil {
			return r
		}
	}
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	return &scanReader{sc: sc}
}

// scanReader accumulates piped lines until the statement is complete.
// There is no editing to do, so it needs no terminal and no raw mode.
type scanReader struct{ sc *bufio.Scanner }

func (r *scanReader) ReadStatement() (string, error) {
	var buf strings.Builder
	for {
		if buf.Len() == 0 {
			fmt.Print("datax> ")
		} else {
			fmt.Print("    -> ")
		}
		if !r.sc.Scan() {
			fmt.Println()
			if err := r.sc.Err(); err != nil {
				return "", err
			}
			if buf.Len() > 0 {
				return "", io.EOF // an unfinished statement is discarded
			}
			return "", io.EOF
		}
		if buf.Len() > 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(r.sc.Text())
		if cli.StatementComplete(buf.String()) {
			return buf.String(), nil
		}
	}
}

func (r *scanReader) Close() {}

// termReader is the shell's own multi-line editor (pkg/cli, issue #175)
// over the terminal in raw mode. Raw mode is entered only while a
// statement is being read and left before it runs, so results and errors
// print normally. x/term is still what puts the terminal in raw mode and
// reports its size; the editing, the key decoding and the history are
// ours, because a single-line editor cannot join a line to the one above
// it and does not decode Ctrl+arrow.
type termReader struct {
	fd   int
	ed   *cli.Editor
	hist *cli.History
}

func newTermReader() (*termReader, error) {
	hist, err := cli.LoadHistory(cli.HistoryPath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "history: %v (continuing without it)\n", err)
	}
	ed := cli.NewEditor(os.Stdin, os.Stdout)
	ed.History = hist
	ed.Complete = cli.StatementComplete
	return &termReader{fd: int(os.Stdin.Fd()), ed: ed, hist: hist}, nil
}

func (r *termReader) ReadStatement() (string, error) {
	state, err := term.MakeRaw(r.fd)
	if err != nil {
		return "", err
	}
	// The width is read per statement so a resized window takes effect
	// without restarting the shell.
	if w, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil && w > 0 {
		r.ed.Width = w
	} else {
		r.ed.Width = 80
	}
	text, err := r.ed.ReadStatement()
	_ = term.Restore(r.fd, state)
	return text, err
}

func (r *termReader) Close() {
	if err := r.hist.Compact(); err != nil {
		fmt.Fprintf(os.Stderr, "history: %v\n", err)
	}
}

// shellLoop is the read-eval-print loop over any statementReader. The
// reader hands back a whole statement — the editor composes it across as
// many lines as it takes — and meta-commands are recognised before it is
// sent to the server.
func shellLoop(ctx context.Context, conn *pgx.Conn, in statementReader) error {
	defer in.Close()
	for {
		stmtText, err := in.ReadStatement()
		if errors.Is(err, cli.ErrInterrupted) {
			continue // ^C abandons the statement, not the shell
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		trimmed := strings.TrimSpace(stmtText)
		if trimmed == "" {
			continue
		}
		switch cli.MetaCommand(trimmed) {
		case cli.MetaQuit:
			return nil
		case cli.MetaHelp:
			fmt.Print(cli.HelpText)
			continue
		case cli.MetaTables:
			stmtText = "SHOW TABLES;"
		}
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
