package cli

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// The interactive SQL shell's pieces that do not need a terminal: the
// command history (recalled with the up and down arrows, persisted across
// sessions like psql's .psql_history), the meta-commands, and the help
// text. cmd/datax wires them to golang.org/x/term's line editor.

// HistoryMax bounds the entries kept in memory and in the file.
const HistoryMax = 1000

// HistoryPath is the history file: $DATAX_SQL_HISTORY, else
// ~/.datax_sql_history, else "" (no persistence: history lives for the
// session only).
func HistoryPath() string {
	if p := os.Getenv("DATAX_SQL_HISTORY"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".datax_sql_history")
}

// History implements golang.org/x/term.History: newest entry first when
// read back, consecutive duplicates collapsed, bounded by HistoryMax, and
// appended to a file as entries arrive so a crash loses nothing.
type History struct {
	entries []string // oldest first
	path    string
}

// LoadHistory reads the history file (missing is fine) and returns a
// History that appends new entries to it. An empty path keeps history
// in memory only.
func LoadHistory(path string) (*History, error) {
	h := &History{path: path}
	if path == "" {
		return h, nil
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return h, nil
		}
		return h, err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if line := sc.Text(); line != "" {
			h.add(line)
		}
	}
	return h, sc.Err()
}

func (h *History) add(entry string) {
	if n := len(h.entries); n > 0 && h.entries[n-1] == entry {
		return
	}
	h.entries = append(h.entries, entry)
	if len(h.entries) > HistoryMax {
		h.entries = h.entries[len(h.entries)-HistoryMax:]
	}
}

// Add records a line the user entered (term.History). Blank lines and
// exact repeats of the previous line are not worth a slot.
func (h *History) Add(entry string) {
	entry = strings.TrimRight(entry, "\r\n")
	if strings.TrimSpace(entry) == "" {
		return
	}
	if n := len(h.entries); n > 0 && h.entries[n-1] == entry {
		return
	}
	h.add(entry)
	if h.path == "" {
		return
	}
	f, err := os.OpenFile(h.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		h.path = "" // stop trying; the session keeps its memory
		return
	}
	_, _ = f.WriteString(entry + "\n")
	_ = f.Close()
}

// Len is the number of entries (term.History).
func (h *History) Len() int { return len(h.entries) }

// At returns an entry, 0 being the most recent (term.History).
func (h *History) At(idx int) string { return h.entries[len(h.entries)-1-idx] }

// Compact rewrites the history file with the bounded in-memory set, so a
// long-lived file does not grow past HistoryMax lines. Called on exit.
func (h *History) Compact() error {
	if h.path == "" {
		return nil
	}
	tmp := h.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, e := range h.entries {
		_, _ = w.WriteString(e + "\n")
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, h.path)
}

// Meta-commands the shell understands at the start of a line.
const (
	MetaNone   = ""
	MetaQuit   = "quit"
	MetaHelp   = "help"
	MetaTables = "tables"
)

// MetaCommand recognizes a shell meta-command on a line of input: \q,
// quit and exit; \?, \h and help; \dt (SHOW TABLES). Anything else is SQL.
func MetaCommand(line string) string {
	t := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ";"))
	switch strings.ToLower(t) {
	case `\q`, "quit", "exit":
		return MetaQuit
	case `\?`, `\h`, "help", `\help`:
		return MetaHelp
	case `\dt`, `\d`:
		return MetaTables
	}
	return MetaNone
}

// HelpText is what \? prints.
const HelpText = `datax sql shell

Type SQL statements terminated by ';'. A statement may span lines; the
prompt changes to '    ->' until the ';'.

Keys (interactive terminal):
  Up / Down      recall earlier lines (history is kept in ` + "`~/.datax_sql_history`" + `,
                 or $DATAX_SQL_HISTORY; the last 1000 lines)
  Left / Right, Home / End, Ctrl-A / Ctrl-E, Ctrl-W, Ctrl-U   edit the line
  Ctrl-D         quit (while typing a multi-line statement: cancel it)

Meta-commands:
  \?  \h  help   this text
  \dt            list tables (SHOW TABLES)
  \q  quit exit  leave the shell

Statements: CREATE DATABASE, DROP DATABASE [CASCADE], SHOW DATABASES, USE db,
  CREATE TABLE ... [WITH (timeseries = true, retention = '7d', shards = 8)],
  CREATE INDEX, ALTER TABLE ... ADD/DROP COLUMN | SET (retention | shards),
  DROP TABLE, INSERT [ON CONFLICT ...] / UPSERT, SELECT [... AS OF SYSTEM
  TIME ...] (joins incl. RIGHT / FULL / NATURAL / USING, GROUP BY, UNION /
  INTERSECT / EXCEPT, ORDER BY ... NULLS FIRST, LIMIT / OFFSET / FETCH),
  window functions (row_number, rank, lag/lead, sum(...) OVER ...),
  WITH [RECURSIVE] on any of them, INSERT ... SELECT, UPDATE, DELETE (all
  three take RETURNING),
  CREATE / ALTER / DROP SEQUENCE, SHOW SEQUENCES (SERIAL, identity
  columns and expression DEFAULTs: nextval, unique_rowid, gen_random_uuid),
  CHECK / UNIQUE / FOREIGN KEY constraints, ALTER TABLE ... ADD / DROP /
  VALIDATE CONSTRAINT, ALTER COLUMN SET / DROP NOT NULL, DROP TABLE CASCADE,
  COPY ... FROM STDIN, BEGIN / COMMIT / ROLLBACK / SAVEPOINT, EXPLAIN [ANALYZE],
  ANALYZE, SHOW TABLES, SHOW STATS, SHOW FUNCTIONS (the builtin
  functions: strings, math, date/time, JSON, casts, aggregates),
  CREATE USER, GRANT / REVOKE.
  Details: docs/user/sql.md (the SQL reference), docs/user/functions.md
  and docs/timeseries.md.

Command line: datax sql -e "<statement>" runs one statement and exits;
  -url, -certs-dir and -user pick the cluster and the identity.
`
