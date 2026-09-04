package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHistoryPersistsAndBounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hist")
	h, err := LoadHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	h.Add("SELECT 1;")
	h.Add("SELECT 1;") // consecutive repeat collapses
	h.Add("   ")       // blank is dropped
	h.Add("SELECT 2;")
	if h.Len() != 2 || h.At(0) != "SELECT 2;" || h.At(1) != "SELECT 1;" {
		t.Fatalf("history: len=%d first=%q", h.Len(), h.At(0))
	}

	// A new shell reads the file back, newest first.
	h2, err := LoadHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if h2.Len() != 2 || h2.At(0) != "SELECT 2;" {
		t.Fatalf("reloaded history: len=%d first=%q", h2.Len(), h2.At(0))
	}

	// The bound holds in memory and, after Compact, in the file.
	for i := 0; i < HistoryMax+50; i++ {
		h2.Add("SELECT " + strings.Repeat("x", i%7) + ";")
	}
	if h2.Len() != HistoryMax {
		t.Fatalf("len %d, want %d", h2.Len(), HistoryMax)
	}
	if err := h2.Compact(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	if n := strings.Count(string(raw), "\n"); n != HistoryMax {
		t.Fatalf("file has %d lines after Compact, want %d", n, HistoryMax)
	}
	if h2.At(0) != strings.TrimRight(strings.Split(string(raw), "\n")[HistoryMax-1], "\n") {
		t.Fatal("file order is not oldest first")
	}

	// No path: memory only, no error.
	h3, err := LoadHistory("")
	if err != nil || h3.Len() != 0 {
		t.Fatal(err)
	}
	h3.Add("x")
	if h3.Len() != 1 || h3.Compact() != nil {
		t.Fatal("memory-only history")
	}
}

func TestHistoryPath(t *testing.T) {
	t.Setenv("DATAX_SQL_HISTORY", "/tmp/custom-hist")
	if HistoryPath() != "/tmp/custom-hist" {
		t.Fatal(HistoryPath())
	}
	t.Setenv("DATAX_SQL_HISTORY", "")
	if p := HistoryPath(); !strings.HasSuffix(p, ".datax_sql_history") {
		t.Fatal(p)
	}
}

func TestMetaCommand(t *testing.T) {
	cases := map[string]string{
		`\q`: MetaQuit, "quit": MetaQuit, " exit ; ": MetaQuit, "QUIT": MetaQuit,
		`\?`: MetaHelp, `\h`: MetaHelp, "help": MetaHelp, "HELP;": MetaHelp,
		`\dt`: MetaTables, `\d`: MetaTables,
		"SELECT 1;": MetaNone, "help me": MetaNone, `\x`: MetaNone,
	}
	for in, want := range cases {
		if got := MetaCommand(in); got != want {
			t.Errorf("MetaCommand(%q) = %q, want %q", in, got, want)
		}
	}
	if !strings.Contains(HelpText, `\dt`) || !strings.Contains(HelpText, "Up / Down") {
		t.Fatal("help text lacks the basics")
	}
}
