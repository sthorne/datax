package parser

import "testing"

// TestCreateTableWithOptions: the trailing WITH (...) option list parses,
// and plain CREATE TABLE (including TIMESTAMP WITH TIME ZONE columns,
// which share the word WITH) is untouched.
func TestCreateTableWithOptions(t *testing.T) {
	ct := parseOne(t, `CREATE TABLE metrics (
		series TEXT,
		ts TIMESTAMPTZ,
		val FLOAT8,
		PRIMARY KEY (series, ts)
	) WITH (timeseries = true, retention = '7d', shards = 8)`).(*CreateTable)
	if ct.Options["timeseries"] != "true" || ct.Options["retention"] != "7d" || ct.Options["shards"] != "8" {
		t.Fatalf("options: %+v", ct.Options)
	}
	if len(ct.Columns) != 3 || len(ct.PrimaryKey) != 2 {
		t.Fatalf("columns/pk: %+v %+v", ct.Columns, ct.PrimaryKey)
	}

	// TRUE arrives as a keyword; it must normalize to lowercase.
	ct = parseOne(t, `CREATE TABLE u (ts TIMESTAMP WITH TIME ZONE PRIMARY KEY) WITH (timeseries = TRUE)`).(*CreateTable)
	if ct.Options["timeseries"] != "true" {
		t.Fatalf("keyword value not normalized: %+v", ct.Options)
	}

	// No WITH clause: Options stays nil.
	ct = parseOne(t, `CREATE TABLE plain (a INT PRIMARY KEY)`).(*CreateTable)
	if ct.Options != nil {
		t.Fatalf("plain table has options: %+v", ct.Options)
	}

	for _, bad := range []string{
		`CREATE TABLE t (a INT PRIMARY KEY) WITH ()`,
		`CREATE TABLE t (a INT PRIMARY KEY) WITH (timeseries)`,
		`CREATE TABLE t (a INT PRIMARY KEY) WITH (timeseries = true`,
		`CREATE TABLE t (a INT PRIMARY KEY) WITH (x = 1, x = 2)`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}
