package pgwire

import (
	"reflect"
	"testing"
)

// TestStartupSettings (PG-T-001): the settings a startup packet
// carries — its own parameters and options' -c name=value — in a fixed
// order, without the connection's user and database.
func TestStartupSettings(t *testing.T) {
	got := startupSettings(map[string]string{
		"user": "root", "database": "datax", "TimeZone": "America/New_York", "application_name": "app",
		"options": "-c statement_timeout=5s -cDateStyle=ISO --search-path=public",
	})
	want := [][2]string{
		{"TimeZone", "America/New_York"}, {"application_name", "app"},
		{"statement_timeout", "5s"}, {"DateStyle", "ISO"}, {"search_path", "public"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("startupSettings = %v, want %v", got, want)
	}
}
