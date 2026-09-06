// Package ui embeds the node's observability console and its sign-in
// page, served on the --http-listen endpoint. The console's script is
// kept as several files under js/ — one per view plus a shared core
// (issue #151) — and assembled into the single page the node serves:
// several files to read and edit, one self-contained page to serve. The
// page loads no external asset and makes no second request for its own
// code, because nodes may be airgapped and because the console must
// work before anything else does.
package ui

import (
	"embed"
	"io/fs"
	"sort"
)

// FS holds the console shell, the sign-in page and the script files.
//
//go:embed index.html login.html js/*.js
var FS embed.FS

// ScriptFiles returns the console's script files in the order they are
// concatenated — name order, which the numeric prefixes make explicit:
// the core and the router come before the views that use them, and the
// boot file, which starts the polling, comes last.
func ScriptFiles() ([]string, error) {
	entries, err := fs.Glob(FS, "js/*.js")
	if err != nil {
		return nil, err
	}
	sort.Strings(entries)
	return entries, nil
}
