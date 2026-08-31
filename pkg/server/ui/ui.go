// Package ui embeds the node's single-file observability dashboard,
// served at / on the --http-listen endpoint. The page is fully
// self-contained (no external assets — nodes may be airgapped) and reads
// only the read-only /api/cluster and /metrics endpoints.
package ui

import "embed"

// FS holds the dashboard page.
//
//go:embed index.html
var FS embed.FS
