package sql

import (
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// The result shapes of the statements that are not queries but return
// rows. Each is one list that both the executor and PlanColumns (the
// extended protocol's Describe) read, so what Describe promises is what
// Execute sends: a client given NoData must never then be sent rows.

// purgeColumns is ALTER TABLE ... PURGE's report: one row per replica.
func purgeColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "range_id", Type: types.Int}, {Name: "node_id", Type: types.Int},
		{Name: "live_keys", Type: types.Int}, {Name: "garbage_versions", Type: types.Int},
		{Name: "tombstones", Type: types.Int}, {Name: "intents", Type: types.Int},
		{Name: "compacted", Type: types.Bool}, {Name: "status", Type: types.String},
	}
}

// splitColumns is ALTER TABLE ... SPLIT AT / UNSPLIT's: the boundaries
// made or released.
func splitColumns() []ResultColumn {
	return []ResultColumn{{Name: "key", Type: types.String}}
}

// alterTableColumns is the shape of an ALTER TABLE form that reports
// rows, nil for the others.
func alterTableColumns(t *parser.AlterTable) []ResultColumn {
	switch {
	case t.Purge:
		return purgeColumns()
	case t.SplitAt != nil || t.UnsplitAt != nil || t.UnsplitAll:
		return splitColumns()
	}
	return nil
}

func showDatabasesColumns() []ResultColumn {
	return []ResultColumn{{Name: "database_name", Type: types.String}, {Name: "owner", Type: types.String}}
}

func showPlacementColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "database_name", Type: types.String},
		{Name: "replicas", Type: types.Int},
		{Name: "constraints", Type: types.String},
		{Name: "source", Type: types.String},
		// Compliance (issue #405): NULL when it cannot be answered.
		{Name: "satisfied", Type: types.Bool},
		{Name: "ranges", Type: types.Int},
		{Name: "misplaced_ranges", Type: types.Int},
		{Name: "unsatisfiable_ranges", Type: types.Int},
		{Name: "unattributed_ranges", Type: types.Int},
		{Name: "unknown_locality_replicas", Type: types.Int},
	}
}

func showRangesColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "range_id", Type: types.Int},
		{Name: "start_key", Type: types.String},
		{Name: "end_key", Type: types.String},
		{Name: "table_name", Type: types.String},
		{Name: "replicas", Type: types.ArrayOf(types.Int)},
		{Name: "replica_localities", Type: types.ArrayOf(types.String)},
		{Name: "status", Type: types.String},
	}
}

func showNodesColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "node_id", Type: types.Int},
		{Name: "address", Type: types.String},
		{Name: "locality", Type: types.String},
		{Name: "state", Type: types.String},
		{Name: "draining", Type: types.Bool},
		{Name: "shutting_down", Type: types.Bool},
		{Name: "last_heartbeat", Type: types.Timestamp},
	}
}

func showTablesColumns() []ResultColumn {
	return []ResultColumn{{Name: "table_name", Type: types.String}}
}

func showSequencesColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "sequence_name", Type: types.String}, {Name: "start", Type: types.Int}, {Name: "increment", Type: types.Int},
		{Name: "min_value", Type: types.Int}, {Name: "max_value", Type: types.Int}, {Name: "cycle", Type: types.Bool},
		{Name: "cache", Type: types.Int}, {Name: "last_value", Type: types.Int}, {Name: "owned_by", Type: types.String},
	}
}

func showStatsColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "table_name", Type: types.String},
		{Name: "row_count", Type: types.Int},
		{Name: "collected_at", Type: types.Timestamp},
		{Name: "column_name", Type: types.String},
		{Name: "distinct_count", Type: types.Int},
		{Name: "null_count", Type: types.Int},
	}
}

// showColumns is the shape of each SHOW form parser.Show carries (nil
// for a form it does not know).
func showColumns(t *parser.Show) []ResultColumn {
	var cols []ResultColumn
	text := func(names ...string) {
		for _, n := range names {
			cols = append(cols, ResultColumn{Name: n, Type: types.String})
		}
	}
	switch t.Kind {
	case "columns":
		text("column_name", "data_type", "is_nullable", "column_default", "indices")
		cols[2].Type = types.Bool
	case "indexes":
		text("table_name", "index_name", "non_unique", "seq_in_index", "column_name")
		cols[2].Type, cols[3].Type = types.Bool, types.Int
	case "views":
		text("view_name", "definition")
	case "create":
		text("table_name", "create_statement")
	case "users":
		text("username", "is_admin", "member_of")
		cols[1].Type = types.Bool
	case "roles":
		text("role_name", "can_login", "is_admin", "member_of")
		cols[1].Type, cols[2].Type = types.Bool, types.Bool
	case "grants":
		if t.OnRole {
			text("role_name", "member", "is_admin")
		} else {
			text("database_name", "schema_name", "relation_name", "grantee", "privilege_type", "is_grantable")
		}
	case "all":
		text("name", "setting")
	case "sessions":
		text("pid", "user_name", "database", "application_name", "client_addr", "state", "query", "backend_start", "query_start", "xact_start")
		cols[0].Type = types.Int
		for i := 7; i < 10; i++ {
			cols[i].Type = types.Timestamp
		}
	}
	return cols
}
