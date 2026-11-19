package sql

import (
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// VACUUM (PG-O-003). datax keeps no dead tuples to reclaim in place: MVCC
// garbage collection drops versions older than a table's gc_ttl on its
// own, and ALTER TABLE ... PURGE drops history on demand. So VACUUM, in
// every form, checks what it names and succeeds; with ANALYZE it also
// refreshes the planner statistics the ANALYZE statement collects.
//
// What PostgreSQL does around it is kept: VACUUM cannot run inside a
// transaction block (25001, checked by the caller), a table it names
// must exist (42P01), a view is skipped with a warning, and a table the
// role does not own is skipped with PostgreSQL's warning rather than
// failing the statement.

// execVacuum runs VACUUM outside any transaction block.
func (s *Session) execVacuum(ctx context.Context, v *parser.Vacuum) (*Result, *Error) {
	res := &Result{Tag: "VACUUM"}
	var analyze []string
	admin := false
	err := s.db.RunTxn(ctx, "vacuum", func(ctx context.Context, txn *kvclient.Txn) error {
		res.Warnings, analyze = nil, nil
		var err error
		if admin, err = s.isAdmin(ctx, txn); err != nil {
			return err
		}
		for _, vt := range v.Tables {
			d, err := s.lookup(ctx, txn, vt.Name)
			if err != nil {
				return err
			}
			if d.Virtual != "" || d.IsView() {
				res.Warnings = append(res.Warnings, fmt.Sprintf("skipping %q --- cannot vacuum non-tables or special system tables", d.Name))
				continue
			}
			for _, c := range vt.Columns {
				if _, ok := d.Col(c); !ok {
					return newErrf(CodeUndefinedColumn, "column %q of relation %q does not exist", c, d.Name)
				}
			}
			owner, err := s.isOwner(ctx, txn, d.Owner)
			if err != nil {
				return err
			}
			switch {
			case !owner:
				res.Warnings = append(res.Warnings, fmt.Sprintf("permission denied to vacuum %q, skipping it", d.Name))
			case v.Analyze && !admin:
				// datax's statistics refresh is an admin's (ANALYZE).
				res.Warnings = append(res.Warnings, fmt.Sprintf("permission denied to analyze %q, skipping it: refreshing statistics takes the admin role", d.Name))
			case v.Analyze:
				analyze = append(analyze, vt.Name)
			}
		}
		return nil
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	if !v.Analyze {
		return res, nil
	}
	if len(v.Tables) == 0 {
		if !admin {
			res.Warnings = append(res.Warnings, "permission denied to analyze, skipping it: refreshing statistics takes the admin role")
			return res, nil
		}
		if _, serr := s.execAnalyze(ctx, &parser.Analyze{}); serr != nil {
			return nil, serr
		}
		return res, nil
	}
	for _, name := range analyze {
		if _, serr := s.execAnalyze(ctx, &parser.Analyze{Table: name}); serr != nil {
			return nil, serr
		}
	}
	return res, nil
}
