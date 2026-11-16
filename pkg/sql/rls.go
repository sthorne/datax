package sql

import (
	"context"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Row-level security (issue #294). A table with row security enabled
// carries policies in its descriptor; a statement run by a role that is
// not exempt reads and targets only the rows its policies' USING
// expressions admit, and writes only rows their WITH CHECK expressions
// admit. With no policy that applies to the role, it reads and writes
// nothing (default deny).
//
// The policy is applied where the statement is planned, as WHERE
// conjuncts. Every read of a table's rows goes through one filter —
// matchesWhere over the conjuncts the fetch was handed (executePlan, the
// streaming iterator, the inverted scan) — and no read is answered from
// an index entry alone, so putting the policy's conjuncts at the head of
// that list covers every access path, and lets the access planner bound
// the scan by them like any other conjunct: a policy on the leading
// primary-key column reads one tenant's span, not the table. The
// conjuncts go first so that the statement's own predicates are never
// evaluated against a row the policy hides; an error raised by one
// (a division by zero, a failed cast) would otherwise tell the caller
// something about a row it may not see.
//
// Who is exempt: root and the node's own sessions always (the row-expiry
// job, restore and the recorders must see every row); a role with the
// BYPASSRLS attribute always; the table's owner and members of admin
// unless the table says FORCE ROW LEVEL SECURITY. Foreign-key checks and
// cascades read and write the other table without its policies, as
// PostgreSQL's do: referential integrity is a property of the data, not
// of who is looking.
//
// Policy expressions are stored as SQL text and bound afresh by every
// statement: current_user, current_setting() and the clock are spliced
// to literals exactly as a WHERE clause's are, so a policy keyed on a
// session setting follows SET without any cache to invalidate. A
// statement that row security applies to plans in full every time and
// never enters the plan cache (plancache.go), which is keyed by
// statement and database alone and so cannot tell two roles, or two
// values of a setting, apart.

// rowSecurityError is the refusal of a row a policy does not admit, in
// PostgreSQL's words and with its SQLSTATE. It names the table and
// nothing about the row or about which policy refused it.
func rowSecurityError(desc *catalog.TableDescriptor) *Error {
	return newErrf(CodeInsufficientPriv, "new row violates row-level security policy for table %q", desc.Name)
}

// requireV21RLS refuses row-security DDL before the cluster has
// finalized v21: an older gateway drops the descriptor fields and would
// serve every row.
func (s *Session) requireV21RLS(what string) error {
	if s.db.ClusterVersion() < version.V21 {
		return newErrf(CodeFeatureNotSupported, "%s needs cluster version v21: finalize the upgrade with `datax debug upgrade` first", what)
	}
	return nil
}

// roleBypassesRLS reports whether the actor carries the BYPASSRLS
// attribute. The attribute is the role's own and is not inherited, so
// this reads the one descriptor, once per transaction.
func (s *Session) roleBypassesRLS(ctx context.Context, txn *kvclient.Txn) (bool, error) {
	name := s.actor()
	if s.bypassCache == nil || s.bypassCacheTxn != txn {
		s.bypassCache = map[string]bool{}
		s.bypassCacheTxn = txn
	}
	if b, ok := s.bypassCache[name]; ok {
		return b, nil
	}
	d, err := catalog.LookupRole(ctx, txn, name)
	if err != nil {
		return false, err
	}
	b := d != nil && d.BypassRLS
	s.bypassCache[name] = b
	return b, nil
}

// rowSecurityActive reports whether the table's policies apply to the
// actor (the view's owner while a view's query runs, as for privileges).
func (s *Session) rowSecurityActive(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor) (bool, error) {
	if desc == nil || !desc.RowSecurity || desc.Virtual != "" {
		return false, nil
	}
	if s.system || s.actor() == catalog.RootRole {
		return false, nil
	}
	bypass, err := s.roleBypassesRLS(ctx, txn)
	if err != nil || bypass {
		return false, err
	}
	if desc.ForceRowSecurity {
		return true, nil
	}
	owner, err := s.isOwner(ctx, txn, desc.Owner)
	if err != nil {
		return false, err
	}
	return !owner, nil
}

// policyApplies reports whether a policy governs cmd for the role set.
func policyApplies(p *catalog.Policy, cmd string, set catalog.RoleSet) bool {
	if p.Command != catalog.PolicyAll && p.Command != cmd {
		return false
	}
	if len(p.Roles) == 0 {
		return true
	}
	for _, r := range p.Roles {
		if r == catalog.PublicRole || set.Has(r) {
			return true
		}
	}
	return false
}

// bindPolicyExpr parses a stored policy expression and splices the
// session's values into it.
func (s *Session) bindPolicyExpr(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, p *catalog.Policy, text string) ([]parser.Comparison, error) {
	conds, err := parser.ParsePolicyExpr(text)
	if err != nil {
		return nil, newErrf(CodeInternal, "policy %q on table %q: stored expression %q: %v", p.Name, desc.Name, text, err)
	}
	return s.resolveWhereSubs(ctx, txn, conds, nil)
}

// policyConjuncts combines the policies that govern cmd into WHERE
// conjuncts: the permissive policies OR-ed together, each restrictive
// one AND-ed on. check selects the expression a written row must
// satisfy (WITH CHECK, or USING when the policy gives none) rather than
// the one a read row must. No applicable permissive policy is the
// constant FALSE: default deny.
func (s *Session) policyConjuncts(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, cmd string, check bool) ([]parser.Comparison, error) {
	// Bound once per statement and transaction attempt: a join looks its
	// inner table up once per outer row, and each lookup asks.
	key := policyKey{txn: txn, desc: desc, actor: s.actor(), cmd: cmd, check: check}
	if conds, ok := s.policyMemo[key]; ok {
		return conds, nil
	}
	conds, err := s.bindPolicies(ctx, txn, desc, cmd, check)
	if err != nil {
		return nil, err
	}
	if s.policyMemo == nil {
		s.policyMemo = map[policyKey][]parser.Comparison{}
	}
	// Capped so that no later append can write into the memoized slice.
	s.policyMemo[key] = conds[:len(conds):len(conds)]
	return s.policyMemo[key], nil
}

// policyKey names one binding of a table's policies; see
// Session.policyMemo.
type policyKey struct {
	txn   *kvclient.Txn
	desc  *catalog.TableDescriptor
	actor string
	cmd   string
	check bool
}

func (s *Session) bindPolicies(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, cmd string, check bool) ([]parser.Comparison, error) {
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return nil, err
	}
	var permissive [][]parser.Comparison
	var restrictive []parser.Comparison
	for i := range desc.Policies {
		p := &desc.Policies[i]
		if !policyApplies(p, cmd, set) {
			continue
		}
		text := p.Using
		if check && p.Check != "" {
			text = p.Check
		}
		var conds []parser.Comparison
		if text == "" {
			// A policy with neither expression admits every row
			// (PostgreSQL's default of TRUE).
			conds = []parser.Comparison{{Op: "TRUE"}}
		} else if conds, err = s.bindPolicyExpr(ctx, txn, desc, p, text); err != nil {
			return nil, err
		}
		if p.Restrictive {
			restrictive = append(restrictive, conds...)
		} else {
			permissive = append(permissive, conds)
		}
	}
	switch len(permissive) {
	case 0:
		return []parser.Comparison{{Op: "FALSE"}}, nil
	case 1:
		// One policy's conjuncts stand directly in the WHERE clause, where
		// the access planner can bound the scan by them.
		return append(permissive[0], restrictive...), nil
	}
	return append([]parser.Comparison{{Op: "OR", Or: permissive}}, restrictive...), nil
}

// rowFilter returns the conjuncts that confine a statement's read of
// desc to what the actor's policies admit, for each command in cmds (a
// SELECT ... FOR UPDATE must pass both the SELECT and the UPDATE
// policies), and whether row security applies at all.
func (s *Session) rowFilter(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, cmds ...string) ([]parser.Comparison, bool, error) {
	if desc == nil || desc.Virtual != "" || s.system || s.actor() == catalog.RootRole {
		return nil, false, nil
	}
	if txn != nil && txn.Historical() && txn != s.freshTxn {
		return s.historicalRowFilter(ctx, txn, desc, cmds...)
	}
	return s.rowFilterIn(ctx, txn, desc, cmds...)
}

func (s *Session) rowFilterIn(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, cmds ...string) ([]parser.Comparison, bool, error) {
	active, err := s.rowSecurityActive(ctx, txn, desc)
	if err != nil || !active {
		return nil, false, err
	}
	var out []parser.Comparison
	for _, cmd := range cmds {
		conds, err := s.policyConjuncts(ctx, txn, desc, cmd, false)
		if err != nil {
			return nil, false, err
		}
		out = append(out, conds...)
	}
	return out, true, nil
}

// historicalRowFilter is rowFilter for an AS OF SYSTEM TIME read. Such
// a read resolves its descriptor — and with it the table's policies —
// as of its timestamp, so read plainly it would let a role look at the
// table as it stood before row security was enabled, or under a policy
// since tightened: every row it is now forbidden. The policies, the
// owner and the roles that decide what a historical read may see are
// therefore today's — as fresh as this node can serve them, which for a
// bounded-staleness read is the state its own replicas hold
// (freshCatalog, shared with the privilege check) — read in a
// transaction of their own; the rows are the past's. If that state
// cannot be read the statement fails rather than run unfiltered. A
// table that no longer exists has no policies today, and its own last
// word stands.
func (s *Session) historicalRowFilter(ctx context.Context, htxn *kvclient.Txn, hdesc *catalog.TableDescriptor, cmds ...string) ([]parser.Comparison, bool, error) {
	// Once per statement and table: a join asks for every outer row.
	key := policyKey{txn: htxn, desc: hdesc, actor: s.actor(), cmd: strings.Join(cmds, ",")}
	if f, ok := s.historicalMemo[key]; ok {
		return f.conds, f.active, nil
	}
	var out []parser.Comparison
	var active bool
	err := s.freshCatalog(ctx, "asof-row-security", func(ctx context.Context, cur *kvclient.Txn) error {
		out, active = nil, false
		desc, err := s.freshTable(ctx, cur, hdesc)
		if err != nil {
			return err
		}
		if desc == nil {
			desc = hdesc
		}
		out, active, err = s.rowFilterIn(ctx, cur, desc, cmds...)
		return err
	})
	// The role sets resolved above belong to that transaction.
	s.invalidateRoles()
	if err != nil {
		return nil, false, err
	}
	if s.historicalMemo == nil {
		s.historicalMemo = map[policyKey]historicalFilter{}
	}
	out = out[:len(out):len(out)]
	s.historicalMemo[key] = historicalFilter{conds: out, active: active}
	return out, active, nil
}

// historicalFilter is one memoized answer of historicalRowFilter.
type historicalFilter struct {
	conds  []parser.Comparison
	active bool
}

// securedWhere puts the actor's row filter in front of a statement's
// own conjuncts. active reports that row security applied: the caller
// must then keep the statement out of the plan cache.
func (s *Session) securedWhere(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, where []parser.Comparison, cmds ...string) ([]parser.Comparison, bool, error) {
	filter, active, err := s.rowFilter(ctx, txn, desc, cmds...)
	if err != nil || !active {
		return where, false, err
	}
	out := make([]parser.Comparison, 0, len(filter)+len(where))
	out = append(out, filter...)
	return append(out, where...), true, nil
}

// updateReadsTable reports whether an UPDATE reads the rows it changes,
// beyond finding them: a WHERE clause, RETURNING, or a SET value
// computed from a column. PostgreSQL applies the SELECT policies to
// exactly those statements (they are the ones that need the SELECT
// privilege there), so an UPDATE-only policy still allows a blind
// "UPDATE t SET flag = true".
func updateReadsTable(t *parser.Update, corr []correlatedConjunct) bool {
	if len(t.Where) > 0 || len(corr) > 0 || t.Returning != nil {
		return true
	}
	for _, set := range t.Set {
		reads := false
		exprColumns(set.Value, func(string) { reads = true })
		if reads || exprHasSubquery(set.Value) {
			return true
		}
	}
	return false
}

// rowCheck is the bound WITH CHECK side of a table's policies for one
// statement: nil when row security does not apply to the actor.
type rowCheck struct {
	desc *catalog.TableDescriptor
	// insert and update are what a newly inserted and an updated row
	// must satisfy; visible is the SELECT policies' filter, which a
	// written row must also pass when the statement reads the table
	// (RETURNING, or an UPDATE that had to see the row to change it).
	insert, update, visible []parser.Comparison
}

// rowCheckFor binds the checks a write to desc must pass. needInsert /
// needUpdate say which kinds of row the statement writes; needVisible
// that written rows must also be readable by the actor.
func (s *Session) rowCheckFor(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, needInsert, needUpdate, needVisible bool) (*rowCheck, error) {
	active, err := s.rowSecurityActive(ctx, txn, desc)
	if err != nil || !active {
		return nil, err
	}
	rc := &rowCheck{desc: desc}
	if needInsert {
		if rc.insert, err = s.policyConjuncts(ctx, txn, desc, catalog.PolicyInsert, true); err != nil {
			return nil, err
		}
	}
	if needUpdate {
		if rc.update, err = s.policyConjuncts(ctx, txn, desc, catalog.PolicyUpdate, true); err != nil {
			return nil, err
		}
	}
	if needVisible {
		if rc.visible, err = s.policyConjuncts(ctx, txn, desc, catalog.PolicySelect, false); err != nil {
			return nil, err
		}
	}
	return rc, nil
}

func (rc *rowCheck) holds(conds []parser.Comparison, row map[catalog.ColumnID]types.Datum) error {
	if conds == nil {
		return nil
	}
	ok, err := matchesWhere(conds, rc.desc, row, nil)
	if err != nil {
		return err
	}
	if !ok {
		return rowSecurityError(rc.desc)
	}
	return nil
}

// checkInsert refuses a row the actor's INSERT policies do not admit.
func (rc *rowCheck) checkInsert(row map[catalog.ColumnID]types.Datum) error {
	if rc == nil {
		return nil
	}
	if err := rc.holds(rc.insert, row); err != nil {
		return err
	}
	return rc.holds(rc.visible, row)
}

// checkUpdate refuses the new version of a row the actor's UPDATE
// policies do not admit: an update may not move a row out of what the
// role is allowed to write (or, when it reads the table, to see).
func (rc *rowCheck) checkUpdate(row map[catalog.ColumnID]types.Datum) error {
	if rc == nil {
		return nil
	}
	if err := rc.holds(rc.update, row); err != nil {
		return err
	}
	return rc.holds(rc.visible, row)
}

// policyFuncsAllowed are the session-dependent functions a policy
// expression may call. They are evaluated once per statement, before
// any row; the rest of the spliced set either has an effect (pg_notify,
// set_config, pg_sleep, the sequence functions) or reads the catalog on
// behalf of whoever runs the statement, and neither belongs in a
// predicate every reader of the table evaluates implicitly.
var policyFuncsAllowed = map[string]bool{
	"current_user": true, "session_user": true, "current_setting": true, "current_database": true, "current_schema": true,
	"now": true, "current_timestamp": true, "localtimestamp": true, "statement_timestamp": true, "transaction_timestamp": true, "current_date": true,
	"current_time": true, "localtime": true,
}

// validatePolicyExpr checks a policy expression against the table when
// the policy is created or altered: it must parse, name only the
// table's own columns (unqualified), and hold no subquery, parameter or
// function with an effect.
func validatePolicyExpr(desc *catalog.TableDescriptor, clause, text string) error {
	conds, err := parser.ParsePolicyExpr(text)
	if err != nil {
		return newErrf(CodeSyntaxError, "%s: %v", clause, err)
	}
	var bad error
	pred := func(x parser.Expr) bool {
		switch {
		case bad != nil:
		case x.Sub != nil:
			bad = newErrf(CodeFeatureNotSupported, "%s: subqueries are not supported in a policy expression", clause)
		case x.Param > 0:
			bad = newErrf(CodeSyntaxError, "%s: a policy expression cannot take parameters", clause)
		case x.Window != nil:
			bad = newErrf(CodeFeatureNotSupported, "%s: window functions are not allowed in a policy expression", clause)
		case x.Func != "" && (volatileFuncs[x.Func] || splicedFuncs[x.Func] && !policyFuncsAllowed[x.Func]):
			bad = newErrf(CodeFeatureNotSupported, "%s: function %s() is not allowed in a policy expression", clause, x.Func)
		}
		return bad != nil
	}
	for _, c := range conds {
		condHas(c, pred)
		if bad != nil {
			return bad
		}
		condColumns(c, func(name string) {
			if bad != nil {
				return
			}
			if col, ok := desc.Col(name); !ok || col.Hidden {
				if strings.Contains(name, ".") {
					bad = newErrf(CodeUndefinedColumn, "%s: column %q: a policy expression names the table's columns without a qualifier", clause, name)
				} else {
					bad = newErrf(CodeUndefinedColumn, "%s: column %q does not exist in table %q", clause, name, desc.Name)
				}
			}
		})
		if bad != nil {
			return bad
		}
	}
	return nil
}

// policyColumnUses names the policies whose expressions read a column.
func policyColumnUses(desc *catalog.TableDescriptor, column string) []string {
	var names []string
	for i := range desc.Policies {
		p := &desc.Policies[i]
		uses := false
		for _, text := range []string{p.Using, p.Check} {
			if text == "" {
				continue
			}
			conds, err := parser.ParsePolicyExpr(text)
			if err != nil {
				uses = true // unreadable: refuse rather than guess
				continue
			}
			for _, c := range conds {
				condColumns(c, func(name string) {
					if name == column {
						uses = true
					}
				})
			}
		}
		if uses {
			names = append(names, p.Name)
		}
	}
	return names
}

// renamePolicyColumn rewrites the policies' references to a renamed
// column, as the CHECK constraints' are.
func renamePolicyColumn(desc *catalog.TableDescriptor, from, to string) error {
	for i := range desc.Policies {
		p := &desc.Policies[i]
		for _, text := range []*string{&p.Using, &p.Check} {
			if *text == "" {
				continue
			}
			renamed, err := parser.RenameColumnRefs(*text, from, to)
			if err != nil {
				return newErrf(CodeInternal, "policy %q: %v", p.Name, err)
			}
			*text = renamed
		}
	}
	return nil
}

// policyRoles validates and normalizes a TO list: every role must
// exist; PUBLIC anywhere in it is every role, stored as no list.
func (s *Session) policyRoles(ctx context.Context, txn *kvclient.Txn, names []string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, n := range names {
		n = strings.ToLower(n)
		if n == catalog.PublicRole {
			return nil, nil
		}
		if seen[n] {
			continue
		}
		if _, err := s.lookupRoleOrErr(ctx, txn, n); err != nil {
			return nil, err
		}
		seen[n] = true
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// policyTable resolves the table of a policy statement for its owner.
func (s *Session) policyTable(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.TableDescriptor, error) {
	shared, err := s.lookup(ctx, txn, name)
	if err != nil {
		return nil, err
	}
	if err := mustBeReal(shared); err != nil {
		return nil, err
	}
	if shared.IsView() {
		return nil, newErrf(CodeWrongObjectType, "%q is a view: row security belongs to tables", shared.Name)
	}
	if catalog.IsSystemTable(shared.Name) && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster: it cannot carry row-security policies", shared.Name)
	}
	if err := s.checkTableOwner(ctx, txn, shared); err != nil {
		return nil, err
	}
	return shared.Clone(), nil
}

// execCreatePolicy runs CREATE POLICY: the table's owner (or an admin).
func (s *Session) execCreatePolicy(ctx context.Context, txn *kvclient.Txn, t *parser.CreatePolicy) (*Result, error) {
	if err := s.requireV21RLS("CREATE POLICY"); err != nil {
		return nil, err
	}
	desc, err := s.policyTable(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	if _, exists := desc.Policy(t.Name); exists {
		return nil, newErrf(CodeDuplicateObject, "policy %q for table %q already exists", t.Name, desc.Name)
	}
	p := catalog.Policy{Name: t.Name, Restrictive: t.Restrictive, Command: t.Command, Using: t.Using, Check: t.Check}
	if p.Roles, err = s.policyRoles(ctx, txn, t.Roles); err != nil {
		return nil, err
	}
	if p.Using != "" {
		if err := validatePolicyExpr(desc, "USING", p.Using); err != nil {
			return nil, err
		}
	}
	if p.Check != "" {
		if err := validatePolicyExpr(desc, "WITH CHECK", p.Check); err != nil {
			return nil, err
		}
	}
	desc.Policies = append(desc.Policies, p)
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	log.Audit("policy-ddl", "stmt", "CREATE POLICY", "target", desc.Name+"."+t.Name, "command", p.Command, "restrictive", p.Restrictive,
		"roles", strings.Join(p.Roles, ","), "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "CREATE POLICY"}, nil
}

// execAlterPolicy runs ALTER POLICY: a rename, or new roles and
// expressions for an existing policy.
func (s *Session) execAlterPolicy(ctx context.Context, txn *kvclient.Txn, t *parser.AlterPolicy) (*Result, error) {
	if err := s.requireV21RLS("ALTER POLICY"); err != nil {
		return nil, err
	}
	desc, err := s.policyTable(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	p, ok := desc.Policy(t.Name)
	if !ok {
		return nil, newErrf(CodeUndefinedObject, "policy %q for table %q does not exist", t.Name, desc.Name)
	}
	if t.RenameTo != "" {
		if _, exists := desc.Policy(t.RenameTo); exists && t.RenameTo != t.Name {
			return nil, newErrf(CodeDuplicateObject, "policy %q for table %q already exists", t.RenameTo, desc.Name)
		}
		p.Name = t.RenameTo
	}
	if t.SetRoles {
		if p.Roles, err = s.policyRoles(ctx, txn, t.Roles); err != nil {
			return nil, err
		}
	}
	if t.Using != nil {
		if p.Command == catalog.PolicyInsert {
			return nil, newErrf(CodeSyntaxError, "only a WITH CHECK expression is allowed for an INSERT policy")
		}
		if err := validatePolicyExpr(desc, "USING", *t.Using); err != nil {
			return nil, err
		}
		p.Using = *t.Using
	}
	if t.Check != nil {
		if p.Command == catalog.PolicySelect || p.Command == catalog.PolicyDelete {
			return nil, newErrf(CodeSyntaxError, "WITH CHECK cannot be applied to a %s policy", strings.ToUpper(p.Command))
		}
		if err := validatePolicyExpr(desc, "WITH CHECK", *t.Check); err != nil {
			return nil, err
		}
		p.Check = *t.Check
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	log.Audit("policy-ddl", "stmt", "ALTER POLICY", "target", desc.Name+"."+t.Name, "to", p.Name,
		"roles", strings.Join(p.Roles, ","), "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "ALTER POLICY"}, nil
}

// execDropPolicy runs DROP POLICY. Dropping the last policy of a table
// whose row security stays enabled leaves it at default deny.
func (s *Session) execDropPolicy(ctx context.Context, txn *kvclient.Txn, t *parser.DropPolicy) (*Result, error) {
	desc, err := s.policyTable(ctx, txn, t.Table)
	if err != nil {
		var nf *catalog.ErrTableNotFound
		if t.IfExists && asErr(err, &nf) {
			return &Result{Tag: "DROP POLICY"}, nil
		}
		return nil, err
	}
	kept := desc.Policies[:0]
	for _, p := range desc.Policies {
		if p.Name != t.Name {
			kept = append(kept, p)
		}
	}
	if len(kept) == len(desc.Policies) {
		if t.IfExists {
			return &Result{Tag: "DROP POLICY"}, nil
		}
		return nil, newErrf(CodeUndefinedObject, "policy %q for table %q does not exist", t.Name, desc.Name)
	}
	desc.Policies = kept
	if len(desc.Policies) == 0 {
		desc.Policies = nil
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	log.Audit("policy-ddl", "stmt", "DROP POLICY", "target", desc.Name+"."+t.Name, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "DROP POLICY"}, nil
}

// applyRowSecurity is ALTER TABLE ... ENABLE | DISABLE | FORCE | NO
// FORCE ROW LEVEL SECURITY on a descriptor copy the caller writes.
func (s *Session) applyRowSecurity(desc *catalog.TableDescriptor, action string) error {
	if desc.IsView() {
		return newErrf(CodeWrongObjectType, "%q is a view: row security belongs to tables", desc.Name)
	}
	switch action {
	case "enable":
		if err := s.requireV21RLS("ENABLE ROW LEVEL SECURITY"); err != nil {
			return err
		}
		desc.RowSecurity = true
	case "disable":
		desc.RowSecurity = false
	case "force":
		if err := s.requireV21RLS("FORCE ROW LEVEL SECURITY"); err != nil {
			return err
		}
		desc.ForceRowSecurity = true
	case "no force":
		desc.ForceRowSecurity = false
	}
	log.Audit("policy-ddl", "stmt", "ALTER TABLE "+strings.ToUpper(action)+" ROW LEVEL SECURITY", "target", desc.Name, "principal", s.sessionUser, "role", s.user)
	return nil
}

// rolePolicyUses lists "table.policy" for every policy whose TO list
// names the role: what DROP ROLE must refuse to orphan.
func (s *Session) rolePolicyUses(ctx context.Context, txn *kvclient.Txn, role string) ([]string, error) {
	all, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, d := range all {
		for i := range d.Policies {
			for _, r := range d.Policies[i].Roles {
				if r == role {
					out = append(out, "policy "+d.Policies[i].Name+" on table "+d.Name)
				}
			}
		}
	}
	return out, nil
}
