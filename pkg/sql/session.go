package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
	"github.com/sthorne/datax/pkg/util/decimal"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// resolveAsOf turns an AS OF SYSTEM TIME operand into a fixed timestamp:
// a negative duration ('-5s', relative to now), an RFC 3339 timestamp
// ('2026-08-29T12:00:00Z'), or a Unix-nanoseconds integer.
func resolveAsOf(operand string, now hlc.Timestamp) (hlc.Timestamp, error) {
	if nanos, err := strconv.ParseInt(operand, 10, 64); err == nil {
		if nanos <= 0 || nanos >= now.WallTime {
			return hlc.Timestamp{}, fmt.Errorf("timestamp %d is not in the past", nanos)
		}
		return hlc.Timestamp{WallTime: nanos}, nil
	}
	if d, err := time.ParseDuration(operand); err == nil {
		if d >= 0 {
			return hlc.Timestamp{}, fmt.Errorf("duration %q must be negative (a time in the past)", operand)
		}
		return now.AddNanos(d.Nanoseconds()), nil
	}
	if operand == parser.FollowerReadTimestamp {
		return now.AddNanos(-FollowerReadLag.Nanoseconds()), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, operand); err == nil {
		ts := hlc.Timestamp{WallTime: t.UnixNano()}
		if !ts.Less(now) {
			return hlc.Timestamp{}, fmt.Errorf("timestamp %q is not in the past", operand)
		}
		return ts, nil
	}
	return hlc.Timestamp{}, fmt.Errorf("cannot interpret %q as a duration, RFC 3339 timestamp, or Unix nanoseconds", operand)
}

// resolveMaxStaleness turns a with_max_staleness operand into the
// statement timestamp: the freshest the local store can serve
// (localClosed), but never older than now minus the bound. A local store
// that has closed past now-bound simply gives a fresher read; one
// lagging past the bound (or absent — a pure gateway) yields
// now-bound, and ranges that cannot serve it locally fall back to their
// leaders through ordinary stale-read routing.
func resolveMaxStaleness(operand string, now, localClosed hlc.Timestamp) (hlc.Timestamp, error) {
	d, err := time.ParseDuration(operand)
	if err != nil {
		// Interval text ('10 seconds', '1 minute'; a day is 24 hours).
		iv, ierr := types.ParseInterval(operand)
		if ierr != nil {
			return hlc.Timestamp{}, fmt.Errorf("cannot interpret %q as a duration", operand)
		}
		d = time.Duration(iv.CmpValue())
	}
	if d <= 0 {
		return hlc.Timestamp{}, fmt.Errorf("max staleness %q must be a positive duration", operand)
	}
	ts := now.AddNanos(-d.Nanoseconds())
	if ts.Less(localClosed) {
		ts = localClosed
	}
	return ts, nil
}

// ResultColumn describes one output column.
type ResultColumn struct {
	Name string
	Type types.Family
	// Typmod is PostgreSQL's atttypmod for the column (0 = none, emitted
	// as -1 on the wire). Set only when a real table column with a type
	// modifier backs the output: ((p<<16) | (s+4)) for DECIMAL(p,s), n+4
	// for VARCHAR(n) / CHAR(n), p for TIMESTAMP(p).
	Typmod int32
	// Width, MaxLen, Char and NoTZ carry the backing column's integer
	// width (2 / 4, 0 = 8), character length, CHAR(n) padding and
	// TIMESTAMP without time zone, which pick the wire type (int2 /
	// int4 / int8, varchar / bpchar / text, timestamp / timestamptz) and
	// its text rendering. Zero for computed columns.
	Width         int32
	MaxLen        int32
	Char          bool
	NoTZ          bool
	TimePrecision int32
	// EnumType and EnumName identify an enum column's type (the wire
	// OID derives from the ID).
	EnumType uint64
	EnumName string
}

// DecimalTypmod encodes a DECIMAL(p,s) declaration as PostgreSQL's
// atttypmod.
func DecimalTypmod(precision, scale int32) int32 {
	return precision<<16 | (scale + 4)
}

// Result is the outcome of one statement.
type Result struct {
	Columns []ResultColumn
	Rows    [][]types.Datum
	// Tag is the PostgreSQL command tag ("SELECT 3", "INSERT 0 2", ...).
	Tag string
	// Notices are messages for the client, sent as NOTICEs before the
	// statement completes (a sealed table's plaintext columns, say).
	Notices []string
	// Warnings are sent the same way as WARNINGs (SQLSTATE 01000): an
	// advisory unlock of a lock the session did not hold.
	Warnings []string
	// Stream, when set, delivers the rows on demand instead of Rows (a
	// scan-shaped SELECT on a streaming session, see stream.go); the
	// consumer pulls it with Next, closes it, and takes the tag from it.
	Stream *RowStream
	// Copy, when set, is a COPY TO's output format: the rows are the
	// COPY's, sent as CopyData rather than DataRows, and the command tag
	// is COPY n (see copyto.go).
	Copy *CopyOut
}

// AddScanned records KV rows read while executing the current
// statement (issue #157).
func (s *Session) AddScanned(n int64) { s.scanned.Add(n) }

// TakeScanned returns the rows scanned since the last call and resets
// the counter, so each statement is charged its own reads.
func (s *Session) TakeScanned() int64 { return s.scanned.Swap(0) }

// TxnState is the session's transaction status, mirrored into the wire
// protocol's ReadyForQuery byte.
type TxnState int

const (
	StateIdle   TxnState = iota // 'I'
	StateOpen                   // 'T'
	StateFailed                 // 'E' — only ROLLBACK escapes
)

// Session executes statements for one client connection.
type Session struct {
	// txnQueried marks a transaction block that has run a statement
	// against data: SET TRANSACTION SNAPSHOT must come before one.
	txnQueried bool
	// cursors are the session's open SQL cursors (cursor.go).
	cursors map[string]*sqlCursor
	// preparedSQL are the session's PREPARE'd statements (prepare.go).
	preparedSQL map[string]*preparedSQL
	// rowFuncIDs are the per-row closures this session registered for
	// its current statement (registerRowFunc).
	rowFuncIDs []int64
	db         *kvclient.DB
	cat        *catalog.Accessor
	// scanned counts the KV rows this session's current statement has
	// read, so the cost of a full scan is attributable to the statement
	// shape that caused it rather than only to a cluster-wide counter
	// (issue #157). Read and reset by the wire layer at statement end;
	// atomic because a statement's execution may fan out.
	scanned atomic.Int64
	// sessionUser is the authenticated SQL user ("root" for internal
	// sessions). In insecure/trust mode it is the client-claimed name:
	// privileges are enforced against it, but nothing verified the
	// identity. user is the current role — the session user, or the role
	// SET ROLE selected (current_user vs session_user).
	sessionUser string
	// isSuperuser is whether sessionUser is a superuser, read when the
	// connection starts (StartupParams; nil until then).
	isSuperuser *bool
	user        string
	// privAs, when set, is the role privilege checks run as instead of
	// user: a view's owner while its query executes (cte.go).
	privAs string
	// roleCache holds the effective role sets resolved in roleCacheTxn
	// (privileges.go).
	roleCache    map[string]catalog.RoleSet
	roleCacheTxn *kvclient.Txn
	// bypassCache holds, per actor, whether the role carries BYPASSRLS,
	// resolved in bypassCacheTxn (rls.go).
	bypassCache    map[string]bool
	bypassCacheTxn *kvclient.Txn
	// policyMemo holds the row-security conjuncts bound so far by the
	// statement being executed (rls.go). It never outlives a statement:
	// the next one may run as another role or under another value of a
	// setting a policy reads, and must bind afresh.
	policyMemo     map[policyKey][]parser.Comparison
	historicalMemo map[policyKey]historicalFilter
	// historicalPrivs holds the statement's answers to "may the actor
	// exercise this privilege on this table today", asked by an AS OF
	// SYSTEM TIME read (historicalTablePrivAllowed); per statement, like
	// historicalMemo.
	historicalPrivs map[historicalPrivKey]bool
	// asofLocal marks a bounded-staleness read in flight, and asofTS its
	// timestamp: its checks of today's privileges and policies read the
	// freshest state this node serves locally (freshCatalog).
	asofLocal bool
	asofTS    hlc.Timestamp
	// freshTxn is the historical transaction freshCatalog is running a
	// check in: one that stands for today, not for the past, so the
	// checks inside it must not be redirected again.
	freshTxn *kvclient.Txn
	// database is the session's current database (the startup parameter,
	// USE, or SET database); unqualified table names resolve in it.
	database string
	// system marks the node's own internal session (see NewSystemSession):
	// the only one allowed to create or drop a system table.
	system bool

	// vars are the session variables (SET / RESET / SHOW); localVars
	// holds the session's values while a transaction block's SET LOCAL
	// overrides them; txnStartedReadOnly marks a block begun under
	// default_transaction_read_only.
	vars sessionVars
	// pathMemo is the parsed form of vars.searchPath (schema.go);
	// viewPath, while a view's query runs, is the view's own search path
	// (nil otherwise).
	pathMemo struct {
		raw    string
		parsed []string
	}
	viewPath           []string
	localVars          *sessionVars
	txnStartedReadOnly bool
	// BackendPID is the wire's process ID for this session (0 for an
	// internal one); BackendControl cancels (or terminates) another
	// session by PID for pg_cancel_backend / pg_terminate_backend, and
	// SessionsHook lists this node's sessions for SHOW SESSIONS and
	// pg_stat_activity. The SQL server sets them.
	BackendPID     int32
	BackendControl func(pid int32, terminate bool) (bool, error)
	SessionsHook   func() []SessionInfo
	// pendingWipes are indexes dropped by statements of this session
	// whose entries are reclaimed once the statement commits.
	pendingWipes []indexWipe
	extraDDL     []string
	// extraDrops are the tables and views a statement dropped, by
	// descriptor id, for the post-commit drain of the drop
	// (catalog.Accessor.FinishDrop, issue #377); pendingDrops gathers
	// them over an explicit transaction, as pendingDDL does names.
	extraDrops   []uint64
	pendingDrops []uint64
	// stmtNow is the statement timestamp (now(), current_timestamp):
	// taken from the clock on first use in each statement.
	stmtNow int64
	// seqs is the session's sequence state (value blocks, currval,
	// lastval), created on first use.
	seqs *seqState
	// rels are the statement-scoped relations (WITH members, joined
	// derived tables) bound by name; see cte.go.
	rels map[string]*relation
	// explain collects each stage's actual rows and time while EXPLAIN
	// ANALYZE runs a statement (nil otherwise).
	explain *explainStats
	// plans is the session's plan cache (plancache.go), made on first
	// use; stmtLookups memoizes the catalog lookups of the statement
	// being executed (a name is resolved once per statement, not once
	// for the view check and again for the plan).
	plans       *planCache
	stmtLookups *lookupMemo
	// streaming lets scan-shaped SELECTs return a RowStream instead of
	// materialized rows (the wire layer opts in; internal callers read
	// Result.Rows). memUsed is the running statement's charged memory
	// (stream.go).
	streaming bool
	memUsed   int64
	// streamTop marks the statement about to run as a top-level SELECT
	// whose result may stream; execSelect takes it on entry, so the
	// selects it runs underneath (WITH members, derived tables, set
	// members, subqueries) materialize as they must.
	streamTop bool
	// dmlJoin, while an UPDATE ... FROM or DELETE ... USING runs its
	// join, receives the joined rows (dmlfrom.go).
	dmlJoin *dmlJoin

	// lo is the session's large-object state: the descriptors open in
	// its transaction (largeobject.go), made on first use.
	lo *loState

	// notify is the session's LISTEN / NOTIFY state (listen.go).
	notify notifyState

	// adv is the session's advisory-lock state (advlock.go); warnings
	// collects the running statement's WARNINGs for its result.
	adv      advisoryState
	warnings []string

	// keys is the node's key service (keyservice.go; nil: none here).
	// transportMarked / transportOK record whether the SQL server said
	// how the client is connected, and whether that may carry key
	// material. keyLeaseAfterCommit marks a statement that took a use of
	// a key away inside the session's transaction (DROP OWNED, REASSIGN
	// OWNED, DROP ROLE): it waits out the key lease once committed.
	keys                KeyService
	transportMarked     bool
	transportOK         bool
	keyLeaseAfterCommit bool

	txn   *kvclient.Txn
	state TxnState
	// pendingDDL names tables changed inside the open explicit transaction;
	// lease adoption for them is drained at COMMIT.
	pendingDDL []string
	// createdTables are the IDs of the tables created in the open
	// transaction block createdTxn (index_keys.go): CREATE INDEX on one
	// of them runs inside the block.
	createdTxn    *kvclient.Txn
	createdTables map[uint64]bool

	// temp is the session's temporary tables (temptable.go).
	temp tempState
}

// NewSession creates a root session (internal components, tests, tools).
// The catalog accessor is shared per node.
func NewSession(db *kvclient.DB, cat *catalog.Accessor) *Session {
	return &Session{db: db, cat: cat, sessionUser: "root", user: "root", database: catalog.DefaultDatabase, vars: defaultVars()}
}

// NewSystemSession creates the node's own root session, the one that may
// create and drop system tables (the metrics recorder's).
func NewSystemSession(db *kvclient.DB, cat *catalog.Accessor) *Session {
	return &Session{db: db, cat: cat, sessionUser: "root", user: "root", database: catalog.DefaultDatabase, system: true, vars: defaultVars()}
}

// NewSessionForUser creates a session for an authenticated user.
func NewSessionForUser(db *kvclient.DB, cat *catalog.Accessor, user string) *Session {
	if user == "" {
		user = "root"
	}
	return &Session{db: db, cat: cat, sessionUser: user, user: user, database: catalog.DefaultDatabase, vars: defaultVars()}
}

// SetUser rebinds the session's user (pgwire sets it after startup);
// any SET ROLE is undone.
func (s *Session) SetUser(user string) {
	if user == "" {
		user = "root"
	}
	s.sessionUser = user
	s.isSuperuser = nil
	s.vars.role = ""
	s.applyRole()
}

// SetStreaming lets scan-shaped SELECTs return their rows as a
// RowStream (Result.Stream) instead of materializing them. Only a
// consumer that pulls and closes streams (the wire layer) should turn
// it on.
func (s *Session) SetStreaming(on bool) { s.streaming = on }

// User returns the session's current role (current_user): the session
// user unless SET ROLE changed it.
func (s *Session) User() string { return s.user }

// SessionUser returns the authenticated session user (session_user).
func (s *Session) SessionUser() string { return s.sessionUser }

// Database returns the session's current database.
func (s *Session) Database() string { return s.database }

// ClusterVersion is the cluster version the session's gateway observes
// (its version-gated DDL keys off this mirror, which starts at the floor
// and catches up with the first heartbeat).
func (s *Session) ClusterVersion() version.Version { return s.db.ClusterVersion() }

// FinalizedClusterVersion reads the replicated cluster version the
// upgrade finalized (the value the gateway's mirror converges to).
func (s *Session) FinalizedClusterVersion(ctx context.Context) (version.Version, error) {
	raw, err := s.db.Get(ctx, keys.ClusterVersionKey())
	if err != nil {
		return 0, err
	}
	if raw == nil {
		return version.V1, nil
	}
	v, err := strconv.Atoi(string(raw))
	if err != nil {
		return 0, err
	}
	return version.Version(v), nil
}

// lookup resolves a table name: a virtual catalog table (pg_catalog.x,
// information_schema.x, or a bare pg_catalog name that no real table in
// the current database shadows) or a table in the session's current
// database (or the name's own qualifier).
func (s *Session) lookup(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.TableDescriptor, error) {
	if parts := catalog.SplitName(name); len(parts) == 3 && parts[0] == s.database && vtable.IsSchema(parts[1]) {
		name = parts[1] + "." + parts[2] // db.pg_catalog.x, in the current database
	}
	db, bare := catalog.SplitTableName(name)
	if len(s.rels) > 0 {
		if db == "" {
			if r, ok := s.rels[strings.ToLower(bare)]; ok {
				return r.desc, nil // a WITH member shadows a table of its name
			}
		} else {
			for _, r := range s.rels {
				if r.alias != "" && r.alias == strings.ToLower(name) {
					return r.desc, nil // a view referenced by its qualified name
				}
			}
		}
	}
	// The session's temporary tables come before every other table, as
	// pg_temp leads PostgreSQL's search path; never inside a view's
	// query, whose names resolve through the view's own path.
	if s.viewPath == nil {
		if d, ok, err := s.lookupTemp(ctx, txn, name); ok {
			return d, err
		}
	}
	if catalog.IsUnreadableSystemTable(bare) && !s.system && !vtable.IsSchema(db) {
		// The named keys (issue #310): no session but the node's own
		// reads the table; every other is shown its metadata.
		if d, ok := keysVirtualTable(); ok {
			return d, nil
		}
	}
	if vtable.IsSchema(db) {
		if vt, ok := vtable.Lookup(name); ok {
			return vt.Descriptor(), nil
		}
		if db != vtable.PgCatalog || !catalog.IsSystemTable(bare) {
			return nil, &catalog.ErrTableNotFound{Name: name}
		}
		// pg_catalog.datax_metrics: the cluster's own tables are listed
		// in pg_catalog (pg_class), so they answer to that name too
		// (pg_dump's GRANT ... ON TABLE pg_catalog.datax_metrics).
		name = bare
	}
	// A descriptor this transaction's DDL wrote is read through the
	// transaction and never cached: the cache is shared with other
	// sessions on the gateway and must not carry uncommitted state, and
	// a rollback must not leave the phantom behind.
	var d *catalog.TableDescriptor
	var err error
	if s.state == StateOpen && s.ddlTouched(name) {
		d, err = s.cat.LookupFreshIn(s.rctx(ctx), txn, s.database, name)
	} else {
		memo := s.stmtLookups
		if s.viewPath != nil {
			// A view's query resolves through the view's own path: the
			// memo holds what the statement's names resolved to under the
			// session's.
			memo = nil
		}
		if memo != nil && memo.txn == txn {
			if md, ok := memo.m[name]; ok {
				return md, nil
			}
		}
		d, err = s.cat.LookupIn(s.rctx(ctx), txn, s.database, name)
		if err == nil && memo != nil && memo.txn == txn {
			memo.m[name] = d
		}
	}
	if err == nil && d != nil {
		// Whatever the statement goes on to do with the table, the row
		// encoder can file its rows in the table's vector indexes.
		if verr := s.loadVectorCentroids(ctx, d); verr != nil {
			return nil, verr
		}
	}
	if err != nil {
		var nf *catalog.ErrTableNotFound
		if errors.As(err, &nf) {
			if db == "" {
				if vt, ok := vtable.Lookup(bare); ok {
					return vt.Descriptor(), nil
				}
			}
			// A sequence reads as a one-row relation (last_value, log_cnt,
			// is_called), as in PostgreSQL — psql's \d of a sequence needs it.
			if sd, serr := s.lookupSequence(ctx, txn, name); serr == nil {
				return sequenceRelation(sd), nil
			}
		}
	}
	return d, err
}

// TestingBeforeImplicitCommit, when set, runs after an implicit
// statement has executed and before its transaction commits — a test's
// window to change the schema underneath a planned statement.
var TestingBeforeImplicitCommit func()

// lookupMemo caches one statement's table lookups within one transaction
// attempt (see Session.stmtLookups).
type lookupMemo struct {
	txn *kvclient.Txn
	m   map[string]*catalog.TableDescriptor
}

// plansFor returns the session's plan cache, creating it.
func (s *Session) plansFor() *planCache {
	if s.plans == nil {
		s.plans = newPlanCache()
	}
	return s.plans
}

// PlanCacheLen is the number of plans the session holds (tests).
func (s *Session) PlanCacheLen() int { return s.plans.len() }

// sequenceRelation is the virtual one-row descriptor a sequence reads as.
func sequenceRelation(sd *catalog.SequenceDescriptor) *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: sd.ID, Name: sd.Name, DatabaseID: sd.DatabaseID, Virtual: "sequence",
		Columns: []catalog.Column{
			{ID: 1, Name: "last_value", Type: types.Int, NotNull: true},
			{ID: 2, Name: "log_cnt", Type: types.Int, NotNull: true},
			{ID: 3, Name: "is_called", Type: types.Bool, NotNull: true},
			{ID: 4, Name: "_ord", Type: types.Int, NotNull: true, Hidden: true},
		},
		PrimaryKey: []catalog.ColumnID{4},
	}
}

// envTables is which tables a virtual environment gathers.
type envTables int

const (
	// envNoTables: the reader wants roles or settings only.
	envNoTables envTables = iota
	// envCurrentTables: the session's current database, which is all
	// that pg_catalog and information_schema describe.
	envCurrentTables
)

// virtualEnv gathers what the virtual tables render: every database,
// the current database's tables, sequences and types (scope
// envCurrentTables), statistics, users and admins. Every role
// reads the catalogs, as in PostgreSQL (issue #312); what each one
// lists follows PostgreSQL too. pg_catalog lists every table, whatever
// privileges the reader holds (filtered false); information_schema
// lists those the reader holds some privilege on (filtered true). The
// statistics, which describe a table's rows rather than its shape, are
// gathered only for the tables the reader may read rows of, and a
// sequence's counter only for one it may read.
func (s *Session) virtualEnv(ctx context.Context, txn *kvclient.Txn, scope envTables, filtered bool) (*vtable.Env, error) {
	env := &vtable.Env{User: s.user, SessionUser: s.sessionUser, Database: s.database, Stats: map[uint64]*catalog.TableStatistics{}, Now: s.statementTime()}
	dbs, err := catalog.ListDatabases(ctx, txn)
	if err != nil {
		return nil, err
	}
	env.Databases = dbs
	var cur *catalog.DatabaseDescriptor
	for _, d := range dbs {
		if d.Name == s.database {
			cur = d
		}
	}
	if scope == envCurrentTables && cur != nil {
		// The current database's alone (issue #322); without one (it was
		// dropped under the session) there is nothing to list.
		if env.Sequences, err = catalog.ListSequences(ctx, txn, cur.ID); err != nil {
			return nil, err
		}
		if env.Types, err = catalog.ListTypes(ctx, txn, cur.ID); err != nil {
			return nil, err
		}
		if env.Schemas, err = catalog.ListSchemas(ctx, txn, cur.ID); err != nil {
			return nil, err
		}
	}
	set, err := s.currentRoleSet(ctx, txn)
	if err != nil {
		return nil, err
	}
	// A historical read lists the schema as of its timestamp but under
	// today's roles (issue #312). What is today's — statistics, a
	// sequence's counter — it shows to an admin only: deciding by the
	// past's grants would undo a revoke, and today's grant on a table
	// the timestamp's schema names is not worth a read per table.
	historical := txn != nil && txn.Historical()
	env.SequenceValue = func(sd *catalog.SequenceDescriptor) (int64, bool, error) {
		// PostgreSQL's pg_sequences shows last_value only to a role that
		// may read the sequence.
		if historical {
			if !set.IsAdmin() {
				return 0, false, nil
			}
		} else if ok, err := s.sequencePrivAllowed(ctx, txn, sd, "SELECT"); err != nil || !ok {
			return 0, false, err
		}
		return s.sequenceValue(ctx, sd)
	}
	env.IsAdmin = set.IsAdmin()
	env.RoleSet = set
	env.LargeObjects = func() ([]vtable.LargeObjectInfo, error) { return s.listLargeObjects(ctx, txn, set) }
	env.Keys = func() ([]vtable.KeyInfo, error) { return s.listKeys(ctx, txn, set) }
	env.SealJobs = func() ([]vtable.SealJobInfo, error) { return s.listSealJobs(ctx, txn) }
	if s.adv.h != nil {
		env.AdvisoryLocks = func() ([]vtable.AdvisoryLockInfo, error) { return s.listAdvisoryLocks(ctx) }
	}
	// information_schema does not list a table whose schema the session
	// may not use, as the console does not list it (issue #259): both
	// ask catalog.SchemaUsageDenied, so SHOW TABLES and /api/schema
	// answer the listing question the same way (issue #197).
	denied := catalog.SchemaUsageDenied(dbs, set)
	listed := func(d *catalog.TableDescriptor) bool {
		return s.tempVisible(d) && (!filtered || (canSeeTable(set, d) && !denied[d.DatabaseID]))
	}
	var all []*catalog.TableDescriptor
	if scope == envCurrentTables {
		if cur != nil {
			if all, err = s.listIn(ctx, txn, cur); err != nil {
				return nil, err
			}
		}
		// A foreign key may reference a table of another database; it is
		// resolved by ID when a row needs it, under the same visibility
		// rule as the listing.
		env.ReadTable = func(id uint64) *catalog.TableDescriptor {
			d, err := catalog.ReadTable(ctx, txn, id)
			if err != nil || d == nil || !listed(d) {
				return nil
			}
			return d
		}
	}
	for _, d := range all {
		if !listed(d) {
			continue
		}
		env.Tables = append(env.Tables, d)
		// Statistics count every row of a table: a role that may not read
		// the rows sees none, and neither does one its row-security
		// policies confine. reltuples and n_live_tup read as "not
		// analyzed" rather than as the size of data it cannot read.
		if !catalog.CanSelectTable(set, d) || denied[d.DatabaseID] || (historical && !set.IsAdmin()) {
			continue
		}
		if confined, err := s.rowSecurityActive(ctx, txn, d); err != nil {
			return nil, err
		} else if confined {
			continue
		}
		if st, _ := s.cat.Stats(ctx, d.ID); st != nil {
			env.Stats[d.ID] = st
		}
	}
	roles, err := catalog.ListRoles(ctx, txn)
	if err != nil {
		return nil, err
	}
	env.Roles = roles
	env.Settings = s.settings()
	env.SettingDefaults = s.settingDefaults()
	env.SettingTypes = settingTypes
	env.SettingRaw = pgSettingRaw
	env.SettingContexts = pgSettingContexts
	if s.SessionsHook != nil {
		env.Sessions = s.SessionsHook()
	}
	return env, nil
}

// fetchVirtual generates a virtual table's rows and applies the WHERE
// conjuncts (no index ever applies).
func (s *Session) fetchVirtual(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, error) {
	if strings.HasPrefix(desc.Virtual, relationPrefix) {
		return s.relationRows(desc, where, params, limit)
	}
	if desc.Virtual == "sequence" {
		sd, err := catalog.ReadSequence(ctx, txn, desc.ID)
		if err != nil {
			return nil, ToSQLError(err)
		}
		v, called, err := s.sequenceValue(ctx, sd)
		if err != nil {
			return nil, err
		}
		if !called {
			v = sd.Start
		}
		row := map[catalog.ColumnID]types.Datum{1: types.NewInt(v), 2: types.NewInt(0), 3: types.NewBool(called), 4: types.NewInt(0)}
		ok, err := matchesWhere(where, desc, row, params)
		if err != nil || !ok {
			return nil, err
		}
		return []fetchedRow{{row: row}}, nil
	}
	vt, ok := vtable.Lookup(desc.Virtual)
	if !ok {
		return nil, newErrf(CodeInternal, "virtual table %q vanished", desc.Virtual)
	}
	env, err := s.virtualEnv(ctx, txn, envCurrentTables, vt.Schema == vtable.InformationSchema)
	if err != nil {
		return nil, err
	}
	rows, err := vt.Rows(ctx, env)
	if err != nil {
		return nil, err
	}
	ord := desc.PrimaryKey[0]
	var out []fetchedRow
	for i, r := range rows {
		row := make(map[catalog.ColumnID]types.Datum, len(r)+1)
		for j, d := range r {
			row[catalog.ColumnID(j+1)] = d
		}
		row[ord] = types.NewInt(int64(i))
		ok, err := matchesWhere(where, desc, row, params)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, fetchedRow{row: row})
		if limit > 0 && int64(len(out)) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Session) State() TxnState { return s.state }

// statementTime is now() for the statement in flight: one reading of
// the clock, so now(), current_timestamp and current_date agree within
// a statement.
func (s *Session) statementTime() int64 {
	if s.stmtNow == 0 {
		s.stmtNow = s.db.Clock().Now().WallTime
	}
	return s.stmtNow
}

// Close rolls back any open transaction (connection teardown).
func (s *Session) Close(ctx context.Context) {
	if s.txn != nil {
		_ = s.txn.Rollback(ctx)
		s.loEndTxn(ctx, false)
		s.txn = nil
	}
	s.discardNotify()
	s.advisoryClose()
	s.releaseRowFuncs()
	s.state = StateIdle
	s.tempEndTxn()
	s.closeTemp()
}

// Execute runs one parsed statement with the given parameter values,
// under the session's statement_timeout and lock_timeout.
func (s *Session) Execute(ctx context.Context, stmt parser.Statement, params []types.Datum) (res *Result, serr *Error) {
	params = s.zoneParams(params)
	s.releaseRowFuncs()
	s.stmtNow = 0
	s.memUsed = 0
	s.policyMemo, s.historicalMemo, s.historicalPrivs = nil, nil, nil
	s.invalidateRoles()
	s.advisoryStatement()
	s.warnings = nil
	defer func() {
		if res != nil && len(s.warnings) > 0 {
			res.Warnings = append(res.Warnings, s.warnings...)
		}
		s.warnings = nil
	}()
	var deadline time.Time
	if d := s.vars.statementTimeout; d > 0 {
		parent := ctx
		var cancel context.CancelFunc
		deadline = time.Now().Add(d)
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
		defer func() {
			// The message names the statement timeout only when it is
			// what fired; the caller's own deadline is its request.
			if serr != nil && serr.Code == CodeQueryCanceled && parent.Err() != nil {
				serr = &Error{Code: CodeQueryCanceled, Msg: "canceling statement due to user request"}
			}
		}()
	}
	if d := s.vars.lockTimeout; d > 0 {
		ctx = kvclient.WithLockTimeout(ctx, d)
	}
	defer func() {
		// A streamed result outlives this call: it carries the timeouts
		// into every pull.
		if res != nil && res.Stream != nil {
			res.Stream.deadline, res.Stream.lockTimeout = deadline, s.vars.lockTimeout
		}
	}()
	defer func() {
		// The outer panic barrier (issue #136; execStmt has the inner one,
		// inside the transaction's retry loop). Whatever panicked outside
		// it — a COMMIT, a DDL finish, a session variable — fails this
		// statement with XX000 and, in a transaction block, the block.
		if r := recover(); r != nil {
			res, serr = nil, s.recoveredPanic(r, stmt)
			if s.state == StateOpen {
				s.state = StateFailed
				s.extraDDL, s.extraDrops, s.pendingWipes = nil, nil, nil
			}
		}
	}()
	if serr := s.readOnlyViolation(stmt); serr != nil {
		return nil, serr
	}
	// A failed transaction accepts only ROLLBACK (or COMMIT, which also
	// rolls back) — and ROLLBACK TO SAVEPOINT, which escapes the failed
	// state by restoring the savepoint (PostgreSQL semantics).
	if s.state == StateFailed {
		switch t := stmt.(type) {
		case *parser.Rollback, *parser.Commit:
			s.rollback(ctx)
			return &Result{Tag: "ROLLBACK"}, nil
		case *parser.RollbackToSavepoint:
			return s.execRollbackToSavepoint(ctx, t.Name)
		default:
			return nil, newErrf(CodeInFailedTransaction,
				"current transaction is aborted, commands ignored until end of transaction block")
		}
	}
	// COPY ... TO runs as its query (copyto.go); the result carries the
	// COPY's format.
	if ct, ok := stmt.(*parser.CopyTo); ok {
		inner, co, err := s.planCopyTo(ctx, ct)
		if err != nil {
			if s.state == StateOpen {
				s.state = StateFailed
				s.extraDDL, s.extraDrops, s.pendingWipes = nil, nil, nil
			}
			return nil, ToSQLError(err)
		}
		stmt = inner
		defer func() {
			if res != nil {
				res.Copy = co
			}
		}()
	}
	// A cast to a type that does not exist fails the statement before it
	// runs (and, in a transaction block, the block).
	{
		var txn *kvclient.Txn
		if s.state == StateOpen {
			txn = s.txn
		}
		if serr := s.checkCastTypes(ctx, txn, stmt); serr != nil {
			if s.state == StateOpen {
				s.state = StateFailed
				s.extraDDL, s.extraDrops, s.pendingWipes = nil, nil, nil
			}
			return nil, serr
		}
	}

	switch t := stmt.(type) {
	case *parser.Begin:
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "there is already a transaction in progress")
		}
		var asOf hlc.Timestamp
		if t.AsOf != nil {
			ts, serr := s.resolveTxnAsOf(*t.AsOf)
			if serr != nil {
				return nil, serr
			}
			asOf = ts
		}
		if asOf.IsEmpty() {
			s.txn = s.db.NewTxn("sql")
		} else {
			// BEGIN AS OF SYSTEM TIME: the read-only historical
			// transaction SET TRANSACTION SNAPSHOT also makes.
			s.txn = s.db.NewHistoricalTxn("sql-asof", asOf)
		}
		s.state = StateOpen
		s.tempEndTxn()
		s.txnQueried = false
		s.txnStartedReadOnly = s.vars.defaultReadOnly
		// BEGIN's modes run as the SET TRANSACTION they stand for.
		for _, m := range t.Modes {
			if _, serr := s.execSetVar(ctx, m); serr != nil {
				s.rollback(ctx)
				return nil, serr
			}
		}
		return &Result{Tag: "BEGIN"}, nil

	case *parser.Commit:
		if s.state != StateOpen {
			return &Result{Tag: "COMMIT"}, nil // PG: warning, not error
		}
		// Large-object pages still buffered are written first; if they
		// cannot be, there is nothing to commit.
		if lerr := s.loBeforeCommit(ctx); lerr != nil {
			s.rollback(ctx)
			return nil, ToSQLError(lerr)
		}
		// ON COMMIT DROP and DELETE ROWS of the session's temporary
		// tables, in the transaction itself.
		s.extraDDL, s.extraDrops = nil, nil
		if terr := s.tempPreCommit(ctx, s.txn); terr != nil {
			s.rollback(ctx)
			return nil, ToSQLError(terr)
		}
		s.pendingDDL = append(s.pendingDDL, s.extraDDL...)
		s.pendingDrops = append(s.pendingDrops, s.extraDrops...)
		s.extraDDL, s.extraDrops = nil, nil
		s.tempEndTxn()
		err := s.txn.Commit(ctx)
		s.loEndTxn(ctx, err == nil)
		s.txn = nil
		s.state = StateIdle
		s.endTxnVars()
		s.endTxnCursors(err == nil)
		s.advisoryEndTxn()
		pending, drops := s.pendingDDL, s.pendingDrops
		if err != nil {
			s.forgetDDL()
			s.pendingDDL, s.extraDDL, s.pendingWipes = nil, nil, nil
			s.pendingDrops, s.extraDrops = nil, nil
			s.keyLeaseAfterCommit = false
			s.discardNotify()
			return nil, ToSQLError(err)
		}
		s.pendingDDL, s.pendingDrops = nil, nil
		// The transaction is durable: its subscriptions take effect and
		// its notifications go out, whatever the steps below report.
		s.commitNotify()
		// Schema changes are visible everywhere once COMMIT returns: drain
		// lease adoption for every table this transaction altered.
		for _, name := range pending {
			if derr := s.cat.FinishDDLIn(s.rctx(ctx), s.database, name); derr != nil {
				return nil, ToSQLError(derr)
			}
		}
		if derr := s.finishDrops(ctx, drops); derr != nil {
			return nil, derr
		}
		s.runPendingWipes(ctx)
		if serr := s.finishKeyLease(ctx); serr != nil {
			return nil, serr
		}
		return &Result{Tag: "COMMIT"}, nil

	case *parser.Rollback:
		s.rollback(ctx)
		return &Result{Tag: "ROLLBACK"}, nil

	case *parser.Savepoint:
		if s.state != StateOpen {
			return nil, newErrf(CodeNoActiveTransaction, "SAVEPOINT can only be used in transaction blocks")
		}
		if err := s.txn.Savepoint(t.Name); err != nil {
			return nil, ToSQLError(err)
		}
		s.notifySavepoint(t.Name)
		s.advisorySavepoint(t.Name)
		return &Result{Tag: "SAVEPOINT"}, nil

	case *parser.ReleaseSavepoint:
		if s.state != StateOpen {
			return nil, newErrf(CodeNoActiveTransaction, "RELEASE SAVEPOINT can only be used in transaction blocks")
		}
		if err := s.txn.ReleaseSavepoint(t.Name); err != nil {
			return nil, newErrf(CodeInvalidSavepoint, "%v", err)
		}
		s.notifyReleaseSavepoint(t.Name)
		s.advisoryReleaseSavepoint(t.Name)
		return &Result{Tag: "RELEASE"}, nil

	case *parser.RollbackToSavepoint:
		if s.state != StateOpen {
			return nil, newErrf(CodeNoActiveTransaction, "ROLLBACK TO SAVEPOINT can only be used in transaction blocks")
		}
		return s.execRollbackToSavepoint(ctx, t.Name)

	case *parser.SetVar:
		if len(t.Name) > 5 && t.Name[:5] == "show:" {
			name, value, ok := s.setting(t.Name[5:])
			if !ok {
				return nil, newErrf(CodeUndefinedObject, "unrecognized configuration parameter %q", t.Name[5:])
			}
			return &Result{Columns: []ResultColumn{{Name: name, Type: types.String}}, Rows: [][]types.Datum{{types.NewString(value)}}, Tag: "SHOW"}, nil
		}
		return s.execSetVar(ctx, t)
	case *parser.Use:
		if serr := s.UseDatabase(ctx, t.Name); serr != nil {
			return nil, serr
		}
		return &Result{Tag: "SET"}, nil

	case *parser.Listen, *parser.Unlisten, *parser.Notify:
		return s.execListenNotify(stmt)
	case *parser.Prepare:
		return s.execPrepare(t)
	case *parser.Deallocate:
		return s.execDeallocate(t)
	case *parser.ExecutePrepared:
		return s.execExecutePrepared(ctx, t, params)
	case *parser.SetTransactionSnapshot:
		return s.execSetTransactionSnapshot(t)
	case *parser.SetTransactionAsOf:
		return s.execSetTransactionAsOf(t)
	case *parser.DeclareCursor:
		return s.execDeclareCursor(ctx, t, params)
	case *parser.Fetch:
		return s.execFetch(t)
	case *parser.CloseCursor:
		return s.execCloseCursor(t)
	case *parser.Discard:
		return s.execDiscard(ctx, t)

	default:
		if s.state == StateOpen {
			return s.executeData(ctx, stmt, params)
		}
		// A statement outside a block is its own transaction: what it
		// notifies (pg_notify) goes out once it has committed. A streamed
		// result has taken its notifications with it by now, and
		// publishes them itself when it is drained.
		s.discardNotify()
		res, serr := s.executeData(ctx, stmt, params)
		// The statement was its transaction: its transaction-level
		// advisory locks go with it.
		s.advisoryEndTxn()
		if serr != nil {
			s.discardNotify()
			return nil, serr
		}
		s.commitNotify()
		return res, nil
	}
}

// executeData runs a data statement in the session transaction (explicit)
// or an auto-retrying implicit one.
func (s *Session) executeData(ctx context.Context, stmt parser.Statement, params []types.Datum) (*Result, *Error) {
	if s.state == StateOpen {
		s.txnQueried = true
	}
	// The key service's statements (keyservice.go) change the datax_keys
	// row in transactions of their own.
	if isKeyStatement(stmt) {
		return s.execKeyStatement(ctx, stmt)
	}
	// CREATE INDEX is a multi-transaction state machine (publish
	// write-only → drain → backfill+publish → drain), so — like
	// PostgreSQL's CREATE INDEX CONCURRENTLY — it cannot run inside an
	// explicit transaction block.
	// ALTER TABLE ... SET (shards = N) is the online re-shard: the same
	// multi-transaction state-machine shape, with the same restrictions.
	// ADD CONSTRAINT, VALIDATE CONSTRAINT and SET NOT NULL publish, drain
	// and then sweep the existing rows: multi-transaction, like CREATE
	// INDEX.
	// Several actions of which one needs the table's rows, and ADD
	// COLUMN with a volatile default (or another that needs them), run
	// online too (alter_multi.go); several that need nothing but the
	// descriptor run below, in the statement's transaction.
	if at, ok := stmt.(*parser.AlterTable); ok {
		r, serr := s.rewriteAddPrimaryKey(ctx, at)
		if serr != nil {
			return nil, serr
		}
		stmt = r
	}
	if at, ok := stmt.(*parser.AlterTable); ok && alterRunsOnline(at) {
		if at.IfExists && !s.tableExists(ctx, at.Table) {
			return &Result{Tag: "ALTER TABLE", Notices: []string{fmt.Sprintf("relation %q does not exist, skipping", at.Table)}}, nil
		}
		if s.state == StateOpen {
			return nil, alterInBlockError(alterCmds(at))
		}
		return s.execAlterTableOnline(ctx, at)
	}
	if at, ok := stmt.(*parser.AlterTable); ok && at.IfExists && (at.AddConstraint != nil || at.ValidateConstraint != "" || at.SetNotNull != "" || at.SetOptions != nil || at.SetType != nil) && !s.tableExists(ctx, at.Table) {
		return &Result{Tag: "ALTER TABLE"}, nil
	}
	// CREATE TABLE ... AS and ALTER COLUMN TYPE are multi-transaction
	// statements too: refused in a block, admin-only.
	if ct, ok := stmt.(*parser.CreateTable); ok && ct.As != nil {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "CREATE TABLE ... AS cannot run inside a transaction block")
		}
		return s.execCreateTableAs(ctx, ct, params)
	}
	if at, ok := stmt.(*parser.AlterTable); ok && at.SetType != nil {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "ALTER TABLE ... ALTER COLUMN TYPE cannot run inside a transaction block")
		}
		if serr := s.checkTableOwnerNoTxn(ctx, at.Table); serr != nil {
			return nil, serr
		}
		return s.execRetypeOnline(ctx, at)
	}
	if at, ok := stmt.(*parser.AlterTable); ok && (at.AddConstraint != nil || at.ValidateConstraint != "" || at.SetNotNull != "") {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "ALTER TABLE ... ADD CONSTRAINT, VALIDATE CONSTRAINT and SET NOT NULL cannot run inside a transaction block")
		}
		if serr := s.checkTableOwnerNoTxn(ctx, at.Table); serr != nil {
			return nil, serr
		}
		switch {
		case at.AddConstraint != nil:
			return s.execAddConstraintOnline(ctx, at)
		case at.ValidateConstraint != "":
			return s.execValidateConstraintOnline(ctx, at)
		default:
			return s.execSetNotNullOnline(ctx, at)
		}
	}

	if at, ok := stmt.(*parser.AlterTable); ok && (at.SplitAt != nil || at.UnsplitAt != nil || at.UnsplitAll) {
		// A split is a range operation, not a descriptor write: it runs
		// outside any transaction, like the manual datax debug split.
		// So does giving one up.
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "ALTER TABLE ... SPLIT AT and UNSPLIT cannot run inside a transaction block")
		}
		if at.IfExists && !s.tableExists(ctx, at.Table) {
			return &Result{Tag: "ALTER TABLE"}, nil
		}
		if serr := s.checkTableOwnerNoTxn(ctx, at.Table); serr != nil {
			return nil, serr
		}
		var res *Result
		var err error
		if at.SplitAt != nil {
			res, err = s.execSplitAt(ctx, at, params)
		} else {
			res, err = s.execUnsplit(ctx, at, params)
		}
		if err != nil {
			return nil, ToSQLError(err)
		}
		return res, nil
	}
	if at, ok := stmt.(*parser.AlterTable); ok && (at.ResetOptions != nil || at.Purge || at.RotateKey) {
		// RESET is one descriptor write and PURGE a range operation;
		// both run outside any transaction, like SET (...) and SPLIT AT.
		// So does ROTATE ENCRYPTION KEY, which rotates the key in a
		// transaction of the key store's and then schedules its job.
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "ALTER TABLE ... RESET, PURGE and ROTATE ENCRYPTION KEY cannot run inside a transaction block")
		}
		if at.IfExists && !s.tableExists(ctx, at.Table) {
			return &Result{Tag: "ALTER TABLE"}, nil
		}
		if serr := s.checkTableOwnerNoTxn(ctx, at.Table); serr != nil {
			return nil, serr
		}
		switch {
		case at.Purge:
			return s.execPurge(ctx, at, params)
		case at.RotateKey:
			return s.execRotateTableKey(ctx, at)
		}
		if serr := refuseResetSeal(at.ResetOptions); serr != nil {
			return nil, serr
		}
		return s.execResetOptions(ctx, at)
	}
	if at, ok := stmt.(*parser.AlterTable); ok && at.SetOptions != nil {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "ALTER TABLE ... SET (...) cannot run inside a transaction block")
		}
		if _, bare := catalog.SplitTableName(at.Table); bare == catalog.AuditTableName && !s.system {
			return s.execSetAuditRetention(ctx, at)
		}
		if serr := s.checkTableOwnerNoTxn(ctx, at.Table); serr != nil {
			return nil, serr
		}
		if s.namesTempTable(ctx, at.Table) {
			return nil, newErrf(CodeFeatureNotSupported, "ALTER TABLE ... SET (...) is not supported on temporary tables")
		}
		for k := range at.SetOptions {
			if sealOption(k) {
				return s.execAlterSeal(ctx, at)
			}
		}
		// The options any table takes are set first, in one descriptor
		// write; what is left is the time-series pair below.
		general, rest := map[string]string{}, map[string]string{}
		for k, v := range at.SetOptions {
			if generalTableOption(k) {
				general[k] = v
			} else {
				rest[k] = v
			}
		}
		if len(general) > 0 {
			if serr := s.execSetGeneralOptions(ctx, at.Table, general); serr != nil {
				return nil, serr
			}
			if len(rest) == 0 {
				return &Result{Tag: "ALTER TABLE"}, nil
			}
			at = &parser.AlterTable{Table: at.Table, SetOptions: rest}
		}
		if _, ok := at.SetOptions["retention"]; ok {
			res, rerr := s.execSetRetention(ctx, at)
			if rerr != nil {
				return nil, rerr
			}
			if _, reshard := at.SetOptions["shards"]; !reshard {
				return res, nil
			}
			rest := map[string]string{}
			for k, v := range at.SetOptions {
				if k != "retention" {
					rest[k] = v
				}
			}
			at = &parser.AlterTable{Table: at.Table, SetOptions: rest}
		}
		return s.execReshardOnline(ctx, at)
	}

	if ci, ok := stmt.(*parser.CreateIndex); ok && s.state != StateOpen {
		// Inside a block, CREATE INDEX runs with the block's other
		// statements (execCreateIndexInTxn): on a table created in the
		// block, or refused.
		if serr := s.checkTableOwnerNoTxn(ctx, ci.Table); serr != nil {
			return nil, serr
		}
		return s.execCreateIndexOnline(ctx, ci)
	}

	// ANALYZE sweeps tables at a frozen timestamp in paced chunks outside
	// any transaction, so — like the other multi-transaction statements —
	// it cannot run inside an explicit transaction block.
	if v, ok := stmt.(*parser.Vacuum); ok {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "VACUUM cannot run inside a transaction block")
		}
		return s.execVacuum(ctx, v)
	}
	if an, ok := stmt.(*parser.Analyze); ok {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "ANALYZE cannot run inside a transaction block")
		}
		var aerr error
		if err := s.db.RunTxn(ctx, "admin-check", func(ctx context.Context, txn *kvclient.Txn) error {
			aerr = s.checkAdmin(ctx, txn)
			return nil
		}); err != nil {
			return nil, ToSQLError(err)
		}
		if aerr != nil {
			return nil, ToSQLError(aerr)
		}
		return s.execAnalyze(ctx, an)
	}

	// AS OF SYSTEM TIME pins a SELECT to a fixed past timestamp: it runs
	// in its own read-only historical transaction (servable by follower
	// replicas), so it cannot join an explicit transaction's timestamp.
	if sel, ok := stmt.(*parser.Select); ok && (sel.AsOf != "" || sel.AsOfMaxStaleness != "" || sel.AsOfMinTimestamp != "") {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "AS OF SYSTEM TIME cannot run inside a transaction block")
		}
		if sel.ForUpdate {
			return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed with AS OF SYSTEM TIME")
		}
		ts, aerr := s.resolveAsOfClause(parser.AsOfClause{Exact: sel.AsOf, MaxStaleness: sel.AsOfMaxStaleness, MinTimestamp: sel.AsOfMinTimestamp, NearestOnly: sel.AsOfNearestOnly})
		if aerr != nil {
			return nil, aerr
		}
		// Historical reads plan against the descriptor current AT their
		// timestamp (the catalog reads it through the historical txn), so
		// a read below a re-shard routes to the retired pre-swap layout.
		// The guard refuses only when that layout has already been wiped
		// by the janitor — its rows are gone, and the read would silently
		// come back short. Checked per referenced table; best-effort with
		// a short timeout on the current-descriptor read (a partitioned
		// leader must not hang a bounded-staleness read whose whole point
		// is serving locally).
		txn := s.db.NewHistoricalTxn("sql-asof", ts)
		if sel.Table != "" {
			if serr := s.checkHistoricalLayout(ctx, txn, sel.Table); serr != nil {
				return nil, serr
			}
		}
		for i := range sel.Joins {
			if serr := s.checkHistoricalLayout(ctx, txn, sel.Joins[i].Table); serr != nil {
				return nil, serr
			}
		}
		s.streamTop = true
		s.asofLocal, s.asofTS = sel.AsOfMaxStaleness != "" || sel.AsOfMinTimestamp != "", ts
		res, serr := s.execSelect(ctx, txn, sel, params)
		s.streamTop = false
		s.asofLocal, s.asofTS = false, hlc.Timestamp{}
		if serr != nil {
			_ = txn.Rollback(ctx)
			return nil, ToSQLError(serr)
		}
		if res.Stream != nil {
			// The stream commits the historical transaction once drained
			// (no restarts: a pinned read has nothing to retry).
			res.Stream.attachImplicit(txn, nil, nil)
			res.Stream.notifs = s.takeNotify()
			res.Stream.attempt = 20
			return res, nil
		}
		_ = txn.Commit(ctx) // read-only: releases nothing, marks finished
		return res, nil
	}

	if s.state == StateOpen {
		s.extraDDL, s.extraDrops = nil, nil
		_, s.streamTop = stmt.(*parser.Select)
		res, err := s.execStmt(ctx, s.txn, stmt, params)
		s.streamTop = false
		if err != nil {
			// Any error fails the explicit transaction (PG semantics).
			s.state = StateFailed
			s.extraDDL, s.extraDrops, s.pendingWipes = nil, nil, nil
			return nil, ToSQLError(err)
		}
		if res.Stream != nil {
			// An error while the client pulls fails the block too (if it
			// is still the block the stream ran in).
			res.Stream.attachExplicit(func() {
				if s.state == StateOpen {
					s.state = StateFailed
				}
			})
		}
		// The drain runs at COMMIT, under the search path in force then
		// (a SET LOCAL has ended by then): each table is recorded by a
		// name that resolves to it alone.
		if name := ddlTableName(stmt); name != "" {
			s.pendingDDL = append(s.pendingDDL, s.drainName(ctx, s.txn, name))
		}
		for _, name := range s.extraDDL {
			s.pendingDDL = append(s.pendingDDL, s.drainName(ctx, s.txn, name))
		}
		s.pendingDrops = append(s.pendingDrops, s.extraDrops...)
		s.extraDDL, s.extraDrops = nil, nil
		return res, nil
	}
	if sel, ok := stmt.(*parser.Select); ok && s.streaming {
		return s.executeStreamingSelect(ctx, sel, params)
	}
	var res *Result
	err := s.db.RunTxn(ctx, "sql-implicit", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		s.extraDDL, s.extraDrops, s.pendingWipes = nil, nil, nil
		s.keyLeaseAfterCommit = false
		s.discardNotify() // a retry starts the statement over
		s.advisoryRestart()
		s.tempEndTxn()
		res, err = s.execStmt(ctx, txn, stmt, params)
		if err == nil {
			err = s.tempPreCommit(ctx, txn)
		}
		if err == nil && TestingBeforeImplicitCommit != nil {
			TestingBeforeImplicitCommit()
		}
		return err
	})
	s.tempEndTxn()
	if err != nil {
		s.extraDDL, s.extraDrops, s.pendingWipes = nil, nil, nil
		return nil, ToSQLError(err)
	}
	names, drops := s.extraDDL, s.extraDrops
	s.extraDDL, s.extraDrops = nil, nil
	if name := ddlTableName(stmt); name != "" {
		names = append([]string{name}, names...)
	}
	for _, name := range names {
		if derr := s.cat.FinishDDLIn(s.rctx(ctx), s.database, name); derr != nil {
			return nil, ToSQLError(derr)
		}
	}
	if derr := s.finishDrops(ctx, drops); derr != nil {
		return nil, derr
	}
	s.runPendingWipes(ctx)
	if serr := s.finishKeyLease(ctx); serr != nil {
		return nil, serr
	}
	return res, nil
}

// executeStreamingSelect runs a SELECT in an implicit transaction the
// way RunTxn would, except that a streamed result keeps the transaction
// open for the stream, which commits it when drained and re-runs the
// statement itself on a retryable error before the first flush.
func (s *Session) executeStreamingSelect(ctx context.Context, sel *parser.Select, params []types.Datum) (*Result, *Error) {
	txn := s.db.NewTxn("sql-implicit")
	txn.EnablePipelining()
	for attempt := 0; ; attempt++ {
		s.streamTop = true
		s.discardNotify() // a retry starts the statement over
		s.advisoryRestart()
		res, err := s.execStmt(ctx, txn, sel, params)
		s.streamTop = false
		if err == nil && res.Stream != nil {
			res.Stream.attachImplicit(txn, sel, params)
			res.Stream.notifs = s.takeNotify()
			res.Stream.attempt = attempt
			return res, nil
		}
		if err == nil {
			err = txn.Commit(ctx)
		}
		if err == nil {
			return res, nil
		}
		_ = txn.Rollback(ctx)
		if !kvclient.IsRetryable(err) || ctx.Err() != nil || attempt >= 20 {
			return nil, ToSQLError(err)
		}
		next := s.db.NewTxn("sql-implicit")
		next.EnablePipelining()
		next.BumpPriority(txn)
		txn = next
		select {
		case <-ctx.Done():
			return nil, ToSQLError(ctx.Err())
		case <-time.After(time.Duration(attempt+1) * 20 * time.Millisecond):
		}
	}
}

// noteDrop records a table or view a statement dropped, for the
// post-commit drain of the drop: until it is done no gateway may still
// plan against the descriptor or commit a statement planned against it
// (issue #377). Every path that drops a table descriptor notes it.
func (s *Session) noteDrop(d *catalog.TableDescriptor) {
	if d != nil {
		s.extraDrops = append(s.extraDrops, d.ID)
	}
}

// finishDrops drains the drops a committed transaction made.
func (s *Session) finishDrops(ctx context.Context, ids []uint64) *Error {
	seen := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		if err := s.cat.FinishDrop(ctx, id); err != nil {
			return ToSQLError(err)
		}
	}
	return nil
}

// noteDDL records another table a statement's DDL changed (a foreign
// key's parent, a dropped table's children), for the post-commit drain.
func (s *Session) noteDDL(name string) {
	s.extraDDL = append(s.extraDDL, name)
}

func (s *Session) rollback(ctx context.Context) {
	if s.txn != nil {
		_ = s.txn.Rollback(ctx)
		s.loEndTxn(ctx, false)
		s.txn = nil
	}
	s.endTxnVars()
	s.endTxnCursors(false)
	s.forgetDDL()
	s.pendingDDL, s.extraDDL, s.pendingWipes = nil, nil, nil
	s.pendingDrops, s.extraDrops = nil, nil
	s.keyLeaseAfterCommit = false
	s.discardNotify()
	s.advisoryEndTxn()
	s.tempEndTxn()
	s.state = StateIdle
}

// ddlTouched reports whether the open transaction's DDL changed name.
func (s *Session) ddlTouched(name string) bool {
	bare := catalog.BareName(name)
	for _, lists := range [][]string{s.pendingDDL, s.extraDDL} {
		for _, n := range lists {
			if b := catalog.BareName(n); strings.EqualFold(b, bare) {
				return true
			}
		}
	}
	return false
}

// forgetDDL drops the cache entries of the tables the transaction's DDL
// touched (a rollback, or a failed commit): what the cache holds for
// them may be the transaction's own uncommitted view.
func (s *Session) forgetDDL() {
	for _, lists := range [][]string{s.pendingDDL, s.extraDDL} {
		for _, n := range lists {
			s.cat.Invalidate(n)
		}
	}
}

// evalExpr evaluates an expression given an optional row context.
// exprEnv resolves column references during expression evaluation: a
// single table's descriptor+row, or a joined row with side-qualified
// names. Everything else about evaluation (literals, params, functions,
// paths, arithmetic) is environment-independent.
type exprEnv interface {
	col(name string) (types.Datum, error)
}

// tableEnv is the single-table environment: descriptor + row map. A nil
// descriptor or row means column references are simply not available
// (constant-only contexts like access planning).
type tableEnv struct {
	desc *catalog.TableDescriptor
	row  map[catalog.ColumnID]types.Datum
}

func (t tableEnv) col(name string) (types.Datum, error) {
	if t.desc == nil || t.row == nil {
		return types.Datum{}, newErrf(CodeUndefinedColumn, "column %q is not available in this context", name)
	}
	col, ok := t.desc.Col(name)
	if !ok {
		return types.Datum{}, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
	}
	d, ok := t.row[col.ID]
	if !ok {
		d = types.DNull
	}
	return d, nil
}

// evalExpr evaluates an expression against a single table's row — the
// historical signature, kept for its many call sites; join evaluation
// passes a joinEnv to evalExprEnv directly.
func evalExpr(e parser.Expr, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) (types.Datum, error) {
	return evalExprEnv(e, tableEnv{desc: desc, row: row}, params)
}

func evalExprEnv(e parser.Expr, env exprEnv, params []types.Datum) (types.Datum, error) {
	var base types.Datum
	switch {
	case e.Left != nil:
		d, err := evalExprEnv(*e.Left, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		base = d
	case e.Case != nil:
		d, err := evalCase(e.Case, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		base = d
	case e.Func != "":
		d, err := evalFunc(e, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		base = d
	case e.Lit != nil:
		base = *e.Lit
	case e.Param > 0:
		if e.Param > len(params) {
			return types.Datum{}, newErrf(CodeInvalidParameter, "missing value for parameter $%d", e.Param)
		}
		base = params[e.Param-1]
	case e.Column != "":
		d, err := env.col(e.Column)
		if err != nil {
			return types.Datum{}, err
		}
		base = d
	case e.Cmp != nil:
		// A predicate as a value is three-valued: a comparison with a
		// NULL operand is NULL, not false (WHERE's rule).
		ok, null, err := cond3(*e.Cmp, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		if null {
			base = types.DNull
		} else {
			base = types.NewBool(ok)
		}
	case e.Sub != nil:
		// Scalar subqueries are evaluated and spliced before execution.
		return types.Datum{}, newErrf(CodeInternal, "unresolved scalar subquery")
	case e.Agg != nil:
		// The grouped executor replaces every aggregate call it computes;
		// one reaching evaluation stands where none may (WHERE, ON, a
		// row-by-row select list).
		return types.Datum{}, newErrf(CodeGrouping, "aggregate function %s() is not allowed here", strings.ToLower(e.Agg.Agg))
	default:
		return types.Datum{}, newErrf(CodeInternal, "empty expression")
	}
	if len(e.Path) > 0 {
		var err error
		base, err = applyPath(base, e.Path)
		if err != nil {
			return types.Datum{}, err
		}
	}
	if e.BinOp != "" {
		rhs, err := evalExprEnv(*e.Right, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		handled := false
		if e.BinOp == "||" {
			// jsonb || jsonb merges (issue #342).
			var d types.Datum
			if d, handled, err = evalJSONBConcat(e, base, rhs); err != nil {
				return types.Datum{}, err
			}
			if handled {
				base = d
			}
			if !handled {
				// An untyped literal beside a bytea is a bytea
				// ('\x01'::bytea || '\x02').
				base, rhs = byteaConcatOperands(e, base, rhs)
			}
		}
		if !handled {
			if base, err = applyArithIn(base, e.BinOp, rhs, e.Zone); err != nil {
				return types.Datum{}, err
			}
		}
	}
	if e.Cast != "" {
		if name := builtins.CastBaseType(e.Cast); name != strings.ToLower(name) {
			// A quoted mixed-case name ("Date", "Role") is a user-defined
			// type (an enum, checked to exist before execution), never the
			// builtin its folded spelling would name: the value passes
			// through, as a cast to any other enum does.
			return base, nil
		}
		d, err := builtins.CastIn(base, e.Cast, e.Zone)
		return d, builtinErr(err)
	}
	return base, nil
}

// applyArithIn evaluates l op r under the session's TimeZone (nil:
// UTC), which timestamp-with-time-zone arithmetic and || rendering
// follow.
func applyArithIn(l types.Datum, op string, r types.Datum, loc *time.Location) (types.Datum, error) {
	if l.Null || r.Null {
		return types.DNull, nil
	}
	switch {
	case op == "-" && l.Fam == types.Jsonb:
		d, _, err := builtins.JSONBDelete(l, r)
		return d, builtinErr(err)
	case op == "#-":
		d, err := builtins.JSONBDeletePath(l, r)
		return d, builtinErr(err)
	case builtins.IsBitwiseOp(op):
		d, err := builtins.Bitwise(l, op, r)
		return d, builtinErr(err)
	}
	if d, ok, err := builtins.DateArithIn(l, op, r, loc); ok {
		return d, builtinErr(err)
	}
	if l.Fam == types.Vector || r.Fam == types.Vector || builtins.IsVectorOp(op) {
		if d, ok, err := builtins.VectorOp(l, op, r); ok {
			return d, builtinErr(err)
		}
	}
	switch op {
	case "||":
		if l.Fam == types.Tsvector || r.Fam == types.Tsvector {
			d, err := builtins.ConcatTSVectors(l, r)
			return d, builtinErr(err)
		}
		if l.Fam.IsArray() || r.Fam.IsArray() {
			d, err := builtins.ConcatArrays(l, r)
			return d, builtinErr(err)
		}
		if l.Fam == types.Bytes && r.Fam == types.Bytes {
			// bytea || bytea is bytea (byteacat), not the hex texts
			// joined.
			return types.NewBytes([]byte(l.S + r.S)), nil
		}
		// A CHAR(n) operand concatenates trimmed, as PostgreSQL's
		// bpchar-to-text cast strips the padding.
		return types.NewString(concatTextIn(l, loc) + concatTextIn(r, loc)), nil
	case "%":
		if d, ok, err := types.DecimalSpecialArith(l, op, r); ok && r.Fam != types.Float && l.Fam != types.Float {
			if err != nil {
				return types.Datum{}, newErrf(CodeDivisionByZero, "%v", err)
			}
			return d, nil
		}
		d, err := builtins.Mod(l, r)
		return d, builtinErr(err)
	case "^":
		d, err := builtins.Power(l, r)
		return d, builtinErr(err)
	}
	if l.Fam == types.Int && r.Fam == types.Int {
		// Integer arithmetic overflows to an error, never wraps.
		switch op {
		case "+":
			s := l.I + r.I
			if (s > l.I) != (r.I > 0) && r.I != 0 {
				return types.Datum{}, newErrf(CodeNumericValueOutOfRange, "integer out of range")
			}
			return types.NewInt(s), nil
		case "-":
			s := l.I - r.I
			if (s < l.I) != (r.I > 0) && r.I != 0 {
				return types.Datum{}, newErrf(CodeNumericValueOutOfRange, "integer out of range")
			}
			return types.NewInt(s), nil
		case "*":
			if l.I != 0 && r.I != 0 {
				p := l.I * r.I
				if p/r.I != l.I || (l.I == -1 && r.I == math.MinInt64) || (r.I == -1 && l.I == math.MinInt64) {
					return types.Datum{}, newErrf(CodeNumericValueOutOfRange, "integer out of range")
				}
				return types.NewInt(p), nil
			}
			return types.NewInt(0), nil
		case "/":
			// Integer division truncates (PostgreSQL semantics).
			if r.I == 0 {
				return types.Datum{}, newErrf(CodeDivisionByZero, "division by zero")
			}
			return types.NewInt(l.I / r.I), nil
		}
	}
	// Exact decimal arithmetic when either side is DECIMAL and the other
	// lifts exactly (DECIMAL or INT). A FLOAT operand falls through to
	// float arithmetic — mixing binary rounding in and calling the result
	// exact would be a lie.
	if (l.Fam == types.Decimal || r.Fam == types.Decimal) &&
		(l.Fam == types.Decimal || l.Fam == types.Int) &&
		(r.Fam == types.Decimal || r.Fam == types.Int) {
		if d, ok, err := types.DecimalSpecialArith(l, op, r); ok {
			if err != nil {
				return types.Datum{}, newErrf(CodeDivisionByZero, "%v", err)
			}
			return d, nil
		}
		ld, err := l.Coerce(types.Decimal)
		if err != nil {
			return types.Datum{}, newErrf(CodeInternal, "%v", err)
		}
		rd, err := r.Coerce(types.Decimal)
		if err != nil {
			return types.Datum{}, newErrf(CodeInternal, "%v", err)
		}
		lv, err := ld.DecimalVal()
		if err != nil {
			return types.Datum{}, newErrf(CodeInternal, "%v", err)
		}
		rv, err := rd.DecimalVal()
		if err != nil {
			return types.Datum{}, newErrf(CodeInternal, "%v", err)
		}
		switch op {
		case "+":
			return types.NewDecimal(decimal.Add(lv, rv).String()), nil
		case "-":
			return types.NewDecimal(decimal.Sub(lv, rv).String()), nil
		case "*":
			return types.NewDecimal(decimal.Mul(lv, rv).String()), nil
		case "/":
			q, err := decimal.DivQuantize(lv, rv, 6)
			if err != nil {
				return types.Datum{}, newErrf(CodeDivisionByZero, "division by zero")
			}
			return types.NewDecimal(q.String()), nil
		}
	}
	lf, err := l.Coerce(types.Float)
	if err != nil {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "unsupported operand for %s: %s", op, l.Fam)
	}
	rf, err := r.Coerce(types.Float)
	if err != nil {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "unsupported operand for %s: %s", op, r.Fam)
	}
	switch op {
	case "+":
		return types.NewFloat(lf.F + rf.F), nil
	case "-":
		return types.NewFloat(lf.F - rf.F), nil
	case "*":
		return types.NewFloat(lf.F * rf.F), nil
	case "/":
		if rf.F == 0 {
			return types.Datum{}, newErrf(CodeDivisionByZero, "division by zero")
		}
		return types.NewFloat(lf.F / rf.F), nil
	}
	return types.Datum{}, newErrf(CodeFeatureNotSupported, "unsupported operator %q", op)
}

// evalFunc evaluates a builtin scalar call ("now" never reaches here —
// the session splices it before execution).
func evalFunc(e parser.Expr, env exprEnv, params []types.Datum) (types.Datum, error) {
	args := make([]types.Datum, len(e.Args))
	for i, a := range e.Args {
		d, err := evalExprEnv(a, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		args[i] = d
	}
	if b, ok := builtins.Lookup(e.Func); ok && !b.Session {
		d, err := b.CallIn(args, e.Zone)
		if err != nil {
			var be *builtins.Error
			if errors.As(err, &be) {
				return types.Datum{}, newErrf(be.Code, "%s", be.Msg)
			}
			return types.Datum{}, err
		}
		return d, nil
	}
	switch e.Func {
	case "quote_ident":
		if args[0].Null {
			return types.DNull, nil
		}
		return types.NewString(quoteIdentifier(args[0].Text())), nil
	case "pg_typeof":
		return types.NewString(vtable.TypeName(args[0].Fam)), nil
	case "to_regtype":
		// The type's OID when the name is a type's, NULL when it is not:
		// how a client finds the vector type, whose OID no client knows
		// in advance (to_regtype('vector')::oid).
		if args[0].Null {
			return types.DNull, nil
		}
		name := strings.ToLower(strings.TrimSpace(args[0].Text()))
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			name = name[i+1:]
		}
		fam, err := types.ParseType(name)
		if err != nil {
			return types.DNull, nil
		}
		return types.NewInt(vtable.TypeOID(fam)), nil
	case "format_type":
		if args[0].Null {
			return types.DNull, nil
		}
		return types.NewString(vtable.FormatTypeOID(args[0].I)), nil
	case "now":
		return types.Datum{}, newErrf(CodeInternal, "now() was not resolved before execution")
	case "array_to_string":
		if args[0].Null || args[1].Null {
			return types.DNull, nil
		}
		return types.NewString(strings.Join(arrayElems(args[0].Text()), args[1].Text())), nil
	case "pg_size_pretty":
		if args[0].Null {
			return types.DNull, nil
		}
		n, err := args[0].Coerce(types.Int)
		if err != nil {
			return types.DNull, nil
		}
		return types.NewString(sizePretty(n.I)), nil
	case "pg_table_size", "pg_total_relation_size", "pg_relation_size", "pg_database_size",
		"pg_get_function_result", "pg_get_function_arguments", "pg_get_function_identity_arguments",
		"pg_get_functiondef", "pg_get_triggerdef", "pg_get_ruledef":
		return types.DNull, nil
	case rowSessionFunc:
		return callRowFunc(args)
	case unrecognizedSettingFunc:
		// current_setting(name [, missing_ok]) per row, for a name that
		// is none of the session's settings: NULL for a NULL name or
		// with missing_ok, else 42704 as PostgreSQL raises it.
		if args[0].Null {
			return types.DNull, nil
		}
		if len(args) == 2 && !args[1].Null {
			missingOK, err := args[1].Coerce(types.Bool)
			if err != nil {
				return types.Datum{}, newErrf(CodeInvalidTextRepresentation, "current_setting: missing_ok: %v", err)
			}
			if !missingOK.Null && missingOK.B {
				return types.DNull, nil
			}
		}
		return types.Datum{}, newErrf(CodeUndefinedObject, "unrecognized configuration parameter %q", args[0].Text())
	case "pg_char_to_encoding":
		return types.NewInt(6), nil // UTF8
	case "getdatabaseencoding":
		return types.NewString("UTF8"), nil
	case "pg_tablespace_location":
		return types.NewString(""), nil
	case "pg_type_is_visible", "pg_function_is_visible":
		return types.NewBool(true), nil
	}
	return types.Datum{}, newErrf(CodeUndefinedFunction, "unknown function %q", e.Func)
}

// sizePretty renders bytes the way pg_size_pretty does.
func sizePretty(n int64) string {
	units := []string{"bytes", "kB", "MB", "GB", "TB", "PB"}
	v := float64(n)
	i := 0
	for i < len(units)-1 && (v >= 10240 || v <= -10240) {
		v /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d bytes", n)
	}
	return fmt.Sprintf("%.0f %s", v, units[i])
}

// matchesWhere evaluates the conjunction against a full row.
func matchesWhere(where []parser.Comparison, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) (bool, error) {
	for _, cmp := range where {
		// Constant conjuncts (evaluated EXISTS subqueries) bind no column.
		if cmp.Op == "TRUE" {
			continue
		}
		if cmp.Op == "FALSE" {
			return false, nil
		}
		if cmp.Expr != nil {
			// Computed left-hand side: evaluate both sides and compare raw
			// (Compare lifts numerics; no column type to coerce toward).
			ok, err := condsHold([]parser.Comparison{cmp}, tableEnv{desc: desc, row: row}, params)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			continue
		}
		if cmp.Op == "OR" {
			matched := false
			for _, disjunct := range cmp.Or {
				for _, inner := range disjunct {
					if inner.Sub != nil {
						return false, newErrf(CodeInternal, "an IN or EXISTS subquery inside OR was not resolved before execution")
					}
				}
				ok, err := matchesWhere(disjunct, desc, row, params)
				if err != nil {
					return false, err
				}
				if ok {
					matched = true
					break
				}
			}
			if !matched {
				return false, nil
			}
			continue
		}
		col, ok := desc.Col(cmp.Column)
		if !ok {
			return false, newErrf(CodeUndefinedColumn, "column %q does not exist", cmp.Column)
		}
		lhs, ok := row[col.ID]
		if !ok {
			lhs = types.DNull
		}
		// A ->/->> path replaces the column value and its comparison type.
		cmpType := col.Type
		if len(cmp.Path) > 0 {
			var perr error
			lhs, perr = applyPath(lhs, cmp.Path)
			if perr != nil {
				return false, perr
			}
			cmpType = pathResultType(cmp.Path)
		}
		if cmp.Op == "IS NULL" || cmp.Op == "IS NOT NULL" {
			if lhs.Null != (cmp.Op == "IS NULL") {
				return false, nil
			}
			continue
		}
		if nullAwareOp(cmp.Op) {
			var rhs types.Datum
			if !isBoolTestOp(cmp.Op) {
				v, err := evalExpr(cmp.Value, desc, row, params)
				if err != nil {
					return false, err
				}
				rhs = v
			}
			ok, err := applyNullAware(cmp.Op, lhs, rhs)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			continue
		}
		if cmp.Op == "IN" || cmp.Op == "NOT IN" {
			match, err := matchesIn(cmp, cmpType, lhs, desc, row, params)
			if err != nil || !match {
				return false, err
			}
			continue
		}
		if cmp.Op == "@>" || cmp.Op == "NOT @>" {
			rhs, err := evalExpr(cmp.Value, desc, row, params)
			if err != nil {
				return false, err
			}
			if cmpType.IsArray() || lhs.Fam.IsArray() {
				if lhs.Null || rhs.Null {
					return false, nil
				}
				ok, aerr := builtins.ArrayOp("@>", lhs, rhs)
				if aerr != nil {
					return false, builtinErr(aerr)
				}
				if ok != (cmp.Op == "@>") {
					return false, nil
				}
				continue
			}
			// JSONB containment. The left side (post-path) must be jsonb —
			// a terminal ->> produces text and is refused.
			if cmpType != types.Jsonb {
				return false, newErrf(CodeFeatureNotSupported, "@> requires jsonb operands (%s is %s)", cmp.Column, cmpType)
			}
			if lhs.Null || rhs.Null {
				return false, nil // NULL operands: UNKNOWN either way
			}
			rhs, err = rhs.Coerce(types.Jsonb)
			if err != nil {
				return false, newErrf(CodeInvalidTextRepresentation, "WHERE %s @>: %v", cmp.Column, err)
			}
			ok, cerr := jsonbContains(lhs, rhs)
			if cerr != nil {
				return false, cerr
			}
			if ok != (cmp.Op == "@>") {
				return false, nil
			}
			continue
		}
		rhs, err := evalExpr(cmp.Value, desc, row, params)
		if err != nil {
			return false, err
		}
		if (lhs.Null || rhs.Null) && !quantDecides(cmp.Op, lhs, rhs) {
			return false, nil // NULL comparisons never match
		}
		if !plainCmpOp(cmp.Op) {
			ok, err := applyCmpOpEsc(cmp.Op, lhs, rhs, cmp.Escape)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			continue
		}
		if cmpType == types.Enum && len(cmp.Path) == 0 {
			// A literal against an enum column: the label resolves through
			// the column (an unknown one is 22P02, as in PostgreSQL).
			if col, ok := desc.Col(cmp.Column); ok {
				if v, cerr := col.EnumValue(rhs); cerr != nil {
					return false, ToSQLError(cerr)
				} else {
					rhs = v
				}
			}
		} else if rhs, err = rhs.Coerce(cmpType); err != nil {
			return false, newErrf(CodeInternal, "WHERE %s: %v", cmp.Column, err)
		}
		c, err := lhs.Compare(rhs)
		if err != nil {
			return false, nil
		}
		if !cmpHolds(cmp.Op, c) {
			return false, nil
		}
	}
	return true, nil
}

// evalCase evaluates a CASE expression: the simple form compares the
// operand with each WHEN value, the searched form tests each condition.
func evalCase(ce *parser.CaseExpr, env exprEnv, params []types.Datum) (types.Datum, error) {
	var operand types.Datum
	if ce.Operand != nil {
		d, err := evalExprEnv(*ce.Operand, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		operand = d
	}
	for _, w := range ce.Whens {
		hit := false
		if ce.Operand != nil {
			v, err := evalExprEnv(*w.Value, env, params)
			if err != nil {
				return types.Datum{}, err
			}
			if !operand.Null && !v.Null {
				if c, err := operand.Compare(v); err == nil && c == 0 {
					hit = true
				}
			}
		} else {
			ok, err := condsHold(w.Cond, env, params)
			if err != nil {
				return types.Datum{}, err
			}
			hit = ok
		}
		if hit {
			return evalExprEnv(w.Result, env, params)
		}
	}
	if ce.Else != nil {
		return evalExprEnv(*ce.Else, env, params)
	}
	return types.DNull, nil
}

// condsHold evaluates WHERE-shaped conjuncts against an expression
// environment (CASE WHEN conditions): comparisons, IS NULL, IN lists, OR.
func condsHold(conds []parser.Comparison, env exprEnv, params []types.Datum) (bool, error) {
	for _, cmp := range conds {
		switch cmp.Op {
		case "TRUE":
			continue
		case "FALSE":
			return false, nil
		case "OR":
			any := false
			for _, alt := range cmp.Or {
				ok, err := condsHold(alt, env, params)
				if err != nil {
					return false, err
				}
				if ok {
					any = true
					break
				}
			}
			if !any {
				return false, nil
			}
			continue
		}
		var lhs types.Datum
		var err error
		if cmp.Expr != nil {
			lhs, err = evalExprEnv(*cmp.Expr, env, params)
		} else {
			lhs, err = env.col(cmp.Column)
		}
		if err != nil {
			return false, err
		}
		switch cmp.Op {
		case "IS NULL":
			if !lhs.Null {
				return false, nil
			}
			continue
		case "IS NOT NULL":
			if lhs.Null {
				return false, nil
			}
			continue
		case "IN", "NOT IN":
			found := false
			for _, ve := range cmp.Values {
				v, err := evalExprEnv(ve, env, params)
				if err != nil {
					return false, err
				}
				if !lhs.Null && !v.Null {
					if c, err := lhs.Compare(v); err == nil && c == 0 {
						found = true
						break
					}
				}
			}
			if found != (cmp.Op == "IN") {
				return false, nil
			}
			continue
		}
		var rhs types.Datum
		if !isBoolTestOp(cmp.Op) {
			if rhs, err = evalExprEnv(cmp.Value, env, params); err != nil {
				return false, err
			}
		}
		if nullAwareOp(cmp.Op) {
			ok, err := applyNullAware(cmp.Op, lhs, rhs)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			continue
		}
		if (lhs.Null || rhs.Null) && !quantDecides(cmp.Op, lhs, rhs) {
			return false, nil
		}
		ok, err := applyCmpOpEsc(cmp.Op, lhs, rhs, cmp.Escape)
		if err != nil || !ok {
			return false, nil
		}
	}
	return true, nil
}

// cond3 evaluates one conjunct under three-valued logic: (value, false)
// when it is TRUE or FALSE, (_, true) when it is UNKNOWN.
func cond3(cmp parser.Comparison, env exprEnv, params []types.Datum) (bool, bool, error) {
	switch cmp.Op {
	case "TRUE":
		return true, false, nil
	case "FALSE":
		return false, false, nil
	case "OR":
		// OR of ANDs: TRUE if any disjunct is TRUE, else UNKNOWN if any
		// is UNKNOWN, else FALSE.
		unknown := false
		for _, alt := range cmp.Or {
			ok, null, err := conds3(alt, env, params)
			if err != nil {
				return false, false, err
			}
			if ok {
				return true, false, nil
			}
			unknown = unknown || null
		}
		return false, unknown, nil
	}
	var lhs types.Datum
	var err error
	if cmp.Expr != nil {
		lhs, err = evalExprEnv(*cmp.Expr, env, params)
	} else {
		lhs, err = env.col(cmp.Column)
		if err == nil && len(cmp.Path) > 0 {
			lhs, err = applyPath(lhs, cmp.Path)
		}
	}
	if err != nil {
		return false, false, err
	}
	switch cmp.Op {
	case "IS NULL":
		return lhs.Null, false, nil
	case "IS NOT NULL":
		return !lhs.Null, false, nil
	case "IN", "NOT IN":
		if lhs.Null {
			return false, true, nil
		}
		sawNull := false
		for _, ve := range cmp.Values {
			v, err := evalExprEnv(ve, env, params)
			if err != nil {
				return false, false, err
			}
			if v.Null {
				sawNull = true
				continue
			}
			if c, err := lhs.Compare(v); err == nil && c == 0 {
				return cmp.Op == "IN", false, nil
			}
		}
		if sawNull {
			return false, true, nil
		}
		return cmp.Op != "IN", false, nil
	}
	var rhs types.Datum
	if !isBoolTestOp(cmp.Op) {
		if rhs, err = evalExprEnv(cmp.Value, env, params); err != nil {
			return false, false, err
		}
	}
	if nullAwareOp(cmp.Op) {
		ok, err := applyNullAware(cmp.Op, lhs, rhs)
		return ok, false, err
	}
	if _, _, q := parser.SplitQuantified(cmp.Op); q {
		return quantCmp3(cmp.Op, lhs, rhs, cmp.Escape)
	}
	if lhs.Null || rhs.Null {
		return false, true, nil
	}
	ok, err := applyCmpOpEsc(cmp.Op, lhs, rhs, cmp.Escape)
	if err != nil {
		return false, false, err
	}
	return ok, false, nil
}

// conds3 is the three-valued AND of a conjunction.
func conds3(conds []parser.Comparison, env exprEnv, params []types.Datum) (bool, bool, error) {
	unknown := false
	for _, c := range conds {
		ok, null, err := cond3(c, env, params)
		if err != nil {
			return false, false, err
		}
		if !ok && !null {
			return false, false, nil
		}
		unknown = unknown || null
	}
	return !unknown, unknown, nil
}

// jsonOp evaluates the jsonb operators: @> and <@ (containment), ?
// (the key or string element exists), ?| and ?& (any / all of a text
// array of keys exist).
func jsonOp(op string, lhs, rhs types.Datum) (bool, error) {
	if lhs.Fam.IsArray() || rhs.Fam.IsArray() || op == "&&" {
		ok, err := builtins.ArrayOp(op, lhs, rhs)
		return ok, builtinErr(err)
	}
	l, err := lhs.Coerce(types.Jsonb)
	if err != nil {
		return false, newErrf(CodeFeatureNotSupported, "%s requires a jsonb left operand (got %s)", op, lhs.Fam)
	}
	switch op {
	case "@>", "<@":
		r, err := rhs.Coerce(types.Jsonb)
		if err != nil {
			return false, newErrf(CodeInvalidTextRepresentation, "%s: %v", op, err)
		}
		if op == "<@" {
			l, r = r, l
		}
		return jsonbContains(l, r)
	case "?":
		v, err := decodeJSONValue(l.S)
		if err != nil {
			return false, err
		}
		return builtins.JSONHasKey(v, rhs.Text()), nil
	case "?|", "?&":
		v, err := decodeJSONValue(l.S)
		if err != nil {
			return false, err
		}
		keys := builtins.TextArrayElems(rhs.Text())
		for _, k := range keys {
			has := builtins.JSONHasKey(v, k)
			if has && op == "?|" {
				return true, nil
			}
			if !has && op == "?&" {
				return false, nil
			}
		}
		return op == "?&" && len(keys) > 0, nil
	}
	return false, newErrf(CodeInternal, "unknown jsonb operator %q", op)
}

func decodeJSONValue(s string) (any, error) {
	var v any
	if err := decodeJSONNumber(s, &v); err != nil {
		return nil, newErrf(CodeInternal, "malformed stored jsonb: %v", err)
	}
	return v, nil
}

// builtinErr converts a builtins error into a SQL error with its
// SQLSTATE (nil stays nil).
// concatText renders a || operand: a padded CHAR(n) value contributes
// its trimmed text.
// concatTextIn is concatText with a timestamp with time zone rendered
// in loc.
func concatTextIn(d types.Datum, loc *time.Location) string {
	if d.Fam == types.Timestamp {
		return builtins.TextIn(d, loc)
	}
	return concatText(d)
}

func concatText(d types.Datum) string {
	if d.Fam == types.String {
		return d.S
	}
	return d.Text()
}

func builtinErr(err error) error {
	if err == nil {
		return nil
	}
	var be *builtins.Error
	if errors.As(err, &be) {
		return newErrf(be.Code, "%s", be.Msg)
	}
	return err
}

// boolTest evaluates x IS [NOT] TRUE | FALSE: a NULL is neither.
func boolTest(op string, d types.Datum) (bool, error) {
	want := strings.HasSuffix(op, "TRUE")
	not := strings.Contains(op, "NOT")
	if d.Null {
		return not, nil
	}
	b, err := d.Coerce(types.Bool)
	if err != nil {
		return false, newErrf(CodeInvalidTextRepresentation, "%s: %v", op, err)
	}
	return (b.B == want) != not, nil
}

// distinctFrom evaluates x IS [NOT] DISTINCT FROM y: NULLs compare as a
// value, equal to each other and different from everything else.
func distinctFrom(op string, lhs, rhs types.Datum) (bool, error) {
	distinct := lhs.Null != rhs.Null
	if !lhs.Null && !rhs.Null {
		if rhs.Fam != lhs.Fam {
			if c, err := rhs.Coerce(lhs.Fam); err == nil {
				rhs = c
			}
		}
		c, err := lhs.Compare(rhs)
		distinct = err != nil || c != 0
	}
	return distinct != strings.Contains(op, "NOT"), nil
}

// isBoolTestOp reports the one-sided IS [NOT] TRUE | FALSE tests.
func isBoolTestOp(op string) bool {
	return strings.HasSuffix(op, "TRUE") || strings.HasSuffix(op, "FALSE")
}

// nullAwareOp reports whether op decides on NULL operands itself
// (instead of the NULL-never-matches rule).
func nullAwareOp(op string) bool {
	switch op {
	case "IS TRUE", "IS NOT TRUE", "IS FALSE", "IS NOT FALSE", "IS DISTINCT FROM", "IS NOT DISTINCT FROM":
		return true
	}
	return false
}

// applyNullAware evaluates the null-aware operators.
func applyNullAware(op string, lhs, rhs types.Datum) (bool, error) {
	if strings.Contains(op, "DISTINCT") {
		return distinctFrom(op, lhs, rhs)
	}
	return boolTest(op, lhs)
}

// applyCmpOp applies an operator to two values: ordering operators
// through Compare, the regular-expression operators on text.
func applyCmpOp(op string, lhs, rhs types.Datum) (bool, error) {
	return applyCmpOpEsc(op, lhs, rhs, "")
}

// applyCmpOpEsc is applyCmpOp with a pattern's ESCAPE character ("" =
// backslash, parser.NoEscape = none).
func applyCmpOpEsc(op string, lhs, rhs types.Datum, escape string) (bool, error) {
	switch op {
	case "~", "!~", "~*", "!~*":
		// Compiled as the regexp_* functions compile a pattern: the
		// ARE escapes translated, back references and lookaround
		// refused (0A000), an invalid pattern 2201B.
		m, err := builtins.RegexpMatch(lhs.Text(), rhs.Text(), op == "~*" || op == "!~*")
		if err != nil {
			return false, builtinErr(err)
		}
		return m == (op == "~" || op == "~*"), nil
	}
	switch op {
	case "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE":
		re, err := regexFor(likeToRegexpEsc(rhs.Text(), escape), strings.HasSuffix(op, "ILIKE"))
		if err != nil {
			return false, err
		}
		m := re.MatchString(lhs.Text())
		return m == !strings.HasPrefix(op, "NOT"), nil
	case "SIMILAR TO", "NOT SIMILAR TO":
		re, err := regexFor(similarToRegexp(rhs.Text(), escape), false)
		if err != nil {
			return false, newErrf(CodeInvalidRegexp, "invalid SIMILAR TO pattern: %v", err)
		}
		m := re.MatchString(lhs.Text())
		return m == !strings.HasPrefix(op, "NOT"), nil
	case "@@", "NOT @@":
		ok, err := builtins.TSMatch(lhs, rhs)
		if err != nil {
			return false, builtinErr(err)
		}
		return ok == (op == "@@"), nil
	case "@>", "NOT @>", "<@", "NOT <@", "&&", "NOT &&", "?", "NOT ?", "?|", "NOT ?|", "?&", "NOT ?&":
		ok, err := jsonOp(strings.TrimPrefix(op, "NOT "), lhs, rhs)
		if err != nil {
			return false, err
		}
		return ok == !strings.HasPrefix(op, "NOT "), nil
	}
	if _, _, ok := parser.SplitQuantified(op); ok {
		// Quantified comparison over an array value (or a text array
		// literal '{a,b}'); UNKNOWN is not a match.
		ok, _, err := quantCmp3(op, lhs, rhs, escape)
		return ok, err
	}
	if lhs.Fam != rhs.Fam {
		if c, err := rhs.Coerce(lhs.Fam); err == nil {
			rhs = c
		}
	}
	c, err := lhs.Compare(rhs)
	if err != nil {
		return false, err
	}
	return cmpHolds(op, c), nil
}

// quantCmp3 evaluates a quantified comparison, x op ANY | ALL (array),
// under three-valued logic, as PostgreSQL does: (value, false), or (_,
// true) when it is UNKNOWN. ANY holds when some element does and is
// UNKNOWN when none does but some comparison is UNKNOWN (a NULL); ALL
// fails when some element fails and is UNKNOWN when none does but some
// is UNKNOWN. Over no elements ANY is false and ALL true, whatever x
// is; a NULL array is UNKNOWN.
func quantCmp3(op string, lhs, rhs types.Datum, escape string) (bool, bool, error) {
	base, quant, _ := parser.SplitQuantified(op)
	if rhs.Null {
		return false, true, nil
	}
	var elems []types.Datum
	if rhs.Fam.IsArray() {
		elems = rhs.A
	} else {
		for _, el := range arrayElems(rhs.Text()) {
			elems = append(elems, types.NewString(el))
		}
	}
	if len(elems) == 0 {
		return quant == "ALL", false, nil
	}
	if lhs.Null {
		return false, true, nil
	}
	unknown := false
	for _, ed := range elems {
		if ed.Null {
			unknown = true
			continue
		}
		if ed.Fam != lhs.Fam && !(numericFam(ed.Fam) && numericFam(lhs.Fam)) {
			if c, err := ed.Coerce(lhs.Fam); err == nil {
				ed = c
			}
		}
		ok, err := applyCmpOpEsc(base, lhs, ed, escape)
		if err != nil {
			return false, false, err
		}
		if ok && quant == "ANY" {
			return true, false, nil
		}
		if !ok && quant == "ALL" {
			return false, false, nil
		}
	}
	if unknown {
		return false, true, nil
	}
	return quant == "ALL", false, nil
}

// numericFam reports the numeric families, which compare with each
// other as they are.
func numericFam(f types.Family) bool {
	return f == types.Int || f == types.Decimal || f == types.Float
}

// quantDecides reports a quantified comparison whose result a NULL
// operand does not settle (an empty array: ANY false, ALL true), for
// the evaluators that refuse a NULL operand up front.
func quantDecides(op string, lhs, rhs types.Datum) bool {
	_, _, ok := parser.SplitQuantified(op)
	return ok && lhs.Null && !rhs.Null
}

// escapeRune resolves a pattern's ESCAPE character: backslash by
// default, none for parser.NoEscape.
func escapeRune(escape string) (rune, bool) {
	switch escape {
	case "":
		return '\\', true
	case parser.NoEscape:
		return 0, false
	}
	r := []rune(escape)
	return r[0], true
}

func likeToRegexpEsc(pattern, escape string) string {
	esc, hasEsc := escapeRune(escape)
	var b strings.Builder
	b.WriteString("^")
	escaped := false
	for _, r := range pattern {
		switch {
		case escaped:
			b.WriteString(regexp.QuoteMeta(string(r)))
			escaped = false
		case hasEsc && r == esc:
			escaped = true
		case r == '%':
			b.WriteString(".*")
		case r == '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return b.String()
}

// plainCmpOp reports whether op is an ordering comparison the typed
// Coerce-and-Compare path handles; the rest go through applyCmpOp.
func plainCmpOp(op string) bool {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
		return true
	}
	return false
}

// arrayElems splits a PostgreSQL text array literal ('{a,"b c",d}') into
// its elements. NULL elements come back as the text "NULL".
func arrayElems(text string) []string {
	text = strings.TrimSpace(text)
	if len(text) >= 2 && text[0] == '{' && text[len(text)-1] == '}' {
		text = text[1 : len(text)-1]
	} else {
		// An int2vector (pg_index.indkey): space-separated, no braces.
		return strings.Fields(text)
	}
	if text == "" {
		return nil
	}
	var out []string
	var cur strings.Builder
	quoted, escaped := false, false
	for _, r := range text {
		switch {
		case escaped:
			cur.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case r == '"':
			quoted = !quoted
		case r == ',' && !quoted:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	out = append(out, cur.String())
	return out
}

var (
	regexMu    sync.Mutex
	regexCache = map[string]*regexp.Regexp{}
)

// regexFor compiles a POSIX-style pattern (Go's RE2 covers what psql and
// the tools send), cached.
// similarToRegexp translates a SIMILAR TO pattern — SQL's % and _
// wildcards over a regular expression using | * + ? {m,n} ( ) [ ] —
// into an anchored RE2 expression.
func similarToRegexp(pattern, escape string) string {
	esc, hasEsc := escapeRune(escape)
	var b strings.Builder
	b.WriteString("^(?:")
	escaped := false
	for _, r := range pattern {
		switch {
		case escaped:
			b.WriteString(regexp.QuoteMeta(string(r)))
			escaped = false
		case hasEsc && r == esc:
			escaped = true
		case r == '%':
			b.WriteString(".*")
		case r == '_':
			b.WriteString(".")
		case strings.ContainsRune("|*+?{}()[]", r):
			b.WriteRune(r)
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString(")$")
	return b.String()
}

func regexFor(pattern string, fold bool) (*regexp.Regexp, error) {
	key := pattern
	if fold {
		key = "(?i)" + pattern
	}
	regexMu.Lock()
	defer regexMu.Unlock()
	if re, ok := regexCache[key]; ok {
		return re, nil
	}
	re, err := regexp.Compile(key)
	if err != nil {
		return nil, newErrf(CodeInvalidParameterValue, "invalid regular expression %q: %v", pattern, err)
	}
	if len(regexCache) > 256 {
		regexCache = map[string]*regexp.Regexp{}
	}
	regexCache[key] = re
	return re, nil
}

// cmpHolds applies a comparison operator to a Compare result.
func cmpHolds(op string, c int) bool {
	switch op {
	case "=":
		return c == 0
	case "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	}
	return false
}

// matchesIn evaluates col [NOT] IN (values) with SQL three-valued
// semantics collapsed to WHERE's boolean: a NULL left-hand side never
// matches; a NULL element can never prove NOT IN (x NOT IN (..., NULL) is
// UNKNOWN unless x matches a non-NULL element).
func matchesIn(cmp parser.Comparison, cmpType types.Family, lhs types.Datum, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) (bool, error) {
	if lhs.Null {
		return false, nil
	}
	matched, hasNull := false, false
	for _, ve := range cmp.Values {
		d, err := evalExpr(ve, desc, row, params)
		if err != nil {
			return false, err
		}
		if d.Null {
			hasNull = true
			continue
		}
		if cmpType == types.Enum {
			// A label matches by text (Compare handles enum against text).
			if c, err := d.Compare(lhs); err == nil && c == 0 {
				matched = true
				break
			}
			continue
		}
		d, cerr := d.Coerce(cmpType)
		if cerr != nil {
			return false, newErrf(CodeInternal, "IN %s: %v", cmp.Column, cerr)
		}
		if c, err := lhs.Compare(d); err == nil && c == 0 {
			matched = true
			break
		}
	}
	if cmp.Op == "IN" {
		return matched, nil
	}
	return !matched && !hasNull, nil
}

// execRollbackToSavepoint restores the named savepoint, escaping the
// in-failed-transaction state (25P02) per PostgreSQL semantics. A
// transaction whose coordinator already finished (aborted by a conflict)
// cannot be rescued — the retryable error stands.
func (s *Session) execRollbackToSavepoint(ctx context.Context, name string) (*Result, *Error) {
	if s.txn == nil {
		return nil, newErrf(CodeNoActiveTransaction, "ROLLBACK TO SAVEPOINT can only be used in transaction blocks")
	}
	if err := s.txn.RollbackToSavepoint(ctx, name); err != nil {
		if kvclient.IsRetryable(err) {
			s.state = StateFailed
			return nil, ToSQLError(err)
		}
		return nil, newErrf(CodeInvalidSavepoint, "%v", err)
	}
	s.notifyRollbackToSavepoint(name)
	s.advisoryRollbackToSavepoint(name)
	s.loRollbackToSavepoint()
	// The tables created in the block may have been rolled back with
	// the savepoint; forget them all rather than track which (CREATE
	// INDEX on one is then refused in the block, never wrongly allowed).
	s.createdTxn, s.createdTables = nil, nil
	s.state = StateOpen
	return &Result{Tag: "ROLLBACK"}, nil
}

// byteaConcatOperands reads the untyped string literal on one side of
// a || whose other side is a bytea as bytea input, as PostgreSQL
// resolves an unknown literal to the other operand's type.
func byteaConcatOperands(e parser.Expr, l, r types.Datum) (types.Datum, types.Datum) {
	untyped := func(x *parser.Expr, d types.Datum) bool {
		return x != nil && x.Lit != nil && x.Cast == "" && x.BinOp == "" && x.Func == "" && !d.Null && d.Fam == types.String
	}
	switch {
	case l.Fam == types.Bytes && untyped(e.Right, r):
		if b, err := r.Coerce(types.Bytes); err == nil {
			r = b
		}
	case r.Fam == types.Bytes && untyped(e.Left, l):
		if b, err := l.Coerce(types.Bytes); err == nil {
			l = b
		}
	}
	return l, r
}
