// ---- Help: everything on the page explains itself ----
//
// A dashboard full of figures is only useful to someone who already
// knows what the figures mean. "40001/s", "compaction debt", "bare
// majority", "stats age" — each is precise and each is opaque until
// somebody explains it, and the person who most needs the explanation
// is the one reading the console during an incident.
//
// So every label, column heading and section title on the page carries
// its own explanation. There are two ways to reach it, because there
// are two questions:
//
//   "what is this one figure?" — the term is marked, and hovering or
//   clicking it explains it in place.
//
//   "what am I looking at?" — the ? in the header opens the glossary
//   for the current view: every term on screen, in the order they
//   appear, with its explanation. That is also the keyboard and screen
//   reader path, which is why the terms themselves are not ninety
//   separate tab stops.
//
// The glossary is keyed by the term as it is written on screen, so a
// column that says "leases" is explained by the entry called "leases"
// and a new column naming a term already in here is explained without
// anyone wiring it up. Where a word means two different things in two
// views — "rows" is a statement's result size on #/sql and a table's
// estimated row count on #/schema — the entry is keyed "view/term" and
// takes precedence over the bare word.

// prose keeps the entries to one idea each: what it measures, and what
// makes a reading worth acting on. An entry that only restates the
// label ("QPS: queries per second") is worse than none, because it
// costs a click to learn nothing.
const HELP = {
  // ---- The cluster rollup (#/ tiles) ----
  "live nodes": "Nodes that have sent a heartbeat recently, out of every node the cluster knows about. A node counted as down is excluded from every cluster total on this page, so totals fall when a node goes away rather than showing stale figures.",
  "cluster qps": "Requests per second served by range leaders across the cluster. Each range is counted by its leader only, so a request is not counted once per replica.",
  "cluster data": "Live logical bytes stored, counting each range once rather than once per replica. Disk use is higher: it includes every replica, older versions not yet garbage collected, and the write-ahead log.",
  "ranges": "The key space is split into ranges, each replicated to several nodes. Ranges split when they grow past the target size or take disproportionate load, and merge when neighbours shrink.",
  "leases": "Range leases held. The leaseholder is the one replica that serves reads and sequences writes for its range, so leases are how load is distributed — a node holding far more than its share is a hotspot.",
  "connections": "Open SQL connections. \"Active\" are running a statement right now; \"idle in txn\" have an open transaction and are running nothing, which holds locks and blocks other writers.",
  "statements/s": "SQL statements finishing per second, measured as the change in a counter between two polls, so it needs two polls before it reads anything.",
  "40001/s": "Serialization failures per second — SQLSTATE 40001, the error a serializable database returns when it cannot order two transactions and one has to retry. A steady rate is normal under contention; a rising one means transactions are fighting over the same rows.",
  "40001": "Serialization failures since this node started: transactions asked to retry because they could not be serialized against a conflicting one.",
  "40001s": "Serialization failures attributed to this statement shape — the retries that this shape caused or suffered.",
  "worst p99": "The highest 99th-percentile statement latency any single node is reporting, and which node that is. Percentiles are never summed or averaged across nodes, so this is one node's measurement, not the cluster's.",

  // ---- Nodes ----
  "nodes": "Every node the cluster knows about, live or not, with what its last heartbeat reported. Figures come from each node's own accounting, so a node that is down shows what it last said.",
  "node": "The node's id. n1, n2 and so on are assigned when a node first joins and are never reused.",
  "status": "Live, draining or down. Draining means the node is shutting down gracefully: it is shedding leases and finishing its SQL connections, and it should not be sent new work.",
  "locality": "The node's declared position in the failure domains — region, zone, rack. Replication uses it to spread a range's replicas across domains, so it decides what a node's loss actually costs.",
  "address": "The address other nodes and this console reach it on.",
  "heartbeat": "How long since this node last checked in. A node is treated as down once it misses enough heartbeats; everything else in its row is as of that last heartbeat.",
  "host cpu": "Processor time in use across the whole machine, not just this process. Sustained high CPU with high iowait usually means the disk, not the processor, is the limit.",
  "cpu": "Processor time in use across the whole machine, not just this process.",
  "load": "The machine's one-minute run queue against its core count. Above the core count means work is queuing for a processor.",
  "memory": "Memory in use across the machine, out of what it has. This is the host's figure, not the process's — see \"process\" for that.",
  "process": "Resident memory and processor time for this node's own process, as against the machine's totals.",
  "disk free": "Space left on the store's filesystem, out of its size. Storage engines need headroom to compact; a store that fills stops accepting writes.",
  "disk i/o": "Bytes read and written per second on the store's device and how busy the device is. Busy near 100% means the disk is the bottleneck whatever the processor is doing.",
  "network": "Bytes per second in and out of the machine's interfaces — all of it, not just this process. Replication traffic dominates on a busy node, so this rises with writes rather than with queries.",
  "file descriptors": "Open file descriptors against the process limit. Every connection, table file and socket costs one, and running out fails requests in ways that read like unrelated bugs.",
  "fds": "Open file descriptors against the process limit.",
  "go runtime": "Goroutines running, heap in use, and the 99th percentile garbage collection pause. Rising goroutines with steady load usually means something is blocked and accumulating.",
  "uptime": "How long this node's process has been running. A short uptime on a node you did not restart means it crashed and came back.",
  "version": "The release this node runs, the protocol version its binary speaks, and the cluster version in force. During an upgrade these differ across nodes, and the cluster version only advances once every node can support it.",
  "replicas": "Replicas of ranges stored on this node, and how many of them it leads.",
  "fills in": "How long until the store fills, projected from the rate it has been growing at over the recent window. It is a straight-line projection of the recent past, not a promise; it says nothing about a workload that changes.",

  // ---- Network ----
  "pair": "The two nodes this row measures between, in the direction measured. A path can be slow one way only, so the reverse pair is its own row.",
  "peer": "The node on the other end of this measurement.",
  "round trip": "Median time for a request between these two nodes and back. It sets the floor on any operation needing agreement from a replica elsewhere.",
  "p99": "The 99th percentile — the value one request in a hundred exceeds. Percentiles come from the node that measured them and are never averaged together.",
  "node/p99": "The 99th percentile round trip to this peer: one request in a hundred takes longer.",
  "clock offset": "How far this pair's clocks disagree. Transaction ordering depends on bounded clock skew, so a node whose offset approaches the limit is refused rather than allowed to break serializability.",
  "reachable": "Whether the last probe between these two nodes got an answer.",
  "measured": "When this pair was last probed. A stale measurement usually means the probe itself is failing, which is worth more than the number beside it.",
  "from \\ to": "Rows are the node measuring, columns the node measured. The matrix is not symmetric: a path can be slow in one direction only.",

  // ---- Data: ranges and replication ----
  "range": "One contiguous span of the key space, replicated as a unit. Ranges are what the cluster moves, splits, merges and leases.",
  "span": "The keys this range covers, from its start key up to but not including its end key.",
  "leader": "The replica currently holding the lease: it serves this range's reads and sequences its writes.",
  "size": "Live logical bytes in this range, counting one replica. A range past the target size is a candidate to split, which is how the cluster keeps any one range from becoming the bottleneck.",
  "qps": "Requests per second this range is serving, counted at its leaseholder.",
  "applied": "The raft log index this replica has applied. A replica far behind the leader is catching up and cannot take over.",
  "log": "Entries in this range's raft log not yet truncated. A log that will not shrink usually means one replica is too far behind to let the others discard what it still needs.",
  "replication": "How many replicas each range is meant to have, how many it has, and which ranges are short. A range below its target survives but has less margin; one at bare majority loses availability if it loses one more replica.",
  "range hotspots": "The ranges taking the most load. A single range far above the rest is the usual cause of a cluster that will not scale by adding nodes: the work is not spread, and adding a node does not move it.",
  "failure domains": "What the cluster loses if a whole domain — a region, a zone, a rack — goes away at once. Replicas spread across domains are only useful if no single domain holds a majority of any range.",
  "domain": "The failure domain this row is about: everything sharing this region, zone or rack value.",
  "bare majority": "Ranges holding exactly the minimum number of replicas needed to stay available in this domain. They are available now and stop being available if this domain is lost.",
  "loses quorum": "Ranges that would lose their majority — and so stop serving reads and writes — if this whole domain went away at once.",

  // ---- Schema ----
  "schema": "Tables in the cluster, with what each holds and what it costs. Sizes are this node's replicas only, so they are a sample of the cluster rather than its total.",
  "table": "The table's name. Every table lives in one namespace here: the database name in a connection URL is accepted and ignored.",
  "columns": "How many columns the table has. Wide tables cost on every read that does not name its columns, because the whole row is fetched to answer it.",
  "primary key": "The columns the table is stored by. Rows are physically ordered by this key, so it decides which scans are cheap and where writes land — a monotonically increasing key sends every insert to one range.",
  "indexes": "Secondary indexes on the table. Each one is a second copy of the indexed columns that every write to the table has to maintain.",
  "schema/rows": "Estimated live rows, from the statistics last collected. It is an estimate: it can lag well behind after a bulk change.",
  "schema/ranges": "How many ranges this table's data is split across.",
  "local size": "Bytes this table's replicas take on this node only, not across the cluster.",
  "stats age": "How long since the table's statistics were last collected. The query planner chooses plans from these, so stale statistics on a table that has changed a lot are a common cause of a plan that suddenly got slow.",
  "grants": "Privileges granted on this table, and to whom.",

  // ---- SQL activity ----
  "sql": "What SQL this cluster is running: connections open, statements in flight, and the shapes that cost the most over time.",
  "statements": "Statements running right now, newest first. This is a live sample, not a log: a statement that finished before the last poll was never seen.",
  "statement": "The statement as it was sent, literals and all. The shapes list is where the same query with different literals is grouped together.",
  "sql/user": "The role the connection authenticated as.",
  "client": "The address the connection came from. Behind a pooler or a proxy this is the pooler, not the application — application_name is the better attribution then.",
  "application": "The application_name the client set on its connection. Clients that set it honestly make this the fastest way to attribute load.",
  "sql/kind": "What the statement does — select, insert, update, and so on.",
  "duration": "How long this statement has been running so far.",
  "rows": "Rows the statement has returned so far. Compare it against rows scanned: the gap between them is the work done to produce the answer.",
  "pid": "The session's process id, as reported to pg_stat_activity. It is what pg_cancel_backend takes.",
  "idle": "How long the session has been running nothing. Idle on its own is harmless — a pooled connection waiting for work. Idle with a transaction open is not.",
  "txn open": "How long this session's transaction has been open. A transaction held open holds its locks with it, so a long one here is often the reason other statements are waiting.",
  "idle in transaction": "Sessions with an open transaction that are running nothing. Nothing is progressing and their locks are still held, so these block other writers while doing no work themselves — usually an application that forgot to commit.",
  "oldest idle txn": "How long the longest-held idle transaction has been open.",
  "who is connected": "Connections grouped by the role that opened them.",
  "by user": "Connections and load grouped by the role that opened them, which is the coarsest useful attribution when application_name is not set.",
  "users": "How many distinct roles this covers. Connection pools usually share one role across many clients, so this counts credentials, not people.",
  "sql/connections": "Connections this role has open.",
  "mix": "The proportion of this role's statements by kind. A role that is almost entirely reads and suddenly is not is a change in what an application is doing.",
  "stmt/s": "Statements per second attributed to this role.",
  "sql/last": "When a statement of this shape was last seen. A shape at the top of the list that was last seen an hour ago is telling you about the past, not about now.",
  "last statement": "The most recent statement seen for this row, for recognising what the row is.",
  "retry hot list": "The statement shapes involved in the most serialization failures. These are the shapes contending with each other; the fix is usually in how the transaction is written, not in the statement.",
  "transactions": "Commits, aborts and retries over the time range, with the latencies underneath them. Retries rising with commits flat means work is being redone rather than done.",
  "commits/s": "Transactions committing per second.",
  "aborts/s": "Transactions ending in an error or a rollback per second.",
  "retries/s": "Transaction retries per second, counted where the transaction restarted rather than where the statement failed.",
  "retried share": "The share of transactions that had to retry at least once. Rising share means growing contention even if throughput has not moved yet.",
  "retries": "Retries attributed to this row. A transaction can retry more than once, so retries can exceed the transactions that caused them.",
  "kv batch p99": "The 99th percentile time for one batch of key-value operations. This is the storage layer underneath SQL: when it rises and statement latency rises with it, the cause is below SQL, not in the query.",
  "statement p99": "The 99th percentile statement latency measured at this node.",
  "p50 / p99": "Median and 99th percentile latency. The gap between them matters more than either: a median that is fine with a far worse p99 is a tail problem, not a throughput one.",
  "plan cache": "How often a statement found a plan already prepared instead of planning again. A low hit rate with repetitive traffic usually means the client is inlining its literals, so every statement looks new.",

  // ---- Statement shapes ----
  "statement shape": "A statement with its literals replaced, so that every execution of the same query counts as one shape. Shapes come from the parsed statement, so two texts differing only in whitespace, keyword case or parenthesisation are the same shape.",
  "executions": "How many times this shape ran in the window.",
  "total time": "Time spent in this shape altogether. This is what ranks the list, because the shape worth fixing is usually a fast one that runs constantly rather than the slowest one.",
  "mean": "Average time for one execution of this shape.",
  "rows returned": "Rows this shape returned, in total.",
  "rows scanned": "Rows read to produce those results. Far more scanned than returned is a scan where an index would do, and it is worth fixing whether or not the statement is also slow.",

  // ---- Operations and events ----
  "operations in flight": "What the cluster is doing to itself right now — splits, merges, rebalances, lease moves, decommissions, backups. These explain load that no client asked for.",
  "in flight": "Operations running now, with how long each has been going.",
  "recently completed": "Operations that finished, with their outcome. An operation that keeps failing and restarting is why something never settles.",
  "operation": "What the cluster is doing and to which range or node. Nothing here was asked for by a client — this is the cluster maintaining itself.",
  "kind": "The kind of operation: a split, a merge, a rebalance, a lease move, a decommission, a backup.",
  "started": "When the operation began, by the clock of the node that started it.",
  "elapsed": "How long this operation has been running so far. An operation whose elapsed time keeps growing past what the same kind usually takes is stuck rather than slow.",
  "finished": "When the operation ended, by the clock of the node that ran it.",
  "took": "How long the operation ran, start to finish. Compare it against the same kind of operation elsewhere in the list: one taking far longer than its peers is the one to look at.",
  "outcome": "Whether it succeeded, failed or was cancelled, and why if it failed.",
  "recent events": "The tail of this node's event ring — what it decided and what happened to it. The ring is bounded and in memory, so it is the recent past only and it does not survive a restart.",
  "when": "When it happened, by the clock of the node that recorded it. Nodes keep their clocks close enough to order transactions, so times from different nodes are comparable — but they are not identical.",
  "what": "What happened, written out. The event ring holds a bounded number of these in memory, so it is the recent past only and it does not survive a restart.",

  // ---- Storage ----
  "storage": "The storage engine underneath this node. These are the figures that explain a node that is slow without being busy.",
  "l0 files": "Files in the storage engine's newest level, where writes land before compaction sorts them deeper. They accumulate when writes arrive faster than compaction can keep up.",
  "l0 sublevels": "Overlapping layers in that newest level. Reads have to check each one, so this is the figure that turns a write backlog into slow reads.",
  "compaction debt": "Bytes the engine still has to rewrite to get back into shape. Growing debt means compaction is losing to the write rate.",
  "memtables": "In-memory write buffers not yet flushed to disk, and what they hold.",
  "write stalls": "Times the engine paused writes to let compaction catch up. Any of these are worth explaining: they are the storage layer applying back pressure to SQL.",
  "disk slow events": "Times a disk operation took long enough for the engine to notice and say so. A device degrading usually shows up here first.",
  "block cache": "Memory holding recently read blocks, and how often a read was served from it rather than from disk.",
  "bloom filters": "The share of point lookups answered without touching a file, because the filter proved the key could not be there.",
  "prefix bloom filters": "Whether this store's filters are built on the key prefix, which lets a lookup skip files by row rather than by exact key.",
  "debt gate": "Whether this node is throttling incoming work because compaction debt has passed its limit. Latched means it is deliberately slowing writes to stop the engine falling further behind.",
  "overload": "Whether this node has decided it is overloaded and is shedding work, and the reason it gave.",
  "store": "The store this row is about — a node has one per data directory. Encryption, compaction and free space are all properties of a store, not of the node above it.",

  // ---- Security ----
  "mode": "Whether the cluster requires authentication. Insecure means every connection is root and there is nothing to authenticate — usable for a local trial, never for anything real.",
  "signed in as": "The role this browser session authenticated as.",
  "signed in by": "How this session authenticated: a client certificate, a password, or a session cookie from an earlier sign-in.",
  "admin role": "Whether this session holds admin. Without it the console shows what this node knows and refuses the cluster-wide fan-outs, rather than showing them empty.",
  "users and roles": "Every role, what it may do, and what it inherits. A privilege held through membership is as real as one granted directly, which is why they are resolved here.",
  "user": "The role the connection authenticated as.",
  "role": "The role's name. Roles are both users and groups: one that can log in is a user, one granted to others is a group.",
  "may log in": "Whether this role can open a connection at all. A role without it exists only to be granted to others.",
  "member of": "Roles this role belongs to, and so inherits privileges from.",
  "effectively holds": "Every privilege this role has once membership is resolved — what it can actually do, rather than what was granted to it directly.",
  "authentication": "How connections actually authenticated, counted by method. This is what was used, not what is configured, which is the difference that matters when something is meant to have been turned off.",
  "how they authenticated": "The method this group of connections used.",
  "security/connections": "Connections that authenticated this way.",
  "certificates": "The certificates this cluster presents and trusts, and when they expire. An expired certificate takes nodes out of the cluster, and it does it to every node at once.",
  "subject": "Who the certificate identifies. For a client certificate this is the role it authenticates as, which is how certificate authentication decides who you are.",
  "issuer": "Who signed the certificate. Nodes trust a certificate only if they trust its issuer, so a node presenting one signed by an authority the others do not have simply cannot join.",
  "expires": "When it stops being valid. Certificates are not renewed automatically here, so this date is a deadline someone has to act on.",
  "on": "The node this certificate was read from. Certificates are per node, so the same logical certificate can be a different file — and a different expiry — on each.",
  "encryption at rest": "Whether the store encrypts what it writes, and the state of any key rotation in progress.",
  "state": "Whether the store is encrypted, plain, or part way through re-encrypting.",
  "encryption": "Whether this store encrypts what it writes to disk. It covers data written from now on; anything already on disk is only encrypted once re-encryption has rewritten it.",
  "re-encryption": "Progress rewriting existing files under a new key after a rotation. Until it finishes, both keys are still needed.",
  "audit": "Security-relevant events this node recorded: sign-ins, refusals, privilege changes.",

  // ---- Metrics and charts ----
  "metrics": "Every series this cluster records, charted over the header's time range. Series are written to an internal table every few seconds, so the resolution is that, not the poll interval.",
  "recent history": "Charts for this node over the header's time range, read from the recorded metrics rather than from the live poll — so they survive a reload and cover time this tab was not open.",
  "machine": "The host this node runs on, as against the node process itself.",
  "settings": "How this node was configured when it started. These are the node's own flags and files; changing them takes a restart of that node.",

  // ---- Controls ----
  "scope": "Which node the node-scoped panels describe. The whole cluster fans out to every node and needs admin; a single node asks only that node.",
  "range": "The time range every chart and every rate on the page uses. It is part of the address, so a link carries it.",
  "jump to": "Open anything by name: a node, a range id, a table, a locality. ⌘K or Ctrl-K from anywhere.",
  "compare": "Overlay every node's series on one chart instead of drawing a chart per node — the fastest way to see that one node disagrees with the rest.",
  "annotate": "Mark cluster operations on the charts where they happened, so a change in a line can be lined up against what the cluster did.",
  "filter": "Narrows this view. It is written into the address, so a filtered view is a link that can be shared.",

  // ---- Section titles that are not also a column ----
  "cluster": "What every live node adds up to. A node counted as down contributes nothing, so these totals describe the cluster as it is now rather than as it was configured.",
  "operations": "Everything the cluster is doing to itself, in flight and recently finished.",
  "statement shapes": "Statements grouped by shape and ranked by what they cost altogether, which is usually a different list from the slowest statements.",
  "time": "The sample's timestamp, at the resolution the metrics were recorded at rather than the resolution of the poll.",

  // ---- Columns in the tables a chart folds out into ----
  "total": "The series summed across every node in the chart. Only figures that can honestly be added are summed here — never a percentile, which belongs to the node that measured it.",
  "max": "The highest value any node reached at this sample. When it sits far above the rest, one node is carrying something the others are not.",
  "p50": "The median — half of the requests were faster than this. Read it beside the 99th percentile: the gap is the tail.",
  "last": "The most recent sample in the range, so a column that can be compared against where the line ends.",
  "closed ts": "The timestamp below which this range will accept no more writes, so reads at or under it are guaranteed stable. A closed timestamp far behind the present is why a follower read returns older data than expected.",
};

// normTerm reduces a label to its glossary key: the words, lowercased,
// with any control the heading happens to contain already stripped by
// the caller. "P50 / p99" and "p50 / p99" are one term.
function normTerm(text) { return String(text || "").toLowerCase().replace(/\s+/g, " ").trim(); }
// viewOf names the view an element is in, so a term can be read the way
// that view means it.
function viewOf(el) {
  const main = el.closest("main[id^=view-]");
  return main ? main.id.slice("view-".length) : "";
}
// helpFor resolves a term, most specific first: the view's own reading
// of the word, then the word, then the word without the parenthetical
// that qualifies it — "ranges (replicas on this node)" is the ranges
// entry, said about one node — and finally the patterns, for terms that
// are generated rather than written ("n7" is a column per node).
function helpFor(term, view) {
  const key = normTerm(term);
  if (!key) return "";
  const bare = key.replace(/\s*\([^)]*\)\s*$/, "").trim();
  return (view && HELP[view + "/" + key]) || HELP[key] ||
    (bare !== key && ((view && HELP[view + "/" + bare]) || HELP[bare])) ||
    patternHelp(key) || "";
}
// Terms the page generates rather than writes: one per node, one per
// range. Keying every id would be endless and would go stale the moment
// a node joined.
function patternHelp(key) {
  if (/^n\d+$/.test(key)) return `Node ${key}'s own reading. Each node measures itself, so a column per node is a column per measurement — they are not summed, and one disagreeing with the rest is the point of showing them side by side.`;
  return "";
}
// labelText is the term an element stands for: its data-help when it
// names one explicitly, otherwise its own words — minus any control
// living inside it, because "Statement shapes" is a heading and the
// sort <select> beside it is not part of the term.
function labelText(el) {
  if (el.dataset.help) return el.dataset.help;
  const copy = el.cloneNode(true);
  for (const ctl of copy.querySelectorAll("select, button, input, label, .sr-only, .cue")) ctl.remove();
  return copy.textContent;
}
// The elements that carry a term. Marking them is idempotent: renders
// replace markup constantly, and every pass re-marks only what is new.
const HELP_TARGETS = "th, h2, .tile .label, [data-help]";
function wireHelp(root) {
  for (const el of (root || document).querySelectorAll(HELP_TARGETS)) {
    if (el.dataset.helped) continue;
    const text = helpFor(labelText(el), viewOf(el));
    if (!text) continue;
    el.dataset.helped = "1";
    el.classList.add("helpable");
    // title carries it to a hover and to a screen reader without any
    // script running; the click below is the same text, laid out.
    if (!el.hasAttribute("title")) el.setAttribute("title", text);
  }
}
// Renders replace markup on every poll, so rather than each view
// remembering to call wireHelp, one observer watches the views and
// marks whatever appears. A term added to a table a year from now is
// explained the moment its word is in the glossary.
function observeHelp() {
  const root = document.getElementById("main-views");
  if (!root) return;
  wireHelp(root);
  // Coalesced: one poll can replace several tables, and marking once
  // per mutation record would run the query dozens of times a second
  // for the same result.
  let pending = false;
  new MutationObserver(() => {
    if (pending) return;
    pending = true;
    requestAnimationFrame(() => { pending = false; wireHelp(root); });
  }).observe(root, { childList: true, subtree: true });
}

// ---- The popover: one term, explained where it is ----
function helpPop() { return document.getElementById("help-pop"); }
function closeHelpPop() {
  const pop = helpPop();
  if (pop) pop.hidden = true;
}
function openHelpPop(el) {
  const pop = helpPop();
  if (!pop) return;
  const term = normTerm(labelText(el));
  const text = helpFor(term, viewOf(el));
  if (!text) return;
  pop.innerHTML = `<b>${esc(term)}</b><p>${esc(text)}</p>`;
  pop.dataset.term = term;
  pop.hidden = false;
  // Anchored under the term, nudged back inside the viewport rather
  // than allowed to hang off the right edge of a narrow screen.
  const r = el.getBoundingClientRect();
  const w = pop.offsetWidth;
  const left = Math.max(8, Math.min(r.left + window.scrollX, window.scrollX + document.documentElement.clientWidth - w - 8));
  pop.style.left = left + "px";
  pop.style.top = (r.bottom + window.scrollY + 6) + "px";
}

// ---- The panel: this view, explained ----
//
// Every term on screen in the order it appears. It is built from the
// marked elements rather than from the glossary, so it describes what
// is actually in front of the reader — a view with no certificates
// table does not explain one.
function renderHelpPanel() {
  const view = ui.view;
  const main = document.getElementById("view-" + view);
  const seen = new Set();
  let rows = "";
  for (const el of main ? main.querySelectorAll(".helpable") : []) {
    const term = normTerm(labelText(el));
    if (!term || seen.has(term)) continue;
    const text = helpFor(term, view);
    if (!text) continue;
    seen.add(term);
    rows += `<dt>${esc(term)}</dt><dd>${esc(text)}</dd>`;
  }
  const body = document.getElementById("help-body");
  body.innerHTML = rows
    ? `<dl>${rows}</dl>`
    : `<p class="muted">Nothing on this view has an explanation yet.</p>`;
  document.getElementById("help-count").textContent =
    seen.size ? `${seen.size} term${seen.size === 1 ? "" : "s"} on this view` : "";
}
let helpReturn = null;
function helpOpen() { return !document.getElementById("help").hidden; }
function openHelp() {
  if (helpOpen()) return;
  helpReturn = document.activeElement;
  renderHelpPanel();
  document.getElementById("help").hidden = false;
  document.getElementById("help-close").focus();
}
function closeHelp() {
  document.getElementById("help").hidden = true;
  const back = helpReturn && document.contains(helpReturn) ? helpReturn : document.body;
  helpReturn = null;
  if (document.activeElement instanceof HTMLElement) document.activeElement.blur();
  if (back instanceof HTMLElement && back !== document.body) back.focus();
}
function wireHelpControls() {
  observeHelp();
  document.getElementById("help-open").addEventListener("click", openHelp);
  document.getElementById("help-close").addEventListener("click", closeHelp);
  // The backdrop closes it; the panel itself does not.
  document.getElementById("help").addEventListener("click", ev => {
    if (ev.target.id === "help") closeHelp();
  });
  // One delegated listener for every term, because the elements
  // carrying them are replaced on every poll.
  document.addEventListener("click", ev => {
    const term = ev.target.closest(".helpable");
    if (term && document.getElementById("main-views").contains(term)) {
      // A heading that is also a link or holds a control keeps its own
      // behaviour; the explanation is the hover and the panel there.
      if (ev.target.closest("a, button, select, input")) return;
      ev.preventDefault();
      // Clicking the same term again closes it, so a term is a toggle
      // rather than a note that has to be dismissed somewhere else.
      const pop = helpPop();
      const same = pop && !pop.hidden && pop.dataset.term === normTerm(labelText(term));
      closeHelpPop();
      if (!same) openHelpPop(term);
      return;
    }
    if (!ev.target.closest("#help-pop")) closeHelpPop();
  });
  window.addEventListener("resize", closeHelpPop);
  window.addEventListener("hashchange", closeHelpPop);
}
