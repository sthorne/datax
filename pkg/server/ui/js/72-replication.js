// Replication status and failure domains (issue #152). Rack-aware
// placement is the headline claim; this is where it is either holding or
// not. Everything here comes from /api/cluster's replication section,
// computed on the server so every node reports the same thing.
//
// Read-only, like the rest of the console: this says four ranges are
// under-replicated on rack c; the operator acts through `datax debug`.

let openBucket = null; // which bucket's range list is expanded

const REPL_BUCKETS = [
  { key: "healthy", label: "healthy", hint: "at the target replica count, every replica live" },
  { key: "under_replicated", label: "under-replicated", hint: "fewer live replicas than the target; the allocator repairs them as nodes allow", warn: "draining" },
  { key: "over_replicated", label: "over-replicated", hint: "more replicas than the target, usually an interrupted rebalance; the allocator trims one per pass", warn: "draining" },
  { key: "no_quorum", label: "no quorum", hint: "fewer than a majority of replicas live: the range cannot serve until nodes return", warn: "down" },
  { key: "undiverse", label: "undiverse", hint: "two replicas share a locality they could spread across; a single failure domain could cost two", warn: "draining" },
];

function renderReplication(d) {
  const rep = d.replication || {};
  renderTiles(document.getElementById("repl-tiles"), REPL_BUCKETS.map(b => {
    const n = (rep[b.key] || {}).count || 0;
    const cls = n > 0 && b.warn ? ` ${b.warn}` : "";
    const open = openBucket === b.key;
    return `<a class="tile${cls}" data-key="${b.key}" href="#" role="button" aria-expanded="${open}"
      data-bucket="${b.key}" title="${esc(b.hint)}"><div class="label">${esc(b.label)}</div><div class="value">${n}</div></a>`;
  }).join(""));

  setHTML(document.getElementById("repl-note"),
    `measured against each range's target replica count — the cluster default is ${rep.default_replication_factor ?? 3}, ` +
    `and a database with a placement policy is measured against its own (<code>SHOW PLACEMENT</code>). ` +
    `A range is counted once, worst state first: one with no quorum is not also counted as under-replicated. ` +
    `Select a state for its ranges.`);

  renderBucketDetail(rep);
  renderDomains(rep, d);
  renderHotRanges(d);
}

function renderBucketDetail(rep) {
  const wrap = document.getElementById("repl-bucket-detail");
  const b = openBucket && rep[openBucket];
  if (!b || !b.count) {
    wrap.hidden = true;
    renderKeyed(document.getElementById("repl-bucket-ranges"), []);
    return;
  }
  wrap.hidden = false;
  const byID = new Map((lastCluster?.ranges || []).map(r => [r.range_id, r]));
  const ids = b.ranges || [];
  renderKeyed(document.getElementById("repl-bucket-ranges"), ids.map(id => {
    const r = byID.get(id);
    return {
      key: "b" + id,
      html: `<tr data-key="b${id}">
        <td><a href="#/data?range=${id}" title="open r${id}'s per-replica detail">r${id}</a></td>
        <td class="key">${r ? spanText(r) : "—"}</td>
        <td>${r ? (r.replicas || []).map(x => "n" + x).join(" ") : ""}</td>
      </tr>`,
    };
  }));
  const label = (REPL_BUCKETS.find(x => x.key === openBucket) || {}).label || openBucket;
  setHTML(document.getElementById("repl-bucket-note"),
    b.truncated
      ? `showing ${ids.length} of ${b.count} ${esc(label)} ranges — the count is exact, the list is capped so a status document stays bounded`
      : `${b.count} ${esc(label)} range(s)`);
}

{
  const tiles = document.getElementById("repl-tiles");
  tiles.addEventListener("click", ev => {
    const t = ev.target.closest("[data-bucket]");
    if (!t || !tiles.contains(t)) return;
    ev.preventDefault();
    openBucket = openBucket === t.dataset.bucket ? null : t.dataset.bucket;
    if (lastCluster) renderReplication(lastCluster);
  });
}

function renderDomains(rep, d) {
  const domains = rep.domains || [];
  renderKeyed(document.getElementById("repl-domains"), domains.map(dm => {
    const key = dm.tier + "=" + dm.value;
    const risk = dm.loses_quorum > 0 ? "down" : dm.bare_majority > 0 ? "draining" : "";
    return {
      key,
      html: `<tr data-key="${esc(key)}">
        <td><b>${esc(dm.tier)}</b>=${esc(dm.value)}</td>
        <td class="num">${dm.nodes}${(dm.live_nodes ?? dm.nodes) < dm.nodes ? ` <span class="st down">${dm.live_nodes} live</span>` : ""}</td>
        <td class="num">${dm.replicas}</td>
        <td class="num">${dm.leases}</td>
        <td class="num">${dm.loses_quorum > 0
          ? `<span class="st ${risk}">${dm.loses_quorum}</span>${dm.example_at_risk_range ? ` <span class="muted">e.g. r${dm.example_at_risk_range}</span>` : ""}`
          : "0"}</td>
        <td class="num">${dm.bare_majority || 0}</td>
      </tr>`,
    };
  }));
  const tiers = rep.tiers || [];
  let note = domains.length
    ? `what the loss of a whole domain would cost, computed from the range descriptors and the nodes' declared localities — no fan-out, so it answers even from a partitioned node. Tiers in use: ${tiers.map(esc).join(" › ")}.`
    : `no node declares a locality, so there are no failure domains to project. Start nodes with <code>--locality=region=…,rack=…</code> and the allocator spreads replicas across them.`;
  if (rep.unlocalized_nodes) {
    note += ` ${rep.unlocalized_nodes} node(s) declare no locality and belong to no domain, so nothing below accounts for them.`;
  }
  setHTML(document.getElementById("repl-domain-note"), note);
}

// Range hotspots come from the nodes' own heartbeats — every node
// advertises its heaviest leaseholders and largest replicas for the
// allocator, so this needs no fan-out either. QPS is the LEADER's local
// measurement of ranges it leads, never a cluster total.
function renderHotRanges(d) {
  const byID = new Map((d.ranges || []).map(r => [r.range_id, r]));
  const rows = new Map();
  for (const n of d.nodes || []) {
    for (const h of n.hot_ranges || []) {
      const e = rows.get(h.range_id) || { id: h.range_id };
      if ((h.qps || 0) > (e.qps || 0)) { e.qps = h.qps; e.leader = n.node_id; }
      rows.set(h.range_id, e);
    }
    for (const h of n.big_ranges || []) {
      const e = rows.get(h.range_id) || { id: h.range_id };
      e.bytes = Math.max(e.bytes || 0, h.bytes || 0);
      rows.set(h.range_id, e);
    }
  }
  const list = [...rows.values()]
    .sort((a, b) => (b.qps || 0) - (a.qps || 0) || (b.bytes || 0) - (a.bytes || 0))
    .slice(0, 20);
  renderKeyed(document.getElementById("repl-hot"), list.map(e => {
    const r = byID.get(e.id);
    return {
      key: "h" + e.id,
      html: `<tr data-key="h${e.id}">
        <td><a href="#/data?range=${e.id}" title="open r${e.id}'s per-replica detail">r${e.id}</a></td>
        <td class="key">${r ? spanText(r) : "—"}</td>
        <td>${e.leader ? "n" + e.leader : "—"}</td>
        <td class="num">${e.qps ? e.qps.toFixed(0) : "—"}</td>
        <td class="num">${e.bytes ? fmtBytes(e.bytes) : "—"}</td>
      </tr>`,
    };
  }));
  setHTML(document.getElementById("repl-hot-note"), list.length
    ? "the heaviest and largest ranges each node advertises in its heartbeat. QPS is the leader's own rate over the ranges it leads — it is not summed across nodes, and a range whose leader is down reports none."
    : "no node is advertising a hot or large range yet; the lists fill once a range's rate tracker matures.");
}
