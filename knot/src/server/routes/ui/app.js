const API = "/api";
let currentNs = "";
let currentTable = "";
let currentLink = "";

// --- Init ---
document.addEventListener("DOMContentLoaded", () => {
  loadNamespaces();
  document.getElementById("ns-select").addEventListener("change", onNsChange);
  document.getElementById("view-select").addEventListener("change", refresh);
});

// --- Namespace ---
async function loadNamespaces() {
  const res = await fetch(`${API}/namespaces`);
  const names = await res.json();
  const sel = document.getElementById("ns-select");
  sel.innerHTML = '<option value="">— select —</option>';
  names.forEach((n) => {
    const opt = document.createElement("option");
    opt.value = n;
    opt.textContent = n;
    sel.appendChild(opt);
  });
}

async function createNamespace() {
  const name = prompt("Namespace name:");
  if (!name) return;
  await fetch(`${API}/namespaces`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  await loadNamespaces();
  document.getElementById("ns-select").value = name;
  onNsChange();
}

async function onNsChange() {
  currentNs = document.getElementById("ns-select").value;
  if (!currentNs) {
    document.getElementById("tables-nav").style.display = "none";
    document.getElementById("links-nav").style.display = "none";
    document.getElementById("content").style.display = "none";
    document.getElementById("welcome").style.display = "block";
    return;
  }
  document.getElementById("welcome").style.display = "none";
  document.getElementById("content").style.display = "block";
  document.getElementById("tables-nav").style.display = "block";
  document.getElementById("links-nav").style.display = "block";
  await loadTables();
  await loadLinks();
}

// --- Tables ---
async function loadTables() {
  const res = await fetch(`${API}/${currentNs}/m/tables`);
  const tables = await res.json();
  const ul = document.getElementById("tables-list");
  ul.innerHTML = "";
  tables.forEach((t) => {
    const li = document.createElement("li");
    li.textContent = t;
    li.onclick = () => selectTable(t);
    if (t === currentTable) li.className = "active";
    ul.appendChild(li);
  });
}

async function createTable() {
  if (!currentNs) return alert("Select a namespace first");
  const name = prompt("Table name:");
  if (!name) return;
  const res = await fetch(`${API}/${currentNs}/m/tables`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) {
    const err = await res.json();
    return alert("Error: " + err.error);
  }
  await loadTables();
  selectTable(name);
}

function selectTable(name) {
  currentTable = name;
  currentLink = "";
  document.getElementById("view-select").value = "nodes";
  loadTables();
  loadLinks();
  refresh();
}

// --- Links ---
async function loadLinks() {
  const res = await fetch(`${API}/${currentNs}/m/links`);
  const links = await res.json();
  const ul = document.getElementById("links-list");
  ul.innerHTML = "";
  links.forEach((l) => {
    const li = document.createElement("li");
    li.textContent = l;
    li.onclick = () => selectLink(l);
    if (l === currentLink) li.className = "active";
    ul.appendChild(li);
  });
}

async function createLink() {
  if (!currentNs) return alert("Select a namespace first");
  const name = prompt("Link table name:");
  if (!name) return;
  const source = prompt("Source table:");
  if (!source) return;
  const target = prompt("Target table:");
  if (!target) return;
  const bidi = confirm("Bidirectional?");

  const res = await fetch(`${API}/${currentNs}/m/links`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, source, target, bidirectional: bidi }),
  });
  if (!res.ok) {
    const err = await res.json();
    return alert("Error: " + err.error);
  }
  await loadLinks();
  selectLink(name);
}

function selectLink(name) {
  currentLink = name;
  currentTable = "";
  document.getElementById("view-select").value = "links";
  loadTables();
  loadLinks();
  refresh();
}

// --- Add Node ---
async function addNode() {
  if (!currentTable) return alert("Select a table first");
  const key = prompt("Node key:");
  if (!key) return;
  const propsStr = prompt(
    'Properties (JSON, e.g. {"age":30} or empty for set mode):',
  );

  let body = null;
  if (propsStr && propsStr.trim()) {
    try {
      body = JSON.parse(propsStr);
    } catch (e) {
      return alert("Invalid JSON: " + e.message);
    }
  }

  const res = await fetch(`${API}/${currentNs}/t/${currentTable}/${key}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : null,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "request failed" }));
    return alert("Error: " + err.error);
  }
  refresh();
}

// --- Add Link Entry ---
async function addLink() {
  if (!currentLink) return alert("Select a link table first");
  const from = prompt("From key:");
  if (!from) return;
  const to = prompt("To key:");
  if (!to) return;
  const propsStr = prompt("Properties (JSON or empty):");

  let body = null;
  if (propsStr && propsStr.trim()) {
    try {
      body = JSON.parse(propsStr);
    } catch (e) {
      return alert("Invalid JSON: " + e.message);
    }
  }

  const res = await fetch(
    `${API}/${currentNs}/l/${currentLink}/${from}/${to}`,
    {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : null,
    },
  );
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "request failed" }));
    return alert("Error: " + err.error);
  }
  refresh();
}

// --- Refresh ---
async function refresh() {
  const view = document.getElementById("view-select").value;
  const results = document.getElementById("results");

  if (view === "nodes" && currentTable) {
    await showNodes(results);
  } else if (view === "links" && currentLink) {
    await showLinkEntries(results);
  } else if (view === "traverse") {
    await showTraverseForm(results);
  } else {
    results.innerHTML =
      '<div class="empty">Select a table or link from the sidebar</div>';
  }
}

async function showNodes(el) {
  const res = await fetch(
    `${API}/${currentNs}/t/${currentTable}?detail=true&limit=50`,
  );
  const data = await res.json();

  if (!data.entries || data.entries.length === 0) {
    el.innerHTML =
      '<div class="empty">No nodes in this table. Click "+ Node" to add one.</div>';
    return;
  }

  const allKeys = new Set();
  data.entries.forEach((e) => {
    if (e.properties) Object.keys(e.properties).forEach((k) => allKeys.add(k));
  });
  const keys = [...allKeys].sort();

  let html = "<table><thead><tr><th>Key</th>";
  keys.forEach((k) => {
    html += `<th>${esc(k)}</th>`;
  });
  html += "<th></th></tr></thead><tbody>";

  data.entries.forEach((e) => {
    html += `<tr><td><strong>${esc(e.key)}</strong></td>`;
    keys.forEach((k) => {
      const v = e.properties ? e.properties[k] : null;
      html += `<td>${
        v !== null && v !== undefined
          ? esc(String(v))
          : '<span class="empty">—</span>'
      }</td>`;
    });
    html += `<td><button class="btn-sm btn-danger" onclick="deleteNode('${esc(
      e.key,
    )}')">×</button></td>`;
    html += "</tr>";
  });

  html += "</tbody></table>";
  if (data.has_more)
    html +=
      '<p style="margin-top:8px;color:#666">More results available...</p>';
  el.innerHTML = html;
}

async function showLinkEntries(el) {
  // Try scanning from all known source nodes — show first available
  const tablesRes = await fetch(`${API}/${currentNs}/m/tables`);
  const tables = await tablesRes.json();

  let allEntries = [];
  for (const t of tables) {
    const nodesRes = await fetch(`${API}/${currentNs}/t/${t}?limit=50`);
    const nodesData = await nodesRes.json();
    if (!nodesData.keys) continue;

    for (const key of nodesData.keys) {
      const linksRes = await fetch(
        `${API}/${currentNs}/l/${currentLink}?from=${encodeURIComponent(
          key,
        )}&detail=true`,
      );
      if (!linksRes.ok) continue;
      const linksData = await linksRes.json();
      if (linksData.entries) allEntries.push(...linksData.entries);
    }
    if (allEntries.length > 0) break;
  }

  if (allEntries.length === 0) {
    el.innerHTML =
      '<div class="empty">No link entries. Click "+ Link" to add one.</div>';
    return;
  }

  const allKeys = new Set();
  allEntries.forEach((e) => {
    if (e.properties) Object.keys(e.properties).forEach((k) => allKeys.add(k));
  });
  const keys = [...allKeys].sort();

  let html = "<table><thead><tr><th>From</th><th>To</th>";
  keys.forEach((k) => {
    html += `<th>${esc(k)}</th>`;
  });
  html += "<th></th></tr></thead><tbody>";

  allEntries.forEach((e) => {
    html += `<tr><td>${esc(e.from)}</td><td>${esc(e.to)}</td>`;
    keys.forEach((k) => {
      const v = e.properties ? e.properties[k] : null;
      html += `<td>${
        v !== null && v !== undefined
          ? esc(String(v))
          : '<span class="empty">—</span>'
      }</td>`;
    });
    html += `<td><button class="btn-sm btn-danger" onclick="deleteLinkEntry('${esc(
      e.from,
    )}','${esc(e.to)}')">×</button></td>`;
    html += "</tr>";
  });

  html += "</tbody></table>";
  el.innerHTML = html;
}

async function showTraverseForm(el) {
  el.innerHTML = `
    <div style="max-width:500px">
      <h3>Traverse</h3>
      <p style="margin:8px 0;color:#666">Follow links from a starting node.</p>
      <label>Table: <input id="tr-table" type="text" value="${esc(
        currentTable,
      )}" /></label><br/>
      <label>Key: <input id="tr-key" type="text" placeholder="alice" /></label><br/>
      <label>Links (comma-separated): <input id="tr-links" type="text" placeholder="attends,located-in" /></label><br/>
      <button class="btn-primary" onclick="doTraverse()" style="margin-top:8px">Traverse</button>
      <div id="tr-results" style="margin-top:12px"></div>
    </div>`;
}

async function doTraverse() {
  const table = document.getElementById("tr-table").value;
  const key = document.getElementById("tr-key").value;
  const links = document.getElementById("tr-links").value;
  if (!table || !key || !links) return alert("Fill all fields");

  const path = links
    .split(",")
    .map((s) => s.trim())
    .join("/");
  const res = await fetch(
    `${API}/${currentNs}/g/${table}/${key}/${path}?detail=true`,
  );
  const data = await res.json();
  const el = document.getElementById("tr-results");

  if (data.leaves && data.leaves.length > 0) {
    let html = "<strong>Results:</strong><ul>";
    data.leaves.forEach((l) => {
      html += `<li>${esc(l)}</li>`;
    });
    html += "</ul>";
    if (data.paths) {
      html += "<strong>Paths:</strong><ul>";
      data.paths.forEach((p) => {
        html += `<li>${p.map(esc).join(" → ")}</li>`;
      });
      html += "</ul>";
    }
    el.innerHTML = html;
  } else {
    el.innerHTML = '<div class="empty">No results</div>';
  }
}

// --- Delete ---
async function deleteNode(key) {
  if (!confirm(`Delete node "${key}"?`)) return;
  await fetch(`${API}/${currentNs}/t/${currentTable}/${key}`, {
    method: "DELETE",
  });
  refresh();
}

async function deleteLinkEntry(from, to) {
  if (!confirm(`Delete link ${from} → ${to}?`)) return;
  await fetch(`${API}/${currentNs}/l/${currentLink}/${from}/${to}`, {
    method: "DELETE",
  });
  refresh();
}

function esc(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/'/g, "&#39;");
}
