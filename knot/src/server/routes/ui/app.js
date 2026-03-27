const API = "/api";
let currentNs = "";
let currentTable = "";
let currentLink = "";
let currentView = "nodes";

document.addEventListener("DOMContentLoaded", () => {
  loadNamespaces();
  document.getElementById("ns-select").addEventListener("change", onNsChange);
});

// ===== Namespace =====
async function loadNamespaces() {
  const res = await fetch(`${API}/namespaces`);
  const names = await res.json();
  const sel = document.getElementById("ns-select");
  sel.innerHTML = '<option value="">choose...</option>';
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
  const res = await fetch(`${API}/namespaces`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "request failed" }));
    return alert("Error creating namespace: " + err.error);
  }
  await loadNamespaces();
  document.getElementById("ns-select").value = name;
  onNsChange();
}

async function onNsChange() {
  currentNs = document.getElementById("ns-select").value;
  const show = !!currentNs;
  document.getElementById("tables-nav").style.display = show ? "" : "none";
  document.getElementById("links-nav").style.display = show ? "" : "none";
  document.getElementById("content").style.display = show ? "" : "none";
  document.getElementById("welcome").style.display = show ? "none" : "";
  if (show) {
    await loadTables();
    await loadLinks();
  }
}

// ===== Tables =====
async function loadTables() {
  const res = await fetch(`${API}/${currentNs}/m/tables`);
  const tables = await res.json();
  const ul = document.getElementById("tables-list");
  ul.innerHTML = "";
  tables.forEach((t) => {
    const li = document.createElement("li");
    const span = document.createElement("span");
    span.textContent = t;
    span.style.flex = "1";
    span.onclick = () => selectTable(t);
    li.appendChild(span);
    const del = document.createElement("button");
    del.className = "sidebar-del";
    del.textContent = "✕";
    del.title = "Drop table";
    del.onclick = (e) => {
      e.stopPropagation();
      dropTable(t);
    };
    li.appendChild(del);
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
    const err = await res.json().catch(() => ({ error: "request failed" }));
    return alert("Error: " + err.error);
  }
  await loadTables();
  selectTable(name);
}

function selectTable(name) {
  currentTable = name;
  currentLink = "";
  switchView("nodes");
  loadTables();
  loadLinks();
  refresh();
}

// ===== Links =====
async function loadLinks() {
  const res = await fetch(`${API}/${currentNs}/m/links`);
  const links = await res.json();
  const ul = document.getElementById("links-list");
  ul.innerHTML = "";
  links.forEach((l) => {
    const li = document.createElement("li");
    const span = document.createElement("span");
    span.textContent = l;
    span.style.flex = "1";
    span.onclick = () => selectLink(l);
    li.appendChild(span);
    const del = document.createElement("button");
    del.className = "sidebar-del";
    del.textContent = "✕";
    del.title = "Drop link table";
    del.onclick = (e) => {
      e.stopPropagation();
      dropLink(l);
    };
    li.appendChild(del);
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
    const err = await res.json().catch(() => ({ error: "request failed" }));
    return alert("Error: " + err.error);
  }
  await loadLinks();
  selectLink(name);
}

function selectLink(name) {
  currentLink = name;
  currentTable = "";
  switchView("links");
  loadTables();
  loadLinks();
  refresh();
}

// ===== View Switching =====
function switchView(view) {
  currentView = view;
  document.querySelectorAll(".tab").forEach((t) => {
    t.classList.toggle("active", t.dataset.view === view);
  });
  refresh();
}

// ===== Add Node / Link =====
async function addNode() {
  if (!currentNs) return alert("Select a namespace first");
  let table = currentTable;
  if (!table) {
    table = prompt("Table name:");
    if (!table) return;
  }
  const key = prompt("Node key:");
  if (!key) return;
  const propsStr = prompt(
    'Properties as JSON (e.g. {"age":30}) or leave empty:',
  );
  let body = null;
  if (propsStr && propsStr.trim()) {
    try {
      body = JSON.parse(propsStr);
    } catch (e) {
      return alert("Invalid JSON: " + e.message);
    }
  }
  const opts = { method: "PUT" };
  if (body) {
    opts.headers = { "Content-Type": "application/json" };
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`${API}/${currentNs}/t/${table}/${key}`, opts);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "request failed" }));
    return alert("Error: " + err.error);
  }
  if (!currentTable) {
    selectTable(table);
  } else {
    refresh();
  }
}

async function addLink() {
  if (!currentNs) return alert("Select a namespace first");
  let link = currentLink;
  if (!link) {
    link = prompt("Link table name:");
    if (!link) return;
  }
  const from = prompt("From key (table.key):");
  if (!from) return;
  const to = prompt("To key:");
  if (!to) return;
  const propsStr = prompt("Properties as JSON or leave empty:");
  let body = null;
  if (propsStr && propsStr.trim()) {
    try {
      body = JSON.parse(propsStr);
    } catch (e) {
      return alert("Invalid JSON: " + e.message);
    }
  }
  const opts = { method: "PUT" };
  if (body) {
    opts.headers = { "Content-Type": "application/json" };
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`${API}/${currentNs}/l/${link}/${from}/${to}`, opts);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "request failed" }));
    return alert("Error: " + err.error);
  }
  if (!currentLink) {
    selectLink(link);
  } else {
    refresh();
  }
}

// ===== Refresh =====
async function refresh() {
  const el = document.getElementById("results");
  if (currentView === "nodes" && currentTable) {
    await showNodes(el);
  } else if (currentView === "links" && currentLink) {
    await showLinkEntries(el);
  } else if (currentView === "traverse") {
    showTraverseForm(el);
  } else {
    el.innerHTML =
      '<div class="empty">Select a table or link table from the sidebar.</div>';
  }
}

// ===== Nodes =====
async function showNodes(el) {
  const res = await fetch(
    `${API}/${currentNs}/t/${currentTable}?detail=true&limit=100`,
  );
  const data = await res.json();
  if (!data.entries || data.entries.length === 0) {
    el.innerHTML =
      '<div class="empty">Empty table. Click <b>+ Node</b> to insert.</div>';
    return;
  }
  const allKeys = new Set();
  data.entries.forEach((e) => {
    if (e.properties) Object.keys(e.properties).forEach((k) => allKeys.add(k));
  });
  const keys = [...allKeys].sort();

  let html = "<table><thead><tr><th>Key</th>";
  keys.forEach((k) => (html += `<th>${esc(k)}</th>`));
  html += "<th></th></tr></thead><tbody>";
  data.entries.forEach((e) => {
    html += `<tr><td><strong>${esc(e.key)}</strong></td>`;
    keys.forEach((k) => {
      const v = e.properties ? e.properties[k] : null;
      html += `<td>${
        v != null
          ? esc(String(v))
          : '<span style="color:var(--text-muted)">—</span>'
      }</td>`;
    });
    html += `<td><button class="btn-del" onclick="deleteNode('${esc(
      e.key,
    )}')">✕</button></td></tr>`;
  });
  html += "</tbody></table>";
  html += `<div class="count-bar">${data.entries.length} node${
    data.entries.length !== 1 ? "s" : ""
  }${data.has_more ? " (more available)" : ""}</div>`;
  el.innerHTML = html;
}

// ===== Links =====
async function showLinkEntries(el) {
  const tablesRes = await fetch(`${API}/${currentNs}/m/tables`);
  const tables = await tablesRes.json();
  let allEntries = [];
  for (const t of tables) {
    const nr = await fetch(`${API}/${currentNs}/t/${t}?limit=100`);
    const nd = await nr.json();
    if (!nd.keys) continue;
    for (const key of nd.keys) {
      const lr = await fetch(
        `${API}/${currentNs}/l/${currentLink}?from=${encodeURIComponent(
          key,
        )}&detail=true`,
      );
      if (!lr.ok) continue;
      const ld = await lr.json();
      if (ld.entries) allEntries.push(...ld.entries);
    }
    if (allEntries.length > 0) break;
  }
  if (allEntries.length === 0) {
    el.innerHTML =
      '<div class="empty">No link entries. Click <b>+ Link</b> to create.</div>';
    return;
  }
  const allKeys = new Set();
  allEntries.forEach((e) => {
    if (e.properties) Object.keys(e.properties).forEach((k) => allKeys.add(k));
  });
  const keys = [...allKeys].sort();
  let html = "<table><thead><tr><th>From</th><th>To</th>";
  keys.forEach((k) => (html += `<th>${esc(k)}</th>`));
  html += "<th></th></tr></thead><tbody>";
  allEntries.forEach((e) => {
    html += `<tr><td><strong>${esc(e.from)}</strong></td><td><strong>${esc(
      e.to,
    )}</strong></td>`;
    keys.forEach((k) => {
      const v = e.properties ? e.properties[k] : null;
      html += `<td>${
        v != null
          ? esc(String(v))
          : '<span style="color:var(--text-muted)">—</span>'
      }</td>`;
    });
    html += `<td><button class="btn-del" onclick="deleteLinkEntry('${esc(
      e.from,
    )}','${esc(e.to)}')">✕</button></td></tr>`;
  });
  html += "</tbody></table>";
  html += `<div class="count-bar">${allEntries.length} link${
    allEntries.length !== 1 ? "s" : ""
  }</div>`;
  el.innerHTML = html;
}

// ===== Traverse =====
function showTraverseForm(el) {
  el.innerHTML = `
    <div class="traverse-form">
      <h3>Graph Traversal</h3>
      <p>Follow links from a starting node to discover connected data.</p>
      <div class="form-field">
        <label>Start Table</label>
        <input id="tr-table" type="text" value="${esc(
          currentTable,
        )}" placeholder="person" />
      </div>
      <div class="form-field">
        <label>Start Key</label>
        <input id="tr-key" type="text" placeholder="alice" />
      </div>
      <div class="form-field">
        <label>Links (comma-separated)</label>
        <input id="tr-links" type="text" placeholder="attends, located-in" />
      </div>
      <button class="action-btn" onclick="doTraverse()" style="margin-top:4px">Traverse</button>
      <div id="tr-results"></div>
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
    let html =
      '<div class="traverse-results"><strong>Destinations</strong><ul>';
    data.leaves.forEach((l) => (html += `<li>${esc(l)}</li>`));
    html += "</ul>";
    if (data.paths) {
      html += "<strong>Paths</strong><ul>";
      data.paths.forEach((p) => (html += `<li>${p.map(esc).join(" → ")}</li>`));
      html += "</ul>";
    }
    html += "</div>";
    el.innerHTML = html;
  } else {
    el.innerHTML =
      '<div class="traverse-results empty">No results found.</div>';
  }
}

// ===== Delete =====
async function deleteNode(key) {
  if (!confirm(`Delete "${key}"?`)) return;
  await fetch(`${API}/${currentNs}/t/${currentTable}/${key}`, {
    method: "DELETE",
  });
  refresh();
}

async function deleteLinkEntry(from, to) {
  if (!confirm(`Delete ${from} → ${to}?`)) return;
  await fetch(`${API}/${currentNs}/l/${currentLink}/${from}/${to}`, {
    method: "DELETE",
  });
  refresh();
}

async function dropTable(name) {
  if (!confirm(`Drop table "${name}" and all its data?`)) return;
  const res = await fetch(`${API}/${currentNs}/m/tables/${name}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "request failed" }));
    return alert("Error: " + err.error);
  }
  if (currentTable === name) {
    currentTable = "";
    document.getElementById("results").innerHTML =
      '<div class="empty">Table dropped.</div>';
  }
  await loadTables();
  await loadLinks();
}

async function dropLink(name) {
  if (!confirm(`Drop link table "${name}" and all its entries?`)) return;
  const res = await fetch(`${API}/${currentNs}/m/links/${name}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "request failed" }));
    return alert("Error: " + err.error);
  }
  if (currentLink === name) {
    currentLink = "";
    document.getElementById("results").innerHTML =
      '<div class="empty">Link table dropped.</div>';
  }
  await loadLinks();
}

async function dropNamespace() {
  if (!currentNs) return;
  if (
    !confirm(
      `Drop namespace "${currentNs}" and ALL its tables, links, and data?`,
    )
  )
    return;
  const res = await fetch(`${API}/${currentNs}`, { method: "DELETE" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "request failed" }));
    return alert("Error: " + err.error);
  }
  currentNs = "";
  currentTable = "";
  currentLink = "";
  document.getElementById("ns-select").value = "";
  onNsChange();
  await loadNamespaces();
}

function esc(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/'/g, "&#39;");
}
