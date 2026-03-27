const API = "/api";
let currentNs = "";
let currentTable = "";
let currentLink = "";
let currentView = "nodes";

document.addEventListener("DOMContentLoaded", () => {
  loadNamespaces();
  document.getElementById("ns-select").addEventListener("change", onNsChange);
  document.getElementById("modal-overlay").addEventListener("click", (e) => {
    if (e.target === e.currentTarget) closeModal();
  });
});

// ==================== MODAL / TOAST ====================

function showModal(html) {
  document.getElementById("modal-box").innerHTML = html;
  document.getElementById("modal-overlay").style.display = "";
}

function closeModal() {
  document.getElementById("modal-overlay").style.display = "none";
}

function toast(msg, type = "info") {
  const c = document.getElementById("toast-container");
  const t = document.createElement("div");
  t.className = `toast ${type}`;
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => t.remove(), 3000);
}

// Helper: create a form modal that resolves with field values or null
function formModal(title, desc, fields) {
  return new Promise((resolve) => {
    let html = `<h3>${esc(title)}</h3>`;
    if (desc) html += `<p>${esc(desc)}</p>`;
    fields.forEach((f) => {
      if (f.type === "checkbox") {
        html += `<div class="form-field form-check"><label><input id="modal-${
          f.name
        }" type="checkbox" ${f.checked ? "checked" : ""} /> ${esc(
          f.label,
        )}</label></div>`;
      } else {
        html += `<div class="form-field"><label>${esc(f.label)}</label>`;
        html += `<input id="modal-${f.name}" type="text" placeholder="${esc(
          f.placeholder || "",
        )}" value="${esc(f.value || "")}" /></div>`;
      }
    });
    html += `<div class="modal-actions">`;
    html += `<button class="action-btn ghost" id="modal-cancel">Cancel</button>`;
    html += `<button class="action-btn" id="modal-ok">OK</button>`;
    html += `</div>`;
    showModal(html);
    const first = document.getElementById(`modal-${fields[0].name}`);
    if (first) first.focus();
    document.getElementById("modal-cancel").onclick = () => {
      closeModal();
      resolve(null);
    };
    document.getElementById("modal-ok").onclick = () => {
      const result = {};
      fields.forEach((f) => {
        const el = document.getElementById(`modal-${f.name}`);
        result[f.name] = f.type === "checkbox" ? el.checked : el.value;
      });
      closeModal();
      resolve(result);
    };
    // Enter key submits
    fields.forEach((f) => {
      document
        .getElementById(`modal-${f.name}`)
        .addEventListener("keydown", (e) => {
          if (e.key === "Enter") document.getElementById("modal-ok").click();
        });
    });
  });
}

function confirmModal(title, desc, danger = false) {
  return new Promise((resolve) => {
    let html = `<h3>${esc(title)}</h3>`;
    if (desc) html += `<p>${desc}</p>`;
    html += `<div class="modal-actions">`;
    html += `<button class="action-btn ghost" id="modal-cancel">Cancel</button>`;
    html += `<button class="action-btn ${
      danger ? "danger" : ""
    }" id="modal-ok">${danger ? "Drop" : "OK"}</button>`;
    html += `</div>`;
    showModal(html);
    document.getElementById("modal-ok").focus();
    document.getElementById("modal-cancel").onclick = () => {
      closeModal();
      resolve(false);
    };
    document.getElementById("modal-ok").onclick = () => {
      closeModal();
      resolve(true);
    };
  });
}

// ==================== NAMESPACE ====================

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
  const r = await formModal("New Namespace", null, [
    { name: "name", label: "Name", placeholder: "campus" },
  ]);
  if (!r || !r.name) return;
  const res = await fetch(`${API}/namespaces`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name: r.name }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "failed" }));
    return toast("Error: " + err.error, "error");
  }
  toast(`Namespace "${r.name}" created`, "success");
  await loadNamespaces();
  document.getElementById("ns-select").value = r.name;
  onNsChange();
}

async function dropNamespace() {
  if (!currentNs) return;
  const ok = await confirmModal(
    "Drop Namespace",
    `This will permanently delete <strong>${esc(
      currentNs,
    )}</strong> and all its tables, links, and data.`,
    true,
  );
  if (!ok) return;
  const res = await fetch(`${API}/${currentNs}`, { method: "DELETE" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "failed" }));
    return toast("Error: " + err.error, "error");
  }
  toast(`Namespace "${currentNs}" dropped`, "success");
  currentNs = "";
  currentTable = "";
  currentLink = "";
  document.getElementById("ns-select").value = "";
  onNsChange();
  await loadNamespaces();
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

// ==================== TABLES ====================

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
  if (!currentNs) return toast("Select a namespace first", "error");
  const r = await formModal("New Table", null, [
    { name: "name", label: "Table Name", placeholder: "person" },
  ]);
  if (!r || !r.name) return;
  const res = await fetch(`${API}/${currentNs}/m/tables`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name: r.name }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "failed" }));
    return toast("Error: " + err.error, "error");
  }
  toast(`Table "${r.name}" created`, "success");
  await loadTables();
  selectTable(r.name);
}

async function dropTable(name) {
  const ok = await confirmModal(
    "Drop Table",
    `Drop <strong>${esc(name)}</strong> and all its nodes and indexes?`,
    true,
  );
  if (!ok) return;
  const res = await fetch(`${API}/${currentNs}/m/tables/${name}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "failed" }));
    return toast("Error: " + err.error, "error");
  }
  toast(`Table "${name}" dropped`, "success");
  if (currentTable === name) {
    currentTable = "";
    document.getElementById("results").innerHTML =
      '<div class="empty">Table dropped.</div>';
  }
  await loadTables();
  await loadLinks();
}

function selectTable(name) {
  currentTable = name;
  currentLink = "";
  switchView("nodes");
  loadTables();
  loadLinks();
  refresh();
}

// ==================== LINKS ====================

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
  if (!currentNs) return toast("Select a namespace first", "error");
  const r = await formModal(
    "New Link Table",
    "Connect two tables with a named relationship.",
    [
      { name: "name", label: "Link Name", placeholder: "attends" },
      { name: "source", label: "Source Table", placeholder: "person" },
      { name: "target", label: "Target Table", placeholder: "school" },
      {
        name: "bidi",
        label: "Bidirectional",
        type: "checkbox",
        checked: false,
      },
    ],
  );
  if (!r || !r.name || !r.source || !r.target) return;
  const res = await fetch(`${API}/${currentNs}/m/links`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name: r.name,
      source: r.source,
      target: r.target,
      bidirectional: r.bidi,
    }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "failed" }));
    return toast("Error: " + err.error, "error");
  }
  toast(`Link table "${r.name}" created`, "success");
  await loadLinks();
  selectLink(r.name);
}

async function dropLink(name) {
  const ok = await confirmModal(
    "Drop Link Table",
    `Drop <strong>${esc(name)}</strong> and all its entries?`,
    true,
  );
  if (!ok) return;
  const res = await fetch(`${API}/${currentNs}/m/links/${name}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "failed" }));
    return toast("Error: " + err.error, "error");
  }
  toast(`Link table "${name}" dropped`, "success");
  if (currentLink === name) {
    currentLink = "";
    document.getElementById("results").innerHTML =
      '<div class="empty">Link table dropped.</div>';
  }
  await loadLinks();
}

function selectLink(name) {
  currentLink = name;
  currentTable = "";
  switchView("links");
  loadTables();
  loadLinks();
  refresh();
}

// ==================== VIEW ====================

function switchView(view) {
  currentView = view;
  document.querySelectorAll(".tab").forEach((t) => {
    t.classList.toggle("active", t.dataset.view === view);
  });
  refresh();
}

// ==================== ADD NODE / LINK ====================

async function addNode() {
  if (!currentNs) return toast("Select a namespace first", "error");
  const fields = [];
  if (!currentTable)
    fields.push({ name: "table", label: "Table", placeholder: "person" });
  fields.push({ name: "key", label: "Key", placeholder: "alice" });
  fields.push({
    name: "props",
    label: "Properties (JSON)",
    placeholder: '{"age":30}',
  });

  const r = await formModal("Insert Node", null, fields);
  if (!r || !r.key) return;

  const table = r.table || currentTable;
  if (!table) return toast("Table name required", "error");

  let body = null;
  if (r.props && r.props.trim()) {
    try {
      body = JSON.parse(r.props);
    } catch (e) {
      return toast("Invalid JSON: " + e.message, "error");
    }
  }
  const opts = { method: "PUT" };
  if (body) {
    opts.headers = { "Content-Type": "application/json" };
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`${API}/${currentNs}/t/${table}/${r.key}`, opts);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "failed" }));
    return toast("Error: " + err.error, "error");
  }
  toast(`Node "${r.key}" inserted`, "success");
  if (!currentTable) selectTable(table);
  else refresh();
}

async function addLink() {
  if (!currentNs) return toast("Select a namespace first", "error");
  const fields = [];
  if (!currentLink)
    fields.push({ name: "link", label: "Link Table", placeholder: "attends" });
  fields.push({ name: "from", label: "From Key", placeholder: "alice" });
  fields.push({ name: "to", label: "To Key", placeholder: "mit" });
  fields.push({
    name: "props",
    label: "Properties (JSON)",
    placeholder: '{"year":2020}',
  });

  const r = await formModal("Insert Link Entry", null, fields);
  if (!r || !r.from || !r.to) return;

  const link = r.link || currentLink;
  if (!link) return toast("Link table name required", "error");

  let body = null;
  if (r.props && r.props.trim()) {
    try {
      body = JSON.parse(r.props);
    } catch (e) {
      return toast("Invalid JSON: " + e.message, "error");
    }
  }
  const opts = { method: "PUT" };
  if (body) {
    opts.headers = { "Content-Type": "application/json" };
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(
    `${API}/${currentNs}/l/${link}/${r.from}/${r.to}`,
    opts,
  );
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "failed" }));
    return toast("Error: " + err.error, "error");
  }
  toast(`Link ${r.from} → ${r.to} created`, "success");
  if (!currentLink) selectLink(link);
  else refresh();
}

// ==================== REFRESH ====================

async function refresh() {
  const el = document.getElementById("results");
  if (currentView === "nodes" && currentTable) await showNodes(el);
  else if (currentView === "links" && currentLink) await showLinkEntries(el);
  else if (currentView === "traverse") showTraverseForm(el);
  else
    el.innerHTML =
      '<div class="empty">Select a table or link table from the sidebar.</div>';
}

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

function showTraverseForm(el) {
  el.innerHTML = `
    <div class="traverse-form">
      <h3>Graph Traversal</h3>
      <p>Follow links from a starting node to discover connected data.</p>
      <div class="form-field"><label>Start Table</label>
        <input id="tr-table" type="text" value="${esc(
          currentTable,
        )}" placeholder="person" /></div>
      <div class="form-field"><label>Start Key</label>
        <input id="tr-key" type="text" placeholder="alice" /></div>
      <div class="form-field"><label>Links (comma-separated)</label>
        <input id="tr-links" type="text" placeholder="attends, located-in" /></div>
      <button class="action-btn" onclick="doTraverse()" style="margin-top:4px">Traverse</button>
      <div id="tr-results"></div>
    </div>`;
}

async function doTraverse() {
  const table = document.getElementById("tr-table").value;
  const key = document.getElementById("tr-key").value;
  const links = document.getElementById("tr-links").value;
  if (!table || !key || !links) return toast("Fill all fields", "error");
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

// ==================== DELETE ====================

async function deleteNode(key) {
  const ok = await confirmModal(
    "Delete Node",
    `Delete <strong>${esc(key)}</strong>?`,
  );
  if (!ok) return;
  await fetch(`${API}/${currentNs}/t/${currentTable}/${key}`, {
    method: "DELETE",
  });
  toast(`Node "${key}" deleted`, "success");
  refresh();
}

async function deleteLinkEntry(from, to) {
  const ok = await confirmModal(
    "Delete Link",
    `Delete <strong>${esc(from)} → ${esc(to)}</strong>?`,
  );
  if (!ok) return;
  await fetch(`${API}/${currentNs}/l/${currentLink}/${from}/${to}`, {
    method: "DELETE",
  });
  toast(`Link ${from} → ${to} deleted`, "success");
  refresh();
}

function esc(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/'/g, "&#39;")
    .replace(/"/g, "&quot;");
}
