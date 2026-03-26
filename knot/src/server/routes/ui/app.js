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

function selectTable(name) {
  currentTable = name;
  currentLink = "";
  document.getElementById("view-select").value = "nodes";
  loadTables();
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

function selectLink(name) {
  currentLink = name;
  currentTable = "";
  document.getElementById("view-select").value = "links";
  loadLinks();
  refresh();
}

// --- Refresh ---
async function refresh() {
  const view = document.getElementById("view-select").value;
  const results = document.getElementById("results");

  if (view === "nodes" && currentTable) {
    await showNodes(results);
  } else if (view === "links" && currentLink) {
    results.innerHTML =
      '<div class="empty">Select a source key with ?from= to scan links</div>';
  } else if (view === "traverse") {
    results.innerHTML =
      '<div class="empty">Traversal: use the API directly at /api/{ns}/g/...</div>';
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
    el.innerHTML = '<div class="empty">No nodes in this table</div>';
    return;
  }

  // Collect all property keys
  const allKeys = new Set();
  data.entries.forEach((e) => {
    if (e.properties) Object.keys(e.properties).forEach((k) => allKeys.add(k));
  });
  const keys = [...allKeys].sort();

  let html = "<table><thead><tr><th>Key</th>";
  keys.forEach((k) => {
    html += `<th>${esc(k)}</th>`;
  });
  html += "</tr></thead><tbody>";

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
    html += "</tr>";
  });

  html += "</tbody></table>";
  if (data.has_more)
    html +=
      '<p style="margin-top:8px;color:#666">More results available...</p>';
  el.innerHTML = html;
}

function esc(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
