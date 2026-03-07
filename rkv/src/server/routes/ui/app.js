/* rKV Web UI — vanilla SPA */
"use strict";

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------
function $(sel, root) {
  return (root || document).querySelector(sel);
}
function $$(sel, root) {
  return (root || document).querySelectorAll(sel);
}

function el(tag, attrs, children) {
  var node = document.createElement(tag);
  if (attrs)
    Object.keys(attrs).forEach(function (k) {
      if (k === "textContent") {
        node.textContent = attrs[k];
      } else if (k === "className") {
        node.className = attrs[k];
      } else if (k.slice(0, 2) === "on") {
        node.addEventListener(k.slice(2).toLowerCase(), attrs[k]);
      } else {
        node.setAttribute(k, attrs[k]);
      }
    });
  if (children)
    children.forEach(function (c) {
      if (typeof c === "string") node.appendChild(document.createTextNode(c));
      else if (c) node.appendChild(c);
    });
  return node;
}

// ---------------------------------------------------------------------------
// Toast
// ---------------------------------------------------------------------------
var toastTimer = null;
function toast(msg, ok) {
  var t = $("#toast");
  t.textContent = msg;
  t.className = ok ? "toast success" : "toast";
  clearTimeout(toastTimer);
  toastTimer = setTimeout(function () {
    t.className = "toast hidden";
  }, 3000);
}

// ---------------------------------------------------------------------------
// API client
// ---------------------------------------------------------------------------
function api(method, path, body) {
  var opts = { method: method, headers: {}, cache: "no-store" };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  return fetch(path, opts).then(function (r) {
    if (r.status === 410) {
      return { status: 410, headers: r.headers, data: null };
    }
    if (!r.ok) {
      return r.text().then(function (t) {
        throw new Error(r.status + " " + (t || r.statusText));
      });
    }
    var ct = r.headers.get("content-type") || "";
    if (r.status === 204 || r.status === 202) {
      return { status: r.status, headers: r.headers, data: null };
    }
    if (ct.indexOf("json") !== -1) {
      return r.json().then(function (d) {
        return { status: r.status, headers: r.headers, data: d };
      });
    }
    return r.text().then(function (t) {
      return { status: r.status, headers: r.headers, data: t };
    });
  });
}

// ---------------------------------------------------------------------------
// Binary detection & download
// ---------------------------------------------------------------------------
// eslint-disable-next-line no-control-regex
var BINARY_RE = /[\x00-\x08\x0e-\x1f]/;
function isBinary(str) {
  return typeof str === "string" && BINARY_RE.test(str);
}

function downloadBlob(url, filename) {
  fetch(url)
    .then(function (r) {
      if (!r.ok) throw new Error(r.status);
      return r.blob();
    })
    .then(function (blob) {
      var a = document.createElement("a");
      var url = URL.createObjectURL(blob);
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(function () {
        URL.revokeObjectURL(url);
      }, 1000);
    })
    .catch(function (e) {
      toast("Download: " + e.message);
    });
}

function downloadKey(key) {
  var url =
    "/api/" + encodeURIComponent(state.ns) + "/keys/" + encodeURIComponent(key);
  downloadBlob(url, key);
}

function downloadRevision(key, idx) {
  var url =
    "/api/" +
    encodeURIComponent(state.ns) +
    "/keys/" +
    encodeURIComponent(key) +
    "/revisions/" +
    idx;
  downloadBlob(url, key + ".rev" + idx);
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
var state = {
  ns: "_",
  namespaces: [],
  keys: [],
  hasMore: false,
  offset: 0,
  prefix: "",
  showDeleted: false,
  role: "standalone",
};
var renderGen = 0;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------
function route() {
  var hash = location.hash.replace("#", "") || "keys";
  var links = $$(".nav-link");
  for (var i = 0; i < links.length; i++) {
    links[i].classList.toggle(
      "active",
      links[i].getAttribute("data-route") === hash,
    );
  }
  var app = $("#app");
  app.innerHTML = "";
  if (hash === "keys") renderKeys(app);
  else if (hash === "admin") renderAdmin(app);
  else if (hash === "namespaces") renderNamespaces(app);
  else renderKeys(app);
}

window.addEventListener("hashchange", route);

// ---------------------------------------------------------------------------
// Namespace loader (shared)
// ---------------------------------------------------------------------------
function loadNamespaces() {
  return api("GET", "/api/namespaces")
    .then(function (r) {
      state.namespaces = r.data || [];
      if (state.namespaces.indexOf(state.ns) === -1) {
        if (state.namespaces.length > 0) {
          state.ns = state.namespaces[0];
        } else {
          state.namespaces = ["_"];
          state.ns = "_";
        }
      }
    })
    .catch(function (e) {
      toast("Load namespaces: " + e.message);
    });
}

// ---------------------------------------------------------------------------
// Keys view
// ---------------------------------------------------------------------------
function renderKeys(app) {
  app.appendChild(el("h2", { textContent: "Keys" }));

  var toolbar = el("div", { className: "toolbar" });

  // Namespace selector
  var nsSelect = el("select", {
    id: "ns-select",
    onChange: function () {
      state.ns = this.value;
      state.offset = 0;
      loadKeys();
    },
  });
  toolbar.appendChild(nsSelect);

  // Prefix filter
  var pfx = el("input", {
    type: "text",
    placeholder: "prefix filter…",
    value: state.prefix,
    onKeyup: function (e) {
      if (e.key === "Enter") {
        state.prefix = this.value;
        state.offset = 0;
        loadKeys();
      }
    },
  });
  toolbar.appendChild(pfx);

  toolbar.appendChild(
    el("button", {
      className: "btn-green",
      textContent: "Search",
      onClick: function () {
        state.prefix = pfx.value;
        state.offset = 0;
        loadKeys();
      },
    }),
  );

  var delBox = el("input", { type: "checkbox", id: "del-check" });
  if (state.showDeleted) delBox.checked = true;
  delBox.addEventListener("change", function () {
    state.showDeleted = delBox.checked;
    state.offset = 0;
    loadKeys();
  });
  toolbar.appendChild(
    el("label", { className: "del-toggle" }, [
      delBox,
      document.createTextNode(" Show deleted"),
    ]),
  );

  var newKeyBtn = el("button", {
    className: "btn-blue",
    textContent: "+ New Key",
    onClick: openCreateDialog,
  });
  if (state.role === "replica") newKeyBtn.disabled = true;
  toolbar.appendChild(newKeyBtn);

  app.appendChild(toolbar);

  // Table
  var table = el("table");
  var thead = el("thead", null, [
    el("tr", null, [
      el("th", { textContent: "Key" }),
      el("th", { textContent: "Value" }),
      el("th", { textContent: "Rev" }),
      el("th", { textContent: "TTL" }),
      el("th", { textContent: "Actions" }),
    ]),
  ]);
  table.appendChild(thead);
  var tbody = el("tbody", { id: "keys-body" });
  table.appendChild(tbody);
  app.appendChild(table);

  // Pagination
  var pag = el("div", { className: "pagination", id: "keys-pag" });
  app.appendChild(pag);

  // Load
  loadNamespaces().then(function () {
    populateNsSelect();
    loadKeys();
  });
}

function populateNsSelect() {
  var sel = $("#ns-select");
  if (!sel) return;
  sel.innerHTML = "";
  state.namespaces.forEach(function (n) {
    var opt = el("option", { value: n, textContent: n });
    if (n === state.ns) opt.selected = true;
    sel.appendChild(opt);
  });
}

function loadKeys() {
  var qs = "?offset=" + state.offset;
  if (state.prefix) qs += "&prefix=" + encodeURIComponent(state.prefix);
  if (state.showDeleted) qs += "&deleted=true";
  api("GET", "/api/" + encodeURIComponent(state.ns) + "/keys" + qs)
    .then(function (r) {
      state.keys = r.data || [];
      state.hasMore = r.headers.get("X-RKV-Has-More") === "true";
      renderKeyRows();
    })
    .catch(function (e) {
      toast("Load keys: " + e.message);
    });
}

function renderKeyRows() {
  var tbody = $("#keys-body");
  if (!tbody) return;
  tbody.innerHTML = "";
  var gen = ++renderGen;

  if (state.keys.length === 0) {
    var row = el("tr", null, [
      el("td", {
        colSpan: "5",
        className: "empty",
        textContent: "No keys found",
      }),
    ]);
    tbody.appendChild(row);
    renderPagination();
    return;
  }

  // Fetch values and revision counts for each key
  var nsPath = "/api/" + encodeURIComponent(state.ns);
  var promises = state.keys.map(function (k) {
    var keyPath = nsPath + "/keys/" + encodeURIComponent(k);
    var valP = api("GET", keyPath)
      .then(function (r) {
        return {
          value: r.data,
          status: r.status,
          expires: r.headers.get("Expires"),
        };
      })
      .catch(function () {
        return { value: null, status: 0, expires: null };
      });
    var revP = api("GET", keyPath + "/revisions")
      .then(function (r) {
        return r.data || 0;
      })
      .catch(function () {
        return 0;
      });
    return Promise.all([valP, revP]).then(function (pair) {
      return {
        key: k,
        value: pair[0].value,
        status: pair[0].status,
        expires: pair[0].expires,
        revCount: pair[1],
      };
    });
  });

  Promise.all(promises).then(function (entries) {
    if (gen !== renderGen) return;
    entries.forEach(function (entry) {
      var isDeleted = entry.status === 410;
      var binary =
        !isDeleted &&
        entry.status !== 204 &&
        entry.value != null &&
        isBinary(String(entry.value));
      var valText;
      if (isDeleted) valText = "(deleted)";
      else if (entry.status === 204) valText = "(null)";
      else if (entry.value == null) valText = "(empty)";
      else if (binary) valText = "(binary)";
      else valText = String(entry.value);
      if (!isDeleted && !binary && valText.length > 80)
        valText = valText.slice(0, 77) + "...";
      var ttlText = entry.expires || "-";

      // Value cell: show text or binary badge + download
      var valCell = el("td");
      if (isDeleted) {
        valCell.appendChild(
          el("span", { className: "val-deleted", textContent: "(deleted)" }),
        );
      } else if (binary) {
        valCell.appendChild(
          el("span", { className: "val-binary", textContent: "(binary)" }),
        );
        valCell.appendChild(
          el("button", {
            className: "btn-download",
            textContent: "Download",
            onClick: function () {
              downloadKey(entry.key);
            },
          }),
        );
      } else {
        valCell.textContent = valText;
      }

      var revCell = el("td", null, [
        el("button", {
          className: "btn-rev",
          textContent: entry.revCount + " rev",
          onClick: function () {
            openRevDialog(entry.key, entry.revCount);
          },
        }),
      ]);

      var tr = el("tr", null, [
        el("td", { textContent: entry.key }),
        valCell,
        revCell,
        el("td", { textContent: ttlText }),
        el("td", null, [
          el(
            "div",
            { className: "actions" },
            (function () {
              if (isDeleted) return [];
              var btns = [];
              var editBtn = el("button", {
                className: "btn-green",
                textContent: "Edit",
                onClick: function () {
                  openEditDialog(
                    entry.key,
                    entry.value,
                    entry.status === 204,
                    entry.expires,
                  );
                },
              });
              if (state.role === "replica") editBtn.disabled = true;
              btns.push(editBtn);
              var delBtn = el("button", {
                className: "btn-red",
                textContent: "Del",
                onClick: function () {
                  deleteKey(entry.key);
                },
              });
              if (state.role === "replica") delBtn.disabled = true;
              btns.push(delBtn);
              return btns;
            })(),
          ),
        ]),
      ]);
      tbody.appendChild(tr);
    });
    renderPagination();
  });
}

function renderPagination() {
  var pag = $("#keys-pag");
  if (!pag) return;
  pag.innerHTML = "";

  if (state.offset > 0) {
    pag.appendChild(
      el("button", {
        textContent: "Prev",
        onClick: function () {
          state.offset = Math.max(0, state.offset - 40);
          loadKeys();
        },
      }),
    );
  }

  pag.appendChild(
    el("span", {
      className: "page-info",
      textContent: "offset " + state.offset + (state.hasMore ? " (more…)" : ""),
    }),
  );

  if (state.hasMore) {
    pag.appendChild(
      el("button", {
        textContent: "Next",
        onClick: function () {
          state.offset += 40;
          loadKeys();
        },
      }),
    );
  }
}

function deleteKey(key) {
  if (!confirm("Delete key: " + key + "?")) return;
  api(
    "DELETE",
    "/api/" + encodeURIComponent(state.ns) + "/keys/" + encodeURIComponent(key),
  )
    .then(function () {
      toast("Deleted " + key, true);
      loadKeys();
    })
    .catch(function (e) {
      toast("Delete: " + e.message);
    });
}

// ---------------------------------------------------------------------------
// Revision browser dialog
// ---------------------------------------------------------------------------
function openRevDialog(key, revCount) {
  var old = $("#rev-dialog");
  if (old) old.remove();

  var dlg = el("dialog", { id: "rev-dialog" });
  dlg.appendChild(el("h3", { textContent: "Revisions: " + key }));
  dlg.appendChild(
    el("div", {
      className: "rev-info",
      textContent: revCount + " revision(s)",
    }),
  );

  var revList = el("div", { className: "rev-list" });
  dlg.appendChild(revList);

  var actions = el("div", { className: "dialog-actions" }, [
    el("button", {
      textContent: "Close",
      onClick: function () {
        dlg.close();
        dlg.remove();
      },
    }),
  ]);
  dlg.appendChild(actions);

  document.body.appendChild(dlg);
  dlg.showModal();

  // Load all revisions
  var nsPath =
    "/api/" + encodeURIComponent(state.ns) + "/keys/" + encodeURIComponent(key);
  for (var i = 0; i < revCount; i++) {
    (function (idx) {
      api("GET", nsPath + "/revisions/" + idx)
        .then(function (r) {
          var isExpired =
            r.status === 410 &&
            r.headers &&
            r.headers.get("X-RKV-Expired") === "true";
          var isDeleted = r.status === 410 && !isExpired;
          var isRevNull =
            !isDeleted && !isExpired && (r.status === 204 || r.data === null);
          var ttlSecs =
            r.headers && r.headers.get("X-RKV-TTL")
              ? parseInt(r.headers.get("X-RKV-TTL"), 10)
              : null;
          var revBinary =
            !isDeleted &&
            !isExpired &&
            !isRevNull &&
            r.data != null &&
            isBinary(String(r.data));
          var valText;
          if (isDeleted) valText = "(deleted)";
          else if (isExpired) valText = "(expired)";
          else if (isRevNull) valText = "(null)";
          else if (revBinary) valText = "(binary)";
          else valText = String(r.data);
          if (!revBinary && valText.length > 120)
            valText = valText.slice(0, 117) + "...";

          var idxLabel = "#" + idx;
          var hint = null;
          if (revCount === 1) {
            hint = "only";
          } else if (idx === 0) {
            hint = "oldest";
          } else if (idx === revCount - 1) {
            hint = "latest";
          }

          var idxChildren = [document.createTextNode(idxLabel)];
          if (hint) {
            idxChildren.push(
              el("span", { className: "rev-hint", textContent: hint }),
            );
          }

          var valChildren = [];
          if (isDeleted) {
            valChildren.push(
              el("span", {
                className: "val-deleted",
                textContent: "(deleted)",
              }),
            );
          } else if (isExpired) {
            valChildren.push(
              el("span", {
                className: "val-deleted",
                textContent: "(expired)",
              }),
            );
          } else if (revBinary) {
            valChildren.push(
              el("span", { className: "val-binary", textContent: "(binary)" }),
            );
            valChildren.push(
              el("button", {
                className: "btn-download",
                textContent: "Download",
                onClick: function () {
                  downloadRevision(key, idx);
                },
              }),
            );
          } else {
            valChildren.push(document.createTextNode(valText));
          }

          // Show TTL badge if the revision has a TTL
          if (ttlSecs !== null && !isExpired && !isDeleted) {
            var ttlText =
              ttlSecs >= 86400
                ? Math.floor(ttlSecs / 86400) + "d"
                : ttlSecs >= 3600
                  ? Math.floor(ttlSecs / 3600) + "h"
                  : ttlSecs >= 60
                    ? Math.floor(ttlSecs / 60) + "m"
                    : ttlSecs + "s";
            valChildren.push(
              el("span", {
                className: "rev-ttl",
                textContent: "TTL " + ttlText,
              }),
            );
          }

          var row = el("div", { className: "rev-row" }, [
            el("span", { className: "rev-idx" }, idxChildren),
            el("span", { className: "rev-val" }, valChildren),
          ]);
          row.setAttribute("data-idx", idx);

          // Insert in order
          var inserted = false;
          var children = revList.children;
          for (var j = 0; j < children.length; j++) {
            if (parseInt(children[j].getAttribute("data-idx"), 10) > idx) {
              revList.insertBefore(row, children[j]);
              inserted = true;
              break;
            }
          }
          if (!inserted) revList.appendChild(row);
        })
        .catch(function () {
          var row = el("div", { className: "rev-row" }, [
            el("span", { className: "rev-idx", textContent: "#" + idx }),
            el("span", { className: "rev-val", textContent: "(error)" }),
          ]);
          revList.appendChild(row);
        });
    })(i);
  }

  if (revCount === 0) {
    revList.appendChild(
      el("div", { className: "empty", textContent: "No revisions" }),
    );
  }
}

// ---------------------------------------------------------------------------
// Create / Edit key dialog
// ---------------------------------------------------------------------------

/** Route TTL input to the right header: X-RKV-TTL for durations, Expires for HTTP dates. */
function setTtlHeader(headers, value) {
  if (/^\d+[smhd]?$/.test(value)) {
    headers["X-RKV-TTL"] = value;
  } else {
    headers["Expires"] = value;
  }
}

function openCreateDialog() {
  showKeyDialog(
    "Create Key",
    "",
    "",
    false,
    "",
    function (key, value, expires) {
      var opts = {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
      };
      if (expires) setTtlHeader(opts.headers, expires);
      opts.body = JSON.stringify(value);
      fetch(
        "/api/" +
          encodeURIComponent(state.ns) +
          "/keys/" +
          encodeURIComponent(key),
        opts,
      )
        .then(function (r) {
          if (!r.ok)
            return r.text().then(function (t) {
              throw new Error(r.status + " " + t);
            });
          toast("Created " + key, true);
          loadKeys();
        })
        .catch(function (e) {
          toast("Create: " + e.message);
        });
    },
  );
}

function openEditDialog(key, currentValue, isNull, currentExpires) {
  var valStr = currentValue != null ? String(currentValue) : "";
  showKeyDialog(
    "Edit Key",
    key,
    valStr,
    isNull,
    currentExpires || "",
    function (_k, value, expires) {
      var opts = {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
      };
      if (expires) setTtlHeader(opts.headers, expires);
      opts.body = JSON.stringify(value);
      fetch(
        "/api/" +
          encodeURIComponent(state.ns) +
          "/keys/" +
          encodeURIComponent(key),
        opts,
      )
        .then(function (r) {
          if (!r.ok)
            return r.text().then(function (t) {
              throw new Error(r.status + " " + t);
            });
          toast("Updated " + key, true);
          loadKeys();
        })
        .catch(function (e) {
          toast("Update: " + e.message);
        });
    },
  );
}

function showKeyDialog(title, keyVal, valueVal, isNull, expiresVal, onSubmit) {
  // Remove any existing dialog
  var old = $("#key-dialog");
  if (old) old.remove();

  var dlg = el("dialog", { id: "key-dialog" });
  dlg.appendChild(el("h3", { textContent: title }));

  dlg.appendChild(el("label", { textContent: "Key" }));
  var keyInput = el("input", { type: "text", value: keyVal });
  if (keyVal) keyInput.disabled = true; // edit mode
  dlg.appendChild(keyInput);

  dlg.appendChild(el("label", { textContent: "Value" }));
  var valInput = el("textarea", {
    textContent: valueVal,
    placeholder: "Enter value (leave empty for empty string)",
  });
  dlg.appendChild(valInput);

  // File upload state
  var fileContent = null;

  var fileInfo = el("div", { className: "file-info hidden" });
  dlg.appendChild(fileInfo);

  function clearFile() {
    fileContent = null;
    fileInfo.className = "file-info hidden";
    valInput.style.display = "";
    valInput.disabled = false;
  }

  function showFile(name, size) {
    fileContent = null; // will be set by caller after this
    valInput.style.display = "none";
    nullBox.checked = false;
    fileInfo.className = "file-info";
    fileInfo.innerHTML = "";
    fileInfo.appendChild(
      el("span", { className: "file-info-name", textContent: name }),
    );
    fileInfo.appendChild(
      el("span", {
        className: "file-info-size",
        textContent: formatBytes(size),
      }),
    );
    fileInfo.appendChild(
      el("button", {
        className: "btn-red file-info-clear",
        textContent: "Clear",
        onClick: function (e) {
          e.preventDefault();
          clearFile();
        },
      }),
    );
  }

  var fileInput = el("input", { type: "file", id: "file-upload" });
  fileInput.style.display = "none";
  fileInput.addEventListener("change", function () {
    var file = fileInput.files[0];
    if (!file) return;
    var reader = new FileReader();
    reader.onload = function () {
      showFile(file.name, file.size);
      fileContent = reader.result;
      toast("Loaded " + file.name, true);
    };
    reader.onerror = function () {
      toast("Failed to read file");
    };
    reader.readAsText(file);
  });
  dlg.appendChild(fileInput);

  var uploadRow = el("div", { className: "upload-row" }, [
    el("button", {
      className: "btn-upload",
      textContent: "Upload from file",
      onClick: function (e) {
        e.preventDefault();
        fileInput.click();
      },
    }),
  ]);
  dlg.appendChild(uploadRow);

  var nullBox = el("input", { type: "checkbox", id: "null-check" });
  if (isNull) nullBox.checked = true;
  var nullLabel = el("label", { className: "null-label" }, [
    nullBox,
    document.createTextNode(" Set as null"),
  ]);
  dlg.appendChild(nullLabel);

  nullBox.addEventListener("change", function () {
    if (nullBox.checked) {
      clearFile();
      valInput.disabled = true;
      valInput.value = "";
    } else {
      valInput.disabled = false;
    }
  });
  if (isNull) valInput.disabled = true;

  dlg.appendChild(
    el("label", { textContent: "TTL (e.g. 60s, 10m, 1h, 1d) or HTTP date" }),
  );
  var expInput = el("input", {
    type: "text",
    value: expiresVal,
    placeholder: "60s / 10m / 1h / 1d",
  });
  dlg.appendChild(expInput);

  var actions = el("div", { className: "dialog-actions" }, [
    el("button", {
      textContent: "Cancel",
      onClick: function () {
        dlg.close();
        dlg.remove();
      },
    }),
    el("button", {
      className: "btn-blue",
      textContent: "Save",
      onClick: function () {
        var k = keyInput.value.trim();
        if (!k) {
          toast("Key is required");
          return;
        }
        var v = nullBox.checked
          ? null
          : fileContent != null
            ? fileContent
            : valInput.value;
        dlg.close();
        dlg.remove();
        onSubmit(k, v, expInput.value.trim());
      },
    }),
  ]);
  dlg.appendChild(actions);

  document.body.appendChild(dlg);
  dlg.showModal();
}

// ---------------------------------------------------------------------------
// Admin view
// ---------------------------------------------------------------------------
function renderAdmin(app) {
  app.appendChild(el("h2", { textContent: "Admin" }));

  // Stats section
  var statsGrid = el("div", { className: "stat-grid", id: "stats-grid" });
  app.appendChild(statsGrid);

  // Action buttons
  var flushBtn = el("button", {
    className: "btn-yellow",
    textContent: "Flush",
    onClick: function () {
      adminAction("flush");
    },
  });
  var syncBtn = el("button", {
    className: "btn-yellow",
    textContent: "Sync",
    onClick: function () {
      adminAction("sync");
    },
  });
  var compactBtn = el("button", {
    className: "btn-yellow",
    textContent: "Compact",
    onClick: function () {
      adminAction("compact");
    },
  });
  var actions = el("div", { className: "toolbar" }, [
    flushBtn,
    syncBtn,
    compactBtn,
    el("button", { textContent: "Refresh", onClick: loadStats }),
  ]);
  app.appendChild(actions);

  // Config section
  app.appendChild(
    el("h2", { textContent: "Configuration", style: "margin-top:24px" }),
  );
  var cfgTable = el("table", { className: "config-table", id: "config-table" });
  app.appendChild(cfgTable);

  loadStats();
  loadConfig();
}

function loadStats() {
  api("GET", "/api/admin/stats")
    .then(function (r) {
      var grid = $("#stats-grid");
      if (!grid) return;
      grid.innerHTML = "";
      var s = r.data;
      var items = [
        ["Role", s.role || "standalone"],
        ["Total Keys", s.total_keys],
        ["Data Size", formatBytes(s.data_size_bytes)],
        ["Namespaces", s.namespace_count],
        ["Levels", s.level_count],
        ["SSTables", s.sstable_count],
        ["Write Buffer", formatBytes(s.write_buffer_bytes)],
        ["Pending Compactions", s.pending_compactions],
        ["Puts", s.op_puts],
        ["Gets", s.op_gets],
        ["Deletes", s.op_deletes],
        ["Cache Hits", s.cache_hits],
        ["Cache Misses", s.cache_misses],
        ["Peers", s.peer_count],
        ["Conflicts Resolved", s.conflicts_resolved],
        ["Uptime", s.uptime_secs + "s"],
      ];
      items.forEach(function (pair) {
        grid.appendChild(
          el("div", { className: "stat-card" }, [
            el("div", { className: "label", textContent: pair[0] }),
            el("div", { className: "value", textContent: String(pair[1]) }),
          ]),
        );
      });
    })
    .catch(function (e) {
      toast("Stats: " + e.message);
    });
}

function loadConfig() {
  api("GET", "/api/admin/config")
    .then(function (r) {
      var tbl = $("#config-table");
      if (!tbl) return;
      tbl.innerHTML = "";
      var c = r.data;
      Object.keys(c).forEach(function (k) {
        tbl.appendChild(
          el("tr", null, [
            el("td", { textContent: k }),
            el("td", { textContent: String(c[k]) }),
          ]),
        );
      });
    })
    .catch(function (e) {
      toast("Config: " + e.message);
    });
}

function adminAction(action) {
  api("POST", "/api/admin/" + action)
    .then(function () {
      toast(action + " ok", true);
      loadStats();
    })
    .catch(function (e) {
      toast(action + ": " + e.message);
    });
}

function formatBytes(b) {
  if (b == null || b < 0) return String(b);
  if (b === 0) return "0 B";
  var units = ["B", "KB", "MB", "GB", "TB"];
  var i = Math.floor(Math.log(b) / Math.log(1024));
  if (i >= units.length) i = units.length - 1;
  return (b / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1) + " " + units[i];
}

// ---------------------------------------------------------------------------
// Namespaces view
// ---------------------------------------------------------------------------
function renderNamespaces(app) {
  app.appendChild(el("h2", { textContent: "Namespaces" }));

  var newNsBtn = el("button", {
    className: "btn-blue",
    textContent: "+ New Namespace",
    onClick: openNsDialog,
  });
  if (state.role === "replica") newNsBtn.disabled = true;
  var toolbar = el("div", { className: "toolbar" }, [
    newNsBtn,
    el("button", {
      textContent: "Refresh",
      onClick: function () {
        loadNsList();
      },
    }),
  ]);
  app.appendChild(toolbar);

  var list = el("div", { className: "ns-list", id: "ns-list" });
  app.appendChild(list);

  loadNsList();
}

function loadNsList() {
  api("GET", "/api/namespaces")
    .then(function (r) {
      var list = $("#ns-list");
      if (!list) return;
      list.innerHTML = "";
      var ns = r.data || [];
      if (ns.length === 0) {
        list.appendChild(
          el("div", { className: "empty", textContent: "No namespaces" }),
        );
        return;
      }
      ns.forEach(function (name) {
        var dropBtn = el("button", {
          className: "btn-red",
          textContent: "Drop",
          onClick: function () {
            dropNs(name);
          },
        });
        if (state.role === "replica") dropBtn.disabled = true;
        list.appendChild(
          el("div", { className: "ns-item" }, [
            el("span", { className: "ns-name", textContent: name }),
            dropBtn,
          ]),
        );
      });
    })
    .catch(function (e) {
      toast("Namespaces: " + e.message);
    });
}

function openNsDialog() {
  var old = $("#ns-dialog");
  if (old) old.remove();

  var dlg = el("dialog", { id: "ns-dialog" });
  dlg.appendChild(el("h3", { textContent: "Create Namespace" }));

  dlg.appendChild(el("label", { textContent: "Name" }));
  var nameInput = el("input", { type: "text" });
  dlg.appendChild(nameInput);

  dlg.appendChild(el("label", { textContent: "Password (optional)" }));
  var pwInput = el("input", { type: "password" });
  dlg.appendChild(pwInput);

  var actions = el("div", { className: "dialog-actions" }, [
    el("button", {
      textContent: "Cancel",
      onClick: function () {
        dlg.close();
        dlg.remove();
      },
    }),
    el("button", {
      className: "btn-blue",
      textContent: "Create",
      onClick: function () {
        var name = nameInput.value.trim();
        if (!name) {
          toast("Name is required");
          return;
        }
        var body = { name: name };
        var pw = pwInput.value;
        if (pw) body.password = pw;
        dlg.close();
        dlg.remove();
        api("POST", "/api/namespaces", body)
          .then(function () {
            toast("Created " + name, true);
            loadNsList();
            loadNamespaces().then(populateNsSelect);
          })
          .catch(function (e) {
            toast("Create NS: " + e.message);
          });
      },
    }),
  ]);
  dlg.appendChild(actions);

  document.body.appendChild(dlg);
  dlg.showModal();
}

function dropNs(name) {
  if (!confirm("Drop namespace: " + name + "?")) return;
  api("DELETE", "/api/" + encodeURIComponent(name))
    .then(function () {
      toast("Dropped " + name, true);
      if (name === "_") {
        return api("POST", "/api/namespaces", { name: "_" }).then(function () {
          toast("Re-created default namespace _", true);
        });
      }
    })
    .then(function () {
      loadNsList();
      loadNamespaces().then(populateNsSelect);
    })
    .catch(function (e) {
      toast("Drop NS: " + e.message);
    });
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener("DOMContentLoaded", function () {
  // Fetch health to set role before rendering views
  fetch("/health")
    .then(function (r) {
      return r.json();
    })
    .then(function (h) {
      var role = h.role || "standalone";
      state.role = role;
      if (role !== "standalone") {
        var logo = $(".logo");
        if (logo) {
          var badge = el("span", {
            className: "role-badge role-" + role,
            textContent: role,
          });
          logo.appendChild(badge);
        }
      }
    })
    .catch(function () {})
    .then(function () {
      route();
    });
});
