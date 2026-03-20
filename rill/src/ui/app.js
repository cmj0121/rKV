/* Rill Web UI — vanilla SPA */
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
      } else if (k === "innerHTML") {
        node.innerHTML = attrs[k];
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
var authToken = localStorage.getItem("rill_token") || "";

function api(method, path, body) {
  var opts = { method: method, headers: {}, cache: "no-store" };
  if (authToken) {
    opts.headers["Authorization"] = "Bearer " + authToken;
  }
  if (body !== undefined) {
    if (typeof body === "string") {
      opts.headers["Content-Type"] = "text/plain";
      opts.body = body;
    } else {
      opts.headers["Content-Type"] = "application/json";
      opts.body = JSON.stringify(body);
    }
  }
  return fetch(path, opts).then(function (r) {
    if (!r.ok) {
      return r.text().then(function (t) {
        throw new Error(r.status + " " + (t || r.statusText));
      });
    }
    var ct = r.headers.get("content-type") || "";
    if (r.status === 204) {
      return { status: r.status, data: null };
    }
    if (ct.indexOf("json") !== -1) {
      return r.json().then(function (d) {
        return { status: r.status, data: d };
      });
    }
    return r.text().then(function (t) {
      return { status: r.status, data: t };
    });
  });
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
var state = {
  queues: [],
  queueLengths: {},
  selectedQueue: null,
  role: null, // "admin", "writer", "reader", "anonymous", or null (unknown)
};

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------
function route() {
  var hash = location.hash.replace("#", "") || "queues";
  var links = $$(".nav-link");
  for (var i = 0; i < links.length; i++) {
    links[i].classList.toggle(
      "active",
      links[i].getAttribute("data-route") === hash,
    );
  }
  var app = $("#app");
  app.innerHTML = "";
  renderQueues(app);
}

window.addEventListener("hashchange", route);

// ---------------------------------------------------------------------------
// Queues view
// ---------------------------------------------------------------------------
function renderToolbar() {
  var toolbar = $("#toolbar");
  if (!toolbar) return;
  toolbar.innerHTML = "";

  var newBtn = el("button", {
    className: "btn btn-blue",
    textContent: "+ New Queue",
    onClick: openCreateQueueDialog,
  });
  if (!canAdmin()) {
    newBtn.disabled = true;
    newBtn.className = "btn btn-disabled";
  }
  toolbar.appendChild(newBtn);

  if (canAdmin()) {
    var dedupBtn = el("button", {
      className: "btn dedup-toggle is-off",
      id: "dedup-btn",
      textContent: "DEDUP",
      title: "Loading\u2026",
      onClick: function () {
        var cur = dedupBtn.classList.contains("is-on");
        api("PUT", "/admin/dedup", { enabled: !cur })
          .then(function () {
            setDedupStyle(dedupBtn, !cur);
            showToast("Dedup " + (!cur ? "enabled" : "disabled"), "success");
          })
          .catch(function (e) {
            showToast("Dedup: " + e.message, "error");
          });
      },
    });
    toolbar.appendChild(dedupBtn);
    api("GET", "/admin/dedup").then(function (r) {
      setDedupStyle(dedupBtn, r.data.dedup);
    });
  }

  toolbar.appendChild(
    el("button", {
      className: "btn",
      textContent: "Refresh",
      onClick: function () {
        loadQueues();
      },
    }),
  );
}

function setDedupStyle(btn, isOn) {
  btn.className = "btn dedup-toggle " + (isOn ? "is-on" : "is-off");
  btn.title =
    "Dedup: " +
    (isOn ? "on \u2014 click to disable" : "off \u2014 click to enable");
}

function renderQueues(app) {
  app.appendChild(el("h2", { textContent: "Queues" }));

  var toolbar = el("div", { className: "toolbar", id: "toolbar" });
  app.appendChild(toolbar);
  renderToolbar();

  var statsGrid = el("div", { className: "stat-grid", id: "stats-grid" });
  app.appendChild(statsGrid);

  var list = el("div", { className: "queue-list", id: "queue-list" });
  app.appendChild(list);

  loadQueues();
}

function loadQueues() {
  api("GET", "/queues")
    .then(function (r) {
      state.queues = (r.data && r.data.queues) || [];
      renderQueueList();
      renderQueueStats();
      // Load lengths for each queue, batch render after all complete
      var pending = state.queues.length;
      if (pending === 0) return;
      state.queues.forEach(function (name) {
        api("GET", "/queues/" + encodeURIComponent(name) + "/info")
          .then(function (r) {
            state.queueLengths[name] = r.data.length;
          })
          .catch(function () {})
          .then(function () {
            pending--;
            if (pending === 0) {
              renderQueueList();
              renderQueueStats();
            }
          });
      });
    })
    .catch(function (e) {
      toast("Load queues: " + e.message);
    });
}

function renderQueueStats() {
  var grid = $("#stats-grid");
  if (!grid) return;
  grid.innerHTML = "";

  var totalMessages = 0;
  state.queues.forEach(function (name) {
    totalMessages += state.queueLengths[name] || 0;
  });

  grid.appendChild(
    el("div", { className: "stat-card" }, [
      el("div", { className: "label", textContent: "Queues" }),
      el("div", {
        className: "value",
        textContent: String(state.queues.length),
      }),
    ]),
  );
  grid.appendChild(
    el("div", { className: "stat-card" }, [
      el("div", { className: "label", textContent: "Total Messages" }),
      el("div", {
        className: "value",
        textContent: String(totalMessages),
      }),
    ]),
  );
}

function renderQueueList() {
  var list = $("#queue-list");
  if (!list) return;
  list.innerHTML = "";

  if (state.queues.length === 0) {
    list.appendChild(
      el("div", { className: "empty", textContent: "No queues yet" }),
    );
    return;
  }

  state.queues.forEach(function (name) {
    var len = state.queueLengths[name];
    var badge =
      len !== undefined
        ? el("span", { className: "queue-badge", textContent: len + " msgs" })
        : el("span", { className: "queue-badge dim", textContent: "..." });

    var item = el("div", { className: "queue-item" }, [
      el("div", { className: "queue-left" }, [
        el("span", { className: "queue-name", textContent: name }),
        badge,
      ]),
      el("div", { className: "actions" }, [
        (function () {
          var btn = el("button", {
            className: canWrite()
              ? "btn btn-green btn-sm"
              : "btn btn-disabled btn-sm",
            textContent: "Push",
            onClick: function (e) {
              e.stopPropagation();
              state.selectedQueue = name;
              openPushDialog();
            },
          });
          if (!canWrite()) btn.disabled = true;
          return btn;
        })(),
        el("button", {
          className: "btn btn-yellow btn-sm",
          textContent: "Pop",
          onClick: function (e) {
            e.stopPropagation();
            state.selectedQueue = name;
            popMessage();
          },
        }),
        (function () {
          var btn = el("button", {
            className: canAdmin()
              ? "btn btn-red btn-sm"
              : "btn btn-disabled btn-sm",
            textContent: "Delete",
            onClick: function (e) {
              e.stopPropagation();
              deleteQueue(name);
            },
          });
          if (!canAdmin()) btn.disabled = true;
          return btn;
        })(),
      ]),
    ]);
    list.appendChild(item);
  });
}

function openCreateQueueDialog() {
  var old = $("#queue-dialog");
  if (old) old.remove();

  var dlg = el("dialog", { id: "queue-dialog" });
  dlg.appendChild(el("h3", { textContent: "Create Queue" }));

  dlg.appendChild(el("label", { textContent: "Queue Name" }));
  var nameInput = el("input", { type: "text", placeholder: "my-queue" });
  dlg.appendChild(nameInput);

  var actions = el("div", { className: "dialog-actions" }, [
    el("button", {
      className: "btn",
      textContent: "Cancel",
      onClick: function () {
        dlg.close();
        dlg.remove();
      },
    }),
    el("button", {
      className: "btn btn-blue",
      textContent: "Create",
      onClick: function () {
        var name = nameInput.value.trim();
        if (!name) {
          toast("Queue name is required");
          return;
        }
        dlg.close();
        dlg.remove();
        api("POST", "/queues", { name: name })
          .then(function () {
            toast("Created queue: " + name, true);
            loadQueues();
          })
          .catch(function (e) {
            toast("Create queue: " + e.message);
          });
      },
    }),
  ]);
  dlg.appendChild(actions);

  document.body.appendChild(dlg);
  dlg.showModal();
  nameInput.focus();
}

function deleteQueue(name) {
  if (!confirm("Delete queue: " + name + "?")) return;
  api("DELETE", "/queues/" + encodeURIComponent(name))
    .then(function () {
      toast("Deleted queue: " + name, true);
      if (state.selectedQueue === name) state.selectedQueue = null;
      delete state.queueLengths[name];
      loadQueues();
    })
    .catch(function (e) {
      toast("Delete queue: " + e.message);
    });
}

// ---------------------------------------------------------------------------
// Push / Pop
// ---------------------------------------------------------------------------
function openPushDialog() {
  if (!state.selectedQueue) {
    toast("Select a queue first");
    return;
  }

  var old = $("#push-dialog");
  if (old) old.remove();

  var dlg = el("dialog", { id: "push-dialog" });
  dlg.appendChild(el("h3", { textContent: "Push to: " + state.selectedQueue }));

  dlg.appendChild(el("label", { textContent: "Message" }));
  var msgInput = el("textarea", { placeholder: "Enter message content..." });
  dlg.appendChild(msgInput);

  dlg.appendChild(
    el("label", { textContent: "TTL (optional, e.g. 30s, 5m, 1h, 2d)" }),
  );
  var ttlInput = el("input", {
    type: "text",
    placeholder: "e.g. 30s, 5m, 1h",
  });
  dlg.appendChild(ttlInput);

  var actions = el("div", { className: "dialog-actions" }, [
    el("button", {
      className: "btn",
      textContent: "Cancel",
      onClick: function () {
        dlg.close();
        dlg.remove();
      },
    }),
    el("button", {
      className: "btn btn-green",
      textContent: "Push",
      onClick: function () {
        var msg = msgInput.value;
        if (!msg) {
          toast("Message cannot be empty");
          return;
        }
        dlg.close();
        dlg.remove();
        var url = "/queues/" + encodeURIComponent(state.selectedQueue);
        var ttl = ttlInput.value.trim();
        if (ttl) {
          url += "?ttl=" + encodeURIComponent(ttl);
        }
        api("POST", url, msg)
          .then(function () {
            toast("Pushed message", true);
            loadQueues();
          })
          .catch(function (e) {
            toast("Push: " + e.message);
          });
      },
    }),
  ]);
  dlg.appendChild(actions);

  document.body.appendChild(dlg);
  dlg.showModal();
  msgInput.focus();
}

function popMessage() {
  if (!state.selectedQueue) {
    toast("Select a queue first");
    return;
  }

  api("GET", "/queues/" + encodeURIComponent(state.selectedQueue))
    .then(function (r) {
      var msg = r.data && r.data.message;
      if (msg === null || msg === undefined) {
        toast("Queue is empty");
      } else {
        toast("Popped: " + String(msg).slice(0, 60), true);
      }
      loadQueues();
    })
    .catch(function (e) {
      toast("Pop: " + e.message);
    });
}

// ---------------------------------------------------------------------------
// Auth token dialog
// ---------------------------------------------------------------------------
function showAuthDialog() {
  var old = $("#auth-dialog");
  if (old) old.remove();

  var dlg = el("dialog", { id: "auth-dialog" });
  dlg.appendChild(el("h3", { textContent: "Authentication" }));
  dlg.appendChild(
    el("label", { textContent: "Bearer Token (leave empty for open mode)" }),
  );
  var tokenInput = el("input", {
    type: "password",
    placeholder: "Enter token...",
    value: authToken,
  });
  dlg.appendChild(tokenInput);

  var actions = el("div", { className: "dialog-actions" }, [
    el("button", {
      className: "btn",
      textContent: "Clear",
      onClick: function () {
        authToken = "";
        localStorage.removeItem("rill_token");
        dlg.close();
        dlg.remove();
        toast("Token cleared", true);
        loadRole();
        route();
      },
    }),
    el("button", {
      className: "btn btn-blue",
      textContent: "Save",
      onClick: function () {
        authToken = tokenInput.value.trim();
        if (authToken) {
          localStorage.setItem("rill_token", authToken);
        } else {
          localStorage.removeItem("rill_token");
        }
        dlg.close();
        dlg.remove();
        toast("Token saved", true);
        loadRole();
        route();
      },
    }),
  ]);
  dlg.appendChild(actions);

  document.body.appendChild(dlg);
  dlg.showModal();
  tokenInput.focus();
}

// ---------------------------------------------------------------------------
// Role display
// ---------------------------------------------------------------------------
function canWrite() {
  return state.role === "admin" || state.role === "writer";
}

function canAdmin() {
  return state.role === "admin";
}

function loadRole() {
  api("GET", "/auth/me")
    .then(function (r) {
      var badge = $("#role-badge");
      if (!badge) return;
      var role = r.data.role;
      badge.textContent = role;
      badge.className = "role-badge role-" + role;
      var prev = state.role;
      state.role = role;
      if (prev !== role) {
        renderQueueList();
        renderToolbar();
      }
    })
    .catch(function () {});
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener("DOMContentLoaded", function () {
  // Auth button in sidebar
  var authBtn = $("#auth-btn");
  if (authBtn) authBtn.addEventListener("click", showAuthDialog);

  loadRole();
  route();
});
