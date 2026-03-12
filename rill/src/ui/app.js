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
    opts.headers["Content-Type"] = "application/json";
    opts.body = typeof body === "string" ? body : JSON.stringify(body);
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
  messages: [],
  msgOffset: 0,
  msgLimit: 20,
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
  if (hash === "queues") renderQueues(app);
  else if (hash === "messages") renderMessages(app);
  else renderQueues(app);
}

window.addEventListener("hashchange", route);

// ---------------------------------------------------------------------------
// Queues view
// ---------------------------------------------------------------------------
function renderQueues(app) {
  app.appendChild(el("h2", { textContent: "Queues" }));

  var toolbar = el("div", { className: "toolbar" }, [
    el("button", {
      className: "btn btn-blue",
      textContent: "+ New Queue",
      onClick: openCreateQueueDialog,
    }),
    el("button", {
      className: "btn",
      textContent: "Refresh",
      onClick: function () {
        loadQueues();
      },
    }),
  ]);
  app.appendChild(toolbar);

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
      // Load lengths for each queue
      state.queues.forEach(function (name) {
        api("GET", "/queues/" + encodeURIComponent(name) + "/info")
          .then(function (r) {
            state.queueLengths[name] = r.data.length;
            renderQueueList();
            renderQueueStats();
          })
          .catch(function () {});
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
        el("button", {
          className: "btn btn-green btn-sm",
          textContent: "Browse",
          onClick: function (e) {
            e.stopPropagation();
            state.selectedQueue = name;
            state.msgOffset = 0;
            location.hash = "#messages";
          },
        }),
        el("button", {
          className: "btn btn-red btn-sm",
          textContent: "Delete",
          onClick: function (e) {
            e.stopPropagation();
            deleteQueue(name);
          },
        }),
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
// Messages view
// ---------------------------------------------------------------------------
function renderMessages(app) {
  if (!state.selectedQueue && state.queues.length > 0) {
    state.selectedQueue = state.queues[0];
  }

  app.appendChild(el("h2", { textContent: "Messages" }));

  // Queue selector toolbar
  var queueSelect = el("select", {
    id: "msg-queue-select",
    onChange: function () {
      state.selectedQueue = this.value;
      state.msgOffset = 0;
      loadMessages();
    },
  });

  var toolbar = el("div", { className: "toolbar" }, [
    queueSelect,
    el("button", {
      className: "btn btn-green",
      textContent: "Push",
      onClick: openPushDialog,
    }),
    el("button", {
      className: "btn btn-yellow",
      textContent: "Pop",
      onClick: popMessage,
    }),
    el("button", {
      className: "btn",
      textContent: "Refresh",
      onClick: function () {
        loadMessages();
      },
    }),
  ]);
  app.appendChild(toolbar);

  // Queue info bar
  app.appendChild(el("div", { className: "queue-info", id: "queue-info" }));

  // Messages table
  var table = el("table", { id: "msg-table" });
  table.appendChild(
    el("thead", null, [
      el("tr", null, [
        el("th", { textContent: "#" }),
        el("th", { textContent: "Message" }),
      ]),
    ]),
  );
  table.appendChild(el("tbody", { id: "msg-body" }));
  app.appendChild(table);

  // Pagination
  app.appendChild(el("div", { className: "pagination", id: "pagination" }));

  // Activity log
  app.appendChild(el("h2", { textContent: "Activity Log" }));
  var activityTable = el("table", { id: "activity-table" });
  activityTable.appendChild(
    el("thead", null, [
      el("tr", null, [
        el("th", { textContent: "Action" }),
        el("th", { textContent: "Queue" }),
        el("th", { textContent: "Message" }),
        el("th", { textContent: "Time" }),
      ]),
    ]),
  );
  activityTable.appendChild(el("tbody", { id: "activity-body" }));
  app.appendChild(activityTable);

  loadMessagesView();
}

function loadMessagesView() {
  api("GET", "/queues")
    .then(function (r) {
      state.queues = (r.data && r.data.queues) || [];
      var sel = $("#msg-queue-select");
      if (!sel) return;
      sel.innerHTML = "";
      if (state.queues.length === 0) {
        sel.appendChild(
          el("option", { textContent: "(no queues)", disabled: true }),
        );
        return;
      }
      state.queues.forEach(function (name) {
        var opt = el("option", { value: name, textContent: name });
        if (name === state.selectedQueue) opt.selected = true;
        sel.appendChild(opt);
      });
      if (!state.selectedQueue && state.queues.length > 0) {
        state.selectedQueue = state.queues[0];
      }
      loadMessages();
    })
    .catch(function (e) {
      toast("Load queues: " + e.message);
    });

  renderActivity();
}

function loadMessages() {
  if (!state.selectedQueue) return;

  var q = encodeURIComponent(state.selectedQueue);

  // Load queue info
  api("GET", "/queues/" + q + "/info")
    .then(function (r) {
      var info = $("#queue-info");
      if (!info) return;
      var len = r.data.length;
      state.queueLengths[state.selectedQueue] = len;
      info.innerHTML = "";
      info.appendChild(
        el("span", {
          className: "info-label",
          textContent:
            state.selectedQueue +
            " \u2014 " +
            len +
            " message" +
            (len !== 1 ? "s" : ""),
        }),
      );
    })
    .catch(function () {});

  // Load messages (peek)
  api(
    "GET",
    "/queues/" +
      q +
      "/messages?offset=" +
      state.msgOffset +
      "&limit=" +
      state.msgLimit,
  )
    .then(function (r) {
      state.messages = (r.data && r.data.messages) || [];
      renderMessageTable();
      renderPagination();
    })
    .catch(function (e) {
      toast("Load messages: " + e.message);
    });
}

function renderMessageTable() {
  var tbody = $("#msg-body");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (state.messages.length === 0) {
    tbody.appendChild(
      el("tr", null, [
        el("td", {
          colSpan: "2",
          className: "empty",
          textContent: "No messages in queue",
        }),
      ]),
    );
    return;
  }

  state.messages.forEach(function (msg, i) {
    var preview =
      msg.body.length > 120 ? msg.body.slice(0, 117) + "..." : msg.body;
    tbody.appendChild(
      el("tr", null, [
        el("td", {
          className: "row-num",
          textContent: String(state.msgOffset + i + 1),
        }),
        el("td", { className: "msg-preview", textContent: preview }),
      ]),
    );
  });
}

function renderPagination() {
  var pag = $("#pagination");
  if (!pag) return;
  pag.innerHTML = "";

  if (state.msgOffset > 0) {
    pag.appendChild(
      el("button", {
        className: "btn btn-sm",
        textContent: "\u2190 Prev",
        onClick: function () {
          state.msgOffset = Math.max(0, state.msgOffset - state.msgLimit);
          loadMessages();
        },
      }),
    );
  }

  pag.appendChild(
    el("span", {
      className: "page-info",
      textContent:
        "Showing " +
        (state.msgOffset + 1) +
        "-" +
        (state.msgOffset + state.messages.length),
    }),
  );

  if (state.messages.length === state.msgLimit) {
    pag.appendChild(
      el("button", {
        className: "btn btn-sm",
        textContent: "Next \u2192",
        onClick: function () {
          state.msgOffset += state.msgLimit;
          loadMessages();
        },
      }),
    );
  }
}

// ---------------------------------------------------------------------------
// Activity log
// ---------------------------------------------------------------------------
var activityLog = [];

function addActivity(action, queue, message) {
  activityLog.unshift({
    action: action,
    queue: queue,
    message: message,
    time: new Date().toLocaleTimeString(),
  });
  if (activityLog.length > 20) activityLog.pop();
  renderActivity();
}

function renderActivity() {
  var tbody = $("#activity-body");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (activityLog.length === 0) {
    tbody.appendChild(
      el("tr", null, [
        el("td", {
          colSpan: "4",
          className: "empty",
          textContent: "No recent activity",
        }),
      ]),
    );
    return;
  }

  activityLog.forEach(function (entry) {
    var preview =
      entry.message && entry.message.length > 60
        ? entry.message.slice(0, 57) + "..."
        : entry.message || "-";
    tbody.appendChild(
      el("tr", null, [
        el("td", null, [
          el("span", {
            className:
              "action-badge " +
              (entry.action === "push" ? "badge-green" : "badge-yellow"),
            textContent: entry.action,
          }),
        ]),
        el("td", { textContent: entry.queue }),
        el("td", { className: "msg-preview", textContent: preview }),
        el("td", { className: "dim", textContent: entry.time }),
      ]),
    );
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
            addActivity("push", state.selectedQueue, msg);
            loadMessages();
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
        addActivity("pop", state.selectedQueue, "(empty)");
      } else {
        toast("Popped message", true);
        addActivity("pop", state.selectedQueue, String(msg));
      }
      loadMessages();
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
// Init
// ---------------------------------------------------------------------------
document.addEventListener("DOMContentLoaded", function () {
  // Auth button in sidebar
  var authBtn = $("#auth-btn");
  if (authBtn) authBtn.addEventListener("click", showAuthDialog);

  route();
});
