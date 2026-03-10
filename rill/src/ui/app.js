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
  selectedQueue: null,
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
      className: "btn-blue",
      textContent: "+ New Queue",
      onClick: openCreateQueueDialog,
    }),
    el("button", {
      textContent: "Refresh",
      onClick: function () {
        loadQueues();
      },
    }),
  ]);
  app.appendChild(toolbar);

  // Stats
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
    })
    .catch(function (e) {
      toast("Load queues: " + e.message);
    });
}

function renderQueueStats() {
  var grid = $("#stats-grid");
  if (!grid) return;
  grid.innerHTML = "";
  grid.appendChild(
    el("div", { className: "stat-card" }, [
      el("div", { className: "label", textContent: "Total Queues" }),
      el("div", {
        className: "value",
        textContent: String(state.queues.length),
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
    var item = el("div", { className: "queue-item" }, [
      el("span", { className: "queue-name", textContent: name }),
      el("div", { className: "actions" }, [
        el("button", {
          className: "btn-green",
          textContent: "Open",
          onClick: function (e) {
            e.stopPropagation();
            state.selectedQueue = name;
            location.hash = "#messages";
          },
        }),
        el("button", {
          className: "btn-red",
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
}

function deleteQueue(name) {
  if (!confirm("Delete queue: " + name + "?")) return;
  api("DELETE", "/queues/" + encodeURIComponent(name))
    .then(function () {
      toast("Deleted queue: " + name, true);
      if (state.selectedQueue === name) state.selectedQueue = null;
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
  app.appendChild(el("h2", { textContent: "Messages" }));

  // Queue selector
  var toolbar = el("div", { className: "toolbar" });

  var queueSelect = el("select", {
    id: "msg-queue-select",
    onChange: function () {
      state.selectedQueue = this.value;
    },
  });
  toolbar.appendChild(queueSelect);

  toolbar.appendChild(
    el("button", {
      className: "btn-green",
      textContent: "Push Message",
      onClick: openPushDialog,
    }),
  );

  toolbar.appendChild(
    el("button", {
      className: "btn-yellow",
      textContent: "Pop Message",
      onClick: popMessage,
    }),
  );

  toolbar.appendChild(
    el("button", {
      textContent: "Refresh",
      onClick: function () {
        loadMessagesView();
      },
    }),
  );

  app.appendChild(toolbar);

  // Recent activity
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
        el("td", { textContent: entry.action }),
        el("td", { textContent: entry.queue }),
        el("td", { className: "msg-preview", textContent: preview }),
        el("td", { textContent: entry.time }),
      ]),
    );
  });
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
    })
    .catch(function (e) {
      toast("Load queues: " + e.message);
    });

  renderActivity();
}

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

  var actions = el("div", { className: "dialog-actions" }, [
    el("button", {
      textContent: "Cancel",
      onClick: function () {
        dlg.close();
        dlg.remove();
      },
    }),
    el("button", {
      className: "btn-green",
      textContent: "Push",
      onClick: function () {
        var msg = msgInput.value;
        dlg.close();
        dlg.remove();
        api("POST", "/queues/" + encodeURIComponent(state.selectedQueue), msg)
          .then(function () {
            toast("Pushed message", true);
            addActivity("push", state.selectedQueue, msg);
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
    })
    .catch(function (e) {
      toast("Pop: " + e.message);
    });
}

// ---------------------------------------------------------------------------
// Auth token dialog
// ---------------------------------------------------------------------------
function checkAuth() {
  if (authToken) return;
  var token = prompt("Enter auth token (leave empty for open mode):");
  if (token) {
    authToken = token;
    localStorage.setItem("rill_token", token);
  }
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener("DOMContentLoaded", function () {
  checkAuth();
  route();
});
