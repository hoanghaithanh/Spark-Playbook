// Spark Playbook — topic-page shell view-state toggles (topic-shell
// redesign, Decision C). Deliberately tiny and scoped to pure UI-toggle
// state (tabs / drawer open-close / breadcrumb dropdown) -- cluster state
// and content stay entirely server-driven (D4, no client-state framework).
(function () {
  "use strict";

  function byId(id) {
    return document.getElementById(id);
  }

  // ---- Tabs (Concept / Notebook / Self-check) ---------------------------
  function initTabs() {
    var buttons = document.querySelectorAll(".tab-btn");
    buttons.forEach(function (btn) {
      btn.addEventListener("click", function () {
        var target = btn.getAttribute("data-tab");
        document.querySelectorAll(".tab-btn").forEach(function (b) {
          b.classList.toggle("active", b === btn);
        });
        document.querySelectorAll(".tab-panel").forEach(function (panel) {
          panel.classList.toggle("active", panel.getAttribute("data-tab-panel") === target);
        });
      });
    });
  }

  // ---- Cluster-config drawer --------------------------------------------
  function initDrawer() {
    var drawer = byId("cluster-drawer");
    var backdrop = byId("drawer-backdrop");
    var openBtn = byId("drawer-open");
    var closeBtn = byId("drawer-close");
    if (!drawer || !backdrop) return;

    function open() {
      drawer.classList.add("open");
      backdrop.hidden = false;
    }
    function close() {
      drawer.classList.remove("open");
      backdrop.hidden = true;
    }

    if (openBtn) openBtn.addEventListener("click", open);
    if (closeBtn) closeBtn.addEventListener("click", close);
    backdrop.addEventListener("click", close);
  }

  // ---- Cluster Monitor panel (US-SH4, Decision B) ------------------------
  // Mirrors the cluster-config drawer's open/close/backdrop pattern, plus
  // an HTMX-fetched body: opening injects the SSE-connect element into the
  // DOM (fetch populates #monitor-body, which includes it -> EventSource
  // opens); closing clears #monitor-body so that element -- and the
  // EventSource it opened -- leaves the DOM (server's `finally:
  // collector.unsubscribe()` fires on the resulting disconnect). Closing
  // must NOT just hide the panel with CSS, or the stream would keep
  // sampling with nobody watching (R-Dash-3).
  function initMonitorPanel() {
    var panel = byId("monitor-panel");
    var backdrop = byId("monitor-backdrop");
    var openBtn = byId("monitor-open");
    var closeBtn = byId("monitor-close");
    var body = byId("monitor-body");
    if (!panel || !backdrop || !body || typeof htmx === "undefined") return;

    function open() {
      htmx.ajax("GET", "/dashboard/panel", { target: "#monitor-body", swap: "innerHTML" });
      panel.classList.add("open");
      backdrop.hidden = false;
    }
    function close() {
      panel.classList.remove("open");
      backdrop.hidden = true;
      if (window.__monitorPanelCleanup) {
        window.__monitorPanelCleanup();
        window.__monitorPanelCleanup = null;
      }
      body.innerHTML = "";
    }

    if (openBtn) openBtn.addEventListener("click", open);
    if (closeBtn) closeBtn.addEventListener("click", close);
    backdrop.addEventListener("click", close);

    // US-SH4 bookmark preservation: /dashboard redirects here with
    // ?monitor=open (Decision B2) -- auto-open so a redirected visitor
    // lands with the panel already showing, no extra click needed.
    if (new URLSearchParams(window.location.search).get("monitor") === "open") {
      open();
    }
  }

  // ---- Breadcrumb topic switcher -----------------------------------------
  function initBreadcrumb() {
    var toggle = byId("breadcrumb-toggle");
    var menu = byId("breadcrumb-menu");
    if (!toggle || !menu) return;

    function open() {
      menu.hidden = false;
      toggle.setAttribute("aria-expanded", "true");
    }
    function close() {
      menu.hidden = true;
      toggle.setAttribute("aria-expanded", "false");
    }

    toggle.addEventListener("click", function (evt) {
      evt.stopPropagation();
      if (menu.hidden) {
        open();
      } else {
        close();
      }
    });

    document.addEventListener("click", function (evt) {
      if (!menu.hidden && !menu.contains(evt.target) && evt.target !== toggle) {
        close();
      }
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    initTabs();
    initDrawer();
    initMonitorPanel();
    initBreadcrumb();
  });
})();
