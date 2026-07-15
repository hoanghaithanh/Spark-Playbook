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
    initBreadcrumb();
  });
})();
