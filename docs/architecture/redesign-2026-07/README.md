# Redesign mockups — imported 2026-07-15

Source: Claude Design project **"Spark Playbook Topic Redesign"**
(`https://claude.ai/design/p/08d7dfe3-637e-435e-a18e-6fa9792e1f34`), project id
`08d7dfe3-637e-435e-a18e-6fa9792e1f34`, imported via the `claude_design` MCP tool
(`list_files`/`get_file`).

## What's here

- `shell-topic-page.dc.html` — the canonical topic-page shell (tabs, cluster drawer, embedded
  monitor panel, breadcrumb topic switcher). Every one of the 11 topic-specific mockups in the
  source project is a byte-identical copy of this component with different content baked in —
  see `topics-content-spec.md` for the extracted differences instead of 11 duplicate files.
- `topics-index.dc.html` — the redesigned topic-list landing page.
- `dashboard-panel.dc.html` — trimmed reference for the "Spark Dashboard" component, which the
  shell embeds as a slide-in panel via `<dc-import name="Spark Dashboard">`. Full markup omitted
  here because it substantially duplicates the already-built Phase 2.5 dashboard
  (`docs/architecture/realtime-monitoring-dashboard.md`, `docs/acceptance/phase-2-5.md`) — see
  the in-file note for what's actually new (placement/integration) vs. already-shipped.
- `topics-content-spec.md` — per-topic Concept/Notebook/Self-check content extracted from the 11
  topic mockups, ready for requirements-analyst to turn into `content/<topic>/concept.md` +
  `manifest.yaml` entries.

**Not persisted:** `support.js`, the design tool's generated component runtime (~65KB, not
design content — re-fetch via the `claude_design` MCP tool with
`method: get_file, projectId: 08d7dfe3-637e-435e-a18e-6fa9792e1f34, path: support.js` if needed
to understand the `.dc.html` component model itself, e.g. `sc-if`/`sc-for`/`dc-import` semantics).

## Scope decision (human, 2026-07-15)

Given the mockup's topic index doesn't match the current backlog (missing built topics
join-strategies/bucketing/aqe; adds 6 topics not yet in `docs/backlog.md`), the human chose to
**adopt the full new topic set** as in-scope backlog stories alongside the shell/dashboard
redesign, rather than treating this as a pure UI reskin of existing topics. See
`topics-content-spec.md`'s per-topic headers for which topics are net-new vs. already built.

## Every `.dc.html` file here is a design reference, not production code

These files use the Claude Design tool's own component syntax (`x-dc`, `sc-if`, `sc-for`,
`dc-import`, inline `oklch()` styling) — they are not meant to be dropped into `app/web/`
verbatim. The actual implementation target is FastAPI + Jinja2 + HTMX per PLAN.md's D4 decision;
translate the visual design and interaction model, not the markup mechanism.
