# Topic-Page Shell Redesign + Cluster Monitor Panel Integration — Requirements

Status: Draft for architect handoff
Owner: requirements-analyst
Date: 2026-07-15 (updated 2026-07-15 — four open questions resolved into settled decisions; see
inline "settled 2026-07-15" markers throughout)

## Source

Derived from the imported Claude Design mockups under
`docs/architecture/redesign-2026-07/` — `shell-topic-page.dc.html` (topic-page shell),
`topics-index.dc.html` (landing page), and `dashboard-panel.dc.html` (Cluster Monitor panel
placement) — adopted per the human's 2026-07-15 scope decision recorded in that directory's
`README.md`. These `.dc.html` files are design references only; this document is the
translation of their visual/interaction design into testable requirements against the actual
FastAPI + Jinja2 + HTMX stack (PLAN.md D4).

## Problem statement

The app's topic pages, cluster controls, and the Phase 2.5 monitoring dashboard were each built
independently as their own phases shipped (Phase 1 topic page + cluster panel, Phase 2.5's
standalone `/dashboard` route). There is no shared page shell, so every new curriculum topic
today means building another one-off page, and the cluster config controls and the monitor
dashboard live on separate surfaces the learner has to navigate between. As the curriculum grows
from 4 built topics (5 once Catalyst plans ships as its own topic — settled 2026-07-15, see
US-SH8) toward the 9 additional topics scoped in
`docs/requirements/curriculum-topics-2026-07.md`, that per-topic-page pattern does not scale and
the navigation model degrades further. This redesign introduces one shared topic-page shell
(Concept / Notebook / Self-check tabs, a slide-out cluster-config drawer, a breadcrumb topic
switcher, and the Cluster Monitor embedded as the sole in-page slide-in panel entry point) driven
entirely by existing `content/<topic>/manifest.yaml` + `concept.md` data, plus a data-driven
topics-index landing page, so every topic — existing and new — renders through one component
instead of per-topic markup.

## Relationship to already-shipped work (read before treating this as greenfield)

- **This changes an already-accepted design decision — now settled by the human.** Phase 2.5's
  `docs/architecture/realtime-monitoring-dashboard.md` (ADR D-E) and
  `docs/requirements/realtime-monitoring-dashboard.md` (US-5.6) resolved dashboard placement as a
  **standalone `/dashboard` route**, and `docs/acceptance/phase-2-5.md` records that placement as
  **PASS, verified** (currently in draft, awaiting human sign-off). **Settled 2026-07-15:** the
  human has decided the Cluster Monitor slide-in panel replaces the standalone route entirely —
  `/dashboard` is retired as an independently reachable route, not kept as a second entry point
  alongside the panel. This is a real change to validated, already-shipped behavior (ADR D-E,
  US-5.6), made by explicit human decision rather than left open. What is **not** yet settled is
  the migration mechanics — the SSE stream's lifecycle as the panel opens/closes/reopens, and
  what happens to any existing direct link/bookmark to the old `/dashboard` URL (redirect vs. 404
  vs. something else) — those remain an architect-level open question (see Open Questions).
- **The underlying cluster lifecycle is unchanged.** The cluster-config drawer relocates the
  existing Phase 1 cluster control panel's inputs (worker count, cores, memory, shuffle
  partitions, AQE toggle) and its spawn/teardown actions into a new UI location. It does not
  change the state machine, the cancel-and-replace behavior (D5), or the resource-ceiling value
  (32GB, PLAN.md §2) — those are locked inputs to this doc, not renegotiated by it. The parameter
  *ranges* exposed by the drawer's UI controls are addressed in US-SH2 below (settled 2026-07-15).
- **"Existing built topics" means the 4 topics actually in `content/` today** —
  `partitioning-shuffle`, `join-strategies`, `bucketing`, `aqe`. The mockup's assumed "Catalyst
  plans" topic page does not exist yet as a standalone `content/` folder, despite backlog item #4
  ("Curriculum topic: Catalyst plans & `.explain`") being marked "Done (Sprint 1)" — **settled
  2026-07-15:** this is real, scoped implementation work, not just a status correction. A
  dedicated `content/catalyst-plans/` topic (following the existing naming convention used by
  `partitioning-shuffle` / `join-strategies`) will be built using the new shell (same
  Concept/Notebook/Self-check pattern as every other topic) and the content in
  `docs/architecture/redesign-2026-07/topics-content-spec.md`'s "02 — Spark SQL Catalyst" section.
  This becomes the 5th topic built onto the shell, not a discrepancy to reconcile before building
  — see backlog #4 and #31 (new), and US-SH8 below. **One clarification found while folding in
  this decision:** `content/join-strategies/concept.md` and its notebook reference "Catalyst
  optimizer" only as passing vocabulary (e.g. "When Spark's Catalyst optimizer plans a join...")
  — they do **not** contain the parse→analyze→optimize→physical-plan phase breakdown, the
  DataFrame/SQL/UDF predicate-pushdown comparison, or the three-cell notebook walkthrough that
  `topics-content-spec.md`'s Catalyst section describes. So this is not an extraction/split of
  existing substantive content out of `join-strategies` — it is new content built from the
  mockup spec, and it requires **no change** to `join-strategies`' own concept text, notebook, or
  manifest. See US-SH8's last acceptance criterion for the explicit no-change confirmation.

## Goals / Non-goals

### Goals

- **G-SH1 — One shared shell component, not per-topic markup.** Every topic page (existing and
  future) renders from the same shell, driven by that topic's `manifest.yaml` + `concept.md`,
  matching the existing "content is data" pattern (PLAN.md §3/§4, G7) rather than one hand-built
  template per topic.
- **G-SH2 — Concept / Notebook / Self-check tab structure**, replacing the current single-scroll
  topic page (US-1.1) with three explicit tabs matching the mockup's information architecture.
- **G-SH3 — Cluster-config drawer** consolidates the existing cluster control panel (US-1.2) into
  a slide-out drawer reachable from any topic page, rather than a separate page/panel.
- **G-SH4 — Breadcrumb topic switcher** lets the learner jump directly between topics without
  returning to the topics-index page first.
- **G-SH5 — Cluster Monitor accessible in-page, panel-only.** The Phase 2.5 dashboard (already
  built) becomes reachable as a slide-in panel from within any topic page, and the standalone
  `/dashboard` route is retired — the panel is the only entry point (**settled 2026-07-15**,
  addressing backlog item #13's placement/integration question; supersedes Phase 2.5 ADR D-E's
  standalone-route decision, see the framing note above).
- **G-SH6 — Topics-index page is data-driven.** The landing page enumerates topics from
  `content/*/manifest.yaml` (order, title, blurb/summary), not a hardcoded array — correcting the
  mockup's own hardcoded, backlog-mismatched topic list (flagged in its own implementer note).
- **G-SH7 — Self-check tab surfaces real annotation output.** The "Reveal self-check" flow in the
  shell must call the real Phase 2 annotation engine reading the learner's own
  `playbook.checkpoint()` dump (G3/G7, PLAN.md §3) — the mockup's hardcoded "actual annotation
  output" placeholder text must not ship as real behavior anywhere.
- **G-SH8 — Notebook tab shows the real embedded JupyterLab iframe** once a cluster is ready
  (existing US-1.3 behavior), not the mockup's static fake code-cell preview.
- **G-SH9 — Catalyst plans becomes a real, standalone topic.** Backlog #4 gets an actual
  `content/catalyst-plans/` folder and topic page built through the shared shell, replacing its
  current state as background material folded into `join-strategies`' concept page (**settled
  2026-07-15**; see US-SH8).

### Non-goals

- **No change to the cluster lifecycle state machine, cancel-and-replace behavior, or the 32GB
  resource ceiling** (D5, PLAN.md §2) — this doc only relocates existing controls into a drawer
  and sets the drawer's UI-level parameter ranges (US-SH2).
- **No change to the annotation engine's matching/mapping logic itself.** New plan-node/failure
  shapes some curriculum topics may need are addressed in
  `docs/requirements/curriculum-topics-2026-07.md` and its own open question, not here.
- **No redesign of the raw Spark UI** (`:8080`/`:4040`) — existing deep-link behavior (US-2.2,
  US-5.6) is unchanged.
- **No mobile/responsive support** — unchanged from the MVP doc's existing non-goal.
- **No custom code editor** — the Notebook tab still embeds JupyterLab via iframe (locked
  decision, MVP doc Non-goals), never a Monaco-based editor.

## User stories and acceptance criteria

**US-SH1 — Unified topic-page shell across all topics.**
As a learner, I want every topic page to look and behave the same way (same tab structure, same
drawer, same switcher), so that learning the navigation once lets me use it on any topic without
relearning a new page layout each time.

- *Given* any topic in `content/`, *when* I open its page, *then* it renders via the one shared
  shell component with Concept / Notebook / Self-check tabs, sourced from that topic's
  `manifest.yaml` + `concept.md` — no topic has bespoke page markup.
- *Given* the shell is built, *when* a new topic folder is added under `content/` with a valid
  manifest and concept file, *then* its page is reachable and correctly rendered without any
  shell code change (content-as-data, matching US-1.1's existing acceptance criterion). This is
  the same mechanism `content/catalyst-plans/` relies on once US-SH8 builds it — no shell
  special-casing for Catalyst.
- *Given* the existing 4 built topics, *when* the shell ships, *then* all 4 are migrated to it and
  their existing content (concept text, notebook links, annotation manifests) continues to work
  unchanged — this is a rendering-layer migration, not a content rewrite.

**US-SH2 — Cluster-config drawer.**
As a learner, I want to open a slide-out drawer from any topic page to configure and spawn/tear
down a cluster, so I don't have to leave the topic I'm working on to manage the cluster.

- *Given* a topic page, *when* I open the cluster-config drawer, *then* I can set worker count,
  cores per worker, memory per worker, `spark.sql.shuffle.partitions`, and AQE on/off, and trigger
  spawn or teardown — functionally equivalent to the existing Phase 1 cluster control panel
  (US-1.2), just relocated.
- *Given* a spawn or teardown in progress, *when* I view the drawer, *then* it reflects the same
  state-machine states already defined (idle / spawning / ready / error, D5) rather than
  introducing a new lifecycle representation.
- *Given* the drawer's parameter ranges, *when* implemented, *then* they are exactly: worker count
  1–5, cores per worker 1–4, memory per worker **1–8 GB**, and `spark.sql.shuffle.partitions`
  **1–300**. **Settled 2026-07-15:** memory keeps the existing locked US-1.2 range (confirming
  this doc's earlier default, and explicitly **not** the mockup's wider 1–16GB slider);
  shuffle-partitions gets an explicit UI-bound range of **1–300** — this both replaces US-1.2's
  original unbounded "any positive integer" language and narrows the mockup's 8–400/step-8
  slider. Both ranges are now locked inputs to implementation, not TBD.
- *Given* the resource-ceiling check (32GB, PLAN.md §2), *when* a drawer configuration would
  exceed it, *then* spawning is rejected with a clear message before any container starts —
  unchanged existing behavior, now surfaced through the drawer's UI instead of the old panel's.

**US-SH3 — Breadcrumb topic switcher.**
As a learner, I want to jump directly from the topic I'm on to any other topic via a breadcrumb
dropdown, so I can move between related topics (e.g., AQE to Skew & Salting) without detouring
through the topics-index page.

- *Given* any topic page, *when* I click the breadcrumb topic label, *then* a dropdown lists all
  available topics (sourced from `content/*/manifest.yaml`, matching G-SH6), with the current
  topic visually distinguished.
- *Given* the dropdown is open, *when* I select a different topic, *then* I navigate to that
  topic's page; the currently spawned cluster (if any) is unaffected by this navigation alone —
  switching topics must not implicitly tear down or respawn a cluster as a side effect.

**US-SH4 — Cluster Monitor slide-in panel (sole entry point).**
As a learner, I want to open the Cluster Monitor dashboard as a panel from within any topic page,
so I can watch live cluster/job diagnostics while reading a topic's concept or running its
notebook, without switching browser tabs or losing my place.

- *Given* a topic page with a running cluster, *when* I open the Cluster Monitor panel, *then* it
  renders the same already-built Phase 2.5 dashboard content (overview node grid, job-detail stage
  timeline + partition table, node-detail sparklines — `docs/architecture/realtime-monitoring-dashboard.md`)
  inside the slide-in panel, with live updates continuing to meet the existing 5-second latency
  target (US-5.5) while the panel is open.
- *Given* the redesign ships, *when* a learner or any existing bookmark/link visits `/dashboard`,
  *then* that standalone route **no longer exists** — **settled 2026-07-15**, superseding Phase
  2.5 ADR D-E's standalone-route decision. The Cluster Monitor panel is the only way to reach the
  dashboard content; this is a route removal, not a "supplemented with a panel" addition. Exact
  migration mechanics — what specifically happens to a request that hits the old `/dashboard`
  URL, and how the SSE stream lifecycle behaves across panel open/close/reopen — are **not**
  settled by this doc; see Open Question 1.
- *Given* the panel is closed and reopened repeatedly during a single session, *when* observed,
  *then* it does not leak or duplicate the underlying SSE connection/collector each time — closing
  the panel must cleanly stop or detach from updates, and reopening must resume live data without
  requiring a full page reload. (Exact mechanism — pause vs. disconnect vs. reuse a single
  page-level stream — is left to the architect; see Open Question 1.)
- *Given* no cluster is running, *when* I open the panel from a topic page, *then* it shows the
  existing "no active cluster" state (US-5.6), not an error or blank panel.
- *Given* the panel surfaces bottleneck/skew signal, *when* rendered, *then* it preserves the
  already-decided G3 constraint that the dashboard shows signal, not conclusions — the mockup's
  "Suggestion:" lines on bottleneck cards must **not** be reintroduced (this was already resolved
  once, per ADR D-A, and the existing implementation already strips them; this criterion exists
  only to guard against regressing it during the panel-embedding work).

**US-SH5 — Topics-index landing page, data-driven.**
As a learner, I want the topics list I land on to reflect the actual set of topics the app has
today, so I never see a topic advertised that doesn't exist or miss one that does.

- *Given* the topics-index page, *when* it renders, *then* its topic cards are generated from
  `content/*/manifest.yaml` (id, title, order, a short blurb sourced from `concept.md` or the
  manifest), not a hardcoded list.
- *Given* a topic folder is added, removed, or reordered under `content/`, *when* the index page
  is next loaded, *then* it reflects that change with no code change to the index page itself.
  This includes `content/catalyst-plans/` once US-SH8 builds it — no special-casing.
- *Given* the full topic set once curriculum-topics-2026-07.md's stories are built, *when* the
  index renders, *then* it lists every actually-built topic and no others — resolving the
  mismatch the imported mockup's hardcoded list had against the real backlog.

**US-SH6 — Self-check tab sources real annotation output.**
As a learner, I want the "Reveal self-check" action on any topic's Self-check tab to show my own
plan/metric evidence, not placeholder text, so the self-check is trustworthy on every topic, not
just the ones built carefully by hand.

- *Given* a topic page's Self-check tab, *when* I write a hypothesis and click "Reveal," *then*
  the panel calls the real annotation engine (`app/annotation/engine.py`) against my most recent
  `playbook.checkpoint()` dump for that topic, per the existing pull-not-push flow (G3, PLAN.md
  §3) — never hardcoded or templated "expected" text.
- *Given* no `playbook.checkpoint()` call has been made yet for the current topic/session, *when*
  I click "Reveal," *then* the panel shows a clear "no checkpoint recorded yet" state rather than
  fabricated output.
- *Given* this is a UI-layer requirement, *when* implemented, *then* it does not require or imply
  any change to the annotation engine's own matching logic — new topics whose plan/failure shapes
  the engine cannot yet label are addressed in `docs/requirements/curriculum-topics-2026-07.md`'s
  open question, not solved here.

**US-SH7 — Notebook tab shows the real embedded Jupyter iframe.**
As a learner, I want the Notebook tab to show my actual running notebook once a cluster is up, not
a static preview, so I can read the walkthrough steps and immediately act on them in the same
tab.

- *Given* a spawned, ready cluster, *when* I open a topic's Notebook tab, *then* it shows the real
  embedded JupyterLab iframe pointed at that stack's driver (existing US-1.3 behavior), alongside
  the walkthrough-steps list from the topic's content.
- *Given* no cluster is running, *when* I open the Notebook tab, *then* it shows the walkthrough
  steps plus the existing "no cluster running" / spawn prompt (matching the shell's idle-state
  design), not a broken or blank iframe.

**US-SH8 — Catalyst plans: dedicated topic page and content build.**
As a learner, I want the "Spark SQL Catalyst" backlog item (#4) to be a real, standalone topic
page like every other topic, not background material folded into another topic's concept page, so
I can find and study it directly and it renders through the same shell as everything else.

- *Given* the settled 2026-07-15 decision, *when* this story is implemented, *then* a new
  `content/catalyst-plans/` folder is created with `concept.md`, `notebook.ipynb`, and
  `manifest.yaml`, following the same pattern as the other 4 built topics (PLAN.md §3/§4).
- *Given* `topics-content-spec.md`'s "02 — Spark SQL Catalyst" section, *when* `concept.md` is
  written, *then* it covers the parse→analyze→optimize→physical-plan phase breakdown and the fact
  that DataFrame and SQL queries compile to the same plan.
- *Given* the notebook walkthrough, *when* built, *then* it includes the three-cell comparison
  described in the spec: a DataFrame filter-after-join, the same query as raw SQL, and the same
  query wrapped in a UDF — demonstrating that the UDF version's filter does **not** get pushed
  below the join (because Catalyst cannot see inside Python bytecode) while the DataFrame and SQL
  versions do.
- *Given* the Self-check tab, *when* the learner hypothesizes whether the UDF-wrapped filter
  pushes below the join and clicks Reveal, *then* the annotation engine's plan-node output (a
  manifest entry distinguishing pushed-down vs. not-pushed-down filter placement across the three
  variants) surfaces the evidence — this maps onto the engine's existing plan-node matching model
  (G7), no new engine capability needed (contrast with the genuine gaps flagged in
  `curriculum-topics-2026-07.md`'s Open Question 1).
- *Given* this new topic exists, *when* the topics-index page (US-SH5) and breadcrumb switcher
  (US-SH3) are built, *then* Catalyst plans appears in both like any other topic, sourced from its
  `manifest.yaml` — no special-casing.
- *Given* `content/join-strategies/concept.md` and its notebook, *when* this story is
  implemented, *then* they are left **unchanged** — confirmed while writing this update that they
  reference "Catalyst optimizer" only as passing vocabulary, not the phase-by-phase content this
  story is sourced from, so there is no content to extract out of them and no gap left behind in
  the join-strategies topic (see the "Relationship to already-shipped work" note above).

## Open questions

1. **SSE panel migration mechanics. RESOLVED 2026-07-15** (architect, approved same day — see
   `docs/architecture/topic-shell-redesign.md` Decision B). The product decision (panel-only, no
   standalone route) was already settled; the mechanics are now settled too:
   - Fresh `EventSource` connection per panel-open, torn down on close via the existing
     subscriber-count gate in `app/monitoring/collector.py` — no hidden always-on page-level
     stream (rejected: it would sample with nobody watching).
   - `GET /dashboard` becomes a 307 redirect into the topic shell with the panel auto-opened
     (`?monitor=open`), not a 404 — preserves bookmarks/links. `GET /dashboard/stream` is kept
     unchanged as the panel's stream endpoint.

   Implementation of this specific slice is tracked as GitHub issue #23 (blocked on
   `docs/acceptance/phase-2-5.md` human sign-off — see that issue for the dependency).

## Constraints

- Builds on top of, and must not break, the existing cluster lifecycle (D5), the existing
  `spark_api/` REST clients, and the already-built Phase 2.5 dashboard's data collection (D-B
  through D-D) — this is a UI/placement redesign, not a rebuild of any of those.
- Same platform constraints as the rest of the project: Windows/WSL2 or Linux, Docker + Docker
  Compose, `localhost`-only, no auth, single user (MVP doc constraints, unchanged).
- **Dependency on Phase 2.5 sign-off.** `docs/acceptance/phase-2-5.md` is currently in draft,
  awaiting human sign-off, at the time this doc was written. This redesign's Cluster Monitor panel
  work (US-SH4) builds directly on that dashboard; if sign-off surfaces changes to the dashboard
  itself, those should land before or alongside this panel-integration work, not be silently
  absorbed into it.
- Frontend implementation stays within the locked D4 decision (server-rendered Jinja2 + HTMX, no
  client-side framework) — the `.dc.html` mockups' component model (`x-dc`, `sc-if`, `sc-for`,
  `dc-import`) is a design-tool artifact, not an implementation target (see the redesign
  directory's own `README.md`).
