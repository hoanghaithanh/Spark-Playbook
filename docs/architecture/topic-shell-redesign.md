# ADR: Topic-Page Shell Redesign + Cluster Monitor Panel Integration

Status: Approved (human sign-off 2026-07-15, "Approve as-is")
Date: 2026-07-15
Requirements: `docs/requirements/topic-shell-redesign.md` (US-SH1–US-SH8),
`docs/requirements/curriculum-topics-2026-07.md` (US-C1–US-C9, Open Question 1)
Design reference: `docs/architecture/redesign-2026-07/` (`shell-topic-page.dc.html`,
`topics-index.dc.html`, `dashboard-panel.dc.html`, `topics-content-spec.md`)
Builds on / supersedes: PLAN.md D4/D5/§2/§3; `docs/architecture/realtime-monitoring-dashboard.md`
(ADR D-A–D-E — this doc supersedes **D-E's standalone-route placement only**, per the human's
2026-07-15 decision).

---

## Context

Three surfaces built independently as their phases shipped — the Phase 1 topic page + cluster
panel, the Phase 2 annotation self-check, and the Phase 2.5 standalone `/dashboard` — now need to
converge into one shared topic-page shell so the curriculum can grow from 5 to 14 topics without a
hand-built page per topic. The redesign is sourced from an imported Claude Design mockup whose
component syntax (`x-dc`, `sc-if`, `sc-for`, `dc-import`) is a design-tool artifact, not an
implementation target — PLAN.md D4 locks server-rendered Jinja2 + HTMX, and this ADR translates
the mockup's *interaction model* onto that stack.

Three decisions actually move the needle and are recorded here:

- **A — the annotation-engine extension question.** Three new curriculum topics (Checkpointing,
  Executor Tuning, Fault Tolerance & Lineage) need self-check evidence the current plan-node
  matcher cannot produce. The human's stated lean is "extend the annotation engine to cover all
  three." This is the expensive-to-reverse decision and the main reason for this consult.
- **B — dashboard panel-only migration mechanics.** `/dashboard` is retired as a standalone route
  (settled); the SSE stream's lifecycle across a repeatedly opened/closed panel, and the fate of
  the old URL, are open.
- **C — shell component architecture.** Where the mockup's interaction model needs client-side
  state vs. the existing server-driven HTMX flow.

The settled inputs (dashboard is panel-only; drawer ranges memory 1–8GB / shuffle 1–300;
`content/catalyst-plans/` is a plain shell topic; shell-first sequencing) are treated as locked
and not re-litigated here.

---

## Decision A — Split the three gap-topics by *which data model the signal lives in*; do **not** grow the plan-node engine for any of them

**Recommendation (differs from the human's initial lean, as explicitly invited):** do not extend
the plan-node matcher (`app/annotation/plan_parser.py`, `engine.py`) to consume executor/task REST
data. Instead split the three topics along the architecture's existing seam — *static plan-structure
fact* vs. *longitudinal runtime event* — and serve each from the surface that already owns that
data model:

| Topic | Signal it needs | Nature | Where it belongs | Engine (`engine.py`) change? |
|---|---|---|---|---|
| **Checkpointing (US-C4)** | post-checkpoint plan is a single scan | static plan-structure fact | **Annotation self-check** (its home) | **No** — one new manifest `plan_nodes` rule |
| **Executor Tuning (US-C3)** | per-executor GC-time fraction, run-vs-run | longitudinal runtime metric | **Annotation self-check tab, reveal-time `/executors` pull** | **No** — new manifest section + route evidence source |
| **Fault Tolerance (US-C9)** | task retried after worker loss | longitudinal runtime event | **Annotation self-check tab, reveal-time task-list pull** | **No** — route evidence source, no plan nodes |

The key reframing: the choice the requirements posed — "extend the annotation **engine** vs. route
through the **dashboard**" — is a false binary that conflates two separable things. The annotation
**engine** (`engine.annotate_plan()` + `plan_parser`) is specifically the *plan-node matcher*. The
annotation **feature** (the Self-check tab, the Reveal action, `app/web/routes/annotation.py`) is
broader, and *already today* blends two evidence sources on one Reveal: (1) plan-node labels from
the checkpoint dump, **and** (2) a live `/stages` REST pull for `stage_metrics`. Reaching for
`/executors` and the task list at reveal-time is the *same shape* as the `stage_metrics` pull it
already does — a point-in-time REST read, reusing `app_client.fetch_executors()` /
`fetch_task_list()` which already exist and are already consumed by the dashboard collector.

This threads the needle the human actually cares about:

- **Keeps all three self-checks in the Self-check tab, hypothesis-first (G3 pull).** The human's
  implicit reason for wanting them in the annotation feature — rather than the always-on dashboard
  — is preserved. They stay reveal-gated pulls, not always-on push.
- **Does not pollute the plan-node matcher** with non-plan concepts. `engine.py` stays
  plan-node-only, honoring G7's "no hardcoded per-topic logic" and its single, clean
  responsibility. GC time and task retries are *not* plan nodes and never become fake ones.
- **Does not duplicate the collector.** Evidence comes from `app_client` (the shared REST library
  both features already depend on), not a second copy of `monitoring/collector.py` inside
  `annotation/`.
- **Respects the pull-vs-push boundary named in PLAN.md §3.** The dashboard remains the always-on
  push view; the self-checks remain single-snapshot pulls. A reveal-time snapshot is sufficient for
  each because the REST endpoints retain the needed post-hoc state (see below).

### Why a reveal-time snapshot is sufficient for each

- **Checkpointing.** The post-checkpoint `explain()` dump *already is* a single flat scan — the
  distinguishing fact lives in one snapshot. A manifest rule matching the checkpoint scan operator
  (e.g. `Scan ExistingRDD` for `df.checkpoint()`) labels it "checkpoint-truncated lineage." The
  learner already saw the 40-node plan and the 1-node plan print in their own two `.explain()`
  calls; the self-check's job is to *label the after-state*, which today's most-specific-first
  matcher does natively. **No engine change; a `plan_nodes` entry only.** (If `plan_parser` doesn't
  yet tokenize the checkpoint scan node cleanly, that's a one-line regex/precedence tweak in the
  parser, not a model change.)
- **Executor Tuning.** `/api/v1/applications/<id>/executors` reports cumulative `totalGCTime` and
  `totalDuration` per executor. A single post-run read yields exactly the "GC-time fraction"
  the topic teaches (`totalGCTime / totalDuration`), per executor. The fat-vs-right-sized
  *comparison* is the learner running twice and revealing twice — no cross-cycle delta needed
  (unlike the dashboard, which must delta because it samples continuously). One reveal-time REST
  read suffices.
- **Fault Tolerance.** The `/stages/<id>/taskList` REST data retains retried tasks as separate
  records sharing an `index` with `attempt ≥ 1` (this is exactly what the collector's
  `retries_by_index` reads post-hoc). After the job, a reveal-time snapshot still shows "2 of 50
  tasks have attempt ≥ 1" — the "48/50 kept results, 2 retried" signal — without needing to have
  observed the failure live.

### What actually changes (concrete, per topic)

1. **Checkpointing** — content + manifest only: `content/checkpointing/manifest.yaml` gets a
   `plan_nodes` rule for the checkpoint scan node. Zero code beyond a possible parser tweak.
2. **Executor Tuning** — a new *optional* manifest section `executor_metrics:` (parallel to
   `stage_metrics:`), listing executor-level keys to spotlight (`totalGCTime`, `totalDuration`,
   `totalTasks`), validated in `manifest.py`, spotlighted by a new
   `engine.spotlight_executor_metrics(executors, manifest)` helper that is *structurally identical*
   to the existing `spotlight_stage_metrics()` (pass-through of REST values, US-2.2). The
   `annotation.py` reveal handler calls `app_client.fetch_executors()` when the manifest declares
   the section. This is an additive, symmetric extension of the existing metric-spotlight
   mechanism — **not** a new matching model.
3. **Fault Tolerance** — reveal-time task-list evidence in `annotation.py`, reusing
   `app_client.fetch_task_list()` and the collector's retry-counting logic (extracted to a small
   shared helper so it isn't duplicated), rendered as a "N of M tasks retried" evidence block.
   A `manifest.yaml` flag (e.g. `task_retry_evidence: true`) gates it so it stays data-driven and
   no per-topic branching lands in code.

The dividing line for a future maintainer: **if the signal is a fact about the query plan, it's a
`plan_nodes` rule and it goes through `engine.annotate_plan()`. If it's a runtime metric or event,
it's a manifest-declared reveal-time REST spotlight (`stage_metrics` / `executor_metrics` /
task-retry), and it never touches the plan-node matcher.** Checkpointing is the first kind;
Executor Tuning and Fault Tolerance are the second.

### US-SH8 (`content/catalyst-plans/`) confirmation

Confirmed: Catalyst plans fits the existing content-driven pattern with **no engine change**. The
"UDF-wrapped filter does not push below the join, DataFrame/SQL versions do" self-check is a pure
plan-node-position fact — expressible with `plan_nodes` rules distinguishing `Filter` above vs.
below the join, using the same manifest-driven adjacency mechanism `content/bucketing/` already
uses (`requires_absent_nearby`/`window`, `manifest.PlanNodeRule`). It is the first kind of signal
in the dividing line above. No change to `content/join-strategies/` (confirmed in the requirements:
its "Catalyst" mentions are passing vocabulary, not the phase-breakdown content this topic owns).

### The one honest cost of this recommendation

It rejects the human's "extend the engine for all three" as literally stated, in favor of "extend
the annotation **feature's reveal-time evidence sources** for two of them, and use a plain manifest
rule for the third." The distinction matters because "extend the engine" most naturally reads as
"teach `engine.py`/`plan_parser.py` to consume executor and task-status data and add match types
beyond node-name matching" — and *that* reading is the structurally wrong move: it rebuilds inside
`annotation/` a data-consumption capability `monitoring/` already has, and it breaks the matcher's
clean one-dump→labels model with temporal/comparison logic it was never shaped for. The
recommendation gives the human what they wanted (signal in the hypothesis-first Self-check tab, not
the dashboard) while avoiding that structural mislabel.

### Addendum (2026-07-18) — Decision A resolved for Checkpointing (US-C4 / issue #47, Sprint 8): manifest-only, concrete rule shape

Decision A settled the *category* (Checkpointing is a `plan_nodes` rule, not a REST pull) but left one
sub-question open, which the requirements-analyst's 2026-07-18 review of US-C4 AC3 sharpened: does
labeling the post-checkpoint scan need only a single-node manifest rule (the `cache-hit-scan`
precedent), or does Reveal need to *assert* "this scan replaced N prior joins" — a before/after
plan-diff `annotate_plan()` cannot do today? **Resolved: manifest-only, single-node rule. No engine
change.** This confirms and makes concrete the checkpoint bullet above, and closes the "Quantified
checkpoint before/after diff" open question below as **declined** for US-C4.

**Why manifest-only is sufficient (pedagogical, not just cheap).** The self-check's job in this app is
to *label what a node is* and confirm the learner's prediction — not to re-prove a fact the learner
already observed. The 40-nested-joins → 1-flat-scan contrast is already fully visible in the learner's
own two raw `.explain()` prints (AC1 and AC2); the engine sees neither of those and adds nothing to
that count. AC3's hypothesis ("still 40 joins, or a single flat scan?") is answered the moment Reveal
confirms the surviving node *is* a checkpoint-derived scan of the materialized data. Building a
cross-plan diff to quantify "40 → 1" would re-derive, inside the engine, a number the learner already
read off two prints — a genuinely new engine capability (two plan captures + a diff model,
`annotate_plan()`'s one-list contract broken) bought for pure polish, needed by exactly zero shipped
topics. Ponytail rung 1: declined. Consistent with the Alternatives row already in this ADR ("Build a
plan-depth before/after diff mechanism into the engine — declined") — this addendum just makes that
call binding for US-C4 rather than provisional.

**Concrete rule shape (mirrors `content/caching-persistence/manifest.yaml`'s `cache-hit-scan`):**

```yaml
annotation:
  plan_nodes:
    - match: "Scan"
      concept: checkpoint-truncated-scan
      label: "Checkpoint-truncated lineage — a single flat scan of the checkpointed data; the 40 nested joins are gone"
```

**Two constraints the developer must honor / confirm empirically (R-Shell-1 concretized):**

1. **`match:` must be `"Scan"`, not `"Scan ExistingRDD"`.** `plan_parser.parse_operators()` tokenizes
   only a node's *first word* (`_OPERATOR_NAME_RE`, plan_parser.py:50; the same #31 constraint that
   forced Serialization Formats / Skew & Salting onto `stage_metrics`). A reliable `df.checkpoint()`
   (and `localCheckpoint()`) backs the DataFrame with a checkpointed RDD, whose physical node is
   `RDDScanExec`, printed as `Scan ExistingRDD` → token `Scan`. A multi-word `match` would silently
   never fire. This also cleanly distinguishes checkpoint from caching: cache re-reads print
   `InMemoryTableScan`, checkpoint prints `Scan ExistingRDD` — genuinely different nodes, reinforcing
   checkpoint ≠ cache.
2. **Feed `annotate_plan()` the *post-checkpoint* capture only, where `Scan` is unambiguous.** `Scan`
   is a deliberately generic token (a source read also prints `Scan parquet` → `Scan`). That is safe
   *because* checkpointing truncates lineage: the post-checkpoint plan the notebook captures and
   passes to Reveal contains a single scan node (the joins and the lookup reads are gone), so `match:
   "Scan"` labels exactly one node — the checkpoint scan. The developer must verify against a live
   `df.checkpoint()` `.explain(mode="formatted")` dump on the target Spark that (a) the surviving node
   is indeed `Scan ExistingRDD` and (b) the captured post-checkpoint plan carries no second residual
   `Scan`. If a stray `Scan` survives, it would take the same generic label — noticed at acceptance as
   a mislabeled node; the fix stays a manifest/label wording tweak, still no engine change. AC4 is
   content-only (`concept.md` durability + streaming-offset tie-in) and needs no manifest rule at all.

---

## Decision B — SSE migration: one shared collector, a fresh per-open panel connection; `/dashboard` full page becomes a redirect, `/dashboard/stream` stays

### B1 — Stream lifecycle across panel open/close/reopen: fresh EventSource per open, single shared collector

The existing collector design already contains the correct answer; the panel just reuses it.

- **Collector stays a module-level singleton** (`app.monitoring.collector.collector`), decoupled
  from delivery (D-B). Its lifecycle gate is unchanged: it samples only while
  `manager.state == READY` **and** `subscriber_count() > 0`, and it exits on the last unsubscribe.
- **The panel connects a fresh SSE stream on open and drops it on close.** The slide-in panel body
  carries the HTMX SSE-extension connect element (`hx-ext="sse" sse-connect="/dashboard/stream"`).
  Opening the panel injects that element into the DOM (→ browser opens an `EventSource` →
  `/dashboard/stream` generator calls `collector.subscribe()`). Closing the panel removes the
  element from the DOM (→ `EventSource` closes → the generator's `finally` calls
  `collector.unsubscribe()`).
- **No leak, no duplication (US-SH4):** because collection is decoupled from delivery, N opens over
  a session share *one* collector. The last close → last unsubscribe → collector `.cancel()` stops
  sampling (no polling with nobody watching, R-Dash-3). A reopen → `subscribe()` →
  `ensure_running()` restarts the collector, and `subscribe()` immediately replays
  `_latest_snapshot` into the new queue (collector.py:187–188) so the panel repaints instantly
  without a full page reload.

**Why fresh-per-open rather than a hidden always-connected page-level stream:** keeping a stream
alive while the panel is closed would keep the collector sampling Docker + Spark with nobody
looking — contradicting US-5.5's "polling stops when the browser leaves" and R-Dash-3. Tying the
connection lifecycle to the panel-open lifecycle keeps sampling exactly coincident with watching.
The "single shared collector" already provides the "don't duplicate sampling" guarantee that a
page-level singleton stream would otherwise be needed for, so the singleton-stream complexity buys
nothing.

### B2 — Old `/dashboard` URL: redirect to a topic page with the panel auto-opened, not a 404

`GET /dashboard` changes from rendering the standalone page to a `307` redirect to the current
topics entry point with a panel-open hint — `/topics/<first-topic>?monitor=open` (reusing the
existing `/`→first-topic resolution in `topics.index`). The shell reads `?monitor=open` and
auto-opens the Cluster Monitor panel on load. This keeps existing bookmarks/deep-links working
(they land on a real page with the monitor open) instead of dead-ending on a 404, at the cost of
one extra line of shell JS to honor the query param.

`GET /dashboard/stream` is **kept unchanged** — it is the SSE endpoint the panel connects to. Only
the full-page `GET /dashboard` handler and the `base.html` persistent "Cluster Monitor" nav link
(which pointed at the standalone page) are removed/replaced; the collector, `model.py`,
`diagnostics.py`, `eta.py`, and the OOB fragment renderers are untouched. The three dashboard views
(`overview`/`job_detail`/`node_detail`) move from `dashboard/page.html` into a body partial
(`dashboard/_dashboard_body.html`) that the panel includes; the existing `hx-swap-oob` fragment
mechanism and the client-side view-switcher (preserve the recent fix that stopped OOB swaps
stripping view-switcher classes) work unchanged inside the panel because they target element IDs,
which now live inside the panel DOM while it's open.

---

## Decision C — Shell = one server-rendered Jinja2 template; cluster state server-driven (existing), view state (tabs/drawer/panel) client-side

The mockup's interaction model maps cleanly onto D4's stack. The split is clean and follows an
existing precedent (the Phase 2.5 dashboard already does client-side view switching over
server-rendered fragments):

| Interaction | Mechanism | Notes |
|---|---|---|
| **Concept / Notebook / Self-check tabs** | **client-side** show/hide | All three panels rendered server-side into the page; ~10 lines of `shell.js` toggle visibility. No round-trip; the Self-check tab's Reveal stays an HTMX POST fragment; the Notebook iframe stays as-is. |
| **Cluster-config drawer** | **client-side** open/close (CSS transform) | The drawer *contains* the existing `cluster_panel.html` spawn/teardown HTMX forms, relocated. Open-state is view state; spawn/teardown stay HTMX POSTs swapping `#cluster-panel`. |
| **Breadcrumb topic switcher** | **server data**, client-side dropdown toggle | Dropdown lists `loader.list_topics()`; each entry is a plain `<a href="/topics/<id>">` (full navigation, US-SH3). Navigation is stateless w.r.t. the cluster (single global slot), so switching topics cannot tear down the cluster — already true. |
| **Cluster Monitor panel** | **client-side** open/close + SSE inject | Per Decision B. |
| **Cluster state machine** (idle / spawning / error / ready) | **server-driven** (existing) | `manager.status()` renders the right-pane state; the spawn POST is synchronous-to-ready (blocks up to 90s per §2), so `hx-indicator` **is** the "spawning" spinner and the swapped response **is** the idle→ready/error transition. No client polling needed. |

Only `shell.js` is new client code: four small vanilla handlers (tab switch, drawer toggle,
breadcrumb dropdown, monitor open/close-with-SSE-inject) + reading `?monitor=open`. This stays
inside D4 (no client-state framework; a tiny UI-toggle script is lighter than the charting lib D4
already sanctions).

### Friction found while translating (all minor, all resolvable)

1. **Slider ranges.** The mockup's memory slider is 1–16GB and shuffle is 8–400/step-8; the locked
   ranges are 1–8GB and 1–300 (US-SH2). Use the locked ranges. Keep the existing number inputs (or
   bound sliders to them) — the existing `cluster_panel.html` inputs already enforce this; only
   `shuffle_partitions` needs a `max="300"` added.
2. **Two spawn entry points.** The mockup offers "Spawn cluster" both in the drawer and in the
   right-pane idle state. Recommendation: the idle-state button does a one-click spawn with the
   topic's `cluster_defaults`; the drawer offers customization. Both POST `/topics/<id>/spawn`.
3. **Top-bar state pill lives outside `#cluster-panel`.** The spawn/teardown responses should
   OOB-swap the pill (`hx-swap-oob`) so it updates without a standing poller; on navigation it
   renders fresh from `manager.status()`. Avoids adding an always-on status poll.
4. **Client-side tab state resets on navigation.** Breadcrumb switching is a full page load, so a
   topic opens on the Concept tab. Acceptable; no state to preserve.
5. **Notebook iframe in a hidden tab.** Rendering the iframe inside an inactive (hidden) tab is
   fine; preserve the `?spawn={{ status.spawn_id }}` reconnect param (US-1.3).

**Conclusion for C:** the interaction model maps cleanly with only cosmetic friction; nothing in
the mockup forces a client-state framework or a departure from D4/D5.

---

## Alternatives considered

| Decision | Alternative | Why not chosen |
|---|---|---|
| A | Extend `engine.py`/`plan_parser` to consume `/executors` + task-status and add non-node match types (human's literal lean) | Rebuilds inside `annotation/` a data-consumption capability `monitoring/` already has; breaks the matcher's clean one-dump→labels model with temporal/comparison logic; conflates the plan-node *matcher* with the annotation *feature*. |
| A | Route Executor Tuning + Fault Tolerance self-checks through the always-on dashboard | Loses the hypothesis-first Reveal pedagogy the human wanted in the Self-check tab; the dashboard is deliberately always-on/not reveal-gated (D-A). The reveal-time `app_client` pull keeps the pull gesture without the dashboard. |
| A (Checkpointing) | Build a plan-depth before/after diff mechanism into the engine | The learner already sees both `.explain()` outputs; labeling the after-state single scan (pure manifest) delivers the self-check. The quantified "40→1" diff is low-value polish for a genuinely new engine capability — declined. |
| B | Page-level singleton SSE stream the panel attaches/detaches from | Would keep the collector sampling while the panel is closed (violates US-5.5 / R-Dash-3); the shared collector already guarantees no duplicate sampling, so the singleton-stream complexity buys nothing. |
| B | Return 404 for old `/dashboard` | Dead-ends existing bookmarks; a redirect-with-panel-open costs one JS line and preserves them. |
| C | HTMX-load each tab on demand | Adds round-trips for pure view state; the panels are cheap to render inline. Client-side show/hide matches the dashboard's own view-switch precedent. |
| C | Adopt a client-state framework (Alpine/Stimulus) for drawer/panel/tabs | D4 forbids a client framework; four vanilla handlers suffice. |

---

## Consequences

**Accepted trade-offs:**

- **A adds a second and third manifest evidence category** (`executor_metrics`, task-retry flag)
  and a reveal-time `/executors` / task-list pull to `annotation.py`. This is new surface in the
  annotation *feature*, but it is symmetric with the existing `stage_metrics` pull and leaves the
  plan-node matcher untouched. What becomes harder: two topics' self-checks now depend on a live
  `:4040` at reveal-time (the `stage_metrics` path already does); a torn-down driver degrades them
  to the existing "no active application" state, reusing `app_client`'s existing handling.
- **A deliberately does not deliver a quantified checkpoint before/after diff.** If the human later
  wants "collapsed from N to 1" shown in-app, that is the one genuinely new engine-shaped feature
  and would be revisited then; the manifest label covers the acceptance criterion now.
- **B removes the standalone `/dashboard` page** (a validated Phase 2.5 surface, D-E). The dashboard
  content is unchanged; only its entry point moves. A redirect preserves old links. Phase 2.5
  acceptance (`docs/acceptance/phase-2-5.md`) is still in draft pending sign-off — if that surfaces
  dashboard changes, they land before/with this panel work (a stated constraint).
- **C introduces `shell.js`** — the first non-HTMX client JS in the app. It is intentionally tiny
  and scoped to view toggles; it does not manage data or cluster state (both stay server-driven).
- **All three built topics + Catalyst migrate to one shell.** After this, adding a topic is a
  `content/<id>/` folder only (US-SH1), and per-topic page markup ceases to exist — the intended
  win. What becomes harder: a shell bug now affects every topic at once (mitigated by the shell
  being thin and content-driven).

---

## Component / data design

New/changed components. Reuses `spark_api/app_client.py`, `lifecycle/manager.py`,
`monitoring/collector.py`; the plan-node matcher (`annotation/engine.py::annotate_plan`,
`plan_parser.py`) is **unchanged**.

```
app/
  web/
    templates/
      shell.html                    # NEW — the one shared topic-page shell (replaces topic.html)
      topics_index.html             # NEW — data-driven landing page (US-SH5)
      fragments/
        _top_bar.html               # NEW — breadcrumb switcher + state pill (OOB target) + monitor btn
        _cluster_drawer.html        # NEW — wraps the relocated cluster_panel.html forms
        cluster_panel.html          # reused; add max="300" to shuffle input; pill OOB on spawn/teardown
        annotation_panel.html       # reused unchanged (Self-check tab body)
        annotation_reveal.html      # extended — renders executor_metrics + task-retry evidence blocks
        _monitor_panel.html         # NEW — slide-in wrapper; includes dashboard body + SSE connect elt
      dashboard/
        _dashboard_body.html        # NEW — the 3 views + SSE listener, extracted from page.html
        page.html                   # retired (full standalone page); body moved to _dashboard_body
        fragments/*_oob.html        # unchanged (OOB targets now live inside the panel)
    static/
      shell.js                      # NEW — tab / drawer / breadcrumb / monitor toggles + ?monitor=open
    routes/
      topics.py                     # topic_page -> renders shell.html; add GET / (index); pill fragment
      annotation.py                 # reveal handler: + executor_metrics pull, + task-retry evidence
      dashboard.py                  # GET /dashboard -> 307 redirect; keep GET /dashboard/stream
  annotation/
    manifest.py                     # + optional executor_metrics section, + task_retry_evidence flag
    engine.py                       # + spotlight_executor_metrics() (mirror of spotlight_stage_metrics)
                                    #   annotate_plan()/plan_parser UNCHANGED
  monitoring/
    collector.py                    # extract retry-count helper for reuse by annotation.py (no behaviour change)
content/
  catalyst-plans/                   # NEW topic (US-SH8): concept.md + notebook.ipynb + manifest.yaml
  checkpointing/ executor-tuning/ fault-tolerance-lineage/  # manifests per Decision A
```

**Reveal-time evidence flow (Decision A, Executor Tuning / Fault Tolerance):**

```
 learner clicks Reveal on Self-check tab  (unchanged pull gesture, G3)
   ├─ plan-node labels      ← plan_parser + engine.annotate_plan(checkpoint dump)   [UNCHANGED]
   ├─ stage_metrics          ← app_client.fetch_stages(app_id)  → spotlight_stage_metrics   [EXISTS]
   ├─ executor_metrics       ← app_client.fetch_executors(app_id) → spotlight_executor_metrics [NEW, if manifest declares]
   └─ task-retry evidence    ← app_client.fetch_task_list(...) → retry count per index      [NEW, if manifest flag]
```

**SSE / panel flow (Decision B):**

```
 open panel  → inject sse-connect element → EventSource → /dashboard/stream → collector.subscribe()
                                                              └─ ensure_running() (READY + subs>0) → sampling
 close panel → remove element → EventSource closes → generator finally → collector.unsubscribe()
                                                              └─ last sub gone → collector stops
 reopen      → subscribe() replays _latest_snapshot → instant repaint, single shared collector
 GET /dashboard (legacy) → 307 → /topics/<first>?monitor=open → shell.js auto-opens panel
```

**Manifest schema addition (data-driven, G7 preserved):**

```yaml
annotation:
  plan_nodes: [ ... ]            # unchanged; Checkpointing + Catalyst use this
  stage_metrics: [ ... ]         # unchanged
  executor_metrics:              # NEW (Executor Tuning) — same shape as stage_metrics
    - key: totalGCTime  ; spotlight: true
    - key: totalDuration
    - key: totalTasks
  task_retry_evidence: true      # NEW (Fault Tolerance) — gates the reveal-time task-list pull
```

---

## Visual design

Source of truth: `docs/architecture/redesign-2026-07/shell-topic-page.dc.html` and
`topics-index.dc.html` (read as spec, not rendered), translated to Jinja2 + HTMX, with the
mockup's fakes wired to real data per its own implementer notes.

**Topic-page shell layout (single fixed-viewport page, no scroll on the frame):**

- **Top bar** (dark `oklch(0.19 …)`, 48px): diamond accent mark + "Spark Playbook"; center =
  breadcrumb `Topics / <current topic> ▾` (button opens a dropdown of all topics, current one
  dot-highlighted); right = cluster-state pill (colored dot + monospace state label, pulsing when
  busy) + a "Cluster Monitor" button.
- **Left pane (~38% width):** a tab strip (`Concept | Notebook | Self-check`) + a gear button
  (opens the cluster drawer). Below, the active tab's body:
  - *Concept* — rendered `concept.md` (TOPIC N eyebrow, title, What it is / Why it matters / What
    to look for). Real markdown, not the mockup's placeholder copy.
  - *Notebook* — walkthrough steps list from content, plus the real note that the live JupyterLab
    is in the right pane (the mockup's static fake cells are preview-only, do not ship).
  - *Self-check* — hypothesis textarea + "Reveal self-check" (existing HTMX Reveal). Revealed
    output shows YOUR PREDICTION + real engine output (plan labels and/or metric spotlights) — the
    mockup's hardcoded "ACTUAL ANNOTATION OUTPUT" text must **not** ship (G-SH7).
- **Cluster drawer** (slides from left, 340px, dimmed backdrop): WORKERS (±), CORES/WORKER (±),
  MEMORY GB (**1–8**, not the mockup's 1–16), SHUFFLE PARTITIONS (**1–300**, not 8–400), AQE
  toggle; footer Spawn / Tear down (with confirm) buttons. Reflects idle/spawning/ready/error
  state (D5), not a new lifecycle.
- **Right pane (state machine):** *idle* → framed empty state + "Spawn cluster"; *spawning* →
  spinner + "Provisioning N workers…" (this is the `hx-indicator` region during the spawn POST);
  *error* → red "!" + failure message + Retry; *ready* → the real embedded JupyterLab iframe
  (US-SH7), not the mockup's fake cells.
- **Cluster Monitor panel** (slides from right, ~70% width, dimmed backdrop, dark 40px header with
  ×): embeds the Phase 2.5 dashboard body (overview node grid / job-detail stage timeline +
  partition table / node-detail sparklines). **No `Suggestion:` lines** (D-A must not regress,
  US-SH4). "No active cluster" empty state when nothing is running (US-5.6).

**Topics-index landing page:** cards generated from `content/*/manifest.yaml` (order, title, a
short blurb) — no hardcoded list (US-SH5). Every built topic present, none that don't exist,
including `catalyst-plans` once built.

**Distinct states to verify (beyond "it works"):**
- *Concept/Notebook/Self-check tabs* switch without a page reload; the URL/cluster is unaffected.
- *Drawer closed by default*; opens over a dimmed backdrop; spawn ranges are 1–8GB / 1–300.
- *Breadcrumb dropdown* lists exactly the built topics; current topic highlighted; selecting one
  navigates without tearing down the cluster.
- *Monitor panel:* opens with live data when a cluster is running; "no active cluster" empty state
  otherwise; **no suggestion text anywhere**; closing then reopening resumes live data with no full
  reload and no duplicate stream.
- *Legacy `/dashboard`:* redirects to a topic page with the panel already open, not a 404.
- *Self-check on the three gap topics:* Checkpointing labels the checkpoint scan node; Executor
  Tuning shows per-executor GC/duration/tasks; Fault Tolerance shows "N of M tasks retried" — all
  hypothesis-first, all reveal-gated, none appearing before Reveal.

---

## Risks

- **R-Shell-1 — Checkpoint scan node not tokenized/labelled as expected.** The post-checkpoint plan
  node name (`Scan ExistingRDD` vs. a Parquet re-read of the checkpoint dir) may not match a naive
  manifest rule, or `plan_parser` may split it oddly. *Noticed by:* Reveal on the Checkpointing
  topic shows the checkpoint scan as "unknown/unannotated." *Mitigation:* verify the actual node
  name against a real `df.checkpoint()` explain dump before authoring the rule; a one-line parser
  precedence tweak if needed — still no model change.
- **R-Shell-2 — Reveal-time `:4040` dependency for two topics.** Executor Tuning / Fault Tolerance
  self-checks pull live REST at reveal; a torn-down or 4041-bumped driver (PLAN.md R2) yields no
  data. *Noticed by:* empty evidence block while the learner expects numbers. *Mitigation:* reuse
  `app_client`'s existing "no active application on 4040" handling and the reveal's existing stale-
  checkpoint warning; render a clear "no active application" evidence state, matching `stage_metrics`.
- **R-Shell-3 — SSE reconnect storm on rapid panel toggling.** Fast open/close/open cancels and
  restarts the collector task each cycle. *Noticed by:* collector start/stop log churn; a brief
  blank panel. *Mitigation:* `_latest_snapshot` replay on subscribe already covers the visual gap;
  if churn is real, add a short (~2s) grace delay before the collector stops on last unsubscribe.
  Single-user, so contention is bounded.
- **R-Shell-4 — OOB fragments target IDs that only exist while the panel is open.** If an SSE event
  arrives mid-close (element removed), the swap no-ops. *Noticed by:* console warnings, never stale
  data. *Mitigation:* acceptable by design — the connection closes with the panel; preserve the
  existing view-switcher-class fix so reopened panels render correctly.
- **R-Shell-5 — Shell regression blast radius.** One shell now backs every topic; a template bug
  breaks all topics at once. *Noticed by:* any topic page failing to render. *Mitigation:* keep the
  shell thin and content-driven; a smoke test that every `content/*` topic renders through the shell
  (extends US-SH1's content-as-data criterion).
- **R-Shell-6 — Scope creep from "signal" back toward "conclusions" in the panel.** Embedding the
  dashboard in-topic invites re-adding the mockup's `Suggestion:` lines. *Noticed by:* acceptance
  vs. D-A/US-SH4. *Mitigation:* the `SignalCard` model still has no suggestion field (D-A, R-Dash-6);
  the panel reuses the same fragments, so the guard is inherited, not re-implemented.

---

## Open questions (unresolved, flagged not blocked)

- **Kill-a-worker / restart-a-query safety UX** (curriculum doc Open Question 2) for the Fault
  Tolerance and Structured Streaming notebooks is a UX/safety design pass, out of scope here. It
  does not affect Decision A's evidence sourcing (the task-retry data is the same whether the
  failure is triggered by a raw shell command or a future in-app control).
- **Quantified checkpoint before/after diff** — declined for now (see Consequences); revisit only
  if the manifest-label self-check proves pedagogically insufficient in acceptance.
