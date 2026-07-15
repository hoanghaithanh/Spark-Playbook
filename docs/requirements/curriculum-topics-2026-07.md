# Curriculum Topics — 2026-07 Redesign Batch — Requirements

Status: Draft for architect handoff
Owner: requirements-analyst
Date: 2026-07-15 (updated 2026-07-15 — Catalyst exclusion note and the annotation-engine open
question both updated with settled/preferred decisions; see inline markers)

## Source and relationship to existing docs

Content derived from `docs/architecture/redesign-2026-07/topics-content-spec.md` (extracted from
the imported Claude Design mockups), per the human's 2026-07-15 decision to adopt the mockup's
full topic set as real backlog scope rather than treating the redesign as a pure UI reskin (see
`docs/architecture/redesign-2026-07/README.md`).

This doc covers **nine topics**, split two ways:

- **Six genuinely new topics**, not previously in `docs/backlog.md` at all: DAG & Lazy Evaluation,
  Skew & Salting, Executor Tuning, Checkpointing, Serialization Formats, Fault Tolerance &
  Lineage.
- **Three topics already scoped but only as thin one-line entries** in
  `docs/requirements/spark-playbook-mvp.md` (US-4.1 Caching/persistence, US-4.2 Window functions,
  US-3.3 Structured Streaming) — this doc **extends and supersedes those three stories'
  acceptance criteria** with the concrete concept/notebook/self-check content the mockup now
  provides. It does not change those topics' phase placement, resource assumptions, or (for
  Structured Streaming) its Kafka dependency — see Constraints.

Two other mockup topics — Spark SQL Catalyst and Partitioning & Shuffle Mechanics — are excluded
from this doc entirely. Partitioning & Shuffle Mechanics maps to already-built content (backlog
#2/#3) and is a pure shell-migration concern, covered by `docs/requirements/topic-shell-redesign.md`.
Spark SQL Catalyst (backlog #4) was flagged, as of this doc's original 2026-07-15 writing, as a
status discrepancy — marked "Done" but with no dedicated `content/` folder. **Settled 2026-07-15:**
it is now real, scoped content-build work, not just a status correction — see
`docs/requirements/topic-shell-redesign.md`'s US-SH8 and backlog #31. It remains outside this
doc's nine topics (it's tracked in `topic-shell-redesign.md` instead, since it's bundled with the
shell/topic-page build), not because it turned out to be a pure shell-migration item after all.

Four other backlogged-but-unbuilt topics — UDF vs pandas UDF (#16), Memory management & spill
(#17), Delta/Iceberg (#20), Tuning/debugging capstone (#21) — are **not** covered by the mockup
and are unaffected by this doc; their existing `spark-playbook-mvp.md` acceptance criteria stand
as-is.

## Problem statement

Six topics core to Spark interview depth — the DAG/laziness model, skew mitigation via salting,
executor sizing, checkpointing/lineage truncation, columnar vs row-oriented file formats, and
fault-tolerance/recomputation semantics — have no requirements coverage today; they exist only as
mockup content with no backlog entry. Separately, three already-backlogged topics (caching,
window functions, structured streaming) have sat as single-line backlog placeholders pointing at
the MVP doc's fairly thin original acceptance criteria, with no concrete concept/notebook/self-check
content to build against until now. This doc gives all nine topics the same level of concrete,
testable acceptance criteria the already-built topics (join-strategies, bucketing, AQE) have,
sourced from the mockup's extracted content rather than invented from scratch.

## Goals / Non-goals

### Goals

- **G-CT1 — Every topic ships concept content, a runnable notebook, and a manifest**, following
  the existing pattern (PLAN.md §3/§4): `concept.md` (what it is / why it matters / what to look
  for), `notebook.ipynb` matching the mockup's walkthrough steps, and `manifest.yaml` declaring
  `plan_nodes`/`stage_metrics` mappings — no hardcoded per-topic annotation logic (G7, unchanged).
- **G-CT2 — Every topic's self-check hypothesis is answerable from real evidence**, not just
  prose. Each topic's "what to look for" / self-check hypothesis (from `topics-content-spec.md`)
  must map to something the annotation engine, the stage-metrics REST data, or the Phase 2.5
  dashboard can actually surface — see G-CT3 for where that isn't yet true.
- **G-CT3 — Annotation/evidence gaps are surfaced, not silently worked around.** Where a topic's
  self-check hypothesis needs evidence the current annotation engine (plan-node matching only) or
  existing REST surfaces don't yet expose, this doc says so explicitly per-topic rather than
  assuming the engine already handles it (see Open Question 1).
- **G-CT4 — Renders through the shared shell.** Each topic's page uses the shell defined in
  `docs/requirements/topic-shell-redesign.md` — no topic in this doc needs its own bespoke page
  design.

### Non-goals

- **No UI/shell design decisions** — covered entirely by `topic-shell-redesign.md`.
- **No commitment yet to specific annotation-engine code changes.** This doc identifies which
  topics likely need them (Checkpointing, Executor Tuning, Fault Tolerance & Lineage); the actual
  engine design is an architect decision. The human has stated a preferred *direction* (extend the
  annotation engine — see Open Question 1), but that is not the same as committing to specific
  code changes, which still await the architect's review.
- **No UX/safety mechanism design for killing or restarting processes** (Fault Tolerance &
  Lineage's worker kill, Structured Streaming's query restart) — flagged as Open Question 2, not
  designed here.
- **No change to Structured Streaming's Phase 3/Kafka sequencing dependency** (backlog #19 must
  ship first) — this doc only refines *what* the topic teaches, not *when* it can be built.
- **No change to the four unaffected topics** (UDF/pandas UDF #16, memory/spill #17, Delta/Iceberg
  #20, tuning capstone #21) — out of scope, not touched by the mockup or this doc.

## User stories and acceptance criteria

**US-C1 — DAG & Lazy Evaluation topic.**
As a learner, I want a topic demonstrating that transformations are lazy and only actions trigger
execution, with the DAG and stage boundaries visible after the fact, so I can reason about *when*
Spark actually does work and stop being surprised by where `.collect()` stack traces point.

- *Given* the topic notebook, *when* I chain `.filter()` → `.select()` → `.groupBy()` with no
  action, *then* no job appears in the Spark UI Jobs tab or the `/api/v1/applications/<id>/jobs`
  REST response.
- *Given* the same chain, *when* I call `.explain(True)`, *then* the parsed/analyzed/optimized/
  physical plans are printed and **still no job is triggered** — the notebook walkthrough must
  make this comparison explicit (step 2 in `topics-content-spec.md`).
- *Given* the same chain, *when* I call `.count()`, *then* a job appears, and the topic's "what to
  look for" checklist directs the learner to the Jobs tab's DAG visualization to confirm the stage
  boundary lines up with the shuffle (the `groupBy`'s `Exchange`).
- *Given* the Self-check tab, *when* the learner hypothesizes whether `.explain(True)` alone
  triggers a job and clicks Reveal, *then* the annotation engine's plan-node output (confirming
  the plan was produced with zero jobs run) is derivable from existing REST job-list data — no
  new annotation-engine capability is needed for this topic (contrast with US-C4/US-C3/US-C9
  below).

**US-C2 — Skew & Salting topic.**
As a learner, I want a topic on manual key salting — distinct from AQE's automatic skew-join
splitting — so I understand the technique for cases AQE can't rebalance (e.g., a skewed `groupBy`
with no join to redistribute against).

- *Given* a dataset generated via the existing datagen utility (US-0.4) with one key covering ~60%
  of rows, *when* I run `groupBy(key).count()`, *then* the Stages tab / REST task-list data shows
  one straggler task whose duration and shuffle-read bytes are visibly larger than the rest.
- *Given* the same aggregation, *when* I salt the hot key with a random `0–9` suffix, aggregate,
  then strip the suffix and re-aggregate, *then* the per-task duration spread flattens (per
  `topics-content-spec.md`'s stated target: “2–4s across all 200 tasks — no single straggler”).
- *Given* the topic's relationship to the existing AQE topic (US-2.5), *when* the concept content
  is written, *then* it explicitly states salting is a *manual* technique for cases
  `spark.sql.adaptive.skewJoin.enabled` cannot help with (no join to rebalance against) — not a
  restatement of AQE's own skew-join splitting.
- *Given* the Self-check tab, *when* the learner hypothesizes about post-salting task-duration
  spread and clicks Reveal, *then* the evidence is sourced from existing stage/task REST data
  (`shuffleReadBytes`, task duration) already supported by the annotation engine's `stage_metrics`
  mechanism — no new engine capability needed.

**US-C3 — Executor Tuning topic.**
As a learner, I want a topic on executor sizing tradeoffs (cores/memory per executor, executors
per node), with GC-time evidence, so I can explain the "5 cores per executor" heuristic and
recognize low-utilization-despite-full-cluster symptoms.

- *Given* the topic notebook, *when* run once with 1 fat executor per node
  (`executor-cores=8`, `executor-memory=28g`) and once with right-sized executors
  (`executor-cores=5`, `executor-memory=12g`) against the same job, *then* the topic captures a
  measurable wall-clock and GC-time-fraction difference between the two runs.
- *Given* the Spark UI Executors tab exposes `totalGCTime` per executor via
  `/api/v1/applications/<id>/executors` (the same field already used by the Phase 2.5 dashboard's
  D-D decision), *when* the self-check evidence is defined, *then* it should reuse that existing
  data source rather than inventing a new one.
- *Given* this topic's self-check needs **per-executor GC time and executor-count/sizing
  comparison**, which is executor-level REST data, not plan-node data, *when* the annotation
  engine (plan-node matching only, per G7) is asked to surface it, *then* **it currently cannot**
  — this is a genuine gap. See Open Question 1: the human's stated preference is to extend the
  annotation engine to cover this, pending architect confirmation.

**US-C4 — Checkpointing topic.**
As a learner, I want a topic on `.checkpoint()` — truncating lineage vs. caching, reliable vs.
local checkpoints, and the tie-in to streaming checkpoint/offset recovery — so I understand why
long transformation chains need this and why it's mandatory for stateful streaming resume.

- *Given* the topic notebook, *when* I loop 40 times joining against a lookup table (growing
  lineage) and call `.explain()`, *then* the plan shows on the order of 40 nested join nodes.
- *Given* the same DataFrame, *when* I call `setCheckpointDir()` + `df.checkpoint()` and then
  `.explain()` again, *then* the plan collapses to a single flat scan (per
  `topics-content-spec.md`'s stated target).
- *Given* this topic's self-check hypothesis is literally "does `.explain()` still show 40 nested
  joins, or a single flat scan after checkpointing," *when* the annotation engine's manifest is
  authored, *then* it needs a `plan_nodes` rule that recognizes a **checkpoint-derived scan node**
  as its own concept (distinct from an ordinary `FileScan`/read) so the "lineage was truncated"
  claim is labeled, not just inferred by the learner counting nodes themselves. **Whether today's
  most-specific-first plan-node matcher can express "this scan replaced N prior nodes" at all, or
  whether it needs an engine change to compare *plan depth before/after* rather than just labeling
  individual nodes, is an open question for the architect** — flagged in Open Question 1, not
  assumed solvable with a manifest entry alone. The human's stated preference (Open Question 1) is
  to extend the engine to cover this, pending architect confirmation.

**US-C5 — Caching & Persistence topic** *(supersedes/extends US-4.1 in `spark-playbook-mvp.md`)*.
As a learner, I want a topic on `.cache()`/`.persist()` storage levels with a measurable
before/after timing comparison and Storage-tab evidence, so I understand when caching helps versus
wastes memory, matching the existing US-4.1 intent with concrete content.

- *Given* an expensive DataFrame (a join of two large parquet tables), *when* I `.cache()` +
  `.count()` to force materialization, *then* the Storage tab / REST equivalent shows the
  fraction cached and size in memory vs. disk for that DataFrame (unchanged from US-4.1's original
  criterion).
- *Given* the cached DataFrame, *when* I run a 2nd and 3rd action against it versus a single-use
  (uncached) DataFrame of comparable cost, *then* the topic notebook captures and displays the
  timing difference — `topics-content-spec.md` states the concrete example target as "~17x
  faster after caching; a single-use DataFrame gains nothing," which the notebook's own timing
  output should reproduce (not hardcode as an assumed number).
- *Given* the eviction/spill-to-disk behavior already required by US-4.1's second criterion,
  *when* this topic's notebook and self-check hypothesis are authored, *then* they explicitly
  connect back to that criterion — this doc does not relax or replace it, only adds concrete
  content and a hypothesis-first self-check flow around it.

**US-C6 — Window Functions topic** *(supersedes/extends US-4.2 in `spark-playbook-mvp.md`)*.
As a learner, I want a topic on window functions covering `row_number()`/running aggregates and
the accidental-single-partition failure mode of a missing `partitionBy`, so I connect window
functions to the earlier shuffle/partitioning concept concretely, not just conceptually.

- *Given* the topic notebook, *when* I compute `row_number()` over
  `partitionBy("user_id").orderBy("ts")` and a running total via
  `rowsBetween(unboundedPreceding, 0)`, *then* `.explain()` shows a `Window` plan node preceded by
  the appropriate sort/exchange, labeled per the existing US-2.1/US-4.2 mapping (unchanged).
- *Given* the same query with `partitionBy` mistakenly dropped, *when* run, *then* the plan/task
  data shows the entire dataset funneled onto a single partition/task (per
  `topics-content-spec.md`: "WARN logged, entire dataset moved to a single partition to guarantee
  global order") — the notebook must include this as a deliberate contrasting example, not just
  the correct-usage case, extending US-4.2's original criteria with this concrete failure mode.
- *Given* the Self-check tab, *when* the learner hypothesizes whether dropping `partitionBy`
  changes shuffle-partition count and clicks Reveal, *then* the evidence (task count collapsing to
  1) is available from existing stage/task REST data — no new annotation-engine capability needed.

**US-C7 — Structured Streaming topic** *(supersedes/extends US-3.3 in `spark-playbook-mvp.md`)*.
As a learner, I want the streaming topic to concretely demonstrate unbounded-state growth without
a watermark versus state plateauing with one, so the watermark/output-mode choice becomes a
visible, measured behavior rather than an abstract warning.

- *Given* a windowed streaming aggregation with **no** watermark, *when* run against the synthetic
  producer (US-3.2), *then* the topic notebook/dashboard shows state row count growing every
  batch, never dropping (per `topics-content-spec.md`'s framing).
- *Given* the same query with `withWatermark("event_time", "10 minutes")` added and restarted,
  *when* run, *then* state row count plateaus (target example: "levels off around 210 rows after
  batch 15") instead of growing unbounded — this is additive detail on top of US-3.3's existing
  late-data-dropped criterion, not a replacement for it.
- *Given* this topic's existing checkpoint-recovery criterion (US-3.3, third bullet), *when* this
  doc's content is built, *then* it is unchanged — this doc does not alter checkpoint-recovery
  scope, only adds the state-growth-vs-plateau demonstration.
- **This topic remains sequenced behind backlog #19 (Phase 3 Kafka compose integration)** — see
  Constraints. It cannot be built before Kafka is available in the compose template regardless of
  how concrete its content now is.

**US-C8 — Serialization Formats topic.**
As a learner, I want a topic comparing columnar (Parquet/ORC) against row-oriented (CSV/JSON)
formats via a measured bytes-read comparison, so I can explain predicate/column pushdown concretely
rather than by definition alone.

- *Given* a 50-column, ~2GB CSV file, *when* I read it and `select()` 3 columns, *then* the
  topic notebook captures total bytes read (from the SQL tab / REST scan metrics).
- *Given* the same data written out as Parquet, *when* I read it and `select()` the same 3
  columns, *then* bytes read drops close to proportionally to columns selected (per
  `topics-content-spec.md`'s target: "drops from 2.1GB to 118MB, close to 3/50 of the data").
- *Given* a Parquet table partitioned on a column, *when* I add a filter on that partitioned
  column, *then* the notebook demonstrates whole-file skipping (fewer files/bytes touched than an
  unfiltered read).
- *Given* the Self-check tab, *when* the learner hypothesizes about the bytes-read delta and
  clicks Reveal, *then* the evidence (bytes-read metric from the scan node) is available from
  existing SQL-tab/REST data — this maps cleanly onto the annotation engine's existing
  `stage_metrics` spotlighting mechanism, no new capability needed.

**US-C9 — Fault Tolerance & Lineage topic.**
As a learner, I want a topic demonstrating that Spark recovers from a lost worker by recomputing
only the affected partitions from lineage, not by restarting the whole job, so I understand
resilience-through-recomputation concretely rather than as a textbook claim.

- *Given* a running multi-stage job (filter → join → groupBy) and a worker killed mid-job (e.g.
  `kill -9` on the worker process), *when* observed via the Stages tab / REST task-list data,
  *then* only the lost partitions' tasks are retried (per `topics-content-spec.md`'s target
  example: "48/50 tasks kept their results, 2 retried"), not the whole job restarted.
- *Given* the same scenario, *when* the job completes, *then* its final result matches a clean run
  with no worker killed — the notebook must include this correctness check, not just the
  retry-count observation.
- *Given* this topic's self-check hypothesis ("does Spark restart the whole job or only recompute
  lost-worker partitions"), *when* the annotation manifest is authored, *then* it needs to
  recognize **task-retry-after-executor-loss** as a distinct signal — this is REST task-status
  data (`FAILED`/`resubmitted` task states tied to a specific executor loss event), not a
  plan-node concept at all, and **today's annotation engine has never been asked to label
  something that isn't a static plan node.** Flagged as a real, likely engine-level gap in Open
  Question 1, where the human's stated preference is to extend the engine to cover it, pending
  architect confirmation — this doc does not assume it's solvable with a manifest entry.
- **How a learner safely kills a worker process from a self-serve local tool is not decided by
  this doc.** See Open Question 2 — this is a UX/safety design question, not a content question,
  and this doc's acceptance criteria describe the *pedagogical* target (what the learner should
  observe), not the mechanism for triggering the failure.

## Open questions

1. **Annotation-engine gaps for non-plan-node evidence (Checkpointing, Executor Tuning, Fault
   Tolerance & Lineage). RESOLVED 2026-07-15** (architect, approved same day — see
   `docs/architecture/topic-shell-redesign.md` Decision A). The architect's recommendation
   *differs from the human's initial "extend the engine" lean* — split by data type instead of
   treating it as a binary:
   - **Checkpointing (US-C4):** genuinely is a plan-node-matcher extension — a manifest
     `plan_nodes` rule on the post-checkpoint scan node. No structural engine change needed beyond
     the rule itself.
   - **Executor Tuning (US-C3)** and **Fault Tolerance & Lineage (US-C9):** do **NOT** extend the
     plan-node matcher. Both route through a reveal-time REST pull inside the Self-check tab's
     existing annotation route, reusing `app_client.fetch_executors()` / `fetch_task_list()` and
     the dashboard collector's existing retry-counting/GC logic — self-check stays hypothesis-first
     and pull-not-push (same UX the human wanted), but the data plumbing is reused from
     `app/monitoring/` rather than rebuilt inside `app/annotation/`. See backlog.md rows #27/#28/#30
     for the settled per-topic disposition.
2. **Kill-a-worker / restart-a-query safety UX (Fault Tolerance & Lineage, Structured Streaming).**
   Both topics' notebook walkthroughs involve killing or restarting a live process
   (`kill -9` on a worker container; stopping/restarting a streaming query). Neither this doc nor
   the source mockup specifies how a learner does this safely and repeatably from a self-serve
   local tool — a raw shell command works for a developer building the topic, but is a real UX gap
   for the tool's actual learner-facing surface. Needs its own UX/safety design pass (e.g., an
   explicit "simulate worker failure" control scoped to the training cluster only, vs. leaving it
   as a documented manual step outside the app). Flagged, not designed here.
3. **Structured Streaming's Phase 3/Kafka dependency is a hard sequencing constraint, not a
   scope question.** Restated from Constraints for visibility: US-C7 cannot start before backlog
   #19 (conditional Kafka in the compose template) ships, regardless of sprint capacity elsewhere
   in this doc's scope.
4. **G1 tension (curriculum depth vs. platform polish) — see `topic-shell-redesign.md`'s own
   section on this.** Six of this doc's nine stories (all but Caching, Window Functions, and
   Structured Streaming, which were already backlogged) are exactly the kind of new
   interview-depth content G1 says should win over platform-polish effort when the two compete for
   the same sprint capacity. This doc does not resolve the sequencing between this curriculum work
   and the shell redesign — that's a sprint-planning call for the human/project-manager, informed
   by both docs' framing of the tension.

## Constraints

- **Content format is unchanged**: `content/<topic-id>/manifest.yaml` + `concept.md` +
  `notebook.ipynb`, per PLAN.md §3/§4 — same schema, same G7 constraint (no hardcoded per-topic
  annotation logic).
- **Structured Streaming (US-C7) is sequenced behind backlog #19** (Phase 3 Kafka compose
  integration) — cannot be built standalone ahead of the Kafka harness, per PLAN.md's Phase 3
  ordering, unchanged by this doc.
- **Resource budget is unchanged** (MVP doc's resource budget section, 64GB host ceiling, 32GB
  implemented resource-ceiling check) — none of these nine topics' notebooks should require
  cluster configurations outside the existing supported ranges (workers 1–5, cores 1–4, memory
  1–8GB) unless a specific topic's exercise explicitly needs the existing "single worker scaled up
  to 8GB" skew/spill allowance already established for Phase 4 topics.
- **Pages render through the shared shell** defined in `docs/requirements/topic-shell-redesign.md`
  — this doc assumes that shell exists or is being built concurrently; it does not specify any
  page-level UI itself.
