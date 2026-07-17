# Curriculum Topics — 2026-07 Redesign Batch — Requirements

Status: Draft for architect handoff
Owner: requirements-analyst
Date: 2026-07-15 (updated 2026-07-15 — Catalyst exclusion note and the annotation-engine open
question both updated with settled/preferred decisions; see inline markers) (updated again
2026-07-15, same day — added **US-C10 Memory Management**, closing a gap where
`topics-content-spec.md`'s "07 — Memory Management" section had never been turned into a user
story; see inline markers)

## Source and relationship to existing docs

Content derived from `docs/architecture/redesign-2026-07/topics-content-spec.md` (extracted from
the imported Claude Design mockups), per the human's 2026-07-15 decision to adopt the mockup's
full topic set as real backlog scope rather than treating the redesign as a pure UI reskin (see
`docs/architecture/redesign-2026-07/README.md`).

This doc covers **ten topics**, split two ways:

- **Six genuinely new topics**, not previously in `docs/backlog.md` at all: DAG & Lazy Evaluation,
  Skew & Salting, Executor Tuning, Checkpointing, Serialization Formats, Fault Tolerance &
  Lineage.
- **Four topics already scoped but only as thin/underspecified entries** in
  `docs/requirements/spark-playbook-mvp.md` (US-4.1 Caching/persistence, US-4.2 Window functions,
  US-3.3 Structured Streaming, and — added 2026-07-15, same day, as a correction, see below —
  US-4.4 Memory management & spill) — this doc **extends and supersedes those four stories'
  acceptance criteria** with the concrete concept/notebook/self-check content the mockup now
  provides. It does not change those topics' phase placement, resource assumptions, or (for
  Structured Streaming) its Kafka dependency; for Memory Management it does not relax or replace
  US-4.4's existing spill/OOM-diagnosis criteria — see Constraints.

Two other mockup topics — Spark SQL Catalyst and Partitioning & Shuffle Mechanics — are excluded
from this doc entirely. Partitioning & Shuffle Mechanics maps to already-built content (backlog
#2/#3) and is a pure shell-migration concern, covered by `docs/requirements/topic-shell-redesign.md`.
Spark SQL Catalyst (backlog #4) was flagged, as of this doc's original 2026-07-15 writing, as a
status discrepancy — marked "Done" but with no dedicated `content/` folder. **Settled 2026-07-15:**
it is now real, scoped content-build work, not just a status correction — see
`docs/requirements/topic-shell-redesign.md`'s US-SH8 and backlog #31. It remains outside this
doc's ten topics (it's tracked in `topic-shell-redesign.md` instead, since it's bundled with the
shell/topic-page build), not because it turned out to be a pure shell-migration item after all.

Three other backlogged-but-unbuilt topics — UDF vs pandas UDF (#16), Delta/Iceberg (#20),
Tuning/debugging capstone (#21) — are **not** covered by the mockup and are unaffected by this
doc; their existing `spark-playbook-mvp.md` acceptance criteria stand as-is.

**Correction, 2026-07-15 (same day).** This doc originally also listed Memory management & spill
(#17) in the "unaffected" group above. That was wrong: `topics-content-spec.md` has a
"07 — Memory Management" section (unified memory manager, execution-vs-storage eviction under
contention) that was never turned into a user story anywhere. It is now covered below as
**US-C10**, in the four-already-scoped-topics group above (alongside Caching, Window Functions,
and Structured Streaming), not in this three-topic unaffected list. Backlog row #17 and the
backlog itself have been updated accordingly (new row #32).

## Problem statement

Six topics core to Spark interview depth — the DAG/laziness model, skew mitigation via salting,
executor sizing, checkpointing/lineage truncation, columnar vs row-oriented file formats, and
fault-tolerance/recomputation semantics — have no requirements coverage today; they exist only as
mockup content with no backlog entry. Separately, four already-backlogged topics (caching, window
functions, structured streaming, and memory management/spill) have sat as backlog entries pointing
at the MVP doc's original, fairly thin acceptance criteria, with no concrete
concept/notebook/self-check content to build against until now. This doc gives all ten topics the
same level of concrete, testable acceptance criteria the already-built topics (join-strategies,
bucketing, AQE) have, sourced from the mockup's extracted content rather than invented from
scratch.

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
- **No change to the three unaffected topics** (UDF/pandas UDF #16, Delta/Iceberg #20, tuning
  capstone #21) — out of scope, not touched by the mockup or this doc. (Memory management/spill
  #17 is **no longer** in this unaffected group — see the Source-and-relationship correction above
  and US-C10 below.)

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
  **Note, added 2026-07-17 during #34 implementation:** the literal `8`/`28g` vs. `5`/`12g`
  figures exceed this platform's own hard-enforced per-worker ranges (`app/lifecycle/renderer.py`
  caps cores/memory well below that), so the shipped notebook substitutes `executor-cores=4`/
  `executor-memory=2g` (fat) vs. `executor-cores=2`/`executor-memory=2g` (right-sized) at fixed
  total cluster capacity — same concept (concurrent tasks sharing one heap vs. more headroom per
  task), scaled to fit. Measured live against a real cluster (5 trials): **GC-time fraction**
  reliably favors the right-sized run (~20-30% relative reduction). **Wall-clock does not**
  reliably favor right-sizing at this project's small dev-cluster scale — added executor count
  brings real shuffle fan-out/coordination overhead that can outweigh the GC saving on a toy
  dataset. The notebook reports both numbers honestly rather than forcing a directional wall-clock
  assertion; this criterion is satisfied by "measurable" GC-time-fraction difference plus a
  measured (not necessarily favorable-direction) wall-clock number, not by a guaranteed wall-clock
  win. See `content/executor-tuning/concept.md` and `manifest.yaml` for the in-topic disclosure.
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
  architect confirmation — this doc does not assume it's solvable with a manifest entry alone.
- **How a learner safely kills a worker process from a self-serve local tool is not decided by
  this doc.** See Open Question 2 — this is a UX/safety design question, not a content question,
  and this doc's acceptance criteria describe the *pedagogical* target (what the learner should
  observe), not the mechanism for triggering the failure.

**US-C10 — Memory Management topic** *(supersedes/extends US-4.4 in `spark-playbook-mvp.md`;
added 2026-07-15, same day, as a correction — see the Source-and-relationship note above)*.
As a learner, I want a topic on the unified memory manager — execution memory and storage memory
sharing one region governed by `spark.memory.fraction`, and what happens to a cached DataFrame
when a memory-hungry shuffle competes for that same pool — so I understand that
`OutOfMemoryError`/spill is almost always a memory-tuning problem, not a "need a bigger cluster"
problem.

- *Given* the topic notebook, *when* I cache a ~3GB feature table with `.cache()` + `.count()` to
  force materialization, *then* the Storage tab / REST equivalent shows the DataFrame fully
  cached in memory (reusing the same materialization-confirmation pattern already established by
  US-C5's Caching topic).
- *Given* the cached DataFrame, *when* I then run a memory-hungry shuffle (a large sort or
  `groupBy`) that needs execution memory from the same shared pool, *then* the Executors tab /
  REST equivalent shows some previously-cached storage blocks evicted to make room — the notebook
  must capture this as a measured before/after state (cached fraction dropping), not just
  describe eviction in prose.
- *Given* the now-partially-evicted DataFrame, *when* I re-run the original cached query, *then*
  the topic notebook captures a partial-recompute signal — per `topics-content-spec.md`'s stated
  target ("3 of 8 partitions evicted"), some partitions return instantly (still cached) while
  others measurably recompute, rather than the query being uniformly fast or uniformly slow.
- *Given* this topic's relationship to the existing Caching & Persistence topic (US-C5), *when*
  the concept content is written, *then* it explicitly distinguishes storage memory (what US-C5
  covers — caching a DataFrame) from execution memory (shuffles/joins/sorts/aggregations) as two
  regions sharing one pool under `spark.memory.fraction`, with execution memory winning
  contention — not a restatement of US-C5's cache-timing content.
- *Given* US-4.4's existing spill/OOM-diagnosis criteria (a deliberately under-provisioned
  executor triggering OOM; spill metrics from a memory-constrained sort/aggregation), *when* this
  topic's notebook and self-check hypothesis are authored, *then* they explicitly connect back to
  those criteria — this doc does not relax or replace them, only adds the mockup's concrete
  eviction-under-contention walkthrough and self-check hypothesis on top.
- *Given* the Self-check tab, *when* the learner hypothesizes whether the cached DataFrame reads
  instantly or partially recomputes after the competing shuffle, and clicks Reveal, *then* the
  evidence needed is **per-executor storage-vs-execution memory usage**, live REST data
  (`memoryMetrics`/`memoryUsed` from `/api/v1/applications/<id>/executors` — the same endpoint
  family already reused for Executor Tuning's GC-time evidence — plus the RDD storage endpoint
  already used for US-C5's "fraction cached" criterion). This is executor-/RDD-level runtime data,
  not a plan-node concept: there is no distinctive plan shape that signals "eviction happened,"
  only a change in live memory-usage numbers between two reveals. Per Open Question 1's resolution
  (`docs/architecture/topic-shell-redesign.md` Decision A), this topic's need is the same *nature*
  of signal as **Executor Tuning (US-C3)** — a longitudinal/point-in-time executor-level runtime
  metric, not a static plan-structure fact — so it gets the same disposition: a reveal-time REST
  pull reusing `app_client.fetch_executors()` (and the existing RDD-storage fetch), through the
  `executor_metrics` manifest mechanism Decision A already introduces for US-C3. **This is not a
  plan-node matcher extension, and applying Decision A's already-settled dividing line here does
  not require a fresh architect round** — see Open Question 1 for the explicit disposition note.

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
   - **Memory Management (US-C10), added 2026-07-15 same day as a doc-gap correction — not part
     of the original architect consult, but disposed without a fresh architect round.** Applying
     Decision A's dividing line directly: per-executor storage-vs-execution memory usage is
     executor-level runtime data, not a plan-node fact, exactly matching Executor Tuning's
     disposition. It routes through the same reveal-time REST pull / `executor_metrics` manifest
     mechanism as US-C3, reusing `app_client.fetch_executors()` plus the RDD-storage fetch already
     used by US-C5's Caching topic. No new architect round is needed; this would only need to be
     revisited if the eviction/partial-recompute evidence turns out to need a capability beyond
     what `executor_metrics` and the existing storage endpoint already provide (no such gap is
     apparent from the mockup's stated target evidence). See backlog.md row #32.
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
   section on this.** Six of this doc's ten stories (all but Caching, Window Functions, Structured
   Streaming, and Memory Management, which were already backlogged) are exactly the kind of new
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
  implemented resource-ceiling check) — none of these ten topics' notebooks should require
  cluster configurations outside the existing supported ranges (workers 1–5, cores 1–4, memory
  1–8GB) unless a specific topic's exercise explicitly needs the existing "single worker scaled up
  to 8GB" skew/spill allowance already established for Phase 4 topics (Memory Management's ~3GB
  cached table plus a competing shuffle is the kind of exercise that may need this allowance,
  same as the original US-4.4 spill/OOM criteria already assumed).
- **Pages render through the shared shell** defined in `docs/requirements/topic-shell-redesign.md`
  — this doc assumes that shell exists or is being built concurrently; it does not specify any
  page-level UI itself.
