# Memory Management — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-17, against commit `c0f1ae0` on `worktree-feature+36-memory-management`
      ("feat(memory-management): add Memory Management curriculum topic (US-C10)"), plus
      three live-run-driven fixes to `content/memory-management/notebook.ipynb` made and
      re-verified during this same pass (see "Defects found and fixed" below) — no other file
      touched.
Scope: US-C10 (`docs/requirements/curriculum-topics-2026-07.md`), all 5 acceptance criteria (4
       cache/eviction/recompute criteria + the US-4.4 spill/OOM connection), plus the
       `executor_metrics` annotation-manifest mechanism (Decision A,
       `docs/architecture/topic-shell-redesign.md`) shared with issue #34. Verified against a real
       1-worker/4-core/8GB cluster spawned through the app's own routes (this topic's own
       `cluster_defaults`), not just re-reading the diff.

## Method

**Unit suite**, re-run before and after this pass: `py -3.9 -m pytest tests/unit -q` → **297
passed**, both times (no unit-test changes made this pass — see "Unit test coverage review"
below).

**Coverage review** (before touching the live cluster): read `tests/unit/test_manifest.py`'s
`TestValidManifestExecutorMetrics`, `tests/unit/test_engine.py`'s
`TestSpotlightExecutorMetrics`, and `tests/unit/test_annotation_routes.py`'s
`TestRevealExecutorMetrics`. These already cover exactly the edge cases this task called out:
parse validation (valid parse with mixed `spotlight`, defaults to `[]` when the section is
absent, non-list raises, missing `key` raises), spotlight extraction (declared keys only, a
missing key in the REST payload yields `value: None` rather than KeyError, no declared metrics
yields an empty dict), and the Reveal route's gating contract (a topic **without**
`executor_metrics` declared never calls `fetch_executors` at all; a topic **with** it declared
renders the spotlighted table; an unreachable REST endpoint degrades to a clear "Could not reach"
message, the identical contract `stage_metrics` already uses). **No gaps found; no tests added.**

**Live cluster.** `docker ps --filter "name=spark-"` and a port-8000 check were both empty
immediately before starting. The FastAPI app was started fresh (`py -3.9 -m uvicorn app.main:app
--host 127.0.0.1 --port 8000`) and a cluster was spawned through the app's own route (`POST
/topics/memory-management/spawn`, this topic's own `cluster_defaults`: 1 worker, 4 cores, 8GB,
200 shuffle partitions, AQE off) — never `compose/cli.py` or `docker compose` directly. `docker
ps` confirmed `spark-master`, `spark-worker-1`, and `spark-driver` up, with `spark-driver`
publishing `:4040-4042` and `:8888` (JupyterLab) as expected.

**Notebook execution.** Same technique as the Serialization Formats pass: drove the real
JupyterLab kernel REST/WebSocket API (`POST /api/kernels` + the kernel's `channels` websocket)
directly, executing the notebook's 8 code cells in file order against real kernels, rather than
opening the file through the Jupyter UI. The notebook required **3 real fixes**, each found by
actually running it and each re-verified live after the fix — see below. One operational
incident, unrelated to this topic's own code, is also recorded.

## Operational incident: cross-worktree cluster collision (not a defect in this topic)

Partway through the first live run, all three containers were killed mid-job (`docker events`
showed a clean `SIGTERM`/`stop`/`die` sequence, `exitCode=0`/`143`, not an OOM-kill) while a
competing shuffle was running. `compose/cli.py`'s `PROJECT_NAME = "sparkpb"` is a single fixed
Docker Compose project name shared across **every** worktree in this repo — a `spawn` or
`teardown` request against port 8000 from *any* concurrently-running worktree's own app instance
runs `docker compose -p sparkpb down`, which tears down whichever containers currently answer to
that project name, regardless of which worktree rendered them. This session's task description
itself named another active worktree (`.claude/worktrees/issue-34-executor-tuning`) working
concurrently on a sibling issue, which is the most likely cause. This is a real, reproducible
cross-worktree risk for any concurrent QA/dev session in this repo, but it is infra/tooling-level
(`compose/cli.py`), not a defect in `content/memory-management/` — **not fixed here** (out of
this topic's scope), just flagged. The run was simply respawned and redone; no cluster state was
lost that mattered.

## Defects found and fixed (all in `content/memory-management/notebook.ipynb`)

**1. `spark.executor.memory` was never set, so the ~3GB feature table never actually cached in
memory at all (blocks criteria 2 and 3 entirely).** This project's compose template
deliberately leaves `spark.executor.memory` unset (defaults to Spark's 1g) — `caching-persistence`'s
own notebook comments on this and *exploits* it (sizes its table to deliberately exceed that tiny
pool). Memory Management needs the opposite: a pool big enough to hold the ~3GB table **fully in
memory first**, so a competing shuffle can then genuinely evict part of it. Without this, the
first live run showed `Memory used: 0.00 GB` on the freshly cached table (real evidence, not a
display bug: `/storage/rdd`'s `memoryUsed` was `0`, `storageLevel: "Disk Memory Deserialized 1x
Replicated"`, `diskUsed: 1880092877` — the whole table landed on disk immediately because the
executor's actual unified memory pool was only ~455MB). `fraction_cached` (partitions-present,
memory-or-disk) still read 8/8 = 100%, silently masking that nothing was resident in memory to
evict. **Fix:** the notebook's own `SparkSession.builder` now sets
`.config("spark.executor.memory", "7g")` (sized against this topic's own 8GB `worker_memory_gb`,
leaving ~1GB worker/OS headroom) — a topic-local notebook change, not a change to the shared
compose template other topics depend on.

**2. `latest_stage_task_durations()` picked the wrong stage, collapsing every per-partition
reading to one entry.** `feature_df.agg(F.sum("c0")).collect()` compiles to a partial-aggregate
stage over all 8 partitions *followed by* a single-task final-reduce stage. The helper's "most
recently submitted stage" (`max(stages, key=lambda s: s["stageId"])`) always landed on that
trivial 1-task reduce stage, so both the baseline and rerun measurements returned a dict with
exactly one entry (`{0: 55}` etc.) instead of eight — silently defeating the whole
partial-recompute measurement this criterion needs. **Fix:** the helper now takes an
`expected_tasks` parameter and selects the most recent stage whose `numTasks == expected_tasks`,
not simply the most recent by `stageId`.

**3. The eviction-classification threshold ("recomputed if duration > baseline max × 3") was too
coarse.** Once (1) and (2) were fixed, real per-partition durations came back correctly (8
entries), but baseline readings themselves varied several-fold across partitions on their own
(JIT warmup / page-cache noise, e.g. `[184, 186, 187, 188, 238, 240, 240, 241]` all while fully
cached) — a single global-max-derived ceiling is fragile against that noise. **Fix:** classify
each partition against *that same partition's own* baseline reading (`max(d * 3, 50)` per index,
with a 50ms floor against tiny-baseline hair-triggering), not the global max.

All three fixes are notebook-local (`content/memory-management/notebook.ipynb` only); no
`app/` code, other topic, or shared compose template was touched. Diff: `git diff --stat
content/memory-management/notebook.ipynb` → 1 file changed, 71 insertions(+), 20 deletions(-).

## US-C10, criterion 1 — `.cache()` + `.count()` shows the ~3GB table fully cached

**PASS**, verified live (post-fix). Cell 3 (`feature_df.cache()` then `.count()`, 24M rows × 16
double columns across 8 partitions):

```
feature_df.count() = 24000000 rows across 8 partitions
Fraction cached: 8/8 = 100%
Memory used: 3.10 GB
```

`/storage/rdd`'s `numCachedPartitions/numPartitions` = 8/8, and `memoryUsed` is real, nonzero,
and consistent with the table's intended ~3GB size — confirming genuine in-memory
materialization, not a disk fallback (see defect 1 above for what this looked like *before* the
fix).

**Criterion 1: PASS.**

## US-C10, criterion 2 — competing shuffle evicts cached storage blocks (measured before/after)

**PASS**, verified live. Cell 7 (a 40M-row sort with only 4 shuffle partitions, forcing large
per-partition execution-memory buffers on the same 1-worker cluster) reported:

```
Executor storage memoryUsed before competing shuffle: 3101.8 MB
Executor storage memoryUsed after competing shuffle:  1938.7 MB
Fraction cached before: 100%  after: 100%
Measured eviction from the competing shuffle: True
```

A real ~1163 MB (~37.5%) drop in executor storage `memoryUsed`, sourced live from
`/api/v1/applications/<id>/executors` — genuine eviction of cached storage blocks to make room
for the shuffle's execution memory demand (`fraction_cached` itself stays at 100% because the
evicted blocks fall back to disk rather than disappearing, which is expected `MEMORY_AND_DISK`
behavior — the real evidence for eviction is `memoryUsed`, not the partition-presence count, and
the notebook's own `evicted_measured` check already ORs both signals for exactly this reason).
Independently cross-checked against the Self-check tab (criterion 5 below): two live Reveals
showed the identical `3102074473` → `1938928925` byte values.

**Criterion 2: PASS.**

## US-C10, criterion 3 — re-run shows a genuine partial-recompute signal, not hardcoded

**PASS**, verified live. Cell 9 (re-running the original cached query after the competing
shuffle, comparing each partition's duration against *that partition's own* fully-cached baseline
from cell 5):

```
Per-partition baseline ceilings (ms): {0: 720, 1: 720, 2: 714, 3: 723, 4: 564, 5: 561, 6: 558, 7: 552}
Rerun per-partition durations (ms): {0: 733, 1: 742, 2: 49, 3: 733, 4: 42, 5: 76, 6: 44, 7: 38}
Partitions still cached (fast): [2, 4, 5, 6, 7]
Partitions recomputed (slow): [0, 1, 3]
Measured: 3 of 8 partitions evicted and recomputed.
```

A clear bimodal split — partitions 0/1/3 at 733-742ms (recomputed from source) vs. the other five
at 38-76ms (still served from cache) — genuinely measured this run via the fixed stage-selection
and per-partition threshold logic (defects 2/3 above), not assumed or hardcoded. This run's
measured fraction (3 of 8) happens to match `topics-content-spec.md`'s own worked example, but
that's this run's real result, not an assumption baked into the notebook (the requirements
doc explicitly warns against hardcoding this number, and the code computes it fresh every run).

**Criterion 3: PASS.**

## US-C10, criterion 4 — distinguishes storage memory (US-C5) from execution memory, one shared pool

**PASS**, by inspection of `content/memory-management/concept.md` and the notebook's own
markdown cells (US-2 in the concept doc explicitly contrasts "storage memory (what caching
holds)" against "execution memory (shuffles/sorts/joins)" as two regions of one
`spark.memory.fraction`-governed pool, with execution winning contention) — this is prose/content
correctness, not a live-measurable claim on its own, but it is directly corroborated by criteria
2/3 above: the live numbers show execution memory (the competing sort) taking priority over
already-cached storage blocks, which is exactly the relationship the concept content describes.

**Criterion 4: PASS.**

## US-4.4 connection — spill metrics and a real OOM (not relaxed or replaced by this topic)

**Spill: PASS**, verified live. Cell 11 (a 60M-row sort with only 2 shuffle partitions,
memory-constrained on purpose) reported real, nonzero spill on both runs performed this pass:

```
Stage 11: memoryBytesSpilled=1073741568, diskBytesSpilled=471529448
```

(~1.07GB memory-spilled, ~472MB disk-spilled — a genuine memory-constrained-aggregation spill,
independently cross-checkable in the Self-check tab's stage-metrics table, which the manifest
already spotlights both `memoryBytesSpilled` and `diskBytesSpilled`.)

**OOM: PASS, with one nuance worth flagging.** Cell 13 (`.hint("broadcast")` forcing a broadcast
build of the ~3GB `feature_df`) reliably reproduced, on the one full run this pass exercised it:

```
Broadcast join failed, as expected. Raw driver-side error (read this, don't skip it):

An error occurred while calling o360.count.
: java.util.concurrent.ExecutionException: org.apache.spark.util.SparkFatalException: org.apache.spark.SparkException: Not enough memory to build and broadcast the table to all worker nodes. As a workaround, you can either disable broadcast by setting spark.sql.autoBroadcastJoinThreshold to -1 or increase the spark driver memory by setting spark.driver.memory to a higher value.
 Diagnosis: execution memory (broadcast build/collection on driver or executor) was exhausted -- read which side the raw error above names.
```

This is a real, deterministic failure — Spark's `BroadcastExchangeExec` catches an actual
`OutOfMemoryError` during broadcast collection and re-wraps it in exactly this
`SparkException`/"Not enough memory..." message (this is Spark's own documented behavior, not an
artifact of this notebook); the notebook's own diagnosis check (`"OutOfMemoryError" in message or
"Not enough memory" in message`) was written anticipating precisely this wrapped form and matched
correctly. **The nuance:** the failure happens **driver-side** (`spark.driver.memory=2g` is fixed
project-wide, unlike `worker_memory_gb`, and broadcast collection happens on the driver), whereas
US-4.4's literal wording says "a deliberately under-provisioned **executor** triggering
`OutOfMemoryError`". Given this project's driver memory is a fixed, non-topic-controlled setting,
reliably forcing an *executor*-side OOM from a broadcast specifically is not achievable from this
notebook alone — but the observed failure still satisfies the criterion's pedagogical intent (a
real, diagnosable `OutOfMemoryError` tied to a memory-sizing decision, read and reported rather
than pre-diagnosed) even though it lands on the driver rather than an executor. Flagging this as
a documented, deliberate scope note rather than a defect — no fix applied, since forcing it
executor-side would need a different, more elaborate exercise (e.g., an executor-only memory
pressure test) that isn't what this cell was designed to teach.

**US-4.4 connection: PASS** (spill unconditionally; OOM PASS with the driver-vs-executor nuance
above flagged for human review).

## Self-check Reveal flow — live, across two separate Reveals

**PASS**, verified live by actually clicking through the flow (direct HTTP calls to the same
routes the Reveal button hits), confirming it changes across two reveals as US-C10's own
acceptance criterion requires (not just the notebook's own prints).

- **Reveal 1** (`POST /topics/memory-management/annotation/reveal`, taken right after re-caching
  `feature_df`, before the competing shuffle): rendered a "Per-executor memory metrics" table with
  executor `0`: `memoryUsed=3102074473`, `maxMemory=4320971980`.
- **Reveal 2** (same route, taken right after re-running the competing shuffle): rendered the same
  table with executor `0`: `memoryUsed=1938928925`, `maxMemory=4320971980` (`maxMemory` unchanged,
  confirming this is the same executor's unified pool, just less of it in use for storage).

The two reveals' `memoryUsed` values (`3102074473` → `1938928925`) match the notebook's own live
printout (`3102.1 MB` → `1938.9 MB`) exactly, confirming the `executor_metrics` reveal-time REST
pull (Decision A) surfaces real, changing evidence — not a static snapshot — across the
before/after Reveal clicks this criterion describes.

**Self-check Reveal flow: PASS.**

## Teardown

```
DELETE /api/kernels/<kernel_id> (all kernels created this pass) → 204 each
POST /topics/memory-management/teardown                          → 200
docker ps -a --filter "name=spark-"                               → (empty)
uvicorn process (port 8000)                                       → stopped, confirmed no listener on :8000
py -3.9 -m pytest tests/unit -q                                    → 297 passed
```

**Notebook cleanliness check** (all 8 code cells were executed directly against JupyterLab
kernels via the REST/WebSocket API, never through the Jupyter UI itself, so the file on disk only
carries the deliberate source edits described above, never live-run outputs):

```
python -c "... execution_count/outputs check over all code cells ..." → bad cells: []
git status --short                                                     → " M content/memory-management/notebook.ipynb"
                                                                          (source-only diff; no other file touched)
```

All 8 code cells confirmed at `execution_count: null` with empty `outputs: []`.

## Overall recommendation

**All of US-C10's acceptance criteria PASS, live-verified against a real 1-worker/8GB cluster**,
after fixing three real defects found only by actually running the notebook (a missing
`spark.executor.memory` config that silently defeated the whole in-memory-caching premise, a
wrong-stage-picked helper that collapsed all 8 partitions' timing into one, and an
eviction-classification threshold too coarse against real run-to-run noise) — none of which were
visible from a static read of the diff. Post-fix, every criterion's live numbers are genuinely
measured, not hardcoded: 100% cached / 3.10GB resident (criterion 1), a real ~37.5%
`memoryUsed` drop from a competing shuffle (criterion 2), a genuinely bimodal 3-of-8
partial-recompute split (criterion 3), nonzero spill (~1.07GB memory / ~472MB disk) and a
reliable, real `OutOfMemoryError` (US-4.4 connection, with the driver-vs-executor nuance flagged
above for human awareness), and a Self-check Reveal flow that visibly changes across two live
Reveals with numbers matching the notebook's own output exactly.

Unit test coverage for the shared `executor_metrics` mechanism (manifest validation, engine
spotlighting, and the Reveal route's gating contract) was reviewed and found already adequate —
no gaps, no tests added.

**One open item for a human decision, not a blocker:** the OOM cell's failure lands on the
driver process (fixed `spark.driver.memory=2g`), not an executor, a nuance vs. US-4.4's literal
"under-provisioned executor" wording — flagged above, not fixed, since the fix would require a
different exercise than this cell teaches. **One operational, non-topic finding:** this repo's
shared, fixed `sparkpb` Docker Compose project name (`compose/cli.py`) is a real cross-worktree
collision risk when multiple worktrees run live acceptance passes concurrently — flagged for
awareness, out of this topic's scope to fix.

This is a recommendation, not an approval — per this project's Definition of Done, please review
this report (particularly the driver-vs-executor OOM nuance) and give explicit sign-off (or flag
anything that needs a second look) before issue #36 is considered done.

## Human sign-off

**Given, 2026-07-17.** All 5 US-C10 acceptance criteria approved as PASS; issue #36 considered done.
The driver-vs-executor OOM nuance is accepted as documented, not a defect. The cross-worktree Docker
Compose collision finding is accepted as a real, separate operational issue — tracked independently
(not blocking this topic).
