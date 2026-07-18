# Skew & Salting — Acceptance Report

Status: Re-validated after redesign, PASS on all 4 criteria — see "Re-validation
(redesigned mechanism)" section below for the current pass. The original run below
(FAIL on 2/4) is kept as history: it drove the architect redesign
(`docs/architecture/skew-salting-demo-mechanism.md`, plus its same-day
"Salted-side assert — physics fix" amendment) and the developer's
`collect_list`-based reimplementation that this re-validation confirms.

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-18, against the developer's uncommitted working tree for issue #35
      (`content/skew-salting/{manifest.yaml,concept.md,notebook.ipynb}` new, `tests/unit/test_manifest.py`
      and `tests/unit/test_topics_loader.py` modified), plus a live-run-driven fix to
      `content/skew-salting/notebook.ipynb` made and re-verified during this same pass (see "Defect found"
      below) — no other file touched. Not yet committed; commit happens at a later pipeline stage.
Scope: US-C2 (`docs/requirements/curriculum-topics-2026-07.md`, ~lines 127-145), all 4 acceptance
       criteria, verified against a real 3-worker cluster spawned through the app's own routes
       (this topic's own `cluster_defaults`), not just re-reading the diff.

## Method

**Unit suite**, run before and after this pass: `py -m pytest tests/unit -q` → **324 passed**, both
times (the developer's 6 new tests — 3 in `TestLoadRealSkewSaltingManifest`, 4 in
`TestLoadRealSkewSaltingTopic` — already present and passing; no test gaps found worth adding: the
manifest/topic-loader coverage shape matches the established `executor-tuning`/`memory-management`
precedent exactly, and this topic adds no new route/engine surface to cover).

**Live cluster.** `docker ps --filter "name=spark-"` was empty before starting. The FastAPI app was
already running on port 8000 (pre-existing dev session); a cluster was spawned through the app's own
route (`POST /topics/skew-salting/spawn`, this topic's own `cluster_defaults`: 3 workers, 2 cores, 4GB,
200 shuffle partitions, AQE off) — never `compose/cli.py` or `docker compose` directly. `docker ps`
confirmed `spark-master`, `spark-worker-1/2/3`, and `spark-driver` all up.

**Notebook execution.** Same technique as prior topics' acceptance passes: a script drove the real
JupyterLab kernel REST/WebSocket API (`POST /api/kernels` + the kernel's `channels` websocket) to
execute `content/skew-salting/notebook.ipynb`'s 5 code cells in file order against real kernels. The
notebook required **1 fix**, found by actually running it, confirmed structural (not tunable) across
3 independent live trials, and re-verified after the fix — see below.

## Defect found: `groupBy(key).count()` cannot show a real shuffle-read-bytes straggler

**The notebook's own hard assertions failed on the very first live run.** Cell `b4bcc460`'s
`assert straggler["shuffleReadBytes"] >= 2 * median_shuffle_bytes` raised `AssertionError` and stopped
the kernel before the checkpoint was written — trial 1 numbers: straggler 1992 bytes vs. median 1728
bytes (1.15x, not the required >=2x). Duration passed that trial (56ms vs. 18ms median, 3.11x).

Two more independent live trials (fresh kernel, fresh dataset each time, same cluster) were run to
check whether this was a one-off:

| trial | un-salted duration ratio (straggler/median) | un-salted shuffle-bytes ratio | salted duration ratio |
|---|---|---|---|
| 1 | 3.11x (56ms/18ms) | 1.15x (1992/1728) | n/a (assert crashed the run) |
| 2 | 4.27x (64ms/15ms) | **1.00x (1728/1728, exact tie)** | 3.77x (barely different) |
| 3 | 3.28x (59ms/18ms) | **0x (0 bytes vs. median 1728)** | 3.67x (worse than un-salted) |

**Root cause, confirmed structural, not a `FACT_ROWS`/`NUM_INPUT_PARTITIONS` tuning issue** (the
developer's own flagged risk in the manifest comment): `groupBy(key).count()` always compiles to
Spark's two-phase `HashAggregate(partial) -> Exchange -> HashAggregate(final)` plan. The partial
aggregate on each of the 24 mapper tasks combines *all* of that mapper's rows for a key into a single
`(key, count)` row *before* the shuffle — so what actually crosses the shuffle boundary for the hot key
is bounded by (distinct-key-count × mapper-count), never by the hot key's raw row volume. This holds at
any data scale (the shuffle-read-bytes median was identically 1728 across trials with different row
counts), so unlike `executor-tuning`'s issue #37, this is not fixable by adjusting constants. The
observed duration spread (a smooth ~4ms–65ms continuum across all 200 tasks, present in both salted and
un-salted runs in nearly the same shape) is more consistent with task-scheduling/JIT-warmup noise at
this tiny job's millisecond scale than a genuine per-key skew signal — it doesn't reliably flatten after
salting (trial 3's salted ratio was *worse* than un-salted).

**Fix applied this pass:** softened the two now-provably-unreliable hard `assert`s (shuffle-bytes
threshold in cell `b4bcc460`, `salted_ratio < 2.0` in cell `6eeb8f2e`) to informational `print()`
warnings, so a learner's run isn't crash-blocked mid-cell the way trial 1 was — same pattern as issue
#37's fix on `executor-tuning`. **This does not fix the underlying pedagogical claim** — criteria 1 and
2 below are not actually demonstrated by the current exercise design. **Filed as issue
[#46](https://github.com/hoanghaithanh/Spark-Playbook/issues/46)** (`bug`, `from:acceptance`,
Sprint 6 milestone) with the full repro, all 3 trials' numbers, and a suggested redesign path
(`F.collect_list` or RDD `groupByKey()` for the "before" demonstration, so the shuffle payload actually
scales with row count) for a developer/architect follow-up pass. Re-ran the fixed notebook end-to-end
after the edit — all 5 cells completed `ok`, no crash, informational NOTE lines printed as designed.

## US-C2, criterion 1 — straggler task with visibly larger duration and shuffle-read bytes

**FAIL** on the shuffle-read-bytes half. Real Stages-tab/REST task-list data across 3 live trials:
shuffle-read bytes for the max-duration task were 1.15x, 1.00x (exact tie), and 0x the median — never
close to "visibly larger." Duration alone did show a real spread each trial (3.11x, 4.27x, 3.28x), but
per the root-cause analysis above this spread is not reliably attributable to `HOT_KEY`'s skew (it
doesn't correlate with a specific known-hot task and doesn't respond to salting). **Criterion 1 as
literally stated (both duration *and* shuffle-read bytes visibly larger) is not met.**

## US-C2, criterion 2 — salting flattens the per-task duration spread

**FAIL.** Real numbers: trial 2's salted ratio (3.77x) barely differed from un-salted (4.27x); trial
3's salted ratio (3.67x) was *worse* than un-salted (3.28x) — salting did not reliably flatten anything,
consistent with the duration spread not being a genuine skew signal to begin with. The **correctness**
half of this step did hold in every trial — `mismatches == 0` (salted-then-stripped counts exactly
matched the un-salted counts) — salting never changes the answer, only how it's computed, which is
itself worth confirming, but it's not the AC's actual claim (the flattening).

## US-C2, criterion 3 — concept content distinguishes salting from AQE's automatic skew-join splitting

**PASS.** `content/skew-salting/concept.md`'s "Why it matters" section states this explicitly and in
detail: `spark.sql.adaptive.skewJoin.enabled` only works because a **join** has two sides to rebalance
against, and "[a] skewed `groupBy(key).count()` (or any single-sided aggregation) has no second side for
AQE's skew-join split to work with, so `spark.sql.adaptive.skewJoin.enabled` cannot help here regardless
of whether AQE is on." It further states "this topic is deliberately **not** a restatement of the AQE
topic's automatic skew-join splitting." This is a real, substantive distinction, not a shared paragraph
with `content/aqe/concept.md` (confirmed by reading both files side by side).

## US-C2, criterion 4 — self-check Reveal sourced from existing REST data, no new engine capability

**PASS**, verified against the actual diff and live end-to-end, not just re-reading the manifest
comment:

- `git diff -- app/annotation/plan_parser.py app/annotation/engine.py` is empty against `main` —
  untouched.
- `content/skew-salting/manifest.yaml` declares no `plan_nodes` section at all (confirmed by
  `test_no_plan_nodes` in `tests/unit/test_manifest.py`, and independently by reading the file), only
  `stage_metrics` (with `shuffleReadBytes` spotlighted) and `task_duration_quantiles: true` — matching
  the manifest's own comment explaining a skewed and salted `groupBy` compile to the same plan shape.
- Live `POST /topics/skew-salting/annotation/reveal` (after a real checkpoint from the notebook run)
  rendered a genuine plan-node list (`unknown / unannotated`, as expected with no `plan_nodes` rules)
  and a real 17-row stage table, with `shuffleReadBytes` rendered under the `spotlight` CSS class and
  real per-task duration quantiles (min/p25/median/p75/max) alongside it — e.g. stage 9:
  `shuffleReadBytes=241968, numTasks=200, duration p25=10, median=15, p75=28, max=64`. This is the
  pre-existing `stage_metrics`/`task_duration_quantiles` mechanism (issues #8/US-2.2) doing all the work
  — no new annotation-engine capability, confirmed live not just in theory.

## Coverage review

Read the developer's new tests: `TestLoadRealSkewSaltingManifest` (3 tests: `shuffleReadBytes`
spotlighted, `plan_nodes == []`, `task_duration_quantiles is True`) and `TestLoadRealSkewSaltingTopic`
(4 tests: manifest fields incl. `order=11`/`worker_count=3`/`aqe_enabled=False`, concept markdown
renders and mentions `salt`/`skewJoin`, notebook path resolves, topic appears in `list_topics()`). This
is the same coverage shape as `TestLoadRealExecutorTuningManifest`/`TestLoadRealExecutorTuningTopic` —
appropriate for a topic that (deliberately) adds no new route/engine surface. **No gaps found; no tests
added** — this topic's only genuine risk was the live notebook behavior, which isn't unit-testable
(it depends on real Spark scheduling/shuffle mechanics), and was instead exercised directly against a
live cluster in this pass.

## Cleanup confirmation

- Notebook reset to clean source form by hand after the live runs (the fix changed cell *source*, so
  `git checkout` alone was not used — verified directly via JSON inspection instead):
  ```
  ee3c40eb execution_count= None outputs= []
  f87515c3 execution_count= None outputs= []
  b4bcc460 execution_count= None outputs= []
  e1ff4f35 execution_count= None outputs= []
  6eeb8f2e execution_count= None outputs= []
  ```
  All 5 code cells confirmed `execution_count: null`, `outputs: []`. `content/skew-salting/notebook.ipynb`
  is untracked (new file), so there is no pre-existing committed baseline to diff against; this is its
  intended committed state going forward.
- `py -m pytest tests/unit -q` → **324 passed**, unchanged before/after this pass (the notebook-content
  fix touches no test-covered code path).
- Both notebook kernels (the original run's and the fixed re-run's) deleted via
  `DELETE /api/kernels/<id>`; `GET /api/kernels` confirmed empty before teardown.
- `POST /topics/skew-salting/teardown` issued; `docker ps --filter "name=spark-"` confirmed empty
  afterward.

## Blockers / gaps for human attention

1. **Issue [#46](https://github.com/hoanghaithanh/Spark-Playbook/issues/46) (filed, real blocker for
   this story's Definition of Done):** criteria 1 and 2 of US-C2 are not actually demonstrated by the
   current notebook design — `groupBy(key).count()`'s automatic partial aggregation structurally
   prevents shuffle-read bytes (and, empirically, task duration too) from reflecting the intended row-
   count skew, confirmed across 3 independent live trials, not a one-off. This needs a developer/
   architect redesign pass (e.g. `F.collect_list` or an RDD `groupByKey()`-based "before" demonstration)
   before US-C2 can be called fully done — softening the hard assertions (this pass's fix) only stops
   the notebook from crashing a learner's session, it doesn't fix the pedagogy.
2. Criteria 3 and 4 (concept content, self-check plumbing) are solid — PASS on both, verified live.

## Recommendation

This is a **recommendation, not final sign-off** — and it is **not a clean pass**: 2 of 4 acceptance
criteria (1 and 2, the core "see the straggler, see it flatten" claims) fail against real measured
numbers on a real cluster, across 3 independent trials. The human should decide whether issue #46's
redesign needs to land before Sprint 6 close-out for #35, or whether this ships with an honest
documented gap (softened notebook assertions + filed issue) for a follow-up sprint — but it should not
be marked done as currently designed.

---

## Re-validation (redesigned mechanism)

Date: 2026-07-18, second pass, against the same uncommitted working tree (no new files/diffs beyond
this doc's own edit — `git status` before and after this pass shows only `tests/unit/test_manifest.py`
and `tests/unit/test_topics_loader.py` modified plus `content/skew-salting/`,
`docs/architecture/skew-salting-demo-mechanism.md`, `docs/qa/skew-salting-acceptance.md` untracked,
identical to the original pass's file set).

**Trigger:** the architect's redesign (`docs/architecture/skew-salting-demo-mechanism.md` + its
same-day "Salted-side assert — physics fix" amendment) replaced `groupBy(key).count()` with
`groupBy(key).agg(F.collect_list("amount"))`, and the AC1/AC2 wording shifted accordingly (un-salted
straggler `>= 2x median` shuffle bytes; salted straggler load cut `>= 3x` vs. the un-salted straggler,
not "flattens to <2x across all tasks" — the amendment proved that's structurally impossible at 10
salt buckets / 200 shuffle partitions). A developer subagent reported a standalone-script live
verification (154x / ~6.9x) but that was against a scratch `spark-submit`, not the app's own routes or
the real notebook/kernel/Reveal path — this pass independently re-verifies through the actual running
system.

### Method

- **Cluster**: spawned via the app's own route, `POST /topics/skew-salting/spawn` with this topic's
  own `cluster_defaults` (3 workers, 2 cores, 4GB, 200 shuffle partitions, AQE off) — confirmed via
  `docker ps --filter "name=spark-"` showing `spark-master`, `spark-worker-1/2/3`, `spark-driver`.
  (One respawn was needed mid-run: the containers unexpectedly disappeared between trial 1 and trial 2
  — `docker ps -a` showed zero containers, not a crash-with-exit-code; re-spawned via the same route
  and continued. Not investigated further as an app defect since it self-recovered and is orthogonal
  to this story's acceptance criteria, but flagged here for visibility.)
- **Notebook execution**: the real JupyterLab kernel REST/WebSocket API (`POST /api/kernels` +
  `ws://.../api/kernels/<id>/channels`), same technique as the original pass, driving
  `content/skew-salting/notebook.ipynb`'s 5 code cells in file order against a real kernel — not a
  standalone script. Run 3 independent trials (fresh kernel each time, 2 against the same cluster
  instance, 1 against the respawned one) to check for flakiness the way the original pass's 3 trials
  did.
- **Self-check Reveal**: after trial 1's checkpoint, `POST /topics/skew-salting/annotation/reveal`
  hit directly against the live app.
- Kernels deleted via `DELETE /api/kernels/<id>` after each trial; `GET /api/kernels` confirmed empty
  before the next trial and before final teardown.

### Live numbers, all 3 trials

| trial | un-salted straggler shuffle bytes | median | ratio | salted max shuffle bytes | reduction vs. un-salted straggler | mismatches |
|---|---|---|---|---|---|---|
| 1 | 2,978,921 | 19,320 | **154.19x** | 432,631 | **6.9x** | 0 |
| 2 | 2,978,921 | 19,320 | **154.19x** | 432,931 | **6.9x** | 0 |
| 3 | 2,978,921 | 19,320 | **154.19x** | 431,609 | **6.9x** | 0 |

The un-salted number is exactly reproducible trial-to-trial (the dataset generation is deterministic —
`make_row()` uses `i % 5 < 3`, no RNG); the salted max varies only slightly (431,609–432,931, ~0.3%)
because only the salt-bucket assignment uses `F.rand()`, and with 10 buckets hashed into 200 fixed
partitions the max-bucket-per-partition collision count is stable across runs, matching the
architect's amendment's own variance analysis. All 5 notebook cells (`ee3c40eb`, `f87515c3`,
`b4bcc460`, `e1ff4f35`, `6eeb8f2e`) completed `ok` with no `AssertionError` in every trial — the
notebook's own hard asserts (both now on shuffle-read bytes, not the previous duration/count-based
ones) passed live, not just informational `print()`s.

### Criterion 1 — un-salted straggler, hard `>= 2x median` assert

**PASS, comfortable margin.** 154.19x vs. a 2x bar — over 75x the required margin, reproduced
identically across all 3 trials. Not knife-edge in any sense; this is not a borderline pass. Matches
the developer's standalone-script number (154x) exactly, now independently confirmed through the real
app + real notebook + real kernel path.

### Criterion 2 — salting cuts the straggler's load `>= 3x`, plus byte-identical correctness

**PASS.** 6.9x reduction across all 3 trials (bar: 3x) — a ~2.3x margin over the bar, consistent and
non-flaky, matching the developer's reported number exactly. `assert mismatches == 0` held in every
trial: `sort_array`/`flatten`-merged salted-then-corrected values byte-for-byte match the un-salted
`collect_list` result. This is the corrected claim per the architect's amendment (load reduction, not
global flattening) — confirmed the notebook does *not* assert or claim "<2x across all tasks"; cell
`e1ff4f35`'s own comment explicitly documents why that would be wrong at N=10/P=200, and the printed
`salted_max/median` ratio (~22.3–22.4x across trials) is shown as expected corroboration, not a failure.

### Criterion 3 — concept.md: collect_list vs. map-side-combinable, corrected salting outcome, AQE distinction

**PASS**, re-checked against the current file (not the stale pre-redesign version):
- (a) `content/skew-salting/concept.md` "What it is" section explicitly contrasts `collect_list`
  (not map-side-combinable — "there's no way to reduce 'every value seen' to a fixed-size partial
  without losing values") against `count()`/`sum()` (map-side-combinable, skew absorbed before the
  shuffle) — present and correct.
- (b) The "What to look for" section was actually updated, not left saying "flattens": it now reads
  "does *not* flatten the entire task-duration/shuffle-bytes distribution to one even level... the
  reduction in the straggler's own load is what collapses the wall-clock, and it scales roughly with
  however many sub-keys you salt into" — matches the corrected claim, no stale "flattens" language
  found anywhere in the file (checked via direct read, not just grep for the word).
- (c) AQE distinction paragraph is untouched from the original pass and still PASSes — re-confirmed by
  re-reading it this pass: single-sided `groupBy` has no second side for
  `spark.sql.adaptive.skewJoin.enabled` to rebalance against, explicitly stated as distinct from this
  topic. No regression.

### Criterion 4 — Self-check Reveal still sourced from existing REST data, no new engine capability

**PASS**, re-confirmed both statically and live:
- `git diff -- app/annotation/plan_parser.py app/annotation/engine.py` against `main`: empty.
- `content/skew-salting/manifest.yaml` still declares no `plan_nodes:` key (only appears in the file's
  explanatory comment, confirmed by direct read) — only `stage_metrics` (`shuffleReadBytes` spotlight)
  and `task_duration_quantiles: true`.
- Live `POST /topics/skew-salting/annotation/reveal` against trial 1's checkpoint rendered a real
  stage-metrics table (16 `spotlight`-classed cells, `numTasks` column, p25/median/p75 quantile columns
  present) with no `manifest_error` and no stale-checkpoint warning — the pre-existing
  `stage_metrics`/`task_duration_quantiles` mechanism doing all the work, confirmed live.

### Unit suite

`py -m pytest tests/unit -q` → **324 passed** — same count as the original pass and unchanged by this
one (no test-covered code path touched; this topic's only real risk is live notebook/cluster behavior,
exercised directly above, consistent with the original pass's coverage-review conclusion that no test
gap exists here).

### Cleanup confirmation

- Notebook reset to source form: never edited on disk this pass (kernel execution via the REST/
  WebSocket API does not write back into the `.ipynb` file the way "Run All" in a UI would) — verified
  directly via JSON inspection, not just `git checkout`:
  ```
  ee3c40eb execution_count= None outputs= []
  f87515c3 execution_count= None outputs= []
  b4bcc460 execution_count= None outputs= []
  e1ff4f35 execution_count= None outputs= []
  6eeb8f2e execution_count= None outputs= []
  ```
- All 3 kernels deleted via `DELETE /api/kernels/<id>`; `GET /api/kernels` confirmed `[]` after the
  last one.
- `POST /topics/skew-salting/teardown` issued after the last trial; `docker ps --filter "name=spark-"`
  confirmed empty afterward.

### Recommendation (re-validation)

This is a **recommendation, not final sign-off**, but unlike the original pass, this one is a **clean
pass on all 4 criteria**, independently re-verified end-to-end through the real app, the real notebook,
a real kernel, and the real Self-check Reveal flow — not just re-reading the diff or trusting the
developer's standalone-script numbers. All measured values (154.19x, 6.9x, 0 mismatches) reproduced
identically or near-identically across 3 trials, with comfortable margin over both bars. Ready for
`code-reviewer` and then human sign-off; no further test-engineer round needed unless the reviewer
surfaces something new.
