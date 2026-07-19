# Checkpointing — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-18, against uncommitted working-tree content in `content/checkpointing/`
      (`manifest.yaml`, `concept.md`, `notebook.ipynb`) on `main` at `fb5dfc6` — issue #47,
      backlog row #28, Sprint 8. Content-only change; no application code touched.
Scope: US-C4 (`docs/requirements/curriculum-topics-2026-07.md`), all 4 acceptance criteria,
       verified against a real 3-worker cluster spawned through the app's own routes, plus
       the new manifest `plan_nodes` rule (`checkpoint-truncated-scan`) exercised live through
       the actual Self-check Reveal endpoint — not a code read-through.

## Method

**Unit suite**, run before and after this pass: `py -3.9 -m pytest tests/unit -q` → **324
passed**, both times, no unit-test changes made or needed. This topic is content-only (a new
`content/checkpointing/` folder + one manifest `plan_nodes` rule) — per this project's
established pattern (Caching/Window Functions/etc.), no new Python unit tests are expected; the
notebook's own inline `assert`s are the runnable self-check. Confirmed `git diff main --
app/annotation/plan_parser.py app/annotation/engine.py app/annotation/manifest.py` is empty —
the manifest addition required zero engine changes, exactly as Decision A's addendum specifies.

**Live cluster.** `docker ps -a` was empty before starting. The FastAPI app was started fresh
(`py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port 8001`) and a cluster was spawned
through the app's own route (`POST /topics/checkpointing/spawn`, this topic's own
`cluster_defaults`: 3 workers, 2 cores, 4GB, 200 shuffle partitions, AQE off) — never
`compose/cli.py` or `docker compose` directly. `docker ps --filter "name=spark-"` confirmed
`spark-master`, `spark-worker-1/2/3`, and `spark-driver` all up, with `spark-driver` publishing
`:4040-4042` and `:8888` (JupyterLab).

**Notebook execution.** Same technique as prior topics' acceptance passes: a script drove the
real JupyterLab kernel REST/WebSocket API (`POST /api/kernels` + the kernel's `channels`
websocket) to execute `content/checkpointing/notebook.ipynb`'s 6 code cells in file order
against a freshly started kernel, leaving the kernel running afterward so `:4040` stayed live
for the Self-check Reveal exercise. The notebook file itself was never modified for this run.

## US-C4, criterion 1 (AC1) — 40-loop join chain, `.explain()` shows ~40 nested join nodes

**PASS**, real measured evidence, cell 2 output:

```
Join-family plan nodes found: 40
```

The plan tree printed 40 nested `SortMergeJoin` operators (visually confirmed in the raw
`.explain(mode="formatted")` dump — a deep `SortMergeJoin(168) → Project(163) → SortMergeJoin(162)
→ ...` chain). The notebook's own assertion (`join_count_before >= NUM_JOINS * 0.75`) passed
with an exact count, not just "roughly."

## US-C4, criterion 2 (AC2) — reliable checkpoint collapses the plan to a single flat scan

**PASS**, real measured evidence, cell 4 output (after `sc.setCheckpointDir()` + `chained_df.checkpoint()`
in cell 3, which used reliable checkpointing — `localCheckpoint()` was never called):

```
== Physical Plan ==
* Scan ExistingRDD (1)

(1) Scan ExistingRDD [codegen id : 1]
Output [3]: [id#2L, val#3, tag_39#121]
Arguments: [id#2L, val#3, tag_39#121], MapPartitionsRDD[140] at checkpoint at
NativeMethodAccessorImpl.java:0, ExistingRDD, hashpartitioning(id#2L, 200), [id#2L ASC NULLS FIRST]

Join-family plan nodes found after checkpoint: 0
Before: 40 join nodes.  After checkpoint: 0 join nodes -- lineage truncated.
```

The plan collapsed from 40 join nodes to exactly 1 scan node with 0 residual joins, and the cell's
own hard asserts (`join_count_after == 0`, `"Scan" in explain_after`) both passed. The
`sc.setCheckpointDir()` + `df.checkpoint()` cell (3) also visibly triggered a real materialization
job (progress-bar stage output over 200 tasks), confirming checkpointing pays a real up-front
write cost, matching `concept.md`'s "what to look for" claim.

**Architect's `Scan ExistingRDD` prediction (`docs/architecture/topic-shell-redesign.md`
addendum): held exactly, independently re-verified.** The surviving node is `Scan ExistingRDD`
(not a Parquet re-read or any other node shape), and the post-checkpoint plan carries no second
residual `Scan` — the only line in the physical-plan tree is the single `Scan ExistingRDD (1)`.
This is a genuine independent confirmation (this pass re-ran `df.checkpoint()` live rather than
trusting the developer's manifest-comment claim), on the same Spark version the manifest comment
cites (4.0.3) though against a fresh cluster/app-id (`app-20260719011713-0000`), not the
developer's original verification run.

## US-C4, criterion 3 (AC3) — self-check Reveal labels the post-checkpoint scan as
`checkpoint-truncated-scan`

**PASS**, verified live via the actual Reveal UI (`POST /topics/checkpointing/annotation/reveal`),
not just the manifest file read in isolation. With the checkpoint annotation freshly written by
cell 6 (`driver.playbook.checkpoint(checkpointed_df, topic="checkpointing")`) and the kernel's
Spark session still live at `:4040`, Reveal rendered:

```html
<ol class="plan-nodes">
  <li class="known">
    <code>Scan</code>
    &mdash; Checkpoint-truncated lineage — a single flat scan of the checkpointed data; the 40
    nested joins are gone <span class="concept-tag">[checkpoint-truncated-scan]</span>
  </li>
</ol>
```

Exactly one plan-node entry, rendered `known` (not `unknown/unannotated`), carrying the
`checkpoint-truncated-scan` concept tag and the manifest's exact label text. No second/stray
`Scan` entry appeared — confirming the manifest author's "post-checkpoint plan carries no second
residual `Scan`" empirical claim independently, on a live run. The runtime stage-metrics section
also rendered correctly (4 completed stages, unrelated to this criterion but confirming the
existing `stage_metrics` path is undisturbed by the new rule).

**No engine change was needed or made** — confirmed via `git diff main -- app/annotation/`:
empty except nothing (no files under `app/annotation/` are modified at all). The manifest-only
change mirrors the `cache-hit-scan` precedent exactly, as the architect's addendum specified.

## US-C4, criterion 4 (AC4) — `concept.md` covers reliable-vs-local durability and the
Structured Streaming tie-in

**PASS**, content read directly (content-only criterion, no live evidence required per the
requirements doc). `content/checkpointing/concept.md`:

- **(a) Reliable vs. local durability tradeoff** — "What it is" section explicitly contrasts
  "Reliable checkpointing" (`setCheckpointDir()` + durable storage, "survives an executor
  failure") against "Local checkpointing" (`localCheckpoint()`, "faster... but not
  fault-tolerant — if the executor holding that local data is lost, the checkpoint itself is
  gone"), and "Why it matters" restates this as "a durability-vs-speed tradeoff, not a
  formality."
- **(b) Structured Streaming tie-in** — "Why it matters"'s last bullet: "Structured Streaming's
  checkpoint mechanism is the same idea, applied continuously," describing offsets + state-store
  persistence and exact resume after restart, explicitly framed as "the same 'replace a chain of
  dependent history with a durable, self-contained snapshot' idea as `df.checkpoint()`."

Both sub-criteria are unambiguously present; no gaps found.

## Coverage review

No unit-test gaps found requiring new tests. This is a pure content + manifest-data addition
(same shape as every prior content-only topic in this project); the manifest schema
(`plan_nodes: match/concept/label`) is already fully covered by `tests/unit/test_manifest.py`'s
existing generic manifest-parsing tests, and adding one more `plan_nodes` entry to one more
topic's YAML doesn't exercise new code paths in `engine.py`/`plan_parser.py`/`manifest.py`. The
324-passed baseline (unchanged before/after this content addition) confirms no regression.

## Blockers / gaps for human attention

None found. All 4 acceptance criteria pass with real, live evidence; the architect's mechanical
prediction (`Scan ExistingRDD` → `Scan` token, no residual second `Scan`) held exactly on an
independent re-run.

## Cleanup confirmation

- `docker ps -a` returned empty after `POST /topics/checkpointing/teardown` (`state: idle`
  response) — cluster torn down.
- The notebook kernel was deleted (`DELETE /api/kernels/<id>` → `204`) before teardown;
  `GET /api/kernels` confirmed empty afterward.
- `git status --porcelain` shows only the pre-existing untracked `content/checkpointing/` and
  the two pre-existing modified docs (`docs/architecture/topic-shell-redesign.md`,
  `docs/requirements/curriculum-topics-2026-07.md`) that were already present before this
  validation pass started — this report adds only itself
  (`docs/qa/checkpointing-acceptance.md`). `git diff -- content/checkpointing/notebook.ipynb`
  against its on-disk state is empty (it is untracked/new, not modified by this run) — the live
  kernel execution went through the JupyterLab kernel API directly and never wrote back into the
  `.ipynb` file's `execution_count`/`outputs`, confirmed by direct inspection: every cell in the
  file on disk still carries no populated `outputs`/non-null `execution_count`.
- The `uvicorn` process and its Python subprocess started for this validation pass were both
  stopped; no process left listening on port 8001 afterward.

## Recommendation

This is a **recommendation, not final sign-off** — the human should review and give final
sign-off before marking US-C4 (issue #47) done. No blockers were found; recommend accepting.
