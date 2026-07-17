# Serialization Formats — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-17, against commit `8779bbd` on `feature/30-serialization-formats` (2 commits on top of the
      original `90b6c9a` feature commit — a private-API fix swapping to `explain(mode="formatted")` and a
      formatting correction, both already applied), following the coverage-review and code-review passes
      (static-diff reads, no live run, no blockers found).
Scope: US-C8 (`docs/requirements/curriculum-topics-2026-07.md`), all 4 acceptance criteria, verified
       against a real 3-worker cluster spawned through the app's own routes, not just re-reading the diff
       or trusting the developer's dev-time screenshots already in
       `docs/qa/screenshots/serialization-formats/dev/`.

## Method

Unit suite re-run before starting: `py -3.9 -m pytest tests/unit -q` → **287 passed**. `docker ps
--filter "name=spark-"` and a port-8000 `curl` check were both empty/free immediately before spawning,
per the concurrency precaution for this pass.

The FastAPI app was started fresh (`py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port 8000`) and a
cluster was spawned through the app's own route (`POST /topics/serialization-formats/spawn`, this
topic's own `cluster_defaults`: 3 workers, 2 cores, 4GB, 200 shuffle partitions, AQE off) — never
`compose/cli.py` or `docker compose` directly. `docker ps` confirmed `spark-master`,
`spark-worker-1/2/3`, and `spark-driver` all up, with `spark-driver` publishing `:4040-4042` and `:8888`
(JupyterLab) as expected.

**Notebook execution.** `content/serialization-formats/notebook.ipynb`'s `notebook_relpath` plumbing
deep-links a learner into the real JupyterLab server at `:8888`; the app itself doesn't execute
notebooks. To reproduce that faithfully, this pass drove the same JupyterLab kernel REST/WebSocket API
JupyterLab's own UI uses (`POST /api/kernels` + `/api/kernels/<id>/channels` websocket), executing the
notebook's 7 code cells in file order against a freshly started kernel, deliberately leaving the kernel
running afterward (not shut down) so `:4040` stayed live for the Self-check Reveal exercise below —
matching what a learner's own open JupyterLab tab would do.

One false start worth recording: an earlier attempt at this same technique appeared to hang (no
output surfaced for several minutes) and was killed as a precaution; a follow-up debug probe against
the same kernel showed a backlog of already-completed messages waiting for delivery — the run had
actually been executing correctly the whole time, but the background process's stdout was fully
block-buffered (not a tty) so nothing had flushed to the log file yet. To avoid ambiguity about which
cells had actually completed under a killed process, the kernel was discarded and a fresh one started,
then re-run with `python -u` (unbuffered) and explicit per-message logging so progress was visible in
real time. All 7 cells then executed cleanly against a brand-new kernel/Spark application in a single
pass, with no errors and every in-notebook `assert` passing (nonzero-bytes check, proportional-drop
check, partition-filter-reduces-bytes check, `PartitionFilters`-present check).

## US-C8, criterion 1 — CSV baseline: `select()`-ing 3/20 columns captures total bytes read

**PASS**, verified live. Cell 5 (`csv_df.select(*SELECT_COLS).agg(...)` against the 20-column, 1.5M-row,
~235MB CSV) reported, live from the running kernel:

```
[CSV] select 3/20 columns -- inputBytes read: 246199514 (246.2 MB)
```

This is the real `inputBytes` sum across the new stages the CSV-select action produced, pulled live from
`/api/v1/applications/<id>/stages` — the SQL-tab/REST scan-metrics surface US-C8 names. 246.2MB is close
to the full CSV file size (a small parsing/overhead delta above the raw ~235MB), confirming CSV's lack of
column pruning: `select()`-ing 3 of 20 columns did not reduce what was read off disk. Independently
cross-checked against the Self-check tab's stage table (criterion 4 below): stage 2 shows the identical
`inputBytes: 246199514`.

**Criterion 1: PASS.**

## US-C8, criterion 2 — same data as Parquet, same `select()`: bytes read drops close to proportionally

**PASS**, verified live. Cell 7 (`parquet_df.select(*SELECT_COLS).agg(...)` against the identical dataset
written as Parquet) reported, live:

```
[Parquet] select 3/20 columns -- inputBytes read: 24099934 (24.1 MB)

Parquet/CSV bytes-read ratio: 0.098  (column-count ratio would predict ~0.150 = 3/20)
```

24.1MB vs. the CSV baseline's 246.2MB is a ~10x drop — better than the 3/20 = 15% column-count ratio
would predict (columnar compression does extra work on 3 all-numeric double columns beyond pure column
pruning), but the same order of magnitude and direction the requirements doc's own worked example
("2.1GB → 118MB") describes. The in-notebook assertion (`parquet_bytes < csv_bytes / 2`) passed.
Independently cross-checked against the Self-check tab's stage table: stage 5 shows the identical
`inputBytes: 24099934`.

**Criterion 2: PASS.**

## US-C8, criterion 3 — partitioned Parquet, filter on partition column: whole-file skipping

**PASS**, verified live. Cell 11 wrote the same wide dataset Parquet-partitioned on an 8-value `region`
column, then compared an unfiltered read against a read filtered to a single region:

```
[Partitioned Parquet] unfiltered -- inputBytes read: 15049316 (15.0 MB)
[Partitioned Parquet] filter region == '0' -- inputBytes read: 1880368 (1.9 MB)

Filtered/unfiltered ratio: 0.125  (expected ~0.125 = 1/8)

PartitionFilters present in plan: True
```

The filtered/unfiltered ratio (0.125) matches 1/8 almost exactly — Spark's partition pruning lists only
the one matching `region=` directory and never opens the other seven files, i.e. whole-file skipping, not
row-level filtering. The plan text for the filtered read, captured live via
`part_read_df.filter(F.col("region") == "0").explain(mode="formatted")`, contains `PartitionFilters`,
confirming the pushed-down partition predicate rather than a post-scan filter. Both in-notebook
assertions (`filtered_bytes < unfiltered_bytes`, `"PartitionFilters" in plan_text`) passed. Independently
cross-checked against the Self-check tab's stage table: stage 9 shows `inputBytes: 15049316` (unfiltered)
and stage 11 shows `inputBytes: 1880368` (filtered) — the identical numbers side by side in the same
table.

**Criterion 3: PASS.**

## US-C8, criterion 4 — Self-check tab Reveal surfaces the bytes-read evidence from existing REST data

**PASS**, verified live by actually clicking through the flow (via direct HTTP calls to the same routes
the Reveal button hits), not just reading `app/web/routes/annotation.py`.

Before the Reveal, `GET /topics/serialization-formats/annotation` rendered the collapsed panel with a
"Reveal self-check" prompt (US-2.1's pull-not-push default state). `checkpoint(parquet_df.select(...),
topic="serialization-formats")` (cell 9) had already written a fresh dump to
`scratch/shared/annotations/serialization-formats/` while the kernel/SparkContext was still live. With
that same cluster/app still up (`app-20260717050804-0001`, confirmed reachable on `:4040`),
`POST /topics/serialization-formats/annotation/reveal` returned, live:

- **No stale-checkpoint warning** — the checkpoint's `app_id` cross-checked against the live driver and
  matched.
- **Plan panel**, correctly labeling the manifest's one `Scan` node per its generic mapping:
  `Scan` → *"File scan -- whether this reads a proportional slice (columnar) or the whole row
  (row-oriented) shows up in the stage's inputBytes below, not in this node's text"*. (A `ColumnarToRow`
  node above it renders as `unknown / unannotated`, expected — the manifest deliberately maps only
  `Scan`, per its own header-comment rationale about the `plan_parser.py` tokenizer only capturing a
  node's first word; the format-dependent evidence was always meant to live on the stage metric, not the
  plan-node label.)
- **Stage-metrics table**, populated with real numbers pulled live from
  `/api/v1/applications/<id>/stages` (13 real stage rows, `inputBytes` spotlighted per the manifest):
  **stage 2 shows `inputBytes=246199514`** (CSV baseline), **stage 5 shows `inputBytes=24099934`**
  (Parquet column-pruned), **stage 9 shows `inputBytes=15049316`** (partitioned unfiltered), and
  **stage 11 shows `inputBytes=1880368`** (partitioned filtered) — all four numbers matching the
  notebook's own live output exactly, side by side in the same table. This *is* the criterion's required
  evidence, sourced from existing stage/task REST data via the existing `stage_metrics` spotlighting
  mechanism (`app.annotation.engine.spotlight_stage_metrics()`), with no new annotation-engine capability,
  exactly as the manifest's header comment and US-C8's own disposition require.

**Criterion 4: PASS.**

## Teardown

```
DELETE /api/kernels/<kernel_id>                    → 204, GET /api/kernels → []
POST /topics/serialization-formats/teardown        → 200
docker ps -a --filter "name=spark-"                → (empty)
uvicorn process (port 8000)                        → stopped, confirmed no listener on :8000 or :8888
py -3.9 -m pytest tests/unit -q                     → 287 passed
```

**Notebook cleanliness check** (this session's cells were executed directly against a JupyterLab kernel
via the REST/WebSocket API, never by opening/saving `notebook.ipynb` itself through the Jupyter UI — the
file on disk was never written to during this pass):

```
grep -c '"execution_count":' content/serialization-formats/notebook.ipynb   → 7
grep -o '"execution_count": [^,]*' ...                                      → "execution_count": null  (x7)
grep -c '"outputs": \[\]' content/serialization-formats/notebook.ipynb      → 7
git status                                                                   → "nothing to commit, working
                                                                                 tree clean"
```

All 7 code cells confirmed at `execution_count: null` with empty `outputs: []`, and `git status` shows a
fully clean working tree — no live-execution artifacts leaked, and nothing outside this report file
changed.

## Overall recommendation

**All 4 of US-C8's acceptance criteria PASS, live-verified against a real 3-worker cluster and a real
JupyterLab kernel** — not re-derived from the diff or the developer's dev-time screenshots. The CSV
baseline's lack of column pruning (246.2MB, ~full file), Parquet's proportional-or-better bytes-read drop
(24.1MB, ~10x), partition-filter whole-file skipping on the partitioned Parquet table (15.0MB →
1.9MB, matching 1/8 almost exactly, with `PartitionFilters` confirmed in the plan), and the Self-check
Reveal flow surfacing all four of those exact `inputBytes` numbers from real stage/task REST data, were
all independently reproduced this pass, not assumed from the manifest/notebook source alone. The
deliberate scope deviation (20-column/~235MB CSV instead of the issue's originally suggested 50-column/
~2GB) and the single generic `Scan` plan-node rule (instead of format-specific rules, blocked by a real
`plan_parser.py` tokenizer limitation, tracked as follow-up issue #31) were both re-confirmed as
documented, deliberate choices rather than defects.

No defects found; nothing filed. This is a recommendation, not an approval — per this project's
Definition of Done, please review this report and give explicit sign-off (or flag anything that needs a
second look) before issue #30 is considered done.

## Human sign-off

**Given, 2026-07-17.** All 4 US-C8 acceptance criteria approved as PASS; issue #30 considered done.
