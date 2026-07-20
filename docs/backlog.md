# Backlog

The single prioritized list of work not yet pulled into a sprint. Owned by the `project-manager` agent (ordering/prioritization); populated by the `requirements-analyst` agent (new stories) as it writes requirements docs.

Ordered top-to-bottom by priority. Entries move out of this list into a sprint milestone during sprint planning, and back in if they're descoped or carried over undecided.

<!-- Example row:
| 1 | As a user, I want to reset my password via email | M | [docs/requirements/password-reset.md](requirements/password-reset.md) | Backlog |
-->

| Priority | Story | Size | Requirements doc | Status |
|---|---|---|---|---|
| 1 | Phase 0 — cluster harness proven manually: spawn/teardown a Spark Standalone cluster (master + 3 workers + driver/Jupyter) from a compose template, reach :8080/:4040/REST API, run a real shuffle job, generate tunable skewed/large synthetic data, and allow unguided notebook practice against the live cluster | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 2 | Curriculum topic: Partitioning & shuffle mechanics — what/why content + runnable notebook | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 3 | Phase 1 — partitioning/shuffle topic end-to-end in the web app: topic page, UI cluster spawn/teardown with configurable parameters, embedded Jupyter wired to the spawned cluster | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 4 | Curriculum topic: Catalyst plans & `.explain` — what/why content + runnable notebook | S | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md), [docs/requirements/topic-shell-redesign.md](requirements/topic-shell-redesign.md) (US-SH8) | Done (Sprint 1) status is inaccurate for a standalone topic page — **settled 2026-07-15**: no dedicated `content/catalyst-plans/` folder exists; Catalyst concepts currently appear only as passing vocabulary inside `content/join-strategies/concept.md` (confirmed on inspection — not the phase-by-phase content the mockup describes, so no extraction/split of `join-strategies` content is needed). This is now scoped as real implementation work: build `content/catalyst-plans/` via the new shell — see `topic-shell-redesign.md` US-SH8 and new row #31. |
| 5 | Curriculum topic: Join strategies (broadcast vs sort-merge vs shuffle-hash) — what/why content + runnable notebook forcing each strategy | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Done (Sprint 1) |
| 6 | Curriculum topic: Bucketing (co-partitioned joins) — what/why content + runnable notebook showing shuffle-free joins vs mismatched-bucket contrast case | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Done (Sprint 1) |
| 7 | Curriculum topic: AQE (skew join, partition coalescing, plan-changes-at-runtime) — what/why content + before/after (AQE on/off) notebook | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Done (Sprint 1) |
| 8 | Phase 2 — annotation engine as a self-check tool: static plan-node labeling (Exchange/BroadcastExchange/SortMergeJoin/Window etc.) + runtime stage-metrics lookup with deep link into the real Spark UI, driven by per-topic manifests | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Done (Sprint 1) |
| 9 | Phase 2.5 — Realtime monitoring dashboard: live per-node (master/worker) CPU & RAM utilization, sourced from Docker container stats (not Spark's own REST API) | M | [docs/requirements/realtime-monitoring-dashboard.md](requirements/realtime-monitoring-dashboard.md) | Done (Sprint 2) |
| 10 | Phase 2.5 — Realtime monitoring dashboard: live per-node task/partition execution detail for the running stage (task count, size, duration per executor), sourced from existing Spark stage/task REST API | M | [docs/requirements/realtime-monitoring-dashboard.md](requirements/realtime-monitoring-dashboard.md) | Done (Sprint 2) |
| 11 | Phase 2.5 — Realtime monitoring dashboard: derived stage ETA + task-duration variance display, clearly labeled as an estimate | S | [docs/requirements/realtime-monitoring-dashboard.md](requirements/realtime-monitoring-dashboard.md) | Done (Sprint 2) |
| 12 | Phase 2.5 — Realtime monitoring dashboard: diagnostic signal surfacing (skew, node-saturation visualization) without automated tuning suggestions, per G3 self-check philosophy | M | [docs/requirements/realtime-monitoring-dashboard.md](requirements/realtime-monitoring-dashboard.md) | Done (Sprint 2) |
| 13 | Phase 2.5 — Realtime monitoring dashboard: UI placement/integration with existing cluster panel, topic pages, and deep links into the real Spark UI | S | [docs/requirements/realtime-monitoring-dashboard.md](requirements/realtime-monitoring-dashboard.md), [docs/requirements/topic-shell-redesign.md](requirements/topic-shell-redesign.md) | Done (Sprint 2) as originally scoped (US-5.6 PASS, human sign-off 2026-07-15). **Follow-on redesign scoped 2026-07-15**: the standalone `/dashboard` route is being retired in favor of a Cluster Monitor slide-in panel — product decision and migration mechanics both settled and approved (`docs/architecture/topic-shell-redesign.md` Decision B). Implementation tracked separately as GitHub issue #23 (Sprint 3), now unblocked since Phase 2.5 sign-off has landed. |
| 14 | Curriculum topic: Caching/persistence — storage levels, eviction/spill-to-disk demo | S | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) (original), [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C5, concrete content + self-check hypothesis, supersedes/extends the original) | Done (Sprint 5) — all 3 US-C5 acceptance criteria (docs/requirements/curriculum-topics-2026-07.md) validated live across two independent runs against a fresh live cluster and the real running app: `.cache()`+`.count()` shows 100% fraction cached with real memory/disk sizes on the Storage tab/REST; measured cache-speedup timing captured for real (never hardcoded — 6.2x first run, 8.2x second run); `MEMORY_ONLY` vs `MEMORY_AND_DISK` eviction/spill-to-disk contrast demonstrated both times (nonzero disk usage for `MEMORY_AND_DISK`, cached fraction >= `MEMORY_ONLY`'s — 94%/99% first run, 94%/100% second run). Notebook development included a genuine live-debugging catch: an early cell used Python-Row-object dataset construction that reliably OOM'd real Spark executors, found and fixed by rewriting it to vectorized `spark.range()` + column expressions instead. Self-check evidence confirmed derivable via the real Reveal endpoint against a live checkpoint. Code-reviewer found no Blockers (one dismissed Minor on a harmless redundant `unpersist()` call, one advisory nit on comment length). Human has given final sign-off. GitHub issue [#28](https://github.com/hoanghaithanh/Spark-Playbook/issues/28) closed 2026-07-16. Second of Sprint 5's 4 stories to ship; all 4 are now done. |
| 15 | Curriculum topic: Window functions — ranking/running aggregates/lead-lag, shuffle+sort cost, skew tie-in | S | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) (original), [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C6, concrete content incl. missing-`partitionBy` failure mode, supersedes/extends the original) | Done (Sprint 5) — all 3 US-C6 acceptance criteria (docs/requirements/curriculum-topics-2026-07.md) validated live against a real 3-worker cluster and a real JupyterLab kernel (docs/qa/window-functions-acceptance.md): the correct-usage `row_number()`/running-total query shows a `Window` plan node preceded by `Sort`/`Exchange` (Stage 12: numTasks=200), with computed results independently cross-checked (`rn == 1` count exactly 2000, running-total vs. a separate `groupBy().agg(sum())` baseline — 0 mismatches across all 2,000 users); the deliberate missing-`partitionBy` contrast produced Spark's own driver-log WARN (`WindowExec: No Partition Defined for Window operation!`) and collapsed the window-reduce stage to a single task (Stage 15: numTasks=1 vs. 200 for the correct-usage case); the Self-check Reveal flow surfaced both stage rows (numTasks=200/numTasks=1) side by side from real stage/task REST data, no new annotation-engine capability needed. Code-reviewer found no Blockers. Human has given final sign-off (2026-07-16). GitHub issue [#29](https://github.com/hoanghaithanh/Spark-Playbook/issues/29) closed 2026-07-16. Third of Sprint 5's 4 stories to ship; all 4 are now done. |
| 16 | Curriculum topic: UDF vs pandas UDF serialization cost — timing comparison + plan distinction | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) (US-4.3, extended in place 2026-07-20 with concrete given/when/then acceptance criteria) | Done (Sprint 11) — all 5 US-4.3 acceptance criteria (`docs/requirements/spark-playbook-mvp.md`) validated live against a real 3-worker cluster and a real JupyterLab kernel (`docs/qa/udf-pandas-udf-acceptance.md`): measured (never hardcoded) wall-clock/`executorRunTime` gap between a row-at-a-time `udf()` and a vectorized `pandas_udf()` over a 20M-row/48-partition dataset, sourced live from `run_time_for()`'s stage REST diff (2.28x wall-clock, 2.40x `executorRunTime` this run, pandas UDF measurably faster on both, in the same direction/order of magnitude as `concept.md`'s dev-time ~2.8–3.2x quote without needing to match it exactly); distinct `BatchEvalPython` (row UDF) vs `ArrowEvalPython` (pandas UDF) physical-plan nodes live-confirmed via `.explain(mode="formatted")` against this repo's real Spark 4.0.3 cluster, resolving the previously-open Open Question 4 (no prior live capture of `ArrowEvalPython` existed in this repo); the Self-check Reveal flow surfaced both new manifest `plan_nodes` rules (`python-udf-eval`/`pandas-udf-eval`, mirroring `content/catalyst-plans/`'s shipped `BatchEvalPython` rule) with correct labels and zero annotation-engine code changes; the same Reveal calls' stage-metrics table reconciled exactly against the notebook's own printed `executorRunTime`/task sums for both runs; and `concept.md` explicitly, non-superficially ties the execution-cost framing back to `content/catalyst-plans/`'s existing `BatchEvalPython`/`ArrowEvalPython` optimizer-angle framing. Test coverage: `tests/unit/test_manifest.py::TestLoadRealUdfPandasUdfManifest`, `tests/unit/test_topics_loader.py::TestLoadRealUdfPandasUdfTopic` (full suite 400 passed, 2 skipped, no regressions). Code-reviewer found no Blockers/Majors. One platform-level event flagged during validation, not a topic defect: mid-pass the cluster was silently torn down by unrelated activity in another worktree sharing the same Docker daemon — a live reproduction of the already-tracked ADR #38 risk (`docs/architecture/worktree-cluster-isolation.md`, still `Status: Proposed`), not filed as a new issue. Human has given final sign-off (2026-07-20, `docs/qa/udf-pandas-udf-acceptance.md`). Sole story of Sprint 11 (GitHub milestone #14). GitHub issue [#51](https://github.com/hoanghaithanh/Spark-Playbook/issues/51) closed 2026-07-20 — closed directly, no `Fixes #51` commit-message keyword (same departure pattern as #47/#38/#50/others in recent sprints, not a new issue). |
| 17 | Curriculum topic: Memory management & spill (unified memory manager, execution vs storage memory, off-heap, OOM diagnosis) — dedicated topic, not folded into joins/AQE | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) (original, US-4.4), [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C10, concrete unified-memory-manager eviction-under-contention notebook + self-check hypothesis, supersedes/extends the original; added 2026-07-15 same day as a doc-gap fix — `curriculum-topics-2026-07.md` originally missed this topic entirely, see new row #32) | Backlog — **flagged stale 2026-07-19, still unresolved**: this row is superseded by row #32 (Memory Management, Done Sprint 6, GitHub issue #36), which already shipped the same US-C10 scope. Likely a duplicate row left over from before #32 was filed; needs a project-manager cleanup pass (retire or re-scope this row) rather than being pulled into a future sprint as-is. |
| 18 | Curriculum topic: Structured Streaming + Kafka — **pivoted 2026-07-19 to real market data** (Coinbase crypto + Finnhub stocks) with genuine Kafka dynamic subscribe/unsubscribe via a log-compacted control topic, a real Structured Streaming job (watermarks, windowed OHLC, checkpoint recovery, real-tick late-data injection), a live SSE/SVG price dashboard panel, and a query-progress widget — split into 4 sub-stories (producer+topics, Spark job, dashboard panel, progress widget) | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) (original, superseded), [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C7, superseded — synthetic-data framing), [docs/requirements/live-market-data-streaming.md](requirements/live-market-data-streaming.md) (US-LMD1-4, current) | Backlog — unblocked now that #19 (Phase 3 Kafka integration) shipped in Sprint 10. Requirements formalized 2026-07-19 under release milestone `v1.1 — Live Market Data Streaming` (GitHub milestone #13) per the human-approved plan `for-18-i-want-lazy-candle.md`; explicitly supersedes US-C7 and `topics-content-spec.md` §11's synthetic-data framing (see `live-market-data-streaming.md`'s Supersedes section). Architect ADR done (`docs/architecture/live-market-data-streaming.md`, D-LMD1-8). Split into 4 GitHub issues 2026-07-19, each milestoned into v1.1: [#52](https://github.com/hoanghaithanh/Spark-Playbook/issues/52) (a — producer + Kafka topics + dynamic subscribe/unsubscribe), [#53](https://github.com/hoanghaithanh/Spark-Playbook/issues/53) (b — Spark Structured Streaming job/notebook), [#54](https://github.com/hoanghaithanh/Spark-Playbook/issues/54) (c — live price dashboard panel), [#55](https://github.com/hoanghaithanh/Spark-Playbook/issues/55) (d — query-progress widget). |
| 19 | Phase 3 — streaming + Kafka integration: conditional Kafka (KRaft) in the compose template, synthetic producer script, wired into the streaming topic | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Done (Sprint 10) — conditional Kafka (KRaft) service added to the compose template plus a synthetic producer (`produce.py` CLI, `driver/playbook/producer.py` wrapper), designed via architect ADR `docs/architecture/kafka-streaming-infra.md` (amended for two human-resolved open questions: dual-listener host access, minimal event schema deferred to #18). Live acceptance (`docs/qa/kafka-streaming-infra-acceptance.md`): US-3.1 both given/thens PASS live against a real 3-worker cluster + real KRaft broker; US-3.2's first given/then PASS live (producer rate/message-count/host-shell-access independently verified); US-3.2's second given/then and all of US-3.3 correctly marked N/A, deferred to #18 (they need the Structured Streaming query+notebook #18 builds, not this infra story). Developer found 3 documented deviations from the ADR's literal draft via live-broker testing; test-engineer added 43 new unit tests (350→393 passed, 2 skipped, no regressions) and found one real bug (advertised-listener host mismatch, R-K6), fixed alongside 2 Minor code-reviewer findings (stale docstring address, unreaped zombie process on force-kill). Code-reviewer found no Blockers. No security-auditor pass — not triggered (no auth/secrets/PII/payments; the one new host-published port is loopback-only, unauthenticated by design, consistent with existing local-only ports). Human has given final sign-off (2026-07-19). GitHub issue [#50](https://github.com/hoanghaithanh/Spark-Playbook/issues/50) closed 2026-07-19 — **convention note:** closed directly, no `Fixes #50` commit-message keyword (same departure pattern as #47/#38/others in recent sprints, not a new issue). |
| 20 | Curriculum topic: Delta/Iceberg (optional) — ACID writes, time travel, schema evolution | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 21 | Curriculum topic: Tuning/debugging capstone with "diagnose cold" exercises — annotation engine hidden until deliberately revealed, covering shuffle/partitioning, join misdiagnosis, skew, and memory/spill scenarios | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 22 | Phase 4 — remaining curriculum integration: caching, window functions, UDF/pandas UDF, memory/spill, Delta/Iceberg (optional), tuning/debugging capstone, all wired end-to-end in the web app | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 23 | UI redesign — topic-page shell: unified Concept/Notebook/Self-check tab shell, cluster-config drawer (relocating the existing cluster control panel, ranges settled 2026-07-15: memory 1–8GB, shuffle partitions 1–300), breadcrumb topic switcher, applied to all 4 existing built topics and every future topic (content-driven, no bespoke per-topic markup). **Excludes** the dashboard-panel-only migration, split out 2026-07-15 into GitHub issue [#23](https://github.com/hoanghaithanh/Spark-Playbook/issues/23) (same number, unrelated numbering — that's a GitHub issue, this is a backlog row) since it's blocked on Phase 2.5 sign-off. | L | [docs/requirements/topic-shell-redesign.md](requirements/topic-shell-redesign.md) | Done (Sprint 3) — shell itself shipped `e9c69aa`; the split-out dashboard-panel-only migration (GitHub issue #23) shipped separately in `4772f00` and closed 2026-07-16. |
| 24 | UI redesign — topics-index landing page, rendered from `content/*/manifest.yaml` instead of a hardcoded list, reflecting the actual built/backlogged topic set (depends on #31 shipping so Catalyst plans appears correctly; #4's status discrepancy itself is now settled, see that row) | S | [docs/requirements/topic-shell-redesign.md](requirements/topic-shell-redesign.md) | Done (Sprint 4) — all 3 US-SH5 acceptance criteria (docs/requirements/topic-shell-redesign.md) validated live: GET / renders all 5 real topics correctly ordered/titled/blurbed from manifest.yaml; the "add/remove/reorder a topic folder needs zero code changes" criterion proven live (a second app instance pointed at a scratch content dir, topic deleted from it on the same running server without restart, page updated correctly); grep confirmed no topic-id special-casing in the implementation. Code-reviewer found no Blockers; human sign-off given. GitHub issue [#26](https://github.com/hoanghaithanh/Spark-Playbook/issues/26) closed 2026-07-16. |
| 25 | Curriculum topic: DAG & Lazy Evaluation — transformations vs actions, `.explain(True)` triggers no job, DAG/stage-boundary reading after `.count()` | S | [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C1) | Done (Sprint 5) — all 4 US-C1 acceptance criteria (docs/requirements/curriculum-topics-2026-07.md) validated live: no job after building the `.filter()→.select()→.groupBy()` chain; no job after `.explain(True)` either; a job appears after `.count()` with the stage boundary matching the shuffle `Exchange` (confirmed via REST stage data, shuffleWrite/shuffleRead byte counts matching across the boundary); self-check evidence (plan-node labels) confirmed derivable from existing REST job-list/stage data via the real Reveal endpoint against a live checkpoint, no new annotation-engine capability added. Code-reviewer found no Blockers; human sign-off given. GitHub issue [#27](https://github.com/hoanghaithanh/Spark-Playbook/issues/27) closed 2026-07-16. |
| 26 | Curriculum topic: Skew & Salting — manual key-salting technique distinct from AQE's automatic skew-join splitting, for cases AQE can't rebalance (e.g. a skewed `groupBy` with no join) | M | [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C2 — see stale-wording note below) | Done (Sprint 6) — did **not** ship clean on the first pass: the first live acceptance attempt (`docs/qa/skew-salting-acceptance.md`, original section) **failed 2 of 4 US-C2 criteria** against a real 3-worker cluster, across 3 independent trials — `groupBy(key).count()`'s map-side partial aggregation structurally bounds shuffle-read bytes regardless of row-count skew, so the hot key never produced a real straggler (byte ratios 1.15x/1.00x/0x vs. the required 2x), and salting did not reliably flatten the (noise-driven, not skew-driven) duration spread. Filed as GitHub issue [#46](https://github.com/hoanghaithanh/Spark-Playbook/issues/46), root-caused as structural (not a `FACT_ROWS`/partition-count tuning issue, confirmed identical shuffle-byte medians at different data scales). Triggered an architect redesign (`docs/architecture/skew-salting-demo-mechanism.md` + same-day "Salted-side assert — physics fix" amendment): the taught operation changed from `groupBy(key).count()` to `groupBy(key).agg(F.collect_list(...))` (not map-side-combinable, so per-row payload genuinely crosses the shuffle), and the salting claim changed from "flattens the distribution" to "cuts the straggler's load by >=3x" (the architect's own variance analysis showed true global flattening at N=10 salt buckets/200 partitions is structurally impossible — would need tens of thousands of buckets). Developer reimplemented the notebook and concept.md against the redesign; re-validation (`docs/qa/skew-salting-acceptance.md` "Re-validation (redesigned mechanism)" section) against a real 3-worker cluster, 3 independent live trials, passed clean on all 4 criteria: un-salted straggler shuffle-read bytes 154.19x the median (bar: 2x); salted straggler load cut 6.9x (bar: 3x); 0 correctness mismatches (salted-then-stripped values byte-identical to un-salted) in every trial; Self-check Reveal confirmed sourced from existing `stage_metrics`/`task_duration_quantiles` REST data, no new annotation-engine capability added. Human has given final sign-off. **Stale-wording note:** `docs/requirements/curriculum-topics-2026-07.md`'s US-C2 acceptance-criteria text still literally reads `groupBy(key).count()` and "flattens" — both superseded by the redesign above; spirit (single-sided `groupBy`, no join, visible straggler, corrected after salting, distinct from AQE) is fully preserved, only the literal operation/verb and success-bar wording are stale. Recommending a quick requirements-analyst touch-up to sync US-C2's text to the shipped mechanism, since two of its four ACs currently read as if the feature failed as delivered — not re-opening scope, just a wording sync; flagged for the human to greenlight. GitHub issue [#35](https://github.com/hoanghaithanh/Spark-Playbook/issues/35) stays **open** pending commit/merge to `main`, per this repo's established convention (issues close via a `Fixes #N` commit-message keyword at merge time, e.g. `d4e410f`'s `Fixes #34, Fixes #37` for Executor Tuning, not a direct close at sign-off time); landed via commit `15e1c12` (`Fixes #35, Fixes #46`), both issues closed 2026-07-18. |
| 27 | Curriculum topic: Executor Tuning — executor-cores/executor-memory sizing tradeoffs, GC time, "5 cores per executor" heuristic; self-check evidence source **settled by architect, approved 2026-07-15**: reveal-time pull from `app_client.fetch_executors()` (already exists), NOT a plan-matcher extension — see `docs/architecture/topic-shell-redesign.md` Decision A | M | [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C3) | Done (Sprint 6) — all 3 US-C3 acceptance criteria (docs/requirements/curriculum-topics-2026-07.md) validated live against a real 3-worker cluster (docs/qa/executor-tuning-acceptance.md), sharing the `executor_metrics` reveal-time plumbing (Decision A) with Memory Management (#36): fat-vs-right-sized executor runs produce measurably different wall-clock and GC-time-fraction numbers (3 vs 6 executors on the same fixed 3-worker cluster), confirming the notebook's cluster-fixed/executor-shape-varied design live; the Self-check Reveal flow surfaces the executor table (`totalGCTime` spotlighted) from real `/api/v1/applications/<id>/executors` data, no new annotation-engine capability needed. A live trial found the GC-fraction direction is not deterministic (contradicting the doc's original 5-trial claim) and that a failed hard assertion left a stuck kernel holding the whole cluster's capacity — filed and fixed as GitHub issue [#37](https://github.com/hoanghaithanh/Spark-Playbook/issues/37) (softened assertion, `.stop()` moved to `finally`), then live-re-verified (commit `05c473a`, re-check addendum). Code-reviewer's one Major finding (duplicate app-resolution network calls on Reveal, shared with #36) was fixed and live-re-verified (commit `1fc4c8d`). Human has given final sign-off (2026-07-17). GitHub issues [#34](https://github.com/hoanghaithanh/Spark-Playbook/issues/34) and [#37](https://github.com/hoanghaithanh/Spark-Playbook/issues/37) resolved via merge `d4e410f`. |
| 28 | Curriculum topic: Checkpointing — lineage truncation vs. caching, reliable vs. local checkpoints, streaming-checkpoint tie-in; self-check evidence source **settled by architect, approved 2026-07-15**: this one genuinely is a plan-node manifest rule (post-checkpoint scan node) — see `docs/architecture/topic-shell-redesign.md` Decision A | M | [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C4) | Done (Sprint 8) — all 4 US-C4 acceptance criteria (docs/requirements/curriculum-topics-2026-07.md) validated live against a real 3-worker cluster (`docs/qa/checkpointing-acceptance.md`): a 40-nested `SortMergeJoin` chain confirmed via `.explain()` before checkpoint; `df.checkpoint()` (reliable, not `localCheckpoint()`) collapsed the plan to a single `Scan ExistingRDD` node with 0 residual joins, matching the architect's `topic-shell-redesign.md` addendum prediction exactly; the new manifest `plan_nodes` rule (`checkpoint-truncated-scan`) verified live through the real Self-check Reveal endpoint (exactly one `known` plan-node entry, correct label, no stray second `Scan`), with zero engine code changes (`git diff -- app/annotation/` empty); `concept.md` confirmed to cover both the reliable-vs-local durability tradeoff and the Structured Streaming checkpoint tie-in. Unit suite unchanged (324 passed before/after). No Blockers found; human has given final sign-off (2026-07-19) on the acceptance report. GitHub issue [#47](https://github.com/hoanghaithanh/Spark-Playbook/issues/47) closed 2026-07-19 — **convention note:** closed directly via commit `1e7b80c`, which does **not** carry a `Fixes #47` keyword, a departure from this repo's established close-at-merge convention (contrast #35/#46 via `15e1c12`'s `Fixes #35, Fixes #46`, or #34/#37 via `d4e410f`'s `Fixes #34, Fixes #37`). Flagging for visibility only — not reversing, since the human closed it directly and the underlying work is genuinely done and human-signed-off. |
| 29 | Curriculum topic: Serialization Formats — columnar (Parquet/ORC) vs. row-oriented (CSV/JSON), predicate/column pushdown, measured bytes-read comparison | S | [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C8) | Done (Sprint 5) — all 4 US-C8 acceptance criteria (docs/requirements/curriculum-topics-2026-07.md) validated live against a real 3-worker cluster and a real JupyterLab kernel (docs/qa/serialization-formats-acceptance.md): CSV baseline shows no column pruning (`select()`-ing 3/20 columns still reads ~246.2MB, ~full file); the identical data as Parquet drops to 24.1MB (~10x); partition-column filtering on partitioned Parquet skips whole files (15.0MB unfiltered → 1.9MB filtered, matching the 1/8 partition-count ratio almost exactly, with `PartitionFilters` confirmed in the plan); and the Self-check Reveal flow surfaces all four of those exact `inputBytes` numbers from real stage/task REST data, no new annotation-engine capability needed. Code-reviewer found no Blockers. Human has given final sign-off (2026-07-17). GitHub issue [#30](https://github.com/hoanghaithanh/Spark-Playbook/issues/30) resolved via merge `465874c`. Fourth and final of Sprint 5's 4 stories to ship. |
| 30 | Curriculum topic: Fault Tolerance & Lineage — recomputation-based recovery after a killed worker, lineage-length cost tradeoff; self-check evidence source **settled by architect, approved 2026-07-15**: reveal-time pull from `app_client.fetch_task_list()` reusing the dashboard collector's existing retry-counting logic, NOT a plan-matcher extension — see `docs/architecture/topic-shell-redesign.md` Decision A. Worker-kill safety UX (Open Question 2) remains genuinely unresolved as an in-app control, but does not block this story: **settled 2026-07-19** — ships as a documented manual/external step (`docker kill`/`kill -9` on the target worker), consistent with `topic-shell-redesign.md`'s note that Decision A's evidence sourcing is the same either way; an in-app safety control is separable future scope, not part of this story. **Requirements confirmed/extended 2026-07-19** (requirements-analyst, ahead of developer pickup): US-C9's self-check-evidence criterion sharpened with concrete grounding (`app/monitoring/collector.py`'s `_build_partitions()` retry-counting logic, `app/web/routes/annotation.py`'s `_duration_quantiles()` per-stage-pull precedent); a 5th acceptance criterion added covering `concept.md`'s recomputation-model/lineage-cost/checkpointing-tie-in content, previously promised by the story text but untested by any AC (same gap-fix pattern as US-C4's AC4); the 48/50-tasks-retried figure clarified as illustrative, not a hard assertion target (pre-empting an Executor-Tuning-#37-style over-specification); and a developer-facing heads-up flagged on the mid-job-kill timing-coordination risk. No new file created — all changes live in `curriculum-topics-2026-07.md`'s existing US-C9 section. | L | [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C9) | Done (Sprint 9) — all 5 US-C9 acceptance criteria (docs/requirements/curriculum-topics-2026-07.md) validated live against a real 3-worker cluster across two independent runs (docs/qa/fault-tolerance-lineage-acceptance.md): a killed worker mid-job produced a real, measured partial retry (7 of 423 tasks across 2 stages on run 1, reproduced qualitatively on run 2 with a different kill target/timing), never a full job restart; the killed-worker run's result matched a clean run exactly (40-category signature, byte-identical); the new reveal-time `_task_retry_evidence()` REST pull (Decision A, reusing `app_client.fetch_task_list()` and a shared `retries_by_index()` helper extracted from the dashboard collector) rendered correct live evidence matching the notebook's own numbers; the worker-kill ships as a documented manual `docker kill` step, no in-app control built; `concept.md` covers the recomputation-from-lineage model and the lineage-cost tie-in to Checkpointing/Caching. Code-reviewer found 1 Major (a FAILED/resubmitted stage's own row falsely reported "0 retried" instead of pointing to where the real evidence landed) and 2 Minor findings, all fixed and re-verified (350 passed, 2 skipped, up from 335 — test-engineer added 15 new unit tests pinning both retry-detection shapes and the fix). One caveat surfaced, not blocking: AC3's exact FAILED-status-on-a-superseded-attempt branch didn't occur naturally in either live run (both times the superseded attempt's REST status came back `COMPLETE`, which already renders correctly), so that specific branch is verified by the mocked unit test rather than independent live reproduction — the fix itself is logically sound and reviewed clean. Human has given final sign-off (2026-07-19). GitHub issue [#49](https://github.com/hoanghaithanh/Spark-Playbook/issues/49) to close via this commit's `Fixes #49` keyword. |
| 31 | Curriculum topic: Spark SQL Catalyst — dedicated `content/catalyst-plans/` topic page (parse→analyze→optimize→physical-plan phases, DataFrame/SQL/UDF predicate-pushdown comparison, three-cell notebook walkthrough), built through the new topic-page shell; resolves backlog #4's status discrepancy — no change needed to `content/join-strategies/` (confirmed its "Catalyst optimizer" mention is unrelated passing vocabulary, not source content) | M | [docs/requirements/topic-shell-redesign.md](requirements/topic-shell-redesign.md) (US-SH8) | Done (Sprint 4) — shipped `content/catalyst-plans/{manifest.yaml,concept.md,notebook.ipynb}`; all 6 US-SH8 acceptance criteria validated against a live 3-worker cluster (evidence: `docs/qa/screenshots/catalyst-plans/`); code-reviewer Minor finding (ambiguous shared "pushed-down" label on the join's auto-generated null-check filter) fixed via clarifying prose in concept.md and two notebook cells; human sign-off given. GitHub issue [#25](https://github.com/hoanghaithanh/Spark-Playbook/issues/25) closed 2026-07-16. |
| 32 | Curriculum topic: Memory Management — unified memory manager, execution memory vs. storage memory sharing one region via `spark.memory.fraction`; eviction-under-contention notebook (cache a large DataFrame, run a competing memory-hungry shuffle, confirm partial recompute of evicted cached partitions); self-check evidence source **determined by requirements-analyst 2026-07-15, applying the architect's already-approved Decision A precedent (no fresh architect round needed)**: reveal-time pull from `app_client.fetch_executors()` (per-executor storage-vs-execution memory usage) plus the existing RDD-storage fetch already used by the Caching topic (#14) — same disposition as Executor Tuning (#27), NOT a plan-matcher extension — see `docs/architecture/topic-shell-redesign.md` Decision A and `curriculum-topics-2026-07.md` Open Question 1 | M | [docs/requirements/curriculum-topics-2026-07.md](requirements/curriculum-topics-2026-07.md) (US-C10) | Done (Sprint 6) — all 5 US-C10 acceptance criteria (docs/requirements/curriculum-topics-2026-07.md) validated live against a real 1-worker/8GB cluster (docs/qa/memory-management-acceptance.md): `.cache()`+`.count()` shows the ~3GB feature table 100% cached (3.10GB resident); a competing memory-hungry shuffle produces a real ~37.5% drop in executor storage `memoryUsed` (3101.8MB→1938.7MB), sourced live from `/api/v1/applications/<id>/executors` via the new `executor_metrics` annotation-manifest mechanism (Decision A, shared plumbing with Executor Tuning #34); re-running the cached query shows a genuinely measured (not hardcoded) partial-recompute split (3 of 8 partitions recomputed); and the US-4.4 spill/OOM connection holds (real spill, real `OutOfMemoryError`, with a driver-vs-executor nuance flagged and accepted, not a defect). Test-engineer found and fixed 3 real defects only visible by executing the notebook live (a missing `spark.executor.memory` config that silently defeated in-memory caching, a wrong-stage-selection bug collapsing per-partition timing, and an eviction-threshold too coarse against real timing noise). Code-reviewer found one Major finding (duplicate app-resolution network calls on Reveal), fixed and verified (297→300 tests passing after a recovered real-topic test class). Human has given final sign-off (2026-07-17). GitHub issue [#36](https://github.com/hoanghaithanh/Spark-Playbook/issues/36) resolved via merge `facb2e6`. A cross-worktree Docker Compose collision found during validation (unrelated to this topic) was filed separately as GitHub issue [#38](https://github.com/hoanghaithanh/Spark-Playbook/issues/38). |
| 33 | Public deploy: containerize the app + base compose stack + one-command `deploy.sh` (Dockerfile.app, deploy/docker-compose.yml, DooD identical-path repo mount) | M | plan: `docs/requirements/public-deploy.md` | Done (Sprint 7) — merged to `main` via PR [#45](https://github.com/hoanghaithanh/Spark-Playbook/pull/45) (merge commit `0c44b5c`, 2026-07-18). GitHub issue [#39](https://github.com/hoanghaithanh/Spark-Playbook/issues/39) closed. **Caveat shared by all of rows #33-#38, updated 2026-07-19:** unit suite passing (317 tests) and best-effort *local* acceptance passed (`docs/acceptance/public-deploy.md` Part A, run on Windows/Docker Desktop, no cloud VM/domain available). The on-VM live acceptance checklist (Part B — one-command deploy, TLS/auth end-to-end, port-surface lockdown, functional smoke through nginx) is **explicitly waived, not pending** — the human decided Spark Playbook will only ever run locally and will never be deployed to a remote VM, so Part B no longer applies as a v1.0 Definition-of-Done gate. v1.0 is considered complete on the strength of Part A + the unit suite alone — see the "v1.0 — Public Deploy rescoped" section and milestone #8 status below. |
| 34 | Public deploy: shrink the spawned-cluster port surface — loopback binding on spark-master/driver, drop unused ports (6066, worker UI publishes, 7078/7079) | S | plan: `docs/requirements/public-deploy.md` | Done (Sprint 7) — merged `0c44b5c`. GitHub issue [#40](https://github.com/hoanghaithanh/Spark-Playbook/issues/40) closed. Live-VM acceptance waived (human decision, not a gap) — see row #33 caveat. |
| 35 | Public deploy: split server-side `CLUSTER_HOST` config from browser-facing proxy paths (`/jupyter`, `/spark-master`) in `app/config.py` | S | plan: `docs/requirements/public-deploy.md` | Done (Sprint 7) — merged `0c44b5c`. GitHub issue [#41](https://github.com/hoanghaithanh/Spark-Playbook/issues/41) closed. Live-VM acceptance waived (human decision, not a gap) — see row #33 caveat. |
| 36 | Public deploy: reverse-proxy Jupyter + Spark Master UI behind nginx, with basic auth + Let's Encrypt TLS as the sole internet-facing surface | M | plan: `docs/requirements/public-deploy.md` | Done (Sprint 7) — merged `0c44b5c`. GitHub issue [#42](https://github.com/hoanghaithanh/Spark-Playbook/issues/42) closed. Security-auditor pass completed pre-merge per DoD. Live-VM acceptance waived (human decision, not a gap) — see row #33 caveat. TLS issuance, HSTS-upgrade behavior, and the auth boundary were statically confirmed against the rendered nginx config; a real domain/browser exercise will never happen, since this will never be deployed to a remote VM. |
| 37 | Public deploy: VM/firewall/certbot prerequisites wired into `deploy.sh` (idempotent) | S | plan: `docs/requirements/public-deploy.md` | Done (Sprint 7) — merged `0c44b5c`. GitHub issue [#43](https://github.com/hoanghaithanh/Spark-Playbook/issues/43) closed. Idempotency and the certbot fail-loud path were read-through/statically confirmed only (`docs/acceptance/public-deploy.md` Part B §1) — live-VM acceptance waived, see row #33 caveat. |
| 38 | Open-source hygiene: LICENSE, secret/history final check, gitignore deploy artifacts, README "Deploy (single-user, remote)" section | S | plan: `docs/requirements/public-deploy.md` | Done (Sprint 7) — merged `0c44b5c`. GitHub issue [#44](https://github.com/hoanghaithanh/Spark-Playbook/issues/44) closed. LICENSE, `.gitignore` coverage, and README deploy docs verified directly (`docs/acceptance/public-deploy.md` A6). Full git-history secret scan explicitly routed to security-auditor per that report, not independently re-verified by project-manager. |
| 39 | Public deploy: driver Spark UI deep links ("Open in Spark UI", dashboard "Driver UI" link, annotation Reveal stage links) are unreachable through the public HTTPS stack — no nginx route exposes the driver's `:4040`-`:4042` range past the `22/80/443` firewall restriction (US-PD5); deliberately left out of scope during the 2026-07-18 link-fix session since fixing it means widening the public port surface, a separate scope decision needing an architect look | S | [docs/architecture/public-deploy.md](architecture/public-deploy.md) (Addendum A2) | Backlog — new tech-debt item, found 2026-07-18. GitHub issue [#48](https://github.com/hoanghaithanh/Spark-Playbook/issues/48) filed, unmilestoned. `deploy-lan` (LAN-only stack) is unaffected — already fixed there via `DRIVER_UI_HOST=${LAN_IP}`. |
| 40 | Multi-broker Kafka cluster & monitor — 5 sub-stories: (a) user-configurable "Kafka" section in the cluster-config drawer (1-5 brokers, default 3, RF=3/min-isr=2, folded into the existing single Spawn/Teardown action), (b) Kafka observability data layer (CLI-shellout, not KafkaAdminClient — see #50's ADR dead end), (c) JMX exporter for heap/GC and produce/consume latency plus per-broker idle-ratio metrics, (d) 4th tab in the existing Cluster Monitor panel built against a specific design mockup, (e) broker-kill fault-tolerance demo (ISR shrink/leader re-election) | L | [docs/requirements/multi-broker-kafka-cluster.md](requirements/multi-broker-kafka-cluster.md) | Backlog — requirements formalized 2026-07-19 (requirements-analyst) per the human-approved plan `for-18-i-want-lazy-candle.md`; explicitly amends `docs/architecture/kafka-streaming-infra.md` Decision D1 (reverses "Kafka is not a user-facing toggle" — see the new doc's Amends section). All 5 sub-stories (US-MBK1-5) have given/then acceptance criteria; two items flagged as genuinely open (exact JMX MBean names, exact Kafka CLI output-parsing shapes) rather than guessed. Filed under release milestone `v1.2 — Multi-Broker Kafka Cluster & Monitor` (GitHub milestone [#15](https://github.com/hoanghaithanh/Spark-Playbook/milestone/15)), not yet pulled into a sprint — GitHub issues for the 5 sub-stories are now filed: (a) [#56](https://github.com/hoanghaithanh/Spark-Playbook/issues/56), (b) [#57](https://github.com/hoanghaithanh/Spark-Playbook/issues/57), (c) [#58](https://github.com/hoanghaithanh/Spark-Playbook/issues/58), (d) [#59](https://github.com/hoanghaithanh/Spark-Playbook/issues/59), (e) [#60](https://github.com/hoanghaithanh/Spark-Playbook/issues/60). **Sequencing note:** must land before v1.1's still-unstarted sub-stories (#52-#55, row #18) — those build the streaming producer/job/dashboard against whatever broker topology is running, so they should target the multi-broker cluster rather than inherit the single-node one from #50 (row #19). Mirrors how row #19 was itself a prerequisite consumed by row #18. **Sub-story status:** (a)/#56 — Done, human sign-off 2026-07-20 (`docs/qa/multi-broker-kafka-cluster-acceptance.md`); (b)/#57 — Done, human sign-off 2026-07-20 (`docs/qa/kafka-observability-layer-acceptance.md`); (c)-(e)/#58-#60 still open, unstarted. |
| 41 | Kafka curriculum topic: `kafka-architecture-kraft` — brokers/controllers/partitions/replication in a KRaft (no ZooKeeper) cluster, contrasted against the legacy ZooKeeper-coordinated architecture | M | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC1) | Done (Sprint 12) — split out of the former combined row #41 2026-07-20 (project-manager), matching the "one topic, one story" grain the rest of this table already uses for curriculum content. Size bumped S->M 2026-07-20 to absorb the one-time topics-index grouping build (D-KC1: new `track` manifest field, `app/topics/loader.py` change, template change) bundled into this issue since it is the first Kafka topic to ship and needs somewhere to render. Pulled into **Sprint 12 (GitHub milestone [#16](https://github.com/hoanghaithanh/Spark-Playbook/milestone/16), 2026-07-20 – 2026-07-24)** 2026-07-20. Implemented, tested, code-reviewed (1 finding fixed — guard `track=None` from a malformed manifest), and live-acceptance-validated against a real 3-broker KRaft cluster: all 3 US-KC1 acceptance criteria plus the D-KC1 grouping criterion PASS (`docs/qa/kafka-architecture-kraft-acceptance.md`). Human has given final sign-off (2026-07-20). GitHub issue [#62](https://github.com/hoanghaithanh/Spark-Playbook/issues/62) closed 2026-07-20 — closed directly, no `Fixes #62` commit-message keyword (branch not yet merged to `main`; same departure pattern as #56/#57/#50, not a new issue). Still on branch `worktree-issue-62-kafka-architecture-kraft` pending merge — remaining Sprint 12 issues #58, #63, #64, #65 still open. |
| 42 | Kafka curriculum topic: `kafka-topics-partitions` — partition count/key choice determining message ordering and distribution, keyed vs. unkeyed produce, per-partition ordering (no cross-partition guarantee) | S | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC2) | Backlog (Sprint 12) — split out of the former combined row #41 2026-07-20. Fully buildable today, no blockers. Pulled into **Sprint 12 (GitHub milestone [#16](https://github.com/hoanghaithanh/Spark-Playbook/milestone/16), 2026-08-03 - 2026-08-07)** 2026-07-20; GitHub issue [#63](https://github.com/hoanghaithanh/Spark-Playbook/issues/63) filed and milestoned. |
| 43 | Kafka curriculum topic: `kafka-producers-delivery` — `acks`/idempotence/retries under induced failure, at-least-once vs. at-most-once vs. (practically) exactly-once producer-side behavior, measured not asserted | M | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC3) | Backlog (Sprint 12) — split out of the former combined row #41 2026-07-20. **Status corrected 2026-07-20**: previously marked "fully buildable today, no blockers" — wrong. The architect's US-KC11 spike (`docs/architecture/kafka-curriculum.md` D-KC3) found `kafka-python==2.0.2` has no idempotent-producer support at all (confirmed directly against the library source). Still fully buildable today with no new dependency — the idempotence bullet now uses the same CLI-subprocess fallback (`kafka-console-producer.sh --producer-property enable.idempotence=true`) already established for US-KC11; the `acks`-only bullets are unaffected (`acks` is a real `kafka-python` config). Pulled into **Sprint 12 (GitHub milestone [#16](https://github.com/hoanghaithanh/Spark-Playbook/milestone/16), 2026-08-03 - 2026-08-07)** 2026-07-20; GitHub issue [#64](https://github.com/hoanghaithanh/Spark-Playbook/issues/64) filed and milestoned. |
| 44 | Kafka curriculum topic: `kafka-consumers-groups` — offset commits, consumer-group rebalancing, partition-bounded parallelism ceiling, manual-vs-auto-commit crash/restart behavior | M | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC4) | Backlog (Sprint 12) — split out of the former combined row #41 2026-07-20. Fully buildable today, no blockers. Pulled into **Sprint 12 (GitHub milestone [#16](https://github.com/hoanghaithanh/Spark-Playbook/milestone/16), 2026-08-03 - 2026-08-07)** 2026-07-20; GitHub issue [#65](https://github.com/hoanghaithanh/Spark-Playbook/issues/65) filed and milestoned. |
| 45 | Kafka curriculum topic: `kafka-replication-fault-tolerance` — manual broker kill, live leader election/ISR shrink-and-regrow, RF=3/min-isr=2 vs. an over-strict min-isr=3 contrast; independent of issue #60's in-app broker-kill panel (manual notebook exercise, same underlying `docker stop` mechanic) | M | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC5) | Backlog — split out of the former combined row #41 2026-07-20. Fully buildable today (does not need #60 to ship). Not yet pulled into a sprint — see narrative section below. |
| 46 | Kafka curriculum topic: `kafka-log-compaction-retention` — `cleanup.policy=compact` (latest-value-per-key survival, tombstone deletion) vs. `cleanup.policy=delete` (time-based segment aging), standalone (no v1.1 dependency) | M | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC6) | Backlog — split out of the former combined row #41 2026-07-20. Fully buildable today, no blockers. Not yet pulled into a sprint — see narrative section below. |
| 47 | ~~Kafka curriculum topic: `kafka-spark-structured-streaming`~~ — **FOLDED INTO v1.1, 2026-07-20** | — | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC7) | **Closed — not an independent story.** Human resolved Open Question 1 on 2026-07-20: US-KC7 folds into v1.1's `structured-streaming` topic (issue [#53](https://github.com/hoanghaithanh/Spark-Playbook/issues/53), row #18, milestone #13) rather than shipping standalone. No GitHub issue filed for this row; its acceptance criteria carry forward as reference material for #53's implementation. The Kafka curriculum's independently-schedulable set is now 11 topics (rows #41-46, #48-52), not 12. |
| 48 | Kafka curriculum topic: `kafka-serialization-schema-evolution` — schema registry + Avro/Protobuf produce/consume, backward-compatible vs. backward-incompatible schema change (registry-enforced rejection), contrasted against JSON's lack of enforcement | M/L (TBD) | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC8) | Backlog — split out of the former combined row #41 2026-07-20. **GATED ON ARCHITECT INFRA DECISION** — needs a new compose-stack schema-registry service and a new driver-image Avro/Protobuf client library (G-KC6, resolved into scope 2026-07-20); product/library choice and resource-budget impact are architect calls, likely warranting its own ADR (comparable in kind to #50's Kafka-infra ADR). Not buildable until that ADR lands. |
| 49 | Kafka curriculum topic: `kafka-performance-tuning` — measured throughput/latency sweeps across `linger.ms`/`batch.size`/`compression.type`, partition-count-vs-consumer-parallelism tie-in to `kafka-consumers-groups` | M | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC9) | Backlog — split out of the former combined row #41 2026-07-20. Fully buildable today, no blockers. Not yet pulled into a sprint — see narrative section below. |
| 50 | Kafka curriculum topic: `kafka-monitoring-observability` — consumer-group lag as the primary operational signal (demonstrable today); broker-level JVM heap/GC/request-latency sections **BLOCKED pending issue [#58](https://github.com/hoanghaithanh/Spark-Playbook/issues/58)** (JMX exporter, v1.2/milestone #15, open/unstarted), with [#59](https://github.com/hoanghaithanh/Spark-Playbook/issues/59) (Monitor UI tab) as a soft (not hard) dependency | M | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC10) | Backlog — split out of the former combined row #41 2026-07-20. **Partially blocked** — the lag-only portion could ship standalone, but per G-KC4 ("honest status per topic") this topic is written as one story covering both; splitting the lag-only slice out further is a possible future call, not made here. Full topic not buildable until #58 ships. |
| 51 | Kafka curriculum topic: `kafka-exactly-once-transactions` — idempotent-producer + transactional-API commit/abort visibility contrast (`read_committed` vs. `read_uncommitted`), tied back to Spark's own exactly-once sink semantics | M | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC11) | Backlog — split out of the former combined row #41 2026-07-20. **NEEDS A PRE-ARCHITECTURE FEASIBILITY SPIKE** — `kafka-python==2.0.2`'s transactional producer support must be verified live against this repo's actual multi-broker topology before a design can be committed to (fallback: CLI-driven `kafka-console-producer.sh --transactional-id`, not a new pinned dependency). Spike timing (before vs. during the architect pass) is Open Question 5, left to the architect. Not buildable until the spike runs. |
| 52 | Kafka curriculum topic: `kafka-multi-broker-cluster-ops` — the "go use the drawer knob yourself" exercise directly exercising #56's shipped broker-count field (1-5), rolling restart with continuous availability, under-replicated-partition signal during a simulated slow node | M | [docs/requirements/kafka-curriculum.md](requirements/kafka-curriculum.md) (US-KC12) | Backlog — split out of the former combined row #41 2026-07-20. Fully buildable today (depends only on #56, already shipped), no blockers. Not yet pulled into a sprint — see narrative section below. |

## Confirmed sprint plan (2026-07-16, human-approved; Sprint 4 milestoned 2026-07-16)

project-manager proposed, human confirmed: Sprints 4-10 below. **Gate cleared 2026-07-16** — Sprint 3
(GitHub milestone #4) is now closed, with all 4 of its issues resolved and closed (#23
dashboard-panel migration shipped `4772f00`; #24 Job Detail freeze shipped `e9c69aa`; #17
elapsed-time placeholder shipped `b68cc77`; #8 duration quantiles shipped `1595011`). **Sprint 4
(GitHub milestone #5, 2026-07-16 – 2026-07-20) is now open** — issues
[#25](https://github.com/hoanghaithanh/Spark-Playbook/issues/25) (Catalyst plans) and
[#26](https://github.com/hoanghaithanh/Spark-Playbook/issues/26) (topics-index) filed and
milestoned 2026-07-16, per the row #31/#24 sequencing below. **Gate cleared 2026-07-16 (same day)** — Sprint 4 (GitHub milestone #5) is now closed, with both of its issues resolved and closed (#25 Catalyst plans shipped and closed 2026-07-16; #26 topics-index shipped and closed 2026-07-16). **Sprint 5 (GitHub milestone #6, 2026-07-17 – 2026-07-21) is now open** — proposed by project-manager as a straight ratification of the plan below (no rescoping), confirmed by the human 2026-07-16; issues [#27](https://github.com/hoanghaithanh/Spark-Playbook/issues/27) (DAG & Lazy Evaluation, row #25), [#28](https://github.com/hoanghaithanh/Spark-Playbook/issues/28) (Caching, row #14), [#29](https://github.com/hoanghaithanh/Spark-Playbook/issues/29) (Window Functions, row #15), and [#30](https://github.com/hoanghaithanh/Spark-Playbook/issues/30) (Serialization Formats, row #29) filed and milestoned 2026-07-16 — all 4 independent, no internal sequencing needed. **#29 (Window Functions) shipped and closed 2026-07-16/17** — third of Sprint 5's 4 stories done. **#30 (Serialization Formats) shipped 2026-07-17** — merged to `main` (`465874c`), human sign-off given, fourth and final Sprint 5 story done. All 4 Sprint 5 stories are now complete; Sprint 5 (GitHub milestone #6) is ready for project-manager to close and run the retro.

**Sprint 5 (GitHub milestone #6) closed 2026-07-17** with all 4 stories done and its retro recorded in `docs/retrospectives.md`. **Sprint 6 (GitHub milestone #7, 2026-07-17 – 2026-07-21) is now open** — proposed by project-manager as a straight ratification of the plan below (no rescoping) plus one addition, confirmed by the human 2026-07-17: issues [#34](https://github.com/hoanghaithanh/Spark-Playbook/issues/34) (Executor Tuning, row #27), [#36](https://github.com/hoanghaithanh/Spark-Playbook/issues/36) (Memory Management, row #32), and [#35](https://github.com/hoanghaithanh/Spark-Playbook/issues/35) (Skew & Salting, row #26) filed and milestoned 2026-07-17, per the row #27/#32 pairing rationale below (shared `fetch_executors()` reveal-time REST-pull mechanism). Pre-existing open issue [#31](https://github.com/hoanghaithanh/Spark-Playbook/issues/31) (plan_parser.py tokenizer first-word-only limitation, tech-debt) was also pulled into Sprint 6 at the human's request and milestoned alongside the three curriculum stories — see `Known issues / watch-items` below. **#36 (Memory Management) shipped and closed 2026-07-17** — merged to `main` (`facb2e6`), human sign-off given, first of Sprint 6's 3 curriculum stories done; #34 (Executor Tuning) and #35 (Skew & Salting) remain open.
**#34 (Executor Tuning) shipped and closed 2026-07-17** — merged to `main` (`d4e410f`, `Fixes #34, Fixes #37`), second of Sprint 6's 3 curriculum stories done. **#35 (Skew & Salting) has human final sign-off (2026-07-18, see row #26 above for the full first-pass-failed/redesign narrative) but is not yet committed** — code is still in the developer's working tree; issue #35 stays open until it lands via a `Fixes #35, Fixes #46` commit, per this repo's established close-at-merge convention. With #35 done pending commit, Sprint 6 (GitHub milestone #7) has only 1 story-adjacent issue still genuinely open: pre-existing tech-debt issue #31 (tokenizer limitation), plus issue #46 (fully resolved, closes alongside #35 at commit time) — not being closed yet to keep the closing commit's `Fixes` keywords accurate.

**Sprint 6 (GitHub milestone #7) closed 2026-07-18** — all 4 issues done: #35 (Skew & Salting) landed via commit `15e1c12` (`Fixes #35, Fixes #46`); #31 (tokenizer tech-debt) landed via commit `8d172d5` (`Fixes #31`, doc-only fix, human-approved YAGNI decision). Milestone confirmed 0 open / 5 closed issues (#34, #35, #36, #31, #46). Retro recorded in `docs/retrospectives.md`.

1. **Sprint 4** — #31 Catalyst plans (M), #24 topics-index (S), in that order (#24 depends on #31).
2. **Sprint 5** — #25 DAG & Lazy Evaluation, #29 Serialization Formats, #14 Caching, #15 Window Functions (4×S).
3. **Sprint 6** — #27 Executor Tuning, #32 Memory Management, #26 Skew & Salting (3×M) — #27/#32 share the same new reveal-time REST-pull mechanism, pair to avoid re-deriving it.
4. **Sprint 7** — #28 Checkpointing (M, solo — different engine-extension style, plan-node rule not REST pull).
5. **Gap before Sprint 8** — architect/design pass to resolve Open Question 2 in `curriculum-topics-2026-07.md` (worker-kill / query-restart safety UX), blocking #30 and #18.
6. **Sprint 8** — #30 Fault Tolerance & Lineage (L, solo).
7. **Sprint 9** — #19 Kafka infra (L).
8. **Sprint 10** — #18 Structured Streaming (L), only after #19 ships.

Untouched: #16, #20, #21 — not part of the design import.

## Known issues / watch-items

Non-feature operational notes that don't need a story/acceptance-criteria entry in the table above, but are worth tracking if they recur.

| Date observed | Item | Status | Revisit condition |
|---|---|---|---|
| 2026-07-14 | Docker Desktop engine threw transient `EOF` pipe errors twice during `docker compose up` calls (e.g. `error during connect: Post "http://%2F%2F.%2Fpipe%2FdockerDesktopLinuxEngine/...": EOF`) while devops-engineer ran the Phase 0 cluster harness validation (build/up/wait-for-ready/smoke-test against a live Docker daemon, `sparkpb` compose stack). Both times a plain retry of the same command succeeded cleanly with no lasting effect; appears to be engine-level flakiness in this machine's Docker Desktop install, unrelated to our compose config. | Low-priority / Watching | Reprioritize and investigate only if it recurs with higher frequency, or if a retry stops resolving it. |
| 2026-07-17 | GitHub issue [#31](https://github.com/hoanghaithanh/Spark-Playbook/issues/31) (`plan_parser.py` tokenizer only captures a plan node's first word, blocking future multi-word `manifest.match` rules; not a blocker for any shipped topic) pulled into Sprint 6 (GitHub milestone #7) at the human's request alongside the three curriculum stories, rather than left unmilestoned. | Resolved (Sprint 6), closed 2026-07-18 via commit `8d172d5` — doc-only fix (documented the tokenizer's first-word-only match constraint), no tokenizer behavior changed, per human-approved YAGNI decision not to extend the tokenizer without a concrete multi-word manifest rule needing it. Code-reviewer found no findings; human signed off. | Closed — revisit only if a future manifest rule genuinely needs multi-word matching. |

## New release milestone: v1.0 — Public Deploy (2026-07-17)

A new body of work — making Spark Playbook remotely deployable by a single user via one command
(`deploy.sh`), containerizing the app, and open-sourcing the repo — was approved by the human on
2026-07-17 (plan: containerize app + base compose stack, shrink spawned-cluster port surface,
config URL/host split, nginx reverse-proxy with basic auth + Let's Encrypt TLS, deploy
prerequisites/firewall/certbot, open-source hygiene). This is release-scale, multi-area
infrastructure work — different in character and size from the curriculum-topic stories that make
up the sprint cadence — so it was filed as its own **release milestone**,
[`v1.0 — Public Deploy`](https://github.com/hoanghaithanh/Spark-Playbook/milestone/8) (GitHub
milestone #8), per CLAUDE.md's provision for release-level milestones alongside sprint milestones.
Six issues filed and milestoned 2026-07-17 (backlog rows #33-#38 above,
[#39](https://github.com/hoanghaithanh/Spark-Playbook/issues/39)-[#44](https://github.com/hoanghaithanh/Spark-Playbook/issues/44)).

**Not pulled into Sprint 6** (GitHub milestone #7, active, due 2026-07-20, 3 days left, 3 issues
still open: #34, #35, #31) — adding six new large infra issues to an active, nearly-due sprint
without flagging it as a scope change would violate the project's own sprint-scope rule. These are
left in the milestone, unscheduled into any sprint, for the human to decide: fold into Sprint 7 (the
next planned curriculum sprint per the Sprint 4-10 plan, currently slated for #28 Checkpointing), run
as its own dedicated sprint (or several, given the size — architect/devops/security-auditor
involvement noted per issue), or interleave alongside curriculum sprints. Also flagging: no
`docs/requirements/public-deploy.md` exists yet — the plan file
(`C:\Users\hoang\.claude\plans\summarize-the-repository-and-compiled-quill.md`) has the full detail,
but per the default pipeline the next step would be requirements-analyst formalizing it (or
architect directly, if the human judges the plan's own detail sufficient to skip straight to design
— that call is the human's per CLAUDE.md's pipeline judgment note).

## Sprint 7 rescoped to Public Deploy issues (2026-07-17)

The human decided to fold the six public-deploy issues into **Sprint 7** rather than run them as a
separate release-only cadence. **Sprint 7 (GitHub milestone #9, 2026-07-21 – 2026-07-25)** created
and issues [#39](https://github.com/hoanghaithanh/Spark-Playbook/issues/39)-[#44](https://github.com/hoanghaithanh/Spark-Playbook/issues/44)
(backlog rows #33-#38) moved into it from the `v1.0 — Public Deploy` release milestone (GitHub
milestone #8).

**Release-vs-sprint milestone constraint:** GitHub allows only one milestone per issue, so moving
these six issues into Sprint 7 removed them from milestone #8 (now shows 0 open issues — not
representative of remaining release scope, all six issues are still open, just tracked under Sprint
7 now). To keep the v1.0 release grouping visible without a second milestone slot, a new label
**`release:v1.0`** was created and applied to all six issues. `v1.0 — Public Deploy` (milestone #8)
remains open as the release-level milestone; going forward, "what's in v1.0" is answered by
`gh issue list --label release:v1.0 --state all` rather than by the milestone's own issue count,
since its member issues will migrate across sprint milestones as they're scheduled/completed.

**Conflict with the Sprint 4-10 plan for #28 Checkpointing:** the plan above (row "4. **Sprint 7**")
tentatively slated Sprint 7 for the Checkpointing curriculum story (backlog row #28 — not yet filed
as a GitHub issue). That is now displaced — Sprint 7 is fully occupied by the six deploy issues.
Checkpointing (backlog row #28) stays in `docs/backlog.md` as **Backlog**, unscheduled, and the
Sprint 4-10 plan's numbering shifts by one: Checkpointing is now the next curriculum story up
whenever a future sprint has room (tentatively Sprint 8, bumping Fault Tolerance & Lineage to Sprint
9, Kafka infra to Sprint 10, and Structured Streaming beyond the original table) — this shift is
**not yet human-confirmed** and should be ratified at the next sprint-planning checkpoint rather than
treated as final.


## Sprint 7 complete; v1.0 merged, live-VM acceptance pending (2026-07-17 close-out check)

All six Sprint 7 issues ([#39](https://github.com/hoanghaithanh/Spark-Playbook/issues/39)-[#44](https://github.com/hoanghaithanh/Spark-Playbook/issues/44), backlog rows #33-#38 above) shipped in a single PR, [#45](https://github.com/hoanghaithanh/Spark-Playbook/pull/45), merged to `main` (`0c44b5c`, 2026-07-18) — all six auto-closed by the merge's per-issue `Fixes` keywords. Sprint 7 (GitHub milestone #9) shows 0 open / 6 closed issues (the milestone's own issue-count API also lists the merged PR itself as a 7th closed item, since the PR was milestoned alongside the issues — not a discrepancy, just PRs and issues sharing GitHub's numbering).

**v1.0 — Public Deploy (GitHub milestone #8) stays open.** Unit suite passes (317 tests) and a best-effort *local* acceptance pass ran clean (`docs/acceptance/public-deploy.md` Part A — this session's dev host is Windows + Docker Desktop, no cloud VM or domain available). The on-VM live acceptance checklist (Part B: one-command `deploy.sh` on real Linux, TLS/Let's-Encrypt issuance, basic-auth boundary over a real browser, port-surface lockdown from an external host, functional smoke of Jupyter/Spark-UI through the nginx proxy) has **not** been executed yet. Milestone #8 will stay open until that live pass runs and passes — closing it now would overstate what's actually been verified.

**Sprint 7 (GitHub milestone #9) closed 2026-07-18** — all committed scope done, zero open issues.

**Resequencing of Checkpointing (backlog row #28) — RATIFIED by the human 2026-07-18.** The Sprint 7 rescope displaced Checkpointing from its originally tentative Sprint 7 slot; the follow-on shift is now confirmed: **Checkpointing → Sprint 8, Fault Tolerance & Lineage → Sprint 9, Kafka infra → Sprint 10.** The corresponding sprint milestones are created at each sprint's own planning checkpoint (not pre-created here).

**What remains before v1.0 can be called shipped:**
1. Provision a real Linux VM + domain and run `docs/acceptance/public-deploy.md` Part B end-to-end (test-engineer).
2. Any defects found there get fixed and re-verified (developer → code-reviewer, security-auditor re-pass if the fix touches auth/TLS/exposure).
3. Human final sign-off on the live acceptance report.
4. project-manager closes milestone #8 only once the above lands.

_(Sprint 8-10 resequencing: ratified 2026-07-18 — see above.)_

## v1.0 — Public Deploy rescoped: live-VM acceptance waived, milestone closed (2026-07-19)

The human decided 2026-07-19 that Spark Playbook will never be deployed to a remote VM — local-only is now the permanent, explicit scope. This changes v1.0's Definition of Done: the on-VM live acceptance checklist (Part B of `docs/acceptance/public-deploy.md`) is no longer a blocker for closing milestone #8 — it is **waived**, not an open gap. v1.0 is considered complete on the strength of the unit suite (317 tests, see `docs/acceptance/public-deploy.md` A1) plus the local acceptance pass (Part A) alone — rows #33-#38 above updated accordingly. `docs/acceptance/public-deploy.md` Part B has been marked out-of-scope/waived (its checklist items are kept as documentation of what a hypothetical future VM deploy would need, not as tracked pending work). README's "Current status" section updated to match — no longer describes Part B as an open caveat.

**Milestone #8 (`v1.0 — Public Deploy`) closed 2026-07-19**, using the same `gh api` PATCH-to-closed mechanics used for the Sprint 5/6/7 milestone close-outs. All 6 of its constituent issues (#39-#44, backlog rows #33-#38) were already shipped/closed via Sprint 7's merge (`0c44b5c`); the only remaining blocker (Part B) is now waived by explicit human decision rather than outstanding, so nothing further gates the close. The milestone object itself shows 0 open / 0 closed issues via `gh api` (its 6 member issues migrated to the Sprint 7 milestone per the earlier release-vs-sprint-milestone note above and are tracked there, plus via the `release:v1.0` label, rather than under milestone #8's own issue count) — that is expected, not a discrepancy, and does not change the close decision: milestone #8 is the release-level tracking object, and its own Definition of Done (Part A + unit suite, Part B now waived) is met.

## Sprint 8 proposed and confirmed (2026-07-18)

project-manager proposed Sprint 8 scope per the ratified Sprint 8-10 resequencing above: Checkpointing (backlog row #28, US-C4, M) as the sole curriculum story, plus a recommendation to pull in the pre-existing open, unmilestoned tech-debt issue [#38](https://github.com/hoanghaithanh/Spark-Playbook/issues/38) (compose/cli.py fixed Compose project name causing cross-worktree cluster collisions, found during #36's acceptance pass) — same pattern as issue #31 riding alongside Sprint 6's curriculum stories. Human confirmed both 2026-07-18.

**Sprint 8 (GitHub milestone #10, 2026-07-21 - 2026-07-25) created 2026-07-18.** Issue [#47](https://github.com/hoanghaithanh/Spark-Playbook/issues/47) (Checkpointing, backlog row #28) filed and milestoned. Issue [#38](https://github.com/hoanghaithanh/Spark-Playbook/issues/38) milestoned into Sprint 8; a comment was added on the issue noting it should route through an architect/devops-engineer design check on port-allocation interaction (DRIVER_APP_UI_PORTS, app port :8000) before straight-to-developer implementation, per the issue's own suggested-fix-direction caveat - not a same-pipeline-as-usual tech-debt fix.

## Sprint 8 status check (2026-07-19)

Both of Sprint 8's issues are now closed. **#47 (Checkpointing, backlog row #28)** closed
2026-07-19 — see row #28 above for the full acceptance evidence and the commit-convention note
(closed directly via commit `1e7b80c`, without a `Fixes #47` keyword). **#38 (cross-worktree
Compose collision fix)** is also closed as of 2026-07-19 (`gh issue view 38` confirms `state:
CLOSED`, `closedAt: 2026-07-19T03:36:24Z`), closed directly via commit `d543f79`
(`fix(lifecycle): guard spawn/teardown against cross-worktree cluster collisions`) — same pattern
as #47: no `Fixes #38` keyword in the commit message, the same departure from this repo's
established close-at-merge convention. `gh api` confirms milestone #10 (Sprint 8) at 0 open / 2
closed issues — Sprint 8 is functionally complete.

Not closing the Sprint 8 milestone or running its retro in this update — sprint close-out is its
own ceremony (disposition of the two-issues-both-done state is trivial here, but CLAUDE.md still
calls for a recorded retro in `docs/retrospectives.md` before the milestone closes) and belongs at
the next project-manager sprint-planning checkpoint, not folded into a same-day backlog-status
edit. Flagging as ready for that checkpoint.

## Sprint 8 close-out complete (2026-07-19)

Sprint 8's close-out ceremony ran: retro recorded in `docs/retrospectives.md` (Sprint 8 section,
covering both issues plus the commit-convention and backlog-table-rendering observations), and
milestone #10 closed via `gh api` PATCH-to-closed (0 open / 2 closed, no open issues to flag).
Backlog row #28 (Checkpointing) status above is unchanged as already-accurate; nothing further to
update here.

## Sprint 9 proposed and confirmed (2026-07-19)

project-manager proposed Sprint 9 scope per the ratified Sprint 8-10 resequencing above: Fault
Tolerance & Lineage (backlog row #30, US-C9, L) as the sole story, solo (same pattern as
Checkpointing in Sprint 8 — an L-sized story with its own distinct engine consideration, no natural
pairing candidate in the backlog). Self-check evidence sourcing was already settled by the architect
2026-07-15 (`docs/architecture/topic-shell-redesign.md` Decision A — reveal-time REST pull reusing
`app_client.fetch_task_list()`, not a plan-matcher extension). Open Question 2 (worker-kill safety
UX) was confirmed still genuinely unresolved as an in-app control (`curriculum-topics-2026-07.md`,
`topic-shell-redesign.md` lines ~499-505), but does not block this story: the recommendation was to
proceed without an architect gate, since the worker-kill mechanism can ship as a documented
manual/external step (`docker kill`/`kill -9`), and the architecture doc itself notes Decision A's
evidence sourcing is unaffected either way. Human confirmed the proposal, including proceeding
without an architect pass, 2026-07-19.

Also assessed: pre-existing open, unmilestoned tech-debt issue
[#48](https://github.com/hoanghaithanh/Spark-Playbook/issues/48) (driver Spark UI deep links
unreachable through the public HTTPS stack, `release:v1.0`/`tech-debt`) as a candidate to ride along
with Sprint 9, per the #31/#38 precedent from Sprints 6/8. **Recommended against and left out** —
unlike #31/#38 (core-app tech debt actively affecting day-to-day work), #48 is scoped to the public
deploy surface the human already ruled out of future scope (local-only-forever decision, 2026-07-19,
see the "v1.0 — Public Deploy rescoped" section above; `deploy-lan` is unaffected). A comment was
left on #48 flagging it as a Won't Fix candidate given that decision, without closing it — the human
did not confirm a close, only agreed to leave it out of Sprint 9.

**Sprint 9 (GitHub milestone #11, 2026-07-19 – 2026-07-23) created 2026-07-19.** Issue
[#49](https://github.com/hoanghaithanh/Spark-Playbook/issues/49) (Fault Tolerance & Lineage, backlog
row #30) filed and milestoned.

## Sprint 9 close-out complete (2026-07-19)

Sprint 9's close-out ceremony ran: retro recorded in `docs/retrospectives.md` (Sprint 9 section,
covering #49 plus a note on the same-day, unrelated `93d8876` deploy-lan CI commit that landed in
the same window but isn't sprint scope), and milestone #11 closed via `gh api` PATCH-to-closed (0
open / 1 closed, no open issues to flag). Backlog row #30 (Fault Tolerance & Lineage) status above
is unchanged as already-accurate; nothing further to update here. Sprint 10 is not proposed in this
pass — that's a separate sprint-planning step.

## Sprint 10 proposed and confirmed (2026-07-19)

project-manager proposed Sprint 10 scope per the ratified Sprint 8-10 resequencing above: Kafka
infra (backlog row #19, Phase 3 — conditional Kafka/KRaft in the compose template, synthetic
producer script, wired into the streaming topic, L) as the sole story, solo (same pattern as
Checkpointing/Fault-Tolerance-&-Lineage in Sprints 8-9 — an L-sized story with its own distinct
infrastructure consideration, no natural pairing candidate in the backlog). Unlike the recent
curriculum stories, this one introduces a new compose-lifecycle service rather than content against
an already-settled engine, so it is routed through an **architect design pass before developer
implementation** — the pipeline's next step is architect, not developer directly. Human confirmed
the proposal, including the architect-first routing, 2026-07-19.

Also assessed: pre-existing open, unmilestoned tech-debt issue
[#48](https://github.com/hoanghaithanh/Spark-Playbook/issues/48) (driver Spark UI deep links
unreachable through the public HTTPS stack) as a candidate to ride along with Sprint 10, per the
#31/#38 precedent from Sprints 6/8. **Left out, same reasoning as Sprint 9** — #48 is scoped to the
public deploy surface the human already ruled out of future scope (local-only-forever decision,
2026-07-19), and a Won't-Fix-candidate comment was already left on it during Sprint 9 planning; no
new action taken on it here.

**Sprint 10 (GitHub milestone #12, 2026-07-23 – 2026-07-27) created 2026-07-19.** Issue
[#50](https://github.com/hoanghaithanh/Spark-Playbook/issues/50) (Kafka infra, backlog row #19)
filed and milestoned; a comment was added on the issue noting the architect-first routing per the
reasoning above.

## Sprint 10 status check (2026-07-19)

Sprint 10's sole story, **#50 (Kafka infra, backlog row #19)**, closed 2026-07-19 — see row #19
above for the full acceptance evidence and the commit-convention note (closed directly via the
delivery-chain commits `8afc625`/`9188254`/`af8fb9e`/`db3ef55`, without a `Fixes #50` keyword — same
departure pattern as #47/#38 in Sprints 8/9, not a new problem). `gh api` confirms milestone #12
(Sprint 10) at 0 open / 1 closed issue — Sprint 10 is functionally complete.

Not closing the Sprint 10 milestone or running its retro in this update — sprint close-out is its
own ceremony (retro needs the human's own what-went-well/what-didn't input, which hasn't been
gathered yet) and belongs at the next project-manager sprint-planning checkpoint, same pattern as
every prior sprint's status-check → close-out split. Flagging as ready for that checkpoint.

## Sprint 10 close-out complete (2026-07-19)

Sprint 10's close-out ceremony ran: retro recorded in `docs/retrospectives.md` (Sprint 10 section,
covering #50 plus the architect-first routing, the ADR resume-and-amend round, the R-K6
multi-stage-catch, and the deliberate US-3.2/3.3 in-scope-vs-deferred acceptance scoping), and
milestone #12 closed via `gh api` PATCH-to-closed (0 open / 1 closed, no open issues to flag).
Backlog row #19 (Phase 3 Kafka infra) status above is unchanged as already-accurate; nothing further
to update here. Sprint 11 is not proposed in this pass — that's a separate sprint-planning step.

## New release milestone: v1.1 — Live Market Data Streaming (2026-07-19)

A new body of work — replacing the synthetic-data framing behind backlog row #18 (Structured
Streaming + Kafka) with a genuinely live demo — was approved by the human on 2026-07-19, per the
full plan at `C:\Users\hoang\.claude\plans\for-18-i-want-lazy-candle.md`: real Coinbase crypto
(public `ticker` channel, no API key needed) and Finnhub stock (free-tier WebSocket) feeds land on
a keyed Kafka `prices` topic; genuine dynamic subscribe/unsubscribe (not a client-side filter) is
driven by a log-compacted `price-subscriptions` control topic, so a browser ticker selection
actually changes what the upstream producer is subscribed to; a real Spark Structured Streaming job
(`content/structured-streaming/`) runs watermarking, windowed OHLC aggregation, and checkpoint
recovery against that data (including a real-data late-tick injection knob for the late-data demo,
since live feeds can't be made to produce late ticks on demand); and a live browser price dashboard
(`PriceCollector` → SSE, hand-rolled inline-SVG charts, no new frontend dependency) shows prices
updating in real time as the subscription selection changes. This supersedes the synthetic-data
framing in the existing requirements docs for #18 (`curriculum-topics-2026-07.md` US-C7,
`topics-content-spec.md` §11) — that divergence is to be stated explicitly when requirements are
updated, not left silently contradicting what's on record.

Same release-scale reasoning as `v1.0 — Public Deploy`: this is multi-area work (Kafka topics,
Spark job, dashboard, secrets handling) different in character and size from a single curriculum
sprint story, so it's filed as its own **release milestone**,
[`v1.1 — Live Market Data Streaming`](https://github.com/hoanghaithanh/Spark-Playbook/milestone/13)
(GitHub milestone #13), mirroring exactly how `v1.0` (milestone #8) was set up — including leaving
it **empty for now**. Per the plan's own execution sequence, issues for the 4 sub-stories
(producer + both Kafka topics + dynamic subscribe/unsubscribe; the Spark Structured Streaming
content; the live price dashboard panel; the query-progress widget) are **not filed yet** — that's
requirements-analyst's job next (formalizing the real-data pivot and superseding the synthetic
framing explicitly), followed by an architect ADR (finalizing the subscribe/unsubscribe wire
protocol, which the plan intentionally left at "shape" level) before development starts, same as the
plan's own step 2/3 sequencing.

**Not pulled into any sprint yet.** The milestone exists purely as the release-level container; when
the sub-stories are ready to be scheduled, that's a future sprint-planning checkpoint, same pattern
as v1.0's issues later landing in Sprint 7.

## Sprint 11 proposed and confirmed (2026-07-19)

project-manager proposed, human confirmed: **Sprint 11** pulls in backlog row #16 (Curriculum
topic: UDF vs pandas UDF serialization cost — timing comparison + plan distinction, M) as its sole
story, interleaved alongside the early, non-coding (project-manager/requirements-analyst/architect)
steps of the newly created `v1.1 — Live Market Data Streaming` release milestone (#13) — same
interleaving precedent as Sprint 7 running alongside curriculum work, applied here in reverse
(a self-contained curriculum story running alongside a release's early planning steps, before any of
its coding-heavy sub-stories are ready to pull into a sprint themselves).

**Sprint 11 (GitHub milestone #14, 2026-07-27 – 2026-07-31) created 2026-07-19**, picking up where
Sprint 10 (GitHub milestone #12, ended 2026-07-27) left off. Issue
[#51](https://github.com/hoanghaithanh/Spark-Playbook/issues/51) (UDF vs pandas UDF, backlog row
#16) filed and milestoned 2026-07-19; requirements-analyst formalization was the next pipeline step
before development starts, same gap as existing between #18's original filing and its eventual
requirements work.

**Correction, 2026-07-20 (requirements-analyst):** the note above anticipated the formalization landing in `curriculum-topics-2026-07.md` alongside the other US-C* stories. That doc explicitly excludes #16 (UDF vs pandas UDF) as unaffected by its 2026-07-15 redesign batch — it was never part of the imported mockup content. The concrete acceptance criteria were formalized instead by extending US-4.3 in place in `docs/requirements/spark-playbook-mvp.md` (its original home), not by adding a new US-C entry. See row #16 above for the full disposition, including the architect-pass recommendation (none needed) and the two genuinely open items flagged for the developer.

**Flagging for a future project-manager cleanup pass (not fixed in this update):** backlog row #17
(Memory Management & spill, US-4.4) appears stale/superseded by row #32 (Memory Management, Done
Sprint 6, GitHub issue #36) — both describe the same unified-memory-manager eviction-under-contention
scope, and #32 already shipped it with human sign-off. Row #17's status note above has been updated
to surface this explicitly so it isn't silently pulled into a future sprint as live scope; retiring
or re-scoping the row is left for a dedicated cleanup pass rather than decided here.

## New release milestone: v1.2 — Multi-Broker Kafka Cluster & Monitor (2026-07-19)

A new body of work — turning the single-node Kafka broker shipped in #50 (backlog row #19) into a
user-configurable multi-broker KRaft cluster, plus the observability layer needed to actually see it
work — was approved by the human on 2026-07-19, per the full plan at
`C:\Users\hoang\.claude\plans\for-18-i-want-lazy-candle.md`: 1-5 user-configurable brokers (default
3, RF=3/min-insync-replicas 2), a new "Kafka" section in the existing cluster-config drawer (folded
into the single Spawn/Teardown action, same UI pattern as the existing memory/shuffle-partition
ranges), a new Kafka observability data layer (CLI-shellout, since KafkaAdminClient was tried and
proven a dead end per #50's own ADR — `docs/architecture/kafka-streaming-infra.md`), a JMX exporter
for heap/GC and produce/consume latency plus per-broker idle-ratio metrics, a 4th tab in the existing
Cluster Monitor slide-in panel (built against a specific design mockup, alongside the existing
per-node/task/ETA/skew tabs from backlog rows #9-#13), and a broker-kill fault-tolerance demo
(kill one broker, observe ISR shrink/leader re-election, RF=3/min-isr=2 keeps the cluster serving) —
the Kafka-cluster analog of row #30's worker-kill Spark fault-tolerance topic.

**This explicitly reverses part of #50's shipped ADR** — Decision D1 in
`docs/architecture/kafka-streaming-infra.md` deliberately made Kafka "not a user-facing toggle" for
that story; broker count now becomes a user-facing config knob. That reversal is a scope decision the
human made explicitly when approving this milestone, not an oversight of the earlier ADR.

Same release-scale reasoning as `v1.0 — Public Deploy` and `v1.1 — Live Market Data Streaming`: this
is multi-area work (compose/cluster-lifecycle, a new observability data layer, JMX/monitoring UI,
fault-tolerance demo) different in character and size from a single curriculum sprint story, so it's
filed as its own **release milestone**,
[`v1.2 — Multi-Broker Kafka Cluster & Monitor`](https://github.com/hoanghaithanh/Spark-Playbook/milestone/15)
(GitHub milestone #15), mirroring exactly how `v1.0` (milestone #8) and `v1.1` (milestone #13) were
set up — including leaving it **empty for now**. The 5 sub-stories (a-e above) are **not filed as
GitHub issues yet** — that's requirements-analyst's job next, followed by an architect design pass,
same pipeline order as v1.0/v1.1's own release-milestone process. Tracked in the backlog table as new
row #40 above.

**Sequencing: BEFORE v1.1's remaining sub-stories.** v1.1 (milestone #13, backlog row #18) is
unblocked but its 4 sub-story issues — [#52](https://github.com/hoanghaithanh/Spark-Playbook/issues/52),
[#53](https://github.com/hoanghaithanh/Spark-Playbook/issues/53),
[#54](https://github.com/hoanghaithanh/Spark-Playbook/issues/54),
[#55](https://github.com/hoanghaithanh/Spark-Playbook/issues/55) — are all still unstarted (no
development has begun on any of them). v1.2's multi-broker cluster is scoped to land **before** those
sub-stories are picked up, since the producer/Spark job/dashboard they build should target the
multi-broker topology rather than inherit the single-node broker from #50 and need rework later. This
mirrors how row #19 (Kafka infra, Sprint 10) was itself a prerequisite consumed by row #18 (v1.1) —
same "infra-before-the-thing-that-depends-on-it" sequencing, one level up.

**Not pulled into any sprint yet.** Sprint 11 (GitHub milestone #14, 2026-07-27 – 2026-07-31, active)
already has its sole story confirmed (#51, UDF vs pandas UDF, backlog row #16) — v1.2's sub-stories
aren't ready to schedule regardless (no requirements/architect pass done yet), so there's no conflict
with Sprint 11's existing scope. When the sub-stories are ready, that's a future sprint-planning
checkpoint, same pattern as v1.0's and v1.1's issues later landing in dedicated sprints.

## Sprint 11 status check (2026-07-20)

Sprint 11's sole story, **#51 (UDF vs pandas UDF, backlog row #16)**, closed 2026-07-20 — see row
#16 above for the full acceptance evidence and the commit-convention note (closed directly, no
`Fixes #51` commit-message keyword, same departure pattern as #47/#38/#50 in recent sprints). `gh
api` confirms milestone #14 (Sprint 11) at 0 open / 1 closed issue — Sprint 11 is functionally
complete.

Not closing the Sprint 11 milestone or running its retro in this update — sprint close-out is its
own ceremony, per the same status-check → close-out split used for every prior sprint. Flagging as
ready for that checkpoint.

## Sprint 11 close-out complete (2026-07-20)

Sprint 11's close-out ceremony ran: retro recorded in `docs/retrospectives.md` (Sprint 11 section),
and milestone #14 closed via `gh api` PATCH-to-closed (0 open / 1 closed, no open issues to flag).
Backlog row #16 (UDF vs pandas UDF) status above is unchanged as already-accurate; nothing further
to update here. Sprint 12 is not proposed in this pass — that's a separate sprint-planning step.


## New body of work: Kafka Curriculum (learn Kafka itself) — pending milestone decision (2026-07-20)

A new curriculum track — Kafka topics that teach Kafka as a distributed system in its own right,
not Kafka-as-plumbing-under-a-PySpark-exercise — was approved by the human on 2026-07-20, via an
interactive planning session (not yet captured as a saved plan file, unlike v1.0/v1.1/v1.2's
plans). It adds 12 new `content/` topic folders (4 basic, 4 intermediate, 4 advanced — see backlog
row #41) using the exact same shell/manifest/notebook pattern every existing topic already uses.
Requirements formalized in `docs/requirements/kafka-curriculum.md` (requirements-analyst, 2026-07-20).

**Scope expanded same-day by two further human decisions (2026-07-20, after reviewing the
requirements doc):** (1) the topics-index page will visually group the 12 Kafka topics and the 15
existing Spark topics into two tracks, so this is **no longer purely `app/`-code-free** — it needs a
new manifest field plus a small `app/topics/loader.py` + template change; (2) US-KC8
(`kafka-serialization-schema-evolution`) will get a real schema-registry + Avro/Protobuf demo, adding
a new compose-stack service and driver-image dependency — comparable in kind to how Kafka itself
was introduced as infra in #50's ADR, and likely warranting its own architecture doc. Both are
recorded in `kafka-curriculum.md`'s "Resolved decisions" section and flow through to backlog row #41
above.

**Unlike v1.0/v1.1/v1.2, no release milestone is created by this update.** This body of work is
wide (12 independently-shippable, mostly S/M-sized topics) rather than deep/multi-area the way
v1.0 (Public Deploy), v1.1 (Live Market Data Streaming), and v1.2 (Multi-Broker Kafka Cluster &
Monitor) were — it may fit more naturally as several ordinary curriculum sprints (the same pattern
already used for rows #14/#15/#25/#29 in Sprint 5, or #27/#32/#26 in Sprint 6) than as its own
release milestone. That call — new release milestone vs. fold into ordinary sprint cadence vs. some
other grouping — is explicitly left to project-manager's next planning pass, per CLAUDE.md's
milestone-ownership rule. No GitHub issues are filed yet either, matching how v1.1/v1.2 both left
issue-filing until after their architect pass.

**Two topics still carry real status caveats, not silently smoothed over** (see backlog row #41 and
`kafka-curriculum.md` for full detail): US-KC10 (monitoring/observability) is partially blocked on
v1.2's still-open #58/#59; US-KC11 (exactly-once/transactions) needs a feasibility spike before an
architect can commit to a design. US-KC8 (serialization/schema evolution) is no longer
blocked-on-missing-tooling — it's gated on an architect infra decision instead, now that a schema
registry is confirmed in scope.

**Relationship to v1.1/v1.2, restated for traceability:** this is additive, not a supersession or
amendment of either — see `kafka-curriculum.md`'s "Relationship to existing docs" section for the
specific overlap with v1.1's `structured-streaming` topic (US-KC7) and v1.2's broker-kill/JMX work
(US-KC5/US-KC10).

**Not pulled into any sprint yet.** Four open questions remain, explicitly deferred by the human
until after project-manager and architect have reviewed this doc: US-KC7's standalone-vs-fold status,
exact `order` values within each of the two now-grouped tracks, US-KC11's spike timing, and per-topic
broker-count defaults.

## Kafka Curriculum: milestone decision (2026-07-20, project-manager)

**Decision: no new release milestone.** Weighing this against the release-scale reasoning used for
v1.0/v1.1/v1.2 (each "multi-area work... different in character and size from a single curriculum
sprint story"): those three were deep and tightly coupled — a handful of interdependent sub-stories
that had to land together as one coherent release (containerize+proxy+TLS+hygiene; producer+job+
dashboard+widget; drawer-config+observability-layer+JMX+UI-tab+demo). The Kafka curriculum is the
opposite shape — 12 independently-shippable, mostly S/M topics, explicitly described by its own
requirements doc as "wide... rather than deep/multi-area." It is structurally the same kind of body
of work as `curriculum-topics-2026-07.md`'s earlier batch (US-C1-C10, ~9 stories formalized in one
doc but never given a release milestone — each pulled into an ordinary sprint individually or in
small pairs: rows #14/#15/#25/#29 into Sprint 5, #26/#27/#32 into Sprint 6, #28 into Sprint 8, #30
into Sprint 9). The same treatment applies here: **backlog row #41 has been split into 12 individual
per-topic rows (#41-#52 above)**, matching the "one topic, one story" grain the doc itself calls out,
so each topic can be scheduled independently as it becomes ready — the same table shape every other
curriculum topic in this backlog already uses.

**No GitHub issues filed yet, for any of the 12.** This is a deliberate difference from v1.2's
pattern (which filed all 5 sub-story issues under its release milestone immediately, before the
architect pass) — that precedent fits release milestones, where all sub-stories are jointly scoped by
one ADR at once. It does not fit here: 9 of the 12 topics are genuinely architect-blocked before any
issue should be filed —
- **US-KC1-KC4, KC6, KC7, KC9, KC12 (rows #41-44, 46-47, 49, 52)** are content-buildable today in
  isolation, but all 27 topics' rendering (including these) changes once the topics-index grouping
  (G-KC5) lands — a new manifest field, `app/topics/loader.py`, and template change that the human
  has already approved in scope but left the mechanism (field name, per-track `order` semantics) to
  the architect. Filing issues for these now, before that mechanism is decided, risks locking in a
  stale manifest shape or forces a second pass later. This mirrors how Kafka infra itself (#50) was
  explicitly routed "architect-first" before any developer work in Sprint 10, despite being a single
  well-scoped story.
- **US-KC8 (row #48)** is explicitly gated on an architect infra decision (schema registry product +
  client library), not buildable at all until that lands.
- **US-KC10 (row #50)** is partially blocked on v1.2's still-open #58/#59.
- **US-KC11 (row #51)** needs a pre-architecture feasibility spike before a design can be committed
  to.

**Next step: an architect pass**, scoped to at minimum (a) the topics-index grouping mechanism
(manifest field, per-track ordering, template change — affects all 27 existing/new topics), (b) the
schema-registry infra decision for US-KC8 (likely its own ADR, comparable to `kafka-streaming-infra.md`),
and (c) a call on when US-KC11's feasibility spike runs relative to the rest of the pass. This is a
larger-than-typical architect pass for what would otherwise be ordinary content topics — flagged
explicitly since 9 of the 12 stories nominally look "just content" but aren't fully unblocked until
this lands. Only after that pass should the ready subset of topics be pulled into a sprint and get
GitHub issues filed, one sprint's worth at a time, same as every prior curriculum batch.

**Not deciding, and not foreclosing, the 4 open questions** (`kafka-curriculum.md`'s Open Questions
1, 2, 5, 6 — US-KC7 standalone-vs-fold, exact per-track `order` values, US-KC11 spike timing,
per-topic `kafka_broker_count` defaults): none of the 12 new backlog rows above assert a value for
any of these; row #47 (US-KC7) explicitly flags Open Question 1 as unresolved, row #48 (US-KC8) and
row #51 (US-KC11) explicitly route the gating decisions to the architect rather than guessing.

**Sequencing note relative to active release milestones:** v1.1 (milestone #13, 4 open sub-story
issues #52-#55) and v1.2 (milestone #15, 3 open sub-story issues #58-#60) remain the two active
release milestones and are unaffected by this decision — the Kafka curriculum work is independent
backlog scope, not folded into either. No sprint is currently active (Sprint 11/milestone #14 closed
2026-07-20); Sprint 12 has not yet been proposed.

## Sprint 12 execution (2026-07-20, project-manager)

Human confirmed go on the previously proposed Sprint 12 scope. **Sprint 12 (GitHub milestone
[#16](https://github.com/hoanghaithanh/Spark-Playbook/milestone/16), 2026-08-03 – 2026-08-07)**
created and 4 issues filed and milestoned: [#62](https://github.com/hoanghaithanh/Spark-Playbook/issues/62)
(`kafka-architecture-kraft`, row #41, bundles the D-KC1 topics-index grouping build),
[#63](https://github.com/hoanghaithanh/Spark-Playbook/issues/63) (`kafka-topics-partitions`, row #42),
[#64](https://github.com/hoanghaithanh/Spark-Playbook/issues/64) (`kafka-producers-delivery`, row #43),
[#65](https://github.com/hoanghaithanh/Spark-Playbook/issues/65) (`kafka-consumers-groups`, row #44).

**Note on Open Question 1 resolution:** since the "Kafka Curriculum: milestone decision" section
above was written, the human resolved Open Question 1 — US-KC7 (`kafka-spark-structured-streaming`)
folds into v1.1's `structured-streaming` topic (issue #53) rather than shipping standalone (see row
#47, now closed/not-an-independent-story). This does not affect Sprint 12's scope; KC7 was never a
candidate for this batch. The Kafka curriculum's independently-schedulable set is 11 topics (rows
#41-46, #48-52), not 12, going forward.

KC5/KC6/KC9/KC12, KC8/KC10/KC11 (gated), and KC7 (folded) remain deferred/unscheduled per the
reasoning already recorded above; unaffected by this Sprint 12 pull.
