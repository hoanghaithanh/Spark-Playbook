# Skew & Salting — demo mechanism (redesign after failed acceptance)

Status: Accepted · Date: 2026-07-18 · Drives: issue #46 / #35 (US-C2), Sprint 6

> **Amendment (2026-07-18, during implementation).** Steps 5–6 below originally
> asserted `salted max shuffleReadBytes < 2 * median` and predicted the salted max would
> "drop to ~1/10th → back near the median." That is arithmetically and physically wrong at
> the taught bucket count and is superseded by the **Salted-side assert** section at the end
> of this doc. Kept inline for provenance; read the amendment as authoritative.

## Context

The live acceptance run (`docs/qa/skew-salting-acceptance.md`, issue #46) proved AC1/AC2 of
US-C2 cannot be demonstrated with `groupBy(key).count()`. `count()` compiles to
`HashAggregate(partial) → Exchange → HashAggregate(final)`: every mapper pre-combines all its
rows for a key into a single `(key, count)` partial row *before* the shuffle. So reduce-side
shuffle-read bytes for any key's task are bounded by `distinct_keys × mappers`, never by the hot
key's raw row volume — the 60%-of-rows skew is absorbed map-side and never reaches the shuffle.
Confirmed structural across 3 trials (byte ratios 1.15x / 1.00x / 0x; identical 1728-byte median
at different row counts), not a `FACT_ROWS`/partition-count tune. We need an aggregation that
carries per-row payload across the shuffle so the hot key's task genuinely reads more bytes and
runs longer, and flattens after salting — while staying a no-join single-`groupBy` scenario
(US-C2 must stay distinct from AQE's skew-*join* splitting).

## Decision

Change the taught operation from `groupBy(key).count()` to
**`groupBy(key).agg(F.collect_list("amount"))`**, framed pedagogically as "collecting each key's
values into a per-key list."

`collect_list` is a `TypedImperativeAggregate`, planned via `ObjectHashAggregateExec`. Its
partial-aggregation buffer is the *accumulated array of every value seen* — the map-side "combine"
step cannot reduce a key's rows to a fixed-size scalar the way `count`/`sum` do, because the
list's whole purpose is to retain every element. Therefore each mapper's partial output for the
hot key still contains all of that mapper's hot-key values, and the reduce task the hot key hashes
to reads *all* ~3.6M hot-key values across the shuffle — genuinely row-volume-proportional skew in
both shuffle-read bytes and task duration. Salting the hot key into 10 sub-keys spreads that work
across 10 reduce tasks; the strip-and-re-aggregate step merges the sub-key arrays back with
`F.flatten`. This keeps the exact `salt → strip → re-aggregate → compare-to-original` verification
pattern that already worked, applied to a genuinely skewing operation.

## Alternatives considered

- **RDD `groupByKey()`** — the textbook no-map-side-combine case; would show real byte skew. Rejected:
  it would be the *first* pedagogical RDD use in `content/` (existing notebooks only use
  `sparkContext.parallelize().map()` for data *generation*, never as the taught operation), mixing
  APIs into an all-DataFrame curriculum, and it doesn't hand `checkpoint(df, …)` a DataFrame without
  extra conversion. `collect_list` gets the same shuffle behaviour without leaving the DataFrame API.
- **A different skewed-shuffle op (`repartition(key)`, window function)** — rejected: US-C2's AC is
  written around a `groupBy(key)` aggregation; `collect_list` stays a `groupBy(key)`, these diverge
  further from the AC text for no added benefit.
- **Keep `count()`, tune the data** — already ruled out by the test-engineer: structural, not a
  constant.

## Consequences

- Small, honest deviation from the AC's literal `groupBy(key).count()` wording — the operation is now
  `groupBy(key).collect_list(...)`. Spirit of the AC (single-sided groupBy, no join, skew visible in
  shuffle bytes + duration, flattens after salting) is fully preserved. Flag to project-manager /
  requirements-analyst to touch up the AC verb from "count" to "collect per-key values"; do not block
  on it.
- The hot key's reduce task now materializes a large in-memory array (~row-count-proportional). This
  is the point (it *is* the straggler) but it costs memory — keep `FACT_ROWS` modest (≈2M → ~1.2M
  hot-key values; a clear straggler without stressing a 4GB/2-core worker). This is a legitimate
  tuning knob, not the structural fix.
- The manifest is unchanged: `shuffleReadBytes` spotlight + `task_duration_quantiles` + no
  `plan_nodes` all stay valid (plan shape is still Exchange-based, self-check still reads existing
  `stage_metrics`). AC3/AC4 already PASS and are untouched.

## Developer notes — reimplementing `content/skew-salting/notebook.ipynb`

Keep the existing structure and the `reduce_side_task_metrics()` / `stages_snapshot()` helpers
as-is (they already pick the `shuffleReadBytes > 0` reduce stage correctly). Changes:

1. **Un-salted op (cell `b4bcc460`):**
   `counts_no_salt = skewed_df.groupBy("key").agg(F.sort_array(F.collect_list("amount")).alias("vals"))`
   — use `sort_array` so the correctness comparison later is order-independent (`collect_list` order
   is non-deterministic).
2. **Materialize with `.foreach(lambda row: None)`, NOT `.collect()`.** The hot key's list is huge;
   collecting to the driver would OOM/hang. Same reasoning as `content/aqe/notebook.ipynb`'s foreach
   note. This applies to both the un-salted and salted aggregations.
3. **Salted op (cell `e1ff4f35`):** same `withColumn("salted_key", …)` salting, then
   `salted_counts = salted_df.groupBy("salted_key").agg(F.sort_array(F.collect_list("amount")).alias("vals"))`.
   Re-aggregate by merging arrays, not summing:
   strip the `_N` suffix → `final = stripped.groupBy("key").agg(F.sort_array(F.flatten(F.collect_list("vals"))).alias("vals"))`.
4. **Correctness check:** compare the two `vals` arrays for equality per key (join on `key`, filter
   `salted_vals != no_salt_vals`, assert 0 mismatches) — both are `sort_array`ed so equal multisets
   compare equal. Keep the `assert mismatches == 0`.
5. **Restore hard asserts on the now-reliable signal.** Make **shuffle-read bytes** the hard AC1/AC2
   check (it is now deterministic and large): assert un-salted `straggler.shuffleReadBytes >= 2 *
   median` and salted `max shuffleReadBytes < 2 * median`. Keep task-*duration* ratios as strong
   corroborating `print()` output (they will also clearly separate, but ms-scale timing stays noisy —
   don't hard-block on duration alone). This satisfies AC1's "duration *and* shuffle-read bytes
   visibly larger" via the deterministic byte metric with duration as supporting evidence.
   **[SUPERSEDED for the salted side — see "Salted-side assert" below. The un-salted assert stands.]**
6. **Expected numbers (≈, 3-worker/2-core/4GB, FACT_ROWS≈2M):** un-salted straggler shuffle-read
   bytes ≫10x the median (all ~1.2M hot-key values land on one task); after 10-bucket salting the max
   drops to ~1/10th → back near the median. Duration follows the same shape.
   **[The "back near the median" clause is WRONG — see below.]**

## Salted-side assert — physics fix (2026-07-18)

Live runs (FACT_ROWS=2M, hot_fraction=0.6, 200 shuffle partitions, N=10 buckets) give a salted
`max/median` ratio of **22–30x**, not `<2x`. This is not noise or a tunable — it is structural,
and the original step-5 assert tests for something salting at N=10 does not deliver.

**Why.** After salting, each of the N sub-keys is an atomic chunk of ~`hot_mass/N` rows that
cannot split below one reduce task. With N=10 sub-keys hash-partitioned into P=200 fixed
partitions, at most 10 partitions carry hot mass (~120K rows each); the other ~190 carry only cold
mass (~4K each). The all-tasks median is dominated by the cold-only tasks, so
`salted_max/median ≈ 120K/4K ≈ 30x`. Empirically `ratio ≈ (f/(1−f))·(P/N) = 1.5·200/10 ≈ 30`,
matching the run. Making the *global distribution* flat requires N ≫ P (buckets-per-partition large
enough for the law of large numbers to smear hot mass across all 200 partitions); a sweep confirms
it: N=200 → ~2.6x, N=2000 → ~2.07x, N=5000 → ~1.88x on a thin CV≈0.2 margin. Robust flatness needs
N in the tens of thousands — pedagogically absurd and contradicting the taught "10 sub-keys."

**The assert was wrong, not the bucket count.** Salting into N buckets does not flatten the
distribution; it reduces the *straggler's load* by ~N-fold (one task goes from holding 100% of the
hot key's rows to ~1/N of them). That reduction is exactly what the "10 sub-keys" lesson teaches and
exactly what the run shows: `unsalted_max ≈ 1.2M-scale → salted_max ≈ 120–240K`, a 5–10x drop.

**Decision — assert the reduction salting actually delivers, as a hard assert:**

- Un-salted (unchanged): `straggler.shuffleReadBytes >= 2 * median` — the ~154x skew.
- Salted (replaces the `< 2 * median` line): **`salted_max_shuffleReadBytes < unsalted_straggler_shuffleReadBytes / 3`.**
  Both values are already computed by the existing metrics helper; no median redefinition or new
  bookkeeping needed. `/3` clears the observed 5–10x reduction with margin even in the worst
  realistic case (two hot buckets colliding on one partition ≈ 5x; three colliding is a rare
  bounded tail). `salted_max` is stable — hot-mass-per-bucket is multinomial (120K ± ~330); the
  only variance is which of the 200 partitions the ≤10 buckets collide in, and max
  buckets-per-partition among 10 balls in 200 bins is almost surely ≤3. Keep N=10, keep 200 shuffle
  partitions (no manifest deviation, consistent with every other topic), keep the `hot-0_0 … hot-0_9`
  framing. Print the achieved `unsalted_max / salted_max` ratio as corroboration.

**Rejected alternatives:** (a) scoping shuffle partitions down to ~8–12 puts N/P≈1 into
balls-in-bins high-variance territory — flat assert stays flaky, and de-flaking means raising N,
breaking the "10 buckets" pedagogy, plus a manifest deviation for no gain. (b) N≈5000–10000 clears
2x only on a thin margin, needs an ugly suffix (`hot-0_4231`) and a materially worse lesson.
(c) softening to `print()` repeats the exact pattern that failed live acceptance in #46.
(d-median) redefining "median" to salted-touched tasks only needs extra per-task bookkeeping to
identify touched tasks — more code than comparing the two straggler bytes already in scope.

## Concept.md changes

`content/skew-salting/concept.md` currently narrates `groupBy(key).count()`. Update it to
`groupBy(key)` + `collect_list`, and add the *why* — this is now a teachable point, not just a
mechanic:

- "What it is": the hot key's rows pile onto one task *because the aggregation carries every row's
  value across the shuffle* (a per-key list), so that one task both reads the most shuffle bytes and
  runs longest.
- Add one line distinguishing it from `count`/`sum`: those are map-side-combinable (each mapper
  pre-reduces a key to one partial row, so the skew never reaches the shuffle) — `collect_list` is
  not, which is exactly why it exposes the skew. This reinforces the map-side-combine concept rather
  than hand-waving.
- "What to look for": change the `groupBy(key).count()` references to the `collect_list` aggregation;
  the shuffle-read-bytes + per-task-duration framing stays.
- **Honesty fix (2026-07-18):** the current "no single straggler, tasks cluster together across the
  board" claim is false at N=10 / P=200 (see Salted-side assert above). Replace it with the true
  outcome: after 10-bucket salting the straggler's shuffle-read load drops ~10x — one task no longer
  holds the entire hot key; each of ~10 tasks now holds ~1/10 of it — which is what collapses the
  wall-clock. Add one line that salting reduces the straggler roughly in proportion to the bucket
  count (the real dial), instead of implying a magic flattening. Keep the `key_0 … key_9` framing.
- Leave the AQE-distinction paragraph (AC3, PASS) exactly as-is — still a no-join single `groupBy`.
