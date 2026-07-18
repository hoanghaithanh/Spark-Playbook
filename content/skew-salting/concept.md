# Skew & Salting

## What it is

Data skew is one (or a few) keys holding vastly more rows than the rest, so
hash partitioning during a shuffle piles all of that key's rows onto a
single task — every other task finishes quickly while one straggler task
does most of the real work. This topic demonstrates it with
`groupBy(key).agg(F.collect_list("amount"))`: the hot key's rows pile onto
one reduce task *because the aggregation carries every row's value across
the shuffle* (a per-key list), so that one task both reads the most
shuffle bytes and runs longest. **Salting** is a manual fix: append a
random suffix (`key_0` … `key_9`) to the hot key *before* the shuffle, so
its rows hash-partition across several sub-keys instead of one. Aggregate
on the salted key first (spreading the hot key's rows across many tasks),
then strip the suffix and aggregate a second time (a much smaller shuffle,
merging the per-sub-key lists with `F.flatten`) to combine the sub-key
partials back into the true per-key result.

Why `collect_list` and not `count()`/`sum()`: those are **map-side
combinable** — each mapper pre-reduces every row it holds for a key into a
single partial row (a running count or sum) *before* the shuffle even
starts, so a hot key's row-volume skew is absorbed on the map side and
never reaches the shuffle boundary at all. `collect_list`'s partial buffer
*is* the accumulated array — there's no way to reduce "every value seen" to
a fixed-size partial without losing values — so each mapper's contribution
for the hot key still carries all of that mapper's rows across the
shuffle, and the reduce task the hot key hashes to genuinely reads more
bytes and runs longer. That's exactly why this topic teaches
`collect_list`, not `count()`, to expose the skew.

## Why it matters

- **Salting is a manual technique for cases AQE's own automatic skew
  handling cannot reach.** `spark.sql.adaptive.skewJoin.enabled` (covered by
  the existing AQE topic) detects an oversized shuffle partition on one side
  of a **join** and splits it against a replicated copy of the matching
  partition on the other side — but that mechanism only exists because a
  join has two sides to rebalance against. A skewed `groupBy(key).count()`
  (or any single-sided aggregation) has no second side for AQE's skew-join
  split to work with, so `spark.sql.adaptive.skewJoin.enabled` cannot help
  here regardless of whether AQE is on. Salting is the technique for exactly
  that gap — this topic is deliberately **not** a restatement of the AQE
  topic's automatic skew-join splitting; it is the manual technique AQE's
  skew-join handling was never built to cover.
- **One straggler task dominates wall-clock even with a healthy cluster.**
  More executors or more shuffle partitions doesn't help a `groupBy` whose
  hot key still hashes to one partition — the job's wall-clock is set by
  that single task, not the average.
- **Salting trades one lopsided shuffle for two balanced ones.** The
  salted-key aggregation spreads the hot key's rows across `N` sub-keys (10
  here), and the strip-and-re-aggregate step is a second, much smaller
  shuffle (one row per original key, not per original row) — a small fixed
  cost for eliminating the straggler.

## What to look for

- **Spark UI Stages tab** (`http://localhost:4040/stages/`, or
  `/api/v1/applications/<id>/stages/<id>/<attempt>/taskList`) — per-task
  shuffle-read bytes (and, as corroboration, `duration`) on the
  `groupBy(key).agg(collect_list(...))` reduce-side stage. Before salting:
  one task's shuffle-read bytes and duration are visibly larger than the
  rest (the hot key's entire ~60%-of-rows partition landed on it). After
  salting into 10 sub-keys and re-aggregating: that straggler's own load
  drops by roughly the bucket count (~10x here) — one task no longer holds
  the *entire* hot key, each of ~10 tasks now holds only about a tenth of
  it. With only 10 sub-keys spread across a much larger number of shuffle
  partitions, this does *not* flatten the entire task-duration/shuffle-bytes
  distribution to one even level (most of the other, never-touched tasks
  were already at the low "cold" baseline) — the reduction in the
  straggler's own load is what collapses the wall-clock, and it scales
  roughly with however many sub-keys you salt into.
- This is a task-duration/shuffle-bytes question, not a plan-shape one — the
  plan for a skewed `groupBy` and a salted one looks the same
  (`HashAggregate` → `Exchange` → `HashAggregate`), so the self-check
  evidence here comes from the Stages tab's per-task metrics, the same
  `stage_metrics` mechanism already used by other topics — not a new
  plan-node label.
