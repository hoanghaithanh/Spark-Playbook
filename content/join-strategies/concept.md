# Join Strategies: Broadcast vs Sort-Merge vs Shuffle-Hash

## What it is

When Spark's Catalyst optimizer plans a join, it must pick a **physical join
strategy** — a concrete algorithm for actually matching rows from both sides
on the join key. For equi-joins (the overwhelming majority of real joins),
Spark chooses between three strategies, in roughly this order of preference:

1. **Broadcast hash join.** If one side is small enough (below
   `spark.sql.autoBroadcastJoinThreshold`, default 10MB, estimated from table
   statistics or an explicit `broadcast()` hint), Spark ships a full copy of
   that side to *every* executor as an in-memory hash table
   (`BroadcastExchange`), then each task on the large side does a local
   hash-join lookup (`BroadcastHashJoin`) — **no shuffle of the large side at
   all**. This is by far the cheapest strategy when it applies.
2. **Sort-merge join.** The default fallback when neither side is
   broadcast-eligible. Both sides are shuffled (`Exchange`) so all rows for a
   given key land on the same partition, each side is sorted on the join key
   (`Sort`), and the join (`SortMergeJoin`) walks both sorted streams together
   like a merge step. Handles arbitrarily large data on both sides, at the
   cost of two shuffles plus two sorts.
3. **Shuffle-hash join.** Both sides are still shuffled (`Exchange`), but
   instead of sorting, Spark builds an in-memory hash table from the smaller
   *shuffled* partition and probes it with the larger side
   (`ShuffledHashJoin`) — skipping the sort step. Spark only picks this when
   `spark.sql.join.preferSortMergeJoin=false` *and* its own cost heuristic
   decides the smaller side's per-partition size is cheap enough to
   hash-build; sort-merge is the safer default because it doesn't risk
   building an oversized hash table in memory.

## Why it matters

- **This is one of the single most common Spark interview topics.** "Why did
  my join not broadcast?" / "how do you force a broadcast join?" / "what's the
  difference between shuffle-hash and sort-merge?" come up constantly, and the
  honest answer requires knowing the actual decision rule, not folklore.
- **Broadcast join elimination is one of the highest-leverage manual
  optimizations available.** Recognizing "this side is small, it should
  broadcast but isn't" (usually because Spark's size *estimate* is wrong, not
  the actual data) is a real, frequently-tested debugging skill — Phase 4's
  capstone (US-4.6) includes exactly this scenario.
- **The strategy choice is driven by size estimates, which can be wrong.**
  Spark estimates table/relation size from either catalog statistics
  (`ANALYZE TABLE`) or a runtime size estimate for derived DataFrames: get the
  estimate wrong (e.g. after a filter Spark can't estimate selectivity for)
  and a join that "should" broadcast falls back to sort-merge instead, with a
  large, silent performance cliff.
- **Connects directly to the partitioning/shuffle topic.** Every join
  strategy here except broadcast introduces `Exchange` nodes — this topic is
  "shuffle mechanics, applied specifically to joins."

## What to look for in this exercise

The notebook forces all three strategies by manipulating table sizes and two
confs (`spark.sql.autoBroadcastJoinThreshold`,
`spark.sql.join.preferSortMergeJoin`) relative to a fixed-size large table.
For each of the three cells that produce a join:

- Form your hypothesis about the plan **before** running the cell.
- Read `.explain(mode="formatted")` yourself and identify the join node
  (`BroadcastHashJoin` / `SortMergeJoin` / `ShuffledHashJoin`) and whether an
  `Exchange`/`BroadcastExchange` precedes it.
- Only then call `playbook.checkpoint(df, topic="join-strategies")` and click
  **Reveal self-check** on this topic's page to compare the manifest-driven
  annotation against your own read (US-2.1) — the mapping in
  `content/join-strategies/manifest.yaml` labels each of the three strategies
  distinctly, with `BroadcastExchange`/`BroadcastHashJoin` taking precedence
  over the generic `Exchange` rule (most-specific-match-first).
- Use the runtime stage-metrics panel (US-2.2) to confirm: the broadcast case
  should show a much smaller (or zero) `shuffleReadBytes`/`shuffleWriteBytes`
  for the large side's stage than either shuffle-based strategy.

Shuffle-hash join selection is genuinely heuristic — if your plan still shows
`SortMergeJoin` after setting `preferSortMergeJoin=false`, that is real,
teachable Spark behavior (the cost heuristic decided against it), not a
notebook bug. Shrinking the smaller side further is the fix, and reasoning
about *why* it needed to shrink is the actual interview-relevant skill.
