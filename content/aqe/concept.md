# Adaptive Query Execution (AQE)

## What it is

Adaptive Query Execution (AQE), on by default since Spark 3.2
(`spark.sql.adaptive.enabled=true`), lets Spark **re-optimize a query's plan
at runtime, using actual shuffle statistics from stages that have already
run** — something the static, pre-execution Catalyst optimizer can never do,
because it only has estimates. Concretely, after each shuffle stage
completes, AQE can:

- **Coalesce post-shuffle partitions.** `spark.sql.shuffle.partitions` sets
  a single fixed number *before* Spark has any idea how big the data
  actually is. AQE looks at the real, measured size of each post-shuffle
  partition and merges small adjacent ones together, so a query that
  configured 200 shuffle partitions but only produced (say) 40MB total of
  post-filter data doesn't pay scheduling overhead for 200 mostly-empty
  tasks.
- **Split skewed partitions in a join.** If one shuffle partition is
  measured to be dramatically larger than the others (a hot key), AQE can
  split just that partition into several smaller sub-partitions and join
  each sub-partition against a replicated copy of the corresponding
  partition on the other side — turning "one task takes 10x as long as
  everything else" into several parallel tasks of roughly even size.
- **Switch join strategies at runtime.** If a join was planned as
  sort-merge because Spark's *pre-execution* size estimate for one side was
  too high, but the *actual* post-shuffle size turns out to be small enough
  to broadcast, AQE can swap in a `BroadcastHashJoin` after the fact.

All three show up in the plan as an **AQE adaptive shuffle-reader** node
(`AQEShuffleRead` in Spark 3.2+/4.x; `CustomShuffleReader` in older 3.x) sitting
where a plain shuffle read would otherwise be, plus (for AQE-enabled queries)
`explain()` printing both an `== Initial Plan ==` (what Catalyst planned
before execution) and a `== Final Plan ==` (what actually ran) — the delta
between the two is AQE's runtime adaptation, made visible.

## Why it matters

- **Skew is one of the most common real-world Spark performance problems**,
  and AQE's skew-join split is the single most direct answer to "the job's
  wall-clock time is one slow task, not the average" — a scenario this
  project's own G1 goal names explicitly as an interview staple.
- **"AQE off vs on" is a genuinely different execution, not just a
  cosmetic plan difference** — the same skewed join can go from one
  dramatically slow task to several evenly-sized ones purely by flipping
  this setting, with no code change. Being able to point at *both* the plan
  difference and the task-duration difference in the Spark UI is what
  separates "I've heard of AQE" from "I can diagnose with it."
- **It changes what "the plan" even means.** Before AQE, `.explain()` showed
  one fixed plan. With AQE, the plan is provisional until execution — a
  detail that matters for anyone reading `explain()` output cold in an
  interview setting and assuming it's the whole story.
- **Builds directly on partitioning/shuffle (topic 1) and join strategies
  (topic 3).** AQE's three behaviors above are, respectively: a
  partitioning-count fix, a skew fix, and a join-strategy fix — all applied
  automatically, at runtime, to problems the earlier topics describe
  happening statically.

## What to look for in this exercise

The notebook generates a dataset with genuine, deliberate skew (three "hot"
keys carrying ~60% of all rows), then runs the **same skewed join** twice —
once with AQE off, once with AQE + skew-join handling on — plus a separate
partition-coalescing demonstration.

- Form a hypothesis **before** each run: for the AQE-off case, do you expect
  even or uneven task durations on the join's shuffle stage? For the AQE-on
  case, what new node type do you expect in the plan?
- After each run, read `.explain(mode="formatted")` yourself, call
  `playbook.checkpoint(df, topic="aqe")`, and click **Reveal self-check** —
  the manifest (`content/aqe/manifest.yaml`) labels the AQE adaptive-reader
  node distinctly from a plain `Exchange` (US-2.5).
- Use the runtime stage-metrics panel (US-2.2) to compare per-task behavior
  between the AQE-off and AQE-on runs of the *same* join — that comparison,
  not just the plan text, is the real evidence for what AQE actually did.
- For the coalescing cell, compare the configured `spark.sql.shuffle.partitions`
  (200 by default on this cluster) against the executed stage's `numTasks` in
  the stage-metrics panel — a materially smaller number is AQE's coalescing
  at work.
