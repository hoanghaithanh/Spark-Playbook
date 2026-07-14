# Partitioning & Shuffle Mechanics

## What it is

A Spark DataFrame/RDD is split into **partitions** — independent chunks of
data that tasks process in parallel, one task per partition per stage. How
many partitions you have, and how evenly rows are distributed across them,
directly determines how much parallelism you get and how balanced the work
is across your cluster's cores.

A **shuffle** is what happens when Spark needs to move data *between*
partitions so that rows sharing some property (a join key, a group-by key,
a window's partition key) end up co-located in the same output partition.
Mechanically, a shuffle is:

1. Each task in the "map" side writes its output records into per-reduce-partition
   buckets (partitioned by a hash or range function of the shuffle key), spilling
   to local disk if the data doesn't fit in memory.
2. Each task in the "reduce" side then *fetches* the bucket meant for it from
   every map-side task's output, across the network.

In Spark's physical plan (`df.explain()`), a shuffle boundary shows up as an
**`Exchange`** node. Anything that requires rows with the same key to be
together — `groupBy`, non-broadcast joins, `repartition()`, most window
functions — introduces one or more `Exchange` nodes.

## Why it matters

Shuffles are usually the most expensive part of a Spark job:

- **Network + disk I/O.** Every byte that crosses a shuffle boundary is
  serialized, written to disk on the map side, transferred over the network,
  and deserialized on the reduce side. This dwarfs the cost of most
  in-partition transformations (`filter`, `map`, `select`).
- **Partition count controls parallelism *and* overhead.** Spark's default
  post-shuffle partition count is controlled by
  `spark.sql.shuffle.partitions` (default 200, independent of input size or
  cluster size). Too few partitions and each task handles too much data
  (slow, prone to spill); too many and per-task/per-partition scheduling
  overhead dominates and small output files pile up.
- **Skew turns "distributed" into "one slow task."** If a small number of
  keys hold a disproportionate share of the rows (a hot key), the reduce-side
  task(s) that own those keys' partitions do far more work than the rest —
  the job's wall-clock time is bounded by the *slowest* task, not the
  average.
- **It's the concept everything else builds on.** Broadcast-vs-shuffle join
  selection, bucketing (which exists specifically to *avoid* a shuffle),
  AQE's skew-join splitting and post-shuffle coalescing, and window-function
  cost are all shuffle mechanics applied to a specific operator — this topic
  is the foundation the rest of the curriculum ladder sits on.

## What to look for in this exercise

The notebook generates a synthetic keyed dataset spread across several input
partitions, then runs a `groupBy().agg()` — an operation that requires a
shuffle because rows with the same key may currently live on different
workers. While/after it runs:

- **`http://localhost:8080`** — confirm your configured worker count is
  `ALIVE`.
- **`http://localhost:4040`**, **Stages tab** — find the stage(s) around the
  `groupBy` and read the **Shuffle Read** / **Shuffle Write** columns; they
  should be nonzero.
- **`http://localhost:4040`**, **SQL tab** — open the query's plan and find
  the `Exchange` node marking the shuffle boundary; compare its "Number of
  partitions written" against `spark.sql.shuffle.partitions` (pre-baked into
  this cluster's `spark-defaults.conf` from what you set in the control
  panel above).
- **Executors tab** — confirm tasks completed across more than one executor,
  i.e. the shuffle really was distributed, not run in a single JVM.

Form a hypothesis about what you expect the shuffle read/write bytes and
partition count to look like *before* you open these tabs — that's the habit
this whole tool is built around (see Phase 2's self-check annotation engine,
coming later).
